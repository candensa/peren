use std::{collections::BTreeSet, fmt::Debug, num::NonZeroUsize};

use peren_primitives::{CellId, EpochOverflow, NodeId, OwnershipEpoch};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    pub const CURRENT: Self = Self { major: 1, minor: 0 };

    #[must_use]
    pub const fn compatible_with(self, peer: Self) -> bool {
        self.major == peer.major && self.minor >= peer.minor
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct PeerHello {
    pub node: NodeId,
    pub version: ProtocolVersion,
    pub capabilities: BTreeSet<PeerCapability>,
}

impl PeerHello {
    pub fn validate(&self) -> Result<(), PeerError> {
        if !ProtocolVersion::CURRENT.compatible_with(self.version) {
            return Err(PeerError::IncompatibleVersion {
                local: ProtocolVersion::CURRENT,
                peer: self.version,
            });
        }
        for capability in required_peer_capabilities() {
            if !self.capabilities.contains(&capability) {
                return Err(PeerError::MissingCapability(capability));
            }
        }
        Ok(())
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum PeerCapability {
    OwnershipCas,
    RecoveryQuorum,
    ReplicaRestore,
    GracefulDrain,
}

#[must_use]
pub fn required_peer_capabilities() -> BTreeSet<PeerCapability> {
    BTreeSet::from([
        PeerCapability::OwnershipCas,
        PeerCapability::RecoveryQuorum,
        PeerCapability::ReplicaRestore,
        PeerCapability::GracefulDrain,
    ])
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum PeerError {
    #[error("peer protocol {peer:?} is incompatible with local protocol {local:?}")]
    IncompatibleVersion {
        local: ProtocolVersion,
        peer: ProtocolVersion,
    },
    #[error("peer is missing required capability {0:?}")]
    MissingCapability(PeerCapability),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Ownership {
    pub cell: CellId,
    #[serde(rename = "node")]
    pub owner: Option<NodeId>,
    pub epoch: OwnershipEpoch,
}

#[derive(Clone, Debug)]
pub struct StoredOwnership<V> {
    pub ownership: Ownership,
    pub version: V,
}

pub trait OwnershipRepository: Send + Sync {
    type Version: Clone + Debug + Send + Sync + 'static;

    fn load(
        &self,
        cell: CellId,
    ) -> impl Future<Output = Result<Option<StoredOwnership<Self::Version>>, RepositoryError>> + Send;

    fn create(
        &self,
        ownership: Ownership,
    ) -> impl Future<Output = Result<CreateOutcome<Self::Version>, RepositoryError>> + Send;

    fn replace(
        &self,
        current: &Self::Version,
        next: Ownership,
    ) -> impl Future<Output = Result<ReplaceOutcome<Self::Version>, RepositoryError>> + Send;
}

pub enum CreateOutcome<V> {
    Created(V),
    Exists,
}

pub enum ReplaceOutcome<V> {
    Replaced(V),
    Changed,
}

pub struct Coordinator<R> {
    repository: R,
    local: NodeId,
}

impl<R: OwnershipRepository> Coordinator<R> {
    #[must_use]
    pub const fn new(repository: R, local: NodeId) -> Self {
        Self { repository, local }
    }

    pub async fn acquire(&self, cell: CellId) -> Result<Acquisition<R::Version>, AcquireError> {
        match self.repository.load(cell).await? {
            None => {
                let ownership = Ownership {
                    cell,
                    owner: Some(self.local),
                    epoch: OwnershipEpoch::new(1),
                };
                match self.repository.create(ownership).await? {
                    CreateOutcome::Created(version) => {
                        Ok(Acquisition::Acquired(Lease { ownership, version }))
                    }
                    CreateOutcome::Exists => Ok(Acquisition::Contended),
                }
            }
            Some(stored) if stored.ownership.owner.is_none() => {
                let next = Ownership {
                    owner: Some(self.local),
                    epoch: stored.ownership.epoch.next()?,
                    ..stored.ownership
                };
                match self.repository.replace(&stored.version, next).await? {
                    ReplaceOutcome::Replaced(version) => Ok(Acquisition::Acquired(Lease {
                        ownership: next,
                        version,
                    })),
                    ReplaceOutcome::Changed => Ok(Acquisition::Contended),
                }
            }
            Some(stored) => Ok(Acquisition::OwnedBy(stored.ownership.owner.unwrap())),
        }
    }
}

pub enum Acquisition<V> {
    Acquired(Lease<V>),
    OwnedBy(NodeId),
    Contended,
}

pub struct Lease<V> {
    pub ownership: Ownership,
    pub version: V,
}

#[derive(Clone, Debug)]
pub struct RecoveryObservation {
    owner: NodeId,
    epoch: OwnershipEpoch,
    lease_expires_at_ms: u64,
    observed_at_ms: u64,
    confirmations: BTreeSet<NodeId>,
}

impl RecoveryObservation {
    #[must_use]
    pub fn new(
        owner: NodeId,
        epoch: OwnershipEpoch,
        lease_expires_at_ms: u64,
        observed_at_ms: u64,
        confirmations: BTreeSet<NodeId>,
    ) -> Self {
        Self {
            owner,
            epoch,
            lease_expires_at_ms,
            observed_at_ms,
            confirmations,
        }
    }
}

pub struct RecoveryPolicy {
    quorum: NonZeroUsize,
    max_clock_skew_ms: u64,
}

impl RecoveryPolicy {
    #[must_use]
    pub const fn new(quorum: NonZeroUsize, max_clock_skew_ms: u64) -> Self {
        Self {
            quorum,
            max_clock_skew_ms,
        }
    }

    pub fn authorize(
        &self,
        observation: &RecoveryObservation,
    ) -> Result<RecoveryEvidence, RecoveryError> {
        let recovery_at = observation
            .lease_expires_at_ms
            .checked_add(self.max_clock_skew_ms)
            .ok_or(RecoveryError::InsufficientEvidence)?;
        let confirmations = observation
            .confirmations
            .iter()
            .filter(|node| **node != observation.owner)
            .count();
        if observation.observed_at_ms < recovery_at || confirmations < self.quorum.get() {
            return Err(RecoveryError::InsufficientEvidence);
        }
        Ok(RecoveryEvidence {
            owner: observation.owner,
            epoch: observation.epoch,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryEvidence {
    owner: NodeId,
    epoch: OwnershipEpoch,
}

impl RecoveryEvidence {
    #[must_use]
    pub const fn owner(self) -> NodeId {
        self.owner
    }

    #[must_use]
    pub const fn epoch(self) -> OwnershipEpoch {
        self.epoch
    }
}

#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error("dead-owner recovery requires expiry beyond clock skew and peer quorum")]
    InsufficientEvidence,
}

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("ownership repository is unavailable")]
    Unavailable,
    #[error("stored ownership record is malformed")]
    Malformed,
}

#[derive(Debug, Error)]
pub enum AcquireError {
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    #[error(transparent)]
    Epoch(#[from] EpochOverflow),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use uuid::Uuid;

    fn node() -> NodeId {
        NodeId::from_uuid(Uuid::new_v4())
    }

    #[test]
    fn expiry_without_peer_quorum_does_not_authorize_recovery() {
        let owner = node();
        let observation =
            RecoveryObservation::new(owner, OwnershipEpoch::new(4), 1_000, 1_500, BTreeSet::new());
        let policy = RecoveryPolicy::new(NonZeroUsize::new(2).unwrap(), 100);

        assert!(matches!(
            policy.authorize(&observation),
            Err(RecoveryError::InsufficientEvidence)
        ));
    }

    #[test]
    fn quorum_after_the_skew_window_authorizes_the_observed_owner_and_epoch() {
        let owner = node();
        let observation = RecoveryObservation::new(
            owner,
            OwnershipEpoch::new(4),
            1_000,
            1_100,
            BTreeSet::from([node(), node()]),
        );
        let evidence = RecoveryPolicy::new(NonZeroUsize::new(2).unwrap(), 100)
            .authorize(&observation)
            .unwrap();

        assert_eq!(evidence.owner(), owner);
        assert_eq!(evidence.epoch(), OwnershipEpoch::new(4));
    }

    #[test]
    fn owner_record_matches_the_released_json_format() {
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/current/persistence/owner.json");
        let bytes = std::fs::read(fixture).unwrap();
        let ownership: Ownership = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(
            ownership.cell,
            CellId::from_bytes(std::array::from_fn(|i| { u8::try_from(i).unwrap() }))
        );
        assert_eq!(
            ownership.owner,
            Some(NodeId::from_uuid(
                Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap()
            ))
        );
        assert_eq!(ownership.epoch, OwnershipEpoch::new(7));
        assert_eq!(
            serde_json::to_vec(&ownership).unwrap(),
            bytes.strip_suffix(b"\n").unwrap_or(&bytes)
        );
    }

    #[test]
    fn peer_hello_accepts_same_major_older_minor_versions() {
        let hello = PeerHello {
            node: node(),
            version: ProtocolVersion { major: 1, minor: 0 },
            capabilities: required_peer_capabilities(),
        };

        assert_eq!(hello.validate(), Ok(()));
    }

    #[test]
    fn peer_hello_rejects_incompatible_major_versions() {
        let hello = PeerHello {
            node: node(),
            version: ProtocolVersion { major: 2, minor: 0 },
            capabilities: required_peer_capabilities(),
        };

        assert!(matches!(
            hello.validate(),
            Err(PeerError::IncompatibleVersion {
                peer: ProtocolVersion { major: 2, minor: 0 },
                ..
            })
        ));
    }

    #[test]
    fn peer_hello_requires_protocol_capability_parity() {
        let mut capabilities = required_peer_capabilities();
        capabilities.remove(&PeerCapability::ReplicaRestore);
        let hello = PeerHello {
            node: node(),
            version: ProtocolVersion::CURRENT,
            capabilities,
        };

        assert_eq!(
            hello.validate(),
            Err(PeerError::MissingCapability(PeerCapability::ReplicaRestore))
        );
    }

    #[test]
    fn peer_hello_wire_format_is_stable() {
        let hello = PeerHello {
            node: NodeId::from_uuid(
                Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
            ),
            version: ProtocolVersion::CURRENT,
            capabilities: required_peer_capabilities(),
        };

        assert_eq!(
            serde_json::to_value(hello).unwrap(),
            serde_json::json!({
                "node": "11111111-1111-4111-8111-111111111111",
                "version": { "major": 1, "minor": 0 },
                "capabilities": ["ownership_cas", "recovery_quorum", "replica_restore", "graceful_drain"]
            })
        );
    }
}
