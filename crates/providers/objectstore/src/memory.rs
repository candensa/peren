use std::{collections::HashMap, num::NonZeroUsize, ops::Range, sync::Arc};

use peren_cell::{LeaseError, OwnershipLease};
use peren_fleet::{
    Acquisition, Coordinator, CreateOutcome, Ownership, OwnershipRepository, RecoveryEvidence,
    ReplaceOutcome, RepositoryError as OwnershipError, StoredOwnership,
};
use peren_primitives::{CellId, NodeId, OwnershipEpoch, StorageRevision};
use peren_replication::{
    ReplicaImage, ReplicaPayload, ReplicaRepository, RepositoryError as ReplicaError,
};
use tokio::sync::Mutex;

#[derive(Clone, Default)]
pub struct MemoryStore {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    ownership: HashMap<CellId, StoredOwnership<u64>>,
    replicas: HashMap<CellId, Replica>,
    objects: HashMap<String, Vec<u8>>,
}

struct Replica {
    epoch: OwnershipEpoch,
    generation: StorageRevision,
    revision: StorageRevision,
    bytes: Vec<u8>,
    wal: Vec<u8>,
}

impl MemoryStore {
    pub async fn acquire(&self, local: NodeId, cell: CellId) -> Result<StoreLease, OwnershipError> {
        match Coordinator::new(self.clone(), local).acquire(cell).await {
            Ok(Acquisition::Acquired(lease)) => Ok(StoreLease {
                store: self.clone(),
                ownership: lease.ownership,
                version: lease.version,
            }),
            Ok(Acquisition::OwnedBy(owner)) if owner == local => {
                let stored = self.load(cell).await?.ok_or(OwnershipError::Malformed)?;
                if stored.ownership.owner != Some(local) {
                    return Err(OwnershipError::Unavailable);
                }
                Ok(StoreLease {
                    store: self.clone(),
                    ownership: stored.ownership,
                    version: stored.version,
                })
            }
            Ok(Acquisition::OwnedBy(_) | Acquisition::Contended) => {
                Err(OwnershipError::Unavailable)
            }
            Err(error) => match error {
                peren_fleet::AcquireError::Repository(error) => Err(error),
                peren_fleet::AcquireError::Epoch(_) => Err(OwnershipError::Malformed),
            },
        }
    }

    pub async fn recover(
        &self,
        local: NodeId,
        cell: CellId,
        evidence: RecoveryEvidence,
    ) -> Result<StoreLease, OwnershipError> {
        let mut state = self.state.lock().await;
        let stored = state
            .ownership
            .get_mut(&cell)
            .ok_or(OwnershipError::Malformed)?;
        if stored.ownership.owner != Some(evidence.owner())
            || stored.ownership.epoch != evidence.epoch()
        {
            return Err(OwnershipError::Unavailable);
        }
        let epoch = stored
            .ownership
            .epoch
            .next()
            .map_err(|_| OwnershipError::Malformed)?;
        stored.version += 1;
        stored.ownership = Ownership {
            cell,
            owner: Some(local),
            epoch,
        };
        Ok(StoreLease {
            store: self.clone(),
            ownership: stored.ownership,
            version: stored.version,
        })
    }

    pub async fn replica(
        &self,
        cell: CellId,
    ) -> Option<(OwnershipEpoch, StorageRevision, Vec<u8>)> {
        self.state
            .lock()
            .await
            .replicas
            .get(&cell)
            .map(|replica| (replica.epoch, replica.revision, replica.bytes.clone()))
    }

    pub async fn read(&self, key: &str) -> Option<Vec<u8>> {
        self.state.lock().await.objects.get(key).cloned()
    }

    pub async fn read_range(&self, key: &str, range: Range<usize>) -> Option<Vec<u8>> {
        self.state
            .lock()
            .await
            .objects
            .get(key)
            .and_then(|bytes| bytes.get(range).map(<[u8]>::to_vec))
    }

    pub async fn write(&self, key: &str, bytes: Vec<u8>) {
        self.state
            .lock()
            .await
            .objects
            .insert(key.to_string(), bytes);
    }
}

impl OwnershipRepository for MemoryStore {
    type Version = u64;

    async fn load(&self, cell: CellId) -> Result<Option<StoredOwnership<u64>>, OwnershipError> {
        Ok(self.state.lock().await.ownership.get(&cell).cloned())
    }

    async fn create(&self, ownership: Ownership) -> Result<CreateOutcome<u64>, OwnershipError> {
        let mut state = self.state.lock().await;
        if state.ownership.contains_key(&ownership.cell) {
            return Ok(CreateOutcome::Exists);
        }
        state.ownership.insert(
            ownership.cell,
            StoredOwnership {
                ownership,
                version: 1,
            },
        );
        Ok(CreateOutcome::Created(1))
    }

    async fn replace(
        &self,
        current: &u64,
        next: Ownership,
    ) -> Result<ReplaceOutcome<u64>, OwnershipError> {
        let mut state = self.state.lock().await;
        let Some(stored) = state.ownership.get_mut(&next.cell) else {
            return Ok(ReplaceOutcome::Changed);
        };
        if &stored.version != current {
            return Ok(ReplaceOutcome::Changed);
        }
        stored.version += 1;
        stored.ownership = next;
        Ok(ReplaceOutcome::Replaced(stored.version))
    }
}

impl ReplicaRepository for MemoryStore {
    async fn publish_through(
        &self,
        cell: CellId,
        epoch: OwnershipEpoch,
        revision: StorageRevision,
        payload: &ReplicaPayload,
    ) -> Result<(), ReplicaError> {
        let mut state = self.state.lock().await;
        let Some(owner) = state.ownership.get(&cell) else {
            return Err(ReplicaError::Fenced);
        };
        if owner.ownership.epoch != epoch || owner.ownership.owner.is_none() {
            return Err(ReplicaError::Fenced);
        }
        match state.replicas.get_mut(&cell) {
            Some(replica) if replica.epoch == epoch && replica.revision >= revision => {
                return Ok(());
            }
            Some(replica) if replica.epoch == epoch => {
                replica.revision = revision;
                replica.wal.extend_from_slice(&payload.wal_frames);
            }
            _ => {
                let mut wal = payload
                    .wal_header
                    .map_or_else(Vec::new, |header| header.to_vec());
                wal.extend_from_slice(&payload.wal_frames);
                state.replicas.insert(
                    cell,
                    Replica {
                        epoch,
                        generation: payload.generation,
                        revision,
                        bytes: payload.database.clone(),
                        wal,
                    },
                );
            }
        }
        Ok(())
    }

    async fn restore(&self, cell: CellId) -> Result<Option<ReplicaImage>, ReplicaError> {
        Ok(self
            .state
            .lock()
            .await
            .replicas
            .get(&cell)
            .map(|replica| ReplicaImage {
                epoch: replica.epoch,
                generation: replica.generation,
                revision: replica.revision,
                database: replica.bytes.clone(),
                wal: replica.wal.clone(),
            }))
    }

    async fn checkpoint(
        &self,
        cell: CellId,
        epoch: OwnershipEpoch,
        revision: StorageRevision,
        database: &[u8],
    ) -> Result<(), ReplicaError> {
        let mut state = self.state.lock().await;
        let owner = state.ownership.get(&cell).ok_or(ReplicaError::Fenced)?;
        if owner.ownership.owner.is_none() || owner.ownership.epoch != epoch {
            return Err(ReplicaError::Fenced);
        }
        state.replicas.insert(
            cell,
            Replica {
                epoch,
                generation: revision,
                revision,
                bytes: database.to_vec(),
                wal: Vec::new(),
            },
        );
        Ok(())
    }

    async fn prune(&self, _cell: CellId, _retain: NonZeroUsize) -> Result<usize, ReplicaError> {
        Ok(0)
    }
}

pub struct StoreLease {
    store: MemoryStore,
    ownership: Ownership,
    version: u64,
}

impl OwnershipLease for StoreLease {
    fn cell(&self) -> CellId {
        self.ownership.cell
    }

    fn owner(&self) -> NodeId {
        self.ownership.owner.expect("a lease always has an owner")
    }

    fn epoch(&self) -> OwnershipEpoch {
        self.ownership.epoch
    }

    async fn verify(&self) -> Result<(), LeaseError> {
        let state = self.store.state.lock().await;
        let current = state
            .ownership
            .get(&self.ownership.cell)
            .ok_or(LeaseError)?;
        (current.version == self.version && current.ownership == self.ownership)
            .then_some(())
            .ok_or(LeaseError)
    }

    async fn release(self) -> Result<(), LeaseError> {
        let next = Ownership {
            owner: None,
            ..self.ownership
        };
        match self.store.replace(&self.version, next).await {
            Ok(ReplaceOutcome::Replaced(_)) => Ok(()),
            Ok(ReplaceOutcome::Changed) | Err(_) => Err(LeaseError),
        }
    }
}
