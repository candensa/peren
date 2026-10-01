use object_store::{PutMode, PutOptions, PutPayload, PutResult, UpdateVersion, path::Path};
use peren_cell::{LeaseError, OwnershipLease};
use peren_fleet::{
    Acquisition, Coordinator, CreateOutcome, Ownership, OwnershipRepository, RecoveryEvidence,
    ReplaceOutcome, RepositoryError as OwnershipError, StoredOwnership,
};
use peren_primitives::{CellId, NodeId, OwnershipEpoch};

const CAS_WRITE_ATTEMPTS: usize = 3;

use crate::BucketStore;

impl BucketStore {
    pub async fn acquire(
        &self,
        local: NodeId,
        cell: CellId,
    ) -> Result<BucketLease, OwnershipError> {
        match Coordinator::new(self.clone(), local).acquire(cell).await {
            Ok(Acquisition::Acquired(lease)) => Ok(BucketLease {
                store: self.clone(),
                ownership: lease.ownership,
                version: lease.version,
            }),
            Ok(Acquisition::OwnedBy(owner)) if owner == local => {
                let stored = self.load(cell).await?.ok_or(OwnershipError::Malformed)?;
                if stored.ownership.owner != Some(local) {
                    return Err(OwnershipError::Unavailable);
                }
                Ok(BucketLease {
                    store: self.clone(),
                    ownership: stored.ownership,
                    version: stored.version,
                })
            }
            Ok(Acquisition::OwnedBy(_) | Acquisition::Contended) => {
                Err(OwnershipError::Unavailable)
            }
            Err(peren_fleet::AcquireError::Repository(error)) => Err(error),
            Err(peren_fleet::AcquireError::Epoch(_)) => Err(OwnershipError::Malformed),
        }
    }

    pub async fn recover(
        &self,
        local: NodeId,
        cell: CellId,
        evidence: RecoveryEvidence,
    ) -> Result<BucketLease, OwnershipError> {
        let stored = self.load(cell).await?.ok_or(OwnershipError::Malformed)?;
        if stored.ownership.owner != Some(evidence.owner())
            || stored.ownership.epoch != evidence.epoch()
        {
            return Err(OwnershipError::Unavailable);
        }
        let next = Ownership {
            cell,
            owner: Some(local),
            epoch: evidence
                .epoch()
                .next()
                .map_err(|_| OwnershipError::Malformed)?,
        };
        match self.replace(&stored.version, next).await? {
            ReplaceOutcome::Replaced(version) => Ok(BucketLease {
                store: self.clone(),
                ownership: next,
                version,
            }),
            ReplaceOutcome::Changed => Err(OwnershipError::Unavailable),
        }
    }
}

impl BucketStore {
    async fn put_cas(
        &self,
        key: &Path,
        bytes: Vec<u8>,
        options: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        let mut last = None;
        for _ in 0..CAS_WRITE_ATTEMPTS {
            match self
                .store
                .put_opts(key, PutPayload::from(bytes.clone()), options.clone())
                .await
            {
                Err(error) if is_conditional_conflict(&error) => last = Some(error),
                Err(object_store::Error::NotImplemented) => {
                    return self.put_conditional_fallback(key, bytes, &options).await;
                }
                result => return result,
            }
        }
        Err(last.expect("conditional conflict retry loop records the last error"))
    }

    async fn put_conditional_fallback(
        &self,
        key: &Path,
        bytes: Vec<u8>,
        options: &PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        let PutMode::Update(version) = &options.mode else {
            return Err(object_store::Error::NotImplemented);
        };
        let _lock = self.cas.lock().await;
        let expected = version.e_tag.clone();
        match self.store.get(key).await {
            Ok(current) if current.meta.e_tag == expected => {}
            Ok(current) => {
                return Err(object_store::Error::Precondition {
                    path: key.to_string(),
                    source: format!("etag {:?} does not match {expected:?}", current.meta.e_tag)
                        .into(),
                });
            }
            Err(object_store::Error::NotFound { path, source }) => {
                return Err(object_store::Error::Precondition { path, source });
            }
            Err(error) => return Err(error),
        }
        let overwrite = PutOptions {
            mode: PutMode::Overwrite,
            ..PutOptions::default()
        };
        self.store
            .put_opts(key, PutPayload::from(bytes), overwrite)
            .await
    }
}

impl OwnershipRepository for BucketStore {
    type Version = String;

    async fn load(&self, cell: CellId) -> Result<Option<StoredOwnership<String>>, OwnershipError> {
        let key = owner_key(cell);
        match self.store.get(&key).await {
            Ok(result) => {
                let version = result.meta.e_tag.clone().ok_or(OwnershipError::Malformed)?;
                let bytes = result.bytes().await.map_err(map_ownership_error)?;
                let ownership: Ownership =
                    serde_json::from_slice(&bytes).map_err(|_| OwnershipError::Malformed)?;
                if ownership.cell != cell {
                    return Err(OwnershipError::Malformed);
                }
                Ok(Some(StoredOwnership { ownership, version }))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(map_ownership_error(error)),
        }
    }

    async fn create(&self, ownership: Ownership) -> Result<CreateOutcome<String>, OwnershipError> {
        let bytes = serde_json::to_vec(&ownership).map_err(|_| OwnershipError::Malformed)?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .put_cas(&owner_key(ownership.cell), bytes, options)
            .await
        {
            Ok(result) => result
                .e_tag
                .map(CreateOutcome::Created)
                .ok_or(OwnershipError::Malformed),
            Err(error) if is_cas_rejection(&error) => Ok(CreateOutcome::Exists),
            Err(_) => match self.load(ownership.cell).await? {
                Some(stored) if stored.ownership == ownership => {
                    Ok(CreateOutcome::Created(stored.version))
                }
                Some(_) => Ok(CreateOutcome::Exists),
                None => Err(OwnershipError::Unavailable),
            },
        }
    }

    async fn replace(
        &self,
        current: &String,
        next: Ownership,
    ) -> Result<ReplaceOutcome<String>, OwnershipError> {
        let bytes = serde_json::to_vec(&next).map_err(|_| OwnershipError::Malformed)?;
        let options = PutOptions {
            mode: PutMode::Update(UpdateVersion {
                e_tag: Some(current.clone()),
                version: None,
            }),
            ..PutOptions::default()
        };
        match self.put_cas(&owner_key(next.cell), bytes, options).await {
            Ok(result) => result
                .e_tag
                .map(ReplaceOutcome::Replaced)
                .ok_or(OwnershipError::Malformed),
            Err(error) if is_cas_rejection(&error) => Ok(ReplaceOutcome::Changed),
            Err(_) => match self.load(next.cell).await? {
                Some(stored) if stored.ownership == next => {
                    Ok(ReplaceOutcome::Replaced(stored.version))
                }
                Some(_) | None => Ok(ReplaceOutcome::Changed),
            },
        }
    }
}

pub struct BucketLease {
    store: BucketStore,
    ownership: Ownership,
    version: String,
}

impl OwnershipLease for BucketLease {
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
        let current = self
            .store
            .load(self.ownership.cell)
            .await
            .map_err(|_| LeaseError)?;
        current
            .filter(|value| value.version == self.version && value.ownership == self.ownership)
            .map(|_| ())
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

fn owner_key(cell: CellId) -> Path {
    Path::from(format!("cells/{cell}/owner.json"))
}

fn map_ownership_error(_: object_store::Error) -> OwnershipError {
    OwnershipError::Unavailable
}

pub(crate) fn is_cas_rejection(error: &object_store::Error) -> bool {
    matches!(error, object_store::Error::Precondition { .. }) || is_conditional_exists(error)
}

pub(crate) fn is_conditional_conflict(error: &object_store::Error) -> bool {
    if !is_conditional_exists(error) {
        return false;
    }
    let details = error.to_string();
    details.contains("409") || details.contains("ConditionalRequestConflict")
}

fn is_conditional_exists(error: &object_store::Error) -> bool {
    matches!(error, object_store::Error::AlreadyExists { .. })
}

#[cfg(test)]
mod tests {
    use super::*;
    use peren_cell::OwnershipLease;

    #[tokio::test]
    async fn file_bucket_acquire_release_and_reacquire() {
        let root = std::env::temp_dir().join(format!("peren-file-owner-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let store = BucketStore::file(&root).unwrap();
        let node = NodeId::from_uuid(uuid::Uuid::from_u128(1));
        let cell = CellId::from_bytes([7; 32]);
        let lease = store.acquire(node, cell).await.unwrap();
        drop(lease);
        let lease = store.acquire(node, cell).await.unwrap();
        lease.release().await.unwrap();
        let lease = store.acquire(node, cell).await.unwrap();
        lease.release().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn conditional_conflicts_are_distinct_from_stale_etags() {
        let conflict = object_store::Error::AlreadyExists {
            path: "cells/cell/owner.json".into(),
            source: Box::new(std::io::Error::other("409 ConditionalRequestConflict")),
        };
        let exists = object_store::Error::AlreadyExists {
            path: "cells/cell/owner.json".into(),
            source: Box::new(std::io::Error::other("object already exists")),
        };
        let stale = object_store::Error::Precondition {
            path: "cells/cell/owner.json".into(),
            source: Box::new(std::io::Error::other("412 PreconditionFailed")),
        };

        assert!(is_conditional_conflict(&conflict));
        assert!(is_cas_rejection(&conflict));
        assert!(!is_conditional_conflict(&exists));
        assert!(is_cas_rejection(&exists));
        assert!(!is_conditional_conflict(&stale));
        assert!(is_cas_rejection(&stale));
    }
}
