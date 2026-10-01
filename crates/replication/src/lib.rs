use std::num::NonZeroUsize;

use peren_primitives::{CellId, OwnershipEpoch, StorageRevision};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableReceipt {
    cell: CellId,
    epoch: OwnershipEpoch,
    generation: StorageRevision,
    through: StorageRevision,
}

impl DurableReceipt {
    #[must_use]
    pub const fn new(
        cell: CellId,
        epoch: OwnershipEpoch,
        generation: StorageRevision,
        through: StorageRevision,
    ) -> Self {
        Self {
            cell,
            epoch,
            generation,
            through,
        }
    }

    #[must_use]
    pub const fn cell(&self) -> CellId {
        self.cell
    }

    #[must_use]
    pub const fn epoch(&self) -> OwnershipEpoch {
        self.epoch
    }

    #[must_use]
    pub const fn generation(&self) -> StorageRevision {
        self.generation
    }

    #[must_use]
    pub const fn through(&self) -> StorageRevision {
        self.through
    }
}

pub trait ReplicaRepository: Send + Sync {
    fn publish_through(
        &self,
        cell: CellId,
        epoch: OwnershipEpoch,
        revision: StorageRevision,
        payload: &ReplicaPayload,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send;

    fn restore(
        &self,
        cell: CellId,
    ) -> impl Future<Output = Result<Option<ReplicaImage>, RepositoryError>> + Send;

    fn checkpoint(
        &self,
        cell: CellId,
        epoch: OwnershipEpoch,
        revision: StorageRevision,
        database: &[u8],
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send;

    fn prune(
        &self,
        cell: CellId,
        retain: NonZeroUsize,
    ) -> impl Future<Output = Result<usize, RepositoryError>> + Send;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaImage {
    pub epoch: OwnershipEpoch,
    pub generation: StorageRevision,
    pub revision: StorageRevision,
    pub database: Vec<u8>,
    pub wal: Vec<u8>,
}

pub struct ReplicaPayload {
    pub generation: StorageRevision,
    pub database: Vec<u8>,
    pub wal_header: Option<[u8; 32]>,
    pub wal_frames: Vec<u8>,
}

pub struct Replicator<R> {
    repository: R,
}

impl<R: ReplicaRepository> Replicator<R> {
    #[must_use]
    pub const fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn publish(
        &self,
        cell: CellId,
        epoch: OwnershipEpoch,
        revision: StorageRevision,
        payload: &ReplicaPayload,
    ) -> Result<DurableReceipt, RepositoryError> {
        self.repository
            .publish_through(cell, epoch, revision, payload)
            .await?;
        Ok(DurableReceipt::new(
            cell,
            epoch,
            payload.generation,
            revision,
        ))
    }

    pub async fn restore(&self, cell: CellId) -> Result<Option<ReplicaImage>, RepositoryError> {
        self.repository.restore(cell).await
    }

    pub async fn restore_satisfying(
        &self,
        receipt: DurableReceipt,
    ) -> Result<Option<ReplicaImage>, RepositoryError> {
        let Some(image) = self.repository.restore(receipt.cell()).await? else {
            return Ok(None);
        };
        if image.epoch != receipt.epoch()
            || image.generation != receipt.generation()
            || image.revision < receipt.through()
        {
            return Ok(None);
        }
        Ok(Some(image))
    }

    pub async fn checkpoint(
        &self,
        cell: CellId,
        epoch: OwnershipEpoch,
        revision: StorageRevision,
        database: &[u8],
    ) -> Result<(), RepositoryError> {
        self.repository
            .checkpoint(cell, epoch, revision, database)
            .await
    }

    pub async fn prune(
        &self,
        cell: CellId,
        retain: NonZeroUsize,
    ) -> Result<usize, RepositoryError> {
        self.repository.prune(cell, retain).await
    }
}

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("replica repository is unavailable")]
    Unavailable,
    #[error("replica publication was rejected by a newer ownership epoch")]
    Fenced,
    #[error("replica data is malformed")]
    Malformed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use peren_primitives::CellId;

    #[derive(Clone)]
    struct Repository {
        image: Option<ReplicaImage>,
    }

    impl ReplicaRepository for Repository {
        async fn publish_through(
            &self,
            _cell: CellId,
            _epoch: OwnershipEpoch,
            _revision: StorageRevision,
            _payload: &ReplicaPayload,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        async fn restore(&self, _cell: CellId) -> Result<Option<ReplicaImage>, RepositoryError> {
            Ok(self.image.as_ref().map(|image| ReplicaImage {
                epoch: image.epoch,
                generation: image.generation,
                revision: image.revision,
                database: image.database.clone(),
                wal: image.wal.clone(),
            }))
        }

        async fn checkpoint(
            &self,
            _cell: CellId,
            _epoch: OwnershipEpoch,
            _revision: StorageRevision,
            _database: &[u8],
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        async fn prune(
            &self,
            _cell: CellId,
            _retain: NonZeroUsize,
        ) -> Result<usize, RepositoryError> {
            Ok(0)
        }
    }

    fn cell() -> CellId {
        CellId::from_bytes([7; 32])
    }

    fn image(epoch: u64, generation: u64, revision: u64) -> ReplicaImage {
        ReplicaImage {
            epoch: OwnershipEpoch::new(epoch),
            generation: StorageRevision::new(generation),
            revision: StorageRevision::new(revision),
            database: b"db".to_vec(),
            wal: Vec::new(),
        }
    }

    #[tokio::test]
    async fn restore_satisfying_returns_replica_that_covers_receipt() {
        let receipt = DurableReceipt::new(
            cell(),
            OwnershipEpoch::new(3),
            StorageRevision::new(7),
            StorageRevision::new(9),
        );
        let replicator = Replicator::new(Repository {
            image: Some(image(3, 7, 10)),
        });

        let restored = replicator
            .restore_satisfying(receipt)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(restored.epoch, OwnershipEpoch::new(3));
        assert_eq!(restored.generation, StorageRevision::new(7));
        assert_eq!(restored.revision, StorageRevision::new(10));
    }

    #[tokio::test]
    async fn restore_satisfying_refuses_stale_or_different_generation() {
        let receipt = DurableReceipt::new(
            cell(),
            OwnershipEpoch::new(3),
            StorageRevision::new(7),
            StorageRevision::new(9),
        );
        for image in [image(2, 7, 10), image(3, 6, 10), image(3, 7, 8)] {
            let replicator = Replicator::new(Repository { image: Some(image) });
            assert!(
                replicator
                    .restore_satisfying(receipt)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }
}
