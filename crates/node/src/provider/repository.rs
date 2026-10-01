use std::ops::Range;

use peren_cell::{LeaseError, OwnershipLease};
use peren_fleet::{
    CreateOutcome, Ownership, OwnershipRepository, ReplaceOutcome,
    RepositoryError as OwnershipError, StoredOwnership,
};
use peren_primitives::{CellId, NodeId, OwnershipEpoch, StorageRevision};
use peren_provider_object_store::{BucketLease, BucketStore, MemoryStore, StoreLease};
use peren_replication::{
    ReplicaImage, ReplicaPayload, ReplicaRepository, RepositoryError as ReplicaError,
};

use crate::NodeRepository;

use super::ProviderError;

#[derive(Clone)]
pub enum Repository {
    Memory(MemoryStore),
    Bucket(BucketStore),
}

pub enum Lease {
    Memory(StoreLease),
    Bucket(BucketLease),
}

impl Repository {
    pub async fn read_object(&self, key: &str) -> Result<Option<Vec<u8>>, ProviderError> {
        match self {
            Self::Memory(store) => Ok(store.read(key).await),
            Self::Bucket(store) => store.read(key).await.map_err(ProviderError::Build),
        }
    }

    pub async fn read_object_range(
        &self,
        key: &str,
        range: Range<usize>,
    ) -> Result<Option<Vec<u8>>, ProviderError> {
        match self {
            Self::Memory(store) => Ok(store.read_range(key, range).await),
            Self::Bucket(store) => store
                .read_range(key, range)
                .await
                .map_err(ProviderError::Build),
        }
    }

    pub async fn write_object(&self, key: &str, bytes: Vec<u8>) -> Result<(), ProviderError> {
        match self {
            Self::Memory(store) => {
                store.write(key, bytes).await;
                Ok(())
            }
            Self::Bucket(store) => store.write(key, bytes).await.map_err(ProviderError::Build),
        }
    }

    pub async fn verify_range_read(&self, probe: &str) -> Result<(), ProviderError> {
        let key = format!("probe/{probe}-range.bin");
        self.write_object(&key, b"0123456789abcdef".to_vec())
            .await?;
        let bytes = self
            .read_object_range(&key, 4..10)
            .await?
            .ok_or(ProviderError::RangeRead)?;
        if bytes == b"456789" {
            Ok(())
        } else {
            Err(ProviderError::RangeRead)
        }
    }

    pub async fn prune_replicas(
        &self,
        dry_run: bool,
    ) -> Result<peren_provider_object_store::ReplicaPruneReport, ReplicaError> {
        match self {
            Self::Memory(_) => Ok(peren_provider_object_store::ReplicaPruneReport {
                dry_run,
                ..peren_provider_object_store::ReplicaPruneReport::default()
            }),
            Self::Bucket(store) => store.prune_replicas(dry_run).await,
        }
    }
}

impl OwnershipLease for Lease {
    fn cell(&self) -> CellId {
        match self {
            Self::Memory(lease) => lease.cell(),
            Self::Bucket(lease) => lease.cell(),
        }
    }
    fn owner(&self) -> NodeId {
        match self {
            Self::Memory(lease) => lease.owner(),
            Self::Bucket(lease) => lease.owner(),
        }
    }
    fn epoch(&self) -> OwnershipEpoch {
        match self {
            Self::Memory(lease) => lease.epoch(),
            Self::Bucket(lease) => lease.epoch(),
        }
    }
    async fn verify(&self) -> Result<(), LeaseError> {
        match self {
            Self::Memory(lease) => lease.verify().await,
            Self::Bucket(lease) => lease.verify().await,
        }
    }
    async fn release(self) -> Result<(), LeaseError> {
        match self {
            Self::Memory(lease) => lease.release().await,
            Self::Bucket(lease) => lease.release().await,
        }
    }
}

impl NodeRepository for Repository {
    type Lease = Lease;
    async fn acquire(&self, local: NodeId, cell: CellId) -> Result<Lease, OwnershipError> {
        match self {
            Self::Memory(store) => store.acquire(local, cell).await.map(Lease::Memory),
            Self::Bucket(store) => store.acquire(local, cell).await.map(Lease::Bucket),
        }
    }
}

impl OwnershipRepository for Repository {
    type Version = Version;
    async fn load(&self, cell: CellId) -> Result<Option<StoredOwnership<Version>>, OwnershipError> {
        match self {
            Self::Memory(store) => Ok(store
                .load(cell)
                .await?
                .map(|stored| stored.map_version(Version::Memory))),
            Self::Bucket(store) => Ok(store
                .load(cell)
                .await?
                .map(|stored| stored.map_version(Version::Bucket))),
        }
    }
    async fn create(&self, ownership: Ownership) -> Result<CreateOutcome<Version>, OwnershipError> {
        match self {
            Self::Memory(store) => Ok(store.create(ownership).await?.map_version(Version::Memory)),
            Self::Bucket(store) => Ok(store.create(ownership).await?.map_version(Version::Bucket)),
        }
    }
    async fn replace(
        &self,
        current: &Version,
        next: Ownership,
    ) -> Result<ReplaceOutcome<Version>, OwnershipError> {
        match (self, current) {
            (Self::Memory(store), Version::Memory(version)) => Ok(store
                .replace(version, next)
                .await?
                .map_version(Version::Memory)),
            (Self::Bucket(store), Version::Bucket(version)) => Ok(store
                .replace(version, next)
                .await?
                .map_version(Version::Bucket)),
            _ => Err(OwnershipError::Malformed),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Version {
    Memory(u64),
    Bucket(String),
}

trait MapVersion<T> {
    type Mapped<U>;
    fn map_version<U>(self, map: impl FnOnce(T) -> U) -> Self::Mapped<U>;
}

impl<T> MapVersion<T> for StoredOwnership<T> {
    type Mapped<U> = StoredOwnership<U>;
    fn map_version<U>(self, map: impl FnOnce(T) -> U) -> StoredOwnership<U> {
        StoredOwnership {
            ownership: self.ownership,
            version: map(self.version),
        }
    }
}
impl<T> MapVersion<T> for CreateOutcome<T> {
    type Mapped<U> = CreateOutcome<U>;
    fn map_version<U>(self, map: impl FnOnce(T) -> U) -> CreateOutcome<U> {
        match self {
            Self::Created(value) => CreateOutcome::Created(map(value)),
            Self::Exists => CreateOutcome::Exists,
        }
    }
}
impl<T> MapVersion<T> for ReplaceOutcome<T> {
    type Mapped<U> = ReplaceOutcome<U>;
    fn map_version<U>(self, map: impl FnOnce(T) -> U) -> ReplaceOutcome<U> {
        match self {
            Self::Replaced(value) => ReplaceOutcome::Replaced(map(value)),
            Self::Changed => ReplaceOutcome::Changed,
        }
    }
}

impl ReplicaRepository for Repository {
    async fn publish_through(
        &self,
        cell: CellId,
        epoch: OwnershipEpoch,
        revision: StorageRevision,
        payload: &ReplicaPayload,
    ) -> Result<(), ReplicaError> {
        match self {
            Self::Memory(store) => store.publish_through(cell, epoch, revision, payload).await,
            Self::Bucket(store) => store.publish_through(cell, epoch, revision, payload).await,
        }
    }
    async fn restore(&self, cell: CellId) -> Result<Option<ReplicaImage>, ReplicaError> {
        match self {
            Self::Memory(store) => store.restore(cell).await,
            Self::Bucket(store) => store.restore(cell).await,
        }
    }
    async fn checkpoint(
        &self,
        cell: CellId,
        epoch: OwnershipEpoch,
        revision: StorageRevision,
        database: &[u8],
    ) -> Result<(), ReplicaError> {
        match self {
            Self::Memory(store) => store.checkpoint(cell, epoch, revision, database).await,
            Self::Bucket(store) => store.checkpoint(cell, epoch, revision, database).await,
        }
    }
    async fn prune(
        &self,
        cell: CellId,
        retain: std::num::NonZeroUsize,
    ) -> Result<usize, ReplicaError> {
        match self {
            Self::Memory(store) => store.prune(cell, retain).await,
            Self::Bucket(store) => store.prune(cell, retain).await,
        }
    }
}
