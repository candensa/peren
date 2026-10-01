use std::{collections::BTreeSet, num::NonZeroUsize};

use futures::StreamExt;
use object_store::{PutMode, PutOptions, PutPayload, path::Path};
use peren_fleet::OwnershipRepository;
use peren_primitives::{CellId, OwnershipEpoch, StorageRevision};
use peren_replication::{
    ReplicaImage, ReplicaPayload, ReplicaRepository, RepositoryError as ReplicaError,
};

use crate::BucketStore;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct ReplicaRoot {
    pub(crate) cell: CellId,
    pub(crate) epoch: OwnershipEpoch,
    pub(crate) generation: StorageRevision,
    pub(crate) revision: StorageRevision,
    pub(crate) wal: Vec<StorageRevision>,
}

impl BucketStore {
    pub(crate) async fn put_immutable(&self, key: &Path, bytes: &[u8]) -> Result<(), ReplicaError> {
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(key, PutPayload::from(bytes.to_vec()), options)
            .await
        {
            Ok(_) => Ok(()),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => {
                let existing = self
                    .store
                    .get(key)
                    .await
                    .map_err(|_| ReplicaError::Unavailable)?
                    .bytes()
                    .await
                    .map_err(|_| ReplicaError::Unavailable)?;
                if existing.as_ref() == bytes {
                    Ok(())
                } else {
                    Err(ReplicaError::Malformed)
                }
            }
            Err(_) => {
                let existing = self
                    .store
                    .get(key)
                    .await
                    .map_err(|_| ReplicaError::Unavailable)?
                    .bytes()
                    .await
                    .map_err(|_| ReplicaError::Unavailable)?;
                if existing.as_ref() == bytes {
                    Ok(())
                } else {
                    Err(ReplicaError::Malformed)
                }
            }
        }
    }

    pub(crate) async fn read_replica(&self, key: &Path) -> Result<Vec<u8>, ReplicaError> {
        self.store
            .get(key)
            .await
            .map_err(|_| ReplicaError::Unavailable)?
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|_| ReplicaError::Unavailable)
    }

    pub(crate) async fn read_header(
        &self,
        cell: CellId,
        epoch: OwnershipEpoch,
        generation: StorageRevision,
    ) -> Result<[u8; 32], ReplicaError> {
        let generation_key = wal_generation_key(cell, epoch, generation);
        let bytes = match self.store.get(&generation_key).await {
            Ok(result) => result
                .bytes()
                .await
                .map_err(|_| ReplicaError::Unavailable)?,
            Err(object_store::Error::NotFound { .. }) => self
                .store
                .get(&wal_header_key(cell, epoch))
                .await
                .map_err(|_| ReplicaError::Unavailable)?
                .bytes()
                .await
                .map_err(|_| ReplicaError::Unavailable)?,
            Err(_) => return Err(ReplicaError::Unavailable),
        };
        bytes
            .as_ref()
            .try_into()
            .map_err(|_| ReplicaError::Malformed)
    }

    pub(crate) async fn positions(
        &self,
        cell: CellId,
        kind: ReplicaKind,
    ) -> Result<BTreeSet<(u64, u64)>, ReplicaError> {
        let prefix = kind.prefix(cell);
        let mut objects = self.store.list(Some(&prefix));
        let mut positions = BTreeSet::new();
        while let Some(item) = objects.next().await {
            let meta = item.map_err(|_| ReplicaError::Unavailable)?;
            let position = kind
                .parse(cell, meta.location.as_ref())
                .ok_or(ReplicaError::Malformed)?;
            positions.insert(position);
        }
        Ok(positions)
    }

    async fn delete_replica(&self, key: &Path) -> Result<(), ReplicaError> {
        match self.store.delete(key).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(_) => Err(ReplicaError::Unavailable),
        }
    }
}
impl BucketStore {
    pub(crate) async fn write_root(&self, root: ReplicaRoot) -> Result<(), ReplicaError> {
        let bytes = serde_json::to_vec(&root).map_err(|_| ReplicaError::Malformed)?;
        self.store
            .put(&root_key(root.cell), PutPayload::from(bytes))
            .await
            .map_err(|_| ReplicaError::Unavailable)?;
        Ok(())
    }

    pub(crate) async fn read_root(
        &self,
        cell: CellId,
    ) -> Result<Option<ReplicaRoot>, ReplicaError> {
        match self.store.get(&root_key(cell)).await {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|_| ReplicaError::Unavailable)?;
                let root: ReplicaRoot =
                    serde_json::from_slice(&bytes).map_err(|_| ReplicaError::Malformed)?;
                if root.cell == cell {
                    Ok(Some(root))
                } else {
                    Err(ReplicaError::Malformed)
                }
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(_) => Err(ReplicaError::Unavailable),
        }
    }

    async fn restore_root(&self, root: ReplicaRoot) -> Result<ReplicaImage, ReplicaError> {
        let database = self
            .read_replica(&snapshot_key(root.cell, root.epoch, root.generation))
            .await?;
        let mut wal = Vec::new();
        if !root.wal.is_empty() {
            let header = self
                .read_header(root.cell, root.epoch, root.generation)
                .await?;
            wal.extend_from_slice(&header);
            for revision in &root.wal {
                wal.extend_from_slice(
                    &self
                        .read_replica(&wal_key(root.cell, root.epoch, *revision))
                        .await?,
                );
            }
        }
        Ok(ReplicaImage {
            epoch: root.epoch,
            generation: root.generation,
            revision: root.revision,
            database,
            wal,
        })
    }
}

impl ReplicaRepository for BucketStore {
    async fn publish_through(
        &self,
        cell: CellId,
        epoch: OwnershipEpoch,
        revision: StorageRevision,
        payload: &ReplicaPayload,
    ) -> Result<(), ReplicaError> {
        let Some(owner) = self
            .load(cell)
            .await
            .map_err(|_| ReplicaError::Unavailable)?
        else {
            return Err(ReplicaError::Fenced);
        };
        if owner.ownership.owner.is_none() || owner.ownership.epoch != epoch {
            return Err(ReplicaError::Fenced);
        }
        let baseline = payload.generation;
        let database_key = snapshot_key(cell, epoch, baseline);
        let wal_key = wal_key(cell, epoch, revision);
        self.put_immutable(&database_key, &payload.database).await?;
        if payload.wal_frames.is_empty() {
            return self
                .write_root(ReplicaRoot {
                    cell,
                    epoch,
                    generation: baseline,
                    revision,
                    wal: Vec::new(),
                })
                .await;
        }
        let header = payload.wal_header.ok_or(ReplicaError::Malformed)?;
        self.store
            .put(
                &wal_header_key(cell, epoch),
                PutPayload::from(header.to_vec()),
            )
            .await
            .map_err(|_| ReplicaError::Unavailable)?;
        self.put_immutable(&wal_generation_key(cell, epoch, baseline), &header)
            .await?;
        self.put_immutable(&wal_key, &payload.wal_frames).await?;
        self.write_root(ReplicaRoot {
            cell,
            epoch,
            generation: baseline,
            revision,
            wal: vec![revision],
        })
        .await
    }

    async fn restore(&self, cell: CellId) -> Result<Option<ReplicaImage>, ReplicaError> {
        if let Some(root) = self.read_root(cell).await? {
            return self.restore_root(root).await.map(Some);
        }
        let snapshots = self.positions(cell, ReplicaKind::Snapshot).await?;
        let wals = self.positions(cell, ReplicaKind::Wal).await?;
        let target_epoch = snapshots.iter().chain(&wals).map(|(epoch, _)| *epoch).max();
        let Some(target_epoch) = target_epoch else {
            return Ok(None);
        };
        let Some(&(snapshot_epoch, snapshot_revision)) =
            snapshots.iter().rfind(|(epoch, _)| *epoch <= target_epoch)
        else {
            return Err(ReplicaError::Malformed);
        };
        let selected_wals: Vec<_> = wals
            .iter()
            .copied()
            .filter(|(epoch, revision)| {
                *epoch == target_epoch
                    && (snapshot_epoch != target_epoch || *revision > snapshot_revision)
            })
            .collect();
        if selected_wals
            .windows(2)
            .any(|pair| pair[1].1 != pair[0].1 + 1)
        {
            return Err(ReplicaError::Malformed);
        }
        let epoch = OwnershipEpoch::new(target_epoch);
        let revision = StorageRevision::new(
            selected_wals
                .last()
                .map_or(snapshot_revision, |(_, revision)| *revision),
        );
        let database = self
            .read_replica(&snapshot_key(
                cell,
                OwnershipEpoch::new(snapshot_epoch),
                StorageRevision::new(snapshot_revision),
            ))
            .await?;
        let mut wal = Vec::new();
        if !selected_wals.is_empty() {
            let generation = if snapshot_epoch == target_epoch {
                snapshot_revision
            } else {
                0
            };
            let header = self
                .read_header(cell, epoch, StorageRevision::new(generation))
                .await?;
            wal.extend_from_slice(&header);
            for (_, sequence) in selected_wals {
                wal.extend_from_slice(
                    &self
                        .read_replica(&wal_key(cell, epoch, StorageRevision::new(sequence)))
                        .await?,
                );
            }
        }
        Ok(Some(ReplicaImage {
            epoch,
            generation: StorageRevision::new(snapshot_revision),
            revision,
            database,
            wal,
        }))
    }

    async fn checkpoint(
        &self,
        cell: CellId,
        epoch: OwnershipEpoch,
        revision: StorageRevision,
        database: &[u8],
    ) -> Result<(), ReplicaError> {
        let Some(owner) = self
            .load(cell)
            .await
            .map_err(|_| ReplicaError::Unavailable)?
        else {
            return Err(ReplicaError::Fenced);
        };
        if owner.ownership.owner.is_none() || owner.ownership.epoch != epoch {
            return Err(ReplicaError::Fenced);
        }
        self.put_immutable(&snapshot_key(cell, epoch, revision), database)
            .await?;
        self.write_root(ReplicaRoot {
            cell,
            epoch,
            generation: revision,
            revision,
            wal: Vec::new(),
        })
        .await
    }

    async fn prune(&self, cell: CellId, retain: NonZeroUsize) -> Result<usize, ReplicaError> {
        let snapshots = self.positions(cell, ReplicaKind::Snapshot).await?;
        let wals = self.positions(cell, ReplicaKind::Wal).await?;
        let remove = snapshots.len().saturating_sub(retain.get());
        if remove == 0 {
            return Ok(0);
        }
        let Some(&cutoff) = snapshots.iter().nth(remove) else {
            return Ok(0);
        };
        for &(epoch, revision) in wals.iter().filter(|position| **position <= cutoff) {
            self.delete_replica(&wal_key(
                cell,
                OwnershipEpoch::new(epoch),
                StorageRevision::new(revision),
            ))
            .await?;
        }
        let retained_epochs: BTreeSet<_> = snapshots
            .iter()
            .skip(remove)
            .map(|(epoch, _)| *epoch)
            .collect();
        let removed: Vec<_> = snapshots.iter().take(remove).copied().collect();
        for (epoch, revision) in removed {
            let epoch = OwnershipEpoch::new(epoch);
            let revision = StorageRevision::new(revision);
            self.delete_replica(&wal_generation_key(cell, epoch, revision))
                .await?;
            self.delete_replica(&snapshot_key(cell, epoch, revision))
                .await?;
            if !retained_epochs.contains(&epoch.get()) {
                self.delete_replica(&wal_header_key(cell, epoch)).await?;
            }
        }
        Ok(remove)
    }
}
fn root_key(cell: CellId) -> Path {
    Path::from(format!("cells/{cell}/root.json"))
}

pub(crate) fn snapshot_key(cell: CellId, epoch: OwnershipEpoch, revision: StorageRevision) -> Path {
    Path::from(format!(
        "cells/{cell}/snapshot/e{}/{:020}.sqlite",
        epoch.get(),
        revision.get()
    ))
}

pub(crate) fn wal_key(cell: CellId, epoch: OwnershipEpoch, revision: StorageRevision) -> Path {
    Path::from(format!(
        "cells/{cell}/wal/e{}/{:020}.bin",
        epoch.get(),
        revision.get()
    ))
}

fn wal_header_key(cell: CellId, epoch: OwnershipEpoch) -> Path {
    Path::from(format!("cells/{cell}/wal-header/e{}.bin", epoch.get()))
}

pub(crate) fn wal_generation_key(
    cell: CellId,
    epoch: OwnershipEpoch,
    generation: StorageRevision,
) -> Path {
    Path::from(format!(
        "cells/{cell}/wal-header/e{}/{:020}.bin",
        epoch.get(),
        generation.get()
    ))
}

#[derive(Clone, Copy)]
pub(crate) enum ReplicaKind {
    Snapshot,
    Wal,
}

impl ReplicaKind {
    fn prefix(self, cell: CellId) -> Path {
        match self {
            Self::Snapshot => Path::from(format!("cells/{cell}/snapshot")),
            Self::Wal => Path::from(format!("cells/{cell}/wal")),
        }
    }

    fn parse(self, cell: CellId, key: &str) -> Option<(u64, u64)> {
        let (directory, extension) = match self {
            Self::Snapshot => ("snapshot", ".sqlite"),
            Self::Wal => ("wal", ".bin"),
        };
        let prefix = format!("cells/{cell}/{directory}/e");
        let remainder = key.strip_prefix(&prefix)?;
        let (epoch, filename) = remainder.split_once('/')?;
        let sequence = filename.strip_suffix(extension)?;
        if sequence.len() != 20 || !sequence.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        Some((epoch.parse().ok()?, sequence.parse().ok()?))
    }
}

#[cfg(test)]
mod behavior;
