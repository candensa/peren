use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
};

use futures::StreamExt;
use object_store::{PutMode, PutOptions, PutPayload, UpdateVersion, path::Path};
use peren_fleet::OwnershipRepository;
use peren_primitives::{CellId, OwnershipEpoch, StorageRevision};
use peren_replication::{
    ReplicaImage, ReplicaPayload, ReplicaRepository, RepositoryError as ReplicaError,
};

use crate::BucketStore;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReplicaPruneReport {
    pub dry_run: bool,
    pub cells_scanned: usize,
    pub objects_retained: usize,
    pub objects_removed: usize,
    pub bytes_removed: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaDeleteReport {
    pub dry_run: bool,
    pub cell: CellId,
    pub objects_removed: usize,
    pub bytes_removed: u64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct ReplicaRoot {
    pub(crate) cell: CellId,
    pub(crate) epoch: OwnershipEpoch,
    pub(crate) generation: StorageRevision,
    pub(crate) revision: StorageRevision,
    pub(crate) wal: Vec<StorageRevision>,
}

impl BucketStore {
    async fn verify_replica_owner(
        &self,
        cell: CellId,
        epoch: OwnershipEpoch,
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
        Ok(())
    }

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
        let key = root_key(root.cell);
        let bytes = serde_json::to_vec(&root).map_err(|_| ReplicaError::Malformed)?;
        for _ in 0..3 {
            let current = self.read_root_version(root.cell).await?;
            let Some(mode) = root_put_mode(current.as_ref(), &root)? else {
                return Ok(());
            };
            let options = PutOptions {
                mode,
                ..PutOptions::default()
            };
            match self
                .store
                .put_opts(&key, PutPayload::from(bytes.clone()), options)
                .await
            {
                Ok(_) => return Ok(()),
                Err(
                    object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. },
                ) => {}
                Err(_) => return Err(ReplicaError::Unavailable),
            }
        }
        Err(ReplicaError::Unavailable)
    }

    pub(crate) async fn read_root(
        &self,
        cell: CellId,
    ) -> Result<Option<ReplicaRoot>, ReplicaError> {
        Ok(self.read_root_version(cell).await?.map(|(root, _)| root))
    }

    async fn read_root_version(
        &self,
        cell: CellId,
    ) -> Result<Option<(ReplicaRoot, UpdateVersion)>, ReplicaError> {
        match self.store.get(&root_key(cell)).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|_| ReplicaError::Unavailable)?;
                let root: ReplicaRoot =
                    serde_json::from_slice(&bytes).map_err(|_| ReplicaError::Malformed)?;
                if root.cell == cell {
                    Ok(Some((root, version)))
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

    pub async fn prune_replicas(&self, dry_run: bool) -> Result<ReplicaPruneReport, ReplicaError> {
        let mut report = ReplicaPruneReport {
            dry_run,
            ..ReplicaPruneReport::default()
        };
        for cell in self.root_cells().await? {
            let cell_report = self.prune_cell_replicas(cell, dry_run).await?;
            report.cells_scanned += cell_report.cells_scanned;
            report.objects_retained += cell_report.objects_retained;
            report.objects_removed += cell_report.objects_removed;
            report.bytes_removed = report
                .bytes_removed
                .saturating_add(cell_report.bytes_removed);
        }
        Ok(report)
    }

    pub async fn delete_cell_replicas(
        &self,
        cell: CellId,
        dry_run: bool,
    ) -> Result<ReplicaDeleteReport, ReplicaError> {
        let mut candidates = self.replica_objects(cell).await?;
        let root = root_key(cell);
        if let Some(size) = self.object_size(&root).await? {
            candidates.insert(root, size);
        }
        let mut report = ReplicaDeleteReport {
            dry_run,
            cell,
            objects_removed: 0,
            bytes_removed: 0,
        };
        for (key, size) in candidates {
            report.objects_removed += 1;
            report.bytes_removed = report.bytes_removed.saturating_add(size);
            if !dry_run {
                self.delete_replica(&key).await?;
            }
        }
        Ok(report)
    }

    async fn prune_cell_replicas(
        &self,
        cell: CellId,
        dry_run: bool,
    ) -> Result<ReplicaPruneReport, ReplicaError> {
        let Some(root) = self.read_root(cell).await? else {
            return Ok(ReplicaPruneReport {
                dry_run,
                ..ReplicaPruneReport::default()
            });
        };
        let reachable = root_reachable(&root);
        let candidates = self.replica_objects(cell).await?;
        let mut report = ReplicaPruneReport {
            dry_run,
            cells_scanned: 1,
            ..ReplicaPruneReport::default()
        };
        for (key, size) in candidates {
            if reachable.contains(&key) || replica_object_is_newer_than_root(cell, &key, &root) {
                report.objects_retained += 1;
                continue;
            }
            if !dry_run {
                let Some(current_root) = self.read_root(cell).await? else {
                    report.objects_retained += 1;
                    continue;
                };
                if root_reachable(&current_root).contains(&key)
                    || replica_object_is_newer_than_root(cell, &key, &current_root)
                {
                    report.objects_retained += 1;
                    continue;
                }
            }
            report.objects_removed += 1;
            report.bytes_removed = report.bytes_removed.saturating_add(size);
            if !dry_run {
                self.delete_replica(&key).await?;
            }
        }
        Ok(report)
    }

    async fn root_cells(&self) -> Result<BTreeSet<CellId>, ReplicaError> {
        let mut objects = self.store.list(Some(&Path::from("cells")));
        let mut cells = BTreeSet::new();
        while let Some(item) = objects.next().await {
            let meta = item.map_err(|_| ReplicaError::Unavailable)?;
            if let Some(cell) = parse_root_cell(meta.location.as_ref()) {
                cells.insert(cell);
            }
        }
        Ok(cells)
    }

    async fn replica_objects(&self, cell: CellId) -> Result<BTreeMap<Path, u64>, ReplicaError> {
        let mut objects = BTreeMap::new();
        for prefix in [
            Path::from(format!("cells/{cell}/snapshot")),
            Path::from(format!("cells/{cell}/wal")),
            Path::from(format!("cells/{cell}/wal-header")),
        ] {
            let mut listed = self.store.list(Some(&prefix));
            while let Some(item) = listed.next().await {
                let meta = item.map_err(|_| ReplicaError::Unavailable)?;
                objects.insert(meta.location, u64::try_from(meta.size).unwrap_or(u64::MAX));
            }
        }
        Ok(objects)
    }

    async fn object_size(&self, key: &Path) -> Result<Option<u64>, ReplicaError> {
        match self.store.head(key).await {
            Ok(meta) => Ok(Some(u64::try_from(meta.size).unwrap_or(u64::MAX))),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(_) => Err(ReplicaError::Unavailable),
        }
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
        self.verify_replica_owner(cell, epoch).await?;
        let baseline = payload.generation;
        let database_key = snapshot_key(cell, epoch, baseline);
        let wal_key = wal_key(cell, epoch, revision);
        self.put_immutable(&database_key, &payload.database).await?;
        if payload.wal_frames.is_empty() {
            self.verify_replica_owner(cell, epoch).await?;
            if self.root_is_ahead(cell, epoch, revision).await? {
                return Ok(());
            }
            if self.root_covers(cell, epoch, baseline, revision).await? {
                return Ok(());
            }
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
        self.verify_replica_owner(cell, epoch).await?;
        if self.root_is_ahead(cell, epoch, revision).await? {
            return Ok(());
        }
        if self.root_covers(cell, epoch, baseline, revision).await? {
            return Ok(());
        }
        let mut wal = self
            .read_root(cell)
            .await?
            .filter(|root| {
                root.epoch == epoch
                    && root.generation == baseline
                    && root.revision < revision
                    && !root.wal.contains(&revision)
            })
            .map_or_else(Vec::new, |root| root.wal);
        wal.push(revision);
        self.write_root(ReplicaRoot {
            cell,
            epoch,
            generation: baseline,
            revision,
            wal,
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
        self.verify_replica_owner(cell, epoch).await?;
        self.put_immutable(&snapshot_key(cell, epoch, revision), database)
            .await?;
        self.verify_replica_owner(cell, epoch).await?;
        if self.root_is_ahead(cell, epoch, revision).await? {
            return Ok(());
        }
        if self.root_covers(cell, epoch, revision, revision).await? {
            return Ok(());
        }
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

impl BucketStore {
    async fn root_is_ahead(
        &self,
        cell: CellId,
        epoch: OwnershipEpoch,
        revision: StorageRevision,
    ) -> Result<bool, ReplicaError> {
        Ok(self
            .read_root(cell)
            .await?
            .is_some_and(|root| root.epoch == epoch && root.revision > revision))
    }

    async fn root_covers(
        &self,
        cell: CellId,
        epoch: OwnershipEpoch,
        generation: StorageRevision,
        revision: StorageRevision,
    ) -> Result<bool, ReplicaError> {
        Ok(self.read_root(cell).await?.is_some_and(|root| {
            root.epoch == epoch && root.generation == generation && root.revision >= revision
        }))
    }
}
fn root_key(cell: CellId) -> Path {
    Path::from(format!("cells/{cell}/root.json"))
}

fn root_put_mode(
    current: Option<&(ReplicaRoot, UpdateVersion)>,
    next: &ReplicaRoot,
) -> Result<Option<PutMode>, ReplicaError> {
    let Some((root, version)) = current else {
        return Ok(Some(PutMode::Create));
    };
    if root.cell != next.cell {
        return Err(ReplicaError::Malformed);
    }
    if root.epoch > next.epoch {
        return Ok(None);
    }
    if root.epoch == next.epoch && root.revision > next.revision {
        return Ok(None);
    }
    if root.epoch == next.epoch
        && root.generation == next.generation
        && root.revision >= next.revision
    {
        return Ok(None);
    }
    Ok(Some(PutMode::Update(version.clone())))
}

fn root_reachable(root: &ReplicaRoot) -> BTreeSet<Path> {
    let mut reachable = BTreeSet::from([
        snapshot_key(root.cell, root.epoch, root.generation),
        wal_generation_key(root.cell, root.epoch, root.generation),
    ]);
    if !root.wal.is_empty() {
        reachable.insert(wal_header_key(root.cell, root.epoch));
    }
    for revision in &root.wal {
        reachable.insert(wal_key(root.cell, root.epoch, *revision));
    }
    reachable
}

fn replica_object_is_newer_than_root(cell: CellId, key: &Path, root: &ReplicaRoot) -> bool {
    let Some(object) = ReplicaObject::parse(cell, key.as_ref()) else {
        return true;
    };
    match object {
        ReplicaObject::Snapshot { epoch, revision }
        | ReplicaObject::Wal { epoch, revision }
        | ReplicaObject::WalGeneration { epoch, revision } => {
            epoch > root.epoch || (epoch == root.epoch && revision > root.revision)
        }
        ReplicaObject::WalHeader { epoch } => epoch >= root.epoch,
    }
}

fn parse_root_cell(key: &str) -> Option<CellId> {
    let value = key.strip_prefix("cells/")?.strip_suffix("/root.json")?;
    if value.len() != 64 {
        return None;
    }
    let mut bytes = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_value(chunk[0])?;
        let low = hex_value(chunk[1])?;
        bytes[index] = (high << 4) | low;
    }
    Some(CellId::from_bytes(bytes))
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

enum ReplicaObject {
    Snapshot {
        epoch: OwnershipEpoch,
        revision: StorageRevision,
    },
    Wal {
        epoch: OwnershipEpoch,
        revision: StorageRevision,
    },
    WalHeader {
        epoch: OwnershipEpoch,
    },
    WalGeneration {
        epoch: OwnershipEpoch,
        revision: StorageRevision,
    },
}

impl ReplicaObject {
    fn parse(cell: CellId, key: &str) -> Option<Self> {
        if let Some((epoch, revision)) = parse_revision_key(cell, "snapshot", ".sqlite", key) {
            return Some(Self::Snapshot { epoch, revision });
        }
        if let Some((epoch, revision)) = parse_revision_key(cell, "wal", ".bin", key) {
            return Some(Self::Wal { epoch, revision });
        }
        if let Some((epoch, revision)) = parse_revision_key(cell, "wal-header", ".bin", key) {
            return Some(Self::WalGeneration { epoch, revision });
        }
        let prefix = format!("cells/{cell}/wal-header/e");
        let epoch = key.strip_prefix(&prefix)?.strip_suffix(".bin")?;
        if epoch.contains('/') || !epoch.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        Some(Self::WalHeader {
            epoch: OwnershipEpoch::new(epoch.parse().ok()?),
        })
    }
}

fn parse_revision_key(
    cell: CellId,
    directory: &str,
    extension: &str,
    key: &str,
) -> Option<(OwnershipEpoch, StorageRevision)> {
    let prefix = format!("cells/{cell}/{directory}/e");
    let remainder = key.strip_prefix(&prefix)?;
    let (epoch, filename) = remainder.split_once('/')?;
    let sequence = filename.strip_suffix(extension)?;
    if sequence.len() != 20 || !sequence.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some((
        OwnershipEpoch::new(epoch.parse().ok()?),
        StorageRevision::new(sequence.parse().ok()?),
    ))
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
