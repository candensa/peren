use std::{fs, path::Path};

use peren_primitives::StorageRevision;

use crate::{CheckpointBytes, ReplicaBytes, StorageError};

pub(crate) fn replica(path: &Path, offset: u64) -> Result<ReplicaBytes, StorageError> {
    let database = fs::read(path).map_err(StorageError::ReadReplica)?;
    let wal_path = path.with_file_name(format!(
        "{}-wal",
        path.file_name()
            .and_then(|name| name.to_str())
            .ok_or(StorageError::MalformedWal)?
    ));
    let wal = match fs::read(wal_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(StorageError::ReadReplica(error)),
    };
    if wal.is_empty() {
        return Ok(ReplicaBytes {
            database,
            header: None,
            frames: Vec::new(),
            offset: 0,
        });
    }
    let header = wal
        .get(..32)
        .ok_or(StorageError::MalformedWal)?
        .try_into()
        .map_err(|_| StorageError::MalformedWal)?;
    let frame_bytes = frame_bytes(&header)?;
    if (wal.len() - 32) % frame_bytes != 0 {
        return Err(StorageError::MalformedWal);
    }
    let start = if offset == 0 {
        32
    } else {
        usize::try_from(offset).map_err(|_| StorageError::MalformedWal)?
    };
    if start < 32 || (start - 32) % frame_bytes != 0 {
        return Err(StorageError::MalformedWal);
    }
    let frames = wal.get(start..).ok_or(StorageError::MalformedWal)?.to_vec();
    Ok(ReplicaBytes {
        database,
        header: Some(header),
        frames,
        offset: u64::try_from(wal.len()).map_err(|_| StorageError::MalformedWal)?,
    })
}

pub(crate) fn checkpoint(
    path: &Path,
    revision: StorageRevision,
) -> Result<CheckpointBytes, StorageError> {
    Ok(CheckpointBytes {
        database: fs::read(path).map_err(StorageError::ReadReplica)?,
        revision,
    })
}

fn frame_bytes(header: &[u8; 32]) -> Result<usize, StorageError> {
    let magic = u32::from_be_bytes(header[..4].try_into().expect("fixed header slice"));
    if !matches!(magic, 0x37_7f_06_82 | 0x37_7f_06_83) {
        return Err(StorageError::MalformedWal);
    }
    let encoded = u32::from_be_bytes(header[8..12].try_into().expect("fixed header slice"));
    let page_bytes = if encoded == 1 {
        65_536
    } else {
        usize::try_from(encoded).map_err(|_| StorageError::MalformedWal)?
    };
    if !(512..=65_536).contains(&page_bytes) || !page_bytes.is_power_of_two() {
        return Err(StorageError::MalformedWal);
    }
    page_bytes.checked_add(24).ok_or(StorageError::MalformedWal)
}
