use super::*;
use std::fs;

#[test]
fn revision_survives_reopen() {
    let (_dir, path) = database();
    let mut storage = CellStorage::open(&path).unwrap();
    assert_eq!(storage.put("do", b"key", b"value").unwrap().get(), 1);
    drop(storage);
    let storage = CellStorage::open(&path).unwrap();
    assert_eq!(storage.revision().get(), 1);
    assert_eq!(storage.get("do", b"key").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn replica_reads_only_frames_after_confirmed_offset() {
    let (_dir, path) = database();
    let mut storage = CellStorage::open(&path).unwrap();
    storage.put("do", b"first", b"1").unwrap();
    let first = storage.replica(0).unwrap();
    assert!(first.header.is_some());
    assert!(!first.frames.is_empty());

    storage.put("do", b"second", b"2").unwrap();
    let second = storage.replica(first.offset).unwrap();
    assert_eq!(second.header, first.header);
    assert!(!second.frames.is_empty());
    assert!(second.offset > first.offset);
}

#[test]
fn replica_refuses_offsets_inside_a_wal_frame() {
    let (_dir, path) = database();
    let mut storage = CellStorage::open(&path).unwrap();
    storage.put("do", b"key", b"value").unwrap();
    let replica = storage.replica(0).unwrap();

    assert!(matches!(
        storage.replica(replica.offset - 1),
        Err(StorageError::MalformedWal)
    ));
    assert!(matches!(
        storage.replica(replica.offset + 1),
        Err(StorageError::MalformedWal)
    ));
}

#[test]
fn replica_bytes_restore_an_executable_database() {
    let (dir, path) = database();
    let mut storage = CellStorage::open(&path).unwrap();
    storage.put("do", b"first", b"1").unwrap();
    storage.put("do", b"second", b"2").unwrap();
    let replica = storage.replica(0).unwrap();
    let restored = dir.path().join("restored.sqlite");
    fs::write(&restored, replica.database).unwrap();
    let mut wal = replica.header.unwrap().to_vec();
    wal.extend_from_slice(&replica.frames);
    fs::write(dir.path().join("restored.sqlite-wal"), wal).unwrap();

    let restored = CellStorage::open_read_only(&restored).unwrap();
    assert_eq!(restored.get("do", b"first").unwrap(), Some(b"1".to_vec()));
    assert_eq!(restored.get("do", b"second").unwrap(), Some(b"2".to_vec()));
    assert_eq!(restored.revision().get(), 2);
}

#[test]
fn checkpoint_returns_a_self_contained_database_and_resets_wal() {
    let (_dir, path) = database();
    let mut storage = CellStorage::open(&path).unwrap();
    storage.put("do", b"key", b"value").unwrap();
    let checkpoint = storage.checkpoint().unwrap();
    assert_eq!(checkpoint.revision.get(), 1);
    assert!(storage.replica(0).unwrap().frames.is_empty());

    let restored = path.with_file_name("checkpoint.sqlite");
    fs::write(&restored, checkpoint.database).unwrap();
    let restored = CellStorage::open_read_only(&restored).unwrap();
    assert_eq!(restored.get("do", b"key").unwrap(), Some(b"value".to_vec()));
    assert_eq!(restored.revision().get(), 1);
}
