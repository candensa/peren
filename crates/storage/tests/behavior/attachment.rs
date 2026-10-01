use super::*;
use std::{fs, path::Path};

#[test]
fn attachment_lifecycle_is_durable() {
    let (_dir, path) = database();
    let mut storage = CellStorage::open(&path).unwrap();
    storage.set_attachment("socket", b"state").unwrap();
    drop(storage);
    let mut storage = CellStorage::open(&path).unwrap();
    assert_eq!(
        storage.attachment("socket").unwrap(),
        Some(b"state".to_vec())
    );
    storage.delete_attachment("socket").unwrap();
    assert_eq!(storage.attachment("socket").unwrap(), None);
}

#[test]
fn purge_removes_all_cell_state_and_advances_revision() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    storage.put("do", b"key", b"value").unwrap();
    storage.set_alarm("do", 10).unwrap();
    storage.set_attachment("socket", b"state").unwrap();
    let before = storage.revision();

    let purged = storage.purge().unwrap();

    assert!(purged > before);
    assert_eq!(storage.get("do", b"key").unwrap(), None);
    assert_eq!(storage.alarm("do").unwrap(), None);
    assert_eq!(storage.attachment("socket").unwrap(), None);
}

#[test]
fn attachment_limit_is_enforced_before_mutation() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    assert!(matches!(
        storage.set_attachment("socket", &vec![0; MAX_ATTACHMENT_BYTES + 1]),
        Err(StorageError::AttachmentTooLarge { .. })
    ));
    assert_eq!(storage.revision().get(), 0);
}

#[test]
fn read_only_storage_refuses_writes() {
    let (_dir, path) = database();
    drop(CellStorage::open(&path).unwrap());
    let mut storage = CellStorage::open_read_only(&path).unwrap();
    assert!(matches!(
        storage.put("do", b"key", b"value"),
        Err(StorageError::Sqlite(_))
    ));
}

#[test]
fn opens_released_snapshot() {
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/current/persistence");
    let (dir, path) = database();
    fs::copy(fixture.join("restore.sqlite"), &path).unwrap();
    fs::copy(
        fixture.join("restore.sqlite-wal"),
        dir.path().join("cell.sqlite-wal"),
    )
    .unwrap();
    let storage = CellStorage::open(&path).unwrap();
    assert_eq!(storage.get("do", b"counter").unwrap(), Some(b"41".to_vec()));
    assert_eq!(
        storage.alarm("do").unwrap().unwrap().at_ms,
        1_700_000_000_000
    );
    assert_eq!(
        storage.attachment("connection-1").unwrap(),
        Some(br#"{"tag":"fixture"}"#.to_vec())
    );
}
