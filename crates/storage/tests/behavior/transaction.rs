use super::*;
use std::path::Path;

#[test]
fn batch_commit_advances_one_revision() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    storage
        .put_many(
            "do",
            &[
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
            ],
        )
        .unwrap();
    assert_eq!(storage.revision().get(), 1);
    assert_eq!(
        storage
            .get_many("do", &[b"b".to_vec(), b"missing".to_vec(), b"a".to_vec()])
            .unwrap(),
        [
            (b"b".to_vec(), b"2".to_vec()),
            (b"a".to_vec(), b"1".to_vec())
        ]
    );
}

#[test]
fn transaction_rolls_back_every_write_on_error() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    let result: Result<Committed<()>, StorageError> = storage.transaction(|transaction| {
        transaction.put("do", b"key", b"value")?;
        transaction.sql("not valid SQL", &[])?;
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(storage.revision().get(), 0);
    assert_eq!(storage.get("do", b"key").unwrap(), None);
}

#[test]
fn competing_writer_fails_without_partial_state() {
    let (_dir, path) = database();
    let mut first = CellStorage::open(&path).unwrap();
    let mut second = CellStorage::open(&path).unwrap();
    first
        .transaction(|transaction| {
            transaction.put("do", b"winner", b"value")?;
            let error = second.put("do", b"loser", b"value").unwrap_err();
            assert!(matches!(
                error,
                StorageError::Sqlite(rusqlite::Error::SqliteFailure(code, _))
                    if code.code == rusqlite::ErrorCode::DatabaseBusy
                        || code.code == rusqlite::ErrorCode::DatabaseLocked
            ));
            Ok(())
        })
        .unwrap();
    assert_eq!(first.get("do", b"winner").unwrap(), Some(b"value".to_vec()));
    assert_eq!(first.get("do", b"loser").unwrap(), None);
    assert_eq!(first.revision().get(), 1);
    assert_eq!(second.put("do", b"next", b"value").unwrap().get(), 2);
}
