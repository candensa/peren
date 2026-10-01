use super::*;
use std::path::Path;

#[test]
fn committed_result_mints_minimum_read_receipt() {
    let cell = peren_primitives::CellId::from_bytes([42; 32]);
    let other = peren_primitives::CellId::from_bytes([43; 32]);
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();

    let committed = storage
        .transaction(|transaction| {
            transaction.put("do", b"key", b"value")?;
            Ok("written")
        })
        .unwrap();
    let receipt = committed.receipt(cell);

    assert_eq!(committed.value, "written");
    assert!(storage.satisfies(receipt, cell));
    assert!(!storage.satisfies(receipt, other));

    let stale =
        peren_primitives::DurabilityReceipt::new(cell, peren_primitives::StorageRevision::new(2));
    assert!(!storage.satisfies(stale, cell));
}

#[test]
fn mutation_outcome_commits_with_state_in_one_revision() {
    let id = uuid::Uuid::new_v4();
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();

    let committed = storage
        .transaction_with_outcome(
            id,
            |transaction| {
                transaction.put("do", b"key", b"value")?;
                Ok("created")
            },
            |outcome| outcome.as_bytes().to_vec(),
        )
        .unwrap();
    let record = storage.mutation_outcome(id).unwrap().unwrap();

    assert_eq!(committed.value, "created");
    assert_eq!(committed.revision.get(), 1);
    assert_eq!(record.revision, committed.revision);
    assert_eq!(record.outcome, b"created");
    assert_eq!(storage.get("do", b"key").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn mutation_outcome_is_not_recorded_when_transaction_rolls_back() {
    let id = uuid::Uuid::new_v4();
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();

    let result: Result<Committed<()>, StorageError> = storage.transaction_with_outcome(
        id,
        |transaction| {
            transaction.put("do", b"key", b"value")?;
            transaction.sql("not valid SQL", &[])?;
            Ok(())
        },
        |()| b"should-not-exist".to_vec(),
    );

    assert!(matches!(result, Err(StorageError::Sql { .. })));
    assert_eq!(storage.mutation_outcome(id).unwrap(), None);
    assert_eq!(storage.get("do", b"key").unwrap(), None);
    assert_eq!(storage.revision().get(), 0);
}

#[test]
fn duplicate_mutation_id_is_refused_without_overwriting_outcome() {
    let id = uuid::Uuid::new_v4();
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    storage
        .transaction_with_outcome(
            id,
            |transaction| {
                transaction.put("do", b"first", b"1")?;
                Ok("first")
            },
            |outcome| outcome.as_bytes().to_vec(),
        )
        .unwrap();

    let result = storage.transaction_with_outcome(
        id,
        |transaction| {
            transaction.put("do", b"second", b"2")?;
            Ok("second")
        },
        |outcome| outcome.as_bytes().to_vec(),
    );
    let record = storage.mutation_outcome(id).unwrap().unwrap();

    assert!(matches!(result, Err(StorageError::DuplicateMutation(duplicate)) if duplicate == id));
    assert_eq!(record.outcome, b"first");
    assert_eq!(storage.get("do", b"second").unwrap(), None);
    assert_eq!(storage.revision().get(), 1);
}
