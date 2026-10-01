use super::*;
use std::path::Path;

#[test]
fn entry_limit_counts_key_and_value() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    assert!(matches!(
        storage.put("do", b"k", &vec![0; MAX_ENTRY_BYTES]),
        Err(StorageError::EntryTooLarge { .. })
    ));
}

#[test]
fn list_uses_stable_keyset_cursors() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    for key in [b"a", b"b", b"c", b"d"] {
        storage.put("do", key, key).unwrap();
    }
    let first = storage
        .list(
            "do",
            &ListOptions {
                limit: 2,
                ..Default::default()
            },
        )
        .unwrap();
    storage.put("do", b"aa", b"aa").unwrap();
    storage.put("do", b"e", b"e").unwrap();
    let second = storage
        .list(
            "do",
            &ListOptions {
                cursor: first.next_cursor.as_deref(),
                limit: 8,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        second
            .entries
            .into_iter()
            .map(|entry| entry.0)
            .collect::<Vec<_>>(),
        [b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]
    );
}

#[test]
fn list_default_is_bounded_and_invalid_limits_are_refused() {
    assert_eq!(ListOptions::default().limit, MAX_LIST_ENTRIES);
    let storage = CellStorage::open(Path::new(":memory:")).unwrap();
    for limit in [0, MAX_LIST_ENTRIES + 1] {
        assert!(matches!(
            storage.list(
                "do",
                &ListOptions {
                    limit,
                    ..ListOptions::default()
                }
            ),
            Err(StorageError::InvalidListLimit(_))
        ));
    }
}
