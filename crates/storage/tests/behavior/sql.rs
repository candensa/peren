use super::*;
use std::path::Path;
use std::process::Command;

#[test]
fn sql_preserves_types_and_enforces_foreign_keys() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    storage
        .sql(
            "CREATE TABLE parent(id INTEGER PRIMARY KEY, label TEXT, data BLOB)",
            &[],
        )
        .unwrap();
    storage
        .sql(
            "CREATE TABLE child(parent_id INTEGER REFERENCES parent(id))",
            &[],
        )
        .unwrap();
    storage
        .sql(
            "INSERT INTO parent(id,label,data) VALUES(?1,?2,?3)",
            &[
                SqlValue::Integer(7),
                SqlValue::Text("seven".to_owned()),
                SqlValue::Blob(vec![0, 1, 2]),
            ],
        )
        .unwrap();
    let selected = storage
        .sql("SELECT id,label,data,NULL FROM parent", &[])
        .unwrap();
    assert_eq!(selected.revision.get(), 3);
    assert_eq!(
        selected.value.rows,
        [vec![
            SqlValue::Integer(7),
            SqlValue::Text("seven".to_owned()),
            SqlValue::Blob(vec![0, 1, 2]),
            SqlValue::Null,
        ]]
    );
    assert!(
        storage
            .sql(
                "INSERT INTO child(parent_id) VALUES(?1)",
                &[SqlValue::Integer(8)],
            )
            .is_err()
    );
    assert_eq!(storage.revision().get(), 3);
}

#[test]
fn sql_rejects_invalid_text_without_panicking() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    assert!(storage.sql("SELECT CAST(x'80' AS TEXT)", &[]).is_err());
    assert_eq!(storage.revision().get(), 0);
}

#[test]
fn sql_limits_are_applied_to_every_connection() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    let oversized = format!("SELECT 1 --{}", "x".repeat(MAX_SQL_BYTES));
    assert!(storage.sql(&oversized, &[]).is_err());
    assert!(storage.sql("SELECT ?101", &[]).is_err());
    assert_eq!(storage.revision().get(), 0);
}

#[test]
fn open_transaction_is_rolled_back_after_process_exit() {
    let (_dir, path) = database();
    drop(CellStorage::open(&path).unwrap());
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["tests::crash_writer", "--exact"])
        .env("PEREN_STORAGE_CRASH_PATH", &path)
        .status()
        .unwrap();
    assert!(status.success());

    let storage = CellStorage::open(&path).unwrap();
    assert_eq!(storage.get("do", b"uncommitted").unwrap(), None);
    assert_eq!(storage.revision().get(), 0);
}

#[test]
fn crash_writer() {
    let Some(path) = std::env::var_os("PEREN_STORAGE_CRASH_PATH") else {
        return;
    };
    let mut storage = CellStorage::open(Path::new(&path)).unwrap();
    let _: Result<Committed<()>, StorageError> = storage.transaction(|transaction| {
        transaction.put("do", b"uncommitted", b"value")?;
        std::process::exit(0);
    });
}

#[test]
fn migration_batch_applies_multiple_statements_once() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();

    let revision = storage
        .apply("CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT); INSERT INTO records(name) VALUES ('alpha');")
        .unwrap();

    assert_eq!(revision.get(), 1);
    let rows = storage
        .sql("SELECT name FROM records", &[])
        .unwrap()
        .value
        .rows;
    assert_eq!(rows, vec![vec![SqlValue::Text("alpha".into())]]);
}
