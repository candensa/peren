use super::*;
use fs2::FileExt;
use peren_bindings::SharedStorageHost;
use peren_runtime::{
    HttpRequest, InvocationLimits, IsolateLimits, Module, ModuleKind, ModuleName, WorkerBundle,
    WorkerEnvironment, WorkerRuntime,
};
use peren_storage::{
    AlarmStore, AttachmentStore, CellStorage, CellStore, ListOptions, ListStore, MutationStore,
    SqlStore, SqlValue,
};
use std::{collections::BTreeMap, fmt::Debug, fs::OpenOptions, path::Path, sync::Arc};
use tokio::sync::Mutex;
use uuid::Uuid;

async fn increment(store: &mut TursoStore) -> StorageRevision {
    store.begin().await.unwrap();
    let value = store
        .load("do", b"counter")
        .await
        .unwrap()
        .map_or(0_u64, |bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
        + 1;
    store
        .put("do", b"counter", &value.to_be_bytes())
        .await
        .unwrap();
    store.commit().await.unwrap().revision
}

async fn read_counter(store: &mut TursoStore) -> u64 {
    store
        .load("do", b"counter")
        .await
        .unwrap()
        .map_or(0, |bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
}

fn bundle() -> WorkerBundle {
    let entry = ModuleName::parse("main.js").unwrap();
    WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([(
            entry,
            Module::new(
                ModuleKind::JavaScript,
                b"export default { async fetch(request) {
                      if (new URL(request.url).pathname === '/increment') {
                        await Peren.storage.transaction(async (storage) => {
                          const current = await storage.get('counter');
                          const next = current === undefined ? 1 : current[0] + 1;
                          await storage.put('counter', new Uint8Array([next]));
                        });
                      }
                      const value = await Peren.storage.get('counter');
                      return new Response(String(value?.[0] ?? 0));
                    } };"
                    .as_slice(),
            )
            .unwrap(),
        )]),
    )
    .unwrap()
}

fn d1_bundle() -> WorkerBundle {
    let entry = ModuleName::parse("main.js").unwrap();
    WorkerBundle::new(
            entry.clone(),
            BTreeMap::from([(
                entry,
                Module::new(
                    ModuleKind::JavaScript,
                    b"export default { async fetch(_request, env) {
                      await env.DB.prepare('CREATE TABLE records(id INTEGER PRIMARY KEY, label TEXT)').run();
                      await env.DB.prepare('INSERT INTO records(label) VALUES (?)').bind('turso').run();
                      const row = await env.DB.prepare('SELECT id,label FROM records WHERE label=?').bind('turso').first();
                      return new Response(`${row.id}:${row.label}`);
                    } };"
                        .as_slice(),
                )
                .unwrap(),
            )]),
        )
        .unwrap()
}

fn request(path: &str) -> HttpRequest {
    HttpRequest {
        method: "GET".into(),
        url: format!("https://worker.invalid{path}"),
        headers: Vec::new(),
        body: Vec::new(),
        mtls: None,
    }
}

async fn storage_contract<S>(store: &mut S)
where
    S: CellStore + ListStore + SqlStore + AlarmStore + AttachmentStore,
    S::Error: Debug,
{
    assert_eq!(store.load("scope", b"key").await.unwrap(), None);
    assert!(store.put("scope", b"key", b"outside").await.is_err());

    store.begin().await.unwrap();
    store.put("scope", b"key", b"value").await.unwrap();
    assert_eq!(
        store.load("scope", b"key").await.unwrap(),
        Some(b"value".to_vec())
    );
    assert!(!store.delete("scope", b"missing").await.unwrap());
    assert_eq!(store.commit().await.unwrap().revision.get(), 1);

    store.begin().await.unwrap();
    assert!(store.begin().await.is_err());
    store.put("scope", b"rolled-back", b"value").await.unwrap();
    store.rollback().await.unwrap();
    assert_eq!(store.load("scope", b"rolled-back").await.unwrap(), None);

    store.begin().await.unwrap();
    assert_eq!(store.commit().await.unwrap().revision.get(), 1);
    assert!(store.commit().await.is_err());

    store.begin().await.unwrap();
    store.put("scope", b"a", b"1").await.unwrap();
    store.put("scope", b"b", b"2").await.unwrap();
    store.set_alarm("scope", 42).await.unwrap();
    store.set_attachment("socket", b"attachment").await.unwrap();
    assert_eq!(store.commit().await.unwrap().revision.get(), 2);
    assert_eq!(store.alarm("scope").await.unwrap().unwrap().at_ms, 42);
    assert_eq!(
        store.attachment("socket").await.unwrap(),
        Some(b"attachment".to_vec())
    );
    let page = store
        .list(
            "scope",
            &ListOptions {
                limit: 1,
                ..ListOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page.entries, [(b"a".to_vec(), b"1".to_vec())]);
    assert_eq!(page.next_cursor, Some(b"a".to_vec()));

    store.begin().await.unwrap();
    assert!(store.delete_alarm("scope").await.unwrap());
    assert!(store.delete_attachment("socket").await.unwrap());
    assert_eq!(store.commit().await.unwrap().revision.get(), 3);
    assert_eq!(store.alarm("scope").await.unwrap(), None);
    assert_eq!(store.attachment("socket").await.unwrap(), None);

    sql_contract(store).await;
}

async fn sql_contract<S>(store: &mut S)
where
    S: SqlStore,
    S::Error: Debug,
{
    store.begin().await.unwrap();
    store
        .sql(
            "CREATE TABLE conformance(id INTEGER PRIMARY KEY, text_value TEXT, blob_value BLOB)",
            &[],
        )
        .await
        .unwrap();
    store
        .sql(
            "INSERT INTO conformance VALUES(?1,?2,?3)",
            &[
                SqlValue::Integer(7),
                SqlValue::Text("seven".to_owned()),
                SqlValue::Blob(vec![0, 1, 2]),
            ],
        )
        .await
        .unwrap();
    let selected = store
        .sql("SELECT id,text_value,blob_value FROM conformance", &[])
        .await
        .unwrap();
    assert_eq!(
        selected.rows,
        [vec![
            SqlValue::Integer(7),
            SqlValue::Text("seven".to_owned()),
            SqlValue::Blob(vec![0, 1, 2]),
        ]]
    );
    assert_eq!(store.commit().await.unwrap().revision.get(), 4);

    store.begin().await.unwrap();
    store.sql("SELECT 1", &[]).await.unwrap();
    assert_eq!(store.commit().await.unwrap().revision.get(), 4);
    store.begin().await.unwrap();
    assert!(store.sql("BEGIN", &[]).await.is_err());
    store.rollback().await.unwrap();

    store.begin().await.unwrap();
    assert!(
        store
            .sql(&" ".repeat(peren_storage::MAX_SQL_BYTES + 1), &[])
            .await
            .is_err()
    );
    assert!(
        store
            .sql(
                "SELECT 1",
                &vec![SqlValue::Null; peren_storage::MAX_SQL_PARAMETERS + 1],
            )
            .await
            .is_err()
    );
    let wide = format!(
        "SELECT {}",
        std::iter::repeat_n("1", peren_storage::MAX_SQL_COLUMNS + 1)
            .collect::<Vec<_>>()
            .join(",")
    );
    assert!(store.sql(&wide, &[]).await.is_err());
    store.rollback().await.unwrap();
}

#[tokio::test]
async fn local_engine_records_mutation_outcomes() {
    let path = std::env::temp_dir().join(format!("{}.sqlite", Uuid::new_v4()));
    let id = Uuid::new_v4();
    let mut store = TursoStore::local(&path).await.unwrap();

    store.begin().await.unwrap();
    store.put("scope", b"key", b"value").await.unwrap();
    store
        .record_mutation_outcome(id, br#"{"ok":true}"#)
        .await
        .unwrap();
    assert!(matches!(
        store.record_mutation_outcome(id, b"duplicate").await,
        Err(TursoError::DuplicateMutation(duplicate)) if duplicate == id
    ));
    assert_eq!(store.commit().await.unwrap().revision.get(), 1);

    let outcome = store.mutation_outcome(id).await.unwrap().unwrap();
    assert_eq!(outcome.id, id);
    assert_eq!(outcome.outcome, br#"{"ok":true}"#);
    assert_eq!(outcome.revision, StorageRevision::new(0));

    drop(store);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn sqlite_and_turso_share_transaction_semantics() {
    let mut sqlite = CellStorage::open(Path::new(":memory:")).unwrap();
    storage_contract(&mut sqlite).await;

    let path = std::env::temp_dir().join(format!("{}.sqlite", Uuid::new_v4()));
    let mut turso = TursoStore::local(&path).await.unwrap();
    storage_contract(&mut turso).await;
    drop(turso);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn local_engine_satisfies_the_worker_storage_contract() {
    let path = std::env::temp_dir().join(format!("{}.sqlite", Uuid::new_v4()));
    let store = Arc::new(Mutex::new(TursoStore::local(&path).await.unwrap()));
    let host = Arc::new(SharedStorageHost::new(Arc::clone(&store)));
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(),
        IsolateLimits::new(128 * 1024 * 1024, std::time::Duration::from_secs(5)),
        host,
    )
    .await
    .unwrap();

    let written = runtime
        .dispatch_http(request("/increment"), InvocationLimits::new(1024, 10))
        .await
        .unwrap();
    assert_eq!(runtime.committed_revision(), StorageRevision::new(1));
    let read = runtime
        .dispatch_http(request("/read"), InvocationLimits::new(1024, 10))
        .await
        .unwrap();

    assert_eq!(written.body, b"1");
    assert_eq!(read.body, b"1");
    drop(runtime);
    drop(store);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn local_engine_satisfies_the_worker_d1_contract() {
    let path = std::env::temp_dir().join(format!("{}.sqlite", Uuid::new_v4()));
    let store = Arc::new(Mutex::new(TursoStore::local(&path).await.unwrap()));
    let host = Arc::new(SharedStorageHost::new(Arc::clone(&store)));
    let environment = WorkerEnvironment::new(BTreeMap::from([(
        "__perenBindings".to_string(),
        r#"{"DB":{"type":"d1"}}"#.to_string(),
    )]));
    let mut runtime = WorkerRuntime::load_with_environment(
        d1_bundle(),
        IsolateLimits::new(128 * 1024 * 1024, std::time::Duration::from_secs(5)),
        environment,
        host,
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(request("/d1"), InvocationLimits::new(1024, 10))
        .await
        .unwrap();

    assert_eq!(response.body, b"1:turso");
    assert_eq!(runtime.committed_revision(), StorageRevision::new(2));
    drop(runtime);
    drop(store);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn local_revision_survives_reopen() {
    let path = std::env::temp_dir().join(format!("{}.sqlite", Uuid::new_v4()));
    let mut store = TursoStore::local(&path).await.unwrap();
    assert_eq!(increment(&mut store).await.get(), 1);
    drop(store);

    let mut store = TursoStore::local(&path).await.unwrap();
    assert_eq!(increment(&mut store).await.get(), 2);
    drop(store);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn sqlite_turso_round_trip_preserves_platform_and_user_schema() {
    let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
    std::fs::create_dir_all(&root).unwrap();
    let sqlite = root.join("sqlite.db");
    let turso = root.join("turso.db");
    let restored = root.join("restored.db");
    let source = rusqlite::Connection::open(&sqlite).unwrap();
    source
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 CREATE TABLE kv(scope TEXT NOT NULL, k BLOB NOT NULL, v BLOB NOT NULL, PRIMARY KEY(scope, k)) WITHOUT ROWID;
                 CREATE TABLE invoices(id TEXT PRIMARY KEY, amount INTEGER NOT NULL) WITHOUT ROWID;",
            )
            .unwrap();
    source
        .execute(
            "INSERT INTO kv VALUES ('do', ?1, ?2)",
            rusqlite::params![b"counter".as_slice(), 41_u64.to_be_bytes().as_slice()],
        )
        .unwrap();
    source
        .execute("INSERT INTO invoices VALUES ('invoice-1', 725)", [])
        .unwrap();

    let imported = migrate(&sqlite, &turso).unwrap();
    assert_eq!(
        migrate(&sqlite, &turso).unwrap().schema(),
        imported.schema()
    );
    let mut store = TursoStore::local(&turso).await.unwrap();
    assert_eq!(
        store.load("do", b"counter").await.unwrap(),
        Some(41_u64.to_be_bytes().to_vec())
    );
    drop(store);
    drop(source);

    migrate(&turso, &restored).unwrap();
    let restored = rusqlite::Connection::open(restored).unwrap();
    let amount: i64 = restored
        .query_row(
            "SELECT amount FROM invoices WHERE id = 'invoice-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(amount, 725);
    let counter: Vec<u8> = restored
        .query_row("SELECT v FROM kv WHERE scope='do'", [], |row| row.get(0))
        .unwrap();
    assert_eq!(counter, 41_u64.to_be_bytes());
    let revision: i64 = restored
        .query_row(
            "SELECT revision FROM storage_metadata WHERE id=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(revision, 0);
    drop(restored);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn migration_never_replaces_a_conflicting_destination() {
    let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
    std::fs::create_dir_all(&root).unwrap();
    let source = root.join("source.sqlite");
    let destination = root.join("destination.sqlite");
    let source_db = rusqlite::Connection::open(&source).unwrap();
    source_db
        .execute_batch(
            "CREATE TABLE source_data(value INTEGER); INSERT INTO source_data VALUES (1);",
        )
        .unwrap();
    let destination_db = rusqlite::Connection::open(&destination).unwrap();
    destination_db
            .execute_batch("CREATE TABLE destination_data(value INTEGER); INSERT INTO destination_data VALUES (2);")
            .unwrap();
    drop(source_db);
    drop(destination_db);
    let before = std::fs::read(&destination).unwrap();

    assert!(matches!(
        migrate(&source, &destination),
        Err(MigrationError::DestinationExists)
    ));
    assert_eq!(std::fs::read(&destination).unwrap(), before);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn concurrent_migration_is_refused_without_touching_the_destination() {
    let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
    std::fs::create_dir_all(&root).unwrap();
    let source = root.join("source.sqlite");
    let destination = root.join("destination.sqlite");
    rusqlite::Connection::open(&source)
        .unwrap()
        .execute_batch("CREATE TABLE values_(value INTEGER);")
        .unwrap();
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(destination.with_extension("migration.lock"))
        .unwrap();
    lock.try_lock_exclusive().unwrap();

    assert!(matches!(
        migrate(&source, &destination),
        Err(MigrationError::InProgress)
    ));
    assert!(!destination.exists());
    drop(lock);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn remote_preflight_rejects_unsupported_without_rowid_schema() {
    let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
    std::fs::create_dir_all(&root).unwrap();
    let database = root.join("database.sqlite");
    rusqlite::Connection::open(&database)
        .unwrap()
        .execute_batch("CREATE TABLE unsupported(id INTEGER PRIMARY KEY) WITHOUT ROWID;")
        .unwrap();

    assert!(matches!(
        validate_remote(&database),
        Err(MigrationError::UnsupportedRemoteSchema { object }) if object == "unsupported"
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "requires PEREN_TURSO_URL and PEREN_TURSO_TOKEN for a qualification database"]
async fn remote_commit_is_visible_to_a_new_session() {
    let url = std::env::var("PEREN_TURSO_URL").unwrap();
    let token = std::env::var("PEREN_TURSO_TOKEN").unwrap();
    let mut writer = TursoStore::remote(url.clone(), token.clone())
        .await
        .unwrap();
    let before = read_counter(&mut writer).await;
    let committed = increment(&mut writer).await;
    assert_eq!(
        read_counter(&mut writer).await,
        before.checked_add(1).unwrap()
    );

    let mut reader = TursoStore::remote(url, token).await.unwrap();
    assert_eq!(
        read_counter(&mut reader).await,
        before.checked_add(1).unwrap()
    );
    assert!(committed.get() > 0);
}
