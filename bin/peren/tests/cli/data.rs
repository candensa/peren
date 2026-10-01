use crate::support::*;
use std::fs;

#[test]
fn d1_restore_fails_with_specific_time_travel_reason() {
    let output = peren(&[
        "d1",
        "restore",
        "fleet.toml",
        "--service",
        "api",
        "--binding",
        "DB",
        "--into",
        "copy",
        "--bookmark",
        "abc",
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("d1 restore requires D1 time-travel snapshots and bookmarks")
    );
}

#[test]
fn d1_prune_history_reports_current_native_model() {
    let directory = temp("peren-d1-prune-cli");
    fs::create_dir_all(&directory).unwrap();
    let worker = directory.join("worker.js");
    fs::write(
        &worker,
        "export default { async fetch() { return new Response('ok'); } };",
    )
    .unwrap();
    let config = directory.join("fleet.toml");
    fs::write(
        &config,
        format!(
            r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:7000"
listen = "127.0.0.1:7000"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[services.bindings.DB]
type = "d1_database"
database_name = "main"
unique_key = "db-key"
[[sockets]]
name = "public"
listen = "127.0.0.1:8080"
service = "api"
"#,
            worker.display()
        ),
    )
    .unwrap();

    let output = command(&[
        "d1",
        "prune-history",
        config.to_str().unwrap(),
        "--service",
        "api",
        "--binding",
        "DB",
        "--dry-run",
    ])
    .env("PEREN_DATA_DIR", directory.join("data"))
    .output()
    .expect("peren process starts");

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "would prune 0 histories\n"
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn d1_migrate_applies_sql_files_then_query_reads_rows() {
    let directory = temp("peren-d1-migrate-cli");
    fs::create_dir_all(&directory).unwrap();
    let worker = directory.join("worker.js");
    fs::write(
        &worker,
        "export default { async fetch() { return new Response('ok'); } };",
    )
    .unwrap();
    let config = directory.join("fleet.toml");
    fs::write(
        &config,
        format!(
            r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:7000"
listen = "127.0.0.1:7000"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[services.bindings.DB]
type = "d1_database"
database_name = "main"
unique_key = "db-key"
[[sockets]]
name = "public"
listen = "127.0.0.1:8080"
service = "api"
"#,
            worker.display()
        ),
    )
    .unwrap();
    let migrations = directory.join("migrations");
    fs::create_dir_all(&migrations).unwrap();
    fs::write(
        migrations.join("002_insert.sql"),
        "INSERT INTO records(name) VALUES ('alpha');",
    )
    .unwrap();
    fs::write(
        migrations.join("001_schema.sql"),
        "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT);",
    )
    .unwrap();
    let data = directory.join("data");

    let migrate = command(&[
        "d1",
        "migrate",
        config.to_str().unwrap(),
        "--service",
        "api",
        "--binding",
        "DB",
        "--dir",
        migrations.to_str().unwrap(),
    ])
    .env("PEREN_DATA_DIR", &data)
    .output()
    .expect("peren process starts");
    assert!(migrate.status.success());
    assert_eq!(
        String::from_utf8(migrate.stdout).unwrap(),
        "applied 2 migrations\n"
    );

    let select = command(&[
        "d1",
        "query",
        config.to_str().unwrap(),
        "--service",
        "api",
        "--binding",
        "DB",
        "--sql",
        "SELECT name FROM records",
    ])
    .env("PEREN_DATA_DIR", data)
    .output()
    .expect("peren process starts");
    assert!(select.status.success());
    assert_eq!(
        String::from_utf8(select.stdout).unwrap(),
        "{\"name\":\"alpha\"}\n"
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn d1_query_prints_metadata_and_json_lines() {
    let directory = temp("peren-d1-cli");
    fs::create_dir_all(&directory).unwrap();
    let worker = directory.join("worker.js");
    fs::write(
        &worker,
        "export default { async fetch() { return new Response('ok'); } };",
    )
    .unwrap();
    let config = directory.join("fleet.toml");
    fs::write(
        &config,
        format!(
            r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:7000"
listen = "127.0.0.1:7000"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[services.bindings.DB]
type = "d1_database"
database_name = "main"
unique_key = "db-key"
[[sockets]]
name = "public"
listen = "127.0.0.1:8080"
service = "api"
"#,
            worker.display()
        ),
    )
    .unwrap();
    let data = directory.join("data");

    let create = command(&[
        "d1",
        "query",
        config.to_str().unwrap(),
        "--service",
        "api",
        "--binding",
        "DB",
        "--sql",
        "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT, bytes BLOB)",
    ])
    .env("PEREN_DATA_DIR", &data)
    .output()
    .expect("peren process starts");
    assert!(create.status.success());
    let metadata: serde_json::Value = serde_json::from_slice(&create.stdout).unwrap();
    assert_eq!(metadata["changes"], 1);

    let insert = command(&[
        "d1",
        "query",
        config.to_str().unwrap(),
        "--service",
        "api",
        "--binding",
        "DB",
        "--sql",
        "INSERT INTO records(name, bytes) VALUES ('alpha', x'000102')",
    ])
    .env("PEREN_DATA_DIR", &data)
    .output()
    .expect("peren process starts");
    assert!(insert.status.success());

    let select = command(&[
        "d1",
        "query",
        config.to_str().unwrap(),
        "--service",
        "api",
        "--binding",
        "DB",
        "--sql",
        "SELECT id, name, bytes FROM records",
    ])
    .env("PEREN_DATA_DIR", data)
    .output()
    .expect("peren process starts");
    assert!(select.status.success());
    assert_eq!(
        String::from_utf8(select.stdout).unwrap(),
        "{\"bytes\":{\"__base64__\":\"AAEC\"},\"id\":1,\"name\":\"alpha\"}\n"
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn queue_depth_reports_durable_depth() {
    let config = config();
    let output = peren(&[
        "queue",
        "depth",
        config.to_str().unwrap(),
        "--queue",
        "jobs",
    ]);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "queue jobs ready=0 delayed=0 leased=0 paused=false\n"
    );
    assert!(output.stderr.is_empty());
    fs::remove_dir_all(config.parent().unwrap()).unwrap();
}

#[test]
fn kv_bulk_import_reports_imported_records() {
    let directory = temp("peren-kv-cli");
    fs::create_dir_all(&directory).unwrap();
    let worker = directory.join("worker.js");
    fs::write(
        &worker,
        "export default { async fetch() { return new Response('ok'); } };",
    )
    .unwrap();
    let config = directory.join("fleet.toml");
    fs::write(
        &config,
        format!(
            r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:7000"
listen = "127.0.0.1:7000"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[services.bindings.CACHE]
type = "kv"
namespace = "cache"
unique_key = "app-key"
[[sockets]]
name = "public"
listen = "127.0.0.1:8080"
service = "api"
"#,
            worker.display()
        ),
    )
    .unwrap();
    let records = directory.join("records.ndjson");
    fs::write(
        &records,
        "{\"key\":\"alpha\",\"value\":\"one\"}\n{\"key\":\"beta\",\"value\":\"two\"}\n",
    )
    .unwrap();

    let output = command(&[
        "kv",
        "bulk-import",
        config.to_str().unwrap(),
        "--service",
        "api",
        "--binding",
        "CACHE",
        "--file",
        records.to_str().unwrap(),
    ])
    .env("PEREN_DATA_DIR", directory.join("data"))
    .output()
    .expect("peren process starts");

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "imported 2 entries\n"
    );
    assert!(output.stderr.is_empty());
    fs::remove_dir_all(directory).unwrap();
}
