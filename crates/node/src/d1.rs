use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use peren_bindings::d1_cell_id;
use peren_config::{Binding, D1Backend, ValidatedConfig};
use peren_primitives::ServiceName;
use peren_provider_turso::{TursoError, TursoStore};
use peren_storage::{CellStorage, CellStore, Committed, SqlResult, SqlStore, SqlValue};
use serde_json::json;
use thiserror::Error;

use crate::Environment;

pub struct Query {
    pub service: String,
    pub binding: String,
    pub sql: String,
}

pub struct Migration {
    pub service: String,
    pub binding: String,
    pub dir: PathBuf,
}

pub struct Prune {
    pub service: String,
    pub binding: String,
    pub dry: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct MigrationReport {
    pub files: Vec<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PruneReport {
    pub pruned: usize,
    pub dry: bool,
}

pub enum QueryOutput {
    Rows(Vec<String>),
    Metadata(String),
}

pub fn query(
    config: &ValidatedConfig,
    environment: &impl Environment,
    command: &Query,
) -> Result<QueryOutput, QueryError> {
    let binding = binding(config, &command.service, &command.binding)?;
    let Binding::D1Database {
        database_name,
        unique_key,
        backend,
    } = binding
    else {
        return Err(QueryError::BindingKind(command.binding.clone()));
    };
    let result = match backend {
        D1Backend::NativeSqlite => {
            let mut storage = open_storage(environment, unique_key, database_name)?;
            storage.sql(&command.sql, &[])?
        }
        D1Backend::Turso { .. } => turso_runtime()?.block_on(async {
            let mut storage = open_turso(environment, backend).await?;
            storage.begin().await?;
            let value = storage.sql(&command.sql, &[]).await?;
            let commit = storage.commit().await?;
            Ok::<Committed<SqlResult>, QueryError>(Committed {
                value,
                revision: commit.revision,
            })
        })?,
        D1Backend::External { .. } => return Err(QueryError::Backend(command.binding.clone())),
    };
    output(result)
}

pub fn prune(
    config: &ValidatedConfig,
    _environment: &impl Environment,
    command: &Prune,
) -> Result<PruneReport, QueryError> {
    let binding = binding(config, &command.service, &command.binding)?;
    let Binding::D1Database { backend, .. } = binding else {
        return Err(QueryError::BindingKind(command.binding.clone()));
    };
    match backend {
        D1Backend::NativeSqlite | D1Backend::Turso { .. } => Ok(PruneReport {
            pruned: 0,
            dry: command.dry,
        }),
        D1Backend::External { .. } => Err(QueryError::Backend(command.binding.clone())),
    }
}

pub fn migrate(
    config: &ValidatedConfig,
    environment: &impl Environment,
    command: &Migration,
) -> Result<MigrationReport, QueryError> {
    let binding = binding(config, &command.service, &command.binding)?;
    let Binding::D1Database {
        database_name,
        unique_key,
        backend,
    } = binding
    else {
        return Err(QueryError::BindingKind(command.binding.clone()));
    };
    let mut files = sql_files(&command.dir)?;
    let mut applied = Vec::with_capacity(files.len());
    match backend {
        D1Backend::NativeSqlite => {
            let mut storage = open_storage(environment, unique_key, database_name)?;
            for file in files.drain(..) {
                let source = migration_source(&file)?;
                storage.apply(&source)?;
                applied.push(migration_name(&file)?);
            }
        }
        D1Backend::Turso { .. } => {
            turso_runtime()?.block_on(async {
                let mut storage = open_turso(environment, backend).await?;
                for file in files.drain(..) {
                    let source = migration_source(&file)?;
                    storage.apply(&source).await?;
                    applied.push(migration_name(&file)?);
                }
                Ok::<(), QueryError>(())
            })?;
        }
        D1Backend::External { .. } => return Err(QueryError::Backend(command.binding.clone())),
    }
    Ok(MigrationReport { files: applied })
}

fn sql_files(dir: &PathBuf) -> Result<Vec<PathBuf>, QueryError> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).map_err(|source| QueryError::ReadDirectory {
        path: dir.clone(),
        source,
    })? {
        let path = entry
            .map_err(|source| QueryError::ReadDirectory {
                path: dir.clone(),
                source,
            })?
            .path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("sql") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn migration_source(file: &Path) -> Result<String, QueryError> {
    fs::read_to_string(file).map_err(|source| QueryError::Read {
        path: file.to_path_buf(),
        source,
    })
}

fn migration_name(file: &Path) -> Result<String, QueryError> {
    file.file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| QueryError::Path(file.to_path_buf()))
        .map(str::to_string)
}

async fn open_turso(
    environment: &impl Environment,
    backend: &D1Backend,
) -> Result<TursoStore, QueryError> {
    let D1Backend::Turso {
        url_env,
        token_env,
        replica_path,
    } = backend
    else {
        unreachable!("Turso opener is only called for Turso D1 backends");
    };
    let url = environment
        .get(url_env)
        .ok_or_else(|| QueryError::Environment(url_env.clone()))?;
    let token = environment
        .get(token_env)
        .ok_or_else(|| QueryError::Environment(token_env.clone()))?;
    match replica_path {
        Some(path) => TursoStore::replica(Path::new(path), url, token)
            .await
            .map_err(Into::into),
        None => TursoStore::remote(url, token).await.map_err(Into::into),
    }
}

fn turso_runtime() -> Result<tokio::runtime::Runtime, QueryError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(QueryError::Runtime)
}

fn open_storage(
    environment: &impl Environment,
    unique_key: &str,
    database_name: &str,
) -> Result<CellStorage, QueryError> {
    let data = environment
        .get("PEREN_DATA_DIR")
        .map_or_else(default_data, PathBuf::from)
        .join("cells");
    fs::create_dir_all(&data).map_err(|source| QueryError::CreateData {
        path: data.clone(),
        source,
    })?;
    let cell = d1_cell_id(unique_key, database_name);
    CellStorage::open(&data.join(format!("{cell}.sqlite"))).map_err(Into::into)
}

fn output(result: peren_storage::Committed<SqlResult>) -> Result<QueryOutput, QueryError> {
    if result.value.columns.is_empty() {
        return Ok(QueryOutput::Metadata(
            json!({
                "changes": result.value.changes,
                "last_insert_rowid": result.value.last_insert_rowid,
                "revision": result.revision.get(),
            })
            .to_string(),
        ));
    }
    let lines = result
        .value
        .rows
        .into_iter()
        .map(|row| row_json(&result.value.columns, row).map(|value| value.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryOutput::Rows(lines))
}

fn row_json(columns: &[String], row: Vec<SqlValue>) -> Result<serde_json::Value, QueryError> {
    if columns.len() != row.len() {
        return Err(QueryError::Shape);
    }
    let values = columns
        .iter()
        .cloned()
        .zip(row.into_iter().map(value_json))
        .collect::<BTreeMap<_, _>>();
    Ok(json!(values))
}

fn value_json(value: SqlValue) -> serde_json::Value {
    match value {
        SqlValue::Null => serde_json::Value::Null,
        SqlValue::Integer(value) => json!(value),
        SqlValue::Real(value) => json!(value),
        SqlValue::Text(value) => json!(value),
        SqlValue::Blob(value) => json!({ "__base64__": STANDARD.encode(value) }),
    }
}

fn binding<'a>(
    config: &'a ValidatedConfig,
    service: &str,
    name: &str,
) -> Result<&'a Binding, QueryError> {
    let service = ServiceName::parse(service.to_string())
        .map_err(|_| QueryError::Service(service.to_string()))?;
    let index = config
        .services
        .get(&service)
        .ok_or_else(|| QueryError::Service(service.as_str().to_string()))?;
    config.raw.services[*index]
        .bindings
        .get(name)
        .ok_or_else(|| QueryError::Binding(name.to_string()))
}

fn default_data() -> PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join("data")
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("service {0:?} is not configured")]
    Service(String),
    #[error("binding {0:?} is not configured")]
    Binding(String),
    #[error("binding {0:?} is not a D1 binding")]
    BindingKind(String),
    #[error("binding {0:?} does not use an operator-supported D1 backend")]
    Backend(String),
    #[error("environment variable {0:?} is required for the D1 backend")]
    Environment(String),
    #[error("failed to read D1 migration directory {path:?}")]
    ReadDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read D1 migration file {path:?}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("D1 migration path {0:?} is not valid UTF-8")]
    Path(PathBuf),
    #[error("failed to create local cell data directory {path:?}")]
    CreateData {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create Turso runtime")]
    Runtime(#[source] std::io::Error),
    #[error("SQL result row shape does not match its columns")]
    Shape,
    #[error(transparent)]
    Storage(#[from] peren_storage::StorageError),
    #[error(transparent)]
    Turso(#[from] TursoError),
}

#[cfg(test)]
mod tests {
    use std::fs;

    use peren_config::FleetConfig;
    use uuid::Uuid;

    use super::*;

    struct TestEnvironment {
        data: PathBuf,
    }

    impl Environment for TestEnvironment {
        fn get(&self, name: &str) -> Option<String> {
            (name == "PEREN_DATA_DIR").then(|| self.data.display().to_string())
        }
    }

    fn config(backend: &str) -> ValidatedConfig {
        FleetConfig::from_toml(&format!(
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
worker_bundle_path = "worker.js"
compatibility_date = "2026-01-01"
[services.bindings.DB]
type = "d1_database"
database_name = "main"
unique_key = "db-key"
{backend}
[[sockets]]
name = "public"
listen = "127.0.0.1:8080"
service = "api"
"#
        ))
        .unwrap()
        .validate()
        .unwrap()
    }

    #[test]
    fn native_query_prints_mutation_metadata_and_row_json() {
        let root = std::env::temp_dir().join(format!("peren-d1-{}", Uuid::new_v4()));
        let environment = TestEnvironment {
            data: root.join("data"),
        };
        let config = config("");

        let create = query(
            &config,
            &environment,
            &Query {
                service: "api".into(),
                binding: "DB".into(),
                sql: "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT, bytes BLOB)".into(),
            },
        )
        .unwrap();
        assert!(matches!(create, QueryOutput::Metadata(_)));
        query(
            &config,
            &environment,
            &Query {
                service: "api".into(),
                binding: "DB".into(),
                sql: "INSERT INTO records(name, bytes) VALUES ('alpha', x'000102')".into(),
            },
        )
        .unwrap();

        let QueryOutput::Rows(rows) = query(
            &config,
            &environment,
            &Query {
                service: "api".into(),
                binding: "DB".into(),
                sql: "SELECT id, name, bytes FROM records".into(),
            },
        )
        .unwrap() else {
            panic!("expected rows");
        };

        assert_eq!(
            rows,
            [r#"{"bytes":{"__base64__":"AAEC"},"id":1,"name":"alpha"}"#]
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn applies_sql_migration_files_in_name_order() {
        let root = std::env::temp_dir().join(format!("peren-d1-{}", Uuid::new_v4()));
        let dir = root.join("migrations");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("002_insert.sql"),
            "INSERT INTO records(name) VALUES ('alpha');",
        )
        .unwrap();
        fs::write(
            dir.join("001_schema.sql"),
            "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT);",
        )
        .unwrap();
        let environment = TestEnvironment {
            data: root.join("data"),
        };
        let config = config("");

        let report = migrate(
            &config,
            &environment,
            &Migration {
                service: "api".into(),
                binding: "DB".into(),
                dir,
            },
        )
        .unwrap();

        assert_eq!(report.files, ["001_schema.sql", "002_insert.sql"]);
        let QueryOutput::Rows(rows) = query(
            &config,
            &environment,
            &Query {
                service: "api".into(),
                binding: "DB".into(),
                sql: "SELECT name FROM records".into(),
            },
        )
        .unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows, [r#"{"name":"alpha"}"#]);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn prune_history_validates_the_native_binding_and_reports_current_model() {
        let root = std::env::temp_dir().join(format!("peren-d1-{}", Uuid::new_v4()));
        let environment = TestEnvironment {
            data: root.join("data"),
        };

        let report = prune(
            &config(""),
            &environment,
            &Prune {
                service: "api".into(),
                binding: "DB".into(),
                dry: true,
            },
        )
        .unwrap();

        assert_eq!(
            report,
            PruneReport {
                pruned: 0,
                dry: true
            }
        );
    }

    #[test]
    fn refuses_external_d1_backend() {
        let root = std::env::temp_dir().join(format!("peren-d1-{}", Uuid::new_v4()));
        let environment = TestEnvironment {
            data: root.join("data"),
        };

        let result = query(
            &config(
                "backend = { kind = \"external\", url_env = \"DATABASE_URL\", driver = \"postgres\" }",
            ),
            &environment,
            &Query {
                service: "api".into(),
                binding: "DB".into(),
                sql: "SELECT 1".into(),
            },
        );

        assert!(matches!(result, Err(QueryError::Backend(binding)) if binding == "DB"));
    }
}
