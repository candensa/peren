use std::{fs, path::PathBuf, sync::Arc};

use peren_bindings::kv_cell_id;
use peren_config::{Binding, KvBackend, ValidatedConfig};
use peren_primitives::ServiceName;
use peren_provider_object_store::{R2Store, S3Credentials, S3Options};
use peren_runtime::{HostError, KvHost, KvPut};
use peren_storage::CellStorage;
use serde::Deserialize;
use thiserror::Error;

use crate::{Environment, host::KvStore};

type Records = Vec<(Vec<u8>, Vec<u8>)>;

pub struct Import {
    pub service: String,
    pub binding: String,
    pub file: PathBuf,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ImportReport {
    pub entries: usize,
}

pub fn import(
    config: &ValidatedConfig,
    environment: &impl Environment,
    command: &Import,
) -> Result<ImportReport, ImportError> {
    let binding = binding(config, &command.service, &command.binding)?;
    let Binding::Kv {
        namespace,
        unique_key,
        backend,
    } = binding
    else {
        return Err(ImportError::BindingKind(command.binding.clone()));
    };
    let source = fs::read_to_string(&command.file).map_err(|source| ImportError::Read {
        path: command.file.clone(),
        source,
    })?;
    let entries = parse(&source)?;
    match backend {
        KvBackend::Native => import_native(environment, unique_key, namespace, &entries)?,
        KvBackend::Redis { .. } | KvBackend::Bucket { .. } => {
            let store = kv_store(environment, backend)?;
            kv_runtime()?.block_on(async {
                for (key, value) in &entries {
                    store
                        .put(KvPut {
                            namespace: namespace.clone(),
                            key: String::from_utf8(key.clone()).map_err(ImportError::Utf8)?,
                            value: value.clone(),
                        })
                        .await?;
                }
                Ok::<(), ImportError>(())
            })?;
        }
    }
    Ok(ImportReport {
        entries: entries.len(),
    })
}

fn binding<'a>(
    config: &'a ValidatedConfig,
    service: &str,
    name: &str,
) -> Result<&'a Binding, ImportError> {
    let service = ServiceName::parse(service.to_string())
        .map_err(|_| ImportError::Service(service.to_string()))?;
    let index = config
        .services
        .get(&service)
        .ok_or_else(|| ImportError::Service(service.as_str().to_string()))?;
    config.raw.services[*index]
        .bindings
        .get(name)
        .ok_or_else(|| ImportError::Binding(name.to_string()))
}

fn import_native(
    environment: &impl Environment,
    unique_key: &str,
    namespace: &str,
    entries: &Records,
) -> Result<(), ImportError> {
    let data = environment
        .get("PEREN_DATA_DIR")
        .map_or_else(default_data, PathBuf::from)
        .join("cells");
    fs::create_dir_all(&data).map_err(|source| ImportError::CreateData {
        path: data.clone(),
        source,
    })?;
    let cell = kv_cell_id(unique_key, namespace);
    let path = data.join(format!("{cell}.sqlite"));
    let mut storage = CellStorage::open(&path)?;
    storage.put_many(namespace, entries)?;
    Ok(())
}

fn kv_store(environment: &impl Environment, backend: &KvBackend) -> Result<KvStore, ImportError> {
    match backend {
        KvBackend::Native => unreachable!("native KV imports use CellStorage directly"),
        KvBackend::Redis { url_env } => {
            let url = environment
                .get(url_env)
                .ok_or_else(|| ImportError::Environment(url_env.clone()))?;
            Ok(KvStore::redis(url, "peren"))
        }
        KvBackend::Bucket {
            endpoint,
            bucket,
            prefix,
            access_key_id_env,
            secret_access_key_env,
            allow_http,
        } => {
            let store = if endpoint.starts_with("memory://") {
                R2Store::new(Arc::new(object_store::memory::InMemory::new()))
            } else {
                let access_key = environment
                    .get(access_key_id_env)
                    .ok_or_else(|| ImportError::Environment(access_key_id_env.clone()))?;
                let secret_key = environment
                    .get(secret_access_key_env)
                    .ok_or_else(|| ImportError::Environment(secret_access_key_env.clone()))?;
                R2Store::s3(
                    S3Options {
                        bucket: bucket.clone(),
                        region: "us-east-1".into(),
                        endpoint: Some(endpoint.clone()),
                        allow_http: *allow_http,
                        virtual_hosted: false,
                    },
                    S3Credentials::new(access_key, secret_key, None),
                )?
            };
            Ok(KvStore::bucket(Arc::new(store), prefix.clone()))
        }
    }
}

fn kv_runtime() -> Result<tokio::runtime::Runtime, ImportError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(ImportError::Runtime)
}

fn parse(source: &str) -> Result<Records, ImportError> {
    source
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            let row: Row = serde_json::from_str(line).map_err(|source| ImportError::Json {
                line: index + 1,
                source,
            })?;
            Ok((row.key.into_bytes(), row.value.into_bytes()))
        })
        .collect()
}

fn default_data() -> PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join("data")
}

#[derive(Deserialize)]
struct Row {
    key: String,
    value: String,
}

#[derive(Debug, Error)]
pub enum ImportError {
    #[error("service {0:?} is not configured")]
    Service(String),
    #[error("binding {0:?} is not configured")]
    Binding(String),
    #[error("binding {0:?} is not a KV binding")]
    BindingKind(String),
    #[error("binding {0:?} does not use an operator-supported KV backend")]
    Backend(String),
    #[error("environment variable {0:?} is required for the KV backend")]
    Environment(String),
    #[error("failed to read KV import file {path:?}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create local cell data directory {path:?}")]
    CreateData {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create KV import runtime")]
    Runtime(#[source] std::io::Error),
    #[error("invalid KV import JSON on line {line}")]
    Json {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("KV provider operation failed")]
    Host(#[from] HostError),
    #[error("KV import key is not valid UTF-8")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    ObjectStore(#[from] peren_provider_object_store::BuildError),
    #[error(transparent)]
    Storage(#[from] peren_storage::StorageError),
}

#[cfg(test)]
mod tests {
    use peren_config::FleetConfig;
    use peren_storage::ListOptions;
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
[services.bindings.CACHE]
type = "kv"
namespace = "cache"
unique_key = "app-key"
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
    fn imports_native_kv_records_into_the_cell_database() {
        let root = std::env::temp_dir().join(format!("peren-kv-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let file = root.join("records.ndjson");
        fs::write(
            &file,
            "{\"key\":\"alpha\",\"value\":\"one\"}\n{\"key\":\"beta\",\"value\":\"two\"}\n",
        )
        .unwrap();
        let environment = TestEnvironment {
            data: root.join("data"),
        };

        let report = import(
            &config(""),
            &environment,
            &Import {
                service: "api".into(),
                binding: "CACHE".into(),
                file,
            },
        )
        .unwrap();

        let cell = kv_cell_id("app-key", "cache");
        let storage = CellStorage::open(
            &environment
                .data
                .join("cells")
                .join(format!("{cell}.sqlite")),
        )
        .unwrap();
        let entries = storage
            .list("cache", &ListOptions::default())
            .unwrap()
            .entries;
        assert_eq!(report.entries, 2);
        assert_eq!(
            entries,
            vec![
                (b"alpha".to_vec(), b"one".to_vec()),
                (b"beta".to_vec(), b"two".to_vec())
            ]
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn redis_import_requires_the_configured_url() {
        let root = std::env::temp_dir().join(format!("peren-kv-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let file = root.join("records.ndjson");
        fs::write(&file, "{\"key\":\"alpha\",\"value\":\"one\"}\n").unwrap();
        let environment = TestEnvironment {
            data: root.join("data"),
        };

        let result = import(
            &config("backend = { kind = \"redis\", url_env = \"REDIS_URL\" }"),
            &environment,
            &Import {
                service: "api".into(),
                binding: "CACHE".into(),
                file,
            },
        );

        assert!(matches!(result, Err(ImportError::Environment(name)) if name == "REDIS_URL"));
        fs::remove_dir_all(root).unwrap();
    }
}
