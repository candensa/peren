use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use peren_config::{SecretsProvider, ValidatedConfig};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::Environment;

const FILE: &str = "secrets.json";

#[derive(Debug)]
pub struct Rotate {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Eq, PartialEq, serde::Serialize)]
pub struct RotateReport {
    pub secret: Metadata,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct Metadata {
    pub name: String,
    pub version: u64,
    pub digest: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Eq, PartialEq, serde::Serialize)]
pub struct ListReport {
    pub secrets: Vec<Metadata>,
}

#[derive(Debug, Eq, PartialEq, serde::Serialize)]
pub struct GetReport {
    pub secret: Metadata,
}

#[derive(Debug, Eq, PartialEq, serde::Serialize)]
pub struct DeleteReport {
    pub deleted: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredSecret {
    name: String,
    version: u64,
    digest: String,
    created_at_ms: i64,
    active: bool,
    value: String,
}

impl StoredSecret {
    fn value(&self) -> &str {
        &self.value
    }

    fn metadata(&self) -> Metadata {
        Metadata {
            name: self.name.clone(),
            version: self.version,
            digest: self.digest.clone(),
            created_at_ms: self.created_at_ms,
        }
    }
}

pub fn rotate(
    config: &ValidatedConfig,
    environment: &impl Environment,
    request: Rotate,
) -> Result<RotateReport, SecretError> {
    validate_name(config, &request.name)?;
    if request.value.is_empty() {
        return Err(SecretError::EmptyValue(request.name));
    }
    backend(config, environment)?.rotate(request)
}

pub fn active_value(
    config: &ValidatedConfig,
    environment: &impl Environment,
    name: &str,
) -> Result<Option<String>, SecretError> {
    validate_name(config, name)?;
    backend(config, environment)?.active_value(name)
}

pub fn list(
    config: &ValidatedConfig,
    environment: &impl Environment,
) -> Result<ListReport, SecretError> {
    let names = declared_names(config).collect();
    backend(config, environment)?.list(names)
}

pub fn get(
    config: &ValidatedConfig,
    environment: &impl Environment,
    name: &str,
) -> Result<GetReport, SecretError> {
    validate_name(config, name)?;
    backend(config, environment)?.get(name)
}

pub fn delete(
    config: &ValidatedConfig,
    environment: &impl Environment,
    name: &str,
) -> Result<DeleteReport, SecretError> {
    validate_name(config, name)?;
    backend(config, environment)?.delete(name)
}

fn backend(
    config: &ValidatedConfig,
    environment: &impl Environment,
) -> Result<ResolvedBackend, SecretError> {
    match config.raw.secrets.provider {
        SecretsProvider::Local => Ok(ResolvedBackend::Local(LocalBackend {
            root: root(environment),
        })),
        provider => Err(SecretError::UnsupportedProvider(provider)),
    }
}

trait Backend {
    fn rotate(&self, request: Rotate) -> Result<RotateReport, SecretError>;
    fn active_value(&self, name: &str) -> Result<Option<String>, SecretError>;
    fn list(&self, names: Vec<String>) -> Result<ListReport, SecretError>;
    fn get(&self, name: &str) -> Result<GetReport, SecretError>;
    fn delete(&self, name: &str) -> Result<DeleteReport, SecretError>;
}

enum ResolvedBackend {
    Local(LocalBackend),
}

impl Backend for ResolvedBackend {
    fn rotate(&self, request: Rotate) -> Result<RotateReport, SecretError> {
        match self {
            Self::Local(backend) => backend.rotate(request),
        }
    }

    fn active_value(&self, name: &str) -> Result<Option<String>, SecretError> {
        match self {
            Self::Local(backend) => backend.active_value(name),
        }
    }

    fn list(&self, names: Vec<String>) -> Result<ListReport, SecretError> {
        match self {
            Self::Local(backend) => backend.list(names),
        }
    }

    fn get(&self, name: &str) -> Result<GetReport, SecretError> {
        match self {
            Self::Local(backend) => backend.get(name),
        }
    }

    fn delete(&self, name: &str) -> Result<DeleteReport, SecretError> {
        match self {
            Self::Local(backend) => backend.delete(name),
        }
    }
}

struct LocalBackend {
    root: PathBuf,
}

impl Backend for LocalBackend {
    fn rotate(&self, request: Rotate) -> Result<RotateReport, SecretError> {
        let mut registry = Registry::load(self.root.clone())?;
        let version = registry.next(&request.name);
        let generation = StoredSecret {
            name: request.name,
            version,
            digest: digest(&request.value),
            created_at_ms: now_ms()?,
            active: true,
            value: request.value,
        };
        registry.insert(generation.clone());
        registry.save()?;
        Ok(RotateReport {
            secret: generation.metadata(),
        })
    }

    fn active_value(&self, name: &str) -> Result<Option<String>, SecretError> {
        Ok(Registry::load(self.root.clone())?
            .active(name)
            .map(|generation| generation.value().to_string()))
    }

    fn list(&self, names: Vec<String>) -> Result<ListReport, SecretError> {
        let registry = Registry::load(self.root.clone())?;
        let mut secrets = Vec::new();
        for name in names {
            if let Some(generation) = registry.active(&name) {
                secrets.push(generation.metadata());
            }
        }
        Ok(ListReport { secrets })
    }

    fn get(&self, name: &str) -> Result<GetReport, SecretError> {
        let registry = Registry::load(self.root.clone())?;
        let generation = registry
            .active(name)
            .ok_or_else(|| SecretError::Missing(name.to_string()))?;
        Ok(GetReport {
            secret: generation.metadata(),
        })
    }

    fn delete(&self, name: &str) -> Result<DeleteReport, SecretError> {
        let mut registry = Registry::load(self.root.clone())?;
        let deleted = registry.delete(name);
        if deleted {
            registry.save()?;
        }
        Ok(DeleteReport { deleted })
    }
}

fn validate_name(config: &ValidatedConfig, name: &str) -> Result<(), SecretError> {
    if declared_names(config).any(|declared| declared == name) {
        Ok(())
    } else {
        Err(SecretError::Unknown(name.to_string()))
    }
}

fn declared_names(config: &ValidatedConfig) -> impl Iterator<Item = String> + '_ {
    config
        .raw
        .secrets_store
        .keys()
        .cloned()
        .chain(config.raw.services.iter().flat_map(|service| {
            service
                .secrets
                .iter()
                .filter_map(|(binding, secret)| secret.store_name(binding).map(str::to_string))
        }))
        .chain(
            config
                .raw
                .services
                .iter()
                .flat_map(|service| service.secrets_store_refs.values().cloned()),
        )
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct Registry {
    generations: Vec<StoredSecret>,
    #[serde(skip)]
    root: PathBuf,
}

impl Registry {
    fn load(root: PathBuf) -> Result<Self, SecretError> {
        let path = root.join(FILE);
        match fs::read(&path) {
            Ok(bytes) => {
                let mut registry: Registry = serde_json::from_slice(&bytes)?;
                registry.root = root;
                Ok(registry)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self {
                generations: Vec::new(),
                root,
            }),
            Err(source) => Err(SecretError::Read { path, source }),
        }
    }

    fn save(&self) -> Result<(), SecretError> {
        fs::create_dir_all(&self.root).map_err(|source| SecretError::Create {
            path: self.root.clone(),
            source,
        })?;
        let path = self.root.join(FILE);
        fs::write(&path, serde_json::to_vec_pretty(self)?)
            .map_err(|source| SecretError::Write { path, source })
    }

    fn next(&self, name: &str) -> u64 {
        self.generations
            .iter()
            .filter(|generation| generation.name == name)
            .map(|generation| generation.version)
            .max()
            .unwrap_or(0)
            + 1
    }

    fn insert(&mut self, generation: StoredSecret) {
        for existing in &mut self.generations {
            if existing.name == generation.name {
                existing.active = false;
            }
        }
        self.generations.push(generation);
        self.generations.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| right.version.cmp(&left.version))
        });
    }

    fn active(&self, name: &str) -> Option<&StoredSecret> {
        self.generations
            .iter()
            .find(|generation| generation.name == name && generation.active)
    }

    fn delete(&mut self, name: &str) -> bool {
        let mut deleted = false;
        for generation in &mut self.generations {
            if generation.name == name && generation.active {
                generation.active = false;
                deleted = true;
            }
        }
        deleted
    }
}

fn root(environment: &impl Environment) -> PathBuf {
    environment
        .get("PEREN_DATA_DIR")
        .map_or_else(super::process::default_data, PathBuf::from)
        .join("secrets")
}

fn digest(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

fn now_ms() -> Result<i64, SecretError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SecretError::Time)?;
    i64::try_from(duration.as_millis()).map_err(|_| SecretError::Time)
}

#[derive(Debug, Error)]
pub enum SecretError {
    #[error("secret {0:?} is not declared by the fleet config")]
    Unknown(String),
    #[error("secret {0:?} cannot be rotated to an empty value")]
    EmptyValue(String),
    #[error("secret {0:?} has no active local value")]
    Missing(String),
    #[error("secrets provider {0:?} is not implemented in this build")]
    UnsupportedProvider(SecretsProvider),
    #[error("system clock cannot produce a valid secret timestamp")]
    Time,
    #[error("failed to read secret registry {path:?}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create secret registry directory {path:?}")]
    Create {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write secret registry {path:?}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::DataEnv;
    use peren_config::FleetConfig;
    use uuid::Uuid;

    #[test]
    fn rotation_versions_secret_without_printable_value() {
        let temp = std::env::temp_dir().join(format!("peren-secret-{}", Uuid::new_v4()));
        fs::create_dir_all(&temp).unwrap();
        let config = config(&temp.join("worker.js"));
        let env = DataEnv::new(temp.join("data"));

        let first = rotate(
            &config,
            &env,
            Rotate {
                name: "TOKEN".into(),
                value: "one".into(),
            },
        )
        .unwrap();
        let second = rotate(
            &config,
            &env,
            Rotate {
                name: "TOKEN".into(),
                value: "two".into(),
            },
        )
        .unwrap();

        assert_eq!(first.secret.version, 1);
        assert_eq!(second.secret.version, 2);
        assert_ne!(first.secret.digest, second.secret.digest);
        let registry = Registry::load(env.path().join("secrets")).unwrap();
        assert_eq!(
            registry
                .generations
                .iter()
                .filter(|item| item.active)
                .count(),
            1
        );
        assert!(
            registry
                .generations
                .iter()
                .any(|item| item.value() == "two")
        );
        fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn unsupported_secret_provider_is_refused_without_reading_values() {
        let temp = std::env::temp_dir().join(format!("peren-secret-{}", Uuid::new_v4()));
        fs::create_dir_all(&temp).unwrap();
        let mut config = config(&temp.join("worker.js"));
        config.raw.secrets.provider = SecretsProvider::AwsSecretsManager;
        let env = DataEnv::new(temp.join("data"));

        let error = rotate(
            &config,
            &env,
            Rotate {
                name: "TOKEN".into(),
                value: "super-secret-value".into(),
            },
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("AwsSecretsManager"), "{error}");
        assert!(!error.contains("super-secret-value"), "{error}");
    }

    #[test]
    fn public_reports_do_not_debug_or_serialize_secret_values() {
        let temp = std::env::temp_dir().join(format!("peren-secret-{}", Uuid::new_v4()));
        fs::create_dir_all(&temp).unwrap();
        let config = config(&temp.join("worker.js"));
        let env = DataEnv::new(temp.join("data"));
        let value = "super-secret-value";

        let rotated = rotate(
            &config,
            &env,
            Rotate {
                name: "TOKEN".into(),
                value: value.into(),
            },
        )
        .unwrap();
        let listed = list(&config, &env).unwrap();
        let fetched = get(&config, &env, "TOKEN").unwrap();
        let deleted = delete(&config, &env, "TOKEN").unwrap();

        let debug = format!("{rotated:?} {listed:?} {fetched:?} {deleted:?}");
        let json = serde_json::json!({
            "rotated": rotated,
            "listed": listed,
            "fetched": fetched,
            "deleted": deleted,
        })
        .to_string();

        assert!(!debug.contains(value), "{debug}");
        assert!(!json.contains(value), "{json}");
        fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn metadata_operations_do_not_return_secret_values() {
        let temp = std::env::temp_dir().join(format!("peren-secret-{}", Uuid::new_v4()));
        fs::create_dir_all(&temp).unwrap();
        let config = config(&temp.join("worker.js"));
        let env = DataEnv::new(temp.join("data"));

        rotate(
            &config,
            &env,
            Rotate {
                name: "TOKEN".into(),
                value: "super-secret-value".into(),
            },
        )
        .unwrap();

        let listed = list(&config, &env).unwrap();
        assert_eq!(listed.secrets.len(), 1);
        assert_eq!(listed.secrets[0].name, "TOKEN");
        assert_ne!(listed.secrets[0].digest, "super-secret-value");
        let fetched = get(&config, &env, "TOKEN").unwrap();
        assert_eq!(fetched.secret, listed.secrets[0]);
        assert!(delete(&config, &env, "TOKEN").unwrap().deleted);
        assert!(
            matches!(get(&config, &env, "TOKEN"), Err(SecretError::Missing(name)) if name == "TOKEN")
        );
        fs::remove_dir_all(temp).unwrap();
    }

    fn config(bundle: &std::path::Path) -> ValidatedConfig {
        fs::write(
            bundle,
            "export default { fetch() { return new Response('ok') } };",
        )
        .unwrap();
        let text = format!(
            r#"
[node]
node_id = "{}"
advertise_addr = "127.0.0.1:0"
listen = "127.0.0.1:0"

[bucket]
kind = "memory"

[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"

[secrets_store]
TOKEN = "TOKEN_ENV"

[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2024-01-01"
"#,
            Uuid::new_v4(),
            bundle.display()
        );
        toml::from_str::<FleetConfig>(&text)
            .unwrap()
            .validate()
            .unwrap()
    }
}
