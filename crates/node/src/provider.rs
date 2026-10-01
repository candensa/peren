use std::{collections::BTreeMap, sync::Arc};

use peren_config::{BucketKind, CredentialsSource, ValidatedConfig};
use peren_provider_object_store::{
    AzureCredentials, AzureOptions, BucketStore, MemoryStore, S3Credentials, S3Options,
};
use peren_runtime::{CompatibilityError, ResolvedCompatibility};
use thiserror::Error;

pub trait Environment {
    fn get(&self, name: &str) -> Option<String>;
}

pub struct ProcessEnvironment;

impl Environment for ProcessEnvironment {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

pub struct Providers {
    pub repository: Repository,
    pub secrets: BTreeMap<String, Arc<str>>,
    environment: BTreeMap<String, Arc<str>>,
    compatibility: BTreeMap<String, ResolvedCompatibility>,
}

impl Providers {
    pub async fn build(
        config: &ValidatedConfig,
        environment: &impl Environment,
    ) -> Result<Self, ProviderError> {
        let compatibility = resolve_compatibility(config)?;
        let repository = match config.raw.bucket.kind {
            BucketKind::Memory => Repository::Memory(MemoryStore::default()),
            BucketKind::File => {
                let path = required(config.raw.bucket.path.clone(), "bucket.path")?;
                std::fs::create_dir_all(&path).map_err(|source| ProviderError::CreateBucket {
                    path: path.clone(),
                    source,
                })?;
                let store = BucketStore::file(path)?;
                store
                    .verify_range_read(&format!("startup-{}", config.raw.node.id.simple()))
                    .await?;
                Repository::Bucket(store)
            }
            BucketKind::S3 => {
                let options = S3Options {
                    bucket: required(config.raw.bucket.name.clone(), "bucket.bucket")?,
                    region: config
                        .raw
                        .bucket
                        .region
                        .clone()
                        .unwrap_or_else(|| "us-east-1".into()),
                    endpoint: config.raw.bucket.endpoint.clone(),
                    allow_http: config.raw.bucket.allow_http,
                    virtual_hosted: false,
                };
                let store = match config.raw.bucket.credentials_source {
                    CredentialsSource::Configured | CredentialsSource::Environment => {
                        let access_name = required(
                            config.raw.bucket.access_key_env.as_deref(),
                            "bucket.access_key_env",
                        )?;
                        let secret_name = required(
                            config.raw.bucket.secret_key_env.as_deref(),
                            "bucket.secret_key_env",
                        )?;
                        let access_key = secret(environment, access_name)?;
                        let secret_key = secret(environment, secret_name)?;
                        BucketStore::s3(options, S3Credentials::new(access_key, secret_key, None))?
                    }
                    CredentialsSource::InstanceRole
                    | CredentialsSource::WorkloadIdentity
                    | CredentialsSource::EksPodIdentity => BucketStore::s3_instance_role(options)?,
                };
                store
                    .verify_cas(&format!("startup-{}", config.raw.node.id.simple()))
                    .await?;
                Repository::Bucket(store)
            }
            BucketKind::AzureBlob => {
                let account_name = required(
                    config.raw.bucket.azure_account_env.as_deref(),
                    "bucket.azure_account_env",
                )?;
                let access_key_name = required(
                    config.raw.bucket.azure_access_key_env.as_deref(),
                    "bucket.azure_access_key_env",
                )?;
                let account = secret(environment, account_name)?;
                let access_key = secret(environment, access_key_name)?;
                let store = BucketStore::azure(
                    AzureOptions {
                        account,
                        container: required(config.raw.bucket.name.clone(), "bucket.bucket")?,
                        endpoint: config.raw.bucket.endpoint.clone(),
                        emulator: config.raw.bucket.azure_emulator,
                    },
                    AzureCredentials::new(access_key),
                )?;
                store
                    .verify_cas(&format!("startup-{}", config.raw.node.id.simple()))
                    .await?;
                Repository::Bucket(store)
            }
        };
        let secrets = collect(config, environment)?;
        Ok(Self {
            repository,
            secrets: secrets.store,
            environment: secrets.environment,
            compatibility,
        })
    }

    #[must_use]
    pub fn resolved(&self, variable: &str) -> Option<&str> {
        self.environment.get(variable).map(AsRef::as_ref)
    }

    #[must_use]
    pub fn secret(&self, name: &str) -> Option<&str> {
        self.secrets.get(name).map(AsRef::as_ref)
    }

    #[must_use]
    pub fn compatibility(&self, service: &str) -> Option<&ResolvedCompatibility> {
        self.compatibility.get(service)
    }
}

mod secret;
use secret::{collect, required, resolve_compatibility, secret};

mod repository;
pub use repository::Repository;

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("failed to create file bucket directory {path:?}")]
    CreateBucket {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Compatibility(#[from] CompatibilityError),
    #[error("required configuration field {0} is missing")]
    MissingField(&'static str),
    #[error("required environment variable {0:?} is not set")]
    MissingEnvironment(String),
    #[error(transparent)]
    Build(#[from] peren_provider_object_store::BuildError),
    #[error("object provider returned an invalid range read")]
    RangeRead,
    #[error(transparent)]
    Conformance(#[from] peren_provider_object_store::ConformanceError),
    #[error(transparent)]
    Secret(#[from] crate::SecretError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use peren_config::FleetConfig;

    struct MapEnvironment(BTreeMap<String, String>);

    impl Environment for MapEnvironment {
        fn get(&self, name: &str) -> Option<String> {
            self.0.get(name).cloned()
        }
    }

    fn config(secret: bool) -> ValidatedConfig {
        let secret = if secret {
            "secrets = { TOKEN = \"PEREN_TEST_TOKEN\" }"
        } else {
            ""
        };
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
{secret}
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

    #[tokio::test]
    async fn memory_provider_builds_without_external_credentials() {
        let providers = Providers::build(&config(false), &MapEnvironment(BTreeMap::new()))
            .await
            .unwrap();
        assert!(matches!(providers.repository, Repository::Memory(_)));
    }

    #[tokio::test]
    async fn file_provider_builds_bucket_repository_with_conformance_checks() {
        let root =
            std::env::temp_dir().join(format!("peren-file-provider-{}", uuid::Uuid::new_v4()));
        let mut config = config(false);
        config.raw.bucket.kind = BucketKind::File;
        config.raw.bucket.path = Some(root.join("objects"));

        let providers = Providers::build(&config, &MapEnvironment(BTreeMap::new()))
            .await
            .unwrap();

        assert!(matches!(providers.repository, Repository::Bucket(_)));
        assert!(root.join("objects").is_dir());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn missing_secret_names_the_reference_without_exposing_a_value() {
        let Err(error) = Providers::build(&config(true), &MapEnvironment(BTreeMap::new())).await
        else {
            panic!("provider construction unexpectedly succeeded");
        };
        assert_eq!(
            error.to_string(),
            "required environment variable \"PEREN_TEST_TOKEN\" is not set"
        );
    }

    #[tokio::test]
    async fn turso_binding_credentials_are_resolved_before_listeners_open() {
        let mut config = config(false);
        config.raw.services[0].bindings.insert(
            "DB".into(),
            peren_config::Binding::D1Database {
                database_name: "app".into(),
                unique_key: "db-key".into(),
                backend: peren_config::D1Backend::Turso {
                    url_env: "TURSO_URL".into(),
                    token_env: "TURSO_TOKEN".into(),
                    replica_path: Some("./data/app.db".into()),
                },
            },
        );
        let environment = MapEnvironment(BTreeMap::from([
            ("TURSO_URL".into(), "libsql://private.example".into()),
            ("TURSO_TOKEN".into(), "secret-token".into()),
        ]));
        let providers = Providers::build(&config, &environment).await.unwrap();

        assert_eq!(
            providers.resolved("TURSO_URL"),
            Some("libsql://private.example")
        );
        assert_eq!(providers.resolved("TURSO_TOKEN"), Some("secret-token"));
    }

    #[tokio::test]
    async fn azure_blob_provider_requires_configured_environment_credentials() {
        let mut config = config(false);
        config.raw.bucket.kind = BucketKind::AzureBlob;
        config.raw.bucket.name = Some("fleet".into());
        config.raw.bucket.azure_account_env = Some("AZURE_STORAGE_ACCOUNT".into());
        config.raw.bucket.azure_access_key_env = Some("AZURE_STORAGE_ACCESS_KEY".into());

        let Err(error) = Providers::build(&config, &MapEnvironment(BTreeMap::new())).await else {
            panic!("Azure Blob provider unexpectedly built without credentials");
        };

        assert_eq!(
            error.to_string(),
            "required environment variable \"AZURE_STORAGE_ACCOUNT\" is not set"
        );
    }

    #[tokio::test]
    async fn ambient_cloud_credentials_skip_static_secret_resolution() {
        let mut config = config(false);
        config.raw.bucket.kind = BucketKind::S3;
        config.raw.bucket.name = Some("fleet".into());
        config.raw.bucket.endpoint = Some("http://127.0.0.1:9".into());
        config.raw.bucket.allow_http = true;
        for source in [
            CredentialsSource::InstanceRole,
            CredentialsSource::WorkloadIdentity,
            CredentialsSource::EksPodIdentity,
        ] {
            config.raw.bucket.credentials_source = source;

            let result = Providers::build(&config, &MapEnvironment(BTreeMap::new())).await;

            assert!(!matches!(result, Err(ProviderError::MissingEnvironment(_))));
        }
    }

    #[tokio::test]
    async fn binding_environment_references_are_resolved_before_listeners_open() {
        let mut config = config(false);
        config.raw.services[0].bindings.insert(
            "DB".into(),
            peren_config::Binding::D1Database {
                database_name: "app".into(),
                unique_key: "db-key".into(),
                backend: peren_config::D1Backend::External {
                    url_env: "DATABASE_URL".into(),
                    driver: peren_config::D1Driver::Postgres,
                },
            },
        );
        let environment = MapEnvironment(BTreeMap::from([(
            "DATABASE_URL".into(),
            "postgres://private".into(),
        )]));
        let providers = Providers::build(&config, &environment).await.unwrap();

        assert_eq!(
            providers.resolved("DATABASE_URL"),
            Some("postgres://private")
        );
    }

    #[tokio::test]
    async fn provider_binding_credentials_are_resolved_before_listeners_open() {
        let mut config = config(false);
        config.raw.services[0].bindings.insert(
            "VECTORS".into(),
            peren_config::Binding::Vectorize {
                endpoint: "articles".into(),
                credential_scope: "vectors".into(),
                provider: peren_config::VectorProvider::Pinecone {
                    url: "https://vectors.example".into(),
                    index: "articles".into(),
                    api_key_env: "PINECONE_API_KEY".into(),
                    namespace: Some("prod".into()),
                },
            },
        );
        config.raw.services[0].bindings.insert(
            "AI".into(),
            peren_config::Binding::Ai {
                endpoint: "https://ai.example".into(),
                credential_scope: "ai".into(),
                provider: peren_config::AiProvider::WorkersAi {
                    account_id_env: "CF_ACCOUNT_ID".into(),
                    api_token_env: "CF_API_TOKEN".into(),
                },
            },
        );
        config.raw.services[0].bindings.insert(
            "IMAGES".into(),
            peren_config::Binding::Images {
                provider: peren_config::ImageProvider::Http {
                    url: "https://images.example".into(),
                    token_env: Some("IMAGE_TOKEN".into()),
                },
            },
        );
        let environment = MapEnvironment(BTreeMap::from([
            ("PINECONE_API_KEY".into(), "vector-secret".into()),
            ("CF_ACCOUNT_ID".into(), "account".into()),
            ("CF_API_TOKEN".into(), "worker-ai-secret".into()),
            ("IMAGE_TOKEN".into(), "image-secret".into()),
        ]));
        let providers = Providers::build(&config, &environment).await.unwrap();

        assert_eq!(
            providers.resolved("PINECONE_API_KEY"),
            Some("vector-secret")
        );
        assert_eq!(providers.resolved("CF_ACCOUNT_ID"), Some("account"));
        assert_eq!(providers.resolved("CF_API_TOKEN"), Some("worker-ai-secret"));
        assert_eq!(providers.resolved("IMAGE_TOKEN"), Some("image-secret"));
    }

    #[tokio::test]
    async fn missing_provider_binding_credentials_fail_before_listeners_open() {
        let mut config = config(false);
        config.raw.services[0].bindings.insert(
            "VECTORS".into(),
            peren_config::Binding::Vectorize {
                endpoint: "articles".into(),
                credential_scope: "vectors".into(),
                provider: peren_config::VectorProvider::Pinecone {
                    url: "https://vectors.example".into(),
                    index: "articles".into(),
                    api_key_env: "PINECONE_API_KEY".into(),
                    namespace: None,
                },
            },
        );

        let result = Providers::build(&config, &MapEnvironment(BTreeMap::new())).await;

        assert!(matches!(
            result,
            Err(ProviderError::MissingEnvironment(variable)) if variable == "PINECONE_API_KEY"
        ));
    }

    #[tokio::test]
    async fn unavailable_compatibility_behavior_is_refused_during_construction() {
        let mut config = config(false);
        config.raw.services[0].compatibility_flags = vec!["nodejs_compat".to_string()];

        let result = Providers::build(&config, &MapEnvironment(BTreeMap::new())).await;
        assert!(matches!(
            result,
            Err(ProviderError::Compatibility(CompatibilityError::Unavailable(flag)))
                if flag == "nodejs_compat"
        ));
    }
}
