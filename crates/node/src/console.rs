use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{Environment, ProviderError, Providers};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use peren_config::{ConsoleBackend, ValidatedConfig};
use rand::TryRngCore;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const SCHEMA: &str = "
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS workspaces (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  created_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS workspace_memberships (
  workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  role TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY(workspace_id, user_id)
);
CREATE TABLE IF NOT EXISTS onboarding_tokens (
  hash TEXT PRIMARY KEY,
  workspace_id TEXT REFERENCES workspaces(id) ON DELETE CASCADE,
  expires_at_ms INTEGER NOT NULL,
  used_at_ms INTEGER
);
";

const TOKEN_BYTES: usize = 32;
const TOKEN_TTL_MS: i64 = 30 * 60 * 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Capability {
    Bootstrap,
    Register,
    SqliteBackend,
    BucketBackend,
    ServiceBrowser,
    CellBrowser,
    DataBrowser,
    QueueBrowser,
    VectorBrowser,
    FleetHealth,
    RbacScreens,
    DegradedAlerts,
}

impl Capability {
    #[must_use]
    pub const fn status(self) -> CapabilityStatus {
        match self {
            Self::Bootstrap | Self::Register | Self::SqliteBackend | Self::BucketBackend => {
                CapabilityStatus::Supported
            }
            Self::ServiceBrowser
            | Self::CellBrowser
            | Self::DataBrowser
            | Self::QueueBrowser
            | Self::VectorBrowser
            | Self::FleetHealth
            | Self::RbacScreens
            | Self::DegradedAlerts => CapabilityStatus::Unsupported,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityStatus {
    Supported,
    Unsupported,
}

pub const CAPABILITIES: &[Capability] = &[
    Capability::Bootstrap,
    Capability::Register,
    Capability::SqliteBackend,
    Capability::BucketBackend,
    Capability::ServiceBrowser,
    Capability::CellBrowser,
    Capability::DataBrowser,
    Capability::QueueBrowser,
    Capability::VectorBrowser,
    Capability::FleetHealth,
    Capability::RbacScreens,
    Capability::DegradedAlerts,
];

#[must_use]
pub fn unsupported_capabilities() -> Vec<Capability> {
    CAPABILITIES
        .iter()
        .copied()
        .filter(|capability| capability.status() == CapabilityStatus::Unsupported)
        .collect()
}

pub struct Bootstrap {
    pub workspace: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct BootstrapReport {
    pub workspace: String,
    pub token: String,
    pub expires_in_minutes: u64,
}

pub struct Register {
    pub token: String,
    pub email: String,
    pub name: String,
}

#[derive(Debug, Eq, PartialEq)]
pub struct RegisterReport {
    pub user: String,
    pub workspace: Option<String>,
}

pub async fn bootstrap<E: Environment>(
    config: &ValidatedConfig,
    environment: &E,
    command: &Bootstrap,
) -> Result<BootstrapReport, ConsoleError> {
    let console = console(config)?;
    let store = Store::open(config, environment, console).await?;
    bootstrap_with_store(console, store, command).await
}

async fn bootstrap_with_store(
    console: &peren_config::Console,
    store: Store,
    command: &Bootstrap,
) -> Result<BootstrapReport, ConsoleError> {
    let connection = open(console)?;
    let now = now_ms()?;
    let workspace = command.workspace.as_deref().unwrap_or("Default Workspace");
    let workspace_id =
        existing_workspace(&connection, workspace)?.unwrap_or_else(|| Uuid::new_v4().to_string());
    connection.execute(
        "INSERT OR IGNORE INTO workspaces(id,name,created_at_ms) VALUES(?1,?2,?3)",
        params![workspace_id, workspace, now],
    )?;
    let token = token()?;
    connection.execute(
        "INSERT INTO onboarding_tokens(hash,workspace_id,expires_at_ms,used_at_ms) VALUES(?1,?2,?3,NULL)",
        params![hash(&token), workspace_id, now + TOKEN_TTL_MS],
    )?;
    let report = BootstrapReport {
        workspace: workspace.to_string(),
        token,
        expires_in_minutes: 30,
    };
    drop(connection);
    store.save(console).await?;
    Ok(report)
}

pub async fn register<E: Environment>(
    config: &ValidatedConfig,
    environment: &E,
    command: &Register,
) -> Result<RegisterReport, ConsoleError> {
    let console = console(config)?;
    let store = Store::open(config, environment, console).await?;
    register_with_store(console, store, command).await
}

async fn register_with_store(
    console: &peren_config::Console,
    store: Store,
    command: &Register,
) -> Result<RegisterReport, ConsoleError> {
    let connection = open(console)?;
    let now = now_ms()?;
    let stored: Option<(Option<String>, i64, Option<i64>)> = connection
        .query_row(
            "SELECT workspace_id,expires_at_ms,used_at_ms FROM onboarding_tokens WHERE hash=?1",
            [hash(&command.token)],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((workspace, expires, consumed)) = stored else {
        return Err(ConsoleError::Token);
    };
    if consumed.is_some() || expires < now {
        return Err(ConsoleError::Token);
    }
    let user_id = Uuid::new_v4().to_string();
    connection.execute(
        "INSERT INTO users(id,email,name,created_at_ms) VALUES(?1,?2,?3,?4)",
        params![user_id, command.email, command.name, now],
    )?;
    if let Some(workspace) = &workspace {
        connection.execute(
            "INSERT INTO workspace_memberships(workspace_id,user_id,role,created_at_ms) VALUES(?1,?2,'owner',?3)",
            params![workspace, user_id, now],
        )?;
    }
    connection.execute(
        "UPDATE onboarding_tokens SET used_at_ms=?1 WHERE hash=?2",
        params![now, hash(&command.token)],
    )?;
    let report = RegisterReport {
        user: user_id,
        workspace,
    };
    drop(connection);
    store.save(console).await?;
    Ok(report)
}

fn console(config: &ValidatedConfig) -> Result<&peren_config::Console, ConsoleError> {
    config.raw.console.as_ref().ok_or(ConsoleError::Missing)
}

enum Store {
    Local,
    Bucket(crate::Repository),
}

impl Store {
    async fn open<E: Environment>(
        config: &ValidatedConfig,
        environment: &E,
        console: &peren_config::Console,
    ) -> Result<Self, ConsoleError> {
        fs::create_dir_all(&console.data_dir).map_err(|source| ConsoleError::Directory {
            path: console.data_dir.clone(),
            source,
        })?;
        match console.effective_backend(&config.raw.seed_peers) {
            ConsoleBackend::Sqlite => Ok(Self::Local),
            ConsoleBackend::Bucket => {
                let repository = Providers::build(config, environment).await?.repository;
                Self::bucket(repository, console).await
            }
        }
    }

    async fn bucket(
        repository: crate::Repository,
        console: &peren_config::Console,
    ) -> Result<Self, ConsoleError> {
        if let Some(bytes) = repository.read_object(console_key()).await? {
            fs::write(database_path(console), bytes).map_err(|source| ConsoleError::File {
                path: database_path(console),
                source,
            })?;
        }
        Ok(Self::Bucket(repository))
    }

    async fn save(self, console: &peren_config::Console) -> Result<(), ConsoleError> {
        match self {
            Self::Local => Ok(()),
            Self::Bucket(repository) => {
                let bytes =
                    fs::read(database_path(console)).map_err(|source| ConsoleError::File {
                        path: database_path(console),
                        source,
                    })?;
                repository.write_object(console_key(), bytes).await?;
                Ok(())
            }
        }
    }
}

fn open(console: &peren_config::Console) -> Result<Connection, ConsoleError> {
    let connection = Connection::open(database_path(console))?;
    connection.execute_batch(SCHEMA)?;
    Ok(connection)
}

fn database_path(console: &peren_config::Console) -> PathBuf {
    console.data_dir.join("console.sqlite")
}

const fn console_key() -> &'static str {
    "console/console.sqlite"
}

fn existing_workspace(connection: &Connection, name: &str) -> Result<Option<String>, ConsoleError> {
    connection
        .query_row("SELECT id FROM workspaces WHERE name=?1", [name], |row| {
            row.get(0)
        })
        .optional()
        .map_err(Into::into)
}

fn token() -> Result<String, ConsoleError> {
    let mut bytes = [0; TOKEN_BYTES];
    rand::rngs::OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|source| ConsoleError::Random(source.to_string()))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn hash(token: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(token.as_bytes()))
}

fn now_ms() -> Result<i64, ConsoleError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ConsoleError::Time)?;
    i64::try_from(duration.as_millis()).map_err(|_| ConsoleError::Time)
}

#[derive(Debug, Error)]
pub enum ConsoleError {
    #[error("console is not configured")]
    Missing,
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error("failed to read or write console database {path:?}")]
    File {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create console data directory {path:?}")]
    Directory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("console onboarding token is invalid, expired, or already used")]
    Token,
    #[error("system clock cannot produce a valid console timestamp")]
    Time,
    #[error("secure random source failed: {0}")]
    Random(String),
    #[error(transparent)]
    Database(#[from] rusqlite::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use peren_config::FleetConfig;

    fn config(root: &std::path::Path) -> ValidatedConfig {
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
[console]
listen = "127.0.0.1:9000"
data_dir = "{}"
[[services]]
name = "api"
worker_bundle_path = "worker.js"
compatibility_date = "2026-01-01"
[[sockets]]
name = "public"
listen = "127.0.0.1:8080"
service = "api"
"#,
            root.join("console").display()
        ))
        .unwrap()
        .validate()
        .unwrap()
    }

    #[tokio::test]
    async fn bootstrap_token_registers_one_operator_once() {
        let root = std::env::temp_dir().join(format!("peren-console-{}", Uuid::new_v4()));
        let config = config(&root);
        let boot = bootstrap(
            &config,
            &crate::ProcessEnvironment,
            &Bootstrap {
                workspace: Some("Ops".into()),
            },
        )
        .await
        .unwrap();
        assert_eq!(boot.workspace, "Ops");
        assert_eq!(boot.expires_in_minutes, 30);

        let registered = register(
            &config,
            &crate::ProcessEnvironment,
            &Register {
                token: boot.token.clone(),
                email: "ops@example.com".into(),
                name: "Operator".into(),
            },
        )
        .await
        .unwrap();

        assert!(registered.workspace.is_some());
        assert!(matches!(
            register(
                &config,
                &crate::ProcessEnvironment,
                &Register {
                    token: boot.token,
                    email: "again@example.com".into(),
                    name: "Again".into()
                }
            )
            .await,
            Err(ConsoleError::Token)
        ));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn bucket_backend_restores_console_database_between_commands() {
        let root = std::env::temp_dir().join(format!("peren-console-bucket-{}", Uuid::new_v4()));
        let config = config(&root);
        let console = config.raw.console.as_ref().unwrap();
        std::fs::create_dir_all(&console.data_dir).unwrap();
        let repository = crate::Repository::Bucket(peren_provider_object_store::BucketStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        ));

        let boot = bootstrap_with_store(
            console,
            Store::bucket(repository.clone(), console).await.unwrap(),
            &Bootstrap {
                workspace: Some("Ops".into()),
            },
        )
        .await
        .unwrap();
        std::fs::remove_file(database_path(console)).unwrap();

        let registered = register_with_store(
            console,
            Store::bucket(repository, console).await.unwrap(),
            &Register {
                token: boot.token,
                email: "ops@example.com".into(),
                name: "Operator".into(),
            },
        )
        .await
        .unwrap();

        assert!(registered.workspace.is_some());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn console_capability_table_separates_backend_from_unsupported_ui() {
        let supported = CAPABILITIES
            .iter()
            .copied()
            .filter(|capability| capability.status() == CapabilityStatus::Supported)
            .collect::<Vec<_>>();
        assert_eq!(
            supported,
            vec![
                Capability::Bootstrap,
                Capability::Register,
                Capability::SqliteBackend,
                Capability::BucketBackend
            ]
        );
        assert_eq!(
            unsupported_capabilities(),
            vec![
                Capability::ServiceBrowser,
                Capability::CellBrowser,
                Capability::DataBrowser,
                Capability::QueueBrowser,
                Capability::VectorBrowser,
                Capability::FleetHealth,
                Capability::RbacScreens,
                Capability::DegradedAlerts,
            ]
        );
    }
}
