use std::path::Path;

use peren_primitives::StorageRevision;
use peren_storage::SqlLimitError;
use thiserror::Error;
use turso::Connection;
use uuid::Uuid;

mod alarm;
mod attachment;
mod cell;
mod list;
mod migration;
mod mutation;
mod sql;

pub use migration::{MigrationError, MigrationReport, migrate, validate_remote};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS kv (scope TEXT NOT NULL, k BLOB NOT NULL, v BLOB NOT NULL, PRIMARY KEY (scope, k))";
const REVISION_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS storage_metadata (id INTEGER PRIMARY KEY CHECK (id = 1), revision INTEGER NOT NULL CHECK (revision >= 0))";
const ALARM_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS alarms (scope TEXT PRIMARY KEY, at_ms INTEGER NOT NULL, retry INTEGER NOT NULL DEFAULT 0, counted_retry INTEGER NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 0)";
const CELL_SCHEMA: &str =
    "CREATE TABLE IF NOT EXISTS cell_metadata (scope TEXT PRIMARY KEY, actor_name TEXT NOT NULL)";
const ATTACHMENT_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS ws_attachments (connection_id TEXT PRIMARY KEY, bytes BLOB NOT NULL)";
const MUTATION_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS mutation_outcomes (id TEXT PRIMARY KEY, outcome BLOB NOT NULL, revision INTEGER NOT NULL CHECK(revision>=0))";

pub struct TursoStore {
    pub(crate) database: Database,
    pub(crate) connection: Connection,
    pub(crate) revision: StorageRevision,
    pub(crate) reconciliation_required: bool,
    pub(crate) session: SessionState,
}

pub(crate) enum Database {
    Local {
        _database: turso::Database,
    },
    Synced {
        database: turso::sync::Database,
        mode: SyncMode,
    },
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum SyncMode {
    Manual,
    Remote,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum SessionState {
    Closed,
    Open { dirty: bool },
}

impl TursoStore {
    pub async fn local(path: &Path) -> Result<Self, TursoError> {
        let path = path.to_str().ok_or(TursoError::InvalidPath)?;
        let database = turso::Builder::new_local(path)
            .experimental_without_rowid(true)
            .build()
            .await?;
        let connection = database.connect()?;
        Self::open(
            Database::Local {
                _database: database,
            },
            connection,
        )
        .await
    }

    pub async fn remote(url: String, token: String) -> Result<Self, TursoError> {
        Self::synced(":memory:", url, token, SyncMode::Remote).await
    }

    pub async fn replica(path: &Path, url: String, token: String) -> Result<Self, TursoError> {
        if path.exists() {
            validate_remote(path)?;
        }
        let path = path.to_str().ok_or(TursoError::InvalidPath)?;
        Self::synced(path, url, token, SyncMode::Manual).await
    }

    async fn synced(
        path: &str,
        url: String,
        token: String,
        mode: SyncMode,
    ) -> Result<Self, TursoError> {
        let database = turso::sync::Builder::new_remote(path)
            .with_remote_url(url)
            .with_auth_token(token)
            .build()
            .await?;
        let connection = database.connect().await?;
        let mut store = Self::open(Database::Synced { database, mode }, connection).await?;
        if mode == SyncMode::Remote {
            store.push().await?;
        }
        Ok(store)
    }

    async fn open(database: Database, connection: Connection) -> Result<Self, TursoError> {
        for schema in [
            SCHEMA,
            ALARM_SCHEMA,
            CELL_SCHEMA,
            ATTACHMENT_SCHEMA,
            MUTATION_SCHEMA,
            REVISION_SCHEMA,
        ] {
            connection.execute(schema, ()).await?;
        }
        connection
            .execute(
                "INSERT OR IGNORE INTO storage_metadata(id, revision) VALUES(1, 0)",
                (),
            )
            .await?;
        let mut rows = connection
            .query("SELECT revision FROM storage_metadata WHERE id = 1", ())
            .await?;
        let revision = rows
            .next()
            .await?
            .ok_or(TursoError::MissingRevision)?
            .get::<i64>(0)?;
        drop(rows);
        let revision = u64::try_from(revision).map_err(|_| TursoError::InvalidRevision)?;
        Ok(Self {
            database,
            connection,
            revision: StorageRevision::new(revision),
            reconciliation_required: false,
            session: SessionState::Closed,
        })
    }

    pub async fn apply(&mut self, source: &str) -> Result<StorageRevision, TursoError> {
        self.ensure_reconciled()?;
        if self.session != SessionState::Closed {
            return Err(TursoError::TransactionOpen);
        }

        self.connection.execute("BEGIN IMMEDIATE", ()).await?;
        let applied = async {
            self.connection.execute_batch(source).await?;
            let mut rows = self
                .connection
                .query("SELECT revision FROM storage_metadata WHERE id = 1", ())
                .await?;
            let current = rows
                .next()
                .await?
                .ok_or(TursoError::MissingRevision)?
                .get::<i64>(0)?;
            drop(rows);
            let next = current.checked_add(1).ok_or(TursoError::RevisionOverflow)?;
            self.connection
                .execute(
                    "UPDATE storage_metadata SET revision = ?1 WHERE id = 1",
                    turso::params![next],
                )
                .await?;
            self.connection.execute("COMMIT", ()).await?;
            Ok::<i64, TursoError>(next)
        }
        .await;

        let next = match applied {
            Ok(next) => next,
            Err(error) => {
                let _ = self.connection.execute("ROLLBACK", ()).await;
                return Err(error);
            }
        };
        self.revision =
            StorageRevision::new(u64::try_from(next).map_err(|_| TursoError::InvalidRevision)?);
        if self.requires_remote_commit() {
            self.push().await?;
        }
        Ok(self.revision)
    }

    pub async fn push(&mut self) -> Result<(), TursoError> {
        match &self.database {
            Database::Synced { database, .. } => match database.push().await {
                Ok(()) => {
                    self.reconciliation_required = false;
                    Ok(())
                }
                Err(error) => {
                    self.reconciliation_required = true;
                    Err(TursoError::RemoteCommit(error))
                }
            },
            Database::Local { .. } => Err(TursoError::LocalSync),
        }
    }

    pub async fn pull(&mut self) -> Result<bool, TursoError> {
        self.ensure_reconciled()?;
        match &self.database {
            Database::Synced { database, .. } => database.pull().await.map_err(Into::into),
            Database::Local { .. } => Err(TursoError::LocalSync),
        }
    }

    pub(crate) fn ensure_reconciled(&self) -> Result<(), TursoError> {
        (!self.reconciliation_required)
            .then_some(())
            .ok_or(TursoError::ReconciliationRequired)
    }

    pub(crate) fn requires_remote_commit(&self) -> bool {
        matches!(
            self.database,
            Database::Synced {
                mode: SyncMode::Remote,
                ..
            }
        )
    }

    pub(crate) fn require_session(&self) -> Result<bool, TursoError> {
        match self.session {
            SessionState::Closed => Err(TursoError::TransactionClosed),
            SessionState::Open { dirty } => Ok(dirty),
        }
    }
}

#[derive(Debug, Error)]
pub enum TursoError {
    #[error("Turso operation failed")]
    Database(#[from] turso::Error),
    #[error(transparent)]
    Migration(#[from] MigrationError),
    #[error("database path is not valid UTF-8")]
    InvalidPath,
    #[error("local databases cannot be synchronized with Turso Cloud")]
    LocalSync,
    #[error("remote commit outcome is ambiguous and must be reconciled before further access")]
    RemoteCommit(#[source] turso::Error),
    #[error("remote commit reconciliation is required before further access")]
    ReconciliationRequired,
    #[error("storage revision is exhausted")]
    RevisionOverflow,
    #[error("storage revision metadata is missing")]
    MissingRevision,
    #[error("storage revision metadata is invalid")]
    InvalidRevision,
    #[error("a storage transaction is already open")]
    TransactionOpen,
    #[error("no storage transaction is open")]
    TransactionClosed,
    #[error("key and value use {actual} bytes; the limit is {limit}")]
    EntryTooLarge { actual: usize, limit: usize },
    #[error("WebSocket attachment uses {actual} bytes; the limit is {limit}")]
    AttachmentTooLarge { actual: usize, limit: usize },
    #[error("mutation {0} was already recorded")]
    DuplicateMutation(Uuid),
    #[error(transparent)]
    InvalidListLimit(#[from] peren_storage::InvalidListLimit),
    #[error(transparent)]
    SqlLimit(#[from] SqlLimitError),
    #[error("SQL is empty")]
    EmptySql,
    #[error("only one SQL statement is allowed")]
    MultipleSql,
    #[error("transaction-control SQL is owned by the storage session")]
    TransactionSql,
    #[error("SQL syntax is invalid: {0}")]
    SqlSyntax(String),
}

#[cfg(test)]
mod store;
