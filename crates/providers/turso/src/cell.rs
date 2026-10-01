use peren_storage::{CellStore, Committed, MAX_ENTRY_BYTES};
use turso::params;

use crate::{SessionState, TursoError, TursoStore};

impl CellStore for TursoStore {
    type Error = TursoError;

    async fn begin(&mut self) -> Result<(), Self::Error> {
        self.ensure_reconciled()?;
        if self.session != SessionState::Closed {
            return Err(TursoError::TransactionOpen);
        }
        self.connection.execute("BEGIN IMMEDIATE", ()).await?;
        self.session = SessionState::Open { dirty: false };
        Ok(())
    }

    async fn load(&mut self, scope: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.ensure_reconciled()?;
        let mut rows = self
            .connection
            .query(
                "SELECT v FROM kv WHERE scope = ?1 AND k = ?2",
                params![scope, key],
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        Ok(Some(row.get::<Vec<u8>>(0)?))
    }

    async fn put(&mut self, scope: &str, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.ensure_reconciled()?;
        self.require_session()?;
        let actual = key.len().saturating_add(value.len());
        if actual > MAX_ENTRY_BYTES {
            return Err(TursoError::EntryTooLarge {
                actual,
                limit: MAX_ENTRY_BYTES,
            });
        }
        self.connection
            .execute(
                "INSERT INTO kv(scope,k,v) VALUES(?1,?2,?3) ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
                params![scope, key, value],
            )
            .await?;
        self.session = SessionState::Open { dirty: true };
        Ok(())
    }

    async fn delete(&mut self, scope: &str, key: &[u8]) -> Result<bool, Self::Error> {
        self.ensure_reconciled()?;
        let dirty = self.require_session()?;
        let deleted = self
            .connection
            .execute(
                "DELETE FROM kv WHERE scope = ?1 AND k = ?2",
                params![scope, key],
            )
            .await?
            > 0;
        self.session = SessionState::Open {
            dirty: dirty || deleted,
        };
        Ok(deleted)
    }

    async fn commit(&mut self) -> Result<Committed<()>, Self::Error> {
        self.ensure_reconciled()?;
        let dirty = self.require_session()?;
        let mut revision_rows = self
            .connection
            .query("SELECT revision FROM storage_metadata WHERE id = 1", ())
            .await?;
        let current_revision = revision_rows
            .next()
            .await?
            .ok_or(TursoError::MissingRevision)?
            .get::<i64>(0)?;
        drop(revision_rows);
        let committed_revision = if dirty {
            let next = current_revision
                .checked_add(1)
                .ok_or(TursoError::RevisionOverflow)?;
            self.connection
                .execute(
                    "UPDATE storage_metadata SET revision = ?1 WHERE id = 1",
                    params![next],
                )
                .await?;
            next
        } else {
            current_revision
        };
        if let Err(error) = self.connection.execute("COMMIT", ()).await {
            let _ = self.connection.execute("ROLLBACK", ()).await;
            self.session = SessionState::Closed;
            return Err(error.into());
        }
        self.session = SessionState::Closed;
        self.revision = peren_primitives::StorageRevision::new(
            u64::try_from(committed_revision).map_err(|_| TursoError::InvalidRevision)?,
        );
        if dirty && self.requires_remote_commit() {
            self.push().await?;
        }
        Ok(Committed {
            value: (),
            revision: self.revision,
        })
    }

    async fn rollback(&mut self) -> Result<(), Self::Error> {
        self.require_session()?;
        let result = self.connection.execute("ROLLBACK", ()).await;
        self.session = SessionState::Closed;
        result.map(|_| ()).map_err(Into::into)
    }
}
