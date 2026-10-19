use std::path::{Path, PathBuf};

mod effect;

use peren_primitives::{CellId, DurabilityReceipt, StorageRevision};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use crate::{
    Alarm, AlarmStore, AttachmentStore, CellStore, CheckpointBytes, Committed, EffectDraft,
    EffectStatus, KvEntries, ListOptions, ListPage, ListStore, MAX_ATTACHMENT_BYTES,
    MutationRecord, MutationStore, ReplicaBytes, SqlResult, SqlStore, SqlValue, StorageError,
    StorageTransaction,
    connection::{self, advance_revision, load_revision},
    kv, replica, sql, validate_sql,
};

pub struct CellStorage {
    connection: Connection,
    path: PathBuf,
    revision: StorageRevision,
    session: SessionState,
}

impl CellStore for CellStorage {
    type Error = StorageError;

    async fn begin(&mut self) -> Result<(), Self::Error> {
        if self.session != SessionState::Closed {
            return Err(StorageError::TransactionOpen);
        }
        self.connection.execute_batch("BEGIN IMMEDIATE")?;
        self.session = SessionState::Open { dirty: false };
        Ok(())
    }

    async fn load(&mut self, scope: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.get(scope, key)
    }

    async fn put(&mut self, scope: &str, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.require_session()?;
        kv::validate(key, value)?;
        self.connection.execute(
            "INSERT INTO kv(scope,k,v) VALUES(?1,?2,?3) ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
            params![scope, key, value],
        )?;
        self.mark_dirty();
        Ok(())
    }

    async fn delete(&mut self, scope: &str, key: &[u8]) -> Result<bool, Self::Error> {
        self.require_session()?;
        let deleted = self.connection.execute(
            "DELETE FROM kv WHERE scope=?1 AND k=?2",
            params![scope, key],
        )? > 0;
        if deleted {
            self.mark_dirty();
        }
        Ok(deleted)
    }

    async fn commit(&mut self) -> Result<Committed<()>, Self::Error> {
        let dirty = self.require_session()?;
        let revision = if dirty {
            advance_revision(&self.connection)?
        } else {
            load_revision(&self.connection)?
        };
        if let Err(error) = self.connection.execute_batch("COMMIT") {
            let _ = self.connection.execute_batch("ROLLBACK");
            self.session = SessionState::Closed;
            return Err(error.into());
        }
        self.session = SessionState::Closed;
        self.revision = revision;
        Ok(Committed {
            value: (),
            revision,
        })
    }

    async fn rollback(&mut self) -> Result<(), Self::Error> {
        self.require_session()?;
        let result = self.connection.execute_batch("ROLLBACK");
        self.session = SessionState::Closed;
        result.map_err(Into::into)
    }
}

impl ListStore for CellStorage {
    async fn list(
        &mut self,
        scope: &str,
        options: &ListOptions<'_>,
    ) -> Result<ListPage, Self::Error> {
        kv::list(&self.connection, scope, options)
    }
}

impl SqlStore for CellStorage {
    async fn sql(
        &mut self,
        query: &str,
        parameters: &[SqlValue],
    ) -> Result<SqlResult, Self::Error> {
        self.require_session()?;
        let result = sql::run(&self.connection, query, parameters)?;
        if result.mutates {
            self.mark_dirty();
        }
        Ok(result.result)
    }
}

impl AlarmStore for CellStorage {
    async fn alarm(&mut self, scope: &str) -> Result<Option<Alarm>, Self::Error> {
        CellStorage::alarm(self, scope)
    }

    async fn set_alarm(&mut self, scope: &str, at_ms: i64) -> Result<(), Self::Error> {
        self.require_session()?;
        self.connection.execute(
            "INSERT INTO alarms(scope,at_ms,retry,counted_retry,generation) VALUES(?1,?2,0,0,0) ON CONFLICT(scope) DO UPDATE SET at_ms=excluded.at_ms,retry=0,counted_retry=0,generation=alarms.generation+1",
            params![scope, at_ms],
        )?;
        self.mark_dirty();
        Ok(())
    }

    async fn retry_alarm(&mut self, scope: &str, generation: i64) -> Result<bool, Self::Error> {
        self.require_session()?;
        let changed = self.connection.execute(
            "UPDATE alarms SET retry=retry+1,counted_retry=counted_retry+1 WHERE scope=?1 AND generation=?2",
            params![scope, generation],
        )? > 0;
        if changed {
            self.mark_dirty();
        }
        Ok(changed)
    }

    async fn delete_alarm(&mut self, scope: &str) -> Result<bool, Self::Error> {
        self.require_session()?;
        let changed = self
            .connection
            .execute("DELETE FROM alarms WHERE scope=?1", [scope])?
            > 0;
        if changed {
            self.mark_dirty();
        }
        Ok(changed)
    }
}

impl MutationStore for CellStorage {
    async fn mutation_outcome(&mut self, id: Uuid) -> Result<Option<MutationRecord>, Self::Error> {
        CellStorage::mutation_outcome(self, id)
    }

    async fn record_mutation_outcome(
        &mut self,
        id: Uuid,
        outcome: &[u8],
    ) -> Result<(), Self::Error> {
        self.require_session()?;
        let revision = load_revision(&self.connection)?;
        let inserted = self.connection.execute(
            "INSERT OR IGNORE INTO mutation_outcomes(id,outcome,revision) VALUES(?1,?2,?3)",
            params![
                id.to_string(),
                outcome,
                i64::try_from(revision.get()).map_err(|_| StorageError::RevisionOverflow)?
            ],
        )?;
        if inserted == 0 {
            return Err(StorageError::DuplicateMutation(id));
        }
        self.mark_dirty();
        Ok(())
    }
}

impl AttachmentStore for CellStorage {
    async fn attachment(&mut self, id: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        CellStorage::attachment(self, id)
    }

    async fn set_attachment(&mut self, id: &str, bytes: &[u8]) -> Result<(), Self::Error> {
        self.require_session()?;
        if bytes.len() > MAX_ATTACHMENT_BYTES {
            return Err(StorageError::AttachmentTooLarge {
                actual: bytes.len(),
                limit: MAX_ATTACHMENT_BYTES,
            });
        }
        self.connection.execute(
            "INSERT INTO ws_attachments(connection_id,bytes) VALUES(?1,?2) ON CONFLICT(connection_id) DO UPDATE SET bytes=excluded.bytes",
            params![id, bytes],
        )?;
        self.mark_dirty();
        Ok(())
    }

    async fn delete_attachment(&mut self, id: &str) -> Result<bool, Self::Error> {
        self.require_session()?;
        let changed = self
            .connection
            .execute("DELETE FROM ws_attachments WHERE connection_id=?1", [id])?
            > 0;
        if changed {
            self.mark_dirty();
        }
        Ok(changed)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SessionState {
    Closed,
    Open { dirty: bool },
}

impl CellStorage {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let (connection, revision) = connection::open(path)?;
        Ok(Self {
            connection,
            path: path.to_path_buf(),
            revision,
            session: SessionState::Closed,
        })
    }

    pub fn open_read_only(path: &Path) -> Result<Self, StorageError> {
        let (connection, revision) = connection::open_read_only(path)?;
        Ok(Self {
            connection,
            path: path.to_path_buf(),
            revision,
            session: SessionState::Closed,
        })
    }

    fn require_session(&self) -> Result<bool, StorageError> {
        match self.session {
            SessionState::Closed => Err(StorageError::TransactionClosed),
            SessionState::Open { dirty } => Ok(dirty),
        }
    }

    fn mark_dirty(&mut self) {
        self.session = SessionState::Open { dirty: true };
    }

    pub fn get(&self, scope: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        kv::get(&self.connection, scope, key)
    }

    pub fn get_many(&self, scope: &str, keys: &[Vec<u8>]) -> Result<KvEntries, StorageError> {
        let mut entries = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = self.get(scope, key)? {
                entries.push((key.clone(), value));
            }
        }
        Ok(entries)
    }

    pub fn put(
        &mut self,
        scope: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<StorageRevision, StorageError> {
        kv::validate(key, value)?;
        self.mutate(|tx| { tx.execute("INSERT INTO kv(scope,k,v) VALUES(?1,?2,?3) ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",params![scope,key,value])?; Ok(()) })
    }

    pub fn put_many(
        &mut self,
        scope: &str,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<StorageRevision, StorageError> {
        self.transaction(|transaction| {
            for (key, value) in entries {
                transaction.put(scope, key, value)?;
            }
            Ok(())
        })
        .map(|commit| commit.revision)
    }

    pub fn delete(&mut self, scope: &str, key: &[u8]) -> Result<Committed<bool>, StorageError> {
        let (value, revision) = self.mutate_when(|tx| {
            Ok(tx.execute(
                "DELETE FROM kv WHERE scope=?1 AND k=?2",
                params![scope, key],
            )? > 0)
        })?;
        Ok(Committed { value, revision })
    }

    pub fn delete_many(
        &mut self,
        scope: &str,
        keys: &[Vec<u8>],
    ) -> Result<Committed<usize>, StorageError> {
        self.transaction(|transaction| {
            let mut deleted = 0;
            for key in keys {
                deleted += usize::from(transaction.delete(scope, key)?);
            }
            Ok(deleted)
        })
    }

    pub fn list(&self, scope: &str, options: &ListOptions<'_>) -> Result<ListPage, StorageError> {
        kv::list(&self.connection, scope, options)
    }

    pub fn alarm(&self, scope: &str) -> Result<Option<Alarm>, StorageError> {
        self.connection
            .query_row(
                "SELECT at_ms,retry,counted_retry,generation FROM alarms WHERE scope=?1",
                [scope],
                |row| {
                    Ok(Alarm {
                        at_ms: row.get(0)?,
                        retry: row.get(1)?,
                        counted_retry: row.get(2)?,
                        generation: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn set_alarm(&mut self, scope: &str, at_ms: i64) -> Result<StorageRevision, StorageError> {
        self.mutate(|tx| { tx.execute("INSERT INTO alarms(scope,at_ms,retry,counted_retry,generation) VALUES(?1,?2,0,0,0) ON CONFLICT(scope) DO UPDATE SET at_ms=excluded.at_ms,retry=0,counted_retry=0,generation=alarms.generation+1",params![scope,at_ms])?; Ok(()) })
    }

    pub fn retry_alarm(
        &mut self,
        scope: &str,
        generation: i64,
    ) -> Result<Committed<bool>, StorageError> {
        let (value,revision)=self.mutate_when(|tx| Ok(tx.execute("UPDATE alarms SET retry=retry+1,counted_retry=counted_retry+1 WHERE scope=?1 AND generation=?2",params![scope,generation])?>0))?;
        Ok(Committed { value, revision })
    }

    pub fn attachment(&self, id: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.connection
            .query_row(
                "SELECT bytes FROM ws_attachments WHERE connection_id=?1",
                [id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn set_attachment(
        &mut self,
        id: &str,
        bytes: &[u8],
    ) -> Result<StorageRevision, StorageError> {
        if bytes.len() > MAX_ATTACHMENT_BYTES {
            return Err(StorageError::AttachmentTooLarge {
                actual: bytes.len(),
                limit: MAX_ATTACHMENT_BYTES,
            });
        }
        self.mutate(|tx| { tx.execute("INSERT INTO ws_attachments(connection_id,bytes) VALUES(?1,?2) ON CONFLICT(connection_id) DO UPDATE SET bytes=excluded.bytes",params![id,bytes])?; Ok(()) })
    }

    pub fn delete_attachment(&mut self, id: &str) -> Result<StorageRevision, StorageError> {
        self.mutate(|tx| {
            tx.execute("DELETE FROM ws_attachments WHERE connection_id=?1", [id])?;
            Ok(())
        })
    }

    pub fn purge(&mut self) -> Result<StorageRevision, StorageError> {
        self.mutate(|tx| {
            tx.execute("DELETE FROM kv", [])?;
            tx.execute("DELETE FROM alarms", [])?;
            tx.execute("DELETE FROM cell_metadata", [])?;
            tx.execute("DELETE FROM ws_attachments", [])?;
            Ok(())
        })
    }

    pub fn sql(
        &mut self,
        query: &str,
        parameters: &[SqlValue],
    ) -> Result<Committed<SqlResult>, StorageError> {
        self.transaction(|transaction| transaction.sql(query, parameters))
    }

    pub fn apply(&mut self, source: &str) -> Result<StorageRevision, StorageError> {
        validate_sql(source, &[])?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(source)
            .map_err(|source| StorageError::Sql {
                query: "migration batch".to_string(),
                source,
            })?;
        let revision = advance_revision(&tx)?;
        tx.commit()?;
        self.revision = revision;
        Ok(revision)
    }

    pub fn transaction<T>(
        &mut self,
        operation: impl FnOnce(&StorageTransaction<'_>) -> Result<T, StorageError>,
    ) -> Result<Committed<T>, StorageError> {
        self.transaction_inner(operation, None::<(Uuid, fn(&T) -> Vec<u8>)>, Vec::new())
    }

    pub fn transaction_with_outcome<T>(
        &mut self,
        id: Uuid,
        operation: impl FnOnce(&StorageTransaction<'_>) -> Result<T, StorageError>,
        encode: impl FnOnce(&T) -> Vec<u8>,
    ) -> Result<Committed<T>, StorageError> {
        self.transaction_inner(operation, Some((id, encode)), Vec::new())
    }

    pub fn transaction_with_effects<T>(
        &mut self,
        operation: impl FnOnce(&StorageTransaction<'_>) -> Result<T, StorageError>,
        effects: Vec<EffectDraft>,
    ) -> Result<Committed<T>, StorageError> {
        self.transaction_inner(operation, None::<(Uuid, fn(&T) -> Vec<u8>)>, effects)
    }

    fn transaction_inner<T, F>(
        &mut self,
        operation: impl FnOnce(&StorageTransaction<'_>) -> Result<T, StorageError>,
        outcome: Option<(Uuid, F)>,
        effects: Vec<EffectDraft>,
    ) -> Result<Committed<T>, StorageError>
    where
        F: FnOnce(&T) -> Vec<u8>,
    {
        let sqlite = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (value, dirty) = {
            let transaction = StorageTransaction::new(&sqlite);
            let value = operation(&transaction)?;
            (value, transaction.is_dirty())
        };
        let revision = if dirty || outcome.is_some() || !effects.is_empty() {
            advance_revision(&sqlite)?
        } else {
            load_revision(&sqlite)?
        };
        if let Some((id, encode)) = outcome {
            let inserted = sqlite.execute(
                "INSERT OR IGNORE INTO mutation_outcomes(id,outcome,revision) VALUES(?1,?2,?3)",
                params![
                    id.to_string(),
                    encode(&value),
                    i64::try_from(revision.get()).map_err(|_| StorageError::RevisionOverflow)?
                ],
            )?;
            if inserted == 0 {
                return Err(StorageError::DuplicateMutation(id));
            }
        }
        for effect in effects {
            sqlite.execute(
                "INSERT OR IGNORE INTO effects(id,destination,inbox_key,payload,status,attempts,due_at_ms,revision) VALUES(?1,?2,?3,?4,?5,0,?6,?7)",
                params![
                    effect.id.to_string(),
                    effect.destination,
                    effect.inbox_key,
                    effect.payload,
                    EffectStatus::Pending.as_str(),
                    effect.due_at_ms,
                    i64::try_from(revision.get()).map_err(|_| StorageError::RevisionOverflow)?
                ],
            )?;
        }
        sqlite.commit()?;
        self.revision = revision;
        Ok(Committed {
            value,
            revision: self.revision,
        })
    }

    pub fn mutation_outcome(&self, id: Uuid) -> Result<Option<MutationRecord>, StorageError> {
        self.connection
            .query_row(
                "SELECT outcome,revision FROM mutation_outcomes WHERE id=?1",
                [id.to_string()],
                |row| {
                    let revision = u64::try_from(row.get::<_, i64>(1)?)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(1, 0))?;
                    Ok(MutationRecord {
                        id,
                        outcome: row.get(0)?,
                        revision: StorageRevision::new(revision),
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    fn mutate(
        &mut self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<(), StorageError>,
    ) -> Result<StorageRevision, StorageError> {
        self.mutate_value(operation).map(|result| result.1)
    }

    fn mutate_value<T>(
        &mut self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<T, StorageError>,
    ) -> Result<(T, StorageRevision), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let value = operation(&tx)?;
        let revision = advance_revision(&tx)?;
        tx.commit()?;
        self.revision = revision;
        Ok((value, self.revision))
    }

    fn mutate_when(
        &mut self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<bool, StorageError>,
    ) -> Result<(bool, StorageRevision), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = operation(&tx)?;
        let revision = if changed {
            advance_revision(&tx)?
        } else {
            load_revision(&tx)?
        };
        tx.commit()?;
        self.revision = revision;
        Ok((changed, self.revision))
    }

    pub fn replica(&self, offset: u64) -> Result<ReplicaBytes, StorageError> {
        replica::replica(&self.path, offset)
    }

    pub fn checkpoint(&mut self) -> Result<CheckpointBytes, StorageError> {
        let busy = self
            .connection
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                row.get::<_, i64>(0)
            })?;
        if busy != 0 {
            return Err(StorageError::CheckpointBusy);
        }
        replica::checkpoint(&self.path, self.revision)
    }

    #[must_use]
    pub const fn revision(&self) -> StorageRevision {
        self.revision
    }

    #[must_use]
    pub fn satisfies(&self, receipt: DurabilityReceipt, cell: CellId) -> bool {
        receipt.is_satisfied_by(cell, self.revision)
    }
}
