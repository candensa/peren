use std::cell::Cell;

use rusqlite::{Connection, params};

use crate::{ListOptions, ListPage, SqlResult, SqlValue, StorageError, kv, sql};

pub struct StorageTransaction<'a> {
    connection: &'a Connection,
    dirty: Cell<bool>,
}

impl<'a> StorageTransaction<'a> {
    pub(crate) fn new(connection: &'a Connection) -> Self {
        Self {
            connection,
            dirty: Cell::new(false),
        }
    }

    pub fn get(&self, scope: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        kv::get(self.connection, scope, key)
    }

    pub fn put(&self, scope: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        kv::validate(key, value)?;
        self.connection.execute(
            "INSERT INTO kv(scope,k,v) VALUES(?1,?2,?3) ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
            params![scope, key, value],
        )?;
        self.dirty.set(true);
        Ok(())
    }

    pub fn delete(&self, scope: &str, key: &[u8]) -> Result<bool, StorageError> {
        let deleted = self.connection.execute(
            "DELETE FROM kv WHERE scope=?1 AND k=?2",
            params![scope, key],
        )? > 0;
        self.dirty.set(self.dirty.get() || deleted);
        Ok(deleted)
    }

    pub fn list(&self, scope: &str, options: &ListOptions<'_>) -> Result<ListPage, StorageError> {
        kv::list(self.connection, scope, options)
    }

    pub fn sql(&self, query: &str, parameters: &[SqlValue]) -> Result<SqlResult, StorageError> {
        let result = sql::run(self.connection, query, parameters)?;
        self.dirty.set(self.dirty.get() || result.mutates);
        Ok(result.result)
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty.get()
    }
}
