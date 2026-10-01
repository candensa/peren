use std::{path::Path, time::Duration};

use peren_primitives::StorageRevision;
use rusqlite::{Connection, OpenFlags, functions::FunctionFlags, limits::Limit};

use crate::{StorageError, schema, vector};

const MAX_SQL_VALUE_BYTES: i32 = 2 * 1024 * 1024;
const MAX_SQL_LENGTH: i32 = 100 * 1024;

pub(crate) fn open(path: &Path) -> Result<(Connection, StorageRevision), StorageError> {
    let connection = Connection::open(path)?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    connection.pragma_update(None, "wal_autocheckpoint", 0)?;
    connection.pragma_update(None, "foreign_keys", true)?;
    connection.execute_batch(schema::SCHEMA)?;
    configure(&connection)?;
    let revision = load_revision(&connection)?;
    Ok((connection, revision))
}

pub(crate) fn open_read_only(path: &Path) -> Result<(Connection, StorageRevision), StorageError> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    configure(&connection)?;
    let revision = load_revision(&connection)?;
    Ok((connection, revision))
}

pub(crate) fn load_revision(connection: &Connection) -> Result<StorageRevision, StorageError> {
    let stored = connection.query_row(
        "SELECT revision FROM storage_metadata WHERE id=1",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    let revision = u64::try_from(stored).map_err(|_| StorageError::RevisionOverflow)?;
    Ok(StorageRevision::new(revision))
}

pub(crate) fn advance_revision(connection: &Connection) -> Result<StorageRevision, StorageError> {
    let current = load_revision(connection)?.get();
    let next = current
        .checked_add(1)
        .ok_or(StorageError::RevisionOverflow)?;
    let stored = i64::try_from(next).map_err(|_| StorageError::RevisionOverflow)?;
    connection.execute(
        "UPDATE storage_metadata SET revision=?1 WHERE id=1",
        [stored],
    )?;
    Ok(StorageRevision::new(next))
}

fn configure(connection: &Connection) -> Result<(), StorageError> {
    connection.busy_timeout(Duration::ZERO)?;
    connection.create_scalar_function(
        "vector_distance",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        vector::distance,
    )?;
    for (limit, value) in [
        (Limit::SQLITE_LIMIT_LENGTH, MAX_SQL_VALUE_BYTES),
        (Limit::SQLITE_LIMIT_SQL_LENGTH, MAX_SQL_LENGTH),
        (Limit::SQLITE_LIMIT_COLUMN, 100),
        (Limit::SQLITE_LIMIT_FUNCTION_ARG, 32),
        (Limit::SQLITE_LIMIT_LIKE_PATTERN_LENGTH, 50),
        (Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 100),
    ] {
        connection.set_limit(limit, value)?;
    }
    Ok(())
}
