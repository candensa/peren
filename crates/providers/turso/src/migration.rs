use std::{
    fs::{self, OpenOptions},
    path::Path,
};

use fs2::FileExt;
use rusqlite::{Connection, MAIN_DB, OpenFlags, types::ValueRef};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub struct MigrationReport {
    schema: [u8; 32],
    rows: [u8; 32],
    tables: usize,
}

impl MigrationReport {
    #[must_use]
    pub const fn schema(&self) -> &[u8; 32] {
        &self.schema
    }

    #[must_use]
    pub const fn rows(&self) -> &[u8; 32] {
        &self.rows
    }

    #[must_use]
    pub const fn tables(&self) -> usize {
        self.tables
    }
}

pub fn migrate(source: &Path, destination: &Path) -> Result<MigrationReport, MigrationError> {
    let lock_path = destination.with_extension("migration.lock");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)?;
    lock.try_lock_exclusive()
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::WouldBlock => MigrationError::InProgress,
            _ => MigrationError::Io(error),
        })?;
    let source = Connection::open_with_flags(source, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let expected = fingerprint(&source)?;
    let staging = destination.with_extension("migrating");
    if destination.exists() {
        let destination_db =
            Connection::open_with_flags(destination, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let actual = fingerprint(&destination_db)?;
        if actual.schema == expected.schema && actual.rows == expected.rows {
            remove_if_present(&staging)?;
            return Ok(actual);
        }
        return Err(MigrationError::DestinationExists);
    }

    remove_if_present(&staging)?;
    source.backup(MAIN_DB, &staging, None)?;
    let staged = Connection::open_with_flags(&staging, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let actual = fingerprint(&staged)?;
    if actual.schema != expected.schema || actual.rows != expected.rows {
        return Err(MigrationError::Verification);
    }
    drop(staged);
    fs::hard_link(&staging, destination).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            MigrationError::DestinationExists
        } else {
            MigrationError::Io(error)
        }
    })?;
    fs::remove_file(staging)?;
    Ok(actual)
}

pub fn validate_remote(path: &Path) -> Result<(), MigrationError> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_schema WHERE sql IS NOT NULL AND instr(upper(sql), 'WITHOUT ROWID') > 0 ORDER BY name",
    )?;
    let unsupported = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .next()
        .transpose()?;
    unsupported.map_or(Ok(()), |object| {
        Err(MigrationError::UnsupportedRemoteSchema { object })
    })
}

fn fingerprint(connection: &Connection) -> Result<MigrationReport, MigrationError> {
    let integrity: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err(MigrationError::Integrity);
    }

    let mut schema = Sha256::new();
    let mut tables = Vec::new();
    let mut statement = connection.prepare(
        "SELECT type, name, COALESCE(sql, '') FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
    )?;
    let objects = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    for object in objects {
        let (kind, name, sql) = object?;
        hash_bytes(&mut schema, kind.as_bytes());
        hash_bytes(&mut schema, name.as_bytes());
        hash_bytes(&mut schema, sql.as_bytes());
        if kind == "table" {
            tables.push(name);
        }
    }

    let mut rows = Sha256::new();
    for table in &tables {
        hash_bytes(&mut rows, table.as_bytes());
        let quoted = table.replace('"', "\"\"");
        let mut statement = connection.prepare(&format!("SELECT * FROM \"{quoted}\""))?;
        let columns = statement.column_count();
        let mut query = statement.query([])?;
        let mut digests = Vec::new();
        while let Some(row) = query.next()? {
            let mut digest = Sha256::new();
            for column in 0..columns {
                hash_value(&mut digest, row.get_ref(column)?);
            }
            digests.push(digest.finalize());
        }
        digests.sort_unstable();
        hash_bytes(&mut rows, &(digests.len() as u64).to_be_bytes());
        for digest in digests {
            rows.update(digest);
        }
    }
    Ok(MigrationReport {
        schema: schema.finalize().into(),
        rows: rows.finalize().into(),
        tables: tables.len(),
    })
}

fn hash_value(digest: &mut Sha256, value: ValueRef<'_>) {
    match value {
        ValueRef::Null => digest.update([0]),
        ValueRef::Integer(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        ValueRef::Real(value) => {
            digest.update([2]);
            digest.update(value.to_bits().to_be_bytes());
        }
        ValueRef::Text(value) => {
            digest.update([3]);
            hash_bytes(digest, value);
        }
        ValueRef::Blob(value) => {
            digest.update([4]);
            hash_bytes(digest, value);
        }
    }
}

fn hash_bytes(digest: &mut Sha256, bytes: &[u8]) {
    digest.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(bytes);
}

fn remove_if_present(path: &Path) -> Result<(), std::io::Error> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("database migration operation failed")]
    Database(#[from] rusqlite::Error),
    #[error("database migration file operation failed")]
    Io(#[from] std::io::Error),
    #[error("another migration is already running for this destination")]
    InProgress,
    #[error("destination exists with different schema or rows")]
    DestinationExists,
    #[error("source or destination failed SQLite integrity verification")]
    Integrity,
    #[error("staged migration does not match the source")]
    Verification,
    #[error("Turso remote sync does not support schema object `{object}` at the pinned version")]
    UnsupportedRemoteSchema { object: String },
}
