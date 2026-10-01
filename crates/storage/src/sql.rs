use rusqlite::{
    Connection, params_from_iter,
    types::{Type, Value, ValueRef},
};

use crate::StorageError;

pub const MAX_SQL_BYTES: usize = 100 * 1024;
pub const MAX_SQL_PARAMETERS: usize = 100;
pub const MAX_SQL_COLUMNS: usize = 100;

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum SqlLimitError {
    #[error("SQL statement uses {actual} bytes; the limit is {limit}")]
    Statement { actual: usize, limit: usize },
    #[error("SQL statement binds {actual} parameters; the limit is {limit}")]
    Parameters { actual: usize, limit: usize },
    #[error("SQL result has {actual} columns; the limit is {limit}")]
    Columns { actual: usize, limit: usize },
}

pub fn validate_sql(query: &str, parameters: &[SqlValue]) -> Result<(), SqlLimitError> {
    if query.len() > MAX_SQL_BYTES {
        return Err(SqlLimitError::Statement {
            actual: query.len(),
            limit: MAX_SQL_BYTES,
        });
    }
    if parameters.len() > MAX_SQL_PARAMETERS {
        return Err(SqlLimitError::Parameters {
            actual: parameters.len(),
            limit: MAX_SQL_PARAMETERS,
        });
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct SqlResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub changes: u64,
    pub last_insert_rowid: i64,
}

pub(crate) struct StatementResult {
    pub result: SqlResult,
    pub mutates: bool,
}

pub(crate) fn run(
    connection: &Connection,
    query: &str,
    parameters: &[SqlValue],
) -> Result<StatementResult, StorageError> {
    validate_sql(query, parameters)?;
    run_inner(connection, query, parameters).map_err(|source| StorageError::Sql {
        query: query.to_owned(),
        source,
    })
}

fn run_inner(
    connection: &Connection,
    query: &str,
    parameters: &[SqlValue],
) -> Result<StatementResult, rusqlite::Error> {
    let values = parameters.iter().map(to_sqlite).collect::<Vec<_>>();
    let mut statement = connection.prepare(query)?;
    let mutates = !statement.readonly();
    let columns = statement
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let width = columns.len();
    let mut rows = Vec::new();
    let mut cursor = statement.query(params_from_iter(values))?;
    while let Some(row) = cursor.next()? {
        let mut values = Vec::with_capacity(width);
        for index in 0..width {
            values.push(from_sqlite(index, row.get_ref(index)?)?);
        }
        rows.push(values);
    }
    drop(cursor);
    Ok(StatementResult {
        result: SqlResult {
            columns,
            rows,
            changes: if mutates { connection.changes() } else { 0 },
            last_insert_rowid: connection.last_insert_rowid(),
        },
        mutates,
    })
}

fn to_sqlite(value: &SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(value) => Value::Integer(*value),
        SqlValue::Real(value) => Value::Real(*value),
        SqlValue::Text(value) => Value::Text(value.clone()),
        SqlValue::Blob(value) => Value::Blob(value.clone()),
    }
}

fn from_sqlite(index: usize, value: ValueRef<'_>) -> Result<SqlValue, rusqlite::Error> {
    Ok(match value {
        ValueRef::Null => SqlValue::Null,
        ValueRef::Integer(value) => SqlValue::Integer(value),
        ValueRef::Real(value) => SqlValue::Real(value),
        ValueRef::Text(value) => SqlValue::Text(
            std::str::from_utf8(value)
                .map_err(|source| {
                    rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(source))
                })?
                .to_owned(),
        ),
        ValueRef::Blob(value) => SqlValue::Blob(value.to_vec()),
    })
}
