use peren_storage::{MAX_SQL_COLUMNS, SqlResult, SqlStore, SqlValue, validate_sql};
use turso_parser::{ast, parser::Parser};

use crate::{SessionState, TursoError, TursoStore};

impl SqlStore for TursoStore {
    async fn sql(
        &mut self,
        query: &str,
        parameters: &[SqlValue],
    ) -> Result<SqlResult, Self::Error> {
        self.ensure_reconciled()?;
        let dirty = self.require_session()?;
        validate_sql(query, parameters)?;
        let mutates = sql_mutates(query)?;
        let values = parameters.iter().map(to_turso).collect::<Vec<_>>();
        let mut statement = self.connection.prepare(query).await?;
        let mut rows = statement.query(turso::params_from_iter(values)).await?;
        let columns = rows.column_names();
        let width = columns.len();
        if width > MAX_SQL_COLUMNS {
            return Err(peren_storage::SqlLimitError::Columns {
                actual: width,
                limit: MAX_SQL_COLUMNS,
            }
            .into());
        }
        let mut result_rows = Vec::new();
        while let Some(row) = rows.next().await? {
            let mut values = Vec::with_capacity(width);
            for index in 0..width {
                values.push(from_turso(row.get_value(index)?));
            }
            result_rows.push(values);
        }
        drop(rows);
        let changes = if mutates { statement.n_change() } else { 0 };
        self.session = SessionState::Open {
            dirty: dirty || mutates,
        };
        Ok(SqlResult {
            columns,
            rows: result_rows,
            changes,
            last_insert_rowid: self.connection.last_insert_rowid(),
        })
    }
}

fn sql_mutates(query: &str) -> Result<bool, TursoError> {
    let mut parser = Parser::new(query.as_bytes());
    let command = parser
        .next_cmd()
        .map_err(|error| TursoError::SqlSyntax(error.to_string()))?
        .ok_or(TursoError::EmptySql)?;
    if parser
        .next_cmd()
        .map_err(|error| TursoError::SqlSyntax(error.to_string()))?
        .is_some()
    {
        return Err(TursoError::MultipleSql);
    }
    match command {
        ast::Cmd::Explain(_)
        | ast::Cmd::ExplainQueryPlan { .. }
        | ast::Cmd::Stmt(ast::Stmt::Select(_)) => Ok(false),
        ast::Cmd::Stmt(
            ast::Stmt::Begin { .. }
            | ast::Stmt::Commit { .. }
            | ast::Stmt::Rollback { .. }
            | ast::Stmt::Savepoint { .. }
            | ast::Stmt::Release { .. },
        ) => Err(TursoError::TransactionSql),
        ast::Cmd::Stmt(_) => Ok(true),
    }
}

fn to_turso(value: &SqlValue) -> turso::Value {
    match value {
        SqlValue::Null => turso::Value::Null,
        SqlValue::Integer(value) => turso::Value::Integer(*value),
        SqlValue::Real(value) => turso::Value::Real(*value),
        SqlValue::Text(value) => turso::Value::Text(value.clone()),
        SqlValue::Blob(value) => turso::Value::Blob(value.clone()),
    }
}

fn from_turso(value: turso::Value) -> SqlValue {
    match value {
        turso::Value::Null => SqlValue::Null,
        turso::Value::Integer(value) => SqlValue::Integer(value),
        turso::Value::Real(value) => SqlValue::Real(value),
        turso::Value::Text(value) => SqlValue::Text(value),
        turso::Value::Blob(value) => SqlValue::Blob(value),
    }
}
