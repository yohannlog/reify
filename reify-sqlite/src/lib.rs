use std::sync::{Arc, Mutex};

use reify_core::db::{Database, DbError, Row, TransactionFn};
use reify_core::value::Value;

// ── SqliteDb ────────────────────────────────────────────────────────

/// SQLite database adapter backed by rusqlite.
pub struct SqliteDb {
    conn: Arc<Mutex<rusqlite::Connection>>,
}

impl SqliteDb {
    /// Open a file-based SQLite database.
    pub fn open(path: &str) -> Result<Self, DbError> {
        let conn = rusqlite::Connection::open(path)
            .map_err(|e| DbError::Connection(e.to_string()))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Open an in-memory SQLite database.
    pub fn open_in_memory() -> Result<Self, DbError> {
        let conn = rusqlite::Connection::open_in_memory()
            .map_err(|e| DbError::Connection(e.to_string()))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn value_to_sqlite(v: &Value) -> rusqlite::types::Value {
    match v {
        Value::Null => rusqlite::types::Value::Null,
        Value::Bool(b) => rusqlite::types::Value::Integer(*b as i64),
        Value::I16(i) => rusqlite::types::Value::Integer(*i as i64),
        Value::I32(i) => rusqlite::types::Value::Integer(*i as i64),
        Value::I64(i) => rusqlite::types::Value::Integer(*i),
        Value::F32(f) => rusqlite::types::Value::Real(*f as f64),
        Value::F64(f) => rusqlite::types::Value::Real(*f),
        Value::String(s) => rusqlite::types::Value::Text(s.clone()),
        Value::Bytes(b) => rusqlite::types::Value::Blob(b.clone()),
        #[allow(unreachable_patterns)]
        _ => rusqlite::types::Value::Null,
    }
}

fn map_rusqlite_err(e: rusqlite::Error) -> DbError {
    use reify_core::db::sqlstate;
    let msg = e.to_string();
    // SQLite doesn't expose SQLSTATE codes natively; map common constraint
    // error messages to the appropriate standard codes.
    if msg.contains("UNIQUE") {
        DbError::Constraint {
            message: msg,
            sqlstate: Some(sqlstate::UNIQUE_VIOLATION.to_owned()),
        }
    } else if msg.contains("FOREIGN KEY") {
        DbError::Constraint {
            message: msg,
            sqlstate: Some(sqlstate::FOREIGN_KEY_VIOLATION.to_owned()),
        }
    } else if msg.contains("NOT NULL") {
        DbError::Constraint {
            message: msg,
            sqlstate: Some(sqlstate::NOT_NULL_VIOLATION.to_owned()),
        }
    } else if msg.contains("CHECK") {
        DbError::Constraint {
            message: msg,
            sqlstate: Some(sqlstate::CHECK_VIOLATION.to_owned()),
        }
    } else {
        DbError::Query(msg)
    }
}

// ── Database impl ───────────────────────────────────────────────────

impl Database for SqliteDb {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        let params: Vec<rusqlite::types::Value> = params.iter().map(value_to_sqlite).collect();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| DbError::Other(e.to_string()))?;
            conn.execute(&sql, rusqlite::params_from_iter(params.iter()))
                .map(|n| n as u64)
                .map_err(map_rusqlite_err)
        })
        .await
        .map_err(|e| DbError::Other(e.to_string()))?
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        let params: Vec<rusqlite::types::Value> = params.iter().map(value_to_sqlite).collect();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| DbError::Other(e.to_string()))?;
            let mut stmt = conn.prepare(&sql).map_err(map_rusqlite_err)?;
            let col_names: Vec<String> = stmt
                .column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();
            let rows = stmt
                .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                    let values: Vec<rusqlite::types::Value> = (0..col_names.len())
                        .map(|i| row.get::<_, rusqlite::types::Value>(i))
                        .collect::<Result<_, _>>()?;
                    Ok(values)
                })
                .map_err(map_rusqlite_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_rusqlite_err)?;

            let result = rows
                .into_iter()
                .map(|raw_values| {
                    let values: Vec<Value> = raw_values
                        .into_iter()
                        .map(|v| match v {
                            rusqlite::types::Value::Null => Value::Null,
                            rusqlite::types::Value::Integer(i) => Value::I64(i),
                            rusqlite::types::Value::Real(f) => Value::F64(f),
                            rusqlite::types::Value::Text(s) => Value::String(s),
                            rusqlite::types::Value::Blob(b) => Value::Bytes(b),
                        })
                        .collect();
                    Row::new(col_names.clone(), values)
                })
                .collect();
            Ok(result)
        })
        .await
        .map_err(|e| DbError::Other(e.to_string()))?
    }

    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        let mut rows = Database::query(self, sql, params).await?;
        if rows.is_empty() {
            Err(DbError::Query("no rows".into()))
        } else {
            Ok(rows.remove(0))
        }
    }

    async fn transaction<'a>(
        &'a self,
        f: TransactionFn<'a>,
    ) -> Result<(), DbError> {
        Database::execute(self, "BEGIN", &[]).await?;
        match f(self).await {
            Ok(()) => {
                Database::execute(self, "COMMIT", &[]).await?;
                Ok(())
            }
            Err(e) => {
                let _ = Database::execute(self, "ROLLBACK", &[]).await;
                Err(e)
            }
        }
    }
}
