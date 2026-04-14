use std::sync::{Arc, Mutex};

use reify_core::db::{Database, DbError, Row, TransactionFn};
use reify_core::value::Value;

// ── SqliteDb ────────────────────────────────────────────────────────

/// SQLite database adapter backed by rusqlite.
///
/// Uses a `std::sync::Mutex` for per-query connection serialization and
/// a `tokio::sync::Mutex<()>` as a transaction lock to prevent other
/// tasks from interleaving queries during a transaction.
pub struct SqliteDb {
    conn: Arc<Mutex<rusqlite::Connection>>,
    /// Held for the entire duration of a transaction to prevent
    /// concurrent tasks from issuing queries that would run inside
    /// the open transaction.
    txn_lock: Arc<tokio::sync::Mutex<()>>,
}

impl SqliteDb {
    /// Open a file-based SQLite database.
    pub fn open(path: &str) -> Result<Self, DbError> {
        let conn =
            rusqlite::Connection::open(path).map_err(|e| DbError::Connection(e.to_string()))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            txn_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Open an in-memory SQLite database.
    pub fn open_in_memory() -> Result<Self, DbError> {
        let conn = rusqlite::Connection::open_in_memory()
            .map_err(|e| DbError::Connection(e.to_string()))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            txn_lock: Arc::new(tokio::sync::Mutex::new(())),
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
        // Any Value variant not handled above (e.g. temporal types from the
        // mysql/postgres feature, or PostgreSQL-only types like Uuid, Jsonb,
        // range and array types) cannot be silently converted to NULL.
        // Panic immediately with a clear message instead.
        #[allow(unreachable_patterns)]
        other => unreachable!(
            "{other:?} cannot be bound as a SQLite parameter; \
             use only Value variants supported by SQLite"
        ),
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

// ── Shared helpers for rusqlite operations ──────────────────────────

/// Run a blocking rusqlite closure on a pooled thread, mapping join errors to `DbError`.
async fn sqlite_spawn<F, T>(conn: Arc<Mutex<rusqlite::Connection>>, f: F) -> Result<T, DbError>
where
    F: FnOnce(&rusqlite::Connection) -> Result<T, DbError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let conn = conn.lock().map_err(|e| DbError::Other(e.to_string()))?;
        f(&conn)
    })
    .await
    .map_err(|e| DbError::Other(e.to_string()))?
}

/// Execute a statement on a locked connection. Returns rows affected.
fn sqlite_execute(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[rusqlite::types::Value],
) -> Result<u64, DbError> {
    conn.execute(sql, rusqlite::params_from_iter(params.iter()))
        .map(|n| n as u64)
        .map_err(map_rusqlite_err)
}

/// Run a query on a locked connection. Returns rows.
fn sqlite_query(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[rusqlite::types::Value],
) -> Result<Vec<Row>, DbError> {
    let mut stmt = conn.prepare(sql).map_err(map_rusqlite_err)?;
    let col_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
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
}

// ── Database impl ───────────────────────────────────────────────────

impl Database for SqliteDb {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        let _guard = self.txn_lock.lock().await;
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        let params: Vec<rusqlite::types::Value> = params.iter().map(value_to_sqlite).collect();
        sqlite_spawn(conn, move |c| sqlite_execute(c, &sql, &params)).await
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let _guard = self.txn_lock.lock().await;
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        let params: Vec<rusqlite::types::Value> = params.iter().map(value_to_sqlite).collect();
        sqlite_spawn(conn, move |c| sqlite_query(c, &sql, &params)).await
    }

    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        let mut rows = Database::query(self, sql, params).await?;
        if rows.is_empty() {
            Err(DbError::Query("no rows".into()))
        } else {
            Ok(rows.remove(0))
        }
    }

    async fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> Result<(), DbError> {
        // Hold the transaction lock for the entire duration, preventing
        // other tasks from issuing queries that would interleave with
        // this transaction.
        let _txn_guard = self.txn_lock.lock().await;

        sqlite_spawn(Arc::clone(&self.conn), |c| sqlite_execute(c, "BEGIN", &[])).await?;

        let txn = SqliteTransaction {
            conn: Arc::clone(&self.conn),
        };

        match f(&txn).await {
            Ok(()) => sqlite_spawn(Arc::clone(&txn.conn), |c| sqlite_execute(c, "COMMIT", &[]))
                .await
                .map(|_| ()),
            Err(e) => {
                let _ = sqlite_spawn(Arc::clone(&txn.conn), |c| {
                    sqlite_execute(c, "ROLLBACK", &[])
                })
                .await;
                Err(e)
            }
        }
    }
}

// ── SqliteTransaction — dedicated wrapper for transaction scope ─────

/// A transaction wrapper that uses the same `Arc<Mutex<Connection>>` as
/// `SqliteDb`. Isolation is guaranteed by the `txn_lock` held by the
/// enclosing `SqliteDb::transaction()` call — no other task can acquire
/// it until the transaction completes.
struct SqliteTransaction {
    conn: Arc<Mutex<rusqlite::Connection>>,
}

impl Database for SqliteTransaction {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        let params: Vec<rusqlite::types::Value> = params.iter().map(value_to_sqlite).collect();
        sqlite_spawn(conn, move |c| sqlite_execute(c, &sql, &params)).await
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        let params: Vec<rusqlite::types::Value> = params.iter().map(value_to_sqlite).collect();
        sqlite_spawn(conn, move |c| sqlite_query(c, &sql, &params)).await
    }

    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        let mut rows = Database::query(self, sql, params).await?;
        if rows.is_empty() {
            Err(DbError::Query("no rows".into()))
        } else {
            Ok(rows.remove(0))
        }
    }

    async fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> Result<(), DbError> {
        // Nested transaction via SAVEPOINT
        sqlite_spawn(Arc::clone(&self.conn), |c| {
            sqlite_execute(c, "SAVEPOINT nested_txn", &[])
        })
        .await?;

        match f(self).await {
            Ok(()) => sqlite_spawn(Arc::clone(&self.conn), |c| {
                sqlite_execute(c, "RELEASE SAVEPOINT nested_txn", &[])
            })
            .await
            .map(|_| ()),
            Err(e) => {
                let _ = sqlite_spawn(Arc::clone(&self.conn), |c| {
                    sqlite_execute(c, "ROLLBACK TO SAVEPOINT nested_txn", &[])
                })
                .await;
                Err(e)
            }
        }
    }
}
