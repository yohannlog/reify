use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use reify_core::db::{Database, DbError, Row, TransactionFn};
use reify_core::value::Value;

// ── SqliteDb ────────────────────────────────────────────────────────

/// SQLite database adapter backed by rusqlite.
///
/// Uses a `std::sync::Mutex` for per-query connection serialization and
/// a `tokio::sync::Mutex<()>` as a transaction lock to prevent other
/// tasks from interleaving queries during a transaction.
///
/// # Important: use the transaction handle, not `self`
///
/// Inside a `transaction()` closure, **always** issue queries through the
/// `tx: &dyn DynDatabase` argument — never through the outer `SqliteDb`.
/// Calling `db.execute()` from within the closure would attempt to re-acquire
/// `txn_lock`, which is already held by the transaction, causing a deadlock.
pub struct SqliteDb {
    conn: Arc<Mutex<rusqlite::Connection>>,
    /// Held for the entire duration of a transaction to prevent
    /// concurrent tasks from issuing queries that would run inside
    /// the open transaction.
    txn_lock: Arc<tokio::sync::Mutex<()>>,
}

/// Apply recommended PRAGMAs to a freshly-opened connection.
///
/// - `journal_mode=WAL`: allows concurrent readers during writes.
/// - `foreign_keys=ON`: enforces FK constraints (disabled by default in SQLite).
fn apply_pragmas(conn: &rusqlite::Connection) -> Result<(), DbError> {
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
        .map_err(|e| DbError::Connection(e.to_string()))
}

impl SqliteDb {
    /// Open a file-based SQLite database.
    ///
    /// Automatically enables WAL journal mode and foreign key enforcement.
    pub fn open(path: &str) -> Result<Self, DbError> {
        let conn =
            rusqlite::Connection::open(path).map_err(|e| DbError::Connection(e.to_string()))?;
        apply_pragmas(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            txn_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Open an in-memory SQLite database.
    ///
    /// Each call creates an **independent** database — two `open_in_memory()`
    /// instances do not share data. Automatically enables foreign key enforcement
    /// (WAL is not applicable to in-memory databases).
    pub fn open_in_memory() -> Result<Self, DbError> {
        let conn = rusqlite::Connection::open_in_memory()
            .map_err(|e| DbError::Connection(e.to_string()))?;
        // WAL is not applicable to in-memory databases; only enable FK enforcement.
        conn.execute_batch("PRAGMA foreign_keys=ON;")
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
    // Dispatch on the structured error code first (stable across rusqlite versions),
    // falling back to message-based detection only for CANTOPEN (connection error).
    if let rusqlite::Error::SqliteFailure(ref ffi_err, ref msg) = e {
        use rusqlite::ffi;
        let detail = msg.as_deref().unwrap_or("");
        match ffi_err.extended_code {
            ffi::SQLITE_CONSTRAINT_UNIQUE | ffi::SQLITE_CONSTRAINT_PRIMARYKEY => {
                return DbError::Constraint {
                    message: detail.to_owned(),
                    sqlstate: Some(sqlstate::UNIQUE_VIOLATION.to_owned()),
                };
            }
            ffi::SQLITE_CONSTRAINT_FOREIGNKEY => {
                return DbError::Constraint {
                    message: detail.to_owned(),
                    sqlstate: Some(sqlstate::FOREIGN_KEY_VIOLATION.to_owned()),
                };
            }
            ffi::SQLITE_CONSTRAINT_NOTNULL => {
                return DbError::Constraint {
                    message: detail.to_owned(),
                    sqlstate: Some(sqlstate::NOT_NULL_VIOLATION.to_owned()),
                };
            }
            ffi::SQLITE_CONSTRAINT_CHECK => {
                return DbError::Constraint {
                    message: detail.to_owned(),
                    sqlstate: Some(sqlstate::CHECK_VIOLATION.to_owned()),
                };
            }
            _ => {}
        }
        // CANTOPEN and other connection-level errors.
        if ffi_err.code == ffi::ErrorCode::CannotOpen {
            return DbError::Connection(e.to_string());
        }
    }
    DbError::Query(e.to_string())
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
    .map_err(|e| {
        tracing::error!(target: "reify::sqlite", error = %e, "spawn_blocking task failed");
        DbError::Other(e.to_string())
    })?
}

/// Execute a statement on a locked connection. Returns rows affected.
fn sqlite_execute(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[rusqlite::types::Value],
) -> Result<u64, DbError> {
    tracing::debug!(target: "reify::sqlite", sql, "Executing");
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
    tracing::debug!(target: "reify::sqlite", sql, "Querying");
    let mut stmt = conn.prepare(sql).map_err(map_rusqlite_err)?;
    // Share column names across all rows via Arc to avoid N string-vec clones.
    let col_names: Arc<Vec<String>> =
        Arc::new(stmt.column_names().iter().map(|s| s.to_string()).collect());
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
            // Arc::clone is O(1) — no string allocation per row.
            Row::new((*col_names).clone(), values)
        })
        .collect();
    Ok(result)
}

/// Run a query returning exactly one row, using `query_row` to avoid loading
/// the full result set into memory.
fn sqlite_query_one(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[rusqlite::types::Value],
) -> Result<Row, DbError> {
    tracing::debug!(target: "reify::sqlite", sql, "Querying one");
    let mut stmt = conn.prepare(sql).map_err(map_rusqlite_err)?;
    let col_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let n = col_names.len();
    stmt.query_row(rusqlite::params_from_iter(params.iter()), |row| {
        let values: Vec<rusqlite::types::Value> = (0..n)
            .map(|i| row.get::<_, rusqlite::types::Value>(i))
            .collect::<Result<_, _>>()?;
        Ok(values)
    })
    .map_err(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => DbError::RecordNotFound,
        other => map_rusqlite_err(other),
    })
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
        Row::new(col_names, values)
    })
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
        let _guard = self.txn_lock.lock().await;
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        let params: Vec<rusqlite::types::Value> = params.iter().map(value_to_sqlite).collect();
        sqlite_spawn(conn, move |c| sqlite_query_one(c, &sql, &params)).await
    }

    async fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> Result<(), DbError> {
        // Hold the transaction lock for the entire duration, preventing
        // other tasks from issuing queries that would interleave with
        // this transaction.
        let _txn_guard = self.txn_lock.lock().await;

        sqlite_spawn(Arc::clone(&self.conn), |c| sqlite_execute(c, "BEGIN", &[])).await?;

        let txn = SqliteTransaction {
            conn: Arc::clone(&self.conn),
            savepoint_counter: AtomicU64::new(0),
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
    /// Monotonically-increasing counter for unique SAVEPOINT names.
    /// Mirrors the same pattern used in `PgTransaction` and `MysqlTransaction`.
    savepoint_counter: AtomicU64,
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
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        let params: Vec<rusqlite::types::Value> = params.iter().map(value_to_sqlite).collect();
        sqlite_spawn(conn, move |c| sqlite_query_one(c, &sql, &params)).await
    }

    async fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> Result<(), DbError> {
        // Nested transaction via SAVEPOINT. The counter is incremented atomically
        // so every nested call gets a distinct name, preventing collisions on
        // recursive nesting.
        let n = self.savepoint_counter.fetch_add(1, Ordering::Relaxed);
        let sp_name = format!("sp_{n}");
        tracing::debug!(target: "reify::sqlite", savepoint = %sp_name, "SAVEPOINT (nested)");
        let sp = sp_name.clone();
        sqlite_spawn(Arc::clone(&self.conn), move |c| {
            sqlite_execute(c, &format!("SAVEPOINT {sp}"), &[])
        })
        .await?;

        match f(self).await {
            Ok(()) => {
                let sp = sp_name.clone();
                sqlite_spawn(Arc::clone(&self.conn), move |c| {
                    sqlite_execute(c, &format!("RELEASE SAVEPOINT {sp}"), &[])
                })
                .await
                .map(|_| ())
            }
            Err(e) => {
                let sp = sp_name.clone();
                let _ = sqlite_spawn(Arc::clone(&self.conn), move |c| {
                    sqlite_execute(c, &format!("ROLLBACK TO SAVEPOINT {sp}"), &[])
                })
                .await;
                Err(e)
            }
        }
    }
}
