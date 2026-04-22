use std::sync::{Arc, Mutex};

use reify_core::adapter::SavepointCounter;
use reify_core::db::{Database, DbError, Row, TransactionFn};
use reify_core::value::Value;

// ── SqliteDb ────────────────────────────────────────────────────────

/// SQLite database adapter backed by rusqlite.
///
/// Uses a `std::sync::Mutex` for per-query connection serialization and
/// a `tokio::sync::Mutex<()>` as a transaction lock to prevent other
/// tasks from interleaving queries during a transaction.
///
/// # Concurrency model & contention
///
/// A single `SqliteDb` holds **one** `Arc<Mutex<Connection>>`. Every
/// `execute` / `query` call serialises on that mutex, so concurrent tasks
/// effectively run one-at-a-time against the database. This is safe but
/// becomes a bottleneck under load:
///
/// - Heavy read concurrency does **not** scale on a single `SqliteDb`
///   instance — queries queue behind the mutex even though SQLite's WAL
///   mode supports multiple readers.
/// - For workloads that need parallel reads, either:
///   1. Create multiple `SqliteDb` instances (each opens its own connection
///      to the same file; WAL mode lets them read concurrently), or
///   2. Wrap a connection pool (e.g. `r2d2_sqlite`) and expose one `Database`
///      adapter per pooled connection.
/// - For write-heavy workloads a single connection is typically fine, since
///   SQLite serialises writes anyway.
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

/// Default `busy_timeout` applied to file-backed connections (5 s).
///
/// Gives concurrent writers a reasonable window to wait for the write lock
/// before returning `SQLITE_BUSY`. Override with [`SqliteOptions`].
pub const DEFAULT_BUSY_TIMEOUT_MS: u32 = 5_000;

/// Tunables applied to a freshly-opened SQLite connection.
///
/// The defaults match what a typical application server wants: WAL journal,
/// FK enforcement, `synchronous=NORMAL` (fsync on WAL checkpoints only, the
/// recommended setting for WAL mode), and a 5-second `busy_timeout`.
///
/// ```no_run
/// use reify_sqlite::{SqliteDb, SqliteOptions};
/// # fn run() -> Result<(), reify_core::db::DbError> {
/// let db = SqliteDb::open_with("app.db", SqliteOptions {
///     busy_timeout_ms: Some(30_000),
///     synchronous_normal: true,
///     ..Default::default()
/// })?;
/// # let _ = db; Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct SqliteOptions {
    /// `PRAGMA busy_timeout = N;` — milliseconds the driver will wait for
    /// the write lock. `None` disables the pragma (default-SQLite behaviour
    /// of returning `SQLITE_BUSY` immediately).
    pub busy_timeout_ms: Option<u32>,
    /// When `true`, issue `PRAGMA synchronous = NORMAL;`. Recommended with
    /// WAL mode: fsync happens at checkpoint time instead of every commit,
    /// boosting throughput with essentially-identical durability guarantees.
    pub synchronous_normal: bool,
    /// Enable FK constraint enforcement (`PRAGMA foreign_keys = ON;`).
    pub foreign_keys: bool,
    /// Enable WAL journal mode (`PRAGMA journal_mode = WAL;`). Ignored for
    /// in-memory databases.
    pub wal: bool,
}

impl Default for SqliteOptions {
    fn default() -> Self {
        Self {
            busy_timeout_ms: Some(DEFAULT_BUSY_TIMEOUT_MS),
            synchronous_normal: true,
            foreign_keys: true,
            wal: true,
        }
    }
}

/// Apply the configured pragmas to a freshly-opened connection.
fn apply_pragmas(conn: &rusqlite::Connection, opts: &SqliteOptions) -> Result<(), DbError> {
    let mut sql = String::new();
    if opts.wal {
        sql.push_str("PRAGMA journal_mode=WAL; ");
    }
    if opts.foreign_keys {
        sql.push_str("PRAGMA foreign_keys=ON; ");
    }
    if opts.synchronous_normal {
        sql.push_str("PRAGMA synchronous=NORMAL; ");
    }
    if let Some(ms) = opts.busy_timeout_ms {
        use std::fmt::Write;
        let _ = write!(sql, "PRAGMA busy_timeout={ms}; ");
    }
    if sql.is_empty() {
        return Ok(());
    }
    conn.execute_batch(&sql)
        .map_err(|e| DbError::Connection(e.to_string()))
}

impl SqliteDb {
    /// Open a file-based SQLite database with default tunables.
    ///
    /// Equivalent to `open_with(path, SqliteOptions::default())`.
    pub fn open(path: &str) -> Result<Self, DbError> {
        Self::open_with(path, SqliteOptions::default())
    }

    /// Open a file-based SQLite database with custom [`SqliteOptions`].
    pub fn open_with(path: &str, opts: SqliteOptions) -> Result<Self, DbError> {
        let conn =
            rusqlite::Connection::open(path).map_err(|e| DbError::Connection(e.to_string()))?;
        apply_pragmas(&conn, &opts)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            txn_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Open an in-memory SQLite database.
    ///
    /// Each call creates an **independent** database — two `open_in_memory()`
    /// instances do not share data. Applies default pragmas minus WAL (WAL is
    /// not applicable to `:memory:` databases).
    pub fn open_in_memory() -> Result<Self, DbError> {
        Self::open_in_memory_with(SqliteOptions {
            wal: false,
            ..SqliteOptions::default()
        })
    }

    /// Open an in-memory SQLite database with custom [`SqliteOptions`].
    /// The `wal` field is forced to `false` because WAL does not apply to
    /// `:memory:`.
    pub fn open_in_memory_with(mut opts: SqliteOptions) -> Result<Self, DbError> {
        opts.wal = false;
        let conn = rusqlite::Connection::open_in_memory()
            .map_err(|e| DbError::Connection(e.to_string()))?;
        apply_pragmas(&conn, &opts)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            txn_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn value_to_sqlite(v: &Value) -> Result<rusqlite::types::Value, DbError> {
    Ok(match v {
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
        // range and array types) cannot be bound to SQLite. Return a
        // conversion error so a shared PostgreSQL/MySQL model used against
        // SQLite fails cleanly instead of panicking at runtime.
        #[allow(unreachable_patterns)]
        other => {
            return Err(DbError::Conversion(format!(
                "{other:?} cannot be bound as a SQLite parameter; \
                 use only Value variants supported by SQLite"
            )));
        }
    })
}

fn values_to_sqlite(params: &[Value]) -> Result<Vec<rusqlite::types::Value>, DbError> {
    params.iter().map(value_to_sqlite).collect()
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
        let params = values_to_sqlite(params)?;
        let _guard = self.txn_lock.lock().await;
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        sqlite_spawn(conn, move |c| sqlite_execute(c, &sql, &params)).await
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let params = values_to_sqlite(params)?;
        let _guard = self.txn_lock.lock().await;
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        sqlite_spawn(conn, move |c| sqlite_query(c, &sql, &params)).await
    }

    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        let params = values_to_sqlite(params)?;
        let _guard = self.txn_lock.lock().await;
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
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
            savepoint_counter: SavepointCounter::new(),
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

    fn dialect(&self) -> reify_core::query::Dialect {
        reify_core::query::Dialect::Generic
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
    /// Shared implementation lives in [`reify_core::adapter::SavepointCounter`].
    savepoint_counter: SavepointCounter,
}

impl Database for SqliteTransaction {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        let params = values_to_sqlite(params)?;
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        sqlite_spawn(conn, move |c| sqlite_execute(c, &sql, &params)).await
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let params = values_to_sqlite(params)?;
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        sqlite_spawn(conn, move |c| sqlite_query(c, &sql, &params)).await
    }

    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        let params = values_to_sqlite(params)?;
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        sqlite_spawn(conn, move |c| sqlite_query_one(c, &sql, &params)).await
    }

    async fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> Result<(), DbError> {
        // Nested transaction via SAVEPOINT. `SavepointCounter` guarantees a
        // distinct name for every call. The name is moved into each closure
        // instead of being cloned per arm — one allocation total.
        let sp_name = self.savepoint_counter.next_name();
        tracing::debug!(target: "reify::sqlite", savepoint = %sp_name, "SAVEPOINT (nested)");
        let begin_sql = format!("SAVEPOINT {sp_name}");
        sqlite_spawn(Arc::clone(&self.conn), move |c| {
            sqlite_execute(c, &begin_sql, &[])
        })
        .await?;

        match f(self).await {
            Ok(()) => {
                let release_sql = format!("RELEASE SAVEPOINT {sp_name}");
                sqlite_spawn(Arc::clone(&self.conn), move |c| {
                    sqlite_execute(c, &release_sql, &[])
                })
                .await
                .map(|_| ())
            }
            Err(e) => {
                let rollback_sql = format!("ROLLBACK TO SAVEPOINT {sp_name}");
                let _ = sqlite_spawn(Arc::clone(&self.conn), move |c| {
                    sqlite_execute(c, &rollback_sql, &[])
                })
                .await;
                Err(e)
            }
        }
    }
}
