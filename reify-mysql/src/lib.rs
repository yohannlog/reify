//! MySQL / MariaDB adapter for Reify.
//!
//! Uses [`mysql_async::Opts`] (via [`mysql_async::OptsBuilder`]) directly for
//! full configuration control (host, port, SSL, pool sizing, compression, etc.).
//!
//! ```ignore
//! use reify_mysql::MysqlDb;
//! use mysql_async::{OptsBuilder, PoolOpts, PoolConstraints};
//!
//! let opts = OptsBuilder::default()
//!     .ip_or_hostname("localhost")
//!     .tcp_port(3306)
//!     .user(Some("app"))
//!     .pass(Some("secret"))
//!     .db_name(Some("mydb"))
//!     .pool_opts(PoolOpts::default()
//!         .with_constraints(PoolConstraints::new(2, 20).unwrap()));
//!
//! let db = MysqlDb::connect(opts).await?;
//! let rows = reify_core::fetch_all(&db, &User::find().filter(User::id.eq(1i64))).await?;
//! ```

pub use mysql_async::{self, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts};

use mysql_async::prelude::*;
use tracing::{debug, error};

use reify_core::adapter::{
    SavepointCounter, rewrite_double_quoted_idents_to_backticks, rewrite_placeholders_to_question,
};
use reify_core::db::{Database, DbError, Row, TransactionFn};
use reify_core::value::Value;

/// MySQL / MariaDB database backed by a `mysql_async` connection pool.
pub struct MysqlDb {
    pool: Pool,
}

impl MysqlDb {
    /// Connect to a MySQL / MariaDB database using [`mysql_async::Opts`].
    ///
    /// Accepts anything that converts into [`Opts`]: an [`OptsBuilder`], a URL
    /// string, or an `Opts` value directly. This gives you full control over
    /// every connection and pool parameter (host, port, SSL, pool sizing,
    /// compression, tcp_keepalive, statement cache, etc.).
    ///
    /// ```ignore
    /// use mysql_async::OptsBuilder;
    ///
    /// let opts = OptsBuilder::default()
    ///     .ip_or_hostname("localhost")
    ///     .user(Some("app"))
    ///     .db_name(Some("mydb"));
    ///
    /// let db = MysqlDb::connect(opts).await?;
    /// ```
    pub async fn connect(opts: impl Into<Opts>) -> Result<Self, DbError> {
        let mysql_opts: Opts = opts.into();
        debug!(
            target: "reify::mysql",
            host = mysql_opts.ip_or_hostname(),
            port = mysql_opts.tcp_port(),
            db   = mysql_opts.db_name().unwrap_or("<none>"),
            "Connecting to MySQL/MariaDB"
        );
        let pool = Pool::new(mysql_opts);
        // Eagerly verify connectivity — drop the connection back to the pool
        // rather than calling disconnect() which would destroy it.
        drop(
            pool.get_conn()
                .await
                .map_err(|e| DbError::Connection(e.to_string()))?,
        );
        Ok(Self { pool })
    }

    /// Build a `MysqlDb` from an already-constructed `mysql_async::Pool`.
    pub fn from_pool(pool: Pool) -> Self {
        Self { pool }
    }
}

// ── Value → mysql_async parameter conversion ────────────────────────

fn values_to_mysql_params(params: &[Value]) -> Result<mysql_async::Params, DbError> {
    if params.is_empty() {
        return Ok(mysql_async::Params::Empty);
    }
    let vals: Vec<mysql_async::Value> = params
        .iter()
        .map(value_to_mysql)
        .collect::<Result<_, _>>()?;
    Ok(mysql_async::Params::Positional(vals))
}

fn value_to_mysql(val: &Value) -> Result<mysql_async::Value, DbError> {
    Ok(match val {
        Value::Null => mysql_async::Value::NULL,
        Value::Bool(v) => mysql_async::Value::from(*v),
        Value::I16(v) => mysql_async::Value::from(*v),
        Value::I32(v) => mysql_async::Value::from(*v),
        Value::I64(v) => mysql_async::Value::from(*v),
        Value::F32(v) => mysql_async::Value::from(*v),
        Value::F64(v) => mysql_async::Value::from(*v),
        Value::String(v) => mysql_async::Value::from(v.as_str()),
        Value::Bytes(v) => mysql_async::Value::from(v.as_slice()),
        // Use chrono's native Display, which emits sub-second precision when
        // present. `mysql_async` then parses it back as a temporal value.
        Value::Timestamp(v) => mysql_async::Value::from(v.to_string()),
        Value::Date(v) => mysql_async::Value::from(v.to_string()),
        Value::Time(v) => mysql_async::Value::from(v.to_string()),
        // Any Value variant not handled above (e.g. PostgreSQL-only types
        // like Uuid, Timestamptz, Jsonb, range and array types) cannot be
        // bound as a MySQL parameter. Return a conversion error so a shared
        // PostgreSQL model used against MySQL fails cleanly instead of
        // panicking at runtime.
        #[allow(unreachable_patterns)]
        other => {
            return Err(DbError::Conversion(format!(
                "{other:?} cannot be bound as a MySQL parameter; \
                 use only Value variants supported by MySQL"
            )));
        }
    })
}

// ── mysql_async row → reify Row conversion ──────────────────────────

fn mysql_row_to_row(row: &mysql_async::Row) -> Row {
    let columns: Vec<String> = row
        .columns_ref()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect();

    let values: Vec<Value> = (0..columns.len())
        .map(|i| mysql_column_to_value(row, i))
        .collect();

    Row::new(columns, values)
}

fn mysql_column_to_value(row: &mysql_async::Row, idx: usize) -> Value {
    use mysql_async::Value as MV;

    // Access the raw mysql_async::Value to dispatch by type
    let raw: Option<&MV> = row.as_ref(idx);
    match raw {
        None | Some(MV::NULL) => Value::Null,
        Some(MV::Bytes(b)) => {
            // Try to interpret as UTF-8 string first
            if let Ok(s) = std::str::from_utf8(b) {
                // Try parsing as temporal types
                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    return Value::Timestamp(dt);
                }
                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
                    return Value::Timestamp(dt);
                }
                if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    return Value::Date(d);
                }
                if let Ok(t) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S") {
                    return Value::Time(t);
                }
                if let Ok(t) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f") {
                    return Value::Time(t);
                }
                Value::String(s.to_owned())
            } else {
                Value::Bytes(b.clone())
            }
        }
        Some(MV::Int(v)) => Value::I64(*v),
        Some(MV::UInt(v)) => {
            // MySQL UNSIGNED BIGINT can exceed i64::MAX. Return such values
            // as a decimal string (lossless) so callers can parse them as
            // u64, BigDecimal, etc. without silent clamping.
            // TODO: handle unsigned integer properly (determine type ?)
            if *v > i64::MAX as u64 {
                Value::String(v.to_string())
            } else {
                Value::I64(*v as i64)
            }
        }
        Some(MV::Float(v)) => Value::F32(*v),
        Some(MV::Double(v)) => Value::F64(*v),
        Some(MV::Date(year, month, day, hour, min, sec, micro)) => {
            let date = chrono::NaiveDate::from_ymd_opt(*year as i32, *month as u32, *day as u32);
            // DATE (no time component and no fractional seconds).
            if *hour == 0 && *min == 0 && *sec == 0 && *micro == 0 {
                if let Some(d) = date {
                    return Value::Date(d);
                }
            }
            // DATETIME(0..6) — preserve full microsecond precision.
            if let Some(d) = date {
                if let Some(t) = chrono::NaiveTime::from_hms_micro_opt(
                    *hour as u32,
                    *min as u32,
                    *sec as u32,
                    *micro,
                ) {
                    return Value::Timestamp(chrono::NaiveDateTime::new(d, t));
                }
            }
            Value::Null
        }
        Some(MV::Time(is_negative, _, hours, mins, secs, micro)) => {
            if *is_negative {
                tracing::warn!(
                    target: "reify::mysql",
                    "Negative TIME value is not representable as NaiveTime; returning Null"
                );
                return Value::Null;
            }
            // TIME(0..6) — preserve full microsecond precision.
            chrono::NaiveTime::from_hms_micro_opt(*hours as u32, *mins as u32, *secs as u32, *micro)
                .map(Value::Time)
                .unwrap_or(Value::Null)
        }
    }
}

// ── Error conversion helpers ─────────────────────────────────────────

/// MySQL server error codes that map to constraint violations.
const MYSQL_CONSTRAINT_CODES: &[u16] = &[
    1062, // ER_DUP_ENTRY (unique)
    1451, // ER_ROW_IS_REFERENCED_2 (FK parent)
    1452, // ER_NO_REFERENCED_ROW_2 (FK child)
    1048, // ER_BAD_NULL_ERROR (NOT NULL)
    3819, // ER_CHECK_CONSTRAINT_VIOLATED
];

/// Map a `mysql_async::Error` to a `DbError`, promoting constraint
/// violations to `DbError::Constraint` with a standardised SQLSTATE.
fn mysql_err(e: mysql_async::Error) -> DbError {
    match &e {
        mysql_async::Error::Server(server_err) => {
            if MYSQL_CONSTRAINT_CODES.contains(&server_err.code) {
                return DbError::Constraint {
                    message: server_err.message.clone(),
                    sqlstate: Some(server_err.state.clone()),
                };
            }
            DbError::Query(e.to_string())
        }
        // Driver-level protocol errors and I/O errors are connection failures,
        // not query failures — classify them accordingly.
        mysql_async::Error::Driver(_) | mysql_async::Error::Io(_) => {
            DbError::Connection(e.to_string())
        }
        _ => DbError::Query(e.to_string()),
    }
}

// ── SQL identifier rewriting ─────────────────────────────────────────

/// Rewrite SQL to MySQL dialect and marshal params in one step.
///
/// Both rewrites (`$N → ?` and `"ident" → `` `ident` ``) are string-literal
/// aware via [`reify_core::adapter`], so constants like `'$1'` or `'"abc"'`
/// survive intact.
fn prepare_mysql(sql: &str, params: &[Value]) -> Result<(String, mysql_async::Params), DbError> {
    let sql = rewrite_double_quoted_idents_to_backticks(&rewrite_placeholders_to_question(sql));
    let mysql_params = values_to_mysql_params(params)?;
    Ok((sql, mysql_params))
}

// ── Database trait implementation ───────────────────────────────────

impl Database for MysqlDb {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        let (sql, mysql_params) = prepare_mysql(sql, params)?;
        debug!(target: "reify::mysql", sql, "Executing");
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| DbError::Connection(e.to_string()))?;
        conn.exec_drop(sql, mysql_params).await.map_err(mysql_err)?;
        Ok(conn.affected_rows())
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let (sql, mysql_params) = prepare_mysql(sql, params)?;
        debug!(target: "reify::mysql", sql, "Querying");
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| DbError::Connection(e.to_string()))?;
        let rows: Vec<mysql_async::Row> = conn.exec(sql, mysql_params).await.map_err(mysql_err)?;
        Ok(rows.iter().map(mysql_row_to_row).collect())
    }

    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        let (sql, mysql_params) = prepare_mysql(sql, params)?;
        debug!(target: "reify::mysql", sql, "Querying one");
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| DbError::Connection(e.to_string()))?;
        let row: Option<mysql_async::Row> = conn
            .exec_first(sql, mysql_params)
            .await
            .map_err(mysql_err)?;
        match row {
            Some(r) => Ok(mysql_row_to_row(&r)),
            None => Err(DbError::RecordNotFound),
        }
    }

    /// # Streaming cursor
    ///
    /// Uses `mysql_async::Queryable::exec_iter` to pull rows from the server on
    /// demand via a background task that feeds a bounded channel. This avoids
    /// loading the entire result set into memory — unlike the default
    /// implementation, which delegates to `query` and buffers everything.
    ///
    /// # Connection lifecycle
    ///
    /// The producer task owns the pooled connection for the duration of the
    /// stream and returns it to the pool when the stream (or channel) is
    /// dropped. Dropping the stream early cancels the cursor cleanly.
    ///
    /// # Pool exhaustion warning
    ///
    /// Each live stream holds one connection. Drop it as soon as consumption
    /// is complete, or wrap it in `tokio::time::timeout` to bound the hold
    /// time. Never persist a stream in long-lived state.
    async fn query_stream<'a>(
        &'a self,
        sql: String,
        params: Vec<Value>,
    ) -> Result<reify_core::db::BoxStream<'a, Row>, DbError> {
        use futures_util::StreamExt;

        let (sql, mysql_params) = prepare_mysql(&sql, &params)?;
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| DbError::Connection(e.to_string()))?;
        debug!(target: "reify::mysql", sql, "Querying (stream)");

        // Small buffer keeps memory bounded while letting the producer stay
        // one row ahead of the consumer for pipelining.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Row, DbError>>(16);
        tokio::spawn(async move {
            let mut result = match conn.exec_iter(sql, mysql_params).await {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Err(mysql_err(e))).await;
                    return;
                }
            };
            loop {
                match result.next().await {
                    Ok(Some(row)) => {
                        if tx.send(Ok(mysql_row_to_row(&row))).await.is_err() {
                            // Receiver dropped — abort the cursor.
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx.send(Err(mysql_err(e))).await;
                        break;
                    }
                }
            }
            // `conn` drops here, returning it to the pool.
            drop(conn);
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx).boxed();
        Ok(stream)
    }

    async fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> Result<(), DbError> {
        debug!(target: "reify::mysql", "BEGIN transaction");
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| DbError::Connection(e.to_string()))?;
        conn.exec_drop("BEGIN", mysql_async::Params::Empty)
            .await
            .map_err(mysql_err)?;

        let txn = MysqlTransaction {
            conn: tokio::sync::Mutex::new(conn),
            savepoint_counter: SavepointCounter::new(),
        };

        match f(&txn).await {
            Ok(()) => {
                debug!(target: "reify::mysql", "COMMIT transaction");
                let mut conn = txn.conn.lock().await;
                conn.exec_drop("COMMIT", mysql_async::Params::Empty)
                    .await
                    .map_err(mysql_err)?;
                Ok(())
            }
            Err(e) => {
                error!(target: "reify::mysql", error = %e, "ROLLBACK transaction");
                let mut conn = txn.conn.lock().await;
                let _ = conn.exec_drop("ROLLBACK", mysql_async::Params::Empty).await;
                Err(e)
            }
        }
    }

    fn dialect(&self) -> reify_core::query::Dialect {
        reify_core::query::Dialect::Mysql
    }
}

// ── MysqlTransaction — dedicated connection for transaction scope ───

/// A single MySQL connection held open for the duration of a transaction.
///
/// Uses a `tokio::sync::Mutex` because `mysql_async::Conn` requires `&mut self`
/// for queries, but the `Database` trait takes `&self`. The tokio Mutex guard
/// is `Send`-safe across await points.
struct MysqlTransaction {
    conn: tokio::sync::Mutex<mysql_async::Conn>,
    /// Monotonically-increasing counter for generating unique SAVEPOINT names.
    /// Shared implementation lives in [`reify_core::adapter::SavepointCounter`].
    savepoint_counter: SavepointCounter,
}

impl Database for MysqlTransaction {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        let (sql, mysql_params) = prepare_mysql(sql, params)?;
        debug!(target: "reify::mysql", sql, "Executing (txn)");
        let mut conn = self.conn.lock().await;
        conn.exec_drop(sql, mysql_params).await.map_err(mysql_err)?;
        Ok(conn.affected_rows())
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let (sql, mysql_params) = prepare_mysql(sql, params)?;
        debug!(target: "reify::mysql", sql, "Querying (txn)");
        let mut conn = self.conn.lock().await;
        let rows: Vec<mysql_async::Row> = conn.exec(sql, mysql_params).await.map_err(mysql_err)?;
        Ok(rows.iter().map(mysql_row_to_row).collect())
    }

    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        let (sql, mysql_params) = prepare_mysql(sql, params)?;
        debug!(target: "reify::mysql", sql, "Querying one (txn)");
        let mut conn = self.conn.lock().await;
        let row: Option<mysql_async::Row> = conn
            .exec_first(sql, mysql_params)
            .await
            .map_err(mysql_err)?;
        match row {
            Some(r) => Ok(mysql_row_to_row(&r)),
            None => Err(DbError::RecordNotFound),
        }
    }

    async fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> Result<(), DbError> {
        // Nested transaction via SAVEPOINT. `SavepointCounter` guarantees a
        // distinct name for every call on this connection.
        let sp_name = self.savepoint_counter.next_name();
        debug!(target: "reify::mysql", savepoint = %sp_name, "SAVEPOINT (nested)");
        {
            let mut conn = self.conn.lock().await;
            conn.exec_drop(format!("SAVEPOINT {sp_name}"), mysql_async::Params::Empty)
                .await
                .map_err(mysql_err)?;
        }
        match f(self).await {
            Ok(()) => {
                let mut conn = self.conn.lock().await;
                conn.exec_drop(
                    format!("RELEASE SAVEPOINT {sp_name}"),
                    mysql_async::Params::Empty,
                )
                .await
                .map_err(mysql_err)?;
                Ok(())
            }
            Err(e) => {
                let mut conn = self.conn.lock().await;
                let _ = conn
                    .exec_drop(
                        format!("ROLLBACK TO SAVEPOINT {sp_name}"),
                        mysql_async::Params::Empty,
                    )
                    .await;
                Err(e)
            }
        }
    }
}
