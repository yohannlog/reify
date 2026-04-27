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

use mysql_async::consts::ColumnType;
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
    /// Maximum time to wait for a free connection. Pre-fix, pool exhaustion
    /// (e.g. a long-lived `query_stream` that doesn't drain) froze every
    /// subsequent query indefinitely with no observable error. With this
    /// guard a stalled pool surfaces as `DbError::Connection` after the
    /// configured deadline so callers can fail fast.
    acquire_timeout: std::time::Duration,
}

/// Default upper bound for [`MysqlDb::with_acquire_timeout`] (30 s).
pub const DEFAULT_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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
        Ok(Self {
            pool,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
        })
    }

    /// Build a `MysqlDb` from an already-constructed `mysql_async::Pool`.
    pub fn from_pool(pool: Pool) -> Self {
        Self {
            pool,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
        }
    }

    /// Override the maximum time spent waiting for a pooled connection.
    ///
    /// Defaults to [`DEFAULT_ACQUIRE_TIMEOUT`] (30 s).
    pub fn with_acquire_timeout(mut self, dur: std::time::Duration) -> Self {
        self.acquire_timeout = dur;
        self
    }

    /// Currently-configured pool acquisition timeout.
    pub fn acquire_timeout(&self) -> std::time::Duration {
        self.acquire_timeout
    }

    /// Acquire a pooled connection bounded by `acquire_timeout`. A stalled
    /// pool surfaces as `DbError::Connection` after the deadline instead of
    /// blocking the caller indefinitely.
    async fn get_conn(&self) -> Result<mysql_async::Conn, DbError> {
        match tokio::time::timeout(self.acquire_timeout, self.pool.get_conn()).await {
            Ok(Ok(conn)) => Ok(conn),
            Ok(Err(e)) => Err(DbError::Connection(e.to_string())),
            Err(_) => Err(DbError::Connection(format!(
                "pool acquisition timed out after {}ms; check for streams or \
                 transactions holding connections",
                self.acquire_timeout.as_millis()
            ))),
        }
    }

    /// Like [`Database::query_stream`] but every `next().await` is bounded
    /// by an inter-row idle timeout.
    ///
    /// If no row arrives within `idle`, a single
    /// [`reify_core::db::DbError::Timeout`] is yielded
    /// and the stream ends. Dropping the returned stream cancels the
    /// background producer task and returns the connection to the pool.
    ///
    /// Use this when streaming to a slow / external consumer (HTTP client,
    /// network bridge, …) to bound how long the connection stays out of
    /// the pool when the consumer stalls.
    pub async fn query_stream_idle<'a>(
        &'a self,
        sql: String,
        params: Vec<Value>,
        idle: std::time::Duration,
    ) -> Result<reify_core::db::BoxStream<'a, Row>, DbError> {
        let inner = <Self as Database>::query_stream(self, sql, params).await?;
        Ok(reify_core::db::with_idle_timeout(inner, idle))
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
        // mysql_async natively supports unsigned 64-bit, so the full
        // BIGINT UNSIGNED range round-trips losslessly.
        Value::U64(v) => mysql_async::Value::UInt(*v),
        Value::F32(v) => mysql_async::Value::from(*v),
        Value::F64(v) => mysql_async::Value::from(*v),
        Value::String(v) => mysql_async::Value::from(v.as_str()),
        Value::Bytes(v) => mysql_async::Value::from(v.as_slice()),
        // Use chrono's native Display, which emits sub-second precision when
        // present. `mysql_async` then parses it back as a temporal value.
        Value::Timestamp(v) => mysql_async::Value::from(v.to_string()),
        Value::Date(v) => mysql_async::Value::from(v.to_string()),
        Value::Time(v) => mysql_async::Value::from(v.to_string()),
        Value::Duration(d) => {
            // Bind via mysql_async's structured `Value::Time`, which the
            // driver serialises using the binary protocol's signed-time
            // layout. This preserves the full MySQL TIME range
            // (-838:59:59.999999 to +838:59:59.999999) including signs and
            // microseconds; values exceeding MySQL's range are clipped by
            // the server itself, matching the documented behaviour.
            let total_us = d.num_microseconds().unwrap_or(if d.num_seconds() >= 0 {
                i64::MAX
            } else {
                i64::MIN
            });
            let neg = total_us < 0;
            let abs = (total_us as i128).unsigned_abs();
            let micros = (abs % 1_000_000) as u32;
            let total_secs = abs / 1_000_000;
            let secs = (total_secs % 60) as u8;
            let mins = ((total_secs / 60) % 60) as u8;
            let hours_total = total_secs / 3600;
            let days = (hours_total / 24).min(u32::MAX as u128) as u32;
            let hours = (hours_total % 24) as u8;
            mysql_async::Value::Time(neg, days, hours, mins, secs, micros)
        }
        // Complex types — serialize as text for MySQL compatibility
        #[cfg(feature = "postgres")]
        Value::Point(p) => {
            // MySQL POINT via ST_GeomFromText
            mysql_async::Value::from(format!("POINT({} {})", p.x(), p.y()))
        }
        #[cfg(feature = "postgres")]
        Value::Inet(i) => mysql_async::Value::from(i.to_string()),
        #[cfg(feature = "postgres")]
        Value::Cidr(c) => mysql_async::Value::from(c.to_string()),
        #[cfg(feature = "postgres")]
        Value::MacAddr(m) => mysql_async::Value::from(m.to_string()),
        #[cfg(feature = "postgres")]
        Value::Interval(i) => mysql_async::Value::from(i.to_string()),
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

/// MySQL/MariaDB column types that carry a temporal value.
///
/// Used to gate temporal parsing on the `MV::Bytes` path: a VARCHAR column
/// whose textual content happens to look like `"2024-01-15"` must NOT be
/// silently coerced to `Value::Date` — that was a real type-confusion bug
/// before this gating was introduced.
fn is_temporal_column_type(t: ColumnType) -> bool {
    matches!(
        t,
        ColumnType::MYSQL_TYPE_DATE
            | ColumnType::MYSQL_TYPE_NEWDATE
            | ColumnType::MYSQL_TYPE_DATETIME
            | ColumnType::MYSQL_TYPE_DATETIME2
            | ColumnType::MYSQL_TYPE_TIMESTAMP
            | ColumnType::MYSQL_TYPE_TIMESTAMP2
            | ColumnType::MYSQL_TYPE_TIME
            | ColumnType::MYSQL_TYPE_TIME2
    )
}

/// Convert a raw `MV::Bytes` payload to a `Value`, using the column type to
/// decide whether and how to parse it as a temporal value.
///
/// Non-temporal columns always return `Value::String` (or `Value::Bytes` for
/// non-UTF-8 payloads). Temporal columns parse with the format that matches
/// their declared precision; on parse failure we log a warning and fall back
/// to `Value::String` rather than discarding the data.
///
/// `name` is a closure to keep column-name lookup off the hot path when
/// no warning is logged.
fn bytes_to_value(bytes: &[u8], col_type: ColumnType, name: impl FnOnce() -> String) -> Value {
    let s = match std::str::from_utf8(bytes) {
        Ok(s) => s,
        Err(_) => return Value::Bytes(bytes.to_vec()),
    };

    if !is_temporal_column_type(col_type) {
        return Value::String(s.to_owned());
    }

    // Temporal column. Try the fractional-seconds format first because it
    // succeeds for both `2024-01-15 10:30:00` (no fraction) and
    // `2024-01-15 10:30:00.123456` (microsecond precision); chrono's `%.f`
    // matches an optional fractional part.
    let parsed = match col_type {
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => {
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").map(Value::Date)
        }
        ColumnType::MYSQL_TYPE_DATETIME
        | ColumnType::MYSQL_TYPE_DATETIME2
        | ColumnType::MYSQL_TYPE_TIMESTAMP
        | ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
                .map(Value::Timestamp)
        }
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => {
            chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
                .or_else(|_| chrono::NaiveTime::parse_from_str(s, "%H:%M:%S"))
                .map(Value::Time)
        }
        _ => return Value::String(s.to_owned()),
    };

    parsed.unwrap_or_else(|_| {
        tracing::warn!(
            target: "reify::mysql",
            column = %name(),
            value = s,
            column_type = ?col_type,
            "MySQL temporal column returned an unparseable string; preserving as Value::String"
        );
        Value::String(s.to_owned())
    })
}

fn mysql_column_to_value(row: &mysql_async::Row, idx: usize) -> Value {
    use mysql_async::Value as MV;

    // Access the raw mysql_async::Value to dispatch by type
    let raw: Option<&MV> = row.as_ref(idx);
    match raw {
        None | Some(MV::NULL) => Value::Null,
        Some(MV::Bytes(b)) => bytes_to_value(b, row.columns()[idx].column_type(), || {
            row.columns()[idx].name_str().to_string()
        }),
        Some(MV::Int(v)) => Value::I64(*v),
        // BIGINT UNSIGNED — always return `Value::U64` so the type stays
        // consistent regardless of the runtime value. Pre-fix, the value
        // alternated between `Value::I64` and `Value::String(...)` based on
        // whether it exceeded `i64::MAX`, breaking type-driven business
        // logic. Callers reading these columns should declare them as `u64`
        // (or use `i64::try_from` via `FromValue<i64>`).
        Some(MV::UInt(v)) => Value::U64(*v),
        Some(MV::Float(v)) => Value::F32(*v),
        Some(MV::Double(v)) => Value::F64(*v),
        Some(MV::Date(year, month, day, hour, min, sec, micro)) => {
            // Use column metadata to distinguish DATE from DATETIME/TIMESTAMP.
            // The runtime value alone is ambiguous: DATETIME at midnight has
            // hour/min/sec/micro all zero, same as a pure DATE.
            let col_type = row.columns()[idx].column_type();
            let date = chrono::NaiveDate::from_ymd_opt(*year as i32, *month as u32, *day as u32);

            match col_type {
                ColumnType::MYSQL_TYPE_DATE => date.map(Value::Date).unwrap_or(Value::Null),
                _ => {
                    // DATETIME, TIMESTAMP, or other temporal types with time component
                    date.and_then(|d| {
                        chrono::NaiveTime::from_hms_micro_opt(
                            *hour as u32,
                            *min as u32,
                            *sec as u32,
                            *micro,
                        )
                        .map(|t| Value::Timestamp(chrono::NaiveDateTime::new(d, t)))
                    })
                    .unwrap_or(Value::Null)
                }
            }
        }
        Some(MV::Time(is_negative, days, hours, mins, secs, micros)) => {
            // MySQL TIME is a *signed interval* (-838:59:59 to +838:59:59),
            // not a wall-clock time. Pre-fix, negative values were dropped
            // and the `days` component (≥ 24 h) was ignored, silently
            // losing data. Build a `chrono::Duration` that preserves all
            // four components and the sign.
            let secs_total = i64::from(*days) * 86_400
                + i64::from(*hours) * 3_600
                + i64::from(*mins) * 60
                + i64::from(*secs);
            let mut d = chrono::Duration::seconds(secs_total)
                + chrono::Duration::microseconds(i64::from(*micros));
            if *is_negative {
                d = -d;
            }
            Value::Duration(d)
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
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, mysql_params).await.map_err(mysql_err)?;
        Ok(conn.affected_rows())
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let (sql, mysql_params) = prepare_mysql(sql, params)?;
        debug!(target: "reify::mysql", sql, "Querying");
        let mut conn = self.get_conn().await?;
        let rows: Vec<mysql_async::Row> = conn.exec(sql, mysql_params).await.map_err(mysql_err)?;
        Ok(rows.iter().map(mysql_row_to_row).collect())
    }

    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        let (sql, mysql_params) = prepare_mysql(sql, params)?;
        debug!(target: "reify::mysql", sql, "Querying one");
        let mut conn = self.get_conn().await?;
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
        let mut conn = self.get_conn().await?;
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
        let mut conn = self.get_conn().await?;
        // MySQL rejects `BEGIN` / `COMMIT` / `ROLLBACK` through the
        // prepared-statement protocol (error 1295: "This command is not
        // supported in the prepared statement protocol yet"). Use the
        // text protocol via `query_drop` to control the transaction
        // boundary. Statements inside the transaction (which use
        // placeholders) still use `exec_drop` as before.
        conn.query_drop("BEGIN").await.map_err(mysql_err)?;

        let txn = MysqlTransaction {
            conn: tokio::sync::Mutex::new(conn),
            savepoint_counter: SavepointCounter::new(),
        };

        match f(&txn).await {
            Ok(()) => {
                debug!(target: "reify::mysql", "COMMIT transaction");
                let mut conn = txn.conn.lock().await;
                conn.query_drop("COMMIT").await.map_err(mysql_err)?;
                Ok(())
            }
            Err(e) => {
                error!(target: "reify::mysql", error = %e, "ROLLBACK transaction");
                let mut conn = txn.conn.lock().await;
                let _ = conn.query_drop("ROLLBACK").await;
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

#[cfg(test)]
mod tests {
    //! Unit tests for the MV::Bytes → Value conversion path.
    //!
    //! Pre-fix, `bytes_to_value` parsed any UTF-8 string that happened to
    //! match a temporal format, regardless of the column's declared type.
    //! That made a `VARCHAR` column whose content looked like a date silently
    //! decode to `Value::Date`, breaking downstream code that expected a
    //! string. These tests pin the column-type-gated parsing.

    use super::*;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    fn bv(s: &str, t: ColumnType) -> Value {
        bytes_to_value(s.as_bytes(), t, || "test_col".to_string())
    }

    #[test]
    fn varchar_with_datelike_content_stays_string() {
        // Pre-fix bug: this returned Value::Date. Now stays Value::String.
        assert_eq!(
            bv("2024-01-15", ColumnType::MYSQL_TYPE_VARCHAR),
            Value::String("2024-01-15".into())
        );
        assert_eq!(
            bv("2024-01-15 10:30:00", ColumnType::MYSQL_TYPE_STRING),
            Value::String("2024-01-15 10:30:00".into())
        );
    }

    #[test]
    fn date_column_parses_to_value_date() {
        assert_eq!(
            bv("2024-01-15", ColumnType::MYSQL_TYPE_DATE),
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap())
        );
    }

    #[test]
    fn datetime_with_microseconds_preserved() {
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveTime::from_hms_micro_opt(10, 30, 45, 123456).unwrap(),
        );
        assert_eq!(
            bv(
                "2024-01-15 10:30:45.123456",
                ColumnType::MYSQL_TYPE_DATETIME
            ),
            Value::Timestamp(dt)
        );
    }

    #[test]
    fn datetime_without_fractional_still_parses() {
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveTime::from_hms_opt(10, 30, 45).unwrap(),
        );
        assert_eq!(
            bv("2024-01-15 10:30:45", ColumnType::MYSQL_TYPE_DATETIME),
            Value::Timestamp(dt)
        );
        assert_eq!(
            bv("2024-01-15 10:30:45", ColumnType::MYSQL_TYPE_TIMESTAMP),
            Value::Timestamp(dt)
        );
    }

    #[test]
    fn time_with_and_without_fractional() {
        let t1 = NaiveTime::from_hms_opt(10, 30, 45).unwrap();
        let t2 = NaiveTime::from_hms_micro_opt(10, 30, 45, 123).unwrap();
        assert_eq!(bv("10:30:45", ColumnType::MYSQL_TYPE_TIME), Value::Time(t1));
        assert_eq!(
            bv("10:30:45.000123", ColumnType::MYSQL_TYPE_TIME),
            Value::Time(t2)
        );
    }

    #[test]
    fn unparseable_temporal_falls_back_to_string_not_null() {
        // Data must be preserved on parse failure so operators can debug.
        // Pre-fix this returned Value::String silently with no log; now we
        // log a warning and still preserve the data.
        assert_eq!(
            bv("not-a-date", ColumnType::MYSQL_TYPE_DATE),
            Value::String("not-a-date".into())
        );
    }

    #[test]
    fn non_utf8_bytes_stay_bytes() {
        let invalid_utf8 = [0xff, 0xfe, 0xfd];
        assert_eq!(
            bytes_to_value(&invalid_utf8, ColumnType::MYSQL_TYPE_BLOB, || "c".into()),
            Value::Bytes(invalid_utf8.to_vec())
        );
    }

    // ── MySQL TIME ↔ Value::Duration round-trip ────────────────────
    //
    // Pre-fix, the read path silently returned `Value::Null` for negative
    // TIME values and ignored the `days` field entirely (so values > 24 h
    // were truncated). The bind path went through chrono::NaiveTime which
    // can only express 0:00:00..24:00:00. Both directions now use
    // chrono::Duration so the full MySQL range round-trips losslessly.

    #[test]
    fn value_to_mysql_duration_positive_under_24h() {
        let d = chrono::Duration::seconds(5 * 3600 + 30 * 60 + 45)
            + chrono::Duration::microseconds(123_456);
        let mv = value_to_mysql(&Value::Duration(d)).unwrap();
        assert_eq!(mv, mysql_async::Value::Time(false, 0, 5, 30, 45, 123_456));
    }

    #[test]
    fn value_to_mysql_duration_negative() {
        let d = -chrono::Duration::seconds(3600 + 30 * 60);
        let mv = value_to_mysql(&Value::Duration(d)).unwrap();
        assert_eq!(mv, mysql_async::Value::Time(true, 0, 1, 30, 0, 0));
    }

    #[test]
    fn value_to_mysql_duration_above_24h_uses_days_field() {
        // 838h59m59s = 34 days + 22h59m59s
        let d = chrono::Duration::seconds(838 * 3600 + 59 * 60 + 59);
        let mv = value_to_mysql(&Value::Duration(d)).unwrap();
        assert_eq!(mv, mysql_async::Value::Time(false, 34, 22, 59, 59, 0));
    }

    #[test]
    fn value_to_mysql_duration_negative_above_24h() {
        // -838h59m59s = -(34 days + 22h59m59s)
        let d = -(chrono::Duration::seconds(838 * 3600 + 59 * 60 + 59));
        let mv = value_to_mysql(&Value::Duration(d)).unwrap();
        assert_eq!(mv, mysql_async::Value::Time(true, 34, 22, 59, 59, 0));
    }

    #[test]
    fn is_temporal_column_type_covers_all_variants() {
        for t in [
            ColumnType::MYSQL_TYPE_DATE,
            ColumnType::MYSQL_TYPE_NEWDATE,
            ColumnType::MYSQL_TYPE_DATETIME,
            ColumnType::MYSQL_TYPE_DATETIME2,
            ColumnType::MYSQL_TYPE_TIMESTAMP,
            ColumnType::MYSQL_TYPE_TIMESTAMP2,
            ColumnType::MYSQL_TYPE_TIME,
            ColumnType::MYSQL_TYPE_TIME2,
        ] {
            assert!(is_temporal_column_type(t), "{t:?} should be temporal");
        }
        for t in [
            ColumnType::MYSQL_TYPE_VARCHAR,
            ColumnType::MYSQL_TYPE_STRING,
            ColumnType::MYSQL_TYPE_BLOB,
            ColumnType::MYSQL_TYPE_LONGLONG,
            ColumnType::MYSQL_TYPE_YEAR,
        ] {
            assert!(!is_temporal_column_type(t), "{t:?} should NOT be temporal");
        }
    }
}
