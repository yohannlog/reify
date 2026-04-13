//! MySQL / MariaDB adapter for Reify.
//!
//! ```ignore
//! use reify_mysql::MysqlDb;
//!
//! let db = MysqlDb::connect("mysql://app:secret@localhost:3306/mydb").await?;
//! let rows = reify_core::fetch_all(&db, &User::find().filter(User::id.eq(1i64))).await?;
//! ```

use mysql_async::prelude::*;
use mysql_async::{Opts, Pool};
use tracing::{debug, error};

use reify_core::db::{Database, DbError, Row, TransactionFn};
use reify_core::value::Value;

/// MySQL / MariaDB database backed by a `mysql_async` connection pool.
pub struct MysqlDb {
    pool: Pool,
}

impl MysqlDb {
    /// Connect to a MySQL / MariaDB database.
    ///
    /// `url` is a standard MySQL connection URL:
    /// `"mysql://user:password@host:3306/database"`
    ///
    /// `mysql_async::Pool` manages connections internally; use
    /// [`mysql_async::PoolOpts`] via [`MysqlDb::from_pool`] for fine-grained
    /// pool sizing (min/max connections, timeouts).
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        debug!(target: "reify::mysql", url, "Connecting to MySQL/MariaDB");
        let opts = Opts::from_url(url).map_err(|e| DbError::Connection(e.to_string()))?;
        let pool = Pool::new(opts);
        // Eagerly verify connectivity.
        pool.get_conn()
            .await
            .map_err(|e| DbError::Connection(e.to_string()))?
            .disconnect()
            .await
            .map_err(|e| DbError::Connection(e.to_string()))?;
        Ok(Self { pool })
    }

    /// Build a `MysqlDb` from an already-constructed `mysql_async::Pool`.
    ///
    /// Use this when you need custom pool options (min/max size, timeouts):
    /// ```ignore
    /// use mysql_async::{Pool, PoolOpts, PoolConstraints, Opts};
    /// let opts = Opts::from_url(url)?;
    /// let pool_opts = PoolOpts::default()
    ///     .with_constraints(PoolConstraints::new(2, 20).unwrap());
    /// let pool = Pool::new(opts.clone().pool_opts(pool_opts));
    /// let db = MysqlDb::from_pool(pool);
    /// ```
    pub fn from_pool(pool: Pool) -> Self {
        Self { pool }
    }
}



// ── Value → mysql_async parameter conversion ────────────────────────

fn values_to_mysql_params(params: &[Value]) -> mysql_async::Params {
    if params.is_empty() {
        return mysql_async::Params::Empty;
    }
    let vals: Vec<mysql_async::Value> = params.iter().map(value_to_mysql).collect();
    mysql_async::Params::Positional(vals)
}

fn value_to_mysql(val: &Value) -> mysql_async::Value {
    match val {
        Value::Null => mysql_async::Value::NULL,
        Value::Bool(v) => mysql_async::Value::from(*v),
        Value::I16(v) => mysql_async::Value::from(*v),
        Value::I32(v) => mysql_async::Value::from(*v),
        Value::I64(v) => mysql_async::Value::from(*v),
        Value::F32(v) => mysql_async::Value::from(*v),
        Value::F64(v) => mysql_async::Value::from(*v),
        Value::String(v) => mysql_async::Value::from(v.as_str()),
        Value::Bytes(v) => mysql_async::Value::from(v.as_slice()),
        Value::Timestamp(v) => mysql_async::Value::from(v.to_string()),
        Value::Date(v) => mysql_async::Value::from(v.to_string()),
        Value::Time(v) => mysql_async::Value::from(v.to_string()),
        #[allow(unreachable_patterns)]
        _ => mysql_async::Value::NULL,
    }
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
                if let Ok(dt) =
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                {
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
        Some(MV::UInt(v)) => Value::I64(*v as i64),
        Some(MV::Float(v)) => Value::F32(*v),
        Some(MV::Double(v)) => Value::F64(*v),
        Some(MV::Date(year, month, day, hour, min, sec, _micro)) => {
            if *hour == 0 && *min == 0 && *sec == 0 {
                if let Some(d) =
                    chrono::NaiveDate::from_ymd_opt(*year as i32, *month as u32, *day as u32)
                {
                    return Value::Date(d);
                }
            }
            if let Some(d) =
                chrono::NaiveDate::from_ymd_opt(*year as i32, *month as u32, *day as u32)
            {
                if let Some(t) =
                    chrono::NaiveTime::from_hms_opt(*hour as u32, *min as u32, *sec as u32)
                {
                    return Value::Timestamp(chrono::NaiveDateTime::new(d, t));
                }
            }
            Value::Null
        }
        Some(MV::Time(_, _, hours, mins, secs, _micro)) => {
            chrono::NaiveTime::from_hms_opt(*hours as u32, *mins as u32, *secs as u32)
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
    if let mysql_async::Error::Server(ref server_err) = e {
        if MYSQL_CONSTRAINT_CODES.contains(&server_err.code) {
            return DbError::Constraint {
                message: server_err.message.clone(),
                sqlstate: Some(server_err.state.clone()),
            };
        }
    }
    DbError::Query(e.to_string())
}

// ── Database trait implementation ───────────────────────────────────

impl Database for MysqlDb {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        let mysql_params = values_to_mysql_params(params);
        debug!(target: "reify::mysql", sql, "Executing");
        let mut conn = self.pool.get_conn().await.map_err(|e| DbError::Connection(e.to_string()))?;
        conn.exec_drop(sql, mysql_params).await.map_err(mysql_err)?;
        Ok(conn.affected_rows())
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let mysql_params = values_to_mysql_params(params);
        debug!(target: "reify::mysql", sql, "Querying");
        let mut conn = self.pool.get_conn().await.map_err(|e| DbError::Connection(e.to_string()))?;
        let rows: Vec<mysql_async::Row> =
            conn.exec(sql, mysql_params).await.map_err(mysql_err)?;
        Ok(rows.iter().map(mysql_row_to_row).collect())
    }

    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        let mysql_params = values_to_mysql_params(params);
        debug!(target: "reify::mysql", sql, "Querying one");
        let mut conn = self.pool.get_conn().await.map_err(|e| DbError::Connection(e.to_string()))?;
        let row: Option<mysql_async::Row> =
            conn.exec_first(sql, mysql_params).await.map_err(mysql_err)?;
        match row {
            Some(r) => Ok(mysql_row_to_row(&r)),
            None => Err(DbError::Query("no rows returned".to_string())),
        }
    }

    async fn transaction<'a>(
        &'a self,
        f: TransactionFn<'a>,
    ) -> Result<(), DbError> {
        debug!(target: "reify::mysql", "BEGIN transaction");
        let mut conn = self.pool.get_conn().await.map_err(|e| DbError::Connection(e.to_string()))?;
        conn.exec_drop("BEGIN", mysql_async::Params::Empty)
            .await
            .map_err(mysql_err)?;

        let txn: Box<MysqlTransaction> = Box::new(MysqlTransaction {
            conn: tokio::sync::Mutex::new(conn),
        });
        // SAFETY: `txn` lives until the end of this async block, which
        // is strictly longer than the `f(&*txn_ref).await` call.
        let txn_ref: &'a MysqlTransaction =
            unsafe { &*(&*txn as *const MysqlTransaction) };

        match f(txn_ref).await {
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
}

// ── MysqlTransaction — dedicated connection for transaction scope ───

/// A single MySQL connection held open for the duration of a transaction.
///
/// Uses a `tokio::sync::Mutex` because `mysql_async::Conn` requires `&mut self`
/// for queries, but the `Database` trait takes `&self`. The tokio Mutex guard
/// is `Send`-safe across await points.
struct MysqlTransaction {
    conn: tokio::sync::Mutex<mysql_async::Conn>,
}

impl Database for MysqlTransaction {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        let mysql_params = values_to_mysql_params(params);
        debug!(target: "reify::mysql", sql, "Executing (txn)");
        let mut conn = self.conn.lock().await;
        conn.exec_drop(sql, mysql_params).await.map_err(mysql_err)?;
        Ok(conn.affected_rows())
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let mysql_params = values_to_mysql_params(params);
        debug!(target: "reify::mysql", sql, "Querying (txn)");
        let mut conn = self.conn.lock().await;
        let rows: Vec<mysql_async::Row> =
            conn.exec(sql, mysql_params).await.map_err(mysql_err)?;
        Ok(rows.iter().map(mysql_row_to_row).collect())
    }

    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        let mysql_params = values_to_mysql_params(params);
        debug!(target: "reify::mysql", sql, "Querying one (txn)");
        let mut conn = self.conn.lock().await;
        let row: Option<mysql_async::Row> =
            conn.exec_first(sql, mysql_params).await.map_err(mysql_err)?;
        match row {
            Some(r) => Ok(mysql_row_to_row(&r)),
            None => Err(DbError::Query("no rows returned".to_string())),
        }
    }

    async fn transaction<'a>(
        &'a self,
        f: TransactionFn<'a>,
    ) -> Result<(), DbError> {
        // Nested transaction via SAVEPOINT
        debug!(target: "reify::mysql", "SAVEPOINT nested_txn");
        {
            let mut conn = self.conn.lock().await;
            conn.exec_drop("SAVEPOINT nested_txn", mysql_async::Params::Empty)
                .await
                .map_err(mysql_err)?;
        }
        match f(self).await {
            Ok(()) => {
                let mut conn = self.conn.lock().await;
                conn.exec_drop("RELEASE SAVEPOINT nested_txn", mysql_async::Params::Empty)
                    .await
                    .map_err(mysql_err)?;
                Ok(())
            }
            Err(e) => {
                let mut conn = self.conn.lock().await;
                let _ = conn.exec_drop("ROLLBACK TO SAVEPOINT nested_txn", mysql_async::Params::Empty).await;
                Err(e)
            }
        }
    }
}
