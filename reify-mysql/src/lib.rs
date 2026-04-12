//! MySQL / MariaDB adapter for Reify.
//!
//! ```ignore
//! use reify_mysql::MysqlDb;
//!
//! let db = MysqlDb::connect("mysql://app:secret@localhost:3306/mydb").await?;
//! let rows = reify_core::fetch_all(&db, &User::find().filter(User::id.eq(1i64))).await?;
//! ```

use std::future::Future;
use std::pin::Pin;

use mysql_async::prelude::*;
use mysql_async::{Opts, Pool};
use tracing::{debug, error};

use reify_core::db::{Database, DbError, Row};
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

/// Map a `mysql_async::Error` to a `DbError`, promoting constraint
/// violations (MySQL error codes 1062, 1451, 1452, 1048, 3819) to
/// `DbError::Constraint`.
fn mysql_err(e: mysql_async::Error) -> DbError {
    if let mysql_async::Error::Server(ref server_err) = e {
        // MySQL server error codes for constraint violations:
        //   1062 = ER_DUP_ENTRY (unique), 1451/1452 = FK violation,
        //   1048 = ER_BAD_NULL_ERROR, 3819 = ER_CHECK_CONSTRAINT_VIOLATED
        let sqlstate = server_err.state.clone();
        match server_err.code {
            1062 | 1451 | 1452 | 1048 | 3819 => {
                return DbError::Constraint {
                    message: server_err.message.clone(),
                    sqlstate: Some(sqlstate),
                };
            }
            _ => {}
        }
    }
    DbError::Query(e.to_string())
}

// ── Database trait implementation ───────────────────────────────────

impl Database for MysqlDb {
    fn execute<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Value],
    ) -> Pin<Box<dyn Future<Output = Result<u64, DbError>> + Send + 'a>> {
        Box::pin(async move {
            let mysql_params = values_to_mysql_params(params);
            debug!(target: "reify::mysql", sql, "Executing");
            let mut conn = self.pool.get_conn().await.map_err(|e| DbError::Connection(e.to_string()))?;
            conn.exec_drop(sql, mysql_params).await.map_err(mysql_err)?;
            Ok(conn.affected_rows())
        })
    }

    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Value],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Row>, DbError>> + Send + 'a>> {
        Box::pin(async move {
            let mysql_params = values_to_mysql_params(params);
            debug!(target: "reify::mysql", sql, "Querying");
            let mut conn = self.pool.get_conn().await.map_err(|e| DbError::Connection(e.to_string()))?;
            let rows: Vec<mysql_async::Row> =
                conn.exec(sql, mysql_params).await.map_err(mysql_err)?;
            Ok(rows.iter().map(mysql_row_to_row).collect())
        })
    }

    fn query_one<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Value],
    ) -> Pin<Box<dyn Future<Output = Result<Row, DbError>> + Send + 'a>> {
        Box::pin(async move {
            let mysql_params = values_to_mysql_params(params);
            debug!(target: "reify::mysql", sql, "Querying one");
            let mut conn = self.pool.get_conn().await.map_err(|e| DbError::Connection(e.to_string()))?;
            let row: Option<mysql_async::Row> =
                conn.exec_first(sql, mysql_params).await.map_err(mysql_err)?;
            match row {
                Some(r) => Ok(mysql_row_to_row(&r)),
                None => Err(DbError::Query("no rows returned".to_string())),
            }
        })
    }

    fn transaction<'a>(
        &'a self,
        f: Box<
            dyn FnOnce(
                    &'a dyn Database,
                )
                    -> Pin<Box<dyn Future<Output = Result<(), DbError>> + Send + 'a>>
                + Send
                + 'a,
        >,
    ) -> Pin<Box<dyn Future<Output = Result<(), DbError>> + Send + 'a>> {
        Box::pin(async move {
            debug!(target: "reify::mysql", "BEGIN transaction");
            {
                let mut conn = self.pool.get_conn().await.map_err(|e| DbError::Connection(e.to_string()))?;
                conn.exec_drop("BEGIN", mysql_async::Params::Empty)
                    .await
                    .map_err(mysql_err)?;
            }

            match f(self).await {
                Ok(()) => {
                    debug!(target: "reify::mysql", "COMMIT transaction");
                    let mut conn = self.pool.get_conn().await.map_err(|e| DbError::Connection(e.to_string()))?;
                    conn.exec_drop("COMMIT", mysql_async::Params::Empty)
                        .await
                        .map_err(mysql_err)?;
                    Ok(())
                }
                Err(e) => {
                    error!(target: "reify::mysql", error = %e, "ROLLBACK transaction");
                    if let Ok(mut conn) = self.pool.get_conn().await {
                        let _ = conn.exec_drop("ROLLBACK", mysql_async::Params::Empty).await;
                    }
                    Err(e)
                }
            }
        })
    }
}
