//! PostgreSQL adapter for Reify.
//!
//! ```ignore
//! use reify_postgres::{PostgresDb, PoolConfig};
//!
//! let db = PostgresDb::connect("host=localhost user=app dbname=mydb", PoolConfig::default()).await?;
//! let rows = reify_core::fetch_all(&db, &User::find().filter(User::id.eq(1i64))).await?;
//! ```

use std::time::Duration;

use deadpool_postgres::{Config as DpConfig, Pool, Runtime};
use tokio_postgres::{NoTls, types::ToSql as PgToSql};
use tracing::{debug, error};

use reify_core::db::{Database, DbError, Row, TransactionFn};
use reify_core::range::{Bound, Range};
use reify_core::value::Value;

// ── Pool configuration ───────────────────────────────────────────────

/// Configuration for the PostgreSQL connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Minimum number of idle connections kept open. Default: 1.
    pub min_connections: usize,
    /// Maximum number of connections in the pool. Default: 10.
    pub max_connections: usize,
    /// Timeout when waiting for a connection from the pool. Default: 5 s.
    pub connection_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 10,
            connection_timeout: Duration::from_secs(5),
        }
    }
}

/// PostgreSQL database backed by a `deadpool-postgres` connection pool.
pub struct PostgresDb {
    pool: Pool,
}

impl PostgresDb {
    /// Connect to a PostgreSQL database and initialise a connection pool.
    ///
    /// `config` is a libpq-style connection string:
    /// `"host=localhost port=5432 user=app password=secret dbname=mydb"`
    pub async fn connect(config: &str, pool_cfg: PoolConfig) -> Result<Self, DbError> {
        debug!(target: "reify::postgres", config, "Connecting to PostgreSQL (pool)");

        let mut dp = DpConfig::new();
        // Parse the libpq connection string into individual fields that
        // deadpool-postgres / tokio-postgres understand.
        let pg_cfg: tokio_postgres::Config = config
            .parse()
            .map_err(|e: tokio_postgres::Error| DbError::Connection(e.to_string()))?;

        dp.host = pg_cfg.get_hosts().first().and_then(|h| match h {
            tokio_postgres::config::Host::Tcp(s) => Some(s.clone()),
            _ => None,
        });
        dp.port = pg_cfg.get_ports().first().copied();
        dp.user = pg_cfg.get_user().map(str::to_owned);
        dp.password = pg_cfg
            .get_password()
            .map(|b| String::from_utf8_lossy(b).into_owned());
        dp.dbname = pg_cfg.get_dbname().map(str::to_owned);

        dp.pool = Some(deadpool_postgres::PoolConfig {
            max_size: pool_cfg.max_connections,
            timeouts: deadpool_postgres::Timeouts {
                wait: Some(pool_cfg.connection_timeout),
                ..Default::default()
            },
            ..Default::default()
        });

        let pool = dp
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| DbError::Connection(e.to_string()))?;

        // Eagerly open `min_connections` connections so the pool is warm.
        for _ in 0..pool_cfg.min_connections {
            match pool.get().await {
                Ok(_) => {}
                Err(e) => {
                    error!(target: "reify::postgres", error = %e, "Failed to pre-warm pool connection");
                }
            }
        }

        Ok(Self { pool })
    }

    /// Build a `PostgresDb` from an already-constructed `deadpool_postgres::Pool`.
    pub fn from_pool(pool: Pool) -> Self {
        Self { pool }
    }
}

// ── Value → PostgreSQL parameter conversion ─────────────────────────

/// Wrapper to implement `ToSql` for our `Value` type.
#[derive(Debug)]
struct PgValue<'a>(&'a Value);

impl PgToSql for PgValue<'_> {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self.0 {
            Value::Null => Ok(tokio_postgres::types::IsNull::Yes),
            Value::Bool(v) => v.to_sql(ty, out),
            Value::I16(v) => v.to_sql(ty, out),
            Value::I32(v) => v.to_sql(ty, out),
            Value::I64(v) => v.to_sql(ty, out),
            Value::F32(v) => (*v as f64).to_sql(ty, out),
            Value::F64(v) => v.to_sql(ty, out),
            Value::String(v) => v.to_sql(ty, out),
            Value::Bytes(v) => v.as_slice().to_sql(ty, out),
            Value::Uuid(v) => v.to_sql(ty, out),
            Value::Timestamptz(v) => v.to_sql(ty, out),
            Value::Timestamp(v) => v.to_sql(ty, out),
            Value::Date(v) => v.to_sql(ty, out),
            Value::Time(v) => v.to_sql(ty, out),
            Value::Jsonb(v) => serde_json::to_value(v)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)?
                .to_sql(ty, out),
            Value::Int4Range(r) => {
                range_to_pg(r, ty, out)?;
                Ok(tokio_postgres::types::IsNull::No)
            }
            Value::Int8Range(r) => {
                range_to_pg(r, ty, out)?;
                Ok(tokio_postgres::types::IsNull::No)
            }
            Value::TsRange(r) => {
                range_to_pg(r, ty, out)?;
                Ok(tokio_postgres::types::IsNull::No)
            }
            Value::TstzRange(r) => {
                range_to_pg(r, ty, out)?;
                Ok(tokio_postgres::types::IsNull::No)
            }
            Value::DateRange(r) => {
                range_to_pg(r, ty, out)?;
                Ok(tokio_postgres::types::IsNull::No)
            }
            Value::ArrayBool(v) => v.to_sql(ty, out),
            Value::ArrayI16(v) => v.to_sql(ty, out),
            Value::ArrayI32(v) => v.to_sql(ty, out),
            Value::ArrayI64(v) => v.to_sql(ty, out),
            Value::ArrayF32(v) => v.to_sql(ty, out),
            Value::ArrayF64(v) => v.to_sql(ty, out),
            Value::ArrayString(v) => v.to_sql(ty, out),
            Value::ArrayUuid(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        true
    }

    tokio_postgres::types::to_sql_checked!();
}

// ── Range serialization helpers ──────────────────────────────────────

/// Serialize a `Range<T>` into the PostgreSQL binary wire format.
fn range_to_pg<T: PgToSql + Sync>(
    range: &Range<T>,
    ty: &tokio_postgres::types::Type,
    out: &mut bytes::BytesMut,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    use postgres_protocol::IsNull as PgIsNull;
    use postgres_protocol::types::{RangeBound, empty_range_to_sql, range_to_sql};

    fn convert_is_null(v: tokio_postgres::types::IsNull) -> PgIsNull {
        match v {
            tokio_postgres::types::IsNull::Yes => PgIsNull::Yes,
            tokio_postgres::types::IsNull::No => PgIsNull::No,
        }
    }

    // Resolve the element type from the range type.
    let element_type = match ty.kind() {
        postgres_types::Kind::Range(inner) => inner.clone(),
        _ => ty.clone(),
    };

    match range {
        Range::Empty => {
            empty_range_to_sql(out);
        }
        Range::Nonempty(lower, upper) => {
            let et_lower = element_type.clone();
            let et_upper = element_type;

            range_to_sql(
                |buf| match lower {
                    Bound::Inclusive(v) => {
                        Ok(RangeBound::Inclusive(convert_is_null(v.to_sql(&et_lower, buf)?)))
                    }
                    Bound::Exclusive(v) => {
                        Ok(RangeBound::Exclusive(convert_is_null(v.to_sql(&et_lower, buf)?)))
                    }
                    Bound::Unbounded => Ok(RangeBound::Unbounded),
                },
                |buf| match upper {
                    Bound::Inclusive(v) => {
                        Ok(RangeBound::Inclusive(convert_is_null(v.to_sql(&et_upper, buf)?)))
                    }
                    Bound::Exclusive(v) => {
                        Ok(RangeBound::Exclusive(convert_is_null(v.to_sql(&et_upper, buf)?)))
                    }
                    Bound::Unbounded => Ok(RangeBound::Unbounded),
                },
                out,
            )?;
        }
    }
    Ok(())
}

/// Deserialize a PostgreSQL range from raw bytes into a `Range<T>`.
fn range_from_pg<T, F>(raw: &[u8], parse_element: F) -> Range<T>
where
    F: Fn(&[u8]) -> Option<T>,
{
    use postgres_protocol::types::{RangeBound as PgBound, range_from_sql};

    let parsed = match range_from_sql(raw) {
        Ok(r) => r,
        Err(_) => return Range::Empty,
    };

    match parsed {
        postgres_protocol::types::Range::Empty => Range::Empty,
        postgres_protocol::types::Range::Nonempty(lower, upper) => {
            let lo = match lower {
                PgBound::Inclusive(Some(bytes)) => {
                    parse_element(bytes).map(Bound::Inclusive).unwrap_or(Bound::Unbounded)
                }
                PgBound::Exclusive(Some(bytes)) => {
                    parse_element(bytes).map(Bound::Exclusive).unwrap_or(Bound::Unbounded)
                }
                PgBound::Inclusive(None) | PgBound::Exclusive(None) => Bound::Unbounded,
                PgBound::Unbounded => Bound::Unbounded,
            };
            let hi = match upper {
                PgBound::Inclusive(Some(bytes)) => {
                    parse_element(bytes).map(Bound::Inclusive).unwrap_or(Bound::Unbounded)
                }
                PgBound::Exclusive(Some(bytes)) => {
                    parse_element(bytes).map(Bound::Exclusive).unwrap_or(Bound::Unbounded)
                }
                PgBound::Inclusive(None) | PgBound::Exclusive(None) => Bound::Unbounded,
                PgBound::Unbounded => Bound::Unbounded,
            };
            Range::Nonempty(lo, hi)
        }
    }
}

// ── PostgreSQL row → reify Row conversion ───────────────────────────

fn pg_row_to_row(row: &tokio_postgres::Row) -> Row {
    let columns: Vec<String> = row.columns().iter().map(|c| c.name().to_string()).collect();
    let values: Vec<Value> = row
        .columns()
        .iter()
        .enumerate()
        .map(|(i, col)| pg_column_to_value(row, i, col.type_()))
        .collect();
    Row::new(columns, values)
}

fn pg_column_to_value(
    row: &tokio_postgres::Row,
    idx: usize,
    ty: &tokio_postgres::types::Type,
) -> Value {
    use tokio_postgres::types::Type;

    match *ty {
        Type::BOOL => row
            .try_get::<_, Option<bool>>(idx)
            .ok()
            .flatten()
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        Type::INT2 => row
            .try_get::<_, Option<i16>>(idx)
            .ok()
            .flatten()
            .map(Value::I16)
            .unwrap_or(Value::Null),
        Type::INT4 => row
            .try_get::<_, Option<i32>>(idx)
            .ok()
            .flatten()
            .map(Value::I32)
            .unwrap_or(Value::Null),
        Type::INT8 => row
            .try_get::<_, Option<i64>>(idx)
            .ok()
            .flatten()
            .map(Value::I64)
            .unwrap_or(Value::Null),
        Type::FLOAT4 => row
            .try_get::<_, Option<f32>>(idx)
            .ok()
            .flatten()
            .map(Value::F32)
            .unwrap_or(Value::Null),
        Type::FLOAT8 => row
            .try_get::<_, Option<f64>>(idx)
            .ok()
            .flatten()
            .map(Value::F64)
            .unwrap_or(Value::Null),
        Type::BYTEA => row
            .try_get::<_, Option<Vec<u8>>>(idx)
            .ok()
            .flatten()
            .map(Value::Bytes)
            .unwrap_or(Value::Null),
        Type::UUID => row
            .try_get::<_, Option<uuid::Uuid>>(idx)
            .ok()
            .flatten()
            .map(Value::Uuid)
            .unwrap_or(Value::Null),
        Type::TIMESTAMPTZ => row
            .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx)
            .ok()
            .flatten()
            .map(Value::Timestamptz)
            .unwrap_or(Value::Null),
        Type::TIMESTAMP => row
            .try_get::<_, Option<chrono::NaiveDateTime>>(idx)
            .ok()
            .flatten()
            .map(Value::Timestamp)
            .unwrap_or(Value::Null),
        Type::DATE => row
            .try_get::<_, Option<chrono::NaiveDate>>(idx)
            .ok()
            .flatten()
            .map(Value::Date)
            .unwrap_or(Value::Null),
        Type::TIME => row
            .try_get::<_, Option<chrono::NaiveTime>>(idx)
            .ok()
            .flatten()
            .map(Value::Time)
            .unwrap_or(Value::Null),
        Type::JSON | Type::JSONB => row
            .try_get::<_, Option<serde_json::Value>>(idx)
            .ok()
            .flatten()
            .map(Value::Jsonb)
            .unwrap_or(Value::Null),
        Type::INT4_RANGE => {
            match row.try_get::<_, Option<&[u8]>>(idx) {
                Ok(Some(raw)) => Value::Int4Range(range_from_pg(raw, |b| {
                    use bytes::Buf;
                    if b.len() == 4 { Some((&b[..]).get_i32()) } else { None }
                })),
                _ => Value::Null,
            }
        }
        Type::INT8_RANGE => {
            match row.try_get::<_, Option<&[u8]>>(idx) {
                Ok(Some(raw)) => Value::Int8Range(range_from_pg(raw, |b| {
                    use bytes::Buf;
                    if b.len() == 8 { Some((&b[..]).get_i64()) } else { None }
                })),
                _ => Value::Null,
            }
        }
        Type::TS_RANGE => {
            match row.try_get::<_, Option<&[u8]>>(idx) {
                Ok(Some(raw)) => Value::TsRange(range_from_pg(raw, |b| {
                    postgres_types::FromSql::from_sql(&Type::TIMESTAMP, b).ok()
                })),
                _ => Value::Null,
            }
        }
        Type::TSTZ_RANGE => {
            match row.try_get::<_, Option<&[u8]>>(idx) {
                Ok(Some(raw)) => Value::TstzRange(range_from_pg(raw, |b| {
                    postgres_types::FromSql::from_sql(&Type::TIMESTAMPTZ, b).ok()
                })),
                _ => Value::Null,
            }
        }
        Type::DATE_RANGE => {
            match row.try_get::<_, Option<&[u8]>>(idx) {
                Ok(Some(raw)) => Value::DateRange(range_from_pg(raw, |b| {
                    postgres_types::FromSql::from_sql(&Type::DATE, b).ok()
                })),
                _ => Value::Null,
            }
        }
        Type::BOOL_ARRAY => row
            .try_get::<_, Option<Vec<bool>>>(idx)
            .ok()
            .flatten()
            .map(Value::ArrayBool)
            .unwrap_or(Value::Null),
        Type::INT2_ARRAY => row
            .try_get::<_, Option<Vec<i16>>>(idx)
            .ok()
            .flatten()
            .map(Value::ArrayI16)
            .unwrap_or(Value::Null),
        Type::INT4_ARRAY => row
            .try_get::<_, Option<Vec<i32>>>(idx)
            .ok()
            .flatten()
            .map(Value::ArrayI32)
            .unwrap_or(Value::Null),
        Type::INT8_ARRAY => row
            .try_get::<_, Option<Vec<i64>>>(idx)
            .ok()
            .flatten()
            .map(Value::ArrayI64)
            .unwrap_or(Value::Null),
        Type::FLOAT4_ARRAY => row
            .try_get::<_, Option<Vec<f32>>>(idx)
            .ok()
            .flatten()
            .map(Value::ArrayF32)
            .unwrap_or(Value::Null),
        Type::FLOAT8_ARRAY => row
            .try_get::<_, Option<Vec<f64>>>(idx)
            .ok()
            .flatten()
            .map(Value::ArrayF64)
            .unwrap_or(Value::Null),
        Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => row
            .try_get::<_, Option<Vec<String>>>(idx)
            .ok()
            .flatten()
            .map(Value::ArrayString)
            .unwrap_or(Value::Null),
        Type::UUID_ARRAY => row
            .try_get::<_, Option<Vec<uuid::Uuid>>>(idx)
            .ok()
            .flatten()
            .map(Value::ArrayUuid)
            .unwrap_or(Value::Null),
        _ => row
            .try_get::<_, Option<String>>(idx)
            .ok()
            .flatten()
            .map(Value::String)
            .unwrap_or(Value::Null),
    }
}

// ── Rewrite `?` placeholders to `$N` for PostgreSQL ─────────────────

use reify_core::rewrite_placeholders_pg as rewrite_placeholders;

// ── Error conversion helpers ─────────────────────────────────────────

/// Map a `tokio_postgres::Error` to a `DbError`, promoting constraint
/// violations (SQLSTATE class 23) to `DbError::Constraint`.
fn pg_err(e: tokio_postgres::Error) -> DbError {
    use reify_core::db::sqlstate;
    if let Some(db_err) = e.as_db_error() {
        let code = db_err.code().code().to_owned();
        if sqlstate::is_constraint_violation(&code) {
            return DbError::Constraint {
                message: db_err.message().to_owned(),
                sqlstate: Some(code),
            };
        }
    }
    DbError::Query(e.to_string())
}

/// Acquire a pooled connection, mapping pool errors to `DbError`.
async fn get_conn(pool: &Pool) -> Result<deadpool_postgres::Object, DbError> {
    pool.get().await.map_err(|e| DbError::Connection(e.to_string()))
}

/// Prepare params and execute a statement on a `tokio_postgres::Client`.
async fn pg_execute(
    client: &tokio_postgres::Client,
    sql: &str,
    params: &[Value],
) -> Result<u64, DbError> {
    let pg_sql = rewrite_placeholders(sql);
    let pg_params: Vec<PgValue> = params.iter().map(PgValue).collect();
    let param_refs: Vec<&(dyn PgToSql + Sync)> =
        pg_params.iter().map(|p| p as &(dyn PgToSql + Sync)).collect();
    debug!(target: "reify::postgres", sql = %pg_sql, "Executing");
    client.execute(&*pg_sql, &param_refs[..]).await.map_err(pg_err)
}

/// Prepare params and run a query on a `tokio_postgres::Client`.
async fn pg_query(
    client: &tokio_postgres::Client,
    sql: &str,
    params: &[Value],
) -> Result<Vec<Row>, DbError> {
    let pg_sql = rewrite_placeholders(sql);
    let pg_params: Vec<PgValue> = params.iter().map(PgValue).collect();
    let param_refs: Vec<&(dyn PgToSql + Sync)> =
        pg_params.iter().map(|p| p as &(dyn PgToSql + Sync)).collect();
    debug!(target: "reify::postgres", sql = %pg_sql, "Querying");
    let rows = client.query(&*pg_sql, &param_refs[..]).await.map_err(pg_err)?;
    Ok(rows.iter().map(pg_row_to_row).collect())
}

/// Prepare params and run a query returning exactly one row.
async fn pg_query_one(
    client: &tokio_postgres::Client,
    sql: &str,
    params: &[Value],
) -> Result<Row, DbError> {
    let pg_sql = rewrite_placeholders(sql);
    let pg_params: Vec<PgValue> = params.iter().map(PgValue).collect();
    let param_refs: Vec<&(dyn PgToSql + Sync)> =
        pg_params.iter().map(|p| p as &(dyn PgToSql + Sync)).collect();
    debug!(target: "reify::postgres", sql = %pg_sql, "Querying one");
    let row = client.query_one(&*pg_sql, &param_refs[..]).await.map_err(pg_err)?;
    Ok(pg_row_to_row(&row))
}

// ── Database trait implementation ───────────────────────────────────

impl Database for PostgresDb {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        let conn = get_conn(&self.pool).await?;
        pg_execute(&conn, sql, params).await
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let conn = get_conn(&self.pool).await?;
        pg_query(&conn, sql, params).await
    }

    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        let conn = get_conn(&self.pool).await?;
        pg_query_one(&conn, sql, params).await
    }

    async fn transaction<'a>(
        &'a self,
        f: TransactionFn<'a>,
    ) -> Result<(), DbError> {
        debug!(target: "reify::postgres", "BEGIN transaction");
        let conn = get_conn(&self.pool).await?;
        conn.execute("BEGIN", &[]).await.map_err(pg_err)?;

        // Heap-allocate the transaction wrapper so it can be borrowed
        // for the `'a` lifetime required by the closure.
        let txn: Box<PgTransaction> = Box::new(PgTransaction { conn });
        // SAFETY: `txn` lives until the end of this async block, which
        // is strictly longer than the `f(&*txn_ref).await` call. The
        // `'a` lifetime in the closure signature is the lifetime of
        // `&'a self`, but the compiler can't prove our local `txn`
        // lives that long. We guarantee it does because we don't drop
        // `txn` until after `f` completes.
        let txn_ref: &'a PgTransaction =
            unsafe { &*(&*txn as *const PgTransaction) };

        match f(txn_ref).await {
            Ok(()) => {
                debug!(target: "reify::postgres", "COMMIT transaction");
                txn.conn.execute("COMMIT", &[]).await.map_err(pg_err)?;
                Ok(())
            }
            Err(e) => {
                error!(target: "reify::postgres", error = %e, "ROLLBACK transaction");
                let _ = txn.conn.execute("ROLLBACK", &[]).await;
                Err(e)
            }
        }
    }
}

// ── PgTransaction — dedicated connection for transaction scope ──────

/// A single PostgreSQL connection held open for the duration of a transaction.
///
/// Implements `Database` so the closure inside `transaction()` can use it
/// transparently. All queries go through this one connection, preserving
/// ACID guarantees.
struct PgTransaction {
    conn: deadpool_postgres::Object,
}

impl Database for PgTransaction {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        pg_execute(&self.conn, sql, params).await
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        pg_query(&self.conn, sql, params).await
    }

    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        pg_query_one(&self.conn, sql, params).await
    }

    async fn transaction<'a>(
        &'a self,
        f: TransactionFn<'a>,
    ) -> Result<(), DbError> {
        // Nested transaction via SAVEPOINT
        debug!(target: "reify::postgres", "SAVEPOINT nested_txn");
        self.conn.execute("SAVEPOINT nested_txn", &[]).await.map_err(pg_err)?;
        match f(self).await {
            Ok(()) => {
                self.conn.execute("RELEASE SAVEPOINT nested_txn", &[]).await.map_err(pg_err)?;
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute("ROLLBACK TO SAVEPOINT nested_txn", &[]).await;
                Err(e)
            }
        }
    }
}
