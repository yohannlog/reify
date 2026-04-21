//! PostgreSQL adapter for Reify.
//!
//! Uses [`deadpool_postgres::Config`] directly for full configuration control
//! (host, port, SSL, keepalives, pool sizing, timeouts, etc.).
//!
//! ```ignore
//! use reify_postgres::PostgresDb;
//! use deadpool_postgres::{Config, Runtime, PoolConfig, Timeouts};
//! use tokio_postgres::NoTls;
//!
//! let mut cfg = Config::new();
//! cfg.host = Some("localhost".into());
//! cfg.user = Some("app".into());
//! cfg.dbname = Some("mydb".into());
//! cfg.pool = Some(PoolConfig { max_size: 16, ..Default::default() });
//!
//! let db = PostgresDb::connect(cfg, NoTls).await?;
//! let rows = reify_core::fetch_all(&db, &User::find().filter(User::id.eq(1i64))).await?;
//! ```

pub use deadpool_postgres::{self, Config as DpConfig, Pool, Runtime};
pub use tokio_postgres::{self, NoTls};

use tokio_postgres::types::ToSql as PgToSql;
use tracing::{debug, error};

use reify_core::adapter::SavepointCounter;
use reify_core::db::{Database, DbError, Row, TransactionFn};
use reify_core::range::{Bound, Range};
use reify_core::value::Value;

/// PostgreSQL database backed by a `deadpool-postgres` connection pool.
pub struct PostgresDb {
    pool: Pool,
}

impl PostgresDb {
    /// Connect to a PostgreSQL database using a [`deadpool_postgres::Config`].
    ///
    /// This gives you full control over every connection and pool parameter
    /// (host, port, SSL mode, keepalives, pool size, timeouts, etc.).
    ///
    /// ```ignore
    /// use deadpool_postgres::{Config, Runtime};
    /// use tokio_postgres::NoTls;
    ///
    /// let mut cfg = Config::new();
    /// cfg.host = Some("localhost".into());
    /// cfg.user = Some("app".into());
    /// cfg.dbname = Some("mydb".into());
    ///
    /// let db = PostgresDb::connect(cfg, NoTls).await?;
    /// ```
    pub async fn connect<T>(config: DpConfig, tls: T) -> Result<Self, DbError>
    where
        T: tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>
            + Clone
            + Sync
            + Send
            + 'static,
        T::Stream: Sync + Send,
        T::TlsConnect: Sync + Send,
        <T::TlsConnect as tokio_postgres::tls::TlsConnect<tokio_postgres::Socket>>::Future: Send,
    {
        debug!(target: "reify::postgres", ?config, "Connecting to PostgreSQL (pool)");

        let pool = config
            .create_pool(Some(Runtime::Tokio1), tls)
            .map_err(|e| DbError::Connection(e.to_string()))?;

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
pub(crate) struct PgValue<'a>(&'a Value);

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
            Value::F32(v) => {
                use tokio_postgres::types::Type;
                if *ty == Type::FLOAT4 {
                    v.to_sql(ty, out)
                } else {
                    (*v as f64).to_sql(ty, out)
                }
            }
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

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        use tokio_postgres::types::Type;
        matches!(
            *ty,
            Type::BOOL
                | Type::INT2
                | Type::INT4
                | Type::INT8
                | Type::FLOAT4
                | Type::FLOAT8
                | Type::TEXT
                | Type::VARCHAR
                | Type::BYTEA
                | Type::UUID
                | Type::TIMESTAMPTZ
                | Type::TIMESTAMP
                | Type::DATE
                | Type::TIME
                | Type::JSON
                | Type::JSONB
                | Type::INT4_RANGE
                | Type::INT8_RANGE
                | Type::TS_RANGE
                | Type::TSTZ_RANGE
                | Type::DATE_RANGE
                | Type::BOOL_ARRAY
                | Type::INT2_ARRAY
                | Type::INT4_ARRAY
                | Type::INT8_ARRAY
                | Type::FLOAT4_ARRAY
                | Type::FLOAT8_ARRAY
                | Type::TEXT_ARRAY
                | Type::VARCHAR_ARRAY
                | Type::UUID_ARRAY
        )
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
                    Bound::Inclusive(v) => Ok(RangeBound::Inclusive(convert_is_null(
                        v.to_sql(&et_lower, buf)?,
                    ))),
                    Bound::Exclusive(v) => Ok(RangeBound::Exclusive(convert_is_null(
                        v.to_sql(&et_lower, buf)?,
                    ))),
                    Bound::Unbounded => Ok(RangeBound::Unbounded),
                },
                |buf| match upper {
                    Bound::Inclusive(v) => Ok(RangeBound::Inclusive(convert_is_null(
                        v.to_sql(&et_upper, buf)?,
                    ))),
                    Bound::Exclusive(v) => Ok(RangeBound::Exclusive(convert_is_null(
                        v.to_sql(&et_upper, buf)?,
                    ))),
                    Bound::Unbounded => Ok(RangeBound::Unbounded),
                },
                out,
            )?;
        }
    }
    Ok(())
}

/// Deserialize a PostgreSQL range from raw bytes into a `Range<T>`.
///
/// Returns `Err(DbError::Conversion)` if the wire-format envelope is invalid
/// or any element fails to decode, so callers never silently observe an
/// `Empty` range in place of corrupt data.
fn range_from_pg<T, F>(raw: &[u8], parse_element: F) -> Result<Range<T>, DbError>
where
    F: Fn(&[u8]) -> Option<T>,
{
    use postgres_protocol::types::{RangeBound as PgBound, range_from_sql};

    let parsed = range_from_sql(raw)
        .map_err(|e| DbError::Conversion(format!("invalid PostgreSQL range wire format: {e}")))?;

    fn decode<T, F>(b: PgBound<Option<&[u8]>>, parse: &F) -> Result<Bound<T>, DbError>
    where
        F: Fn(&[u8]) -> Option<T>,
    {
        Ok(match b {
            PgBound::Inclusive(Some(bytes)) => {
                Bound::Inclusive(parse(bytes).ok_or_else(|| {
                    DbError::Conversion("range element decode failed".to_string())
                })?)
            }
            PgBound::Exclusive(Some(bytes)) => {
                Bound::Exclusive(parse(bytes).ok_or_else(|| {
                    DbError::Conversion("range element decode failed".to_string())
                })?)
            }
            PgBound::Inclusive(None) | PgBound::Exclusive(None) | PgBound::Unbounded => {
                Bound::Unbounded
            }
        })
    }

    Ok(match parsed {
        postgres_protocol::types::Range::Empty => Range::Empty,
        postgres_protocol::types::Range::Nonempty(lower, upper) => Range::Nonempty(
            decode(lower, &parse_element)?,
            decode(upper, &parse_element)?,
        ),
    })
}

/// Convenience for column-level range decoding: map a `DbError` to a
/// logged warning + `Value::Null` so one malformed row doesn't abort the
/// entire result set, but the error is still visible in logs.
fn range_value_or_null<T, F>(raw: &[u8], ctor: fn(Range<T>) -> Value, parse_element: F) -> Value
where
    F: Fn(&[u8]) -> Option<T>,
{
    match range_from_pg(raw, parse_element) {
        Ok(r) => ctor(r),
        Err(e) => {
            tracing::warn!(
                target: "reify::postgres",
                error = %e,
                "Failed to deserialize PostgreSQL range column — returning Null"
            );
            Value::Null
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
        Type::INT4_RANGE => match row.try_get::<_, Option<&[u8]>>(idx) {
            Ok(Some(raw)) => range_value_or_null(raw, Value::Int4Range, |b| {
                use bytes::Buf;
                if b.len() == 4 {
                    Some((&b[..]).get_i32())
                } else {
                    None
                }
            }),
            _ => Value::Null,
        },
        Type::INT8_RANGE => match row.try_get::<_, Option<&[u8]>>(idx) {
            Ok(Some(raw)) => range_value_or_null(raw, Value::Int8Range, |b| {
                use bytes::Buf;
                if b.len() == 8 {
                    Some((&b[..]).get_i64())
                } else {
                    None
                }
            }),
            _ => Value::Null,
        },
        Type::TS_RANGE => match row.try_get::<_, Option<&[u8]>>(idx) {
            Ok(Some(raw)) => range_value_or_null(raw, Value::TsRange, |b| {
                postgres_types::FromSql::from_sql(&Type::TIMESTAMP, b).ok()
            }),
            _ => Value::Null,
        },
        Type::TSTZ_RANGE => match row.try_get::<_, Option<&[u8]>>(idx) {
            Ok(Some(raw)) => range_value_or_null(raw, Value::TstzRange, |b| {
                postgres_types::FromSql::from_sql(&Type::TIMESTAMPTZ, b).ok()
            }),
            _ => Value::Null,
        },
        Type::DATE_RANGE => match row.try_get::<_, Option<&[u8]>>(idx) {
            Ok(Some(raw)) => range_value_or_null(raw, Value::DateRange, |b| {
                postgres_types::FromSql::from_sql(&Type::DATE, b).ok()
            }),
            _ => Value::Null,
        },
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
        _ => {
            // Unknown PostgreSQL type — fall back to text representation.
            // Log a warning so users know which type OID is not natively mapped.
            let oid = ty.oid();
            let type_name = ty.name();
            tracing::warn!(
                target: "reify::postgres",
                oid,
                type_name,
                column_index = idx,
                "Unknown PostgreSQL column type — falling back to String representation. \
                 Consider opening an issue or using a raw SQL query if precision is required."
            );
            row.try_get::<_, Option<String>>(idx)
                .ok()
                .flatten()
                .map(Value::String)
                .unwrap_or(Value::Null)
        }
    }
}

// ── Error conversion helpers ─────────────────────────────────────────

/// Map a `tokio_postgres::Error` to a `DbError`, promoting constraint
/// violations (SQLSTATE class 23) to `DbError::Constraint`.
pub(crate) fn pg_err(e: tokio_postgres::Error) -> DbError {
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
pub(crate) async fn get_conn(pool: &Pool) -> Result<deadpool_postgres::Object, DbError> {
    pool.get()
        .await
        .map_err(|e| DbError::Connection(e.to_string()))
}

/// Rewrite `?` placeholders to PostgreSQL-style `$1, $2, …` positional params.
///
/// Raw SQL helpers (e.g. `raw_execute`, `MigrationRunner::mark_applied`) use `?`
/// as the canonical placeholder. The postgres adapter normalises them to `$N`
/// at execution time so callers never need to know which dialect they're on.
/// SQL that already uses `$N` (from `build_pg()`) passes through unchanged.
fn rewrite_placeholders_pg(sql: &str) -> String {
    reify_core::query::rewrite_placeholders_pg(sql)
}

/// Rewrite placeholders and marshal `Value` params into postgres-typed refs.
///
/// Returns `(rewritten_sql, owned_wrappers, param_refs)`. The refs borrow from
/// the owned wrappers, so all three must be kept alive for the duration of the
/// client call.
fn prepare_pg_params<'a>(
    sql: &str,
    params: &'a [Value],
    pg_params: &'a mut Vec<PgValue<'a>>,
) -> (String, Vec<&'a (dyn PgToSql + Sync)>) {
    let sql = rewrite_placeholders_pg(sql);
    *pg_params = params.iter().map(PgValue).collect();
    let param_refs: Vec<&(dyn PgToSql + Sync)> = pg_params
        .iter()
        .map(|p| p as &(dyn PgToSql + Sync))
        .collect();
    (sql, param_refs)
}

/// Prepare params and execute a statement on a `tokio_postgres::Client`.
async fn pg_execute(
    client: &tokio_postgres::Client,
    sql: &str,
    params: &[Value],
) -> Result<u64, DbError> {
    let mut pg_params = Vec::new();
    let (sql, param_refs) = prepare_pg_params(sql, params, &mut pg_params);
    debug!(target: "reify::postgres", sql = %sql, "Executing");
    client.execute(&sql, &param_refs[..]).await.map_err(pg_err)
}

/// Prepare params and run a query on a `tokio_postgres::Client`.
async fn pg_query(
    client: &tokio_postgres::Client,
    sql: &str,
    params: &[Value],
) -> Result<Vec<Row>, DbError> {
    let mut pg_params = Vec::new();
    let (sql, param_refs) = prepare_pg_params(sql, params, &mut pg_params);
    debug!(target: "reify::postgres", sql = %sql, "Querying");
    let rows = client.query(&sql, &param_refs[..]).await.map_err(pg_err)?;
    Ok(rows.iter().map(pg_row_to_row).collect())
}

/// Prepare params and run a query returning exactly one row.
async fn pg_query_one(
    client: &tokio_postgres::Client,
    sql: &str,
    params: &[Value],
) -> Result<Row, DbError> {
    let mut pg_params = Vec::new();
    let (sql, param_refs) = prepare_pg_params(sql, params, &mut pg_params);
    debug!(target: "reify::postgres", sql = %sql, "Querying one");
    let row = client
        .query_one(&sql, &param_refs[..])
        .await
        .map_err(pg_err)?;
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

    /// # Connection lifecycle
    ///
    /// The underlying connection is held for the **entire lifetime of the stream**
    /// and returned to the pool only when the stream is **dropped**. If the stream
    /// is never fully consumed (e.g. via `take(n)` or an early `break`), the
    /// connection is still returned to the pool once the stream value is dropped —
    /// but it will remain checked out until that point.
    ///
    /// # Pool exhaustion warning
    ///
    /// Avoid holding streams across long-running operations or storing them in
    /// long-lived data structures. Each live stream holds one connection from the
    /// pool. If `pool.max_size` streams are held simultaneously, all subsequent
    /// `get_conn` calls will block until a stream is dropped. Always drop the
    /// stream as soon as you are done consuming it, or wrap it in a
    /// `tokio::time::timeout` to bound the maximum hold time.
    async fn query_stream<'a>(
        &'a self,
        sql: String,
        params: Vec<Value>,
    ) -> Result<reify_core::db::BoxStream<'a, Row>, DbError> {
        let conn = get_conn(&self.pool).await?;
        let sql = rewrite_placeholders_pg(&sql);
        let pg_params: Vec<PgValue> = params.iter().map(PgValue).collect();
        let param_refs: Vec<&(dyn PgToSql + Sync)> = pg_params
            .iter()
            .map(|p| p as &(dyn PgToSql + Sync))
            .collect();

        debug!(target: "reify::postgres", sql = %sql, "Querying (stream)");
        let row_stream = Box::pin(conn.query_raw(&sql, param_refs).await.map_err(pg_err)?);

        let stream =
            futures_util::stream::unfold((row_stream, conn), |(mut row_stream, conn)| async move {
                use futures_util::StreamExt;
                match row_stream.next().await {
                    Some(res) => Some((
                        res.map(|r| pg_row_to_row(&r)).map_err(pg_err),
                        (row_stream, conn),
                    )),
                    None => None,
                }
            });

        Ok(Box::pin(stream))
    }

    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        let conn = get_conn(&self.pool).await?;
        pg_query_one(&conn, sql, params).await
    }

    async fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> Result<(), DbError> {
        debug!(target: "reify::postgres", "BEGIN transaction");
        let conn = get_conn(&self.pool).await?;
        conn.execute("BEGIN", &[]).await.map_err(pg_err)?;

        let txn = PgTransaction {
            conn,
            savepoint_counter: SavepointCounter::new(),
        };
        match f(&txn).await {
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
    /// Monotonically-increasing counter for generating unique SAVEPOINT names
    /// within this connection. Shared implementation lives in
    /// [`reify_core::adapter::SavepointCounter`].
    savepoint_counter: SavepointCounter,
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

    async fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> Result<(), DbError> {
        // Nested transaction via SAVEPOINT. `SavepointCounter` guarantees a
        // distinct name for every call on this connection.
        let sp_name = self.savepoint_counter.next_name();
        debug!(target: "reify::postgres", savepoint = %sp_name, "SAVEPOINT (nested)");
        self.conn
            .execute(&format!("SAVEPOINT {sp_name}"), &[])
            .await
            .map_err(pg_err)?;
        match f(self).await {
            Ok(()) => {
                self.conn
                    .execute(&format!("RELEASE SAVEPOINT {sp_name}"), &[])
                    .await
                    .map_err(pg_err)?;
                Ok(())
            }
            Err(e) => {
                let _ = self
                    .conn
                    .execute(&format!("ROLLBACK TO SAVEPOINT {sp_name}"), &[])
                    .await;
                Err(e)
            }
        }
    }
}

mod copy;
