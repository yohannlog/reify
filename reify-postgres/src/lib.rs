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
    /// Maximum time to wait for a free connection in [`get_conn`]. Pre-fix,
    /// pool exhaustion (e.g. a long-lived `query_stream` that doesn't drain)
    /// froze every subsequent query indefinitely with no observable error.
    /// With this guard a stalled pool surfaces as `DbError::Connection`
    /// after the configured deadline so callers can fail fast.
    acquire_timeout: std::time::Duration,
}

/// Default upper bound for [`PostgresDb::with_acquire_timeout`] (30 s).
pub const DEFAULT_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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

        Ok(Self {
            pool,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
        })
    }

    /// Build a `PostgresDb` from an already-constructed `deadpool_postgres::Pool`.
    pub fn from_pool(pool: Pool) -> Self {
        Self {
            pool,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
        }
    }

    /// Override the maximum time spent waiting for a pooled connection.
    ///
    /// Defaults to [`DEFAULT_ACQUIRE_TIMEOUT`] (30 s). A `Duration::ZERO` is
    /// rejected by `tokio::time::timeout` (it always yields `Err`); use a
    /// very small duration like `Duration::from_millis(1)` to effectively
    /// disable waiting.
    pub fn with_acquire_timeout(mut self, dur: std::time::Duration) -> Self {
        self.acquire_timeout = dur;
        self
    }

    /// Currently-configured pool acquisition timeout.
    pub fn acquire_timeout(&self) -> std::time::Duration {
        self.acquire_timeout
    }

    /// Like [`Database::query_stream`] but every `next().await` is bounded
    /// by an inter-row idle timeout.
    ///
    /// If no row arrives within `idle`, a single
    /// [`reify_core::db::DbError::Timeout`] is yielded
    /// and the stream ends. Dropping the returned stream cancels the
    /// underlying cursor and returns the connection to the pool.
    ///
    /// Use this when streaming to a slow / external consumer (HTTP client,
    /// network bridge, …) to bound how long the connection stays out of
    /// the pool when the consumer stalls.
    ///
    /// ```ignore
    /// let mut stream = db
    ///     .query_stream_idle(sql, params, std::time::Duration::from_secs(5))
    ///     .await?;
    /// while let Some(row) = stream.next().await {
    ///     match row {
    ///         Ok(r)  => /* … */,
    ///         Err(e) => /* timeout or driver error — break and drop */,
    ///     }
    /// }
    /// ```
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
            Value::U64(v) => {
                // PostgreSQL has no native u64 type. Bind as i64 (BIGINT)
                // when the value fits; refuse otherwise rather than
                // silently truncating.
                let signed = i64::try_from(*v).map_err(|_| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Value::U64({v}) exceeds i64::MAX and PostgreSQL has no \
                             native u64 type; bind as NUMERIC via to_string() or use a \
                             smaller value"
                        ),
                    )) as Box<dyn std::error::Error + Sync + Send>
                })?;
                signed.to_sql(ty, out)
            }
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
            Value::Duration(d) => {
                // PostgreSQL `INTERVAL` binary format: i64 microseconds,
                // i32 days, i32 months. A `chrono::Duration` carries no
                // calendar component, so days and months are zero —
                // PostgreSQL normalises microseconds into days as needed.
                use bytes::BufMut;
                let micros = d.num_microseconds().ok_or_else(|| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Value::Duration({d:?}) is too large to fit in PostgreSQL \
                             INTERVAL microseconds (i64); the chrono::Duration exceeds \
                             ~292 000 years"
                        ),
                    )) as Box<dyn std::error::Error + Sync + Send>
                })?;
                out.put_i64(micros);
                out.put_i32(0);
                out.put_i32(0);
                Ok(tokio_postgres::types::IsNull::No)
            }
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
            // Complex types
            Value::Point(p) => {
                // PostgreSQL POINT binary format: two f64 in network byte order
                use bytes::BufMut;
                out.put_f64(p.x());
                out.put_f64(p.y());
                Ok(tokio_postgres::types::IsNull::No)
            }
            Value::Inet(inet) => {
                // Use text representation — postgres-types handles INET natively
                inet.to_string().to_sql(ty, out)
            }
            Value::Cidr(cidr) => {
                // Use text representation
                cidr.to_string().to_sql(ty, out)
            }
            Value::MacAddr(mac) => {
                // PostgreSQL MACADDR binary format: 6 bytes
                use bytes::BufMut;
                out.put_slice(&mac.octets());
                Ok(tokio_postgres::types::IsNull::No)
            }
            Value::Interval(interval) => {
                // PostgreSQL INTERVAL binary format: i64 microseconds, i32 days, i32 months
                use bytes::BufMut;
                out.put_i64(interval.microseconds());
                out.put_i32(interval.days());
                out.put_i32(interval.months());
                Ok(tokio_postgres::types::IsNull::No)
            }
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
                | Type::POINT
                | Type::INET
                | Type::CIDR
                | Type::MACADDR
                | Type::INTERVAL
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

// ── PostgreSQL row → reify Row conversion ───────────────────────────

fn pg_row_to_row(row: &tokio_postgres::Row) -> Result<Row, DbError> {
    let columns: Vec<String> = row.columns().iter().map(|c| c.name().to_string()).collect();
    let mut values = Vec::with_capacity(row.columns().len());

    for (i, col) in row.columns().iter().enumerate() {
        values.push(pg_column_to_value(row, i, col.type_())?);
    }

    Ok(Row::new(columns, values))
}

/// Log a wire-format / decode failure for a PG range column.
///
/// Pre-fix, both `Ok(None)` (genuine NULL) and `Err(...)` (decode failure)
/// silently mapped to `Value::Null`, hiding data-integrity issues.
/// Now we surface decode errors at `warn` so operators can distinguish
/// real NULLs from corruption or column-type drift.
fn log_range_decode_err(
    row: &tokio_postgres::Row,
    idx: usize,
    pg_type: &str,
    e: &impl std::fmt::Display,
) {
    let column = row
        .columns()
        .get(idx)
        .map(|c| c.name())
        .unwrap_or("<unknown>");
    tracing::warn!(
        target: "reify::postgres",
        column = %column,
        column_idx = idx,
        pg_type = pg_type,
        error = %e,
        "Failed to decode PG range column; returning Value::Null. Possible \
         causes: corrupt wire format, column-type drift, or driver mismatch."
    );
}

fn pg_column_to_value(
    row: &tokio_postgres::Row,
    idx: usize,
    ty: &tokio_postgres::types::Type,
) -> Result<Value, DbError> {
    use tokio_postgres::types::Type;

    Ok(match *ty {
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
            Ok(None) => Value::Null,
            Ok(Some(raw)) => {
                let r = range_from_pg(raw, |b| {
                    use bytes::Buf;
                    if b.len() == 4 {
                        Some((&b[..]).get_i32())
                    } else {
                        None
                    }
                })?;
                Value::Int4Range(r)
            }
            Err(e) => {
                log_range_decode_err(row, idx, "int4range", &e);
                Value::Null
            }
        },
        Type::INT8_RANGE => match row.try_get::<_, Option<&[u8]>>(idx) {
            Ok(None) => Value::Null,
            Ok(Some(raw)) => {
                let r = range_from_pg(raw, |b| {
                    use bytes::Buf;
                    if b.len() == 8 {
                        Some((&b[..]).get_i64())
                    } else {
                        None
                    }
                })?;
                Value::Int8Range(r)
            }
            Err(e) => {
                log_range_decode_err(row, idx, "int8range", &e);
                Value::Null
            }
        },
        Type::TS_RANGE => match row.try_get::<_, Option<&[u8]>>(idx) {
            Ok(None) => Value::Null,
            Ok(Some(raw)) => {
                let r = range_from_pg(raw, |b| {
                    postgres_types::FromSql::from_sql(&Type::TIMESTAMP, b).ok()
                })?;
                Value::TsRange(r)
            }
            Err(e) => {
                log_range_decode_err(row, idx, "tsrange", &e);
                Value::Null
            }
        },
        Type::TSTZ_RANGE => match row.try_get::<_, Option<&[u8]>>(idx) {
            Ok(None) => Value::Null,
            Ok(Some(raw)) => {
                let r = range_from_pg(raw, |b| {
                    postgres_types::FromSql::from_sql(&Type::TIMESTAMPTZ, b).ok()
                })?;
                Value::TstzRange(r)
            }
            Err(e) => {
                log_range_decode_err(row, idx, "tstzrange", &e);
                Value::Null
            }
        },
        Type::DATE_RANGE => match row.try_get::<_, Option<&[u8]>>(idx) {
            Ok(None) => Value::Null,
            Ok(Some(raw)) => {
                let r = range_from_pg(raw, |b| {
                    postgres_types::FromSql::from_sql(&Type::DATE, b).ok()
                })?;
                Value::DateRange(r)
            }
            Err(e) => {
                log_range_decode_err(row, idx, "daterange", &e);
                Value::Null
            }
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
        Type::POINT => match row.try_get::<_, Option<&[u8]>>(idx) {
            Ok(Some(raw)) if raw.len() == 16 => {
                use bytes::Buf;
                let mut buf = raw;
                let x = buf.get_f64();
                let y = buf.get_f64();
                Value::Point(reify_core::types::Point::new(x, y))
            }
            _ => Value::Null,
        },
        Type::INET => row
            .try_get::<_, Option<std::net::IpAddr>>(idx)
            .ok()
            .flatten()
            .map(|ip| Value::Inet(reify_core::types::Inet::new(ip)))
            .unwrap_or(Value::Null),
        Type::CIDR => {
            // CIDR comes as text from postgres-types
            match row.try_get::<_, Option<String>>(idx) {
                Ok(Some(s)) => s
                    .parse::<reify_core::types::Cidr>()
                    .map(Value::Cidr)
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            }
        }
        Type::MACADDR => match row.try_get::<_, Option<&[u8]>>(idx) {
            Ok(Some(raw)) if raw.len() == 6 => {
                let mut octets = [0u8; 6];
                octets.copy_from_slice(raw);
                Value::MacAddr(reify_core::types::MacAddr::new(octets))
            }
            _ => Value::Null,
        },
        Type::INTERVAL => match row.try_get::<_, Option<&[u8]>>(idx) {
            Ok(Some(raw)) if raw.len() == 16 => {
                use bytes::Buf;
                let mut buf = raw;
                let microseconds = buf.get_i64();
                let days = buf.get_i32();
                let months = buf.get_i32();
                Value::Interval(reify_core::types::Interval::new(months, days, microseconds))
            }
            _ => Value::Null,
        },
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
    })
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

/// Acquire a pooled connection, mapping pool errors to `DbError` and
/// bounding the wait by `acquire_timeout`.
///
/// Pre-fix this just delegated to `pool.get().await` with no timeout, so a
/// stalled pool (e.g. all connections held by un-drained `query_stream`s)
/// froze callers indefinitely. The timeout converts pool exhaustion into
/// an observable `DbError::Connection`.
pub(crate) async fn get_conn(
    pool: &Pool,
    acquire_timeout: std::time::Duration,
) -> Result<deadpool_postgres::Object, DbError> {
    match tokio::time::timeout(acquire_timeout, pool.get()).await {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(e)) => Err(DbError::Connection(e.to_string())),
        Err(_) => Err(DbError::Connection(format!(
            "pool acquisition timed out after {}ms; check for streams or \
             transactions holding connections",
            acquire_timeout.as_millis()
        ))),
    }
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
    rows.iter().map(pg_row_to_row).collect()
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
    pg_row_to_row(&row)
}

// ── Database trait implementation ───────────────────────────────────

impl Database for PostgresDb {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        let conn = get_conn(&self.pool, self.acquire_timeout).await?;
        pg_execute(&conn, sql, params).await
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let conn = get_conn(&self.pool, self.acquire_timeout).await?;
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
        let conn = get_conn(&self.pool, self.acquire_timeout).await?;
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
                row_stream.next().await.map(|res| {
                    (
                        match res {
                            Ok(r) => pg_row_to_row(&r),
                            Err(e) => Err(pg_err(e)),
                        },
                        (row_stream, conn),
                    )
                })
            });

        Ok(Box::pin(stream))
    }

    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        let conn = get_conn(&self.pool, self.acquire_timeout).await?;
        pg_query_one(&conn, sql, params).await
    }

    async fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> Result<(), DbError> {
        debug!(target: "reify::postgres", "BEGIN transaction");
        let conn = get_conn(&self.pool, self.acquire_timeout).await?;
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

    fn dialect(&self) -> reify_core::query::Dialect {
        reify_core::query::Dialect::Postgres
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_from_pg_corrupt_data() {
        let corrupt_raw = &[];
        let result = range_from_pg(corrupt_raw, |b| if b.len() == 4 { Some(0) } else { None });

        assert!(matches!(result, Err(DbError::Conversion(_))));
    }

    #[test]
    fn test_range_from_pg_element_decode_failure() {
        let valid_envelope_but_parse_fail = &[
            0x01 | 0x02 | 0x04,
            0x00,
            0x00,
            0x00,
            0x04,
            0x00,
            0x00,
            0x00,
            0x01,
            0x00,
            0x00,
            0x00,
            0x04,
            0x00,
            0x00,
            0x00,
            0x02,
        ];

        let result = range_from_pg::<i32, _>(valid_envelope_but_parse_fail, |_| None);

        assert!(
            matches!(result, Err(DbError::Conversion(msg)) if msg == "range element decode failed")
        );
    }
}
