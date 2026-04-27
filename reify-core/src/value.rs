/// A dynamically-typed SQL value used for parameter binding.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    /// Unsigned 64-bit integer.
    ///
    /// Primarily produced by the MySQL adapter for `BIGINT UNSIGNED` columns
    /// whose values exceed `i64::MAX` — pre-fix, those were silently coerced
    /// to `Value::String`, breaking type-driven business logic. Adapters bind
    /// this back to the database's BIGINT type when the value fits in `i64`,
    /// and error otherwise (PostgreSQL has no native `u64`; SQLite stores
    /// integers as `i64`).
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),

    // ── PostgreSQL-specific types ──────────────────────────────────
    #[cfg(feature = "postgres")]
    Uuid(uuid::Uuid),
    #[cfg(feature = "postgres")]
    Timestamptz(chrono::DateTime<chrono::Utc>),
    #[cfg(feature = "postgres")]
    Jsonb(serde_json::Value),

    // ── Shared temporal types (PostgreSQL & MySQL) ─────────────────
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    Timestamp(chrono::NaiveDateTime),
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    Date(chrono::NaiveDate),
    /// Wall-clock time of day (`00:00:00` to `23:59:59.999999`).
    ///
    /// Use [`Value::Duration`] for MySQL `TIME` columns, which are signed
    /// intervals (`-838:59:59` to `+838:59:59`) and may exceed 24 h.
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    Time(chrono::NaiveTime),
    /// Signed time interval / duration.
    ///
    /// Use this for any column that semantically represents a duration
    /// (positive or negative, possibly exceeding 24 h).
    ///
    /// ## Adapter mapping
    ///
    /// - **MySQL**: maps to native `TIME`. MySQL `TIME` is **not** a
    ///   wall-clock type — it is a signed interval covering
    ///   `-838:59:59.999999` to `+838:59:59.999999`. Pre-fix, negative
    ///   values silently became `Value::Null` and the `days` component
    ///   was dropped, so durations > 24 h round-tripped lossily.
    /// - **PostgreSQL**: maps to native `INTERVAL`. PostgreSQL `TIME` is
    ///   a wall-clock type (`00:00:00` … `24:00:00`) and rejects
    ///   negatives, which is why we don't bind `Duration` to `TIME` on
    ///   PG. Use [`Value::Time`] for wall-clock columns instead.
    /// - **SQLite**: bound as `TEXT` in the canonical
    ///   `[-]HHH:MM:SS[.ffffff]` format (no native interval type).
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    Duration(chrono::Duration),

    // ── PostgreSQL range types ────────────────────────────────────────
    /// `int4range` — range of `i32`.
    #[cfg(feature = "postgres")]
    Int4Range(crate::range::Range<i32>),
    /// `int8range` — range of `i64`.
    #[cfg(feature = "postgres")]
    Int8Range(crate::range::Range<i64>),
    /// `tsrange` — range of `NaiveDateTime` (timestamp without time zone).
    #[cfg(feature = "postgres")]
    TsRange(crate::range::Range<chrono::NaiveDateTime>),
    /// `tstzrange` — range of `DateTime<Utc>` (timestamp with time zone).
    #[cfg(feature = "postgres")]
    TstzRange(crate::range::Range<chrono::DateTime<chrono::Utc>>),
    /// `daterange` — range of `NaiveDate`.
    #[cfg(feature = "postgres")]
    DateRange(crate::range::Range<chrono::NaiveDate>),

    // ── PostgreSQL array types ───────────────────────────────────────
    /// `bool[]`
    #[cfg(feature = "postgres")]
    ArrayBool(Vec<bool>),
    /// `int2[]`
    #[cfg(feature = "postgres")]
    ArrayI16(Vec<i16>),
    /// `int4[]`
    #[cfg(feature = "postgres")]
    ArrayI32(Vec<i32>),
    /// `int8[]`
    #[cfg(feature = "postgres")]
    ArrayI64(Vec<i64>),
    /// `float4[]`
    #[cfg(feature = "postgres")]
    ArrayF32(Vec<f32>),
    /// `float8[]`
    #[cfg(feature = "postgres")]
    ArrayF64(Vec<f64>),
    /// `text[]` / `varchar[]`
    #[cfg(feature = "postgres")]
    ArrayString(Vec<String>),
    /// `uuid[]`
    #[cfg(feature = "postgres")]
    ArrayUuid(Vec<uuid::Uuid>),

    // ── Complex types (PostgreSQL) ──────────────────────────────────
    /// `POINT` — 2D geometric point.
    #[cfg(feature = "postgres")]
    Point(crate::types::Point),
    /// `INET` — IP address (IPv4 or IPv6).
    #[cfg(feature = "postgres")]
    Inet(crate::types::Inet),
    /// `CIDR` — Network address with prefix.
    #[cfg(feature = "postgres")]
    Cidr(crate::types::Cidr),
    /// `MACADDR` — MAC address.
    #[cfg(feature = "postgres")]
    MacAddr(crate::types::MacAddr),
    /// `INTERVAL` — Time interval.
    #[cfg(feature = "postgres")]
    Interval(crate::types::Interval),
}

impl Value {
    /// Return the `SqlType` that corresponds to this value variant.
    pub fn sql_type(&self) -> crate::schema::SqlType {
        use crate::schema::SqlType;
        match self {
            Value::Null => SqlType::Text,
            Value::Bool(_) => SqlType::Boolean,
            Value::I16(_) => SqlType::SmallInt,
            Value::I32(_) => SqlType::Integer,
            Value::I64(_) => SqlType::BigInt,
            Value::U64(_) => SqlType::BigInt,
            Value::F32(_) => SqlType::Float,
            Value::F64(_) => SqlType::Double,
            Value::String(_) => SqlType::Text,
            Value::Bytes(_) => SqlType::Bytea,
            #[cfg(feature = "postgres")]
            Value::Uuid(_) => SqlType::Uuid,
            #[cfg(feature = "postgres")]
            Value::Timestamptz(_) => SqlType::Timestamptz,
            #[cfg(feature = "postgres")]
            Value::Jsonb(_) => SqlType::Jsonb,
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            Value::Timestamp(_) => SqlType::Timestamp,
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            Value::Date(_) => SqlType::Date,
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            Value::Time(_) => SqlType::Time,
            // PostgreSQL `TIME` is a wall-clock type and explicitly
            // rejects negatives; the right SQL type for a signed duration
            // is `INTERVAL`. MySQL has no `INTERVAL` column type, so the
            // dialect-aware schema renderer maps `SqlType::Interval` to
            // MySQL `TIME` (which *is* a signed interval). Either way,
            // `SqlType::Interval` is the correct portable shape.
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            Value::Duration(_) => SqlType::Interval,
            // Ranges and arrays fall back to Text — they are PG-specific
            // and don't have a single portable SQL type.
            #[cfg(feature = "postgres")]
            Value::Int4Range(_)
            | Value::Int8Range(_)
            | Value::TsRange(_)
            | Value::TstzRange(_)
            | Value::DateRange(_) => SqlType::Text,
            #[cfg(feature = "postgres")]
            Value::ArrayBool(_)
            | Value::ArrayI16(_)
            | Value::ArrayI32(_)
            | Value::ArrayI64(_)
            | Value::ArrayF32(_)
            | Value::ArrayF64(_)
            | Value::ArrayString(_)
            | Value::ArrayUuid(_) => SqlType::Text,
            #[cfg(feature = "postgres")]
            Value::Point(_) => SqlType::Point,
            #[cfg(feature = "postgres")]
            Value::Inet(_) => SqlType::Inet,
            #[cfg(feature = "postgres")]
            Value::Cidr(_) => SqlType::Cidr,
            #[cfg(feature = "postgres")]
            Value::MacAddr(_) => SqlType::MacAddr,
            #[cfg(feature = "postgres")]
            Value::Interval(_) => SqlType::Interval,
        }
    }

    /// Render this value as a SQL literal (for use in generated SQL fragments
    /// like `COALESCE(col, <literal>)`).
    ///
    /// Strings are single-quoted with inner quotes escaped. `NULL` is rendered
    /// as the keyword `NULL`. Numeric and boolean types use their natural
    /// representation.
    pub fn to_sql_literal(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            Value::I16(v) => v.to_string(),
            Value::I32(v) => v.to_string(),
            Value::I64(v) => v.to_string(),
            Value::U64(v) => v.to_string(),
            Value::F32(v) => {
                if v.is_nan() {
                    "'NaN'::float4".to_string()
                } else if v.is_infinite() {
                    if *v > 0.0 {
                        "'Infinity'::float4".to_string()
                    } else {
                        "'-Infinity'::float4".to_string()
                    }
                } else {
                    v.to_string()
                }
            }
            Value::F64(v) => {
                if v.is_nan() {
                    "'NaN'::float8".to_string()
                } else if v.is_infinite() {
                    if *v > 0.0 {
                        "'Infinity'::float8".to_string()
                    } else {
                        "'-Infinity'::float8".to_string()
                    }
                } else {
                    v.to_string()
                }
            }
            Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Bytes(_) => "NULL".to_string(),
            #[cfg(feature = "postgres")]
            Value::Uuid(u) => format!("'{u}'"),
            #[cfg(feature = "postgres")]
            Value::Timestamptz(t) => format!("'{t}'"),
            #[cfg(feature = "postgres")]
            Value::Jsonb(j) => format!("'{}'", j.to_string().replace('\'', "''")),
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            Value::Timestamp(t) => format!("'{t}'"),
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            Value::Date(d) => format!("'{d}'"),
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            Value::Time(t) => format!("'{t}'"),
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            Value::Duration(d) => format!("'{}'", format_mysql_time(*d)),
            // Ranges and arrays — fall back to NULL for literal rendering.
            #[cfg(feature = "postgres")]
            Value::Int4Range(_)
            | Value::Int8Range(_)
            | Value::TsRange(_)
            | Value::TstzRange(_)
            | Value::DateRange(_) => "NULL".to_string(),
            #[cfg(feature = "postgres")]
            Value::ArrayBool(_)
            | Value::ArrayI16(_)
            | Value::ArrayI32(_)
            | Value::ArrayI64(_)
            | Value::ArrayF32(_)
            | Value::ArrayF64(_)
            | Value::ArrayString(_)
            | Value::ArrayUuid(_) => "NULL".to_string(),
            // Complex types — render as PostgreSQL literals
            #[cfg(feature = "postgres")]
            Value::Point(p) => format!("'{}'::point", p),
            #[cfg(feature = "postgres")]
            Value::Inet(i) => format!("'{}'::inet", i),
            #[cfg(feature = "postgres")]
            Value::Cidr(c) => format!("'{}'::cidr", c),
            #[cfg(feature = "postgres")]
            Value::MacAddr(m) => format!("'{}'::macaddr", m),
            #[cfg(feature = "postgres")]
            Value::Interval(i) => format!("'{}'::interval", i),
        }
    }
}

/// Format a [`chrono::Duration`] as a MySQL `TIME` string
/// (`[-]HHH:MM:SS` or `[-]HHH:MM:SS.ffffff` when fractional seconds are
/// non-zero).
///
/// Used by [`Value::Duration::to_sql_literal`](Value::to_sql_literal) and
/// by the MySQL adapter when binding durations as text. Values outside
/// MySQL's documented range (`±838:59:59.999999`) are still formatted
/// literally; MySQL itself will clip on insert.
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub fn format_mysql_time(d: chrono::Duration) -> String {
    // num_microseconds returns None on chrono::Duration > i64::MAX µs
    // (~292 000 years). Saturate so we always produce a finite string.
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
    let hours = total_secs / 3600;
    let sign = if neg { "-" } else { "" };
    if micros == 0 {
        format!("{sign}{hours:02}:{mins:02}:{secs:02}")
    } else {
        format!("{sign}{hours:02}:{mins:02}:{secs:02}.{micros:06}")
    }
}

/// Trait for types that can be converted into a `Value`.
pub trait IntoValue {
    fn into_value(self) -> Value;
}

// ── Payload size caps ───────────────────────────────────────────────

/// Default upper bound for a single `Value::String` (1 MiB).
///
/// Raised or lowered at runtime via [`set_value_string_limit`]. This is a
/// defence-in-depth limit against unbounded client-supplied payloads — real
/// per-column limits still come from the database schema.
pub const DEFAULT_VALUE_STRING_LIMIT: usize = 1 << 20;

/// Default upper bound for a single `Value::Bytes` (16 MiB).
pub const DEFAULT_VALUE_BYTES_LIMIT: usize = 16 << 20;

use std::sync::atomic::{AtomicUsize, Ordering};

static VALUE_STRING_LIMIT: AtomicUsize = AtomicUsize::new(DEFAULT_VALUE_STRING_LIMIT);
static VALUE_BYTES_LIMIT: AtomicUsize = AtomicUsize::new(DEFAULT_VALUE_BYTES_LIMIT);

/// Configure the maximum accepted length for `Value::String` payloads.
///
/// Set to `usize::MAX` to effectively disable the guard. Applies process-wide.
pub fn set_value_string_limit(max_bytes: usize) {
    VALUE_STRING_LIMIT.store(max_bytes, Ordering::Relaxed);
}

/// Configure the maximum accepted length for `Value::Bytes` payloads.
pub fn set_value_bytes_limit(max_bytes: usize) {
    VALUE_BYTES_LIMIT.store(max_bytes, Ordering::Relaxed);
}

/// Current maximum accepted length for `Value::String` payloads.
pub fn value_string_limit() -> usize {
    VALUE_STRING_LIMIT.load(Ordering::Relaxed)
}

/// Current maximum accepted length for `Value::Bytes` payloads.
pub fn value_bytes_limit() -> usize {
    VALUE_BYTES_LIMIT.load(Ordering::Relaxed)
}

/// Error returned by the checked constructors
/// [`Value::string_checked`] / [`Value::bytes_checked`] when the payload
/// exceeds the configured limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadTooLarge {
    pub kind: &'static str,
    pub size: usize,
    pub limit: usize,
}

impl std::fmt::Display for PayloadTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} payload is {} bytes, exceeds configured limit of {} bytes",
            self.kind, self.size, self.limit
        )
    }
}

impl std::error::Error for PayloadTooLarge {}

impl Value {
    /// Build a `Value::String` after checking the payload against the
    /// process-wide [`value_string_limit`].
    ///
    /// The plain `Value::String(s)` variant constructor is unchanged \u2014
    /// trusted internal callers can bypass the check. Adapters and public
    /// APIs that bind untrusted client input should prefer this constructor.
    pub fn string_checked(s: String) -> Result<Self, PayloadTooLarge> {
        let limit = value_string_limit();
        if s.len() > limit {
            return Err(PayloadTooLarge {
                kind: "Value::String",
                size: s.len(),
                limit,
            });
        }
        Ok(Value::String(s))
    }

    /// Build a `Value::Bytes` after checking the payload against the
    /// process-wide [`value_bytes_limit`].
    pub fn bytes_checked(b: Vec<u8>) -> Result<Self, PayloadTooLarge> {
        let limit = value_bytes_limit();
        if b.len() > limit {
            return Err(PayloadTooLarge {
                kind: "Value::Bytes",
                size: b.len(),
                limit,
            });
        }
        Ok(Value::Bytes(b))
    }
}

/// Trait for types that can be extracted from a `Value`.
///
/// Used by `#[derive(Table)]` to auto-generate `FromRow` implementations.
/// Each supported Rust type implements this to convert from the dynamically-typed
/// `Value` enum back into a concrete type.
pub trait FromValue: Sized {
    fn from_value(val: Value) -> Result<Self, String>;
}

impl FromValue for bool {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Bool(v) => Ok(v),
            Value::I64(v) => Ok(v != 0),
            Value::I32(v) => Ok(v != 0),
            Value::I16(v) => Ok(v != 0),
            _ => Err(format!("expected bool, got {:?}", val)),
        }
    }
}

impl FromValue for i16 {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::I16(v) => Ok(v),
            Value::I32(v) => i16::try_from(v).map_err(|e| e.to_string()),
            Value::I64(v) => i16::try_from(v).map_err(|e| e.to_string()),
            _ => Err(format!("expected i16, got {:?}", val)),
        }
    }
}

impl FromValue for i32 {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::I32(v) => Ok(v),
            Value::I16(v) => Ok(v as i32),
            Value::I64(v) => i32::try_from(v).map_err(|e| e.to_string()),
            _ => Err(format!("expected i32, got {:?}", val)),
        }
    }
}

impl FromValue for i64 {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::I64(v) => Ok(v),
            Value::I32(v) => Ok(v as i64),
            Value::I16(v) => Ok(v as i64),
            // Accept U64 if it fits; reject otherwise so callers don't
            // silently lose data.
            Value::U64(v) => i64::try_from(v).map_err(|e| e.to_string()),
            _ => Err(format!("expected i64, got {:?}", val)),
        }
    }
}

impl FromValue for u64 {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::U64(v) => Ok(v),
            Value::I64(v) => u64::try_from(v).map_err(|e| e.to_string()),
            Value::I32(v) => u64::try_from(v).map_err(|e| e.to_string()),
            Value::I16(v) => u64::try_from(v).map_err(|e| e.to_string()),
            _ => Err(format!("expected u64, got {:?}", val)),
        }
    }
}

impl FromValue for u8 {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::I16(v) => u8::try_from(v).map_err(|e| e.to_string()),
            Value::I32(v) => u8::try_from(v).map_err(|e| e.to_string()),
            Value::I64(v) => u8::try_from(v).map_err(|e| e.to_string()),
            Value::U64(v) => u8::try_from(v).map_err(|e| e.to_string()),
            _ => Err(format!("expected u8, got {:?}", val)),
        }
    }
}

impl FromValue for u16 {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::I16(v) => u16::try_from(v).map_err(|e| e.to_string()),
            Value::I32(v) => u16::try_from(v).map_err(|e| e.to_string()),
            Value::I64(v) => u16::try_from(v).map_err(|e| e.to_string()),
            Value::U64(v) => u16::try_from(v).map_err(|e| e.to_string()),
            _ => Err(format!("expected u16, got {:?}", val)),
        }
    }
}

impl FromValue for u32 {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::I32(v) => u32::try_from(v).map_err(|e| e.to_string()),
            Value::I16(v) => u32::try_from(v).map_err(|e| e.to_string()),
            Value::I64(v) => u32::try_from(v).map_err(|e| e.to_string()),
            Value::U64(v) => u32::try_from(v).map_err(|e| e.to_string()),
            _ => Err(format!("expected u32, got {:?}", val)),
        }
    }
}

impl FromValue for f32 {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::F32(v) => Ok(v),
            Value::F64(v) => Ok(v as f32),
            Value::I32(v) => Ok(v as f32),
            Value::I64(v) => Ok(v as f32),
            _ => Err(format!("expected f32, got {:?}", val)),
        }
    }
}

impl FromValue for f64 {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::F64(v) => Ok(v),
            Value::F32(v) => Ok(v as f64),
            Value::I32(v) => Ok(v as f64),
            Value::I64(v) => Ok(v as f64),
            _ => Err(format!("expected f64, got {:?}", val)),
        }
    }
}

impl FromValue for String {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::String(v) => Ok(v),
            _ => Err(format!("expected String, got {:?}", val)),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Bytes(v) => Ok(v),
            _ => Err(format!("expected Bytes, got {:?}", val)),
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Null => Ok(None),
            other => T::from_value(other).map(Some),
        }
    }
}

// ── PostgreSQL-specific FromValue impls ────────────────────────────

#[cfg(feature = "postgres")]
impl FromValue for uuid::Uuid {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Uuid(v) => Ok(v),
            Value::String(s) => s.parse().map_err(|e: uuid::Error| e.to_string()),
            _ => Err(format!("expected Uuid, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl FromValue for chrono::DateTime<chrono::Utc> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Timestamptz(v) => Ok(v),
            _ => Err(format!("expected Timestamptz, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl FromValue for serde_json::Value {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Jsonb(v) => Ok(v),
            Value::String(s) => serde_json::from_str(&s).map_err(|e| e.to_string()),
            _ => Err(format!("expected Jsonb, got {:?}", val)),
        }
    }
}

// ── Shared temporal FromValue impls (PostgreSQL & MySQL) ───────────

#[cfg(any(feature = "postgres", feature = "mysql"))]
impl FromValue for chrono::NaiveDateTime {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Timestamp(v) => Ok(v),
            _ => Err(format!("expected Timestamp, got {:?}", val)),
        }
    }
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
impl FromValue for chrono::NaiveDate {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Date(v) => Ok(v),
            _ => Err(format!("expected Date, got {:?}", val)),
        }
    }
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
impl FromValue for chrono::NaiveTime {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Time(v) => Ok(v),
            // Accept a Duration if it lies within the wall-clock range
            // [00:00:00, 24:00:00). Outside that range the conversion is
            // ambiguous and the caller probably wanted `chrono::Duration`.
            Value::Duration(d) => {
                let micros = d
                    .num_microseconds()
                    .ok_or_else(|| "Duration is too large to convert to NaiveTime".to_string())?;
                if !(0..86_400_000_000).contains(&micros) {
                    return Err(format!(
                        "Duration {d:?} is outside the NaiveTime range \
                         [00:00:00, 24:00:00); use chrono::Duration instead"
                    ));
                }
                let total_secs = (micros / 1_000_000) as u32;
                let micros_part = (micros % 1_000_000) as u32;
                chrono::NaiveTime::from_hms_micro_opt(
                    total_secs / 3600,
                    (total_secs / 60) % 60,
                    total_secs % 60,
                    micros_part,
                )
                .ok_or_else(|| "internal: failed to build NaiveTime".to_string())
            }
            _ => Err(format!("expected Time, got {:?}", val)),
        }
    }
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
impl FromValue for chrono::Duration {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Duration(d) => Ok(d),
            // `NaiveTime` is always non-negative and < 24h, so it converts
            // losslessly into a Duration.
            Value::Time(t) => {
                let h = t.format("%H").to_string().parse::<i64>().unwrap_or(0);
                let m = t.format("%M").to_string().parse::<i64>().unwrap_or(0);
                let s = t.format("%S").to_string().parse::<i64>().unwrap_or(0);
                let us = t.format("%f").to_string().parse::<i64>().unwrap_or(0) / 1_000;
                let micros = h * 3_600_000_000 + m * 60_000_000 + s * 1_000_000 + us;
                Ok(chrono::Duration::microseconds(micros))
            }
            _ => Err(format!("expected Duration, got {:?}", val)),
        }
    }
}

// ── PostgreSQL array FromValue impls ──────────────────────────────

#[cfg(feature = "postgres")]
impl FromValue for Vec<bool> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::ArrayBool(v) => Ok(v),
            _ => Err(format!("expected ArrayBool, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl FromValue for Vec<i16> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::ArrayI16(v) => Ok(v),
            _ => Err(format!("expected ArrayI16, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl FromValue for Vec<i32> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::ArrayI32(v) => Ok(v),
            _ => Err(format!("expected ArrayI32, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl FromValue for Vec<i64> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::ArrayI64(v) => Ok(v),
            _ => Err(format!("expected ArrayI64, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl FromValue for Vec<f32> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::ArrayF32(v) => Ok(v),
            _ => Err(format!("expected ArrayF32, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl FromValue for Vec<f64> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::ArrayF64(v) => Ok(v),
            _ => Err(format!("expected ArrayF64, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl FromValue for Vec<String> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::ArrayString(v) => Ok(v),
            _ => Err(format!("expected ArrayString, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl FromValue for Vec<uuid::Uuid> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::ArrayUuid(v) => Ok(v),
            _ => Err(format!("expected ArrayUuid, got {:?}", val)),
        }
    }
}

impl IntoValue for i16 {
    fn into_value(self) -> Value {
        Value::I16(self)
    }
}

impl IntoValue for i32 {
    fn into_value(self) -> Value {
        Value::I32(self)
    }
}

impl IntoValue for i64 {
    fn into_value(self) -> Value {
        Value::I64(self)
    }
}

// Unsigned integers: u8 / u16 / u32 always fit in their next-wider signed
// counterpart and are coerced losslessly. `u64` gets its own variant
// because values above `i64::MAX` cannot be represented as `i64`.
impl IntoValue for u8 {
    fn into_value(self) -> Value {
        Value::I16(self as i16)
    }
}

impl IntoValue for u16 {
    fn into_value(self) -> Value {
        Value::I32(self as i32)
    }
}

impl IntoValue for u32 {
    fn into_value(self) -> Value {
        Value::I64(self as i64)
    }
}

impl IntoValue for u64 {
    fn into_value(self) -> Value {
        Value::U64(self)
    }
}

impl IntoValue for f32 {
    fn into_value(self) -> Value {
        Value::F32(self)
    }
}

impl IntoValue for f64 {
    fn into_value(self) -> Value {
        Value::F64(self)
    }
}

impl IntoValue for bool {
    fn into_value(self) -> Value {
        Value::Bool(self)
    }
}

impl IntoValue for String {
    fn into_value(self) -> Value {
        Value::String(self)
    }
}

impl IntoValue for &str {
    fn into_value(self) -> Value {
        Value::String(self.to_owned())
    }
}

impl IntoValue for Vec<u8> {
    fn into_value(self) -> Value {
        Value::Bytes(self)
    }
}

impl<T: IntoValue> IntoValue for Option<T> {
    fn into_value(self) -> Value {
        match self {
            Some(v) => v.into_value(),
            None => Value::Null,
        }
    }
}

// ── PostgreSQL-specific IntoValue impls ────────────────────────────

#[cfg(feature = "postgres")]
impl IntoValue for uuid::Uuid {
    fn into_value(self) -> Value {
        Value::Uuid(self)
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for chrono::DateTime<chrono::Utc> {
    fn into_value(self) -> Value {
        Value::Timestamptz(self)
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for serde_json::Value {
    fn into_value(self) -> Value {
        Value::Jsonb(self)
    }
}

// ── PostgreSQL array IntoValue impls ──────────────────────────────

#[cfg(feature = "postgres")]
impl IntoValue for Vec<bool> {
    fn into_value(self) -> Value {
        Value::ArrayBool(self)
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for Vec<i16> {
    fn into_value(self) -> Value {
        Value::ArrayI16(self)
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for Vec<i32> {
    fn into_value(self) -> Value {
        Value::ArrayI32(self)
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for Vec<i64> {
    fn into_value(self) -> Value {
        Value::ArrayI64(self)
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for Vec<f32> {
    fn into_value(self) -> Value {
        Value::ArrayF32(self)
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for Vec<f64> {
    fn into_value(self) -> Value {
        Value::ArrayF64(self)
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for Vec<String> {
    fn into_value(self) -> Value {
        Value::ArrayString(self)
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for Vec<uuid::Uuid> {
    fn into_value(self) -> Value {
        Value::ArrayUuid(self)
    }
}

// ── Shared temporal IntoValue impls (PostgreSQL & MySQL) ───────────

#[cfg(any(feature = "postgres", feature = "mysql"))]
impl IntoValue for chrono::NaiveDateTime {
    fn into_value(self) -> Value {
        Value::Timestamp(self)
    }
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
impl IntoValue for chrono::NaiveDate {
    fn into_value(self) -> Value {
        Value::Date(self)
    }
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
impl IntoValue for chrono::NaiveTime {
    fn into_value(self) -> Value {
        Value::Time(self)
    }
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
impl IntoValue for chrono::Duration {
    fn into_value(self) -> Value {
        Value::Duration(self)
    }
}

// ── PostgreSQL range FromValue impls ──────────────────────────────

#[cfg(feature = "postgres")]
impl FromValue for crate::range::Range<i32> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Int4Range(v) => Ok(v),
            _ => Err(format!("expected Int4Range, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl FromValue for crate::range::Range<i64> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Int8Range(v) => Ok(v),
            _ => Err(format!("expected Int8Range, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl FromValue for crate::range::Range<chrono::NaiveDateTime> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::TsRange(v) => Ok(v),
            _ => Err(format!("expected TsRange, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl FromValue for crate::range::Range<chrono::DateTime<chrono::Utc>> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::TstzRange(v) => Ok(v),
            _ => Err(format!("expected TstzRange, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl FromValue for crate::range::Range<chrono::NaiveDate> {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::DateRange(v) => Ok(v),
            _ => Err(format!("expected DateRange, got {:?}", val)),
        }
    }
}

// ── Complex types: IntoValue / FromValue ────────────────────────────

#[cfg(feature = "postgres")]
impl IntoValue for crate::types::Point {
    fn into_value(self) -> Value {
        Value::Point(self)
    }
}

#[cfg(feature = "postgres")]
impl FromValue for crate::types::Point {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Point(v) => Ok(v),
            _ => Err(format!("expected Point, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for crate::types::Inet {
    fn into_value(self) -> Value {
        Value::Inet(self)
    }
}

#[cfg(feature = "postgres")]
impl FromValue for crate::types::Inet {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Inet(v) => Ok(v),
            _ => Err(format!("expected Inet, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for crate::types::Cidr {
    fn into_value(self) -> Value {
        Value::Cidr(self)
    }
}

#[cfg(feature = "postgres")]
impl FromValue for crate::types::Cidr {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Cidr(v) => Ok(v),
            _ => Err(format!("expected Cidr, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for crate::types::MacAddr {
    fn into_value(self) -> Value {
        Value::MacAddr(self)
    }
}

#[cfg(feature = "postgres")]
impl FromValue for crate::types::MacAddr {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::MacAddr(v) => Ok(v),
            _ => Err(format!("expected MacAddr, got {:?}", val)),
        }
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for crate::types::Interval {
    fn into_value(self) -> Value {
        Value::Interval(self)
    }
}

#[cfg(feature = "postgres")]
impl FromValue for crate::types::Interval {
    fn from_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Interval(v) => Ok(v),
            _ => Err(format!("expected Interval, got {:?}", val)),
        }
    }
}

#[cfg(test)]
mod u64_tests {
    //! Round-trip tests for `Value::U64`.
    //!
    //! Pre-fix, MySQL `BIGINT UNSIGNED` values exceeding `i64::MAX` were
    //! silently coerced to `Value::String`, breaking type-driven downstream
    //! logic. The dedicated variant restores type stability.

    use super::*;

    #[test]
    fn into_value_for_u64_uses_dedicated_variant() {
        assert_eq!(u64::MAX.into_value(), Value::U64(u64::MAX));
        assert_eq!(0u64.into_value(), Value::U64(0));
    }

    #[test]
    fn unsigned_smaller_than_u64_widens_losslessly() {
        // Pre-fix there was no IntoValue for u8/u16/u32 either; users had
        // to cast manually.
        assert_eq!(255u8.into_value(), Value::I16(255));
        assert_eq!(u16::MAX.into_value(), Value::I32(u16::MAX as i32));
        assert_eq!(u32::MAX.into_value(), Value::I64(u32::MAX as i64));
    }

    #[test]
    fn from_value_u64_round_trip() {
        assert_eq!(u64::from_value(Value::U64(u64::MAX)).unwrap(), u64::MAX);
    }

    #[test]
    fn from_value_u64_accepts_compatible_signed() {
        assert_eq!(u64::from_value(Value::I64(42)).unwrap(), 42);
        assert!(u64::from_value(Value::I64(-1)).is_err());
    }

    #[test]
    fn from_value_i64_accepts_u64_within_range() {
        assert_eq!(i64::from_value(Value::U64(42)).unwrap(), 42);
        // Above i64::MAX must error rather than silently wrapping.
        assert!(i64::from_value(Value::U64(u64::MAX)).is_err());
    }

    #[test]
    fn sql_type_for_u64_is_bigint() {
        use crate::schema::SqlType;
        assert_eq!(Value::U64(0).sql_type(), SqlType::BigInt);
    }

    #[test]
    fn to_sql_literal_for_u64_is_decimal() {
        assert_eq!(Value::U64(u64::MAX).to_sql_literal(), u64::MAX.to_string());
    }
}

#[cfg(all(test, any(feature = "postgres", feature = "mysql")))]
mod duration_tests {
    //! `Value::Duration` covers the full MySQL TIME range, including
    //! negative values and durations exceeding 24 h — both previously lost.

    use super::*;

    #[test]
    fn into_value_for_duration_uses_dedicated_variant() {
        let d = chrono::Duration::seconds(-3600);
        assert_eq!(d.into_value(), Value::Duration(d));
    }

    #[test]
    fn format_mysql_time_signed_no_fraction() {
        assert_eq!(format_mysql_time(chrono::Duration::zero()), "00:00:00");
        assert_eq!(
            format_mysql_time(chrono::Duration::seconds(3600 + 30 * 60 + 45)),
            "01:30:45"
        );
        assert_eq!(
            format_mysql_time(chrono::Duration::seconds(-(3600 + 30 * 60))),
            "-01:30:00"
        );
    }

    #[test]
    fn format_mysql_time_with_microseconds() {
        // 1h30m45.000123s
        let d =
            chrono::Duration::seconds(3600 + 30 * 60 + 45) + chrono::Duration::microseconds(123);
        assert_eq!(format_mysql_time(d), "01:30:45.000123");
    }

    #[test]
    fn format_mysql_time_above_24h() {
        // 838h59m59s — MySQL's positive maximum.
        let d = chrono::Duration::seconds(838 * 3600 + 59 * 60 + 59);
        assert_eq!(format_mysql_time(d), "838:59:59");
    }

    #[test]
    fn format_mysql_time_negative_above_24h() {
        // -838h59m59s — MySQL's negative minimum.
        let d = -(chrono::Duration::seconds(838 * 3600 + 59 * 60 + 59));
        assert_eq!(format_mysql_time(d), "-838:59:59");
    }

    #[test]
    fn from_value_duration_round_trip() {
        let d = chrono::Duration::microseconds(-123_456_789);
        assert_eq!(chrono::Duration::from_value(Value::Duration(d)).unwrap(), d);
    }

    #[test]
    fn from_value_duration_accepts_naive_time_in_range() {
        let t = chrono::NaiveTime::from_hms_opt(12, 34, 56).unwrap();
        let d = chrono::Duration::from_value(Value::Time(t)).unwrap();
        assert_eq!(d, chrono::Duration::seconds(12 * 3600 + 34 * 60 + 56));
    }

    #[test]
    fn from_value_naive_time_accepts_in_range_duration() {
        let d = chrono::Duration::seconds(12 * 3600 + 34 * 60 + 56);
        let t = chrono::NaiveTime::from_value(Value::Duration(d)).unwrap();
        assert_eq!(t, chrono::NaiveTime::from_hms_opt(12, 34, 56).unwrap());
    }

    #[test]
    fn from_value_naive_time_rejects_negative_duration() {
        let d = -chrono::Duration::seconds(1);
        assert!(chrono::NaiveTime::from_value(Value::Duration(d)).is_err());
    }

    #[test]
    fn from_value_naive_time_rejects_over_24h_duration() {
        let d = chrono::Duration::hours(25);
        assert!(chrono::NaiveTime::from_value(Value::Duration(d)).is_err());
    }

    #[test]
    fn duration_sql_type_is_interval() {
        // PG-correctness check: a signed duration must surface as INTERVAL,
        // not TIME (PG `TIME` rejects negatives).
        assert_eq!(
            Value::Duration(chrono::Duration::seconds(-1)).sql_type(),
            crate::schema::SqlType::Interval
        );
    }

    #[test]
    fn interval_renders_per_dialect() {
        use crate::query::Dialect;
        use crate::schema::SqlType;
        assert_eq!(SqlType::Interval.to_sql(Dialect::Postgres), "INTERVAL");
        // MySQL: native TIME (signed interval). Not TEXT — diverges from
        // the previous default on purpose.
        assert_eq!(SqlType::Interval.to_sql(Dialect::Mysql), "TIME");
        assert_eq!(SqlType::Interval.to_sql(Dialect::Sqlite), "TEXT");
        assert_eq!(SqlType::Interval.to_sql(Dialect::Generic), "TEXT");
    }
}
