/// A dynamically-typed SQL value used for parameter binding.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
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
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    Time(chrono::NaiveTime),

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
            _ => Err(format!("expected i64, got {:?}", val)),
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
            _ => Err(format!("expected Time, got {:?}", val)),
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
