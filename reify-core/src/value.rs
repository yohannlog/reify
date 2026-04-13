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
            Value::F32(v) => v.to_string(),
            Value::F64(v) => v.to_string(),
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
        }
    }
}

/// Trait for types that can be converted into a `Value`.
pub trait IntoValue {
    fn into_value(self) -> Value;
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
