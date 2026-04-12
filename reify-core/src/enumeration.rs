use crate::db::DbError;
use crate::value::Value;

/// Trait for Rust enums stored as text columns in the database.
///
/// Use `#[derive(DbEnum)]` to auto-implement this trait. By default,
/// variants are lowercased (`Admin` → `"admin"`). Use `#[db_enum(rename = "...")]`
/// to override a specific variant's name.
///
/// ```ignore
/// #[derive(DbEnum, Debug, Clone, PartialEq)]
/// pub enum Role {
///     Admin,
///     Member,
///     Guest,
/// }
/// // Stored as "admin", "member", "guest" in the database.
/// ```
///
/// ## Custom variant names
///
/// ```ignore
/// #[derive(DbEnum, Debug, Clone, PartialEq)]
/// pub enum Status {
///     Active,
///     #[db_enum(rename = "on_hold")]
///     OnHold,
///     Archived,
/// }
/// ```
pub trait DbEnum: Sized + Clone {
    /// All valid string representations (for validation / schema).
    fn variants() -> &'static [&'static str];

    /// Convert this variant to its database string.
    fn as_str(&self) -> &'static str;

    /// Parse from a database string. Returns `None` for unknown values.
    fn from_str(s: &str) -> Option<Self>;
}

// ── FromValue helper ───────────────────────────────────────────────

/// Parse a `Value` back into a `DbEnum` variant.
pub fn enum_from_value<T: DbEnum>(val: &Value) -> Result<T, DbError> {
    match val {
        Value::String(s) => T::from_str(s).ok_or_else(|| {
            DbError::Conversion(format!(
                "unknown enum variant '{}', expected one of {:?}",
                s,
                T::variants()
            ))
        }),
        Value::Null => Err(DbError::Conversion(
            "expected enum value, got NULL".to_string(),
        )),
        other => Err(DbError::Conversion(format!(
            "expected string for enum, got {:?}",
            other
        ))),
    }
}
