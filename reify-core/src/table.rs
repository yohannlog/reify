use crate::schema::{ColumnDef, IndexDef};
use crate::value::Value;

/// Trait implemented by `#[derive(Table)]` on user structs.
pub trait Table: Sized {
    /// SQL table name.
    fn table_name() -> &'static str;

    /// Ordered list of column names (matches struct field order).
    fn column_names() -> &'static [&'static str];

    /// Convert this instance into a list of `Value`s (same order as `column_names`).
    fn into_values(&self) -> Vec<Value>;

    /// Rich column metadata (SQL types, constraints) derived from Rust types.
    ///
    /// Generated automatically by `#[derive(Table)]`. Falls back to empty
    /// when not implemented — callers should use `column_names()` as fallback.
    fn column_defs() -> Vec<ColumnDef> {
        Vec::new()
    }

    /// Index definitions for this table (from `#[column(index)]` and `#[table(index(...))]`).
    fn indexes() -> Vec<IndexDef> {
        Vec::new()
    }
}
