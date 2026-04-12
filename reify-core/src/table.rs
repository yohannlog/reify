use crate::schema::IndexDef;
use crate::value::Value;

/// Trait implemented by `#[derive(Table)]` on user structs.
pub trait Table: Sized {
    /// SQL table name.
    fn table_name() -> &'static str;

    /// Ordered list of column names (matches struct field order).
    fn column_names() -> &'static [&'static str];

    /// Convert this instance into a list of `Value`s (same order as `column_names`).
    fn into_values(&self) -> Vec<Value>;

    /// Index definitions for this table (from `#[column(index)]` and `#[table(index(...))]`).
    fn indexes() -> Vec<IndexDef> {
        Vec::new()
    }
}
