use crate::schema::{ColumnDef, ComputedColumn, ForeignKeyDef, IndexDef, TimestampKind, TimestampSource};
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

    /// Foreign-key constraints derived from `#[column(references = "Table::col")]`.
    ///
    /// Returns one [`ForeignKeyDef`] per column that carries a foreign-key
    /// annotation.  The default implementation collects them from
    /// [`column_defs()`](Table::column_defs).
    fn foreign_keys() -> Vec<ForeignKeyDef> {
        Self::column_defs()
            .into_iter()
            .filter_map(|d| d.foreign_key)
            .collect()
    }

    /// Column names that are writable (excludes computed and DB-managed timestamp columns).
    ///
    /// Used by INSERT/UPDATE builders to skip DB-generated and virtual columns.
    /// Default implementation filters `column_defs()` or falls back to `column_names()`.
    fn writable_column_names() -> Vec<&'static str> {
        let defs = Self::column_defs();
        if defs.is_empty() {
            return Self::column_names().to_vec();
        }
        defs.iter()
            .filter(|d| d.computed.is_none() && d.timestamp_source != TimestampSource::Db)
            .map(|d| d.name)
            .collect()
    }

    /// Values for writable columns only (excludes computed and DB-managed timestamp columns).
    ///
    /// Pairs with `writable_column_names()` — same order, same length.
    fn writable_values(&self) -> Vec<Value> {
        let defs = Self::column_defs();
        if defs.is_empty() {
            return self.into_values();
        }
        let all_values = self.into_values();
        defs.iter()
            .zip(all_values)
            .filter(|(d, _)| d.computed.is_none() && d.timestamp_source != TimestampSource::Db)
            .map(|(_, v)| v)
            .collect()
    }

    /// Column names marked as `update_timestamp` with `Vm` source.
    ///
    /// Used by `UpdateBuilder` to auto-inject `SET col = NOW()` on every UPDATE.
    /// Returns an empty vec when no update-timestamp columns exist.
    fn update_timestamp_columns() -> Vec<&'static str> {
        let defs = Self::column_defs();
        defs.iter()
            .filter(|d| {
                d.timestamp_kind == Some(TimestampKind::Update)
                    && d.timestamp_source == TimestampSource::Vm
            })
            .map(|d| d.name)
            .collect()
    }

    /// Column names that exist in the database (excludes `computed_rust` virtual columns).
    ///
    /// Includes DB-generated computed columns (they exist in the schema) but
    /// excludes Rust-side virtual columns (they don't exist in the DB at all).
    fn db_column_names() -> Vec<&'static str> {
        let defs = Self::column_defs();
        if defs.is_empty() {
            return Self::column_names().to_vec();
        }
        defs.iter()
            .filter(|d| !matches!(d.computed, Some(ComputedColumn::Virtual)))
            .map(|d| d.name)
            .collect()
    }
}
