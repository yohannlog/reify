use crate::schema::{
    ColumnDef, ComputedColumn, ForeignKeyDef, IndexDef, TimestampKind, TimestampSource,
};
use crate::value::Value;

/// Trait implemented by `#[derive(Table)]` on user structs.
pub trait Table: Sized {
    /// SQL table name.
    fn table_name() -> &'static str;

    /// Ordered list of column names (matches struct field order).
    fn column_names() -> &'static [&'static str];

    /// Borrow this instance and produce an ordered list of `Value`s (same order as `column_names`).
    ///
    /// Prefer [`into_values`](Table::into_values) on insert paths where you already own the
    /// value — it moves heap fields (`String`, `Vec<u8>`, etc.) instead of cloning them.
    fn as_values(&self) -> Vec<Value>;

    /// Consume this instance and produce an ordered list of `Value`s.
    ///
    /// Default implementation calls [`as_values`](Table::as_values). The `#[derive(Table)]`
    /// macro overrides this with a move-based version that avoids cloning heap fields.
    fn into_values(self) -> Vec<Value>
    where
        Self: Sized,
    {
        self.as_values()
    }

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
    /// Uses [`insert_values`](Table::insert_values) as the source, so VM-source
    /// timestamps are injected with `Utc::now()`.
    ///
    /// Pairs with `writable_column_names()` — same order, same length.
    fn writable_values(&self) -> Vec<Value> {
        let defs = Self::column_defs();
        if defs.is_empty() {
            return self.insert_values();
        }
        let all_values = self.insert_values();
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

    /// Column names marked as `creation_timestamp` with `Vm` source.
    ///
    /// Used by `InsertBuilder` to auto-inject timestamp values on INSERT.
    /// Returns an empty vec when no creation-timestamp columns exist.
    fn creation_timestamp_columns() -> Vec<&'static str> {
        let defs = Self::column_defs();
        defs.iter()
            .filter(|d| {
                d.timestamp_kind == Some(TimestampKind::Creation)
                    && d.timestamp_source == TimestampSource::Vm
            })
            .map(|d| d.name)
            .collect()
    }

    /// Values for INSERT operations, with VM-source timestamps injected.
    ///
    /// Unlike [`as_values`](Table::as_values) which returns the struct's actual
    /// field values, this method injects `Utc::now()` for `creation_timestamp`
    /// and `update_timestamp` columns with `Vm` source.
    ///
    /// Use this for INSERT operations. Use `as_values()` for reads, comparisons,
    /// and serialization where you want the actual stored values.
    fn insert_values(&self) -> Vec<Value> {
        // Default implementation falls back to as_values().
        // The derive macro overrides this with timestamp injection.
        self.as_values()
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
