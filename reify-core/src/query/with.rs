use super::select::SelectBuilder;
use crate::condition::Condition;
use crate::ident::qi;
use crate::table::Table;
use crate::value::Value;
use std::marker::PhantomData;

// ── Eager loading — WithBuilder ──────────────────────────────────────

/// Result of a `.with(relation)` eager-load: the parent rows paired with
/// their associated child rows, assembled in memory from two queries.
///
/// The two queries issued are:
/// 1. `SELECT * FROM from_table [WHERE …]`
/// 2. `SELECT * FROM to_table WHERE to_col IN (parent_key_values…)`
///
/// Then rows are grouped by the join key in memory — no N+1.
pub struct WithBuilder<F: Table, T: Table> {
    pub(crate) parent: SelectBuilder<F>,
    pub(crate) rel: crate::relation::Relation<F, T>,
}

impl<F: Table, T: Table> WithBuilder<F, T> {
    /// Build the two SQL statements needed for eager loading.
    ///
    /// Returns `(parent_sql, parent_params, child_sql_template)`.
    /// The child SQL uses an `IN (?)` placeholder; the caller must
    /// substitute the actual parent key values at runtime.
    ///
    /// In practice use [`crate::db::with_related`] which handles both
    /// queries and the in-memory grouping automatically.
    pub fn build_queries(&self) -> ((String, Vec<Value>), String) {
        let (parent_sql, parent_params) = self.parent.build();
        // Child query: SELECT * FROM to_table WHERE to_col IN (?)
        // The `?` is a placeholder for the IN list — expanded at runtime.
        let child_sql = format!(
            "SELECT * FROM {} WHERE {} IN (?)",
            qi(T::table_name()),
            qi(self.rel.to_col),
        );
        ((parent_sql, parent_params), child_sql)
    }

    /// The relation this builder was constructed from.
    pub fn relation(&self) -> &crate::relation::Relation<F, T> {
        &self.rel
    }

    /// The parent query builder.
    pub fn parent_builder(&self) -> &SelectBuilder<F> {
        &self.parent
    }
}
