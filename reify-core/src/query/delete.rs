use super::select::SelectBuilder;
use super::{BuildError, trace_query};
#[cfg(feature = "postgres")]
use super::{rewrite_placeholders_pg, write_returning};
use crate::column::Column;
use crate::condition::Condition;
use crate::ident::qi;
use crate::sql::{ToSql, write_joined};
use crate::table::Table;
use crate::value::Value;
use std::fmt::Write;
use std::marker::PhantomData;

/// Soft-delete mode for delete operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum SoftDeleteMode {
    /// Use soft delete if the model has a soft-delete column (default).
    #[default]
    Auto,
    /// Force a hard DELETE regardless of soft-delete column.
    Force,
}

// ── DeleteBuilder ───────────────────────────────────────────────────

/// A fluent builder for `DELETE` statements.
///
/// Obtain one via the generated `Model::delete()` method.
///
/// # Example
///
/// ```ignore
/// let (sql, params) = User::delete()
///     .filter(User::id.eq(42))
///     .build();
/// // DELETE FROM users WHERE id = ?
/// ```
#[derive(Clone)]
#[must_use = "DeleteBuilder is lazy; chain `.build()` or `.execute(&db)` to run the DELETE"]
pub struct DeleteBuilder<M: Table> {
    conditions: Vec<Condition>,
    unfiltered: bool,
    soft_delete_mode: SoftDeleteMode,
    #[cfg(feature = "postgres")]
    returning: Option<Vec<&'static str>>,
    #[cfg(feature = "postgres18")]
    returning_old_new: Option<super::ReturningOldNew>,
    _model: PhantomData<M>,
}

impl<M: Table> DeleteBuilder<M> {
    /// Construct an empty `DeleteBuilder`.
    ///
    /// Prefer the generated `Model::delete()` factory method over calling this directly.
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
            unfiltered: false,
            soft_delete_mode: SoftDeleteMode::Auto,
            #[cfg(feature = "postgres")]
            returning: None,
            #[cfg(feature = "postgres18")]
            returning_old_new: None,
            _model: PhantomData,
        }
    }

    /// Force a hard DELETE, bypassing soft delete.
    ///
    /// By default, if the model has a `#[column(soft_delete)]` column,
    /// `delete()` performs an UPDATE to set the deletion timestamp.
    /// Call `.force()` to perform an actual DELETE instead.
    ///
    /// ```ignore
    /// // Permanently delete the user (hard delete)
    /// User::delete()
    ///     .filter(User::id.eq(42))
    ///     .force()
    ///     .execute(&db)
    ///     .await?;
    /// ```
    pub fn force(mut self) -> Self {
        self.soft_delete_mode = SoftDeleteMode::Force;
        self
    }

    /// Append a `RETURNING` clause (PostgreSQL only).
    #[cfg(feature = "postgres")]
    pub fn returning(mut self, cols: &[&'static str]) -> Self {
        self.returning = Some(cols.to_vec());
        self
    }

    /// Append a `RETURNING` clause using typed [`Column`] references (PostgreSQL only).
    #[cfg(feature = "postgres")]
    pub fn returning_cols<T>(mut self, cols: &[Column<M, T>]) -> Self {
        self.returning = Some(cols.iter().map(|c| c.name).collect());
        self
    }

    /// Append `RETURNING old.*` clause (PostgreSQL 18+).
    ///
    /// Returns the deleted row state.
    #[cfg(feature = "postgres18")]
    pub fn returning_old_all(mut self) -> Self {
        self.returning_old_new = Some(super::ReturningOldNew::Old);
        self
    }

    /// Append a WHERE condition.
    ///
    /// Multiple calls are combined with `AND`.
    pub fn filter(mut self, cond: Condition) -> Self {
        self.conditions.push(cond);
        self
    }

    /// Convert this `DeleteBuilder` into a `SelectBuilder` with the same WHERE conditions.
    ///
    /// Used by `audited_delete` to capture old rows before deletion.
    pub fn to_select(&self) -> SelectBuilder<M> {
        let mut sel = SelectBuilder::new();
        for cond in &self.conditions {
            sel = sel.filter(cond.clone());
        }
        sel
    }

    /// Explicitly allow a DELETE without a WHERE clause.
    ///
    /// By default, `build()` and `try_build()` reject unfiltered deletes
    /// to prevent accidental full-table deletions. Call `.unfiltered()` to
    /// opt in:
    ///
    /// ```ignore
    /// User::delete().unfiltered().build()
    /// // → DELETE FROM users (no WHERE clause)
    /// ```
    pub fn unfiltered(mut self) -> Self {
        self.unfiltered = true;
        self
    }

    /// Build the `DELETE FROM … WHERE …` or `UPDATE … SET deleted_at = …` SQL string.
    ///
    /// If the model has a `#[column(soft_delete)]` column and `.force()` was not called,
    /// this emits an UPDATE statement instead of DELETE.
    ///
    /// # Panics
    ///
    /// Panics if no `.filter()` or `.unfiltered()` has been called — bare
    /// DELETE/UPDATE without a WHERE clause is forbidden to prevent accidental
    /// full-table operations. Use [`try_build`](Self::try_build) for a
    /// non-panicking alternative.
    #[must_use]
    pub fn build(&self) -> (String, Vec<Value>) {
        self.try_build()
            .expect("DELETE without WHERE is forbidden. Use .filter() or .unfiltered() explicitly.")
    }

    /// Build the SQL string and parameter list.
    ///
    /// Priority order:
    /// 1. If `#[table(sql_delete = "...")]` is set, use that custom SQL
    /// 2. If the model has a `#[column(soft_delete)]` column and `.force()` was not called,
    ///    emit `UPDATE table SET deleted_at = CURRENT_TIMESTAMP WHERE …`
    /// 3. Otherwise, emit `DELETE FROM table WHERE …`
    ///
    /// Returns `Err(BuildError::MissingFilter)` if no `.filter()` or
    /// `.unfiltered()` has been called (unless using custom sql_delete).
    #[allow(unused_mut)]
    pub fn try_build(&self) -> Result<(String, Vec<Value>), BuildError> {
        // Custom SQL override takes precedence
        if let Some(custom_sql) = M::sql_delete() {
            let mut params = Vec::new();
            let mut sql = custom_sql.to_string();

            // Append WHERE conditions if any
            if !self.conditions.is_empty() {
                // If custom SQL already has WHERE, use AND; otherwise add WHERE
                let has_where = custom_sql.to_uppercase().contains(" WHERE ");
                if has_where {
                    sql.push_str(" AND ");
                } else {
                    sql.push_str(" WHERE ");
                }
                write_joined(&mut sql, &self.conditions, " AND ", |buf, c| {
                    c.write_sql(buf, &mut params);
                });
            }

            trace_query("delete(custom)", M::table_name(), &sql, &params);
            return Ok((sql, params));
        }

        if !self.unfiltered && self.conditions.is_empty() {
            return Err(BuildError::MissingFilter {
                operation: "DELETE",
            });
        }

        // Check if we should use soft delete
        let soft_delete_col = match self.soft_delete_mode {
            SoftDeleteMode::Force => None,
            SoftDeleteMode::Auto => M::soft_delete_column(),
        };

        let mut params = Vec::new();
        let mut sql = String::with_capacity(64);

        if let Some(col) = soft_delete_col {
            // Soft delete: UPDATE table SET col = CURRENT_TIMESTAMP WHERE ...
            let _ = write!(
                sql,
                "UPDATE {} SET {} = CURRENT_TIMESTAMP",
                qi(M::table_name()),
                qi(col)
            );
        } else {
            // Hard delete: DELETE FROM table WHERE ...
            let _ = write!(sql, "DELETE FROM {}", qi(M::table_name()));
        }

        if !self.conditions.is_empty() {
            sql.push_str(" WHERE ");
            write_joined(&mut sql, &self.conditions, " AND ", |buf, c| {
                c.write_sql(buf, &mut params);
            });
        }

        #[cfg(feature = "postgres")]
        if soft_delete_col.is_none() {
            // RETURNING only makes sense for hard deletes
            write_returning(&mut sql, &self.returning);
        }

        #[cfg(feature = "postgres18")]
        if soft_delete_col.is_none()
            && let Some(mode) = self.returning_old_new
        {
            super::write_returning_old_new(&mut sql, mode, M::table_name());
        }

        let op = if soft_delete_col.is_some() {
            "soft_delete"
        } else {
            "delete"
        };
        trace_query(op, M::table_name(), &sql, &params);
        Ok((sql, params))
    }

    /// Check if this delete will use soft delete.
    ///
    /// Returns `true` if the model has a soft-delete column and `.force()` was not called.
    pub fn is_soft_delete(&self) -> bool {
        match self.soft_delete_mode {
            SoftDeleteMode::Force => false,
            SoftDeleteMode::Auto => M::soft_delete_column().is_some(),
        }
    }

    /// Build a [`crate::BuiltQuery`] with `$N` placeholders already applied (PostgreSQL only).
    ///
    /// # Panics
    ///
    /// Panics if no `.filter()` or `.unfiltered()` has been called.
    #[cfg(feature = "postgres")]
    pub fn build_pg(&self) -> crate::built_query::BuiltQuery {
        let (sql, params) = self.build();
        let pg_sql = rewrite_placeholders_pg(&sql);
        crate::built_query::BuiltQuery::new(pg_sql, params)
    }

    /// Build a [`crate::BuiltQuery`] with `$N` placeholders already applied (PostgreSQL only).
    ///
    /// Returns `Err(BuildError::MissingFilter)` if no `.filter()` or `.unfiltered()` has been called.
    #[cfg(feature = "postgres")]
    pub fn try_build_pg(&self) -> Result<crate::built_query::BuiltQuery, BuildError> {
        let (sql, params) = self.try_build()?;
        let pg_sql = rewrite_placeholders_pg(&sql);
        Ok(crate::built_query::BuiltQuery::new(pg_sql, params))
    }
}

impl<M: Table> Default for DeleteBuilder<M> {
    fn default() -> Self {
        Self::new()
    }
}

// ── DeleteBuilder direct execution methods ──────────────────────────

impl<M: Table> DeleteBuilder<M> {
    /// Execute this DELETE statement.
    ///
    /// ```ignore
    /// let affected = User::delete().filter(User::id.eq(42)).execute(&db).await?;
    /// ```
    pub async fn execute(&self, db: &impl crate::db::Database) -> Result<u64, crate::db::DbError> {
        crate::db::delete(db, self).await
    }

    /// Execute this DELETE … RETURNING and return typed results (PostgreSQL only).
    #[cfg(feature = "postgres")]
    pub async fn fetch(&self, db: &impl crate::db::Database) -> Result<Vec<M>, crate::db::DbError>
    where
        M: crate::db::FromRow,
    {
        crate::db::delete_returning(db, self).await
    }

    /// Execute this DELETE … RETURNING old.* and return `OldNew<M>` results (PostgreSQL 18+).
    ///
    /// Requires `.returning_old_all()` to be called first.
    #[cfg(feature = "postgres18")]
    pub async fn fetch_old(
        &self,
        db: &impl crate::db::Database,
    ) -> Result<Vec<crate::db::OldNew<M>>, crate::db::DbError>
    where
        M: crate::db::FromRowPositional,
    {
        crate::db::delete_returning_old(db, self).await
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::condition::Condition;

    /// Plain table with no soft-delete column — exercises the hard-delete path.
    struct Hard;
    impl Table for Hard {
        fn table_name() -> &'static str {
            "hard"
        }
        fn column_names() -> &'static [&'static str] {
            &["id", "name"]
        }
        fn as_values(&self) -> Vec<Value> {
            vec![]
        }
    }

    /// Table with a soft-delete column — exercises the auto-promotion path.
    struct Soft;
    impl Table for Soft {
        fn table_name() -> &'static str {
            "soft"
        }
        fn column_names() -> &'static [&'static str] {
            &["id", "deleted_at"]
        }
        fn as_values(&self) -> Vec<Value> {
            vec![]
        }
        fn soft_delete_column() -> Option<&'static str> {
            Some("deleted_at")
        }
    }

    fn id_eq(v: i64) -> Condition {
        Condition::Eq("id", Value::I64(v))
    }

    #[test]
    fn build_with_filter_emits_delete_from_with_where() {
        let (sql, params) = DeleteBuilder::<Hard>::new().filter(id_eq(7)).build();
        assert!(sql.starts_with("DELETE FROM \"hard\""), "sql: {sql}");
        assert!(sql.contains(" WHERE "), "sql: {sql}");
        assert_eq!(params, vec![Value::I64(7)]);
    }

    #[test]
    fn unfiltered_emits_delete_without_where() {
        let (sql, params) = DeleteBuilder::<Hard>::new().unfiltered().build();
        assert!(sql.starts_with("DELETE FROM \"hard\""));
        assert!(!sql.contains("WHERE"), "must have no WHERE: {sql}");
        assert!(params.is_empty());
    }

    #[test]
    #[should_panic(expected = "DELETE without WHERE is forbidden")]
    fn build_without_filter_or_unfiltered_panics() {
        let _ = DeleteBuilder::<Hard>::new().build();
    }

    #[test]
    fn try_build_without_filter_returns_missing_filter() {
        let err = DeleteBuilder::<Hard>::new().try_build().unwrap_err();
        assert!(matches!(
            err,
            BuildError::MissingFilter {
                operation: "DELETE"
            }
        ));
    }

    #[test]
    fn soft_delete_auto_promotes_to_update() {
        let b = DeleteBuilder::<Soft>::new().filter(id_eq(1));
        assert!(b.is_soft_delete());
        let (sql, _) = b.build();
        assert!(sql.starts_with("UPDATE \"soft\""), "sql: {sql}");
        assert!(
            sql.contains("\"deleted_at\" = CURRENT_TIMESTAMP"),
            "sql: {sql}"
        );
        assert!(sql.contains(" WHERE "));
    }

    #[test]
    fn force_disables_soft_delete() {
        let b = DeleteBuilder::<Soft>::new().filter(id_eq(1)).force();
        assert!(!b.is_soft_delete());
        let (sql, _) = b.build();
        assert!(sql.starts_with("DELETE FROM \"soft\""), "sql: {sql}");
    }

    #[test]
    fn is_soft_delete_false_on_table_without_soft_delete_column() {
        let b = DeleteBuilder::<Hard>::new().filter(id_eq(1));
        assert!(!b.is_soft_delete());
    }

    #[test]
    fn to_select_carries_filters() {
        let del = DeleteBuilder::<Hard>::new().filter(id_eq(42));
        let sel = del.to_select();
        let (sql, params) = sel.build();
        assert!(sql.contains("FROM \"hard\""));
        assert!(sql.contains(" WHERE "));
        assert_eq!(params, vec![Value::I64(42)]);
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn returning_appends_clause_for_hard_delete() {
        let (sql, _) = DeleteBuilder::<Hard>::new()
            .filter(id_eq(1))
            .returning(&["id", "name"])
            .build();
        assert!(sql.contains("RETURNING \"id\", \"name\""), "sql: {sql}");
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn returning_skipped_for_soft_delete() {
        // Soft delete emits an UPDATE; RETURNING is intentionally suppressed
        // because the meaningful "old row" semantics are different.
        let (sql, _) = DeleteBuilder::<Soft>::new()
            .filter(id_eq(1))
            .returning(&["id"])
            .build();
        assert!(!sql.contains("RETURNING"), "sql: {sql}");
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn build_pg_rewrites_placeholders() {
        let bq = DeleteBuilder::<Hard>::new().filter(id_eq(9)).build_pg();
        let (sql, _) = bq.into_parts();
        assert!(sql.contains("$1"), "sql: {sql}");
        assert!(!sql.contains('?'));
    }
}
