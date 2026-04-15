use super::select::SelectBuilder;
use super::{BuildError, Dialect, trace_query};
#[cfg(feature = "postgres")]
use super::{rewrite_placeholders_pg, write_returning};
use crate::condition::Condition;
use crate::ident::qi;
use crate::sql::{ToSql, write_joined};
use crate::table::Table;
use crate::value::Value;
use std::fmt::Write;
use std::marker::PhantomData;
use tracing::debug;

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
pub struct DeleteBuilder<M: Table> {
    conditions: Vec<Condition>,
    unfiltered: bool,
    #[cfg(feature = "postgres")]
    returning: Option<Vec<&'static str>>,
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
            #[cfg(feature = "postgres")]
            returning: None,
            _model: PhantomData,
        }
    }

    /// Append a `RETURNING` clause (PostgreSQL only).
    #[cfg(feature = "postgres")]
    pub fn returning(mut self, cols: &[&'static str]) -> Self {
        self.returning = Some(cols.to_vec());
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

    /// Build the `DELETE FROM … WHERE …` SQL string and parameter list.
    ///
    /// # Panics
    ///
    /// Panics if no `.filter()` or `.unfiltered()` has been called — bare
    /// `DELETE` without a `WHERE` clause is forbidden to prevent accidental
    /// full-table deletes. Use [`try_build`](Self::try_build) for a
    /// non-panicking alternative.
    pub fn build(&self) -> (String, Vec<Value>) {
        self.try_build()
            .expect("DELETE without WHERE is forbidden. Use .filter() or .unfiltered() explicitly.")
    }

    /// Build the `DELETE FROM … WHERE …` SQL string and parameter list.
    ///
    /// Returns `Err(BuildError::MissingFilter)` if no `.filter()` or
    /// `.unfiltered()` has been called.
    #[allow(unused_mut)]
    pub fn try_build(&self) -> Result<(String, Vec<Value>), BuildError> {
        if !self.unfiltered && self.conditions.is_empty() {
            return Err(BuildError::MissingFilter {
                operation: "DELETE",
            });
        }

        let mut params = Vec::new();
        let mut sql = String::with_capacity(64);
        let _ = write!(sql, "DELETE FROM {}", qi(M::table_name()));

        if !self.conditions.is_empty() {
            sql.push_str(" WHERE ");
            write_joined(&mut sql, &self.conditions, " AND ", |buf, c| {
                c.write_sql(buf, &mut params);
            });
        }

        #[cfg(feature = "postgres")]
        write_returning(&mut sql, &self.returning);

        trace_query("delete", M::table_name(), &sql, &params);
        Ok((sql, params))
    }

    /// Build a [`BuiltQuery`] with `$N` placeholders already applied (PostgreSQL only).
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

    /// Build a [`BuiltQuery`] with `$N` placeholders already applied (PostgreSQL only).
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
}
