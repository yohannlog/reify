use super::{BuildError, Dialect, rewrite_placeholders_pg, trace_query, write_returning};
use crate::condition::Condition;
use crate::ident::qi;
use crate::sql::{ToSql, write_joined};
use crate::table::Table;
use crate::value::Value;
use std::fmt::Write;
use std::marker::PhantomData;
use tracing::debug;

// ── UpdateBuilder ───────────────────────────────────────────────────

/// A fluent builder for `UPDATE` statements.
///
/// Obtain one via the generated `Model::update()` method.
///
/// # Example
///
/// ```ignore
/// let (sql, params) = User::update()
///     .set(User::active, false)
///     .filter(User::id.eq(42))
///     .build();
/// // UPDATE users SET active = ? WHERE id = ?
/// ```
#[derive(Clone)]
pub struct UpdateBuilder<M: Table> {
    sets: Vec<(&'static str, Value)>,
    conditions: Vec<Condition>,
    unfiltered: bool,
    #[cfg(feature = "postgres")]
    returning: Option<Vec<&'static str>>,
    _model: PhantomData<M>,
}

impl<M: Table> UpdateBuilder<M> {
    /// Construct an empty `UpdateBuilder`.
    ///
    /// Prefer the generated `Model::update()` factory method over calling this directly.
    pub fn new() -> Self {
        Self {
            sets: Vec::new(),
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

    /// Append a `SET col = val` assignment.
    pub fn set<T: crate::value::IntoValue>(
        mut self,
        col: crate::column::Column<M, T>,
        val: impl crate::value::IntoValue,
    ) -> Self {
        self.sets.push((col.name, val.into_value()));
        self
    }

    /// Append a WHERE condition.
    ///
    /// Multiple calls are combined with `AND`.
    pub fn filter(mut self, cond: Condition) -> Self {
        self.conditions.push(cond);
        self
    }

    /// Explicitly allow an UPDATE without a WHERE clause.
    ///
    /// By default, `build()` and `try_build()` reject unfiltered updates
    /// to prevent accidental full-table mutations. Call `.unfiltered()` to
    /// opt in:
    ///
    /// ```ignore
    /// User::update().set(User::active, false).unfiltered().build()
    /// // → UPDATE users SET active = ? (no WHERE clause)
    /// ```
    pub fn unfiltered(mut self) -> Self {
        self.unfiltered = true;
        self
    }

    /// Build the `UPDATE … SET … WHERE …` SQL string and parameter list.
    ///
    /// Automatically injects `SET col = <now>` for any `update_timestamp` columns
    /// not already set explicitly via `.set()`.
    ///
    /// # Panics
    ///
    /// Panics if no `.filter()` or `.unfiltered()` has been called — bare
    /// `UPDATE` without a `WHERE` clause is forbidden to prevent accidental
    /// full-table updates. Use [`try_build`](Self::try_build) for a
    /// non-panicking alternative.
    pub fn build(&self) -> (String, Vec<Value>) {
        self.try_build()
            .expect("UPDATE without WHERE is forbidden. Use .filter() or .unfiltered() explicitly.")
    }

    /// Build the `UPDATE … SET … WHERE …` SQL string and parameter list.
    ///
    /// Returns `Err(BuildError::MissingFilter)` if no `.filter()` or
    /// `.unfiltered()` has been called.
    #[allow(unused_mut)]
    pub fn try_build(&self) -> Result<(String, Vec<Value>), BuildError> {
        if !self.unfiltered && self.conditions.is_empty() {
            return Err(BuildError::MissingFilter {
                operation: "UPDATE",
            });
        }

        // Auto-inject update_timestamp columns (Vm source) that the caller didn't set.
        let mut all_sets = self.sets.clone();
        #[cfg(any(feature = "postgres", feature = "mysql"))]
        {
            let ts_cols = M::update_timestamp_columns();
            for col_name in ts_cols {
                if !all_sets.iter().any(|(c, _)| *c == col_name) {
                    #[cfg(feature = "postgres")]
                    let now_val = Value::Timestamptz(chrono::Utc::now());
                    #[cfg(all(feature = "mysql", not(feature = "postgres")))]
                    let now_val = Value::Timestamp(chrono::Utc::now().naive_utc());
                    all_sets.push((col_name, now_val));
                }
            }
        }

        let mut params = Vec::new();
        let mut sql = String::with_capacity(64 + all_sets.len() * 16);
        let _ = write!(sql, "UPDATE {} SET ", qi(M::table_name()));

        write_joined(&mut sql, &all_sets, ", ", |buf, (col, val)| {
            params.push(val.clone());
            let _ = write!(buf, "{} = ?", qi(col));
        });

        if !self.conditions.is_empty() {
            sql.push_str(" WHERE ");
            write_joined(&mut sql, &self.conditions, " AND ", |buf, c| {
                c.write_sql(buf, &mut params);
            });
        }

        #[cfg(feature = "postgres")]
        write_returning(&mut sql, &self.returning);

        trace_query("update", M::table_name(), &sql, &params);
        Ok((sql, params))
    }

    /// Build a [`BuiltQuery`] with `$N` placeholders already applied (PostgreSQL only).
    ///
    /// Delegates to `try_build()` (enforcing the no-bare-update guard) and rewrites
    /// placeholders once at build time.
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

impl<M: Table> Default for UpdateBuilder<M> {
    fn default() -> Self {
        Self::new()
    }
}

// ── UpdateBuilder direct execution methods ──────────────────────────

impl<M: Table> UpdateBuilder<M> {
    /// Execute this UPDATE statement.
    ///
    /// ```ignore
    /// let affected = User::update().set(User::active, false).filter(User::id.eq(42)).execute(&db).await?;
    /// ```
    pub async fn execute(&self, db: &impl crate::db::Database) -> Result<u64, crate::db::DbError> {
        crate::db::update(db, self).await
    }

    /// Execute this UPDATE … RETURNING and return typed results (PostgreSQL only).
    #[cfg(feature = "postgres")]
    pub async fn fetch(&self, db: &impl crate::db::Database) -> Result<Vec<M>, crate::db::DbError>
    where
        M: crate::db::FromRow,
    {
        crate::db::update_returning(db, self).await
    }
}
