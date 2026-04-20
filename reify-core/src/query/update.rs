use super::{BuildError, Dialect, trace_query};
#[cfg(feature = "postgres")]
use super::{rewrite_placeholders_pg, write_returning};
use crate::condition::Condition;
use crate::ident::qi;
use crate::sql::{ToSql, write_joined};
use crate::table::Table;
use crate::value::{IntoValue, Value};
use std::fmt::Write;
use std::marker::PhantomData;
use tracing::debug;

// ── SetExpr — assignment RHS ─────────────────────────────────────────

/// The right-hand side of a `SET col = <expr>` assignment.
#[derive(Clone)]
enum SetExpr {
    /// Plain value: `col = ?`
    Value(Value),
    /// Raw SQL fragment with no bound parameter: `col = <sql>`.
    ///
    /// Used for server-side expressions like `CURRENT_TIMESTAMP` that must
    /// be evaluated by the database, not by the client. The fragment is
    /// `&'static str` to prevent user input from reaching SQL verbatim.
    RawExpr(&'static str),
    /// Array append: `col = col || ?`  (PostgreSQL only)
    #[cfg(feature = "postgres")]
    ArrayAppend(Value),
    /// Array prepend: `col = ? || col`  (PostgreSQL only)
    #[cfg(feature = "postgres")]
    ArrayPrepend(Value),
}

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
    sets: Vec<(&'static str, SetExpr)>,
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
    pub fn set<T: IntoValue>(
        mut self,
        col: crate::column::Column<M, T>,
        val: impl IntoValue,
    ) -> Self {
        self.sets.push((col.name, SetExpr::Value(val.into_value())));
        self
    }

    /// `SET col = col || ARRAY[val]` — append an element to a PostgreSQL array column.
    ///
    /// ```ignore
    /// Post::update()
    ///     .set_array_append(Post::tags, "new_tag".to_string())
    ///     .filter(Post::id.eq(1i64))
    ///     .build();
    /// // → UPDATE "posts" SET "tags" = "tags" || ? WHERE "id" = ?
    /// ```
    #[cfg(feature = "postgres")]
    pub fn set_array_append<T: IntoValue + Clone + 'static>(
        mut self,
        col: crate::column::Column<M, Vec<T>>,
        val: impl IntoValue,
    ) -> Self {
        self.sets
            .push((col.name, SetExpr::ArrayAppend(val.into_value())));
        self
    }

    /// `SET col = ARRAY[val] || col` — prepend an element to a PostgreSQL array column.
    ///
    /// ```ignore
    /// Post::update()
    ///     .set_array_prepend(Post::tags, "first_tag".to_string())
    ///     .filter(Post::id.eq(1i64))
    ///     .build();
    /// // → UPDATE "posts" SET "tags" = ? || "tags" WHERE "id" = ?
    /// ```
    #[cfg(feature = "postgres")]
    pub fn set_array_prepend<T: IntoValue + Clone + 'static>(
        mut self,
        col: crate::column::Column<M, Vec<T>>,
        val: impl IntoValue,
    ) -> Self {
        self.sets
            .push((col.name, SetExpr::ArrayPrepend(val.into_value())));
        self
    }

    /// Append a WHERE condition.
    ///
    /// Multiple calls are combined with `AND`.
    pub fn filter(mut self, cond: Condition) -> Self {
        self.conditions.push(cond);
        self
    }

    /// Convert this `UpdateBuilder` into a `SelectBuilder` with the same WHERE conditions.
    ///
    /// Used by `audited_update` to capture the before-image of rows before modification.
    pub fn to_select(&self) -> crate::query::SelectBuilder<M> {
        let mut sel = crate::query::SelectBuilder::new();
        for cond in &self.conditions {
            sel = sel.filter(cond.clone());
        }
        sel
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
        //
        // We emit `CURRENT_TIMESTAMP` as a raw SQL expression rather than
        // binding a client-computed `chrono::Utc::now()`. This avoids the
        // MySQL pitfall where a bound `TIMESTAMP` parameter is interpreted in
        // the session's `time_zone` — a client in UTC writing to a server
        // configured with `time_zone = 'SYSTEM'` (non-UTC) would see silent
        // offsets. Letting the server generate the timestamp keeps semantics
        // consistent across backends.
        let mut all_sets = self.sets.clone();
        #[cfg(any(feature = "postgres", feature = "mysql"))]
        {
            let ts_cols = M::update_timestamp_columns();
            for col_name in ts_cols {
                if !all_sets.iter().any(|(c, _)| *c == col_name) {
                    all_sets.push((col_name, SetExpr::RawExpr("CURRENT_TIMESTAMP")));
                }
            }
        }

        let mut params = Vec::new();
        let mut sql = String::with_capacity(64 + all_sets.len() * 16);
        let _ = write!(sql, "UPDATE {} SET ", qi(M::table_name()));

        write_joined(&mut sql, &all_sets, ", ", |buf, (col, expr)| match expr {
            SetExpr::Value(val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} = ?", qi(col));
            }
            SetExpr::RawExpr(expr) => {
                let _ = write!(buf, "{} = {}", qi(col), expr);
            }
            #[cfg(feature = "postgres")]
            SetExpr::ArrayAppend(val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} = {} || ?", qi(col), qi(col));
            }
            #[cfg(feature = "postgres")]
            SetExpr::ArrayPrepend(val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} = ? || {}", qi(col), qi(col));
            }
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

    /// Build a [`crate::BuiltQuery`] with `$N` placeholders already applied (PostgreSQL only).
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
