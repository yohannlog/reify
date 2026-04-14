use super::join::{JoinClause, JoinKind};
use super::with::WithBuilder;
use super::{
    BuildError, Dialect, Expr, OnConflict, Order, OrderExpr, rewrite_placeholders_pg, trace_query,
    write_on_conflict, write_returning,
};
use crate::condition::{AggregateCondition, Condition};
use crate::ident::qi;
use crate::sql::{ToSql, write_joined};
use crate::table::Table;
use crate::value::Value;
use std::fmt::Write;
use std::marker::PhantomData;
use tracing::debug;

// ── SelectBuilder ───────────────────────────────────────────────────

/// A fluent builder for `SELECT` statements.
///
/// Obtain one via the generated `Model::find()` method.
///
/// # Example
///
/// ```ignore
/// let (sql, params) = User::find()
///     .filter(User::active.eq(true))
///     .order_by(User::name.asc())
///     .limit(10)
///     .build();
/// ```
#[derive(Clone)]
pub struct SelectBuilder<M: Table> {
    distinct: bool,
    columns: Option<Vec<&'static str>>,
    exprs: Option<Vec<Expr>>,
    conditions: Vec<Condition>,
    joins: Vec<JoinClause>,
    group_by: Vec<&'static str>,
    having: Vec<Condition>,
    orders: Vec<Order>,
    limit: Option<u64>,
    offset: Option<u64>,
    _model: PhantomData<M>,
}

impl<M: Table> SelectBuilder<M> {
    /// Construct an empty `SelectBuilder`.
    ///
    /// Prefer the generated `Model::find()` factory method over calling this directly.
    pub fn new() -> Self {
        Self {
            distinct: false,
            columns: None,
            exprs: None,
            conditions: Vec::new(),
            joins: Vec::new(),
            group_by: Vec::new(),
            having: Vec::new(),
            orders: Vec::new(),
            limit: None,
            offset: None,
            _model: PhantomData,
        }
    }

    /// Emit `SELECT DISTINCT` instead of `SELECT`.
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Restrict the SELECT list to the given column names.
    ///
    /// If not called, all columns (`SELECT *`) are returned.
    pub fn select(mut self, cols: &[&'static str]) -> Self {
        self.columns = Some(cols.to_vec());
        self
    }

    /// Select a list of expressions (columns and/or aggregates).
    ///
    /// ```ignore
    /// User::find()
    ///     .select_expr(&[Expr::Col("role"), count_all()])
    ///     .group_by(&["role"])
    ///     .build();
    /// ```
    pub fn select_expr(mut self, exprs: &[Expr]) -> Self {
        self.exprs = Some(exprs.to_vec());
        self
    }

    /// Append a WHERE condition.
    ///
    /// Multiple calls are combined with `AND`.
    pub fn filter(mut self, cond: Condition) -> Self {
        self.conditions.push(cond);
        self
    }

    /// Add a GROUP BY clause.
    pub fn group_by(mut self, cols: &[&'static str]) -> Self {
        self.group_by.extend_from_slice(cols);
        self
    }

    /// Add a HAVING condition (applied after GROUP BY).
    pub fn having(mut self, cond: Condition) -> Self {
        self.having.push(cond);
        self
    }

    /// Append an ORDER BY clause.
    ///
    /// Multiple calls add multiple sort keys in the order they are called.
    pub fn order_by(mut self, order: Order) -> Self {
        self.orders.push(order);
        self
    }

    /// Set the LIMIT clause.
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    /// Set the OFFSET clause.
    pub fn offset(mut self, n: u64) -> Self {
        self.offset = Some(n);
        self
    }

    /// Select typed columns (alternative to string-based `select()`).
    ///
    /// ```ignore
    /// User::find().select_cols(&[User::id, User::email]).build();
    /// ```
    pub fn select_cols<T>(mut self, cols: &[crate::column::Column<M, T>]) -> Self {
        self.columns = Some(cols.iter().map(|c| c.name).collect());
        self
    }

    /// Group by typed columns (alternative to string-based `group_by()`).
    pub fn group_by_cols<T>(mut self, cols: &[crate::column::Column<M, T>]) -> Self {
        self.group_by.extend(cols.iter().map(|c| c.name));
        self
    }

    /// Build a structured `SqlFragment` AST for this query.
    ///
    /// Use this when you need to manipulate the query structure (e.g. pagination)
    /// without parsing rendered SQL text.
    pub fn build_ast(&self) -> crate::sql::SqlFragment {
        let has_joins = !self.joins.is_empty();

        let columns = if let Some(ref exprs) = self.exprs {
            exprs.iter().map(|e| e.to_sql_fragment()).collect()
        } else if has_joins {
            // With joins: qualify columns as `table.*` for each table
            let mut select_tables = vec![format!("{}.*", qi(M::table_name()))];
            let mut seen = std::collections::HashSet::new();
            seen.insert(M::table_name());
            for j in &self.joins {
                if seen.insert(j.table) {
                    select_tables.push(format!("{}.*", qi(j.table)));
                }
            }
            match &self.columns {
                Some(c) => c.iter().map(|s| qi(s)).collect(),
                None => select_tables,
            }
        } else {
            match &self.columns {
                Some(c) => c.iter().map(|s| qi(s)).collect(),
                None => vec![],
            }
        };

        let joins = self
            .joins
            .iter()
            .map(|j| crate::sql::JoinFragment {
                kind: j.kind,
                table: j.table.to_string(),
                on_condition: j.on.clone(),
            })
            .collect();

        let order_by = self
            .orders
            .iter()
            .map(|o| match o {
                Order::Asc(c) => crate::sql::OrderFragment {
                    column: qi(c),
                    descending: false,
                },
                Order::Desc(c) => crate::sql::OrderFragment {
                    column: qi(c),
                    descending: true,
                },
            })
            .collect();

        crate::sql::SqlFragment::Select {
            distinct: self.distinct,
            columns,
            from: M::table_name().to_string(),
            joins,
            conditions: self.conditions.clone(),
            group_by: self.group_by.iter().map(|s| qi(s)).collect(),
            having: self.having.clone(),
            order_by,
            limit: self.limit,
            offset: self.offset,
        }
    }

    /// Build the SQL string and parameter list.
    pub fn build(&self) -> (String, Vec<Value>) {
        let ast = self.build_ast();
        let mut params = Vec::new();
        let sql = ast.render(&mut params);
        trace_query("select", M::table_name(), &sql, &params);
        (sql, params)
    }

    /// Build a [`BuiltQuery`] with `$N` placeholders already applied (PostgreSQL only).
    ///
    /// Equivalent to calling `build()` followed by `rewrite_placeholders_pg`, but
    /// performs the rewrite once at build time so the adapter can skip it at execution time.
    #[cfg(feature = "postgres")]
    pub fn build_pg(&self) -> crate::built_query::BuiltQuery {
        let (sql, params) = self.build();
        let pg_sql = rewrite_placeholders_pg(&sql);
        crate::built_query::BuiltQuery::new(pg_sql, params)
    }
}

impl<M: Table> Default for SelectBuilder<M> {
    fn default() -> Self {
        Self::new()
    }
}

// ── SelectBuilder direct execution methods ──────────────────────────

impl<M: Table + crate::db::FromRow> SelectBuilder<M> {
    /// Execute this SELECT and return typed results.
    ///
    /// ```ignore
    /// let users = User::find().filter(User::active.eq(true)).fetch(&db).await?;
    /// ```
    pub async fn fetch(&self, db: &impl crate::db::Database) -> Result<Vec<M>, crate::db::DbError> {
        crate::db::fetch(db, self).await
    }

    /// Execute this SELECT and return exactly one typed result.
    ///
    /// Returns an error if the query returns 0 or 2+ rows.
    pub async fn fetch_one(&self, db: &impl crate::db::Database) -> Result<M, crate::db::DbError> {
        crate::db::fetch_one(db, self).await
    }

    /// Execute this SELECT and return 0 or 1 typed result.
    ///
    /// Returns an error if the query returns 2+ rows.
    pub async fn fetch_optional(
        &self,
        db: &impl crate::db::Database,
    ) -> Result<Option<M>, crate::db::DbError> {
        crate::db::fetch_optional(db, self).await
    }
}

// ── SelectBuilder join / with entry points ───────────────────────────

impl<M: Table> SelectBuilder<M> {
    /// Add an INNER JOIN via a [`Relation`](crate::relation::Relation).
    ///
    /// ```ignore
    /// User::find()
    ///     .join(User::posts())
    ///     .build();
    /// // SELECT users.*, posts.* FROM users INNER JOIN posts ON users.id = posts.user_id
    /// ```
    pub fn join<T: Table>(mut self, rel: crate::relation::Relation<M, T>) -> Self {
        self.joins.push(JoinClause {
            kind: JoinKind::Inner,
            table: T::table_name(),
            on: rel.join_condition(),
        });
        self
    }

    /// Add a LEFT JOIN via a [`Relation`](crate::relation::Relation).
    pub fn left_join<T: Table>(mut self, rel: crate::relation::Relation<M, T>) -> Self {
        self.joins.push(JoinClause {
            kind: JoinKind::Left,
            table: T::table_name(),
            on: rel.join_condition(),
        });
        self
    }

    /// Add a RIGHT JOIN via a [`Relation`](crate::relation::Relation).
    pub fn right_join<T: Table>(mut self, rel: crate::relation::Relation<M, T>) -> Self {
        self.joins.push(JoinClause {
            kind: JoinKind::Right,
            table: T::table_name(),
            on: rel.join_condition(),
        });
        self
    }

    /// Eager-load a relation using two queries + in-memory grouping (N+1-safe).
    ///
    /// ```ignore
    /// let wb = User::find()
    ///     .filter(User::active.eq(true))
    ///     .with(User::posts());
    /// let ((parent_sql, pp), child_tpl) = wb.build_queries();
    /// ```
    pub fn with<T: Table>(self, rel: crate::relation::Relation<M, T>) -> WithBuilder<M, T> {
        WithBuilder { parent: self, rel }
    }
}
