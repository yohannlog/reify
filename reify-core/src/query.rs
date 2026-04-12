use std::marker::PhantomData;

use crate::condition::Condition;
use crate::sql::ToSql;
use crate::table::Table;
use crate::value::Value;
use tracing::debug;

// ── Dialect ─────────────────────────────────────────────────────────

/// SQL dialect — controls syntax differences between backends.
///
/// Pass to `InsertBuilder::build_with_dialect` /
/// `InsertManyBuilder::build_with_dialect` when you need dialect-specific
/// SQL (upsert syntax, placeholder style, …).
///
/// The default `build()` method emits portable SQL with `?` placeholders
/// and no dialect-specific extensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Dialect {
    /// Generic SQL — `?` placeholders, no vendor extensions. Default.
    #[default]
    Generic,
    /// PostgreSQL — `ON CONFLICT … DO UPDATE SET` upsert syntax.
    Postgres,
    /// MySQL / MariaDB — `ON DUPLICATE KEY UPDATE` upsert syntax.
    Mysql,
}

// ── OnConflict ──────────────────────────────────────────────────────

/// Conflict-resolution strategy for INSERT statements.
#[derive(Debug, Clone)]
pub enum OnConflict {
    /// `INSERT … ON CONFLICT DO NOTHING` (PostgreSQL) /
    /// `INSERT IGNORE …` (MySQL).
    DoNothing,
    /// Upsert: on conflict on `target_cols`, update `updates`.
    ///
    /// - PostgreSQL: `ON CONFLICT (col, …) DO UPDATE SET col = EXCLUDED.col, …`
    /// - MySQL: `ON DUPLICATE KEY UPDATE col = VALUES(col), …`
    ///
    /// `target_cols` is only used by PostgreSQL (MySQL infers the conflict
    /// target from the unique key that triggered the violation).
    DoUpdate {
        /// Columns that form the conflict target (PostgreSQL `ON CONFLICT (…)`).
        target_cols: Vec<&'static str>,
        /// Columns to update on conflict.
        updates: Vec<&'static str>,
    },
}

fn trace_query(operation: &str, table: &'static str, sql: &str, params: &[Value]) {
    debug!(
        target: "reify::query",
        operation,
        table,
        sql = %sql,
        params = ?params,
        "Built SQL query"
    );
}

// ── Aggregate expressions ───────────────────────────────────────────

/// A SQL expression that can appear in a SELECT list.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A plain column reference: `col`.
    Col(&'static str),
    /// `COUNT(col)` or `COUNT(*)`.
    Count(Option<&'static str>),
    /// `SUM(col)`.
    Sum(&'static str),
    /// `AVG(col)`.
    Avg(&'static str),
    /// `MIN(col)`.
    Min(&'static str),
    /// `MAX(col)`.
    Max(&'static str),
}

impl Expr {
    /// Render the expression to a SQL fragment.
    pub fn to_sql_fragment(&self) -> String {
        match self {
            Expr::Col(c) => c.to_string(),
            Expr::Count(None) => "COUNT(*)".to_string(),
            Expr::Count(Some(c)) => format!("COUNT({c})"),
            Expr::Sum(c) => format!("SUM({c})"),
            Expr::Avg(c) => format!("AVG({c})"),
            Expr::Min(c) => format!("MIN({c})"),
            Expr::Max(c) => format!("MAX({c})"),
        }
    }
}

/// Shorthand for `COUNT(*)`.
pub fn count_all() -> Expr {
    Expr::Count(None)
}

impl Expr {
    /// `expr > val` — for use in HAVING clauses.
    pub fn gt(self, val: impl crate::value::IntoValue) -> Condition {
        Condition::AggregateGt(self, val.into_value())
    }
    /// `expr < val`
    pub fn lt(self, val: impl crate::value::IntoValue) -> Condition {
        Condition::AggregateLt(self, val.into_value())
    }
    /// `expr >= val`
    pub fn gte(self, val: impl crate::value::IntoValue) -> Condition {
        Condition::AggregateGte(self, val.into_value())
    }
    /// `expr <= val`
    pub fn lte(self, val: impl crate::value::IntoValue) -> Condition {
        Condition::AggregateLte(self, val.into_value())
    }
    /// `expr = val`
    pub fn eq(self, val: impl crate::value::IntoValue) -> Condition {
        Condition::AggregateEq(self, val.into_value())
    }
}

// ── Ordering ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Order {
    Asc(&'static str),
    Desc(&'static str),
}

/// Helper returned by `Column` — lets you write `User::id.asc()`.
pub struct OrderExpr {
    pub col: &'static str,
}

impl OrderExpr {
    pub fn asc(self) -> Order {
        Order::Asc(self.col)
    }
    pub fn desc(self) -> Order {
        Order::Desc(self.col)
    }
}

// ── SelectBuilder ───────────────────────────────────────────────────

#[derive(Clone)]
pub struct SelectBuilder<M: Table> {
    columns: Option<Vec<&'static str>>,
    exprs: Option<Vec<Expr>>,
    conditions: Vec<Condition>,
    group_by: Vec<&'static str>,
    having: Vec<Condition>,
    orders: Vec<Order>,
    limit: Option<u64>,
    offset: Option<u64>,
    _model: PhantomData<M>,
}

impl<M: Table> SelectBuilder<M> {
    pub fn new() -> Self {
        Self {
            columns: None,
            exprs: None,
            conditions: Vec::new(),
            group_by: Vec::new(),
            having: Vec::new(),
            orders: Vec::new(),
            limit: None,
            offset: None,
            _model: PhantomData,
        }
    }

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

    pub fn order_by(mut self, order: Order) -> Self {
        self.orders.push(order);
        self
    }

    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn offset(mut self, n: u64) -> Self {
        self.offset = Some(n);
        self
    }

    /// Build the SQL string and parameter list.
    pub fn build(&self) -> (String, Vec<Value>) {
        let mut params = Vec::new();

        // SELECT list: exprs take priority over plain columns.
        let select_list = if let Some(ref exprs) = self.exprs {
            exprs
                .iter()
                .map(|e| e.to_sql_fragment())
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            match &self.columns {
                Some(c) => c.join(", "),
                None => "*".to_owned(),
            }
        };
        let mut sql = format!("SELECT {} FROM {}", select_list, M::table_name());

        if !self.conditions.is_empty() {
            let where_parts: Vec<String> = self
                .conditions
                .iter()
                .map(|c| c.to_sql(&mut params))
                .collect();
            sql.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));
        }

        if !self.group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", self.group_by.join(", ")));
        }

        if !self.having.is_empty() {
            let having_parts: Vec<String> =
                self.having.iter().map(|c| c.to_sql(&mut params)).collect();
            sql.push_str(&format!(" HAVING {}", having_parts.join(" AND ")));
        }

        if !self.orders.is_empty() {
            let order_parts: Vec<String> = self
                .orders
                .iter()
                .map(|o| match o {
                    Order::Asc(c) => format!("{c} ASC"),
                    Order::Desc(c) => format!("{c} DESC"),
                })
                .collect();
            sql.push_str(&format!(" ORDER BY {}", order_parts.join(", ")));
        }

        if let Some(n) = self.limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        if let Some(n) = self.offset {
            sql.push_str(&format!(" OFFSET {n}"));
        }

        trace_query("select", M::table_name(), &sql, &params);
        (sql, params)
    }
}

impl<M: Table> Default for SelectBuilder<M> {
    fn default() -> Self {
        Self::new()
    }
}

// ── InsertBuilder ───────────────────────────────────────────────────

pub struct InsertBuilder<M: Table> {
    values: Vec<Value>,
    on_conflict: Option<OnConflict>,
    #[cfg(feature = "postgres")]
    returning: Option<Vec<&'static str>>,
    _model: PhantomData<M>,
}

impl<M: Table> InsertBuilder<M> {
    pub fn new(model: &M) -> Self {
        Self {
            values: model.into_values(),
            on_conflict: None,
            #[cfg(feature = "postgres")]
            returning: None,
            _model: PhantomData,
        }
    }

    /// Append a `RETURNING` clause (PostgreSQL only).
    ///
    /// ```ignore
    /// let (sql, params) = User::insert(&alice).returning(&["id", "email"]).build();
    /// // INSERT INTO users (id, email, role) VALUES (?, ?, ?) RETURNING id, email
    /// ```
    #[cfg(feature = "postgres")]
    pub fn returning(mut self, cols: &[&'static str]) -> Self {
        self.returning = Some(cols.to_vec());
        self
    }

    /// On conflict, do nothing.
    ///
    /// - PostgreSQL: `ON CONFLICT DO NOTHING`
    /// - MySQL: `INSERT IGNORE …`
    pub fn on_conflict_do_nothing(mut self) -> Self {
        self.on_conflict = Some(OnConflict::DoNothing);
        self
    }

    /// On conflict on `target_cols`, update `updates`.
    ///
    /// - PostgreSQL: `ON CONFLICT (target_cols) DO UPDATE SET col = EXCLUDED.col, …`
    /// - MySQL: `ON DUPLICATE KEY UPDATE col = VALUES(col), …`
    pub fn on_conflict_do_update(
        mut self,
        target_cols: &[&'static str],
        updates: &[&'static str],
    ) -> Self {
        self.on_conflict = Some(OnConflict::DoUpdate {
            target_cols: target_cols.to_vec(),
            updates: updates.to_vec(),
        });
        self
    }

    /// Build with the default (generic) dialect — no upsert extensions.
    #[allow(unused_mut)]
    pub fn build(&self) -> (String, Vec<Value>) {
        self.build_with_dialect(Dialect::Generic)
    }

    /// Build SQL for a specific [`Dialect`].
    #[allow(unused_mut)]
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let cols = M::column_names().join(", ");
        let placeholders: Vec<&str> = (0..self.values.len()).map(|_| "?").collect();

        // MySQL INSERT IGNORE prefix
        let insert_kw = match (&self.on_conflict, dialect) {
            (Some(OnConflict::DoNothing), Dialect::Mysql) => "INSERT IGNORE",
            _ => "INSERT",
        };

        let mut sql = format!(
            "{} INTO {} ({}) VALUES ({})",
            insert_kw,
            M::table_name(),
            cols,
            placeholders.join(", ")
        );

        // Conflict clause
        match (&self.on_conflict, dialect) {
            (Some(OnConflict::DoNothing), Dialect::Postgres) => {
                sql.push_str(" ON CONFLICT DO NOTHING");
            }
            (
                Some(OnConflict::DoUpdate {
                    target_cols,
                    updates,
                }),
                Dialect::Postgres,
            ) => {
                sql.push_str(&format!(
                    " ON CONFLICT ({}) DO UPDATE SET {}",
                    target_cols.join(", "),
                    updates
                        .iter()
                        .map(|c| format!("{c} = EXCLUDED.{c}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            (Some(OnConflict::DoUpdate { updates, .. }), Dialect::Mysql) => {
                sql.push_str(&format!(
                    " ON DUPLICATE KEY UPDATE {}",
                    updates
                        .iter()
                        .map(|c| format!("{c} = VALUES({c})"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            _ => {}
        }

        #[cfg(feature = "postgres")]
        if let Some(ref ret_cols) = self.returning {
            sql.push_str(&format!(" RETURNING {}", ret_cols.join(", ")));
        }

        trace_query("insert", M::table_name(), &sql, &self.values);
        (sql, self.values.clone())
    }
}

// ── InsertManyBuilder ────────────────────────────────────────────────

/// Builds a multi-row `INSERT INTO … VALUES (…), (…), …` statement.
///
/// Obtain one via the generated `Model::insert_many(&[…])` method.
pub struct InsertManyBuilder<M: Table> {
    /// Flat list of all values: row0_col0, row0_col1, …, rowN_colM.
    rows: Vec<Vec<Value>>,
    on_conflict: Option<OnConflict>,
    #[cfg(feature = "postgres")]
    returning: Option<Vec<&'static str>>,
    _model: PhantomData<M>,
}

impl<M: Table> InsertManyBuilder<M> {
    /// Create a builder from a slice of model instances.
    ///
    /// Panics if `models` is empty — an empty INSERT is a logic error.
    pub fn new(models: &[M]) -> Self {
        assert!(!models.is_empty(), "insert_many requires at least one row");
        Self {
            rows: models.iter().map(|m| m.into_values()).collect(),
            on_conflict: None,
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

    /// On conflict, do nothing.
    pub fn on_conflict_do_nothing(mut self) -> Self {
        self.on_conflict = Some(OnConflict::DoNothing);
        self
    }

    /// On conflict on `target_cols`, update `updates`.
    pub fn on_conflict_do_update(
        mut self,
        target_cols: &[&'static str],
        updates: &[&'static str],
    ) -> Self {
        self.on_conflict = Some(OnConflict::DoUpdate {
            target_cols: target_cols.to_vec(),
            updates: updates.to_vec(),
        });
        self
    }

    /// Build with the default (generic) dialect.
    pub fn build(&self) -> (String, Vec<Value>) {
        self.build_with_dialect(Dialect::Generic)
    }

    /// Build SQL for a specific [`Dialect`].
    #[allow(unused_mut)]
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let col_names = M::column_names();
        let cols = col_names.join(", ");
        let num_cols = col_names.len();

        // Build the VALUES list: (?, ?, ?), (?, ?, ?), …
        let row_placeholders: Vec<String> = self
            .rows
            .iter()
            .map(|_| {
                let ph: Vec<&str> = (0..num_cols).map(|_| "?").collect();
                format!("({})", ph.join(", "))
            })
            .collect();

        // Flatten all row values into a single params vec.
        let params: Vec<Value> = self.rows.iter().flat_map(|r| r.iter().cloned()).collect();

        let insert_kw = match (&self.on_conflict, dialect) {
            (Some(OnConflict::DoNothing), Dialect::Mysql) => "INSERT IGNORE",
            _ => "INSERT",
        };

        let mut sql = format!(
            "{} INTO {} ({}) VALUES {}",
            insert_kw,
            M::table_name(),
            cols,
            row_placeholders.join(", ")
        );

        // Conflict clause
        match (&self.on_conflict, dialect) {
            (Some(OnConflict::DoNothing), Dialect::Postgres) => {
                sql.push_str(" ON CONFLICT DO NOTHING");
            }
            (
                Some(OnConflict::DoUpdate {
                    target_cols,
                    updates,
                }),
                Dialect::Postgres,
            ) => {
                sql.push_str(&format!(
                    " ON CONFLICT ({}) DO UPDATE SET {}",
                    target_cols.join(", "),
                    updates
                        .iter()
                        .map(|c| format!("{c} = EXCLUDED.{c}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            (Some(OnConflict::DoUpdate { updates, .. }), Dialect::Mysql) => {
                sql.push_str(&format!(
                    " ON DUPLICATE KEY UPDATE {}",
                    updates
                        .iter()
                        .map(|c| format!("{c} = VALUES({c})"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            _ => {}
        }

        #[cfg(feature = "postgres")]
        if let Some(ref ret_cols) = self.returning {
            sql.push_str(&format!(" RETURNING {}", ret_cols.join(", ")));
        }

        trace_query("insert_many", M::table_name(), &sql, &params);
        (sql, params)
    }
}

// ── JoinedSelectBuilder ──────────────────────────────────────────────

/// Kind of SQL JOIN.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
}

impl JoinKind {
    fn sql_keyword(self) -> &'static str {
        match self {
            JoinKind::Inner => "INNER JOIN",
            JoinKind::Left => "LEFT JOIN",
            JoinKind::Right => "RIGHT JOIN",
        }
    }
}

/// A single JOIN clause: kind + target table + ON condition.
#[derive(Debug, Clone)]
pub struct JoinClause {
    pub kind: JoinKind,
    /// SQL table name of the joined table.
    pub table: &'static str,
    /// Raw ON condition string, e.g. `"users.id = posts.user_id"`.
    pub on: String,
}

/// A `SelectBuilder` augmented with one or more JOIN clauses.
///
/// Obtain one via [`SelectBuilder::join`], [`SelectBuilder::left_join`], or
/// [`SelectBuilder::right_join`].
///
/// ```ignore
/// let (sql, params) = User::find()
///     .join(User::posts())          // INNER JOIN posts ON users.id = posts.user_id
///     .left_join(User::profile())   // LEFT  JOIN profiles ON users.id = profiles.user_id
///     .filter(Post::published.eq(true))
///     .build();
/// ```
pub struct JoinedSelectBuilder<M: Table> {
    inner: SelectBuilder<M>,
    joins: Vec<JoinClause>,
}

impl<M: Table> JoinedSelectBuilder<M> {
    fn new(inner: SelectBuilder<M>, clause: JoinClause) -> Self {
        Self {
            inner,
            joins: vec![clause],
        }
    }

    /// Add an INNER JOIN via a [`Relation`](crate::relation::Relation).
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

    /// Add an additional WHERE filter.
    pub fn filter(mut self, cond: Condition) -> Self {
        self.inner = self.inner.filter(cond);
        self
    }

    /// Add an ORDER BY clause.
    pub fn order_by(mut self, order: Order) -> Self {
        self.inner = self.inner.order_by(order);
        self
    }

    /// Set LIMIT.
    pub fn limit(mut self, n: u64) -> Self {
        self.inner = self.inner.limit(n);
        self
    }

    /// Set OFFSET.
    pub fn offset(mut self, n: u64) -> Self {
        self.inner = self.inner.offset(n);
        self
    }

    /// Build the final SQL and parameter list.
    ///
    /// Generates `SELECT from_table.*, joined_table.*, … FROM from_table
    /// INNER/LEFT/RIGHT JOIN joined_table ON … WHERE … ORDER BY … LIMIT … OFFSET …`.
    pub fn build(&self) -> (String, Vec<Value>) {
        let mut params = Vec::new();

        // SELECT list: qualify every table with `table.*`
        let mut select_tables = vec![format!("{}.*", M::table_name())];
        // Deduplicate joined tables while preserving order.
        let mut seen = std::collections::HashSet::new();
        seen.insert(M::table_name());
        for j in &self.joins {
            if seen.insert(j.table) {
                select_tables.push(format!("{}.*", j.table));
            }
        }
        let select_list = select_tables.join(", ");

        let mut sql = format!("SELECT {} FROM {}", select_list, M::table_name());

        // JOIN clauses
        for j in &self.joins {
            sql.push_str(&format!(
                " {} {} ON {}",
                j.kind.sql_keyword(),
                j.table,
                j.on
            ));
        }

        // WHERE
        if !self.inner.conditions.is_empty() {
            let where_parts: Vec<String> = self
                .inner
                .conditions
                .iter()
                .map(|c| c.to_sql(&mut params))
                .collect();
            sql.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));
        }

        // GROUP BY
        if !self.inner.group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", self.inner.group_by.join(", ")));
        }

        // HAVING
        if !self.inner.having.is_empty() {
            let having_parts: Vec<String> = self
                .inner
                .having
                .iter()
                .map(|c| c.to_sql(&mut params))
                .collect();
            sql.push_str(&format!(" HAVING {}", having_parts.join(" AND ")));
        }

        // ORDER BY
        if !self.inner.orders.is_empty() {
            let order_parts: Vec<String> = self
                .inner
                .orders
                .iter()
                .map(|o| match o {
                    Order::Asc(c) => format!("{c} ASC"),
                    Order::Desc(c) => format!("{c} DESC"),
                })
                .collect();
            sql.push_str(&format!(" ORDER BY {}", order_parts.join(", ")));
        }

        if let Some(n) = self.inner.limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        if let Some(n) = self.inner.offset {
            sql.push_str(&format!(" OFFSET {n}"));
        }

        trace_query("select_join", M::table_name(), &sql, &params);
        (sql, params)
    }
}

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
    parent: SelectBuilder<F>,
    rel: crate::relation::Relation<F, T>,
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
            T::table_name(),
            self.rel.to_col,
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

// ── SelectBuilder join / with entry points ───────────────────────────

impl<M: Table> SelectBuilder<M> {
    /// Start a joined query with an INNER JOIN.
    ///
    /// ```ignore
    /// User::find()
    ///     .join(User::posts())
    ///     .build();
    /// // SELECT users.*, posts.* FROM users INNER JOIN posts ON users.id = posts.user_id
    /// ```
    pub fn join<T: Table>(self, rel: crate::relation::Relation<M, T>) -> JoinedSelectBuilder<M> {
        let clause = JoinClause {
            kind: JoinKind::Inner,
            table: T::table_name(),
            on: rel.join_condition(),
        };
        JoinedSelectBuilder::new(self, clause)
    }

    /// Start a joined query with a LEFT JOIN.
    pub fn left_join<T: Table>(
        self,
        rel: crate::relation::Relation<M, T>,
    ) -> JoinedSelectBuilder<M> {
        let clause = JoinClause {
            kind: JoinKind::Left,
            table: T::table_name(),
            on: rel.join_condition(),
        };
        JoinedSelectBuilder::new(self, clause)
    }

    /// Start a joined query with a RIGHT JOIN.
    pub fn right_join<T: Table>(
        self,
        rel: crate::relation::Relation<M, T>,
    ) -> JoinedSelectBuilder<M> {
        let clause = JoinClause {
            kind: JoinKind::Right,
            table: T::table_name(),
            on: rel.join_condition(),
        };
        JoinedSelectBuilder::new(self, clause)
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

// ── UpdateBuilder ───────────────────────────────────────────────────

#[derive(Clone)]
pub struct UpdateBuilder<M: Table> {
    sets: Vec<(&'static str, Value)>,
    conditions: Vec<Condition>,
    #[cfg(feature = "postgres")]
    returning: Option<Vec<&'static str>>,
    _model: PhantomData<M>,
}

impl<M: Table> UpdateBuilder<M> {
    pub fn new() -> Self {
        Self {
            sets: Vec::new(),
            conditions: Vec::new(),
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

    pub fn set<T: crate::value::IntoValue>(
        mut self,
        col: crate::column::Column<M, T>,
        val: impl crate::value::IntoValue,
    ) -> Self {
        self.sets.push((col.name, val.into_value()));
        self
    }

    pub fn filter(mut self, cond: Condition) -> Self {
        self.conditions.push(cond);
        self
    }

    /// Build the SQL. Panics if no WHERE clause — safety by default.
    #[allow(unused_mut)]
    pub fn build(&self) -> (String, Vec<Value>) {
        assert!(
            !self.conditions.is_empty(),
            "UPDATE without WHERE is forbidden. Use .filter() or .filter_all() explicitly."
        );

        let mut params = Vec::new();
        let set_parts: Vec<String> = self
            .sets
            .iter()
            .map(|(col, val)| {
                params.push(val.clone());
                format!("{col} = ?")
            })
            .collect();

        let mut sql = format!("UPDATE {} SET {}", M::table_name(), set_parts.join(", "));

        let where_parts: Vec<String> = self
            .conditions
            .iter()
            .map(|c| c.to_sql(&mut params))
            .collect();
        sql.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));

        #[cfg(feature = "postgres")]
        if let Some(ref ret_cols) = self.returning {
            sql.push_str(&format!(" RETURNING {}", ret_cols.join(", ")));
        }

        trace_query("update", M::table_name(), &sql, &params);
        (sql, params)
    }
}

impl<M: Table> Default for UpdateBuilder<M> {
    fn default() -> Self {
        Self::new()
    }
}

// ── DeleteBuilder ───────────────────────────────────────────────────

#[derive(Clone)]
pub struct DeleteBuilder<M: Table> {
    conditions: Vec<Condition>,
    #[cfg(feature = "postgres")]
    returning: Option<Vec<&'static str>>,
    _model: PhantomData<M>,
}

impl<M: Table> DeleteBuilder<M> {
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
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

    pub fn filter(mut self, cond: Condition) -> Self {
        self.conditions.push(cond);
        self
    }

    /// Build the SQL. Panics if no WHERE clause — safety by default.
    #[allow(unused_mut)]
    pub fn build(&self) -> (String, Vec<Value>) {
        assert!(
            !self.conditions.is_empty(),
            "DELETE without WHERE is forbidden. Use .filter() explicitly."
        );

        let mut params = Vec::new();
        let mut sql = format!("DELETE FROM {}", M::table_name());

        let where_parts: Vec<String> = self
            .conditions
            .iter()
            .map(|c| c.to_sql(&mut params))
            .collect();
        sql.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));

        #[cfg(feature = "postgres")]
        if let Some(ref ret_cols) = self.returning {
            sql.push_str(&format!(" RETURNING {}", ret_cols.join(", ")));
        }

        trace_query("delete", M::table_name(), &sql, &params);
        (sql, params)
    }
}

impl<M: Table> Default for DeleteBuilder<M> {
    fn default() -> Self {
        Self::new()
    }
}
