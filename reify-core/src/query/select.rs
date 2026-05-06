use super::join::{JoinClause, JoinKind};
#[cfg(feature = "postgres")]
use super::rewrite_placeholders_pg;
use super::with::WithBuilder;
use super::{Expr, Order, trace_query};
use crate::condition::Condition;
use crate::ident::qi;
use crate::soft_delete::SoftDeleteFilter;
use crate::table::Table;
use crate::value::Value;
use std::marker::PhantomData;

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
#[must_use = "SelectBuilder is lazy; chain `.build()`, `.fetch(&db)`, or another execution method to use it"]
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
    soft_delete_filter: SoftDeleteFilter,
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
            soft_delete_filter: SoftDeleteFilter::Default,
            _model: PhantomData,
        }
    }

    /// Include soft-deleted rows in the results.
    ///
    /// By default, models with a `#[column(soft_delete)]` column automatically
    /// filter out deleted rows. Call this to include them.
    ///
    /// ```ignore
    /// // Include both active and deleted users
    /// let all_users = User::find().with_deleted().fetch(&db).await?;
    /// ```
    pub fn with_deleted(mut self) -> Self {
        self.soft_delete_filter = SoftDeleteFilter::WithDeleted;
        self
    }

    /// Return only soft-deleted rows.
    ///
    /// Filters to rows where the soft-delete column IS NOT NULL.
    ///
    /// ```ignore
    /// // Get only deleted users (e.g., for a trash view)
    /// let deleted_users = User::find().only_deleted().fetch(&db).await?;
    /// ```
    pub fn only_deleted(mut self) -> Self {
        self.soft_delete_filter = SoftDeleteFilter::OnlyDeleted;
        self
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
    #[must_use]
    pub fn build_ast<'a>(&'a self) -> crate::sql::SqlFragment<'a> {
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
            .collect::<Vec<_>>();

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
            .collect::<Vec<_>>();

        crate::sql::SqlFragment::Select {
            distinct: self.distinct,
            columns,
            from: M::table_name().to_string(),
            joins: std::borrow::Cow::Owned(joins),
            conditions: std::borrow::Cow::Borrowed(self.conditions.as_slice()),
            group_by: self.group_by.iter().map(|s| qi(s)).collect(),
            having: std::borrow::Cow::Borrowed(self.having.as_slice()),
            order_by: std::borrow::Cow::Owned(order_by),
            limit: self.limit,
            offset: self.offset,
        }
    }

    /// Build the SQL string and parameter list.
    #[must_use]
    pub fn build(&self) -> (String, Vec<Value>) {
        // Determine if we need to inject a soft-delete filter
        let soft_delete_condition = self.soft_delete_condition();

        let ast = if soft_delete_condition.is_some() {
            // Clone conditions and add soft-delete filter
            let mut conditions = self.conditions.clone();
            if let Some(cond) = soft_delete_condition {
                // Prepend soft-delete condition so it appears first in WHERE
                conditions.insert(0, cond);
            }
            self.build_ast_with_conditions(conditions)
        } else {
            self.build_ast()
        };

        let mut params = Vec::new();
        let sql = ast.render(&mut params);
        trace_query("select", M::table_name(), &sql, &params);
        (sql, params)
    }

    /// Compute the soft-delete condition based on filter mode and model config.
    ///
    /// `SoftDeleteFilter::Default` always emits `IS NULL` — the safe
    /// default. Callers wanting to see deleted rows must opt in
    /// per-query via `.with_deleted()` or `.only_deleted()`; there is
    /// no process-wide toggle.
    fn soft_delete_condition(&self) -> Option<Condition> {
        let col = M::soft_delete_column()?;

        match self.soft_delete_filter {
            SoftDeleteFilter::Default => Some(Condition::IsNull(col)),
            SoftDeleteFilter::WithDeleted => None,
            SoftDeleteFilter::OnlyDeleted => Some(Condition::IsNotNull(col)),
        }
    }

    /// Build AST with custom conditions (used for soft-delete injection).
    fn build_ast_with_conditions(&self, conditions: Vec<Condition>) -> crate::sql::SqlFragment<'_> {
        let has_joins = !self.joins.is_empty();

        let columns = if let Some(ref exprs) = self.exprs {
            exprs.iter().map(|e| e.to_sql_fragment()).collect()
        } else if has_joins {
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
            .collect::<Vec<_>>();

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
            .collect::<Vec<_>>();

        crate::sql::SqlFragment::Select {
            distinct: self.distinct,
            columns,
            from: M::table_name().to_string(),
            joins: std::borrow::Cow::Owned(joins),
            conditions: std::borrow::Cow::Owned(conditions),
            group_by: self.group_by.iter().map(|s| qi(s)).collect(),
            having: std::borrow::Cow::Borrowed(self.having.as_slice()),
            order_by: std::borrow::Cow::Owned(order_by),
            limit: self.limit,
            offset: self.offset,
        }
    }

    /// Build a [`crate::BuiltQuery`] with `$N` placeholders already applied (PostgreSQL only).
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

impl<M: Table> Clone for SelectBuilder<M> {
    fn clone(&self) -> Self {
        Self {
            distinct: self.distinct,
            columns: self.columns.clone(),
            exprs: self.exprs.clone(),
            conditions: self.conditions.clone(),
            joins: self.joins.clone(),
            group_by: self.group_by.clone(),
            having: self.having.clone(),
            orders: self.orders.clone(),
            limit: self.limit,
            offset: self.offset,
            soft_delete_filter: self.soft_delete_filter,
            _model: PhantomData,
        }
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

    /// Execute this SELECT and return an asynchronous stream of typed results.
    pub async fn fetch_stream<'a>(
        &self,
        db: &'a impl crate::db::Database,
    ) -> Result<crate::db::BoxStream<'a, M>, crate::db::DbError> {
        crate::db::fetch_stream(db, self).await
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

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::query::Order;
    use crate::relation::{Relation, RelationType};

    /// Standard table without soft-delete.
    struct User;
    impl Table for User {
        fn table_name() -> &'static str {
            "users"
        }
        fn column_names() -> &'static [&'static str] {
            &["id", "name", "age"]
        }
        fn as_values(&self) -> Vec<Value> {
            vec![]
        }
    }

    /// Child table for join/with tests.
    struct Post;
    impl Table for Post {
        fn table_name() -> &'static str {
            "posts"
        }
        fn column_names() -> &'static [&'static str] {
            &["id", "user_id", "title"]
        }
        fn as_values(&self) -> Vec<Value> {
            vec![]
        }
    }

    /// Table with a soft-delete column.
    struct Doc;
    impl Table for Doc {
        fn table_name() -> &'static str {
            "docs"
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

    const ID: Column<User, i64> = Column::new("id");
    const AGE: Column<User, i32> = Column::new("age");

    fn posts_relation() -> Relation<User, Post> {
        Relation::new("posts", RelationType::HasMany, "id", "user_id")
    }

    #[test]
    fn empty_select_emits_select_star() {
        let (sql, params) = SelectBuilder::<User>::new().build();
        assert!(sql.contains("FROM \"users\""), "sql: {sql}");
        assert!(params.is_empty());
    }

    #[test]
    fn distinct_emits_distinct_keyword() {
        let (sql, _) = SelectBuilder::<User>::new().distinct().build();
        assert!(sql.contains("SELECT DISTINCT"), "sql: {sql}");
    }

    #[test]
    fn select_cols_restricts_column_list() {
        // `select_cols` is generic over a single column type — pass a slice
        // of homogeneous columns (here: just the i64 PK).
        let (sql, _) = SelectBuilder::<User>::new().select_cols(&[ID]).build();
        assert!(sql.contains("\"id\""), "sql: {sql}");
        assert!(!sql.contains("\"name\""), "sql: {sql}");
        assert!(!sql.contains("\"age\""), "sql: {sql}");
    }

    #[test]
    fn select_string_cols_restricts_column_list() {
        let (sql, _) = SelectBuilder::<User>::new().select(&["id", "name"]).build();
        assert!(sql.contains("\"id\"") && sql.contains("\"name\""));
    }

    #[test]
    fn filter_accumulates_with_and() {
        let (sql, params) = SelectBuilder::<User>::new()
            .filter(ID.eq(1i64))
            .filter(AGE.gt(18i32))
            .build();
        assert!(sql.contains(" WHERE "), "sql: {sql}");
        assert!(sql.contains(" AND "), "sql: {sql}");
        assert_eq!(params, vec![Value::I64(1), Value::I32(18)]);
    }

    #[test]
    fn order_by_preserves_call_order() {
        let (sql, _) = SelectBuilder::<User>::new()
            .order_by(Order::Asc("a"))
            .order_by(Order::Desc("b"))
            .build();
        assert!(sql.contains("ORDER BY"), "sql: {sql}");
        let asc = sql.find("\"a\" ASC").expect("a ASC");
        let desc = sql.find("\"b\" DESC").expect("b DESC");
        assert!(asc < desc, "ascending key must come first: {sql}");
    }

    #[test]
    fn limit_and_offset_appear_in_sql() {
        let (sql, _) = SelectBuilder::<User>::new().limit(10).offset(20).build();
        assert!(sql.contains("LIMIT 10"), "sql: {sql}");
        assert!(sql.contains("OFFSET 20"), "sql: {sql}");
    }

    #[test]
    fn group_by_and_having() {
        let (sql, _) = SelectBuilder::<User>::new()
            .group_by(&["age"])
            .having(AGE.gt(0i32))
            .build();
        assert!(sql.contains("GROUP BY \"age\""), "sql: {sql}");
        assert!(sql.contains("HAVING"), "sql: {sql}");
    }

    #[test]
    fn group_by_cols_typed_variant() {
        let (sql, _) = SelectBuilder::<User>::new().group_by_cols(&[AGE]).build();
        assert!(sql.contains("GROUP BY \"age\""), "sql: {sql}");
    }

    #[test]
    fn join_emits_inner_join() {
        let (sql, _) = SelectBuilder::<User>::new().join(posts_relation()).build();
        assert!(sql.contains("INNER JOIN \"posts\""), "sql: {sql}");
        assert!(sql.contains("ON"), "sql: {sql}");
    }

    #[test]
    fn left_join_emits_left_join() {
        let (sql, _) = SelectBuilder::<User>::new()
            .left_join(posts_relation())
            .build();
        assert!(sql.contains("LEFT JOIN \"posts\""), "sql: {sql}");
    }

    #[test]
    fn right_join_emits_right_join() {
        let (sql, _) = SelectBuilder::<User>::new()
            .right_join(posts_relation())
            .build();
        assert!(sql.contains("RIGHT JOIN \"posts\""), "sql: {sql}");
    }

    #[test]
    fn join_qualifies_columns_per_table() {
        let (sql, _) = SelectBuilder::<User>::new().join(posts_relation()).build();
        assert!(sql.contains("\"users\".*"), "sql: {sql}");
        assert!(sql.contains("\"posts\".*"), "sql: {sql}");
    }

    // ── Soft-delete behaviour ───────────────────────────────────────

    #[test]
    fn soft_delete_default_filters_is_null() {
        let (sql, _) = SelectBuilder::<Doc>::new().build();
        assert!(
            sql.contains("\"deleted_at\" IS NULL"),
            "expected soft-delete filter, got: {sql}"
        );
    }

    #[test]
    fn with_deleted_drops_filter() {
        let (sql, _) = SelectBuilder::<Doc>::new().with_deleted().build();
        assert!(
            !sql.contains("\"deleted_at\""),
            "with_deleted must omit filter: {sql}"
        );
    }

    #[test]
    fn only_deleted_emits_is_not_null() {
        let (sql, _) = SelectBuilder::<Doc>::new().only_deleted().build();
        assert!(sql.contains("\"deleted_at\" IS NOT NULL"), "sql: {sql}");
    }

    #[test]
    fn soft_delete_filter_skipped_for_table_without_column() {
        let (sql, _) = SelectBuilder::<User>::new().build();
        assert!(!sql.contains("deleted_at"), "sql: {sql}");
    }

    // ── with() returns a WithBuilder bridging to with.rs tests ──────

    #[test]
    fn with_returns_builder_referencing_relation() {
        let wb = SelectBuilder::<User>::new().with(posts_relation());
        assert_eq!(wb.relation().name, "posts");
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn build_pg_rewrites_placeholders() {
        let bq = SelectBuilder::<User>::new()
            .filter(ID.eq(1i64))
            .filter(AGE.gt(0i32))
            .build_pg();
        let (sql, _) = bq.into_parts();
        assert!(sql.contains("$1") && sql.contains("$2"), "sql: {sql}");
        assert!(!sql.contains('?'));
    }
}
