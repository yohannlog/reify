use std::marker::PhantomData;

use crate::column::Column;
use crate::schema::ColumnDef;
use crate::table::Table;
use crate::value::Value;

// ── ViewQuery ───────────────────────────────────────────────────────

/// How the view's underlying SELECT is defined.
#[derive(Debug, Clone)]
pub enum ViewQuery {
    /// A raw SQL string — for complex queries (joins, subqueries, aggregations).
    ///
    /// ```ignore
    /// ViewQuery::Raw("SELECT u.id, u.email FROM users u WHERE u.deleted_at IS NULL".into())
    /// ```
    Raw(String),

    /// A query built from a typed `SelectBuilder` — compile-time checked columns.
    ///
    /// The SQL and params are pre-rendered from the builder.
    Typed { sql: String, params: Vec<Value> },
}

// ── View trait ───────────────────────────────────────────────────────

/// Trait implemented by `#[derive(View)]` on user structs.
///
/// Views are read-only projections — they support `find()` (SELECT) but
/// not INSERT, UPDATE, or DELETE.
pub trait View: Table {
    /// SQL view name.
    fn view_name() -> &'static str;

    /// The SELECT query that defines this view.
    fn view_query() -> ViewQuery;
}

// ── ViewDef ─────────────────────────────────────────────────────────

/// Metadata for a view definition, used by the migration runner.
#[derive(Debug, Clone)]
pub struct ViewDef {
    /// SQL view name.
    pub name: &'static str,
    /// The SELECT query (rendered SQL).
    pub query: String,
}

// ── ViewSchema builder ──────────────────────────────────────────────

/// Schema definition for a view, built via the fluent `view()` entry point.
///
/// ```ignore
/// reify::view_schema::<ActiveUser>("active_users")
///     .column(ActiveUser::id, |c| c.sql_type(SqlType::BigInt))
///     .column(ActiveUser::email, |c| c.sql_type(SqlType::Text))
///     .raw_query("SELECT id, email FROM users WHERE deleted_at IS NULL")
/// ```
#[derive(Debug, Clone)]
pub struct ViewSchema<M> {
    pub name: &'static str,
    pub columns: Vec<ColumnDef>,
    pub query: Option<ViewQuery>,
    _model: PhantomData<M>,
}

impl<M> ViewSchema<M> {
    /// Set the view query from a compile-time SQL literal.
    ///
    /// Use this for complex queries that cannot be expressed with
    /// `SelectBuilder` (multi-table joins, subqueries, window
    /// functions, etc.).
    ///
    /// `sql` is `&'static str` so that no runtime-built string \(and
    /// therefore no attacker-controlled input\) can become a view
    /// definition. If the SQL genuinely needs to vary at runtime,
    /// whitelist the inputs at the call site and `Box::leak` the
    /// resulting string \(an explicit, auditable opt-in\).
    pub fn raw_query(mut self, sql: &'static str) -> Self {
        self.query = Some(ViewQuery::Raw(sql.to_owned()));
        self
    }

    /// Extract the query SQL string (for migration use).
    pub fn query_sql(&self) -> Option<String> {
        self.query.as_ref().map(|q| match q {
            ViewQuery::Raw(s) => s.clone(),
            ViewQuery::Typed { sql, .. } => sql.clone(),
        })
    }
}

impl<M: Table> ViewSchema<M> {
    /// Add a column with its attributes configured via a closure.
    pub fn column<T, S: crate::schema::TimestampState>(
        mut self,
        col: Column<M, T>,
        configure: impl FnOnce(
            crate::schema::ColumnBuilder<T, crate::schema::NoTimestamp>,
        ) -> crate::schema::ColumnBuilder<T, S>,
    ) -> Self {
        let builder =
            crate::schema::ColumnBuilder::<T, crate::schema::NoTimestamp>::new_pub(col.name);
        self.columns.push(configure(builder).build());
        self
    }

    /// Set the view query from a typed `SelectBuilder`.
    ///
    /// The builder is rendered to SQL immediately. Column references are
    /// compile-time checked.
    ///
    /// ```ignore
    /// .query(User::find().select(&["id", "email"]).filter(User::deleted_at.is_null()))
    /// ```
    pub fn query<S: Table>(mut self, builder: crate::query::SelectBuilder<S>) -> Self {
        let (sql, params) = builder.build();
        self.query = Some(ViewQuery::Typed { sql, params });
        self
    }
}

/// Entry point: create a `ViewSchema` for a model.
///
/// ```ignore
/// reify::view_schema::<ActiveUser>("active_users")
///     .column(ActiveUser::id, |c| c.sql_type(SqlType::BigInt))
///     .raw_query("SELECT id, email FROM users WHERE deleted_at IS NULL")
/// ```
pub fn view<M>(name: &'static str) -> ViewSchema<M> {
    ViewSchema {
        name,
        columns: Vec::new(),
        query: None,
        _model: PhantomData,
    }
}

// ── ViewSchemaDef trait ─────────────────────────────────────────────

/// Trait for defining view schema via the builder API.
///
/// ```ignore
/// impl reify::ViewSchemaDef for ActiveUser {
///     fn view_schema() -> reify::ViewSchema<Self> {
///         reify::view_schema::<Self>("active_users")
///             .column(ActiveUser::id, |c| c.sql_type(SqlType::BigInt))
///             .raw_query("SELECT id, email FROM users WHERE deleted_at IS NULL")
///     }
/// }
/// ```
pub trait ViewSchemaDef: View {
    fn view_schema() -> ViewSchema<Self>;
}

// ── DDL generation ──────────────────────────────────────────────────

/// Generate a `CREATE OR REPLACE VIEW` statement.
pub fn create_view_sql(name: &str, query: &str) -> String {
    let quoted = crate::ident::qi(name);
    format!("CREATE OR REPLACE VIEW {quoted} AS {query};")
}

/// Generate a `DROP VIEW IF EXISTS` statement.
pub fn drop_view_sql(name: &str) -> String {
    let quoted = crate::ident::qi(name);
    format!("DROP VIEW IF EXISTS {quoted};")
}

// ── Materialized view DDL (PostgreSQL) ──────────────────────────────

/// Generate a `CREATE MATERIALIZED VIEW` statement (PostgreSQL).
///
/// Pass `with_data = true` to populate the view immediately (default behaviour).
/// Pass `with_data = false` to create the view structure without loading data
/// (`WITH NO DATA`) — useful when you want to create indexes before the first
/// `REFRESH MATERIALIZED VIEW`.
///
/// ```ignore
/// // Populate immediately (most common)
/// create_materialized_view_sql("sales_summary", "SELECT ...", true);
///
/// // Create empty, add indexes, then refresh
/// create_materialized_view_sql("sales_summary", "SELECT ...", false);
/// ```
pub fn create_materialized_view_sql(name: &str, query: &str, with_data: bool) -> String {
    let quoted = crate::ident::qi(name);
    let data_clause = if with_data {
        "WITH DATA"
    } else {
        "WITH NO DATA"
    };
    format!("CREATE MATERIALIZED VIEW IF NOT EXISTS {quoted} AS {query} {data_clause};")
}

/// Generate a `DROP MATERIALIZED VIEW IF EXISTS` statement (PostgreSQL).
pub fn drop_materialized_view_sql(name: &str) -> String {
    let quoted = crate::ident::qi(name);
    format!("DROP MATERIALIZED VIEW IF EXISTS {quoted};")
}

/// Generate a `REFRESH MATERIALIZED VIEW` statement (PostgreSQL).
///
/// Pass `concurrently = true` to use `REFRESH MATERIALIZED VIEW CONCURRENTLY`,
/// which allows reads to continue during the refresh. Requires at least one
/// unique index on the materialized view.
///
/// Pass `concurrently = false` for a plain refresh that locks the view for
/// the duration of the operation.
///
/// ```ignore
/// // Non-blocking refresh (requires a unique index)
/// refresh_materialized_view_sql("sales_summary", true);
///
/// // Blocking refresh (no index required)
/// refresh_materialized_view_sql("sales_summary", false);
/// ```
pub fn refresh_materialized_view_sql(name: &str, concurrently: bool) -> String {
    let quoted = crate::ident::qi(name);
    if concurrently {
        format!("REFRESH MATERIALIZED VIEW CONCURRENTLY {quoted};")
    } else {
        format!("REFRESH MATERIALIZED VIEW {quoted};")
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_view_sql_generates_correct_ddl() {
        let sql = create_view_sql(
            "active_users",
            "SELECT id, email FROM users WHERE deleted_at IS NULL",
        );
        assert_eq!(
            sql,
            "CREATE OR REPLACE VIEW \"active_users\" AS SELECT id, email FROM users WHERE deleted_at IS NULL;"
        );
    }

    #[test]
    fn drop_view_sql_generates_correct_ddl() {
        let sql = drop_view_sql("active_users");
        assert_eq!(sql, "DROP VIEW IF EXISTS \"active_users\";");
    }

    #[test]
    fn view_query_raw_variant() {
        let q = ViewQuery::Raw("SELECT 1".into());
        match q {
            ViewQuery::Raw(s) => assert_eq!(s, "SELECT 1"),
            _ => panic!("expected Raw"),
        }
    }

    #[test]
    fn view_query_typed_variant() {
        let q = ViewQuery::Typed {
            sql: "SELECT id FROM users WHERE email = ?".into(),
            params: vec![Value::String("test@example.com".into())],
        };
        match q {
            ViewQuery::Typed { sql, params } => {
                assert!(sql.contains("SELECT id"));
                assert_eq!(params.len(), 1);
            }
            _ => panic!("expected Typed"),
        }
    }

    #[test]
    fn view_schema_builder_raw_query() {
        let schema: ViewSchema<()> = view::<()>("test_view").raw_query("SELECT 1");
        assert_eq!(schema.name, "test_view");
        assert!(schema.query_sql().unwrap().contains("SELECT 1"));
    }
}
