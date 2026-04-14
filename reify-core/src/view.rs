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
    /// Set the view query from a raw SQL string.
    ///
    /// Use this for complex queries that cannot be expressed with `SelectBuilder`
    /// (multi-table joins, subqueries, window functions, etc.).
    pub fn raw_query(mut self, sql: impl Into<String>) -> Self {
        self.query = Some(ViewQuery::Raw(sql.into()));
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
