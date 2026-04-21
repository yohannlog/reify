use crate::ident::qi;

/// Context passed to `Migration::up` and `Migration::down`.
///
/// Collects SQL statements to be executed (or previewed in dry-run mode).
pub struct MigrationContext {
    /// Accumulated SQL statements in execution order.
    statements: Vec<String>,
}

impl MigrationContext {
    /// Create a new, empty migration context.
    pub fn new() -> Self {
        Self {
            statements: Vec::new(),
        }
    }

    /// Return the accumulated SQL statements.
    pub fn statements(&self) -> &[String] {
        &self.statements
    }

    /// Consume the context and return the accumulated SQL statements.
    pub(crate) fn into_statements(self) -> Vec<String> {
        self.statements
    }

    /// Add a column to an existing table.
    ///
    /// ```ignore
    /// ctx.add_column("users", "city", "TEXT NOT NULL DEFAULT ''");
    /// ```
    pub fn add_column(&mut self, table: &str, column: &str, sql_type: &str) {
        self.statements.push(format!(
            "ALTER TABLE {} ADD COLUMN {} {sql_type};",
            qi(table),
            qi(column)
        ));
    }

    /// Drop a column from an existing table.
    pub fn drop_column(&mut self, table: &str, column: &str) {
        self.statements.push(format!(
            "ALTER TABLE {} DROP COLUMN {};",
            qi(table),
            qi(column)
        ));
    }

    /// Rename a column in an existing table.
    pub fn rename_column(&mut self, table: &str, from: &str, to: &str) {
        self.statements.push(format!(
            "ALTER TABLE {} RENAME COLUMN {} TO {};",
            qi(table),
            qi(from),
            qi(to)
        ));
    }

    /// Execute a raw SQL statement as part of this migration.
    ///
    /// Use `?` as the placeholder character.
    ///
    /// The statement is trimmed of surrounding whitespace. A trailing semicolon
    /// is appended automatically if absent — some drivers reject statements
    /// without one.
    pub fn execute(&mut self, sql: impl Into<String>) {
        let mut s = sql.into();
        let trimmed = s.trim_end();
        if !trimmed.ends_with(';') {
            s = format!("{trimmed};");
        } else {
            s = trimmed.to_string();
        }
        self.statements.push(s);
    }

    /// Create or replace a SQL view.
    ///
    /// ```ignore
    /// ctx.create_view("active_users", "SELECT id, email FROM users WHERE deleted_at IS NULL");
    /// ```
    pub fn create_view(&mut self, name: &str, query: &str) {
        self.statements
            .push(crate::view::create_view_sql(name, query));
    }

    /// Drop a SQL view if it exists.
    ///
    /// ```ignore
    /// ctx.drop_view("active_users");
    /// ```
    pub fn drop_view(&mut self, name: &str) {
        self.statements.push(crate::view::drop_view_sql(name));
    }

    // ── Materialized views (PostgreSQL) ──────────────────────────────

    /// Create a materialized view (PostgreSQL).
    ///
    /// The view is populated immediately (`WITH DATA`). To create it empty
    /// first (e.g. to add indexes before the initial refresh), use
    /// [`create_materialized_view_no_data`](Self::create_materialized_view_no_data).
    ///
    /// ```ignore
    /// ctx.create_materialized_view(
    ///     "sales_summary",
    ///     "SELECT seller_no, invoice_date, sum(invoice_amt) FROM invoice GROUP BY 1, 2",
    /// );
    /// ```
    pub fn create_materialized_view(&mut self, name: &str, query: &str) {
        self.statements
            .push(crate::view::create_materialized_view_sql(name, query, true));
    }

    /// Create a materialized view without loading data (`WITH NO DATA`, PostgreSQL).
    ///
    /// Use this when you need to create indexes on the view before the first
    /// `REFRESH MATERIALIZED VIEW CONCURRENTLY`. Follow up with
    /// [`refresh_materialized_view`](Self::refresh_materialized_view) once
    /// indexes are in place.
    ///
    /// ```ignore
    /// ctx.create_materialized_view_no_data("sales_summary", "SELECT ...");
    /// ctx.execute("CREATE UNIQUE INDEX sales_summary_seller ON sales_summary (seller_no, invoice_date);");
    /// ctx.refresh_materialized_view("sales_summary", false);
    /// ```
    pub fn create_materialized_view_no_data(&mut self, name: &str, query: &str) {
        self.statements
            .push(crate::view::create_materialized_view_sql(
                name, query, false,
            ));
    }

    /// Drop a materialized view if it exists (PostgreSQL).
    ///
    /// ```ignore
    /// ctx.drop_materialized_view("sales_summary");
    /// ```
    pub fn drop_materialized_view(&mut self, name: &str) {
        self.statements
            .push(crate::view::drop_materialized_view_sql(name));
    }

    /// Refresh a materialized view (PostgreSQL).
    ///
    /// Set `concurrently = true` for a non-blocking refresh that allows reads
    /// during the operation — requires at least one unique index on the view.
    /// Set `concurrently = false` for a plain blocking refresh.
    ///
    /// ```ignore
    /// // Non-blocking (unique index required)
    /// ctx.refresh_materialized_view("sales_summary", true);
    ///
    /// // Blocking
    /// ctx.refresh_materialized_view("sales_summary", false);
    /// ```
    pub fn refresh_materialized_view(&mut self, name: &str, concurrently: bool) {
        self.statements
            .push(crate::view::refresh_materialized_view_sql(
                name,
                concurrently,
            ));
    }
}
