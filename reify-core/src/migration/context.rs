use crate::ident::qi;

/// Context passed to `Migration::up` and `Migration::down`.
///
/// Collects SQL statements to be executed (or previewed in dry-run mode).
pub struct MigrationContext {
    /// Accumulated SQL statements in execution order.
    pub(crate) statements: Vec<String>,
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
    pub fn execute(&mut self, sql: impl Into<String>) {
        self.statements.push(sql.into());
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
}
