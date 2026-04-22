mod apply;
pub(crate) mod entries;
mod hooks;
mod inspect;
mod plans;
mod queries;
mod rollback;

use entries::{MatViewEntry, TableEntry, ViewEntry};
pub use hooks::MigrationHooks;
use hooks::{MigrationErrorHookFn, MigrationHookFn};

use super::ddl::{
    add_column_sql, create_table_sql, create_table_sql_named, create_table_sql_with_checks,
};
use super::traits::Migration;
use crate::migration::plan::MigrationPlan;
use crate::query::Dialect;
use crate::table::Table;
use futures_core::future::BoxFuture;

/// Orchestrates automatic diff-based migrations and manual `Migration` impls.
///
/// # Usage
///
/// ```ignore
/// MigrationRunner::new()
///     .add_table::<User>()                  // auto CREATE TABLE / ADD COLUMN
///     .add_table::<Post>()
///     .add_view::<ActiveUser>()             // auto CREATE OR REPLACE VIEW
///     .add_materialized_view::<SalesSummary>() // auto CREATE MATERIALIZED VIEW
///     .add(SplitAddress)                    // manual migration
///     .run(&db)
///     .await?;
/// ```
///
/// # Known limitation — auto-versioning keys
///
/// Auto-diff entries use `auto__{table_name}` as their version key. If you
/// rename a table the old key becomes an orphan in `_reify_migrations` and
/// the new name is treated as a brand-new table. Clean up the orphan row
/// manually after renaming.
pub struct MigrationRunner {
    pub(super) tables: Vec<TableEntry>,
    pub(super) views: Vec<ViewEntry>,
    pub(super) mat_views: Vec<MatViewEntry>,
    pub(super) manual: Vec<Box<dyn Migration>>,
    /// SQL dialect override. When `None`, dialect is auto-detected from the
    /// database connection at runtime (Drizzle-style ergonomics).
    pub(super) dialect_override: Option<Dialect>,
    /// Lifecycle hooks — called around each plan execution.
    pub(super) hooks: MigrationHooks,
}

impl MigrationRunner {
    /// Create a new, empty runner with auto-detected dialect.
    ///
    /// The SQL dialect is automatically detected from the database connection
    /// when `run()` / `dry_run()` is called. Use `with_dialect()` to override.
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            views: Vec::new(),
            mat_views: Vec::new(),
            manual: Vec::new(),
            dialect_override: None,
            hooks: MigrationHooks::default(),
        }
    }

    /// Resolve the effective dialect for this run.
    ///
    /// If `with_dialect()` was called, that dialect is used (explicit override).
    /// Otherwise, the dialect is auto-detected from the database connection.
    /// This enables Drizzle-style ergonomics where the dialect is intrinsic to
    /// the connection — no need to specify it twice.
    pub(super) fn resolve_dialect(&self, db: &impl crate::db::Database) -> Dialect {
        self.dialect_override.unwrap_or_else(|| db.dialect())
    }

    /// Set the SQL dialect for this runner (explicit override).
    ///
    /// By default, the dialect is auto-detected from the database connection.
    /// Use this method to override when needed (e.g., testing with a mock DB).
    pub fn with_dialect(mut self, dialect: Dialect) -> Self {
        self.dialect_override = Some(dialect);
        self
    }

    /// Register a `Table` type for automatic diff-based migration.
    ///
    /// - If the table does not exist → emits `CREATE TABLE IF NOT EXISTS`.
    /// - If the table exists but has new columns → emits `ALTER TABLE ADD COLUMN`.
    /// - Drops, renames, and type changes are **never** auto-generated.
    ///
    /// # Warning — additive-only diff
    ///
    /// The auto-diff is **additive only**. If you rename a Rust field, the diff
    /// will report a `Removed` column and an `Added` column but will **not**
    /// emit any SQL — the runner silently skips non-additive changes.
    ///
    /// For renames, type changes, or column drops, write a manual migration
    /// with [`MigrationRunner::add`] instead.
    pub fn add_table<T: Table>(mut self) -> Self {
        // Use rich metadata from column_defs() when available;
        // fall back to minimal defs from column_names() for plain Table impls.
        let column_defs = {
            let defs = T::column_defs();
            if defs.is_empty() {
                T::column_names()
                    .iter()
                    .map(|name| crate::schema::ColumnDef {
                        name,
                        sql_type: if *name == "id" {
                            crate::schema::SqlType::BigSerial
                        } else {
                            crate::schema::SqlType::Text
                        },
                        primary_key: *name == "id",
                        auto_increment: *name == "id",
                        unique: false,
                        index: false,
                        nullable: false,
                        default: None,
                        computed: None,
                        timestamp_kind: None,
                        timestamp_source: crate::schema::TimestampSource::Vm,
                        check: None,
                        foreign_key: None,
                    })
                    .collect()
            } else {
                defs
            }
        };

        self.tables.push(TableEntry {
            table_name: T::table_name(),
            column_names: T::column_names(),
            column_defs,
            indexes: T::indexes(),
            checks: Vec::new(),
        });
        self
    }

    /// Register a `Table` type with explicit `Schema` metadata for richer DDL.
    pub fn add_table_with_schema<T>(mut self, schema: crate::schema::TableSchema<T>) -> Self
    where
        T: Table,
    {
        self.tables.push(TableEntry {
            table_name: T::table_name(),
            column_names: T::column_names(),
            column_defs: schema.columns,
            indexes: schema.indexes,
            checks: schema.checks,
        });
        self
    }

    /// Register a `Table` type that has `#[table(audit)]` for automatic diff-based migration.
    ///
    /// Registers both the main table (via `add_table::<T>()`) and a synthetic audit companion
    /// table (`<table>_audit`) with the 6 fixed audit columns.
    pub fn add_audited_table<T: Table + crate::audit::Auditable>(mut self) -> Self {
        self = self.add_table::<T>();
        let audit_defs = T::audit_column_defs();
        let audit_name = T::audit_table_name();
        self.tables.push(TableEntry {
            table_name: audit_name,
            // NOTE: `column_names` is intentionally empty for audit tables.
            // Audit tables are managed by the fixed `audit_column_defs()` schema
            // and are never subject to ADD COLUMN auto-diff. If you extend the
            // audit schema, register a manual migration instead.
            column_names: &[],
            column_defs: audit_defs,
            indexes: Vec::new(), // Audit tables have no auto-managed indexes
            checks: Vec::new(),
        });
        self
    }

    /// Register a `Table` type with explicit `Schema` metadata, plus its audit companion table.
    ///
    /// Same as [`add_audited_table`](Self::add_audited_table) but delegates the main table
    /// registration to `add_table_with_schema(schema)` for users who define their schema via
    /// the builder API.
    pub fn add_audited_table_with_schema<T>(mut self, schema: crate::schema::TableSchema<T>) -> Self
    where
        T: Table + crate::audit::Auditable,
    {
        self = self.add_table_with_schema(schema);
        let audit_defs = T::audit_column_defs();
        let audit_name = T::audit_table_name();
        self.tables.push(TableEntry {
            table_name: audit_name,
            column_names: &[],
            column_defs: audit_defs,
            indexes: Vec::new(), // Audit tables have no auto-managed indexes
            checks: Vec::new(),
        });
        self
    }

    /// Register a `View` type for automatic materialized-view migration (PostgreSQL).
    ///
    /// Emits `CREATE MATERIALIZED VIEW IF NOT EXISTS … AS … WITH DATA` the first
    /// time the runner sees this view. Subsequent runs skip it (idempotent via the
    /// tracking table).
    ///
    /// To refresh the view on every deploy, use a manual `Migration` with
    /// `ctx.refresh_materialized_view(name, concurrently)`.
    pub fn add_materialized_view<V: crate::view::View>(mut self) -> Self {
        let query = match V::view_query() {
            crate::view::ViewQuery::Raw(s) => s,
            crate::view::ViewQuery::Typed { sql, .. } => sql,
        };
        self.mat_views.push(MatViewEntry {
            view_name: V::view_name(),
            query,
        });
        self
    }

    /// Register a manual `Migration` implementation.
    pub fn add(mut self, migration: impl Migration + 'static) -> Self {
        self.manual.push(Box::new(migration));
        self
    }

    /// Register an async hook called **before** each migration plan is executed.
    ///
    /// If the hook returns `Err`, the plan is **not** executed and `run()` returns
    /// that error immediately. Use this for pre-flight checks, logging, or
    /// sending notifications before a migration starts.
    ///
    /// # Example
    /// ```ignore
    /// runner.on_before_each(|plan| Box::pin(async move {
    ///     println!("→ applying {}", plan.version);
    ///     Ok(())
    /// }))
    /// ```
    pub fn on_before_each(
        mut self,
        f: impl for<'a> Fn(
            &'a MigrationPlan,
        ) -> BoxFuture<'a, Result<(), crate::migration::MigrationError>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.hooks.before_each = Some(Box::new(f));
        self
    }

    /// Register an async hook called **after** each migration plan succeeds.
    ///
    /// If the hook returns `Err`, `run()` propagates that error (the migration
    /// itself was already committed). Use this for post-migration notifications,
    /// cache invalidation, or audit logging.
    pub fn on_after_each(
        mut self,
        f: impl for<'a> Fn(
            &'a MigrationPlan,
        ) -> BoxFuture<'a, Result<(), crate::migration::MigrationError>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.hooks.after_each = Some(Box::new(f));
        self
    }

    /// Register an async hook called when a migration plan **fails**.
    ///
    /// The hook cannot cancel or modify the error — it is called for side-effects
    /// only (logging, alerting, metrics). The original error is always propagated.
    pub fn on_migration_error(
        mut self,
        f: impl for<'a> Fn(&'a MigrationPlan, &'a crate::migration::MigrationError) -> BoxFuture<'a, ()>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.hooks.on_error = Some(Box::new(f));
        self
    }

    /// Register a `View` type for automatic migration.
    ///
    /// Emits `CREATE OR REPLACE VIEW` when the view hasn't been applied yet.
    pub fn add_view<V: crate::view::View>(mut self) -> Self {
        let query = match V::view_query() {
            crate::view::ViewQuery::Raw(s) => s,
            crate::view::ViewQuery::Typed { sql, .. } => sql,
        };
        self.views.push(ViewEntry {
            view_name: V::view_name(),
            query,
        });
        self
    }

    /// Register a view with explicit `ViewSchema` metadata.
    pub fn add_view_with_schema<V: crate::view::View>(
        mut self,
        schema: crate::view::ViewSchema<V>,
    ) -> Self {
        let query = schema.query_sql().unwrap_or_else(|| match V::view_query() {
            crate::view::ViewQuery::Raw(s) => s,
            crate::view::ViewQuery::Typed { sql, .. } => sql,
        });
        self.views.push(ViewEntry {
            view_name: V::view_name(),
            query,
        });
        self
    }

    /// Apply pending migrations interactively, prompting for confirmation before each.
    ///
    /// Similar to `run()`, but calls `confirm(&plan)` before executing each migration.
    /// If the callback returns `false`, the migration is **not** applied and
    /// `MigrationError::UserAborted` is returned immediately — no subsequent
    /// migrations are attempted.
    ///
    /// This enables Drizzle-style `--strict` mode where users review each SQL
    /// statement before it touches the database.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::io::{self, Write};
    ///
    /// runner.run_interactive(&db, |plan| {
    ///     println!("{}", plan.display());
    ///     print!("Apply this migration? [Y/n] ");
    ///     io::stdout().flush().unwrap();
    ///
    ///     let mut input = String::new();
    ///     io::stdin().read_line(&mut input).unwrap();
    ///     let input = input.trim().to_lowercase();
    ///     input.is_empty() || input == "y" || input == "yes"
    /// }).await?;
    /// ```
    ///
    /// # Lifecycle hooks
    ///
    /// `on_before_each` / `on_after_each` / `on_migration_error` hooks are still
    /// called around each migration, just as with `run()`. The confirm callback
    /// is invoked **before** `on_before_each`.
    pub async fn run_interactive<F>(
        &self,
        db: &impl crate::db::Database,
        confirm: F,
    ) -> Result<(), crate::migration::MigrationError>
    where
        F: Fn(&MigrationPlan) -> bool + Send + Sync,
    {
        use crate::migration::lock::MigrationLock;

        let dialect = self.resolve_dialect(db);
        self.ensure_tracking_table(db, dialect).await?;
        MigrationLock::ensure(db, dialect).await?;
        MigrationLock::acquire(db, dialect).await?;

        let result = self.run_interactive_inner(db, confirm, dialect).await;

        MigrationLock::release(db, dialect).await.ok();

        result
    }
}

impl Default for MigrationRunner {
    fn default() -> Self {
        Self::new()
    }
}
