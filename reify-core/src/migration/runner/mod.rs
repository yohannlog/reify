mod apply;
mod entries;
mod hooks;
mod inspect;
mod plans;
mod queries;
mod rollback;

pub use hooks::MigrationHooks;
use entries::{MatViewEntry, TableEntry, ViewEntry};
use hooks::{MigrationErrorHookFn, MigrationHookFn};

use super::ddl::{add_column_sql, create_table_sql, create_table_sql_named, create_table_sql_with_checks};
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
    /// SQL dialect used for backend-specific DDL and DML.
    pub(super) dialect: Dialect,
    /// Lifecycle hooks — called around each plan execution.
    pub(super) hooks: MigrationHooks,
}

impl MigrationRunner {
    /// Create a new, empty runner targeting the generic (PostgreSQL-compatible) dialect.
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            views: Vec::new(),
            mat_views: Vec::new(),
            manual: Vec::new(),
            dialect: Dialect::default(),
            hooks: MigrationHooks::default(),
        }
    }

    /// Set the SQL dialect for this runner.
    ///
    /// Must be called before `run()` / `dry_run()`. Affects DDL for system
    /// tables, `CURRENT_SCHEMA()` vs `DATABASE()`, and upsert syntax.
    pub fn with_dialect(mut self, dialect: Dialect) -> Self {
        self.dialect = dialect;
        self
    }

    /// Register a `Table` type for automatic diff-based migration.
    ///
    /// - If the table does not exist → emits `CREATE TABLE IF NOT EXISTS`.
    /// - If the table exists but has new columns → emits `ALTER TABLE ADD COLUMN`.
    /// - Drops, renames, and type changes are **never** auto-generated.
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

        let create_sql = create_table_sql::<T>(&column_defs, Dialect::Generic);

        self.tables.push(TableEntry {
            table_name: T::table_name(),
            column_names: T::column_names(),
            column_defs,
            create_sql,
        });
        self
    }

    /// Register a `Table` type with explicit `Schema` metadata for richer DDL.
    pub fn add_table_with_schema<T>(mut self, schema: crate::schema::TableSchema<T>) -> Self
    where
        T: Table,
    {
        let create_sql = create_table_sql_with_checks::<T>(
            &schema.columns,
            &schema.checks,
            Dialect::Generic,
        );
        self.tables.push(TableEntry {
            table_name: T::table_name(),
            column_names: T::column_names(),
            column_defs: schema.columns,
            create_sql,
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
        let create_sql = create_table_sql_named(audit_name, &audit_defs, Dialect::Generic);
        self.tables.push(TableEntry {
            table_name: audit_name,
            // NOTE: `column_names` is intentionally empty for audit tables.
            // Audit tables are managed by the fixed `audit_column_defs()` schema
            // and are never subject to ADD COLUMN auto-diff. If you extend the
            // audit schema, register a manual migration instead.
            column_names: &[],
            column_defs: audit_defs,
            create_sql,
        });
        self
    }

    /// Register a `Table` type with explicit `Schema` metadata, plus its audit companion table.
    ///
    /// Same as [`add_audited_table`](Self::add_audited_table) but delegates the main table
    /// registration to `add_table_with_schema(schema)` for users who define their schema via
    /// the builder API.
    pub fn add_audited_table_with_schema<T>(
        mut self,
        schema: crate::schema::TableSchema<T>,
    ) -> Self
    where
        T: Table + crate::audit::Auditable,
    {
        self = self.add_table_with_schema(schema);
        let audit_defs = T::audit_column_defs();
        let audit_name = T::audit_table_name();
        let create_sql = create_table_sql_named(audit_name, &audit_defs, Dialect::Generic);
        self.tables.push(TableEntry {
            table_name: audit_name,
            column_names: &[],
            column_defs: audit_defs,
            create_sql,
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
        f: impl for<'a> Fn(&'a MigrationPlan) -> BoxFuture<'a, Result<(), crate::migration::MigrationError>>
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
        f: impl for<'a> Fn(&'a MigrationPlan) -> BoxFuture<'a, Result<(), crate::migration::MigrationError>>
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
}

impl Default for MigrationRunner {
    fn default() -> Self {
        Self::new()
    }
}
