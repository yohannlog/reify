use super::context::MigrationContext;
use super::ddl::{add_column_sql, create_table_sql, create_table_sql_with_checks};
use super::diff::{ColumnDiff, DbColumnInfo, SchemaDiff, TableDiff, normalize_sql_type};
use super::error::MigrationError;
use super::plan::{MigrationPlan, MigrationStatus};
use super::traits::Migration;
use crate::db::{Database, DbError};
use crate::schema::Schema;
use crate::table::Table;
use crate::value::Value;
use std::collections::HashSet;

const TRACKING_TABLE: &str = "\"_reify_migrations\"";

const CREATE_TRACKING_TABLE: &str = "
CREATE TABLE IF NOT EXISTS \"_reify_migrations\" (
    \"version\"     TEXT        NOT NULL PRIMARY KEY,
    \"description\" TEXT        NOT NULL,
    \"applied_at\"  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);";

/// An entry registered via `MigrationRunner::add_table::<T>()`.
struct TableEntry {
    table_name: &'static str,
    column_names: &'static [&'static str],
    column_defs: Vec<crate::schema::ColumnDef>,
    /// Pre-built CREATE TABLE SQL.
    create_sql: String,
}

// ── ViewEntry — auto-diff entry for views ───────────────────────────

/// An entry registered via `MigrationRunner::add_view::<V>()`.
struct ViewEntry {
    view_name: &'static str,
    /// The SELECT query that defines this view.
    query: String,
}

// ── MigrationRunner ──────────────────────────────────────────────────

/// Orchestrates automatic diff-based migrations and manual `Migration` impls.
///
/// # Usage
///
/// ```ignore
/// MigrationRunner::new()
///     .add_table::<User>()      // auto CREATE TABLE / ADD COLUMN
///     .add_table::<Post>()
///     .add_view::<ActiveUser>() // auto CREATE OR REPLACE VIEW
///     .add(SplitAddress)        // manual migration
///     .run(&db)
///     .await?;
/// ```
pub struct MigrationRunner {
    tables: Vec<TableEntry>,
    views: Vec<ViewEntry>,
    manual: Vec<Box<dyn Migration>>,
}

impl MigrationRunner {
    /// Create a new, empty runner.
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            views: Vec::new(),
            manual: Vec::new(),
        }
    }

    /// Register a `Table + Schema` type for automatic diff-based migration.
    ///
    /// Reads the full schema from `T::schema()` — column types, constraints,
    /// indexes, and table-level CHECK expressions are all sourced from there.
    ///
    /// - If the table does not exist → emits `CREATE TABLE IF NOT EXISTS`.
    /// - If the table exists but has new columns → emits `ALTER TABLE ADD COLUMN`.
    /// - Drops, renames, and type changes are **never** auto-generated.
    pub fn add_table<T: Schema>(mut self) -> Self {
        let schema = T::schema();
        let create_sql = create_table_sql_with_checks(
            schema.name,
            &schema.columns,
            &schema.checks,
            crate::query::Dialect::Generic,
        );
        self.tables.push(TableEntry {
            table_name: T::table_name(),
            column_names: T::column_names(),
            column_defs: schema.columns,
            create_sql,
        });
        self
    }

    /// Register a `Schema + Auditable` type for automatic diff-based migration.
    ///
    /// Registers both the main table (via `add_table::<T>()`) and a synthetic
    /// audit companion table (`<table>_audit`) with the 5 fixed audit columns.
    pub fn add_audited_table<T: Schema + crate::audit::Auditable>(mut self) -> Self {
        self = self.add_table::<T>();
        let audit_defs = T::audit_column_defs();
        let audit_name = T::audit_table_name();
        let create_sql = create_table_sql(audit_name, &audit_defs, crate::query::Dialect::Generic);
        self.tables.push(TableEntry {
            table_name: audit_name,
            column_names: &[],
            column_defs: audit_defs,
            create_sql,
        });
        self
    }

    /// Register a manual `Migration` implementation.
    pub fn add(mut self, migration: impl Migration + 'static) -> Self {
        self.manual.push(Box::new(migration));
        self
    }

    // ── Internal helpers ─────────────────────────────────────────────

    /// Ensure the tracking table exists.
    async fn ensure_tracking_table(&self, db: &impl Database) -> Result<(), MigrationError> {
        db.execute(CREATE_TRACKING_TABLE.trim(), &[]).await?;
        Ok(())
    }

    /// Fetch the set of already-applied migration versions.
    async fn applied_versions(
        &self,
        db: &impl Database,
    ) -> Result<std::collections::HashSet<String>, MigrationError> {
        let rows = db
            .query(&format!("SELECT \"version\" FROM {TRACKING_TABLE}"), &[])
            .await?;
        let versions = rows
            .into_iter()
            .filter_map(|r| {
                r.get("version").and_then(|v| {
                    if let Value::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
            })
            .collect();
        Ok(versions)
    }

    /// Record a migration as applied.
    async fn mark_applied(
        &self,
        db: &impl Database,
        version: &str,
        description: &str,
    ) -> Result<(), MigrationError> {
        db.execute(
            &format!("INSERT INTO {TRACKING_TABLE} (\"version\", \"description\") VALUES (?, ?);"),
            &[
                Value::String(version.into()),
                Value::String(description.into()),
            ],
        )
        .await?;
        Ok(())
    }

    /// Remove a migration record (rollback).
    async fn mark_reverted(&self, db: &impl Database, version: &str) -> Result<(), MigrationError> {
        db.execute(
            &format!("DELETE FROM {TRACKING_TABLE} WHERE \"version\" = ?;"),
            &[Value::String(version.into())],
        )
        .await?;
        Ok(())
    }

    /// Fetch detailed column metadata for a table from `information_schema`.
    ///
    /// Returns `None` when the table does not exist in the database.
    /// Also queries `information_schema.table_constraints` and
    /// `information_schema.key_column_usage` to determine which columns carry a
    /// UNIQUE constraint.
    pub async fn existing_column_details(
        &self,
        db: &impl Database,
        table: &str,
    ) -> Result<Option<Vec<DbColumnInfo>>, MigrationError> {
        // ── 1. Fetch column metadata ──────────────────────────────────
        let col_rows = db
            .query(
                "SELECT column_name, data_type, is_nullable, column_default \
                 FROM information_schema.columns \
                 WHERE table_name = ? \
                 ORDER BY ordinal_position;",
                &[Value::String(table.into())],
            )
            .await;

        let col_rows = match col_rows {
            Ok(rows) if rows.is_empty() => return Ok(None), // table absent
            Ok(rows) => rows,
            Err(_) => return Ok(None), // treat errors as "table absent"
        };

        // ── 2. Fetch unique-constrained column names ──────────────────
        let unique_rows = db
            .query(
                "SELECT kcu.column_name \
                 FROM information_schema.table_constraints tc \
                 JOIN information_schema.key_column_usage kcu \
                   ON tc.constraint_name = kcu.constraint_name \
                  AND tc.table_name      = kcu.table_name \
                 WHERE tc.table_name      = ? \
                   AND tc.constraint_type = 'UNIQUE';",
                &[Value::String(table.into())],
            )
            .await
            .unwrap_or_default();

        let unique_cols: std::collections::HashSet<String> = unique_rows
            .into_iter()
            .filter_map(|r| {
                r.get("column_name").and_then(|v| {
                    if let Value::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
            })
            .collect();

        // ── 3. Build DbColumnInfo list ────────────────────────────────
        let infos = col_rows
            .into_iter()
            .filter_map(|r| {
                let name = r.get("column_name").and_then(|v| {
                    if let Value::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                })?;
                let data_type = r.get("data_type").and_then(|v| {
                    if let Value::String(s) = v {
                        Some(normalize_sql_type(s))
                    } else {
                        None
                    }
                })?;
                let is_nullable = r
                    .get("is_nullable")
                    .and_then(|v| {
                        if let Value::String(s) = v {
                            Some(s.eq_ignore_ascii_case("yes"))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(false);
                let column_default = r.get("column_default").and_then(|v| match v {
                    Value::String(s) => Some(s.clone()),
                    _ => None,
                });
                let is_unique = unique_cols.contains(&name);
                Some(DbColumnInfo {
                    name,
                    data_type,
                    is_nullable,
                    column_default,
                    is_unique,
                })
            })
            .collect();

        Ok(Some(infos))
    }

    /// Fetch existing column names for a table from the DB (information_schema).
    async fn existing_columns(
        &self,
        db: &impl Database,
        table: &str,
    ) -> Result<Option<Vec<String>>, MigrationError> {
        // Try information_schema first (works on PostgreSQL, MySQL, MariaDB).
        // Returns None if the table doesn't exist yet.
        let rows = db
            .query(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_name = ? ORDER BY ordinal_position;",
                &[Value::String(table.into())],
            )
            .await;

        match rows {
            Ok(rows) if rows.is_empty() => Ok(None), // table absent
            Ok(rows) => {
                let cols = rows
                    .into_iter()
                    .filter_map(|r| {
                        r.get("column_name").and_then(|v| {
                            if let Value::String(s) = v {
                                Some(s.clone())
                            } else {
                                None
                            }
                        })
                    })
                    .collect();
                Ok(Some(cols))
            }
            Err(e) => Err(MigrationError::Db(e)),
        }
    }

    /// Build the list of auto-diff plans (CREATE TABLE / ADD COLUMN).
    async fn auto_diff_plans(
        &self,
        db: &impl Database,
        applied: &std::collections::HashSet<String>,
    ) -> Result<Vec<MigrationPlan>, MigrationError> {
        let mut plans = Vec::new();

        for entry in &self.tables {
            let version = format!("auto__{}", entry.table_name);

            // Skip if already recorded as applied
            if applied.contains(&version) {
                // Still check for new columns even if the table was created before
                let existing = self.existing_columns(db, entry.table_name).await?;
                if let Some(existing_cols) = existing {
                    let mut stmts = Vec::new();
                    for col in entry.column_names {
                        if !existing_cols.iter().any(|c| c == col) {
                            let def = entry.column_defs.iter().find(|d| d.name == *col);
                            stmts.push(add_column_sql(
                                entry.table_name,
                                col,
                                def,
                                crate::query::Dialect::Generic,
                            ));
                        }
                    }
                    if !stmts.is_empty() {
                        let add_version = format!("auto__{}_add_columns", entry.table_name);
                        if !applied.contains(&add_version) {
                            plans.push(MigrationPlan {
                                version: add_version,
                                description: format!("Add new columns to {}", entry.table_name),
                                statements: stmts,
                                is_up: true,
                            });
                        }
                    }
                }
                continue;
            }

            let existing = self.existing_columns(db, entry.table_name).await?;
            match existing {
                None => {
                    // Table doesn't exist → CREATE TABLE
                    plans.push(MigrationPlan {
                        version,
                        description: format!("Create table {}", entry.table_name),
                        statements: vec![entry.create_sql.clone()],
                        is_up: true,
                    });
                }
                Some(existing_cols) => {
                    // Table exists → ADD COLUMN for new fields
                    let mut stmts = Vec::new();
                    for col in entry.column_names {
                        if !existing_cols.iter().any(|c| c == col) {
                            let def = entry.column_defs.iter().find(|d| d.name == *col);
                            stmts.push(add_column_sql(
                                entry.table_name,
                                col,
                                def,
                                crate::query::Dialect::Generic,
                            ));
                        }
                    }
                    if !stmts.is_empty() {
                        plans.push(MigrationPlan {
                            version,
                            description: format!("Add new columns to {}", entry.table_name),
                            statements: stmts,
                            is_up: true,
                        });
                    }
                }
            }
        }

        Ok(plans)
    }

    /// Build the list of view plans (CREATE OR REPLACE VIEW).
    fn view_plans(&self, applied: &std::collections::HashSet<String>) -> Vec<MigrationPlan> {
        let mut plans = Vec::new();

        for entry in &self.views {
            let version = format!("auto_view__{}", entry.view_name);

            // Views use CREATE OR REPLACE, so we always emit the statement
            // to keep the view definition in sync. But we only track it once.
            if applied.contains(&version) {
                continue;
            }

            plans.push(MigrationPlan {
                version,
                description: format!("Create view {}", entry.view_name),
                statements: vec![crate::view::create_view_sql(entry.view_name, &entry.query)],
                is_up: true,
            });
        }

        plans
    }

    /// Build plans for pending manual migrations.
    fn manual_plans(
        &self,
        applied: &std::collections::HashSet<String>,
    ) -> Vec<(MigrationPlan, &dyn Migration)> {
        self.manual
            .iter()
            .filter(|m| !applied.contains(m.version()))
            .map(|m| {
                let mut ctx = MigrationContext::new();
                m.up(&mut ctx);
                let plan = MigrationPlan {
                    version: m.version().to_string(),
                    description: m.description().to_string(),
                    statements: ctx.statements,
                    is_up: true,
                };
                (plan, m.as_ref())
            })
            .collect()
    }

    // ── Public API ───────────────────────────────────────────────────

    /// Compare all registered `Table` types against the live database schema
    /// and return a [`SchemaDiff`] describing every difference found.
    ///
    /// Unlike [`Self::dry_run`], this method does **not** consult the migration
    /// tracking table — it performs a direct structural comparison between the
    /// Rust struct definitions and `information_schema`.
    ///
    /// # What is detected
    ///
    /// | Situation | Reported as |
    /// |---|---|
    /// | Table absent from DB | `TableDiff { is_new_table: true }` with all columns as `Added` |
    /// | Column in struct, absent from DB | `ColumnDiff::Added` |
    /// | Column in DB, absent from struct | `ColumnDiff::Removed` |
    /// | SQL type mismatch | `ColumnDiff::TypeChanged` |
    /// | Nullability mismatch | `ColumnDiff::NullableChanged` |
    /// | UNIQUE constraint mismatch | `ColumnDiff::UniqueChanged` |
    /// | Default value mismatch | `ColumnDiff::DefaultChanged` |
    pub async fn diff(&self, db: &impl Database) -> Result<SchemaDiff, MigrationError> {
        let mut table_diffs = Vec::new();

        for entry in &self.tables {
            let db_cols = self.existing_column_details(db, entry.table_name).await?;

            let table_diff = match db_cols {
                // ── Table does not exist yet ──────────────────────
                None => TableDiff {
                    table_name: entry.table_name.to_string(),
                    is_new_table: true,
                    column_diffs: entry
                        .column_names
                        .iter()
                        .map(|col| ColumnDiff::Added {
                            column: col.to_string(),
                        })
                        .collect(),
                },

                // ── Table exists — compare column by column ───────
                Some(db_cols) => {
                    let mut diffs = Vec::new();

                    // Struct columns vs DB
                    for col_name in entry.column_names {
                        match db_cols.iter().find(|c| c.name == *col_name) {
                            None => {
                                // Column present in struct but absent from DB
                                diffs.push(ColumnDiff::Added {
                                    column: col_name.to_string(),
                                });
                            }
                            Some(db_col) => {
                                let def = entry.column_defs.iter().find(|d| d.name == *col_name);

                                // Type check: use metadata sql_type, falling back to TEXT.
                                let raw_type = def
                                    .map(|d| d.sql_type.to_sql(crate::query::Dialect::Generic))
                                    .unwrap_or(std::borrow::Cow::Borrowed("TEXT"));
                                let struct_type = normalize_sql_type(&raw_type);
                                if struct_type != db_col.data_type {
                                    diffs.push(ColumnDiff::TypeChanged {
                                        column: col_name.to_string(),
                                        from: db_col.data_type.clone(),
                                        to: struct_type,
                                    });
                                }

                                // Nullability
                                let struct_nullable = def.map(|d| d.nullable).unwrap_or(false);
                                if struct_nullable != db_col.is_nullable {
                                    diffs.push(ColumnDiff::NullableChanged {
                                        column: col_name.to_string(),
                                        from: db_col.is_nullable,
                                        to: struct_nullable,
                                    });
                                }

                                // Uniqueness
                                let struct_unique = def.map(|d| d.unique).unwrap_or(false);
                                if struct_unique != db_col.is_unique {
                                    diffs.push(ColumnDiff::UniqueChanged {
                                        column: col_name.to_string(),
                                        from: db_col.is_unique,
                                        to: struct_unique,
                                    });
                                }

                                // Default
                                let struct_default =
                                    def.and_then(|d| d.default.as_deref().map(str::to_string));
                                if struct_default != db_col.column_default {
                                    diffs.push(ColumnDiff::DefaultChanged {
                                        column: col_name.to_string(),
                                        from: db_col.column_default.clone(),
                                        to: struct_default,
                                    });
                                }
                            }
                        }
                    }

                    // DB columns absent from struct → Removed
                    for db_col in &db_cols {
                        if !entry.column_names.iter().any(|n| *n == db_col.name) {
                            diffs.push(ColumnDiff::Removed {
                                column: db_col.name.clone(),
                            });
                        }
                    }

                    TableDiff {
                        table_name: entry.table_name.to_string(),
                        is_new_table: false,
                        column_diffs: diffs,
                    }
                }
            };

            if !table_diff.is_empty() {
                table_diffs.push(table_diff);
            }
        }

        Ok(SchemaDiff {
            tables: table_diffs,
        })
    }

    /// Apply all pending migrations (auto-diff + manual + views) against the database.
    ///
    /// Creates the `_reify_migrations` tracking table if it doesn't exist.
    pub async fn run(&self, db: &impl Database) -> Result<(), MigrationError> {
        self.ensure_tracking_table(db).await?;
        let applied = self.applied_versions(db).await?;

        // Auto-diff plans (tables)
        let auto_plans = self.auto_diff_plans(db, &applied).await?;
        for plan in &auto_plans {
            for stmt in &plan.statements {
                db.execute(stmt, &[]).await?;
            }
            self.mark_applied(db, &plan.version, &plan.description)
                .await?;
        }

        // View plans (after tables, since views may reference them)
        let view_plans = self.view_plans(&applied);
        for plan in &view_plans {
            for stmt in &plan.statements {
                db.execute(stmt, &[]).await?;
            }
            self.mark_applied(db, &plan.version, &plan.description)
                .await?;
        }

        // Manual migrations
        let manual_plans = self.manual_plans(&applied);
        for (plan, migration) in &manual_plans {
            // Re-run up() to get a fresh context for actual execution
            let mut ctx = MigrationContext::new();
            migration.up(&mut ctx);
            for stmt in &ctx.statements {
                db.execute(stmt, &[]).await?;
            }
            self.mark_applied(db, &plan.version, &plan.description)
                .await?;
        }

        Ok(())
    }

    /// Preview all pending migrations without applying them.
    ///
    /// Returns a `Vec<MigrationPlan>` describing what SQL would be executed.
    pub async fn dry_run(&self, db: &impl Database) -> Result<Vec<MigrationPlan>, MigrationError> {
        self.ensure_tracking_table(db).await?;
        let applied = self.applied_versions(db).await?;

        let mut plans = self.auto_diff_plans(db, &applied).await?;

        // View plans
        plans.extend(self.view_plans(&applied));

        for (plan, _) in self.manual_plans(&applied) {
            plans.push(plan);
        }

        Ok(plans)
    }

    /// Roll back the last applied migration.
    ///
    /// Returns `MigrationError::NotReversible` if the migration declared
    /// `is_reversible() = false`.
    pub async fn rollback(&self, db: &impl Database) -> Result<(), MigrationError> {
        self.ensure_tracking_table(db).await?;

        // Find the most recently applied manual migration
        let rows = db
            .query(
                &format!(
                    "SELECT \"version\" FROM {TRACKING_TABLE} \
                     WHERE \"version\" NOT LIKE 'auto__%' \
                     ORDER BY \"applied_at\" DESC LIMIT 1;"
                ),
                &[],
            )
            .await?;

        let last_version = rows.first().and_then(|r| r.get("version")).and_then(|v| {
            if let Value::String(s) = v {
                Some(s.clone())
            } else {
                None
            }
        });

        let version = match last_version {
            Some(v) => v,
            None => {
                return Err(MigrationError::Other(
                    "no applied migrations to roll back".into(),
                ));
            }
        };

        // Find the matching Migration impl
        let migration = self
            .manual
            .iter()
            .find(|m| m.version() == version)
            .ok_or_else(|| {
                MigrationError::Other(format!(
                    "migration '{version}' is applied but not registered in this runner"
                ))
            })?;

        if !migration.is_reversible() {
            return Err(MigrationError::NotReversible(version));
        }

        let mut ctx = MigrationContext::new();
        migration.down(&mut ctx);
        for stmt in &ctx.statements {
            db.execute(stmt, &[]).await?;
        }

        self.mark_reverted(db, &version).await?;
        Ok(())
    }

    /// Roll back all applied migrations up to (and including) `target_version`.
    pub async fn rollback_to(
        &self,
        db: &impl Database,
        target_version: &str,
    ) -> Result<(), MigrationError> {
        self.ensure_tracking_table(db).await?;

        let rows = db
            .query(
                &format!(
                    "SELECT \"version\" FROM {TRACKING_TABLE} \
                     WHERE \"version\" NOT LIKE 'auto__%' \
                     ORDER BY \"applied_at\" DESC;"
                ),
                &[],
            )
            .await?;

        let versions: Vec<String> = rows
            .into_iter()
            .filter_map(|r| {
                r.get("version").and_then(|v| {
                    if let Value::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
            })
            .collect();

        // Roll back from newest to target (inclusive)
        for version in &versions {
            let migration = self
                .manual
                .iter()
                .find(|m| m.version() == version)
                .ok_or_else(|| {
                    MigrationError::Other(format!(
                        "migration '{version}' is applied but not registered"
                    ))
                })?;

            if !migration.is_reversible() {
                return Err(MigrationError::NotReversible(version.clone()));
            }

            let mut ctx = MigrationContext::new();
            migration.down(&mut ctx);
            for stmt in &ctx.statements {
                db.execute(stmt, &[]).await?;
            }
            self.mark_reverted(db, version).await?;

            if version == target_version {
                break;
            }
        }

        Ok(())
    }

    /// Return the status of all registered migrations.
    pub async fn status(&self, db: &impl Database) -> Result<Vec<MigrationStatus>, MigrationError> {
        self.ensure_tracking_table(db).await?;
        let applied = self.applied_versions(db).await?;

        let mut statuses = Vec::new();

        // Auto-diff entries
        for entry in &self.tables {
            let version = format!("auto__{}", entry.table_name);
            statuses.push(MigrationStatus {
                version: version.clone(),
                description: format!("Auto-manage table {}", entry.table_name),
                applied: applied.contains(&version),
                is_auto: true,
            });
        }

        // View entries
        for entry in &self.views {
            let version = format!("auto_view__{}", entry.view_name);
            statuses.push(MigrationStatus {
                version: version.clone(),
                description: format!("Auto-manage view {}", entry.view_name),
                applied: applied.contains(&version),
                is_auto: true,
            });
        }

        // Manual migrations
        for m in &self.manual {
            statuses.push(MigrationStatus {
                version: m.version().to_string(),
                description: m.description().to_string(),
                applied: applied.contains(m.version()),
                is_auto: false,
            });
        }

        Ok(statuses)
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
