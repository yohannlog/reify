use super::MigrationRunner;
use crate::db::Database;
use crate::migration::context::MigrationContext;
use crate::migration::ddl::{add_column_sql, create_index_sql, create_table_sql_with_checks};
use crate::migration::diff::ColumnDiff;
use crate::migration::error::MigrationError;
use crate::migration::plan::{MigrationPlan, compute_checksum};
use crate::query::Dialect;
use crate::table::Table;
use std::collections::HashSet;

impl MigrationRunner {
    /// Build the list of auto-diff plans (CREATE TABLE / ADD COLUMN).
    pub(super) async fn auto_diff_plans(
        &self,
        db: &impl Database,
        applied: &HashSet<String>,
        dialect: Dialect,
    ) -> Result<Vec<MigrationPlan>, MigrationError> {
        let mut plans = Vec::new();

        for entry in &self.tables {
            let version = format!("auto__{}", entry.table_name);

            // Skip if already recorded as applied
            if applied.contains(&version) {
                // Still check for new columns even if the table was created before.
                // Each distinct set of new columns gets its own versioned plan so
                // successive ADD COLUMN waves don't collide on the same version key.
                let existing = self.existing_columns(db, entry.table_name, dialect).await?;
                if let Some(existing_cols) = existing {
                    let mut new_cols: Vec<&str> = entry
                        .column_names
                        .iter()
                        .copied()
                        .filter(|col| !existing_cols.iter().any(|c| c == col))
                        .collect();
                    if !new_cols.is_empty() {
                        // Sort for a stable, deterministic version key.
                        // Use comma as separator — underscores in column names would
                        // corrupt an underscore-joined key and break table-name extraction.
                        new_cols.sort_unstable();
                        let cols_key = new_cols.join(",");
                        let add_version = format!("auto__{}_add_{}", entry.table_name, cols_key);
                        if !applied.contains(&add_version) {
                            let stmts: Vec<String> = new_cols
                                .iter()
                                .map(|col| {
                                    let def = entry.column_defs.iter().find(|d| d.name == *col);
                                    add_column_sql(
                                        entry.table_name,
                                        col,
                                        def,
                                        dialect,
                                    )
                                })
                                .collect();
                            let checksum = compute_checksum(&stmts);
                            plans.push(MigrationPlan {
                                version: add_version,
                                description: format!(
                                    "Add columns ({}) to {}",
                                    new_cols.join(", "),
                                    entry.table_name
                                ),
                                comment: None,
                                statements: stmts,
                                checksum,
                                schema_diff: None,
                                timeout: None,
                            });
                        }
                    }
                }
                continue;
            }

            // Fetch column details once — reuse for both ADD COLUMN logic and warnings.
            let details = self.existing_column_details_with_dialect(db, entry.table_name, dialect).await?;
            match details {
                None => {
                    // Table doesn't exist → CREATE TABLE + CREATE INDEX for all indexes
                    // Generate CREATE TABLE SQL with the resolved dialect
                    let create_sql = crate::migration::ddl::create_table_sql_named_with_checks(
                        entry.table_name,
                        &entry.column_defs,
                        &entry.checks,
                        dialect,
                    );
                    let mut stmts = vec![create_sql];
                    for idx in &entry.indexes {
                        stmts.push(create_index_sql(entry.table_name, idx, dialect));
                    }
                    let checksum = compute_checksum(&stmts);
                    plans.push(MigrationPlan {
                        version,
                        description: format!("Create table {}", entry.table_name),
                        comment: None,
                        statements: stmts,
                        checksum,
                        schema_diff: None,
                        timeout: None,
                    });
                }
                Some(ref db_cols) => {
                    // Table exists → ADD COLUMN for new fields
                    let existing_col_names: Vec<&str> =
                        db_cols.iter().map(|c| c.name.as_str()).collect();
                    let mut stmts = Vec::new();
                    for col in entry.column_names {
                        if !existing_col_names.iter().any(|c| *c == *col) {
                            let def = entry.column_defs.iter().find(|d| d.name == *col);
                            stmts.push(add_column_sql(
                                entry.table_name,
                                col,
                                def,
                                dialect,
                            ));
                        }
                    }

                    // Check for missing indexes
                    let existing_indexes = self.existing_indexes(db, entry.table_name, dialect).await?;
                    for idx in &entry.indexes {
                        let idx_name = idx.name.clone().unwrap_or_else(|| {
                            let col_names: Vec<&str> =
                                idx.columns.iter().map(|c| c.name).collect();
                            let prefix = if idx.unique { "uidx" } else { "idx" };
                            format!("{}_{}", prefix, col_names.join("_"))
                        });
                        if !existing_indexes.iter().any(|n| n == &idx_name) {
                            stmts.push(create_index_sql(entry.table_name, idx, dialect));
                        }
                    }

                    if !stmts.is_empty() {
                        let checksum = compute_checksum(&stmts);
                        plans.push(MigrationPlan {
                            version,
                            description: format!("Add new columns/indexes to {}", entry.table_name),
                            comment: None,
                            statements: stmts,
                            checksum,
                            schema_diff: None,
                            timeout: None,
                        });
                    }

                    // Warn about non-additive diffs that auto-migration cannot handle.
                    // These require manual migrations — log them so users know.
                    Self::warn_non_additive_diffs(entry, db_cols, dialect);
                }
            }
        }

        Ok(plans)
    }

    /// Log warnings for schema diffs that auto-migration cannot handle.
    fn warn_non_additive_diffs(
        entry: &super::entries::TableEntry,
        db_cols: &[crate::migration::diff::DbColumnInfo],
        dialect: Dialect,
    ) {
        use crate::migration::diff::ColumnDiff;

        for col_name in entry.column_names {
            if let Some(db_col) = db_cols.iter().find(|c| c.name == *col_name) {
                let def = entry.column_defs.iter().find(|d| d.name == *col_name);

                // Type mismatch
                let struct_type = def
                    .map(|d| {
                        crate::migration::diff::normalize_sql_type(&d.sql_type.to_sql(dialect))
                    })
                    .unwrap_or_else(|| "text".to_string());
                if struct_type != db_col.data_type {
                    let diff = ColumnDiff::TypeChanged {
                        column: col_name.to_string(),
                        from: db_col.data_type.clone(),
                        to: struct_type,
                    };
                    tracing::warn!(table = entry.table_name, "{}", diff.display());
                }

                // Nullability mismatch
                let struct_nullable = def.map(|d| d.nullable).unwrap_or(false);
                if struct_nullable != db_col.is_nullable {
                    let diff = ColumnDiff::NullableChanged {
                        column: col_name.to_string(),
                        from: db_col.is_nullable,
                        to: struct_nullable,
                    };
                    tracing::warn!(table = entry.table_name, "{}", diff.display());
                }

                // Unique mismatch
                let struct_unique = def.map(|d| d.unique).unwrap_or(false);
                if struct_unique != db_col.is_unique {
                    let diff = ColumnDiff::UniqueChanged {
                        column: col_name.to_string(),
                        from: db_col.is_unique,
                        to: struct_unique,
                    };
                    tracing::warn!(table = entry.table_name, "{}", diff.display());
                }
            }
        }

        // Columns in DB but not in struct → Removed
        for db_col in db_cols {
            if !entry.column_names.iter().any(|n| *n == db_col.name) {
                let diff = ColumnDiff::Removed {
                    column: db_col.name.clone(),
                };
                tracing::warn!(table = entry.table_name, "{}", diff.display());
            }
        }
    }


    /// Build the list of view plans (CREATE OR REPLACE VIEW).
    pub(super) fn view_plans(&self, applied: &HashSet<String>) -> Vec<MigrationPlan> {
        let mut plans = Vec::new();

        for entry in &self.views {
            let version = format!("auto_view__{}", entry.view_name);

            // Views use CREATE OR REPLACE, so we always emit the statement
            // to keep the view definition in sync. But we only track it once.
            if applied.contains(&version) {
                continue;
            }

            let stmts = vec![crate::view::create_view_sql(entry.view_name, &entry.query)];
            let checksum = compute_checksum(&stmts);
            plans.push(MigrationPlan {
                version,
                description: format!("Create view {}", entry.view_name),
                comment: None,
                statements: stmts,
                checksum,
                schema_diff: None,
                timeout: None,
            });
        }

        plans
    }

    /// Build the list of materialized-view plans (`CREATE MATERIALIZED VIEW IF NOT EXISTS`).
    pub(super) fn mat_view_plans(&self, applied: &HashSet<String>) -> Vec<MigrationPlan> {
        let mut plans = Vec::new();

        for entry in &self.mat_views {
            let version = format!("auto_matview__{}", entry.view_name);

            // Materialized views are created once and tracked. Refreshes must
            // be done via explicit manual migrations.
            if applied.contains(&version) {
                continue;
            }

            let stmts = vec![crate::view::create_materialized_view_sql(
                entry.view_name,
                &entry.query,
                true,
            )];
            let checksum = compute_checksum(&stmts);
            plans.push(MigrationPlan {
                version,
                description: format!("Create materialized view {}", entry.view_name),
                comment: None,
                statements: stmts,
                checksum,
                schema_diff: None,
                timeout: None,
            });
        }

        plans
    }

    /// Build plans for pending manual migrations.
    ///
    /// `up()` is called exactly once here; the resulting statements are stored
    /// in the plan and reused by both `dry_run` and `run` — no double invocation.
    pub(super) fn manual_plans(&self, applied: &HashSet<String>) -> Vec<MigrationPlan> {
        self.manual
            .iter()
            .filter(|m| !applied.contains(m.version()))
            .map(|m| {
                let mut ctx = MigrationContext::new();
                m.up(&mut ctx);
                let statements = ctx.into_statements();
                let checksum = compute_checksum(&statements);
                MigrationPlan {
                    version: m.version().to_string(),
                    description: m.description().to_string(),
                    comment: m.comment().map(str::to_string),
                    statements,
                    checksum,
                    schema_diff: None,
                    timeout: m.timeout(),
                }
            })
            .collect()
    }
}
