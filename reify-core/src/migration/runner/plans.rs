use super::MigrationRunner;
use crate::db::Database;
use crate::migration::context::MigrationContext;
use crate::migration::ddl::add_column_sql;
use crate::migration::error::MigrationError;
use crate::migration::plan::{MigrationPlan, compute_checksum};
use std::collections::HashSet;

impl MigrationRunner {
    /// Build the list of auto-diff plans (CREATE TABLE / ADD COLUMN).
    pub(super) async fn auto_diff_plans(
        &self,
        db: &impl Database,
        applied: &HashSet<String>,
    ) -> Result<Vec<MigrationPlan>, MigrationError> {
        let mut plans = Vec::new();

        for entry in &self.tables {
            let version = format!("auto__{}", entry.table_name);

            // Skip if already recorded as applied
            if applied.contains(&version) {
                // Still check for new columns even if the table was created before.
                // Each distinct set of new columns gets its own versioned plan so
                // successive ADD COLUMN waves don't collide on the same version key.
                let existing = self.existing_columns(db, entry.table_name).await?;
                if let Some(existing_cols) = existing {
                    let mut new_cols: Vec<&str> = entry
                        .column_names
                        .iter()
                        .copied()
                        .filter(|col| !existing_cols.iter().any(|c| c == col))
                        .collect();
                    if !new_cols.is_empty() {
                        // Sort for a stable, deterministic version key.
                        new_cols.sort_unstable();
                        let cols_key = new_cols.join("_");
                        let add_version =
                            format!("auto__{}_add_{}", entry.table_name, cols_key);
                        if !applied.contains(&add_version) {
                            let stmts: Vec<String> = new_cols
                                .iter()
                                .map(|col| {
                                    let def =
                                        entry.column_defs.iter().find(|d| d.name == *col);
                                    add_column_sql(
                                        entry.table_name,
                                        col,
                                        def,
                                        crate::query::Dialect::Generic,
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
                    let stmts = vec![entry.create_sql.clone()];
                    let checksum = compute_checksum(&stmts);
                    plans.push(MigrationPlan {
                        version,
                        description: format!("Create table {}", entry.table_name),
                        comment: None,
                        statements: stmts,
                        checksum,
                        schema_diff: None,
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
                        let checksum = compute_checksum(&stmts);
                        plans.push(MigrationPlan {
                            version,
                            description: format!("Add new columns to {}", entry.table_name),
                            comment: None,
                            statements: stmts,
                            checksum,
                            schema_diff: None,
                        });
                    }
                }
            }
        }

        Ok(plans)
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
                }
            })
            .collect()
    }
}
