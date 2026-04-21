use super::MigrationRunner;
use super::entries::TRACKING_TABLE;
use crate::db::Database;
use crate::migration::diff::{ColumnDiff, SchemaDiff, TableDiff, normalize_sql_type};
use crate::migration::error::MigrationError;
use crate::migration::plan::{MigrationPlan, MigrationStatus};
use std::collections::HashSet;

impl MigrationRunner {
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
                                let struct_default = def.and_then(|d| {
                                    d.default.as_ref().map(|dv| dv.as_sql().to_string())
                                });
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

    /// Return the status of all registered migrations.
    pub async fn status(&self, db: &impl Database) -> Result<Vec<MigrationStatus>, MigrationError> {
        self.ensure_tracking_table(db).await?;
        let applied = self.applied_timestamps(db).await?;

        let mut statuses = Vec::new();

        // Auto-diff entries
        for entry in &self.tables {
            let version = format!("auto__{}", entry.table_name);
            let applied_at = applied.get(&version).cloned();
            statuses.push(MigrationStatus {
                version: version.clone(),
                description: format!("Auto-manage table {}", entry.table_name),
                applied: applied.contains_key(&version),
                is_auto: true,
                applied_at,
            });
        }

        // View entries
        for entry in &self.views {
            let version = format!("auto_view__{}", entry.view_name);
            let applied_at = applied.get(&version).cloned();
            statuses.push(MigrationStatus {
                version: version.clone(),
                description: format!("Auto-manage view {}", entry.view_name),
                applied: applied.contains_key(&version),
                is_auto: true,
                applied_at,
            });
        }

        // Materialized view entries
        for entry in &self.mat_views {
            let version = format!("auto_matview__{}", entry.view_name);
            let applied_at = applied.get(&version).cloned();
            statuses.push(MigrationStatus {
                version: version.clone(),
                description: format!("Auto-manage materialized view {}", entry.view_name),
                applied: applied.contains_key(&version),
                is_auto: true,
                applied_at,
            });
        }

        // Manual migrations
        for m in &self.manual {
            let applied_at = applied.get(m.version()).cloned();
            statuses.push(MigrationStatus {
                version: m.version().to_string(),
                description: m.description().to_string(),
                applied: applied.contains_key(m.version()),
                is_auto: false,
                applied_at,
            });
        }

        Ok(statuses)
    }

    /// Preview all pending migrations without applying them.
    ///
    /// Returns a `Vec<MigrationPlan>` describing what SQL would be executed.
    /// Each auto-diff plan carries a `schema_diff` field with the structural
    /// diff (✚/✖/⇄ per column) for that table.
    ///
    /// This method is **read-only**: it does not acquire the lock, does not
    /// verify checksums, and does not write anything to the database.
    /// If the tracking table does not exist yet, it is treated as empty
    /// (all migrations are pending).
    pub async fn dry_run(&self, db: &impl Database) -> Result<Vec<MigrationPlan>, MigrationError> {
        // Read applied versions without creating the tracking table.
        // If the table doesn't exist the query will fail — treat that as "no
        // migrations applied yet" rather than a hard error.
        let applied: HashSet<String> = db
            .query(&format!("SELECT \"version\" FROM {TRACKING_TABLE}"), &[])
            .await
            .unwrap_or_default()
            .into_iter()
            .filter_map(|r| r.get_string("version"))
            .collect();

        let mut plans = self.auto_diff_plans(db, &applied).await?;
        plans.extend(self.view_plans(&applied));
        plans.extend(self.mat_view_plans(&applied));
        plans.extend(self.manual_plans(&applied));

        // Enrich auto-diff plans with structural schema diffs.
        // diff() performs a direct information_schema comparison — read-only.
        let schema_diff = self.diff(db).await?;
        for plan in &mut plans {
            // Only auto-table plans carry a schema_diff; views and manual plans do not.
            let table_name = if let Some(rest) = plan.version.strip_prefix("auto__") {
                // Strip any trailing "_add_<cols>" suffix to recover the base table name.
                // The cols segment uses comma separators (e.g. "auto__users_add_city,role"),
                // so splitting on "_add_" is unambiguous — table names never contain "_add_".
                rest.split("_add_").next().unwrap_or(rest)
            } else {
                continue;
            };
            if let Some(td) = schema_diff
                .tables
                .iter()
                .find(|t| t.table_name == table_name)
            {
                plan.schema_diff = Some(SchemaDiff {
                    tables: vec![td.clone()],
                });
            }
        }

        Ok(plans)
    }
}
