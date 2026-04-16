use super::MigrationRunner;
use super::entries::{TRACKING_TABLE, create_tracking_table_sql};
use crate::db::Database;
use crate::migration::diff::{DbColumnInfo, normalize_sql_type};
use crate::migration::error::MigrationError;
use crate::query::Dialect;
use crate::value::Value;
use std::collections::{HashMap, HashSet};

impl MigrationRunner {
    // ── Internal helpers ─────────────────────────────────────────────

    /// Ensure the tracking table exists.
    pub(super) async fn ensure_tracking_table(
        &self,
        db: &impl Database,
    ) -> Result<(), MigrationError> {
        db.execute(create_tracking_table_sql(self.dialect), &[])
            .await?;
        Ok(())
    }

    /// Return the dialect-appropriate current-schema expression.
    ///
    /// PostgreSQL: `CURRENT_SCHEMA()` — MySQL/MariaDB: `DATABASE()`.
    pub(super) fn current_schema_expr(&self) -> &'static str {
        match self.dialect {
            Dialect::Mysql => "DATABASE()",
            _ => "CURRENT_SCHEMA()",
        }
    }

    /// Fetch the set of already-applied migration versions.
    pub(super) async fn applied_versions(
        &self,
        db: &impl Database,
    ) -> Result<HashSet<String>, MigrationError> {
        let rows = db
            .query(&format!("SELECT \"version\" FROM {TRACKING_TABLE}"), &[])
            .await?;
        let versions = rows
            .into_iter()
            .filter_map(|r| r.get_string("version"))
            .collect();
        Ok(versions)
    }

    /// Fetch applied migration versions together with their stored checksums.
    ///
    /// Returns a map of `version → checksum` for all applied migrations.
    pub(super) async fn applied_checksums(
        &self,
        db: &impl Database,
    ) -> Result<HashMap<String, String>, MigrationError> {
        let rows = db
            .query(
                &format!("SELECT \"version\", \"checksum\" FROM {TRACKING_TABLE}"),
                &[],
            )
            .await?;
        let map = rows
            .into_iter()
            .filter_map(|r| {
                let version = r.get_string("version")?;
                let checksum = r.get_string("checksum").unwrap_or_default();
                Some((version, checksum))
            })
            .collect();
        Ok(map)
    }

    /// Fetch applied versions with their `applied_at` timestamps.
    ///
    /// Returns a map of `version → applied_at` (as raw string from the DB).
    pub(super) async fn applied_timestamps(
        &self,
        db: &impl Database,
    ) -> Result<HashMap<String, String>, MigrationError> {
        let rows = db
            .query(
                &format!(
                    "SELECT \"version\", CAST(\"applied_at\" AS TEXT) AS \"applied_at\" \
                     FROM {TRACKING_TABLE}"
                ),
                &[],
            )
            .await?;
        let map = rows
            .into_iter()
            .filter_map(|r| {
                let version = r.get_string("version")?;
                let ts = r.get_string("applied_at").unwrap_or_default();
                Some((version, ts))
            })
            .collect();
        Ok(map)
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
        // Filter by table_schema to avoid false matches in multi-schema environments.
        let schema_expr = self.current_schema_expr();
        let col_rows = db
            .query(
                &format!(
                    "SELECT column_name, data_type, is_nullable, column_default \
                     FROM information_schema.columns \
                     WHERE table_name = ? \
                       AND table_schema = {schema_expr} \
                     ORDER BY ordinal_position;"
                ),
                &[Value::String(table.into())],
            )
            .await
            .map_err(MigrationError::Db)?;

        if col_rows.is_empty() {
            return Ok(None); // table absent
        }

        // ── 2. Fetch unique-constrained column names ──────────────────
        let unique_rows = db
            .query(
                &format!(
                    "SELECT kcu.column_name \
                     FROM information_schema.table_constraints tc \
                     JOIN information_schema.key_column_usage kcu \
                       ON tc.constraint_name = kcu.constraint_name \
                      AND tc.table_name      = kcu.table_name \
                     WHERE tc.table_name      = ? \
                       AND tc.table_schema    = {schema_expr} \
                       AND tc.constraint_type = 'UNIQUE';"
                ),
                &[Value::String(table.into())],
            )
            .await
            .unwrap_or_default();

        let unique_cols: std::collections::HashSet<String> = unique_rows
            .into_iter()
            .filter_map(|r| r.get_string("column_name"))
            .collect();

        // ── 3. Build DbColumnInfo list ────────────────────────────────
        let infos = col_rows
            .into_iter()
            .filter_map(|r| {
                let name = r.get_string("column_name")?;
                let data_type = r.get_string("data_type").map(|s| normalize_sql_type(&s))?;
                let is_nullable = r
                    .get_string("is_nullable")
                    .map(|s| s.eq_ignore_ascii_case("yes"))
                    .unwrap_or(false);
                let column_default = r.get_string("column_default");
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
    ///
    /// Filters by the dialect-appropriate current-schema expression to avoid
    /// false matches in multi-schema / multi-database environments.
    pub(super) async fn existing_columns(
        &self,
        db: &impl Database,
        table: &str,
    ) -> Result<Option<Vec<String>>, MigrationError> {
        let schema_expr = self.current_schema_expr();
        let rows = db
            .query(
                &format!(
                    "SELECT column_name FROM information_schema.columns \
                     WHERE table_name = ? \
                       AND table_schema = {schema_expr} \
                     ORDER BY ordinal_position;"
                ),
                &[Value::String(table.into())],
            )
            .await
            .map_err(MigrationError::Db)?;

        if rows.is_empty() {
            return Ok(None); // table absent
        }

        let cols = rows
            .into_iter()
            .filter_map(|r| r.get_string("column_name"))
            .collect();
        Ok(Some(cols))
    }
}
