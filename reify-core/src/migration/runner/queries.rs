use super::MigrationRunner;
use super::entries::{create_tracking_table_sql, select_checksums_sql, select_timestamps_sql, select_versions_sql};
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
        dialect: Dialect,
    ) -> Result<(), MigrationError> {
        db.execute(create_tracking_table_sql(dialect), &[])
            .await?;
        Ok(())
    }

    /// Return the dialect-appropriate current-schema expression.
    ///
    /// PostgreSQL: `CURRENT_SCHEMA()` — MySQL/MariaDB: `DATABASE()`.
    pub(super) fn current_schema_expr(dialect: Dialect) -> &'static str {
        match dialect {
            Dialect::Mysql => "DATABASE()",
            _ => "CURRENT_SCHEMA()",
        }
    }

    /// Fetch the set of already-applied migration versions.
    pub(super) async fn applied_versions(
        &self,
        db: &impl Database,
        dialect: Dialect,
    ) -> Result<HashSet<String>, MigrationError> {
        let rows = db
            .query(&select_versions_sql(dialect), &[])
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
        dialect: Dialect,
    ) -> Result<HashMap<String, String>, MigrationError> {
        let rows = db
            .query(&select_checksums_sql(dialect), &[])
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
        dialect: Dialect,
    ) -> Result<HashMap<String, String>, MigrationError> {
        let rows = db
            .query(&select_timestamps_sql(dialect), &[])
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

    /// Fetch detailed column metadata for a table.
    ///
    /// Returns `None` when the table does not exist in the database.
    /// Uses `information_schema` for PostgreSQL/MySQL, `PRAGMA table_info` for SQLite.
    pub async fn existing_column_details_with_dialect(
        &self,
        db: &impl Database,
        table: &str,
        dialect: Dialect,
    ) -> Result<Option<Vec<DbColumnInfo>>, MigrationError> {
        match dialect {
            Dialect::Sqlite => self.existing_column_details_sqlite(db, table).await,
            _ => self.existing_column_details_information_schema(db, table, dialect).await,
        }
    }

    /// SQLite-specific column metadata via `PRAGMA table_info`.
    async fn existing_column_details_sqlite(
        &self,
        db: &impl Database,
        table: &str,
    ) -> Result<Option<Vec<DbColumnInfo>>, MigrationError> {
        // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
        let col_rows = db
            .query(&format!("PRAGMA table_info(\"{table}\");"), &[])
            .await
            .map_err(MigrationError::Db)?;

        if col_rows.is_empty() {
            return Ok(None); // table absent
        }

        // Fetch unique indexes to determine which columns have UNIQUE constraints
        let unique_rows = db
            .query(
                "SELECT il.name AS index_name, ii.name AS column_name \
                 FROM sqlite_master sm \
                 JOIN pragma_index_list(sm.name) il ON 1=1 \
                 JOIN pragma_index_info(il.name) ii ON 1=1 \
                 WHERE sm.type = 'table' AND sm.name = ? AND il.\"unique\" = 1;",
                &[Value::String(table.into())],
            )
            .await
            .unwrap_or_default();

        let unique_cols: std::collections::HashSet<String> = unique_rows
            .into_iter()
            .filter_map(|r| r.get_string("column_name"))
            .collect();

        let infos = col_rows
            .into_iter()
            .filter_map(|r| {
                let name = r.get_string("name")?;
                let data_type = r.get_string("type").map(|s| normalize_sql_type(&s))?;
                // notnull = 1 means NOT NULL, so is_nullable = (notnull == 0)
                let is_nullable = r
                    .get_string("notnull")
                    .map(|s| s == "0")
                    .unwrap_or(true);
                let column_default = r.get_string("dflt_value");
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

    /// PostgreSQL/MySQL column metadata via `information_schema`.
    async fn existing_column_details_information_schema(
        &self,
        db: &impl Database,
        table: &str,
        dialect: Dialect,
    ) -> Result<Option<Vec<DbColumnInfo>>, MigrationError> {
        // Filter by table_schema to avoid false matches in multi-schema environments.
        let schema_expr = Self::current_schema_expr(dialect);
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

        // Fetch unique-constrained column names
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
            .map_err(MigrationError::Db)?;

        let unique_cols: std::collections::HashSet<String> = unique_rows
            .into_iter()
            .filter_map(|r| r.get_string("column_name"))
            .collect();

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

    /// Fetch detailed column metadata for a table from `information_schema`.
    ///
    /// This is a convenience wrapper that auto-detects the dialect from the database.
    pub async fn existing_column_details(
        &self,
        db: &impl Database,
        table: &str,
    ) -> Result<Option<Vec<DbColumnInfo>>, MigrationError> {
        let dialect = self.resolve_dialect(db);
        self.existing_column_details_with_dialect(db, table, dialect).await
    }

    /// Fetch existing column names for a table from the DB.
    ///
    /// Delegates to [`existing_column_details_with_dialect`] and extracts just the names,
    /// avoiding a redundant `information_schema` query when both are needed.
    pub(super) async fn existing_columns(
        &self,
        db: &impl Database,
        table: &str,
        dialect: Dialect,
    ) -> Result<Option<Vec<String>>, MigrationError> {
        Ok(self
            .existing_column_details_with_dialect(db, table, dialect)
            .await?
            .map(|cols| cols.into_iter().map(|c| c.name).collect()))
    }

    /// Fetch existing index names for a table from the database.
    ///
    /// Uses `pg_indexes` for PostgreSQL, `information_schema.statistics`
    /// for MySQL, `sqlite_master` for SQLite. Returns an empty vec if the table doesn't exist.
    pub(super) async fn existing_indexes(
        &self,
        db: &impl Database,
        table: &str,
        dialect: Dialect,
    ) -> Result<Vec<String>, MigrationError> {
        let (query, col_name) = match dialect {
            Dialect::Mysql => (
                "SELECT DISTINCT index_name FROM information_schema.statistics \
                 WHERE table_name = ? AND table_schema = DATABASE() \
                 AND index_name != 'PRIMARY';",
                "index_name",
            ),
            Dialect::Sqlite => (
                "SELECT name FROM sqlite_master \
                 WHERE type = 'index' AND tbl_name = ? AND name NOT LIKE 'sqlite_%';",
                "name",
            ),
            _ => (
                "SELECT indexname FROM pg_indexes \
                 WHERE tablename = ? AND schemaname = CURRENT_SCHEMA();",
                "indexname",
            ),
        };

        let rows = db
            .query(query, &[Value::String(table.into())])
            .await
            .unwrap_or_default();

        Ok(rows
            .into_iter()
            .filter_map(|r| r.get_string(col_name))
            .collect())
    }
}
