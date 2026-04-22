use crate::query::Dialect;

/// Return the dialect-appropriate quoted name for the migration tracking table.
///
/// - PostgreSQL/SQLite/Generic: `"_reify_migrations"` (double quotes)
/// - MySQL: `` `_reify_migrations` `` (backticks)
pub(crate) fn tracking_table(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::Mysql => "`_reify_migrations`",
        _ => "\"_reify_migrations\"",
    }
}

/// Return a dialect-appropriate quoted column name.
///
/// - PostgreSQL/SQLite/Generic: `"col"` (double quotes)
/// - MySQL: `` `col` `` (backticks)
pub(crate) fn quote_col(col: &str, dialect: Dialect) -> String {
    match dialect {
        Dialect::Mysql => format!("`{col}`"),
        _ => format!("\"{col}\""),
    }
}

// ── Pre-built SQL statements for tracking table operations ──────────

/// SELECT version FROM tracking table.
pub(crate) fn select_versions_sql(dialect: Dialect) -> String {
    let t = tracking_table(dialect);
    let v = quote_col("version", dialect);
    format!("SELECT {v} FROM {t}")
}

/// SELECT version, checksum FROM tracking table.
pub(crate) fn select_checksums_sql(dialect: Dialect) -> String {
    let t = tracking_table(dialect);
    let v = quote_col("version", dialect);
    let c = quote_col("checksum", dialect);
    format!("SELECT {v}, {c} FROM {t}")
}

/// SELECT version, applied_at FROM tracking table.
pub(crate) fn select_timestamps_sql(dialect: Dialect) -> String {
    let t = tracking_table(dialect);
    let v = quote_col("version", dialect);
    let a = quote_col("applied_at", dialect);
    format!("SELECT {v}, CAST({a} AS TEXT) AS {a} FROM {t}")
}

/// INSERT INTO tracking table (version, description, checksum, comment).
pub(crate) fn insert_migration_sql(dialect: Dialect) -> String {
    let t = tracking_table(dialect);
    let v = quote_col("version", dialect);
    let d = quote_col("description", dialect);
    let c = quote_col("checksum", dialect);
    let m = quote_col("comment", dialect);
    format!("INSERT INTO {t} ({v}, {d}, {c}, {m}) VALUES (?, ?, ?, ?);")
}

/// DELETE FROM tracking table WHERE version = ?.
pub(crate) fn delete_migration_sql(dialect: Dialect) -> String {
    let t = tracking_table(dialect);
    let v = quote_col("version", dialect);
    format!("DELETE FROM {t} WHERE {v} = ?;")
}

/// SELECT version FROM tracking table for manual migrations (not auto__%), ordered by applied_at DESC.
pub(crate) fn select_manual_versions_sql(dialect: Dialect) -> String {
    let t = tracking_table(dialect);
    let v = quote_col("version", dialect);
    let a = quote_col("applied_at", dialect);
    format!("SELECT {v} FROM {t} WHERE {v} NOT LIKE 'auto__%' ORDER BY {a} DESC")
}

/// SELECT version FROM tracking table for manual migrations, LIMIT 1.
pub(crate) fn select_last_manual_version_sql(dialect: Dialect) -> String {
    format!("{} LIMIT 1;", select_manual_versions_sql(dialect))
}

/// SELECT version, applied_at FROM tracking table for manual migrations since a timestamp.
pub(crate) fn select_manual_versions_since_sql(dialect: Dialect) -> String {
    let t = tracking_table(dialect);
    let v = quote_col("version", dialect);
    let a = quote_col("applied_at", dialect);
    format!(
        "SELECT {v}, CAST({a} AS TEXT) AS {a} FROM {t} \
         WHERE {v} NOT LIKE 'auto__%' AND CAST({a} AS TEXT) >= ? \
         ORDER BY {a} DESC;"
    )
}

/// UPSERT for run_since — dialect-specific ON CONFLICT / ON DUPLICATE KEY.
pub(crate) fn upsert_migration_sql(dialect: Dialect) -> String {
    let t = tracking_table(dialect);
    let v = quote_col("version", dialect);
    let d = quote_col("description", dialect);
    let c = quote_col("checksum", dialect);
    let m = quote_col("comment", dialect);
    let a = quote_col("applied_at", dialect);

    match dialect {
        Dialect::Mysql => format!(
            "INSERT INTO {t} ({v}, {d}, {c}, {m}) VALUES (?, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE \
             {d} = VALUES({d}), {c} = VALUES({c}), {m} = VALUES({m}), {a} = CURRENT_TIMESTAMP;"
        ),
        _ => format!(
            "INSERT INTO {t} ({v}, {d}, {c}, {m}) VALUES (?, ?, ?, ?) \
             ON CONFLICT ({v}) DO UPDATE SET \
             {d} = EXCLUDED.{d}, {c} = EXCLUDED.{c}, {m} = EXCLUDED.{m}, {a} = NOW();"
        ),
    }
}

/// DDL for the migration tracking table, parameterised by dialect.
///
/// PostgreSQL uses `TIMESTAMPTZ`; MySQL/MariaDB uses `DATETIME`.
pub(crate) fn create_tracking_table_sql(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::Mysql => {
            "CREATE TABLE IF NOT EXISTS `_reify_migrations` (\
             `version`     VARCHAR(512) NOT NULL PRIMARY KEY,\
             `description` TEXT         NOT NULL,\
             `applied_at`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,\
             `checksum`    TEXT         NOT NULL DEFAULT '',\
             `comment`     TEXT\
             );"
        }
        _ => {
            "CREATE TABLE IF NOT EXISTS \"_reify_migrations\" (\
             \"version\"     TEXT        NOT NULL PRIMARY KEY,\
             \"description\" TEXT        NOT NULL,\
             \"applied_at\"  TIMESTAMPTZ NOT NULL DEFAULT NOW(),\
             \"checksum\"    TEXT        NOT NULL DEFAULT '',\
             \"comment\"     TEXT\
             );"
        }
    }
}

/// An entry registered via `MigrationRunner::add_table::<T>()`.
pub(super) struct TableEntry {
    pub(super) table_name: &'static str,
    pub(super) column_names: &'static [&'static str],
    pub(super) column_defs: Vec<crate::schema::ColumnDef>,
    /// Index definitions from `Table::indexes()`.
    pub(super) indexes: Vec<crate::schema::IndexDef>,
    /// Optional CHECK constraints from `TableSchema`.
    pub(super) checks: Vec<String>,
}

/// An entry registered via `MigrationRunner::add_view::<V>()`.
pub(super) struct ViewEntry {
    pub(super) view_name: &'static str,
    /// The SELECT query that defines this view.
    pub(super) query: String,
}

/// An entry registered via `MigrationRunner::add_materialized_view::<V>()`.
pub(super) struct MatViewEntry {
    pub(super) view_name: &'static str,
    /// The SELECT query that defines this materialized view.
    pub(super) query: String,
}
