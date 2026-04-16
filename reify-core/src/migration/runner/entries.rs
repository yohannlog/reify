use crate::query::Dialect;

pub(crate) const TRACKING_TABLE: &str = "\"_reify_migrations\"";

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
    /// Pre-built CREATE TABLE SQL.
    pub(super) create_sql: String,
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
