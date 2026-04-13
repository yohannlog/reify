use crate::db::{Database, DynDatabase, DbError};
use crate::table::Table;
use crate::value::Value;

// ── Error ────────────────────────────────────────────────────────────

/// Error type for migration operations.
#[derive(Debug)]
pub enum MigrationError {
    /// Underlying database error.
    Db(DbError),
    /// A migration is not reversible but rollback was requested.
    NotReversible(String),
    /// Generic migration error.
    Other(String),
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationError::Db(e) => write!(f, "database error: {e}"),
            MigrationError::NotReversible(v) => {
                write!(f, "migration '{v}' is not reversible")
            }
            MigrationError::Other(msg) => write!(f, "migration error: {msg}"),
        }
    }
}

impl std::error::Error for MigrationError {}

impl From<DbError> for MigrationError {
    fn from(e: DbError) -> Self {
        MigrationError::Db(e)
    }
}

// ── MigrationContext ─────────────────────────────────────────────────

/// Context passed to `Migration::up` and `Migration::down`.
///
/// Collects SQL statements to be executed (or previewed in dry-run mode).
pub struct MigrationContext {
    /// Accumulated SQL statements in execution order.
    pub(crate) statements: Vec<String>,
}

impl MigrationContext {
    /// Create a new, empty migration context.
    pub fn new() -> Self {
        Self { statements: Vec::new() }
    }

    /// Return the accumulated SQL statements.
    pub fn statements(&self) -> &[String] {
        &self.statements
    }

    /// Add a column to an existing table.
    ///
    /// ```ignore
    /// ctx.add_column("users", "city", "TEXT NOT NULL DEFAULT ''");
    /// ```
    pub fn add_column(&mut self, table: &str, column: &str, sql_type: &str) {
        self.statements.push(format!(
            "ALTER TABLE {table} ADD COLUMN {column} {sql_type};"
        ));
    }

    /// Drop a column from an existing table.
    pub fn drop_column(&mut self, table: &str, column: &str) {
        self.statements
            .push(format!("ALTER TABLE {table} DROP COLUMN {column};"));
    }

    /// Rename a column in an existing table.
    pub fn rename_column(&mut self, table: &str, from: &str, to: &str) {
        self.statements.push(format!(
            "ALTER TABLE {table} RENAME COLUMN {from} TO {to};"
        ));
    }

    /// Execute a raw SQL statement as part of this migration.
    ///
    /// Use `?` as the placeholder character.
    pub fn execute(&mut self, sql: impl Into<String>) {
        self.statements.push(sql.into());
    }

    /// Create or replace a SQL view.
    ///
    /// ```ignore
    /// ctx.create_view("active_users", "SELECT id, email FROM users WHERE deleted_at IS NULL");
    /// ```
    pub fn create_view(&mut self, name: &str, query: &str) {
        self.statements.push(crate::view::create_view_sql(name, query));
    }

    /// Drop a SQL view if it exists.
    ///
    /// ```ignore
    /// ctx.drop_view("active_users");
    /// ```
    pub fn drop_view(&mut self, name: &str) {
        self.statements.push(crate::view::drop_view_sql(name));
    }
}

// ── Migration trait ──────────────────────────────────────────────────

/// A single, versioned database migration.
///
/// Implement this trait for complex migrations that cannot be auto-detected
/// (renames, type changes, data migrations, etc.).
///
/// # Example
///
/// ```ignore
/// pub struct AddUserCity;
///
/// impl Migration for AddUserCity {
///     fn version(&self) -> &'static str { "20240320_000001_add_user_city" }
///     fn description(&self) -> &'static str { "Add city column to users" }
///
///     fn up(&self, ctx: &mut MigrationContext) {
///         ctx.add_column("users", "city", "TEXT NOT NULL DEFAULT ''");
///     }
///
///     fn down(&self, ctx: &mut MigrationContext) {
///         ctx.drop_column("users", "city");
///     }
/// }
/// ```
pub trait Migration: Send + Sync {
    /// Unique version string — used as the primary key in the tracking table.
    ///
    /// Convention: `YYYYMMDD_NNNNNN_snake_case_description`
    fn version(&self) -> &'static str;

    /// Human-readable description shown in `reify status` output.
    fn description(&self) -> &'static str;

    /// Apply the migration (forward direction).
    fn up(&self, ctx: &mut MigrationContext);

    /// Revert the migration (backward direction).
    ///
    /// Only called when `is_reversible()` returns `true`.
    /// Default implementation is a no-op (migration is irreversible).
    fn down(&self, ctx: &mut MigrationContext) {
        let _ = ctx;
    }

    /// Whether this migration can be rolled back via `down()`.
    ///
    /// Return `false` for destructive migrations (DROP TABLE, DROP COLUMN, …)
    /// where reversal is impossible or unsafe.
    fn is_reversible(&self) -> bool {
        true
    }
}

// ── MigrationPlan ────────────────────────────────────────────────────

/// The result of a dry-run: what *would* be executed, without applying it.
#[derive(Debug, Clone)]
pub struct MigrationPlan {
    /// Migration version string.
    pub version: String,
    /// Human-readable description.
    pub description: String,
    /// SQL statements that would be executed.
    pub statements: Vec<String>,
    /// Direction: `true` = up (apply), `false` = down (rollback).
    pub is_up: bool,
}

impl MigrationPlan {
    /// Pretty-print the plan to a string (mirrors the dry-run output format).
    pub fn display(&self) -> String {
        let dir = if self.is_up { "up" } else { "down" };
        let mut out = format!("  ~ Would apply ({dir}): {}\n", self.version);
        out.push_str(&format!("    -- {}\n", self.description));
        for stmt in &self.statements {
            for line in stmt.lines() {
                out.push_str(&format!("    {line}\n"));
            }
        }
        out
    }
}

// ── Schema diff types ───────────────────────────────────────────────

/// Column metadata fetched from the live database via `information_schema`.
#[derive(Debug, Clone, PartialEq)]
pub struct DbColumnInfo {
    /// Column name.
    pub name: String,
    /// SQL data type as reported by the database (lowercased and normalised).
    pub data_type: String,
    /// Whether the column accepts NULL values.
    pub is_nullable: bool,
    /// Column default expression, if any.
    pub column_default: Option<String>,
    /// Whether the column has a UNIQUE constraint.
    pub is_unique: bool,
}

/// A single column-level difference between the Rust struct definition and the
/// live database schema.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnDiff {
    /// Column exists in the struct but not in the database.
    Added { column: String },
    /// Column exists in the database but not in the struct.
    Removed { column: String },
    /// The SQL data type differs between struct and database.
    TypeChanged { column: String, from: String, to: String },
    /// The nullability differs between struct and database.
    NullableChanged { column: String, from: bool, to: bool },
    /// The UNIQUE constraint differs between struct and database.
    UniqueChanged { column: String, from: bool, to: bool },
    /// The column default differs between struct and database.
    DefaultChanged { column: String, from: Option<String>, to: Option<String> },
}

impl ColumnDiff {
    /// Human-readable description of this diff entry.
    pub fn display(&self) -> String {
        match self {
            ColumnDiff::Added { column } =>
                format!("    ✚ `{column}` added"),
            ColumnDiff::Removed { column } =>
                format!("    ✖ `{column}` removed"),
            ColumnDiff::TypeChanged { column, from, to } =>
                format!("    ⇄ `{column}`: type {from} → {to}"),
            ColumnDiff::NullableChanged { column, from, to } => {
                let from_s = if *from { "nullable" } else { "not null" };
                let to_s   = if *to   { "nullable" } else { "not null" };
                format!("    ⇄ `{column}`: {from_s} → {to_s}")
            }
            ColumnDiff::UniqueChanged { column, from, to } => {
                let from_s = if *from { "unique" } else { "non-unique" };
                let to_s   = if *to   { "unique" } else { "non-unique" };
                format!("    ⇄ `{column}`: {from_s} → {to_s}")
            }
            ColumnDiff::DefaultChanged { column, from, to } => {
                let fmt = |v: &Option<String>| v.as_deref().unwrap_or("none").to_string();
                format!("    ⇄ `{column}`: default {} → {}", fmt(from), fmt(to))
            }
        }
    }
}

/// Diff for a single table — collects all column-level differences.
#[derive(Debug, Clone)]
pub struct TableDiff {
    /// Name of the table.
    pub table_name: String,
    /// `true` when the table does not yet exist in the database.
    pub is_new_table: bool,
    /// Per-column differences.
    pub column_diffs: Vec<ColumnDiff>,
}

impl TableDiff {
    /// `true` when there are no differences for this table.
    pub fn is_empty(&self) -> bool {
        !self.is_new_table && self.column_diffs.is_empty()
    }

    /// Human-readable summary of this table's diff.
    pub fn display(&self) -> String {
        let mut out = if self.is_new_table {
            format!("  ✚ table `{}` (new)\n", self.table_name)
        } else {
            format!("  ⇄ table `{}`\n", self.table_name)
        };
        for diff in &self.column_diffs {
            out.push_str(&diff.display());
            out.push('\n');
        }
        out
    }
}

/// Full schema diff between all registered Rust structs and the live database.
#[derive(Debug, Clone)]
pub struct SchemaDiff {
    /// Per-table diffs (only tables with at least one difference are included).
    pub tables: Vec<TableDiff>,
}

impl SchemaDiff {
    /// `true` when there are no differences across all tables.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Human-readable summary of the full schema diff, grouped by table.
    ///
    /// Symbols used:
    /// - `✚` — new table or added column
    /// - `✖` — removed column
    /// - `⇄` — changed attribute (type, nullability, uniqueness, default)
    pub fn display(&self) -> String {
        if self.is_empty() {
            return "  (no schema differences detected)\n".to_string();
        }
        let mut out = String::from("Schema diff:\n");
        for table in &self.tables {
            out.push_str(&table.display());
        }
        out
    }
}

// ── Type normalisation ───────────────────────────────────────────────

/// Normalise a SQL type string so that aliases and case variants compare equal.
///
/// Examples:
/// - `"BIGSERIAL"` → `"bigint"`
/// - `"CHARACTER VARYING"` → `"varchar"`
/// - `"INT8"` → `"bigint"`
/// - `"BOOL"` → `"boolean"`
pub fn normalize_sql_type(raw: &str) -> String {
    let lower = raw.trim().to_lowercase();
    // Split base type from parenthesised params, e.g. "varchar(255)" → ("varchar", Some("255"))
    let (base, params) = match lower.find('(') {
        Some(idx) => (
            lower[..idx].trim(),
            Some(lower[idx..].trim().to_string()),
        ),
        None => (lower.as_str(), None),
    };
    let normalized_base = match base {
        // Serial / auto-increment shorthands
        "serial" | "serial4" => "integer",
        "bigserial" | "serial8" => "bigint",
        "smallserial" | "serial2" => "smallint",
        // Integer aliases
        "int" | "int4" | "integer" => "integer",
        "int8" | "bigint" => "bigint",
        "int2" | "smallint" => "smallint",
        // Character aliases — preserve params
        "character varying" | "varchar" => "varchar",
        "character" | "char" => "char",
        // Numeric aliases — normalize both to "numeric", preserve params
        "decimal" | "numeric" => "numeric",
        // Boolean aliases
        "bool" | "boolean" => "boolean",
        // Float aliases
        "float4" | "real" => "real",
        "float8" | "double precision" => "double precision",
        // Timestamp aliases
        "timestamp without time zone" | "timestamp" => "timestamp",
        "timestamp with time zone" | "timestamptz" => "timestamptz",
        // Pass through anything else unchanged
        other => return match params {
            Some(p) => format!("{other}{p}"),
            None => other.to_string(),
        },
    };
    // Preserve params for types where precision/length matters
    match normalized_base {
        "varchar" | "char" | "numeric" => match params {
            Some(p) => format!("{normalized_base}{p}"),
            None => normalized_base.to_string(),
        },
        _ => normalized_base.to_string(),
    }
}

// ── DDL generation ───────────────────────────────────────────────────

/// Generate a `CREATE TABLE IF NOT EXISTS` statement for a `Table` type.
///
/// Column types and constraints are read from the provided `ColumnDef` metadata
/// (generated by `#[derive(Table)]` or the `Schema` builder API).
/// The `dialect` controls backend-specific type rendering.
pub fn create_table_sql<T: Table>(
    column_defs: &[crate::schema::ColumnDef],
    dialect: crate::query::Dialect,
) -> String {
    use crate::schema::{ComputedColumn, TimestampKind, TimestampSource};

    let table = T::table_name();
    let names = T::column_names();

    let mut col_lines: Vec<String> = Vec::new();

    for name in names.iter() {
        let def = column_defs.iter().find(|d| d.name == *name);

        // Skip Rust-side virtual columns — they don't exist in the DB.
        if let Some(d) = def {
            if matches!(d.computed, Some(ComputedColumn::Virtual)) {
                continue;
            }
        }

        let mut parts: Vec<String> = vec![format!("    {name}")];

        let is_nullable = def.map(|d| d.nullable).unwrap_or(false);
        let is_pk = def.map(|d| d.primary_key).unwrap_or(false);
        let is_unique = def.map(|d| d.unique).unwrap_or(false);
        let default_val = def.and_then(|d| d.default.as_deref());
        let computed = def.and_then(|d| d.computed.as_ref());
        let ts_kind = def.and_then(|d| d.timestamp_kind);
        let ts_source = def.map(|d| d.timestamp_source).unwrap_or(TimestampSource::Vm);

        // Type — from metadata, not from column name heuristics
        let sql_type = def
            .map(|d| d.sql_type.to_sql(dialect))
            .unwrap_or(std::borrow::Cow::Borrowed("TEXT"));
        parts.push(sql_type.into_owned());

        // DB-generated computed column: GENERATED ALWAYS AS (expr) STORED
        if let Some(ComputedColumn::Stored(expr)) = computed {
            parts.push(format!("GENERATED ALWAYS AS ({expr}) STORED"));
        } else {
            // Constraints (not applicable to generated columns)
            if is_pk {
                parts.push("PRIMARY KEY".into());
            }
            if !is_nullable && !is_pk {
                parts.push("NOT NULL".into());
            }
            if is_unique {
                parts.push("UNIQUE".into());
            }

            // DB-source timestamps: emit dialect-appropriate DEFAULT
            if ts_source == TimestampSource::Db && ts_kind.is_some() {
                let default_now = match dialect {
                    crate::query::Dialect::Mysql => "DEFAULT CURRENT_TIMESTAMP",
                    _ => "DEFAULT NOW()",
                };
                parts.push(default_now.into());

                // MySQL: update_timestamp with Db source gets ON UPDATE CURRENT_TIMESTAMP
                if ts_kind == Some(TimestampKind::Update)
                    && dialect == crate::query::Dialect::Mysql
                {
                    parts.push("ON UPDATE CURRENT_TIMESTAMP".into());
                }
            } else if let Some(dv) = default_val {
                parts.push(format!("DEFAULT {dv}"));
            }

            // Column-level CHECK constraint
            if let Some(check_expr) = def.and_then(|d| d.check.as_deref()) {
                parts.push(format!("CHECK ({check_expr})"));
            }
        }

        col_lines.push(parts.join(" "));
    }

    format!(
        "CREATE TABLE IF NOT EXISTS {table} (\n{}\n);",
        col_lines.join(",\n")
    )
}

/// Generate a `CREATE TABLE IF NOT EXISTS` statement with optional table-level
/// CHECK constraints (from `TableSchema.checks`).
///
/// Column-level CHECK constraints are rendered inline by `create_table_sql`;
/// this function additionally appends table-level CHECK lines after all columns.
pub fn create_table_sql_with_checks<T: Table>(
    column_defs: &[crate::schema::ColumnDef],
    checks: &[String],
    dialect: crate::query::Dialect,
) -> String {
    if checks.is_empty() {
        return create_table_sql::<T>(column_defs, dialect);
    }

    // Build the base DDL (without the closing ");") and append table-level checks
    let base = create_table_sql::<T>(column_defs, dialect);
    // Strip trailing "\n);" to append more lines
    let trimmed = base.trim_end_matches("\n);");
    let mut result = trimmed.to_string();
    for check in checks {
        result.push_str(&format!(",\n    CHECK ({check})"));
    }
    result.push_str("\n);");
    result
}

/// Generate a `CREATE TABLE IF NOT EXISTS` statement from an explicit table name
/// and column definitions (used for synthetic tables like audit companions).
pub(crate) fn create_table_sql_named(
    table_name: &str,
    column_defs: &[crate::schema::ColumnDef],
    dialect: crate::query::Dialect,
) -> String {
    use crate::schema::TimestampSource;

    let mut col_lines: Vec<String> = Vec::new();

    for def in column_defs {
        let mut parts: Vec<String> = vec![format!("    {}", def.name)];

        let sql_type = def.sql_type.to_sql(dialect);
        parts.push(sql_type.into_owned());

        if def.primary_key {
            parts.push("PRIMARY KEY".into());
        }
        if !def.nullable && !def.primary_key {
            parts.push("NOT NULL".into());
        }
        if def.unique {
            parts.push("UNIQUE".into());
        }
        if def.timestamp_source == TimestampSource::Db {
            let default_now = match dialect {
                crate::query::Dialect::Mysql => "DEFAULT CURRENT_TIMESTAMP",
                _ => "DEFAULT NOW()",
            };
            parts.push(default_now.into());
        } else if let Some(ref dv) = def.default {
            parts.push(format!("DEFAULT {dv}"));
        }

        // Column-level CHECK constraint
        if let Some(ref check_expr) = def.check {
            parts.push(format!("CHECK ({check_expr})"));
        }

        col_lines.push(parts.join(" "));
    }

    format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (\n{}\n);",
        col_lines.join(",\n")
    )
}

/// Generate `ALTER TABLE … ADD COLUMN` for columns present in the struct
/// but missing from the database.
pub fn add_column_sql(
    table: &str,
    column: &str,
    def: Option<&crate::schema::ColumnDef>,
    dialect: crate::query::Dialect,
) -> String {
    use crate::schema::ComputedColumn;

    // DB-generated computed column
    if let Some(d) = def {
        if let Some(ComputedColumn::Stored(expr)) = &d.computed {
            let sql_type = d.sql_type.to_sql(dialect);
            return format!(
                "ALTER TABLE {table} ADD COLUMN {column} {} GENERATED ALWAYS AS ({expr}) STORED;",
                &*sql_type
            );
        }
    }

    let is_nullable = def.map(|d| d.nullable).unwrap_or(false);
    let sql_type = def
        .map(|d| d.sql_type.to_sql(dialect))
        .unwrap_or(std::borrow::Cow::Borrowed("TEXT"));
    let null_clause = if is_nullable { "" } else { " NOT NULL" };
    let default_clause = if !is_nullable {
        format!(" DEFAULT {}", default_for_type(&sql_type))
    } else {
        String::new()
    };
    format!("ALTER TABLE {table} ADD COLUMN {column} {sql_type}{null_clause}{default_clause};")
}

fn default_for_type(ty: &str) -> &'static str {
    if ty.starts_with("DECIMAL") || ty.starts_with("NUMERIC") {
        return "0";
    }
    if ty.starts_with("VARCHAR") || ty.starts_with("CHAR(") {
        return "''";
    }
    match ty {
        "BIGINT" | "INTEGER" | "SMALLINT" | "NUMERIC" | "BIGSERIAL" | "SERIAL" => "0",
        "BOOLEAN" => "FALSE",
        "TIMESTAMPTZ" | "TIMESTAMP" | "DATETIME" => "NOW()",
        _ => "''",
    }
}

// ── Tracking table ───────────────────────────────────────────────────

const TRACKING_TABLE: &str = "_reify_migrations";

const CREATE_TRACKING_TABLE: &str = "
CREATE TABLE IF NOT EXISTS _reify_migrations (
    version     TEXT        NOT NULL PRIMARY KEY,
    description TEXT        NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);";

// ── TableEntry — auto-diff entry ─────────────────────────────────────

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
        Self { tables: Vec::new(), views: Vec::new(), manual: Vec::new() }
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
                    })
                    .collect()
            } else {
                defs
            }
        };

        let create_sql = create_table_sql::<T>(&column_defs, crate::query::Dialect::Generic);

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

    /// Register a `Table` type that has `#[table(audit)]` for automatic diff-based migration.
    ///
    /// Registers both the main table (via `add_table::<T>()`) and a synthetic audit companion
    /// table (`<table>_audit`) with the 5 fixed audit columns.
    pub fn add_audited_table<T: Table + crate::audit::Auditable>(mut self) -> Self {
        self = self.add_table::<T>();
        let audit_defs = T::audit_column_defs();
        let audit_name = T::audit_table_name();
        let create_sql = create_table_sql_named(audit_name, &audit_defs, crate::query::Dialect::Generic);
        self.tables.push(TableEntry {
            table_name: audit_name,
            column_names: &[],
            column_defs: audit_defs,
            create_sql,
        });
        self
    }

    /// Register a `Table` type with explicit `Schema` metadata, plus its audit companion table.
    ///
    /// Same as `add_audited_table` but delegates the main table registration to
    /// `add_table_with_schema(schema)` for users who define their schema via the builder API.
    pub fn add_audited_table_with_schema<T>(mut self, schema: crate::schema::TableSchema<T>) -> Self
    where
        T: Table + crate::audit::Auditable,
    {
        self = self.add_table_with_schema(schema);
        let audit_defs = T::audit_column_defs();
        let audit_name = T::audit_table_name();
        let create_sql = create_table_sql_named(audit_name, &audit_defs, crate::query::Dialect::Generic);
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
            .query(
                &format!("SELECT version FROM {TRACKING_TABLE}"),
                &[],
            )
            .await?;
        let versions = rows
            .into_iter()
            .filter_map(|r| {
                r.get("version")
                    .and_then(|v| if let Value::String(s) = v { Some(s.clone()) } else { None })
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
            &format!(
                "INSERT INTO {TRACKING_TABLE} (version, description) VALUES (?, ?);"
            ),
            &[Value::String(version.into()), Value::String(description.into())],
        )
        .await?;
        Ok(())
    }

    /// Remove a migration record (rollback).
    async fn mark_reverted(
        &self,
        db: &impl Database,
        version: &str,
    ) -> Result<(), MigrationError> {
        db.execute(
            &format!("DELETE FROM {TRACKING_TABLE} WHERE version = ?;"),
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
                r.get("column_name")
                    .and_then(|v| if let Value::String(s) = v { Some(s.clone()) } else { None })
            })
            .collect();

        // ── 3. Build DbColumnInfo list ────────────────────────────────
        let infos = col_rows
            .into_iter()
            .filter_map(|r| {
                let name = r.get("column_name").and_then(|v| {
                    if let Value::String(s) = v { Some(s.clone()) } else { None }
                })?;
                let data_type = r.get("data_type").and_then(|v| {
                    if let Value::String(s) = v { Some(normalize_sql_type(s)) } else { None }
                })?;
                let is_nullable = r.get("is_nullable").and_then(|v| {
                    if let Value::String(s) = v {
                        Some(s.eq_ignore_ascii_case("yes"))
                    } else {
                        None
                    }
                }).unwrap_or(false);
                let column_default = r.get("column_default").and_then(|v| {
                    match v {
                        Value::String(s) => Some(s.clone()),
                        _ => None,
                    }
                });
                let is_unique = unique_cols.contains(&name);
                Some(DbColumnInfo { name, data_type, is_nullable, column_default, is_unique })
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
                            if let Value::String(s) = v { Some(s.clone()) } else { None }
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
                            stmts.push(add_column_sql(entry.table_name, col, def, crate::query::Dialect::Generic));
                        }
                    }
                    if !stmts.is_empty() {
                        let add_version = format!("auto__{}_add_columns", entry.table_name);
                        if !applied.contains(&add_version) {
                            plans.push(MigrationPlan {
                                version: add_version,
                                description: format!(
                                    "Add new columns to {}",
                                    entry.table_name
                                ),
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
                            stmts.push(add_column_sql(entry.table_name, col, def, crate::query::Dialect::Generic));
                        }
                    }
                    if !stmts.is_empty() {
                        plans.push(MigrationPlan {
                            version,
                            description: format!(
                                "Add new columns to {}",
                                entry.table_name
                            ),
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
    fn view_plans(
        &self,
        applied: &std::collections::HashSet<String>,
    ) -> Vec<MigrationPlan> {
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
                            .map(|col| ColumnDiff::Added { column: col.to_string() })
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
                                    let def = entry
                                        .column_defs
                                        .iter()
                                        .find(|d| d.name == *col_name);

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
                                    let struct_nullable =
                                        def.map(|d| d.nullable).unwrap_or(false);
                                    if struct_nullable != db_col.is_nullable {
                                        diffs.push(ColumnDiff::NullableChanged {
                                            column: col_name.to_string(),
                                            from: db_col.is_nullable,
                                            to: struct_nullable,
                                        });
                                    }

                                    // Uniqueness
                                    let struct_unique =
                                        def.map(|d| d.unique).unwrap_or(false);
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

        Ok(SchemaDiff { tables: table_diffs })
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
                self.mark_applied(db, &plan.version, &plan.description).await?;
            }

            // View plans (after tables, since views may reference them)
            let view_plans = self.view_plans(&applied);
            for plan in &view_plans {
                for stmt in &plan.statements {
                    db.execute(stmt, &[]).await?;
                }
                self.mark_applied(db, &plan.version, &plan.description).await?;
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
                self.mark_applied(db, &plan.version, &plan.description).await?;
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
                    "SELECT version FROM {TRACKING_TABLE} \
                     WHERE version NOT LIKE 'auto__%' \
                     ORDER BY applied_at DESC LIMIT 1;"
                ),
                &[],
            )
            .await?;

        let last_version = rows
            .first()
            .and_then(|r| r.get("version"))
            .and_then(|v| if let Value::String(s) = v { Some(s.clone()) } else { None });

        let version = match last_version {
            Some(v) => v,
            None => return Err(MigrationError::Other("no applied migrations to roll back".into())),
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
    pub async fn rollback_to(&self, db: &impl Database, target_version: &str) -> Result<(), MigrationError> {
        self.ensure_tracking_table(db).await?;

        let rows = db
            .query(
                &format!(
                    "SELECT version FROM {TRACKING_TABLE} \
                     WHERE version NOT LIKE 'auto__%' \
                     ORDER BY applied_at DESC;"
                ),
                &[],
            )
            .await?;

        let versions: Vec<String> = rows
            .into_iter()
            .filter_map(|r| {
                r.get("version").and_then(|v| {
                    if let Value::String(s) = v { Some(s.clone()) } else { None }
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
        let query = schema
            .query_sql()
            .unwrap_or_else(|| {
                match V::view_query() {
                    crate::view::ViewQuery::Raw(s) => s,
                    crate::view::ViewQuery::Typed { sql, .. } => sql,
                }
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

// ── MigrationStatus ──────────────────────────────────────────────────

/// Status of a single migration (applied or pending).
#[derive(Debug, Clone)]
pub struct MigrationStatus {
    /// Migration version string.
    pub version: String,
    /// Human-readable description.
    pub description: String,
    /// Whether this migration has been applied.
    pub applied: bool,
    /// Whether this is an auto-diff migration (vs. manual).
    pub is_auto: bool,
}

impl MigrationStatus {
    /// Format for CLI display.
    pub fn display(&self) -> String {
        let mark = if self.applied { "✓ Applied " } else { "~ Pending " };
        format!("  {mark}  {}", self.version)
    }
}

// ── Template generation ──────────────────────────────────────────────

/// Generate the content of a new migration file from a name.
///
/// Used by `reify new <name>`.
pub fn generate_migration_file(name: &str, version: &str) -> String {
    // Convert snake_case name to PascalCase struct name
    let struct_name: String = name
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect();

    format!(
        r#"// Generated by: reify new {name}
use reify::{{Migration, MigrationContext}};

pub struct {struct_name};

impl Migration for {struct_name} {{
    fn version(&self) -> &'static str {{
        "{version}"
    }}

    fn description(&self) -> &'static str {{
        "{name}"
    }}

    fn up(&self, ctx: &mut MigrationContext) {{
        todo!("implement up migration")
    }}

    fn down(&self, ctx: &mut MigrationContext) {{
        todo!("implement down migration")
    }}
}}
"#
    )
}

/// Generate the content of a new **view** migration file from a name.
///
/// Used by `reify new --view <name>`.
pub fn generate_view_migration_file(name: &str, version: &str) -> String {
    let struct_name: String = name
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect();

    format!(
        r#"// Generated by: reify new --view {name}
use reify::{{Migration, MigrationContext}};

pub struct {struct_name};

impl Migration for {struct_name} {{
    fn version(&self) -> &'static str {{
        "{version}"
    }}

    fn description(&self) -> &'static str {{
        "Create view {name}"
    }}

    fn up(&self, ctx: &mut MigrationContext) {{
        ctx.create_view("{name}", "SELECT ... FROM ... WHERE ...");
    }}

    fn down(&self, ctx: &mut MigrationContext) {{
        ctx.drop_view("{name}");
    }}
}}
"#
    )
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Row;
    use std::sync::{Arc, Mutex};

    // ── Mock Database ────────────────────────────────────────────────

    /// Captures all SQL executed and returns configurable query results.
    #[derive(Clone)]
    struct MockDb {
        executed: Arc<Mutex<Vec<String>>>,
        query_rows: Arc<Mutex<Vec<Vec<Row>>>>,
    }

    impl MockDb {
        fn new() -> Self {
            Self {
                executed: Arc::new(Mutex::new(Vec::new())),
                query_rows: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Pre-load rows to be returned by successive `query()` calls.
        fn push_query_result(&self, rows: Vec<Row>) {
            self.query_rows.lock().unwrap().push(rows);
        }

        fn executed_sql(&self) -> Vec<String> {
            self.executed.lock().unwrap().clone()
        }
    }

    impl Database for MockDb {
        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<u64, DbError> {
            self.executed.lock().unwrap().push(sql.to_string());
            Ok(1)
        }

        async fn query(&self, _sql: &str, _params: &[Value]) -> Result<Vec<Row>, DbError> {
            let rows = {
                let mut q = self.query_rows.lock().unwrap();
                if q.is_empty() { vec![] } else { q.remove(0) }
            };
            Ok(rows)
        }

        async fn query_one(&self, _sql: &str, _params: &[Value]) -> Result<Row, DbError> {
            Err(DbError::Query("no rows".into()))
        }

        async fn transaction<'a>(
            &'a self,
            f: Box<
                dyn FnOnce(
                        &'a dyn DynDatabase,
                    ) -> std::pin::Pin<
                        Box<dyn std::future::Future<Output = Result<(), DbError>> + Send + 'a>,
                    > + Send
                    + 'a,
            >,
        ) -> Result<(), DbError> {
            f(self).await
        }
    }

    // ── Minimal Table impl for tests ─────────────────────────────────

    struct Users;
    impl Table for Users {
        fn table_name() -> &'static str { "users" }
        fn column_names() -> &'static [&'static str] { &["id", "email", "role"] }
        fn into_values(&self) -> Vec<Value> { vec![] }
        fn column_defs() -> Vec<crate::schema::ColumnDef> {
            vec![
                crate::schema::ColumnDef {
                    name: "id",
                    sql_type: crate::schema::SqlType::BigInt,
                    primary_key: true,
                    auto_increment: false,
                    unique: false,
                    index: false,
                    nullable: false,
                    default: None,
                    computed: None,
                    timestamp_kind: None,
                    timestamp_source: crate::schema::TimestampSource::Vm,
                    check: None,
                },
                crate::schema::ColumnDef {
                    name: "email",
                    sql_type: crate::schema::SqlType::Text,
                    primary_key: false,
                    auto_increment: false,
                    unique: false,
                    index: false,
                    nullable: false,
                    default: None,
                    computed: None,
                    timestamp_kind: None,
                    timestamp_source: crate::schema::TimestampSource::Vm,
                    check: None,
                },
                crate::schema::ColumnDef {
                    name: "role",
                    sql_type: crate::schema::SqlType::Text,
                    primary_key: false,
                    auto_increment: false,
                    unique: false,
                    index: false,
                    nullable: false,
                    default: None,
                    computed: None,
                    timestamp_kind: None,
                    timestamp_source: crate::schema::TimestampSource::Vm,
                    check: None,
                },
            ]
        }
    }

    // ── Manual migration fixture ─────────────────────────────────────

    struct AddUserCity;
    impl Migration for AddUserCity {
        fn version(&self) -> &'static str { "20240320_000001_add_user_city" }
        fn description(&self) -> &'static str { "Add city column to users" }
        fn up(&self, ctx: &mut MigrationContext) {
            ctx.add_column("users", "city", "TEXT NOT NULL DEFAULT ''");
        }
        fn down(&self, ctx: &mut MigrationContext) {
            ctx.drop_column("users", "city");
        }
    }

    struct IrreversibleMigration;
    impl Migration for IrreversibleMigration {
        fn version(&self) -> &'static str { "20240321_000001_irreversible" }
        fn description(&self) -> &'static str { "Drop old table" }
        fn is_reversible(&self) -> bool { false }
        fn up(&self, ctx: &mut MigrationContext) {
            ctx.execute("DROP TABLE old_table;");
        }
    }

    // ── Tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn dry_run_new_table_emits_create_table() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns users → absent

        let runner = MigrationRunner::new().add_table::<Users>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].version, "auto__users");
        assert!(plans[0].statements[0].contains("CREATE TABLE IF NOT EXISTS users"));
    }

    #[tokio::test]
    async fn dry_run_existing_table_no_new_columns_emits_nothing() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        // existing_columns returns all three columns → no diff
        let existing = vec![
            Row::new(vec!["column_name".into()], vec![Value::String("id".into())]),
            Row::new(vec!["column_name".into()], vec![Value::String("email".into())]),
            Row::new(vec!["column_name".into()], vec![Value::String("role".into())]),
        ];
        db.push_query_result(existing);

        let runner = MigrationRunner::new().add_table::<Users>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert!(plans.is_empty());
    }

    #[tokio::test]
    async fn dry_run_existing_table_new_column_emits_add_column() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        // Table exists but missing "role"
        let existing = vec![
            Row::new(vec!["column_name".into()], vec![Value::String("id".into())]),
            Row::new(vec!["column_name".into()], vec![Value::String("email".into())]),
        ];
        db.push_query_result(existing);

        let runner = MigrationRunner::new().add_table::<Users>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        assert!(plans[0].statements[0].contains("ADD COLUMN role"));
    }

    #[tokio::test]
    async fn dry_run_manual_migration_included() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        let runner = MigrationRunner::new().add(AddUserCity);
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].version, "20240320_000001_add_user_city");
        assert!(plans[0].statements[0].contains("ADD COLUMN city"));
    }

    #[tokio::test]
    async fn dry_run_skips_already_applied_manual() {
        let db = MockDb::new();
        // applied_versions returns the manual migration as already applied
        let applied_row = Row::new(
            vec!["version".into()],
            vec![Value::String("20240320_000001_add_user_city".into())],
        );
        db.push_query_result(vec![applied_row]);

        let runner = MigrationRunner::new().add(AddUserCity);
        let plans = runner.dry_run(&db).await.unwrap();

        assert!(plans.is_empty());
    }

    #[tokio::test]
    async fn run_creates_tracking_table_and_executes_ddl() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns users → absent

        let runner = MigrationRunner::new().add_table::<Users>();
        runner.run(&db).await.unwrap();

        let sql = db.executed_sql();
        // First statement: CREATE tracking table
        assert!(sql[0].contains("_reify_migrations"));
        // Second: CREATE TABLE users
        assert!(sql[1].contains("CREATE TABLE IF NOT EXISTS users"));
        // Third: INSERT into tracking table
        assert!(sql[2].contains("INSERT INTO _reify_migrations"));
    }

    #[tokio::test]
    async fn run_manual_migration_executes_up_statements() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        let runner = MigrationRunner::new().add(AddUserCity);
        runner.run(&db).await.unwrap();

        let sql = db.executed_sql();
        let has_add_column = sql.iter().any(|s| s.contains("ADD COLUMN city"));
        assert!(has_add_column);
    }

    #[tokio::test]
    async fn rollback_executes_down_and_removes_tracking_row() {
        let db = MockDb::new();
        // applied_versions for rollback query
        let applied_row = Row::new(
            vec!["version".into()],
            vec![Value::String("20240320_000001_add_user_city".into())],
        );
        db.push_query_result(vec![applied_row]); // last applied query

        let runner = MigrationRunner::new().add(AddUserCity);
        runner.rollback(&db).await.unwrap();

        let sql = db.executed_sql();
        let has_drop = sql.iter().any(|s| s.contains("DROP COLUMN city"));
        let has_delete = sql.iter().any(|s| s.contains("DELETE FROM _reify_migrations"));
        assert!(has_drop, "expected DROP COLUMN city in: {sql:?}");
        assert!(has_delete, "expected DELETE FROM tracking in: {sql:?}");
    }

    #[tokio::test]
    async fn rollback_irreversible_returns_error() {
        let db = MockDb::new();
        let applied_row = Row::new(
            vec!["version".into()],
            vec![Value::String("20240321_000001_irreversible".into())],
        );
        db.push_query_result(vec![applied_row]);

        let runner = MigrationRunner::new().add(IrreversibleMigration);
        let result = runner.rollback(&db).await;

        assert!(matches!(result, Err(MigrationError::NotReversible(_))));
    }

    #[tokio::test]
    async fn migration_context_collects_statements() {
        let mut ctx = MigrationContext::new();
        ctx.add_column("users", "city", "TEXT NOT NULL");
        ctx.drop_column("users", "old_col");
        ctx.rename_column("users", "nm", "name");
        ctx.execute("UPDATE users SET city = 'Paris';");

        assert_eq!(ctx.statements.len(), 4);
        assert!(ctx.statements[0].contains("ADD COLUMN city"));
        assert!(ctx.statements[1].contains("DROP COLUMN old_col"));
        assert!(ctx.statements[2].contains("RENAME COLUMN nm TO name"));
        assert!(ctx.statements[3].contains("UPDATE users"));
    }

    #[tokio::test]
    async fn migration_plan_display_format() {
        let plan = MigrationPlan {
            version: "20240320_000001_add_user_city".into(),
            description: "Add city column to users".into(),
            statements: vec!["ALTER TABLE users ADD COLUMN city TEXT NOT NULL;".into()],
            is_up: true,
        };
        let display = plan.display();
        assert!(display.contains("Would apply (up)"));
        assert!(display.contains("20240320_000001_add_user_city"));
        assert!(display.contains("ALTER TABLE users"));
    }

    #[test]
    fn generate_migration_file_produces_valid_template() {
        let content = generate_migration_file(
            "add_user_city",
            "20240320_000001_add_user_city",
        );
        assert!(content.contains("struct AddUserCity"));
        assert!(content.contains("impl Migration for AddUserCity"));
        assert!(content.contains("20240320_000001_add_user_city"));
        assert!(content.contains("todo!(\"implement up migration\")"));
    }

    // ── Schema diff / DbColumnInfo tests ────────────────────────────

    #[test]
    fn normalize_sql_type_aliases() {
        assert_eq!(normalize_sql_type("BIGSERIAL"), "bigint");
        assert_eq!(normalize_sql_type("bigserial"), "bigint");
        assert_eq!(normalize_sql_type("serial"), "integer");
        assert_eq!(normalize_sql_type("smallserial"), "smallint");
        assert_eq!(normalize_sql_type("int"), "integer");
        assert_eq!(normalize_sql_type("INT4"), "integer");
        assert_eq!(normalize_sql_type("int8"), "bigint");
        assert_eq!(normalize_sql_type("CHARACTER VARYING"), "varchar");
        assert_eq!(normalize_sql_type("varchar(255)"), "varchar");
        assert_eq!(normalize_sql_type("bool"), "boolean");
        assert_eq!(normalize_sql_type("float4"), "real");
        assert_eq!(normalize_sql_type("float8"), "double precision");
        assert_eq!(normalize_sql_type("timestamp without time zone"), "timestamp");
        assert_eq!(normalize_sql_type("timestamp with time zone"), "timestamptz");
        assert_eq!(normalize_sql_type("TIMESTAMPTZ"), "timestamptz");
        // Unknown types pass through lowercased
        assert_eq!(normalize_sql_type("JSONB"), "jsonb");
        assert_eq!(normalize_sql_type("uuid"), "uuid");
    }

    #[test]
    fn column_diff_display_variants() {
        assert!(ColumnDiff::Added { column: "city".into() }.display().contains("✚ `city`"));
        assert!(ColumnDiff::Removed { column: "old".into() }.display().contains("✖ `old`"));
        assert!(ColumnDiff::TypeChanged {
            column: "age".into(),
            from: "integer".into(),
            to: "bigint".into(),
        }
        .display()
        .contains("integer → bigint"));
        assert!(ColumnDiff::NullableChanged {
            column: "email".into(),
            from: false,
            to: true,
        }
        .display()
        .contains("not null → nullable"));
        assert!(ColumnDiff::UniqueChanged {
            column: "slug".into(),
            from: false,
            to: true,
        }
        .display()
        .contains("non-unique → unique"));
        assert!(ColumnDiff::DefaultChanged {
            column: "role".into(),
            from: None,
            to: Some("'member'".into()),
        }
        .display()
        .contains("none → 'member'"));
    }

    #[test]
    fn schema_diff_is_empty_and_display() {
        let empty = SchemaDiff { tables: vec![] };
        assert!(empty.is_empty());
        assert!(empty.display().contains("no schema differences"));

        let diff = SchemaDiff {
            tables: vec![TableDiff {
                table_name: "users".into(),
                is_new_table: true,
                column_diffs: vec![],
            }],
        };
        assert!(!diff.is_empty());
        assert!(diff.display().contains("✚ table `users`"));
    }

    #[test]
    fn table_diff_is_empty() {
        let empty = TableDiff {
            table_name: "posts".into(),
            is_new_table: false,
            column_diffs: vec![],
        };
        assert!(empty.is_empty());

        let non_empty = TableDiff {
            table_name: "posts".into(),
            is_new_table: false,
            column_diffs: vec![ColumnDiff::Added { column: "slug".into() }],
        };
        assert!(!non_empty.is_empty());
    }

    #[tokio::test]
    async fn existing_column_details_returns_none_for_absent_table() {
        let db = MockDb::new();
        // Both queries return empty → table absent
        db.push_query_result(vec![]);
        db.push_query_result(vec![]);

        let runner = MigrationRunner::new();
        let result = runner.existing_column_details(&db, "missing_table").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn existing_column_details_parses_rows() {
        let db = MockDb::new();

        // Column metadata rows
        let col_rows = vec![
            Row::new(
                vec![
                    "column_name".into(),
                    "data_type".into(),
                    "is_nullable".into(),
                    "column_default".into(),
                ],
                vec![
                    Value::String("id".into()),
                    Value::String("bigint".into()),
                    Value::String("NO".into()),
                    Value::Null,
                ],
            ),
            Row::new(
                vec![
                    "column_name".into(),
                    "data_type".into(),
                    "is_nullable".into(),
                    "column_default".into(),
                ],
                vec![
                    Value::String("email".into()),
                    Value::String("CHARACTER VARYING".into()),
                    Value::String("YES".into()),
                    Value::String("''".into()),
                ],
            ),
        ];
        db.push_query_result(col_rows);

        // Unique constraint rows — only "email" is unique
        let unique_rows = vec![Row::new(
            vec!["column_name".into()],
            vec![Value::String("email".into())],
        )];
        db.push_query_result(unique_rows);

        let runner = MigrationRunner::new();
        let infos = runner
            .existing_column_details(&db, "users")
            .await
            .unwrap()
            .expect("should return Some");

        assert_eq!(infos.len(), 2);

        let id_col = &infos[0];
        assert_eq!(id_col.name, "id");
        assert_eq!(id_col.data_type, "bigint");
        assert!(!id_col.is_nullable);
        assert!(id_col.column_default.is_none());
        assert!(!id_col.is_unique);

        let email_col = &infos[1];
        assert_eq!(email_col.name, "email");
        assert_eq!(email_col.data_type, "varchar"); // normalised
        assert!(email_col.is_nullable);
        assert_eq!(email_col.column_default.as_deref(), Some("''"));
        assert!(email_col.is_unique);
    }

    // ── diff() tests ─────────────────────────────────────────────────

    /// Build a single-column DbColumnInfo row for MockDb.
    fn make_col_row(name: &str, data_type: &str, nullable: &str) -> Row {
        Row::new(
            vec![
                "column_name".into(),
                "data_type".into(),
                "is_nullable".into(),
                "column_default".into(),
            ],
            vec![
                Value::String(name.into()),
                Value::String(data_type.into()),
                Value::String(nullable.into()),
                Value::Null,
            ],
        )
    }

    #[tokio::test]
    async fn diff_new_table_reports_all_columns_added() {
        let db = MockDb::new();
        // existing_column_details: col query → empty (table absent)
        db.push_query_result(vec![]);

        let runner = MigrationRunner::new().add_table::<Users>();
        let diff = runner.diff(&db).await.unwrap();

        assert_eq!(diff.tables.len(), 1);
        let td = &diff.tables[0];
        assert_eq!(td.table_name, "users");
        assert!(td.is_new_table);
        // All three columns (id, email, role) reported as Added
        assert_eq!(td.column_diffs.len(), 3);
        assert!(td.column_diffs.iter().all(|d| matches!(d, ColumnDiff::Added { .. })));
    }

    #[tokio::test]
    async fn diff_no_changes_returns_empty() {
        let db = MockDb::new();
        // Column rows matching Users exactly
        db.push_query_result(vec![
            make_col_row("id",    "bigint", "NO"),
            make_col_row("email", "text",   "NO"),
            make_col_row("role",  "text",   "NO"),
        ]);
        db.push_query_result(vec![]); // no unique constraints

        let runner = MigrationRunner::new().add_table::<Users>();
        let diff = runner.diff(&db).await.unwrap();

        assert!(diff.is_empty(), "expected no diff, got: {:#?}", diff.tables);
    }

    #[tokio::test]
    async fn diff_added_column_detected() {
        let db = MockDb::new();
        // DB only has id + email; struct also has role
        db.push_query_result(vec![
            make_col_row("id",    "bigint", "NO"),
            make_col_row("email", "text",   "NO"),
        ]);
        db.push_query_result(vec![]); // no unique constraints

        let runner = MigrationRunner::new().add_table::<Users>();
        let diff = runner.diff(&db).await.unwrap();

        assert_eq!(diff.tables.len(), 1);
        let added: Vec<_> = diff.tables[0]
            .column_diffs
            .iter()
            .filter(|d| matches!(d, ColumnDiff::Added { column } if column == "role"))
            .collect();
        assert_eq!(added.len(), 1);
    }

    #[tokio::test]
    async fn diff_removed_column_detected() {
        let db = MockDb::new();
        // DB has an extra column "legacy" not in the struct
        db.push_query_result(vec![
            make_col_row("id",     "bigint", "NO"),
            make_col_row("email",  "text",   "NO"),
            make_col_row("role",   "text",   "NO"),
            make_col_row("legacy", "text",   "YES"),
        ]);
        db.push_query_result(vec![]); // no unique constraints

        let runner = MigrationRunner::new().add_table::<Users>();
        let diff = runner.diff(&db).await.unwrap();

        assert_eq!(diff.tables.len(), 1);
        let removed: Vec<_> = diff.tables[0]
            .column_diffs
            .iter()
            .filter(|d| matches!(d, ColumnDiff::Removed { column } if column == "legacy"))
            .collect();
        assert_eq!(removed.len(), 1);
    }

    #[tokio::test]
    async fn diff_type_change_detected() {
        let db = MockDb::new();
        // DB has id as "integer" but struct infers "bigint"
        db.push_query_result(vec![
            make_col_row("id",    "integer", "NO"),
            make_col_row("email", "text",    "NO"),
            make_col_row("role",  "text",    "NO"),
        ]);
        db.push_query_result(vec![]);

        let runner = MigrationRunner::new().add_table::<Users>();
        let diff = runner.diff(&db).await.unwrap();

        assert_eq!(diff.tables.len(), 1);
        let type_changes: Vec<_> = diff.tables[0]
            .column_diffs
            .iter()
            .filter(|d| matches!(d, ColumnDiff::TypeChanged { column, .. } if column == "id"))
            .collect();
        assert_eq!(type_changes.len(), 1);
        if let ColumnDiff::TypeChanged { from, to, .. } = type_changes[0] {
            assert_eq!(from, "integer");
            assert_eq!(to, "bigint");
        }
    }

    #[tokio::test]
    async fn diff_nullable_change_detected() {
        let db = MockDb::new();
        // DB has email as nullable; struct has it as NOT NULL (nullable: false)
        db.push_query_result(vec![
            make_col_row("id",    "bigint", "NO"),
            make_col_row("email", "text",   "YES"), // nullable in DB
            make_col_row("role",  "text",   "NO"),
        ]);
        db.push_query_result(vec![]);

        let runner = MigrationRunner::new().add_table::<Users>();
        let diff = runner.diff(&db).await.unwrap();

        assert_eq!(diff.tables.len(), 1);
        let nullable_changes: Vec<_> = diff.tables[0]
            .column_diffs
            .iter()
            .filter(|d| matches!(d, ColumnDiff::NullableChanged { column, .. } if column == "email"))
            .collect();
        assert_eq!(nullable_changes.len(), 1);
        if let ColumnDiff::NullableChanged { from, to, .. } = nullable_changes[0] {
            assert!(*from, "DB was nullable");
            assert!(!*to, "struct is not nullable");
        }
    }

    #[tokio::test]
    async fn diff_display_output_contains_symbols() {
        let diff = SchemaDiff {
            tables: vec![TableDiff {
                table_name: "orders".into(),
                is_new_table: false,
                column_diffs: vec![
                    ColumnDiff::Added   { column: "total".into() },
                    ColumnDiff::Removed { column: "old_col".into() },
                    ColumnDiff::TypeChanged {
                        column: "amount".into(),
                        from: "integer".into(),
                        to: "numeric".into(),
                    },
                ],
            }],
        };
        let out = diff.display();
        assert!(out.contains("⇄ table `orders`"),  "missing table header: {out}");
        assert!(out.contains("✚ `total`"),          "missing added symbol: {out}");
        assert!(out.contains("✖ `old_col`"),        "missing removed symbol: {out}");
        assert!(out.contains("⇄ `amount`"),         "missing changed symbol: {out}");
        assert!(out.contains("Schema diff:"),       "missing header: {out}");
    }

    #[test]
    fn create_table_sql_contains_all_columns() {
        let defs: Vec<crate::schema::ColumnDef> = vec![
            crate::schema::ColumnDef {
                name: "id",
                sql_type: crate::schema::SqlType::BigSerial,
                primary_key: true,
                auto_increment: true,
                unique: false,
                index: false,
                nullable: false,
                default: None,
                computed: None,
                timestamp_kind: None,
                timestamp_source: crate::schema::TimestampSource::Vm,
                check: None,
            },
            crate::schema::ColumnDef {
                name: "email",
                sql_type: crate::schema::SqlType::Text,
                primary_key: false,
                auto_increment: false,
                unique: true,
                index: false,
                nullable: false,
                default: None,
                computed: None,
                timestamp_kind: None,
                timestamp_source: crate::schema::TimestampSource::Vm,
                check: None,
            },
        ];
        let sql = create_table_sql::<Users>(&defs, crate::query::Dialect::Postgres);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS users"));
        assert!(sql.contains("id"));
        assert!(sql.contains("email"));
        assert!(sql.contains("BIGSERIAL"));
        assert!(sql.contains("PRIMARY KEY"));
    }

    // ── View migration tests ────────────────────────────────────────

    #[test]
    fn migration_context_create_view() {
        let mut ctx = MigrationContext::new();
        ctx.create_view("active_users", "SELECT id, email FROM users WHERE deleted_at IS NULL");
        assert_eq!(ctx.statements.len(), 1);
        assert!(ctx.statements[0].contains("CREATE OR REPLACE VIEW active_users"));
        assert!(ctx.statements[0].contains("SELECT id, email FROM users"));
    }

    #[test]
    fn migration_context_drop_view() {
        let mut ctx = MigrationContext::new();
        ctx.drop_view("active_users");
        assert_eq!(ctx.statements.len(), 1);
        assert!(ctx.statements[0].contains("DROP VIEW IF EXISTS active_users"));
    }

    // Minimal View impl for tests
    struct TestView;
    impl Table for TestView {
        fn table_name() -> &'static str { "active_users" }
        fn column_names() -> &'static [&'static str] { &["id", "email"] }
        fn into_values(&self) -> Vec<Value> { vec![] }
    }
    impl crate::view::View for TestView {
        fn view_name() -> &'static str { "active_users" }
        fn view_query() -> crate::view::ViewQuery {
            crate::view::ViewQuery::Raw(
                "SELECT id, email FROM users WHERE deleted_at IS NULL".into(),
            )
        }
    }

    #[tokio::test]
    async fn dry_run_view_emits_create_view() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        let runner = MigrationRunner::new().add_view::<TestView>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].version, "auto_view__active_users");
        assert!(plans[0].statements[0].contains("CREATE OR REPLACE VIEW active_users"));
    }

    #[tokio::test]
    async fn dry_run_view_skips_already_applied() {
        let db = MockDb::new();
        let applied_row = Row::new(
            vec!["version".into()],
            vec![Value::String("auto_view__active_users".into())],
        );
        db.push_query_result(vec![applied_row]);

        let runner = MigrationRunner::new().add_view::<TestView>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert!(plans.is_empty());
    }

    #[tokio::test]
    async fn run_view_executes_create_view() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        let runner = MigrationRunner::new().add_view::<TestView>();
        runner.run(&db).await.unwrap();

        let sql = db.executed_sql();
        let has_create_view = sql.iter().any(|s| s.contains("CREATE OR REPLACE VIEW active_users"));
        assert!(has_create_view, "expected CREATE VIEW in: {sql:?}");
    }

    #[test]
    fn generate_view_migration_file_produces_valid_template() {
        let content = generate_view_migration_file(
            "active_users",
            "20240320_000001_active_users",
        );
        assert!(content.contains("struct ActiveUsers"));
        assert!(content.contains("impl Migration for ActiveUsers"));
        assert!(content.contains("ctx.create_view(\"active_users\""));
        assert!(content.contains("ctx.drop_view(\"active_users\""));
    }
}
