use crate::ident::qi;

/// Core DDL renderer — single source of truth for CREATE TABLE generation.
///
/// Iterates `column_defs` in order, renders each column with its constraints,
/// appends FK table-level lines, then appends table-level CHECK constraints.
fn render_create_table(
    table_name: &str,
    column_defs: &[crate::schema::ColumnDef],
    checks: &[String],
    dialect: crate::query::Dialect,
) -> String {
    use crate::schema::{ComputedColumn, TimestampKind, TimestampSource};

    let mut col_lines: Vec<String> = Vec::new();

    for def in column_defs {
        // Skip Rust-side virtual columns — they don't exist in the DB.
        if matches!(def.computed, Some(ComputedColumn::Virtual)) {
            continue;
        }

        let mut parts: Vec<String> = vec![format!("    {}", qi(def.name))];

        let sql_type = def.sql_type.to_sql(dialect);
        parts.push(sql_type.into_owned());

        // DB-generated computed column: GENERATED ALWAYS AS (expr) STORED
        if let Some(ComputedColumn::Stored(expr)) = &def.computed {
            parts.push(format!("GENERATED ALWAYS AS ({expr}) STORED"));
        } else {
            if def.primary_key {
                parts.push("PRIMARY KEY".into());
            }
            if !def.nullable && !def.primary_key {
                parts.push("NOT NULL".into());
            }
            if def.unique {
                parts.push("UNIQUE".into());
            }

            // DB-source timestamps: emit dialect-appropriate DEFAULT
            if def.timestamp_source == TimestampSource::Db && def.timestamp_kind.is_some() {
                let default_now = match dialect {
                    crate::query::Dialect::Mysql => "DEFAULT CURRENT_TIMESTAMP",
                    _ => "DEFAULT NOW()",
                };
                parts.push(default_now.into());

                // MySQL: update_timestamp with Db source gets ON UPDATE CURRENT_TIMESTAMP
                if def.timestamp_kind == Some(TimestampKind::Update)
                    && dialect == crate::query::Dialect::Mysql
                {
                    parts.push("ON UPDATE CURRENT_TIMESTAMP".into());
                }
            } else if let Some(ref dv) = def.default {
                parts.push(format!("DEFAULT {dv}"));
            }

            // Column-level CHECK constraint
            if let Some(ref check_expr) = def.check {
                parts.push(format!("CHECK ({check_expr})"));
            }
        }

        col_lines.push(parts.join(" "));
    }

    // Table-level FOREIGN KEY constraints
    let fk_lines: Vec<String> = column_defs
        .iter()
        .filter_map(|def| {
            let fk = def.foreign_key.as_ref()?;
            Some(format!(
                "    FOREIGN KEY ({}) {}",
                qi(def.name),
                fk.to_references_clause()
            ))
        })
        .collect();

    // Table-level CHECK constraints
    let check_lines: Vec<String> = checks
        .iter()
        .map(|expr| format!("    CHECK ({expr})"))
        .collect();

    let mut all_lines = col_lines;
    all_lines.extend(fk_lines);
    all_lines.extend(check_lines);

    format!(
        "CREATE TABLE IF NOT EXISTS {} (\n{}\n);",
        qi(table_name),
        all_lines.join(",\n")
    )
}

/// Generate a `CREATE TABLE IF NOT EXISTS` statement from an explicit table name
/// and column definitions.
///
/// Used by `MigrationRunner` and for synthetic tables (e.g. audit companions).
pub fn create_table_sql(
    table_name: &str,
    column_defs: &[crate::schema::ColumnDef],
    dialect: crate::query::Dialect,
) -> String {
    render_create_table(table_name, column_defs, &[], dialect)
}

/// Generate a `CREATE TABLE IF NOT EXISTS` statement with optional table-level
/// CHECK constraints (from `TableSchema.checks`).
pub fn create_table_sql_with_checks(
    table_name: &str,
    column_defs: &[crate::schema::ColumnDef],
    checks: &[String],
    dialect: crate::query::Dialect,
) -> String {
    render_create_table(table_name, column_defs, checks, dialect)
}

/// Generate `ALTER TABLE … ADD COLUMN` for a column present in the struct
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
                "ALTER TABLE {} ADD COLUMN {} {} GENERATED ALWAYS AS ({expr}) STORED;",
                qi(table),
                qi(column),
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
    format!(
        "ALTER TABLE {} ADD COLUMN {} {sql_type}{null_clause}{default_clause};",
        qi(table),
        qi(column)
    )
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
