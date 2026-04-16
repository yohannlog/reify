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
    TypeChanged {
        column: String,
        from: String,
        to: String,
    },
    /// The nullability differs between struct and database.
    NullableChanged {
        column: String,
        from: bool,
        to: bool,
    },
    /// The UNIQUE constraint differs between struct and database.
    UniqueChanged {
        column: String,
        from: bool,
        to: bool,
    },
    /// The column default differs between struct and database.
    DefaultChanged {
        column: String,
        from: Option<String>,
        to: Option<String>,
    },
}

impl ColumnDiff {
    /// Human-readable description of this diff entry.
    pub fn display(&self) -> String {
        match self {
            ColumnDiff::Added { column } => format!("    ✚ `{column}` added"),
            ColumnDiff::Removed { column } => format!("    ✖ `{column}` removed"),
            ColumnDiff::TypeChanged { column, from, to } => {
                format!("    ⇄ `{column}`: type {from} → {to}")
            }
            ColumnDiff::NullableChanged { column, from, to } => {
                let from_s = if *from { "nullable" } else { "not null" };
                let to_s = if *to { "nullable" } else { "not null" };
                format!("    ⇄ `{column}`: {from_s} → {to_s}")
            }
            ColumnDiff::UniqueChanged { column, from, to } => {
                let from_s = if *from { "unique" } else { "non-unique" };
                let to_s = if *to { "unique" } else { "non-unique" };
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
        Some(idx) => (lower[..idx].trim(), Some(lower[idx..].trim().to_string())),
        None => (lower.as_str(), None),
    };
    // Handle PostgreSQL array notation: "integer[]" or internal names like "_int4".
    let base = if base.ends_with("[]") {
        return format!("{}[]", normalize_sql_type(&base[..base.len() - 2]));
    } else if let Some(inner) = base.strip_prefix('_') {
        // PostgreSQL internal array prefix: _int4 → integer[]
        let inner_normalized = normalize_sql_type(inner);
        return format!("{}[]", inner_normalized);
    } else {
        base
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
        other => {
            return match params {
                Some(p) => format!("{other}{p}"),
                None => other.to_string(),
            };
        }
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
