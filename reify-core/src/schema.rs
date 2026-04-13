use std::marker::PhantomData;

use crate::column::Column;
use crate::query::Dialect;

// ── SQL types ──────────────────────────────────────────────────────

/// SQL column type — the source-of-truth for DDL generation.
///
/// Derived automatically from Rust types by `#[derive(Table)]`, or set
/// explicitly via `#[column(sql_type = "JSONB")]` / the builder API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlType {
    /// `SMALLINT` / `INT2`
    SmallInt,
    /// `INTEGER` / `INT4`
    Integer,
    /// `BIGINT` / `INT8`
    BigInt,
    /// `REAL` / `FLOAT4`
    Float,
    /// `DOUBLE PRECISION` / `FLOAT8`
    Double,
    /// `NUMERIC` / `DECIMAL`
    Numeric,
    /// `BOOLEAN`
    Boolean,
    /// `TEXT`
    Text,
    /// `BYTEA` (Postgres) / `BLOB` (SQLite/MySQL)
    Bytea,
    /// `UUID` (Postgres) / `CHAR(36)` (MySQL) / `TEXT` (SQLite)
    Uuid,
    /// `TIMESTAMPTZ` (Postgres) / `DATETIME` (MySQL/SQLite)
    Timestamptz,
    /// `TIMESTAMP` (without time zone)
    Timestamp,
    /// `DATE`
    Date,
    /// `TIME`
    Time,
    /// `JSONB` (Postgres) / `JSON` (MySQL) / `TEXT` (SQLite)
    Jsonb,
    /// `BIGSERIAL` (Postgres) / `BIGINT AUTO_INCREMENT` (MySQL) /
    /// `INTEGER` with AUTOINCREMENT (SQLite)
    BigSerial,
    /// `SERIAL` (Postgres) / `INT AUTO_INCREMENT` (MySQL) /
    /// `INTEGER` with AUTOINCREMENT (SQLite)
    Serial,
    /// Escape hatch — raw SQL type string.
    Custom(&'static str),
}

impl SqlType {
    /// Render this type as a SQL string for the given dialect.
    pub fn to_sql(&self, dialect: Dialect) -> &'static str {
        match self {
            SqlType::SmallInt => "SMALLINT",
            SqlType::Integer => "INTEGER",
            SqlType::BigInt => "BIGINT",
            SqlType::Float => match dialect {
                Dialect::Postgres => "REAL",
                _ => "FLOAT",
            },
            SqlType::Double => match dialect {
                Dialect::Postgres => "DOUBLE PRECISION",
                _ => "DOUBLE",
            },
            SqlType::Numeric => "NUMERIC",
            SqlType::Boolean => "BOOLEAN",
            SqlType::Text => "TEXT",
            SqlType::Bytea => match dialect {
                Dialect::Postgres => "BYTEA",
                Dialect::Mysql => "LONGBLOB",
                _ => "BLOB",
            },
            SqlType::Uuid => match dialect {
                Dialect::Postgres => "UUID",
                Dialect::Mysql => "CHAR(36)",
                _ => "TEXT",
            },
            SqlType::Timestamptz => match dialect {
                Dialect::Postgres => "TIMESTAMPTZ",
                _ => "DATETIME",
            },
            SqlType::Timestamp => match dialect {
                Dialect::Postgres => "TIMESTAMP",
                _ => "DATETIME",
            },
            SqlType::Date => "DATE",
            SqlType::Time => "TIME",
            SqlType::Jsonb => match dialect {
                Dialect::Postgres => "JSONB",
                Dialect::Mysql => "JSON",
                _ => "TEXT",
            },
            SqlType::BigSerial => match dialect {
                Dialect::Postgres => "BIGSERIAL",
                Dialect::Mysql => "BIGINT AUTO_INCREMENT",
                _ => "INTEGER",
            },
            SqlType::Serial => match dialect {
                Dialect::Postgres => "SERIAL",
                Dialect::Mysql => "INT AUTO_INCREMENT",
                _ => "INTEGER",
            },
            SqlType::Custom(s) => s,
        }
    }
}

impl Default for SqlType {
    fn default() -> Self {
        SqlType::Text
    }
}

// ── Column attributes ───────────────────────────────────────────────

/// Metadata for a single column, built via the fluent `ColumnBuilder`.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: &'static str,
    pub sql_type: SqlType,
    pub primary_key: bool,
    pub auto_increment: bool,
    pub unique: bool,
    pub index: bool,
    pub nullable: bool,
    pub default: Option<String>,
}

/// Fluent builder for column attributes — fully autocompleted by rust-analyzer.
pub struct ColumnBuilder {
    def: ColumnDef,
}

impl ColumnBuilder {
    fn new(name: &'static str) -> Self {
        Self {
            def: ColumnDef {
                name,
                sql_type: SqlType::Text,
                primary_key: false,
                auto_increment: false,
                unique: false,
                index: false,
                nullable: false,
                default: None,
            },
        }
    }

    /// Set the SQL type for this column.
    pub fn sql_type(mut self, ty: SqlType) -> Self {
        self.def.sql_type = ty;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.def.primary_key = true;
        self
    }

    pub fn auto_increment(mut self) -> Self {
        self.def.auto_increment = true;
        self
    }

    pub fn unique(mut self) -> Self {
        self.def.unique = true;
        self
    }

    pub fn index(mut self) -> Self {
        self.def.index = true;
        self
    }

    pub fn nullable(mut self) -> Self {
        self.def.nullable = true;
        self
    }

    pub fn default(mut self, value: impl Into<String>) -> Self {
        self.def.default = Some(value.into());
        self
    }
}

// ── Index definition ────────────────────────────────────────────────

/// Type of index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexKind {
    /// Standard B-tree index.
    BTree,
    /// Hash index (PostgreSQL).
    Hash,
    /// GIN index for full-text / JSONB (PostgreSQL).
    Gin,
    /// GiST index for geometric / range types (PostgreSQL).
    Gist,
}

/// Definition of a table-level index (single or composite).
#[derive(Debug, Clone)]
pub struct IndexDef {
    /// Optional explicit name. Auto-generated if `None`.
    pub name: Option<String>,
    /// Columns included in this index, in order.
    pub columns: Vec<&'static str>,
    /// Whether this is a UNIQUE index.
    pub unique: bool,
    /// Index type.
    pub kind: IndexKind,
    /// Optional partial-index predicate (`WHERE ...` clause).
    ///
    /// Only supported by PostgreSQL.
    /// See: <https://www.postgresql.org/docs/current/indexes-partial.html>
    pub predicate: Option<String>,
}

/// Fluent builder for index definitions.
pub struct IndexBuilder {
    def: IndexDef,
}

impl IndexBuilder {
    fn new() -> Self {
        Self {
            def: IndexDef {
                name: None,
                columns: Vec::new(),
                unique: false,
                kind: IndexKind::BTree,
                predicate: None,
            },
        }
    }

    /// Set an explicit index name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.def.name = Some(name.into());
        self
    }

    /// Add a column to this index.
    pub fn column<M, T>(mut self, col: Column<M, T>) -> Self {
        self.def.columns.push(col.name);
        self
    }

    /// Mark this index as UNIQUE.
    pub fn unique(mut self) -> Self {
        self.def.unique = true;
        self
    }

    /// Set the index kind (default: BTree).
    pub fn kind(mut self, kind: IndexKind) -> Self {
        self.def.kind = kind;
        self
    }

    /// Shorthand: set index kind to Hash.
    pub fn hash(mut self) -> Self {
        self.def.kind = IndexKind::Hash;
        self
    }

    /// Shorthand: set index kind to GIN.
    pub fn gin(mut self) -> Self {
        self.def.kind = IndexKind::Gin;
        self
    }

    /// Shorthand: set index kind to GiST.
    pub fn gist(mut self) -> Self {
        self.def.kind = IndexKind::Gist;
        self
    }

    /// Set a partial-index predicate (PostgreSQL `WHERE ...` clause).
    ///
    /// ```ignore
    /// .predicate("status = 'active'")
    /// .predicate("deleted_at IS NULL")
    /// ```
    pub fn predicate(mut self, pred: impl Into<String>) -> Self {
        self.def.predicate = Some(pred.into());
        self
    }
}

// ── TableSchema ─────────────────────────────────────────────────────

/// Schema definition for a table, built via the fluent `table()` entry point.
#[derive(Debug, Clone)]
pub struct TableSchema<M> {
    pub name: &'static str,
    pub columns: Vec<ColumnDef>,
    pub indexes: Vec<IndexDef>,
    _model: PhantomData<M>,
}

impl<M> TableSchema<M> {
    /// Add a column with its attributes configured via a closure.
    ///
    /// ```ignore
    /// .column(User::id, |c| c.primary_key().auto_increment())
    /// ```
    pub fn column<T>(
        mut self,
        col: Column<M, T>,
        configure: impl FnOnce(ColumnBuilder) -> ColumnBuilder,
    ) -> Self {
        let builder = ColumnBuilder::new(col.name);
        self.columns.push(configure(builder).def);
        self
    }

    /// Add a table-level index via a closure.
    ///
    /// ```ignore
    /// .index(|idx| idx.column(User::email).column(User::role).unique())
    /// ```
    pub fn index(mut self, configure: impl FnOnce(IndexBuilder) -> IndexBuilder) -> Self {
        let builder = IndexBuilder::new();
        self.indexes.push(configure(builder).def);
        self
    }
}

/// Entry point: create a `TableSchema` for a model.
///
/// ```ignore
/// reify::table::<User>("users")
///     .column(User::id, |c| c.primary_key().auto_increment())
///     .index(|idx| idx.column(User::email).unique())
/// ```
pub fn table<M>(name: &'static str) -> TableSchema<M> {
    TableSchema {
        name,
        columns: Vec::new(),
        indexes: Vec::new(),
        _model: PhantomData,
    }
}

// ── Schema trait ────────────────────────────────────────────────────

/// Trait for defining table schema via the builder API (alternative to `#[column(...)]` attributes).
///
/// ```ignore
/// impl reify::Schema for User {
///     fn schema() -> reify::TableSchema<Self> {
///         reify::table::<Self>("users")
///             .column(User::id, |c| c.primary_key().auto_increment())
///             .column(User::email, |c| c.unique())
///             .column(User::role, |c| c.nullable())
///             .index(|idx| idx.column(User::email).unique())
///             .index(|idx| idx.column(User::email).column(User::role).name("idx_users_email_role"))
///     }
/// }
/// ```
pub trait Schema: crate::table::Table {
    fn schema() -> TableSchema<Self>;
}
