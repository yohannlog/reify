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

// ── Timestamp generation ────────────────────────────────────────────

/// Identifies the role of an auto-managed timestamp column.
///
/// Equivalent to Hibernate's `@CreationTimestamp` / `@UpdateTimestamp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampKind {
    /// Set once on INSERT (like Hibernate's `@CreationTimestamp`).
    Creation,
    /// Set on INSERT **and** every UPDATE (like Hibernate's `@UpdateTimestamp`).
    Update,
}

/// Where the current date/time value comes from.
///
/// Equivalent to Hibernate's `CurrentTimestampGeneration` source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampSource {
    /// Application-side: Rust generates `Utc::now()` and binds it as a parameter.
    ///
    /// This is the default — the ORM controls the value, ensuring consistency
    /// across all database backends.
    #[default]
    Vm,
    /// Database-side: the column uses `DEFAULT NOW()` / `CURRENT_TIMESTAMP`.
    ///
    /// The column is excluded from INSERT/UPDATE parameter lists and the
    /// database engine provides the value. For `Update` + `Db` on MySQL,
    /// `ON UPDATE CURRENT_TIMESTAMP` is emitted in DDL.
    Db,
}

// ── Computed column ─────────────────────────────────────────────────

/// Describes how a column value is computed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComputedColumn {
    /// Database-generated column: `GENERATED ALWAYS AS (expr) STORED`.
    ///
    /// The expression is raw SQL evaluated by the database engine.
    /// The column exists in the table but is never included in INSERT/UPDATE.
    ///
    /// ```ignore
    /// #[column(computed = "first_name || ' ' || last_name")]
    /// pub full_name: String,
    /// ```
    Stored(String),

    /// Rust-side virtual column: computed in application code after fetch.
    ///
    /// The column does **not** exist in the database at all — it is populated
    /// by a closure or `Default` after the row is read.
    ///
    /// ```ignore
    /// #[column(computed_rust)]
    /// pub display_name: String,
    /// ```
    Virtual,
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
    /// When set, the column is computed and excluded from INSERT/UPDATE.
    pub computed: Option<ComputedColumn>,
    /// Auto-managed timestamp role (`Creation` or `Update`).
    pub timestamp_kind: Option<TimestampKind>,
    /// Where the timestamp value comes from (`Vm` or `Db`).
    pub timestamp_source: TimestampSource,
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
                computed: None,
                timestamp_kind: None,
                timestamp_source: TimestampSource::Vm,
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

    /// Mark this column as a database-generated computed column.
    ///
    /// The column will be defined as `GENERATED ALWAYS AS (expr) STORED`
    /// and excluded from INSERT/UPDATE statements.
    pub fn computed_stored(mut self, expr: impl Into<String>) -> Self {
        self.def.computed = Some(ComputedColumn::Stored(expr.into()));
        self
    }

    /// Mark this column as a Rust-side virtual column.
    ///
    /// The column will not exist in the database — it is computed in
    /// application code after fetching.
    pub fn computed_virtual(mut self) -> Self {
        self.def.computed = Some(ComputedColumn::Virtual);
        self
    }

    /// Mark as a creation timestamp — set once on INSERT.
    ///
    /// By default uses `Vm` source (Rust-side `Utc::now()`). Chain
    /// `.source_db()` to let the database provide the value instead.
    ///
    /// ```ignore
    /// .column(User::created_at, |c| c.creation_timestamp())
    /// .column(User::created_at, |c| c.creation_timestamp().source_db())
    /// ```
    pub fn creation_timestamp(mut self) -> Self {
        self.def.timestamp_kind = Some(TimestampKind::Creation);
        self
    }

    /// Mark as an update timestamp — set on INSERT **and** every UPDATE.
    ///
    /// By default uses `Vm` source (Rust-side `Utc::now()`). Chain
    /// `.source_db()` to let the database provide the value instead.
    ///
    /// ```ignore
    /// .column(User::updated_at, |c| c.update_timestamp())
    /// .column(User::updated_at, |c| c.update_timestamp().source_db())
    /// ```
    pub fn update_timestamp(mut self) -> Self {
        self.def.timestamp_kind = Some(TimestampKind::Update);
        self
    }

    /// Use the database as the source of the current date/time.
    ///
    /// The column will get `DEFAULT NOW()` / `CURRENT_TIMESTAMP` in DDL
    /// and be excluded from INSERT/UPDATE parameter lists.
    pub fn source_db(mut self) -> Self {
        self.def.timestamp_source = TimestampSource::Db;
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
