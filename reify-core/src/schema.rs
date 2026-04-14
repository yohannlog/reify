use std::borrow::Cow;
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
    /// `VARCHAR(n)` — variable-length string with a maximum length.
    ///
    /// Portable across all dialects. Prefer over `Custom("VARCHAR(255)")` for
    /// cross-dialect DDL generation.
    Varchar(u32),
    /// `CHAR(n)` — fixed-length string.
    Char(u32),
    /// `DECIMAL(precision, scale)` / `NUMERIC(precision, scale)`.
    ///
    /// Renders as `NUMERIC(p,s)` on PostgreSQL, `DECIMAL(p,s)` elsewhere.
    Decimal(u8, u8),
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
    ///
    /// Returns `Cow::Borrowed` for fixed types (zero-alloc) and
    /// `Cow::Owned` for parameterized types like `Varchar(255)`.
    pub fn to_sql(&self, dialect: Dialect) -> Cow<'static, str> {
        match self {
            SqlType::SmallInt => Cow::Borrowed("SMALLINT"),
            SqlType::Integer => Cow::Borrowed("INTEGER"),
            SqlType::BigInt => Cow::Borrowed("BIGINT"),
            SqlType::Float => match dialect {
                Dialect::Postgres => Cow::Borrowed("REAL"),
                _ => Cow::Borrowed("FLOAT"),
            },
            SqlType::Double => match dialect {
                Dialect::Postgres => Cow::Borrowed("DOUBLE PRECISION"),
                _ => Cow::Borrowed("DOUBLE"),
            },
            SqlType::Numeric => Cow::Borrowed("NUMERIC"),
            SqlType::Boolean => Cow::Borrowed("BOOLEAN"),
            SqlType::Text => Cow::Borrowed("TEXT"),
            SqlType::Varchar(len) => Cow::Owned(format!("VARCHAR({len})")),
            SqlType::Char(len) => Cow::Owned(format!("CHAR({len})")),
            SqlType::Decimal(p, s) => match dialect {
                Dialect::Postgres => Cow::Owned(format!("NUMERIC({p},{s})")),
                _ => Cow::Owned(format!("DECIMAL({p},{s})")),
            },
            SqlType::Bytea => match dialect {
                Dialect::Postgres => Cow::Borrowed("BYTEA"),
                Dialect::Mysql => Cow::Borrowed("LONGBLOB"),
                _ => Cow::Borrowed("BLOB"),
            },
            SqlType::Uuid => match dialect {
                Dialect::Postgres => Cow::Borrowed("UUID"),
                Dialect::Mysql => Cow::Borrowed("CHAR(36)"),
                _ => Cow::Borrowed("TEXT"),
            },
            SqlType::Timestamptz => match dialect {
                Dialect::Postgres => Cow::Borrowed("TIMESTAMPTZ"),
                _ => Cow::Borrowed("DATETIME"),
            },
            SqlType::Timestamp => match dialect {
                Dialect::Postgres => Cow::Borrowed("TIMESTAMP"),
                _ => Cow::Borrowed("DATETIME"),
            },
            SqlType::Date => Cow::Borrowed("DATE"),
            SqlType::Time => Cow::Borrowed("TIME"),
            SqlType::Jsonb => match dialect {
                Dialect::Postgres => Cow::Borrowed("JSONB"),
                Dialect::Mysql => Cow::Borrowed("JSON"),
                _ => Cow::Borrowed("TEXT"),
            },
            SqlType::BigSerial => match dialect {
                Dialect::Postgres => Cow::Borrowed("BIGSERIAL"),
                Dialect::Mysql => Cow::Borrowed("BIGINT AUTO_INCREMENT"),
                _ => Cow::Borrowed("INTEGER"),
            },
            SqlType::Serial => match dialect {
                Dialect::Postgres => Cow::Borrowed("SERIAL"),
                Dialect::Mysql => Cow::Borrowed("INT AUTO_INCREMENT"),
                _ => Cow::Borrowed("INTEGER"),
            },
            SqlType::Custom(s) => Cow::Borrowed(s),
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

// ── Foreign key action ─────────────────────────────────────────────

/// Action to perform on the referencing row when the referenced row is
/// deleted or updated.
///
/// Used in [`ForeignKeyDef`] via `#[column(on_delete = "...")]` /
/// `#[column(on_update = "...")]`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ForeignKeyAction {
    /// Do nothing — the default SQL behaviour.
    #[default]
    NoAction,
    /// Reject the operation if any referencing rows exist.
    Restrict,
    /// Automatically delete/update the referencing rows.
    Cascade,
    /// Set the referencing column(s) to `NULL`.
    SetNull,
    /// Set the referencing column(s) to their column default.
    SetDefault,
}

impl ForeignKeyAction {
    /// Return the SQL keyword for this action.
    pub fn as_sql(&self) -> &'static str {
        match self {
            ForeignKeyAction::NoAction => "NO ACTION",
            ForeignKeyAction::Restrict => "RESTRICT",
            ForeignKeyAction::Cascade => "CASCADE",
            ForeignKeyAction::SetNull => "SET NULL",
            ForeignKeyAction::SetDefault => "SET DEFAULT",
        }
    }

    /// Parse from a string (case-insensitive).
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "NO ACTION" | "NOACTION" => Some(ForeignKeyAction::NoAction),
            "RESTRICT" => Some(ForeignKeyAction::Restrict),
            "CASCADE" => Some(ForeignKeyAction::Cascade),
            "SET NULL" | "SETNULL" => Some(ForeignKeyAction::SetNull),
            "SET DEFAULT" | "SETDEFAULT" => Some(ForeignKeyAction::SetDefault),
            _ => None,
        }
    }
}

/// Describes a foreign-key constraint on a single column.
///
/// Produced by `#[column(references = "User::id")]` on a struct field.
///
/// # Example
/// ```ignore
/// #[column(references = "User::id", on_delete = "CASCADE")]
/// pub user_id: i64,
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyDef {
    /// Name of the referenced table (snake_case, e.g. `"users"`).
    pub references_table: String,
    /// Name of the referenced column (e.g. `"id"`).
    pub references_column: String,
    /// Action when the referenced row is deleted.
    pub on_delete: ForeignKeyAction,
    /// Action when the referenced row is updated.
    pub on_update: ForeignKeyAction,
}

impl ForeignKeyDef {
    /// Render the inline `REFERENCES` clause for DDL.
    ///
    /// Example output: `REFERENCES "users" ("id") ON DELETE CASCADE`
    pub fn to_references_clause(&self) -> String {
        use crate::ident::qi;
        let mut s = format!(
            "REFERENCES {} ({})",
            qi(&self.references_table),
            qi(&self.references_column)
        );
        if self.on_delete != ForeignKeyAction::NoAction {
            s.push_str(&format!(" ON DELETE {}", self.on_delete.as_sql()));
        }
        if self.on_update != ForeignKeyAction::NoAction {
            s.push_str(&format!(" ON UPDATE {}", self.on_update.as_sql()));
        }
        s
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
    /// When set, the column is computed and excluded from INSERT/UPDATE.
    pub computed: Option<ComputedColumn>,
    /// Auto-managed timestamp role (`Creation` or `Update`).
    pub timestamp_kind: Option<TimestampKind>,
    /// Where the timestamp value comes from (`Vm` or `Db`).
    pub timestamp_source: TimestampSource,
    /// Optional SQL CHECK constraint expression.
    ///
    /// Rendered as `CHECK (expr)` inline after the column definition in DDL.
    ///
    /// ```ignore
    /// .column(Product::price, |c| c.check("price >= 0"))
    /// // → price DECIMAL(10,2) NOT NULL CHECK (price >= 0)
    /// ```
    pub check: Option<String>,
    /// Optional foreign-key constraint for this column.
    ///
    /// Set via `#[column(references = "Table::column")]` on a struct field.
    pub foreign_key: Option<ForeignKeyDef>,
}

// ── Type-state for timestamp builder ────────────────────────────────

/// Sealed trait for the timestamp state of a [`ColumnBuilder`].
///
/// Only [`NoTimestamp`] and [`HasTimestamp`] implement this trait.
#[doc(hidden)]
pub trait TimestampState: ts_state::Sealed {}

mod ts_state {
    #[doc(hidden)]
    pub trait Sealed {}
    impl Sealed for super::NoTimestamp {}
    impl Sealed for super::HasTimestamp {}
}

/// Marker type: the column builder has not yet been configured as a timestamp.
///
/// This is the default state for [`ColumnBuilder`].
pub struct NoTimestamp;

/// Marker type: the column builder has been configured as a timestamp via
/// [`.creation_timestamp()`](ColumnBuilder::creation_timestamp) or
/// [`.update_timestamp()`](ColumnBuilder::update_timestamp).
///
/// Only in this state is [`.source_db()`](ColumnBuilder::source_db) available.
pub struct HasTimestamp;

impl TimestampState for NoTimestamp {}
impl TimestampState for HasTimestamp {}

/// Fluent builder for column attributes — fully autocompleted by rust-analyzer.
///
/// `T` is the Rust type of the column field; `S` is the timestamp state
/// ([`NoTimestamp`] or [`HasTimestamp`]).  Both parameters are inferred
/// automatically — you never need to write them explicitly.
pub struct ColumnBuilder<T, S: TimestampState = NoTimestamp> {
    def: ColumnDef,
    _type: PhantomData<T>,
    _state: PhantomData<S>,
}

impl<T> ColumnBuilder<T, NoTimestamp> {
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
                check: None,
                foreign_key: None,
            },
            _type: PhantomData,
            _state: PhantomData,
        }
    }

    /// Public constructor for use by `ViewSchema` and other builders.
    pub fn new_pub(name: &'static str) -> Self {
        Self::new(name)
    }
}

impl<T, S: TimestampState> ColumnBuilder<T, S> {
    /// Consume the builder and return the `ColumnDef`.
    pub fn build(self) -> ColumnDef {
        self.def
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
    /// Only available when `T: Temporal` (requires `postgres` or `mysql` feature).
    /// Transitions the builder state to [`HasTimestamp`], enabling `.source_db()`.
    ///
    /// ```ignore
    /// .column(User::created_at, |c| c.creation_timestamp())
    /// .column(User::created_at, |c| c.creation_timestamp().source_db())
    /// ```
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    pub fn creation_timestamp(mut self) -> ColumnBuilder<T, HasTimestamp>
    where
        T: crate::column::Temporal,
    {
        self.def.timestamp_kind = Some(TimestampKind::Creation);
        ColumnBuilder {
            def: self.def,
            _type: PhantomData,
            _state: PhantomData,
        }
    }

    /// Mark as an update timestamp — set on INSERT **and** every UPDATE.
    ///
    /// Only available when `T: Temporal` (requires `postgres` or `mysql` feature).
    /// Transitions the builder state to [`HasTimestamp`], enabling `.source_db()`.
    ///
    /// ```ignore
    /// .column(User::updated_at, |c| c.update_timestamp())
    /// .column(User::updated_at, |c| c.update_timestamp().source_db())
    /// ```
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    pub fn update_timestamp(mut self) -> ColumnBuilder<T, HasTimestamp>
    where
        T: crate::column::Temporal,
    {
        self.def.timestamp_kind = Some(TimestampKind::Update);
        ColumnBuilder {
            def: self.def,
            _type: PhantomData,
            _state: PhantomData,
        }
    }

    /// Set the column type to `VARCHAR(length)`.
    ///
    /// ```ignore
    /// .column(User::name, |c| c.varchar(255))
    /// ```
    pub fn varchar(mut self, length: u32) -> Self {
        self.def.sql_type = SqlType::Varchar(length);
        self
    }

    /// Set the column type to `CHAR(length)`.
    ///
    /// ```ignore
    /// .column(Product::currency_code, |c| c.char_type(3))
    /// ```
    pub fn char_type(mut self, length: u32) -> Self {
        self.def.sql_type = SqlType::Char(length);
        self
    }

    /// Set the column type to `DECIMAL(precision, scale)`.
    ///
    /// Renders as `NUMERIC(p,s)` on PostgreSQL, `DECIMAL(p,s)` elsewhere.
    ///
    /// ```ignore
    /// .column(Product::price, |c| c.decimal(10, 2))
    /// ```
    pub fn decimal(mut self, precision: u8, scale: u8) -> Self {
        self.def.sql_type = SqlType::Decimal(precision, scale);
        self
    }

    /// Add a SQL `CHECK` constraint to this column.
    ///
    /// The expression is rendered inline as `CHECK (expr)` in DDL.
    ///
    /// ```ignore
    /// .column(Product::price, |c| c.decimal(10, 2).check("price >= 0"))
    /// // → price DECIMAL(10,2) NOT NULL CHECK (price >= 0)
    /// ```
    pub fn check(mut self, expr: impl Into<String>) -> Self {
        self.def.check = Some(expr.into());
        self
    }

    /// Add a foreign-key constraint to this column.
    ///
    /// ```ignore
    /// .column(Post::user_id, |c| c.references("users", "id").on_delete(ForeignKeyAction::Cascade))
    /// ```
    pub fn references(mut self, table: impl Into<String>, column: impl Into<String>) -> Self {
        self.def.foreign_key = Some(ForeignKeyDef {
            references_table: table.into(),
            references_column: column.into(),
            on_delete: ForeignKeyAction::NoAction,
            on_update: ForeignKeyAction::NoAction,
        });
        self
    }

    /// Set the `ON DELETE` action for the foreign-key constraint on this column.
    ///
    /// Has no effect if `.references()` was not called first.
    pub fn on_delete(mut self, action: ForeignKeyAction) -> Self {
        if let Some(ref mut fk) = self.def.foreign_key {
            fk.on_delete = action;
        }
        self
    }

    /// Set the `ON UPDATE` action for the foreign-key constraint on this column.
    ///
    /// Has no effect if `.references()` was not called first.
    pub fn on_update(mut self, action: ForeignKeyAction) -> Self {
        if let Some(ref mut fk) = self.def.foreign_key {
            fk.on_update = action;
        }
        self
    }
}

impl<T> ColumnBuilder<T, HasTimestamp> {
    /// Use the database as the source of the current date/time.
    ///
    /// Only available after calling `.creation_timestamp()` or `.update_timestamp()`.
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

/// Sort direction for an index column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    /// Ascending order (default).
    Asc,
    /// Descending order.
    Desc,
}

impl std::fmt::Display for SortDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SortDirection::Asc => write!(f, "ASC"),
            SortDirection::Desc => write!(f, "DESC"),
        }
    }
}

/// A column within an index, with its sort direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexColumnDef {
    /// Column name.
    pub name: &'static str,
    /// Sort direction (ASC or DESC).
    pub direction: SortDirection,
}

impl IndexColumnDef {
    /// Create a new index column with the given direction.
    pub fn new(name: &'static str, direction: SortDirection) -> Self {
        Self { name, direction }
    }

    /// Create an ascending index column.
    pub fn asc(name: &'static str) -> Self {
        Self {
            name,
            direction: SortDirection::Asc,
        }
    }

    /// Create a descending index column.
    pub fn desc(name: &'static str) -> Self {
        Self {
            name,
            direction: SortDirection::Desc,
        }
    }
}

/// Definition of a table-level index (single or composite).
#[derive(Debug, Clone)]
pub struct IndexDef {
    /// Optional explicit name. Auto-generated if `None`.
    pub name: Option<String>,
    /// Columns included in this index, in order, with sort direction.
    pub columns: Vec<IndexColumnDef>,
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

    /// Add a column to this index (ascending by default).
    pub fn column<M, T>(mut self, col: Column<M, T>) -> Self {
        self.def.columns.push(IndexColumnDef::asc(col.name));
        self
    }

    /// Add a column with ascending sort order.
    pub fn column_asc<M, T>(mut self, col: Column<M, T>) -> Self {
        self.def.columns.push(IndexColumnDef::asc(col.name));
        self
    }

    /// Add a column with descending sort order.
    pub fn column_desc<M, T>(mut self, col: Column<M, T>) -> Self {
        self.def.columns.push(IndexColumnDef::desc(col.name));
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
    /// Table-level CHECK constraints.
    ///
    /// Each entry is a raw SQL expression rendered as `CHECK (expr)` at the
    /// end of the `CREATE TABLE` column list.
    pub checks: Vec<String>,
    _model: PhantomData<M>,
}

impl<M> TableSchema<M> {
    /// Add a column with its attributes configured via a closure.
    ///
    /// ```ignore
    /// .column(User::id, |c| c.primary_key().auto_increment())
    /// ```
    pub fn column<T, S: TimestampState>(
        mut self,
        col: Column<M, T>,
        configure: impl FnOnce(ColumnBuilder<T, NoTimestamp>) -> ColumnBuilder<T, S>,
    ) -> Self {
        let builder = ColumnBuilder::<T, NoTimestamp>::new(col.name);
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

    /// Add a table-level `CHECK` constraint.
    ///
    /// The expression is rendered as a separate `CHECK (expr)` line at the
    /// end of the `CREATE TABLE` column list.
    ///
    /// ```ignore
    /// reify::table::<Event>("events")
    ///     .column(Event::start_date, |c| c)
    ///     .column(Event::end_date, |c| c)
    ///     .check("start_date < end_date")
    /// ```
    pub fn check(mut self, expr: impl Into<String>) -> Self {
        self.checks.push(expr.into());
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
        checks: Vec::new(),
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
