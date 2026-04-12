use std::marker::PhantomData;

use crate::column::Column;

// ── Column attributes ───────────────────────────────────────────────

/// Metadata for a single column, built via the fluent `ColumnBuilder`.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: &'static str,
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
                primary_key: false,
                auto_increment: false,
                unique: false,
                index: false,
                nullable: false,
                default: None,
            },
        }
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
