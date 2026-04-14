use super::{
    BuildError, Dialect, OnConflict, rewrite_placeholders_pg, trace_query, write_on_conflict,
    write_returning,
};
use crate::condition::Condition;
use crate::ident::qi;
use crate::sql::{ToSql, write_joined};
use crate::table::Table;
use crate::value::Value;
use std::fmt::Write;
use std::marker::PhantomData;
use tracing::debug;

// ── InsertBuilder ───────────────────────────────────────────────────

/// A fluent builder for `INSERT` statements.
///
/// Obtain one via the generated `Model::insert(&model)` method.
///
/// # Example
///
/// ```ignore
/// let (sql, params) = User::insert(&alice)
///     .on_conflict_do_nothing()
///     .build();
/// // INSERT INTO users (name, email) VALUES (?, ?)
/// ```
pub struct InsertBuilder<M: Table> {
    values: Vec<Value>,
    on_conflict: Option<OnConflict>,
    #[cfg(feature = "postgres")]
    returning: Option<Vec<&'static str>>,
    _model: PhantomData<M>,
}

impl<M: Table> InsertBuilder<M> {
    /// Create a builder from a model instance.
    ///
    /// Extracts the writable column values from `model`.
    pub fn new(model: &M) -> Self {
        Self {
            values: model.writable_values(),
            on_conflict: None,
            #[cfg(feature = "postgres")]
            returning: None,
            _model: PhantomData,
        }
    }

    /// Append a `RETURNING` clause (PostgreSQL only).
    ///
    /// ```ignore
    /// let (sql, params) = User::insert(&alice).returning(&["id", "email"]).build();
    /// // INSERT INTO users (id, email, role) VALUES (?, ?, ?) RETURNING id, email
    /// ```
    #[cfg(feature = "postgres")]
    pub fn returning(mut self, cols: &[&'static str]) -> Self {
        self.returning = Some(cols.to_vec());
        self
    }

    /// On conflict, do nothing.
    ///
    /// - PostgreSQL: `ON CONFLICT DO NOTHING`
    /// - MySQL: `INSERT IGNORE …`
    pub fn on_conflict_do_nothing(mut self) -> Self {
        self.on_conflict = Some(OnConflict::DoNothing);
        self
    }

    /// On conflict on `target_cols`, update `updates`.
    ///
    /// - PostgreSQL: `ON CONFLICT (target_cols) DO UPDATE SET col = EXCLUDED.col, …`
    /// - MySQL: `ON DUPLICATE KEY UPDATE col = VALUES(col), …`
    pub fn on_conflict_do_update(
        mut self,
        target_cols: &[&'static str],
        updates: &[&'static str],
    ) -> Self {
        self.on_conflict = Some(OnConflict::DoUpdate {
            target_cols: target_cols.to_vec(),
            updates: updates.to_vec(),
        });
        self
    }

    /// Build with the default (generic) dialect — no upsert extensions.
    #[allow(unused_mut)]
    pub fn build(&self) -> (String, Vec<Value>) {
        self.build_with_dialect(Dialect::Generic)
    }

    /// Build SQL for a specific [`Dialect`].
    #[allow(unused_mut)]
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let col_names = M::writable_column_names();
        let num_cols = self.values.len();

        // MySQL INSERT IGNORE prefix
        let insert_kw = match (&self.on_conflict, dialect) {
            (Some(OnConflict::DoNothing), Dialect::Mysql) => "INSERT IGNORE",
            _ => "INSERT",
        };

        let mut sql = String::with_capacity(64 + num_cols * 3);
        let _ = write!(sql, "{insert_kw} INTO {} (", qi(M::table_name()));
        write_joined(&mut sql, &col_names, ", ", |buf, c| buf.push_str(&qi(c)));
        sql.push_str(") VALUES (");
        for i in 0..num_cols {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push('?');
        }
        sql.push(')');

        // Conflict clause
        write_on_conflict(&mut sql, &self.on_conflict, dialect);

        #[cfg(feature = "postgres")]
        write_returning(&mut sql, &self.returning);

        trace_query("insert", M::table_name(), &sql, &self.values);
        (sql, self.values.clone())
    }
}

// ── InsertBuilder direct execution methods ─────────────────────────

impl<M: Table> InsertBuilder<M> {
    /// Execute this INSERT statement.
    ///
    /// ```ignore
    /// let affected = User::insert(&alice).execute(&db).await?;
    /// ```
    pub async fn execute(&self, db: &impl crate::db::Database) -> Result<u64, crate::db::DbError> {
        crate::db::insert(db, self).await
    }

    /// Execute this INSERT … RETURNING and return typed results (PostgreSQL only).
    #[cfg(feature = "postgres")]
    pub async fn fetch(&self, db: &impl crate::db::Database) -> Result<Vec<M>, crate::db::DbError>
    where
        M: crate::db::FromRow,
    {
        crate::db::insert_returning(db, self).await
    }
}

// ── InsertManyBuilder ────────────────────────────────────────────────

/// Builds a multi-row `INSERT INTO … VALUES (…), (…), …` statement.
///
/// Obtain one via the generated `Model::insert_many(&[…])` method.
pub struct InsertManyBuilder<M: Table> {
    /// Flat list of all values: row0_col0, row0_col1, …, rowN_colM.
    rows: Vec<Vec<Value>>,
    on_conflict: Option<OnConflict>,
    #[cfg(feature = "postgres")]
    returning: Option<Vec<&'static str>>,
    _model: PhantomData<M>,
}

impl<M: Table> InsertManyBuilder<M> {
    /// Create a builder from a slice of model instances.
    ///
    /// # Panics
    ///
    /// Panics if `models` is empty — an empty INSERT is a logic error.
    /// Use [`try_new`](Self::try_new) for a non-panicking alternative.
    pub fn new(models: &[M]) -> Self {
        Self::try_new(models).expect("insert_many requires at least one row")
    }

    /// Create a builder from a slice of model instances.
    ///
    /// Returns `Err(BuildError::EmptyInsert)` if `models` is empty.
    pub fn try_new(models: &[M]) -> Result<Self, BuildError> {
        if models.is_empty() {
            return Err(BuildError::EmptyInsert);
        }
        Ok(Self {
            rows: models.iter().map(|m| m.writable_values()).collect(),
            on_conflict: None,
            #[cfg(feature = "postgres")]
            returning: None,
            _model: PhantomData,
        })
    }

    /// Append a `RETURNING` clause (PostgreSQL only).
    #[cfg(feature = "postgres")]
    pub fn returning(mut self, cols: &[&'static str]) -> Self {
        self.returning = Some(cols.to_vec());
        self
    }

    /// On conflict, do nothing.
    pub fn on_conflict_do_nothing(mut self) -> Self {
        self.on_conflict = Some(OnConflict::DoNothing);
        self
    }

    /// On conflict on `target_cols`, update `updates`.
    pub fn on_conflict_do_update(
        mut self,
        target_cols: &[&'static str],
        updates: &[&'static str],
    ) -> Self {
        self.on_conflict = Some(OnConflict::DoUpdate {
            target_cols: target_cols.to_vec(),
            updates: updates.to_vec(),
        });
        self
    }

    /// Build with the default (generic) dialect.
    pub fn build(&self) -> (String, Vec<Value>) {
        self.build_with_dialect(Dialect::Generic)
    }

    /// Build SQL for a specific [`Dialect`].
    #[allow(unused_mut)]
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        let col_names = M::writable_column_names();
        let num_cols = col_names.len();
        let num_rows = self.rows.len();

        // Flatten all row values into a single params vec.
        let params: Vec<Value> = self.rows.iter().flat_map(|r| r.iter().cloned()).collect();

        let insert_kw = match (&self.on_conflict, dialect) {
            (Some(OnConflict::DoNothing), Dialect::Mysql) => "INSERT IGNORE",
            _ => "INSERT",
        };

        // Capacity: keyword + table + cols + VALUES rows
        let mut sql = String::with_capacity(64 + num_cols * 3 + num_rows * (num_cols * 3 + 4));
        let _ = write!(sql, "{insert_kw} INTO {} (", qi(M::table_name()));
        write_joined(&mut sql, &col_names, ", ", |buf, c| buf.push_str(&qi(c)));
        sql.push_str(") VALUES ");

        // Write (?, ?, ?), (?, ?, ?), … directly
        for row_idx in 0..num_rows {
            if row_idx > 0 {
                sql.push_str(", ");
            }
            sql.push('(');
            for col_idx in 0..num_cols {
                if col_idx > 0 {
                    sql.push_str(", ");
                }
                sql.push('?');
            }
            sql.push(')');
        }

        // Conflict clause
        write_on_conflict(&mut sql, &self.on_conflict, dialect);

        #[cfg(feature = "postgres")]
        write_returning(&mut sql, &self.returning);

        trace_query("insert_many", M::table_name(), &sql, &params);
        (sql, params)
    }
}

// ── InsertManyBuilder direct execution methods ───────────────────────

impl<M: Table> InsertManyBuilder<M> {
    /// Execute this batch INSERT statement.
    ///
    /// ```ignore
    /// let affected = User::insert_many(&users).execute(&db).await?;
    /// ```
    pub async fn execute(&self, db: &impl crate::db::Database) -> Result<u64, crate::db::DbError> {
        crate::db::insert_many(db, self).await
    }

    /// Execute this batch INSERT … RETURNING and return typed results (PostgreSQL only).
    #[cfg(feature = "postgres")]
    pub async fn fetch(&self, db: &impl crate::db::Database) -> Result<Vec<M>, crate::db::DbError>
    where
        M: crate::db::FromRow,
    {
        crate::db::insert_many_returning(db, self).await
    }
}
