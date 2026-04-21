use super::{BuildError, Dialect, OnConflict, trace_query, write_on_conflict};
#[cfg(feature = "postgres")]
use super::{rewrite_placeholders_pg, write_returning};

/// Error returned when a statement would exceed the database's bind-parameter
/// limit. Recover by calling [`InsertManyBuilder::build_chunked`] (or
/// `build_chunked_pg`) which splits rows across multiple statements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParamLimitExceeded {
    pub dialect: Dialect,
    pub limit: usize,
    pub requested: usize,
    pub num_cols: usize,
    pub num_rows: usize,
}

impl std::fmt::Display for ParamLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "insert_many with {} rows × {} columns = {} bind parameters exceeds {:?} limit of {}. Use .build_chunked(dialect) to split across multiple statements.",
            self.num_rows, self.num_cols, self.requested, self.dialect, self.limit
        )
    }
}

impl std::error::Error for ParamLimitExceeded {}
use crate::column::Column;
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
    #[cfg(feature = "postgres18")]
    returning_old_new: Option<super::ReturningOldNew>,
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
            #[cfg(feature = "postgres18")]
            returning_old_new: None,
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

    /// Append a `RETURNING` clause using typed [`Column`] references (PostgreSQL only).
    #[cfg(feature = "postgres")]
    pub fn returning_cols<T>(mut self, cols: &[Column<M, T>]) -> Self {
        self.returning = Some(cols.iter().map(|c| c.name).collect());
        self
    }

    /// Append `RETURNING new.*` clause (PostgreSQL 18+).
    ///
    /// Returns the inserted row state (INSERT has no `old` state).
    #[cfg(feature = "postgres18")]
    pub fn returning_new_all(mut self) -> Self {
        self.returning_old_new = Some(super::ReturningOldNew::New);
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

    /// Build a [`crate::BuiltQuery`] with `$N` placeholders and PostgreSQL upsert syntax (PostgreSQL only).
    ///
    /// Uses `build_with_dialect(Dialect::Postgres)` so that `ON CONFLICT … DO UPDATE SET`
    /// syntax is generated correctly, then rewrites `?` → `$N` once at build time.
    #[cfg(feature = "postgres")]
    pub fn build_pg(&self) -> crate::built_query::BuiltQuery {
        let (sql, params) = self.build_with_dialect(Dialect::Postgres);
        let pg_sql = rewrite_placeholders_pg(&sql);
        crate::built_query::BuiltQuery::new(pg_sql, params)
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

        #[cfg(feature = "postgres18")]
        if let Some(mode) = self.returning_old_new {
            super::write_returning_old_new(&mut sql, mode, M::table_name());
        }

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

    /// Execute this INSERT … RETURNING new.* and return `OldNew<M>` results (PostgreSQL 18+).
    ///
    /// Requires `.returning_new_all()` to be called first.
    #[cfg(feature = "postgres18")]
    pub async fn fetch_new(
        &self,
        db: &impl crate::db::Database,
    ) -> Result<Vec<crate::db::OldNew<M>>, crate::db::DbError>
    where
        M: crate::db::FromRowPositional,
    {
        crate::db::insert_returning_new(db, self).await
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
    #[cfg(feature = "postgres18")]
    returning_old_new: Option<super::ReturningOldNew>,
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
            #[cfg(feature = "postgres18")]
            returning_old_new: None,
            _model: PhantomData,
        })
    }

    /// Append a `RETURNING` clause (PostgreSQL only).
    #[cfg(feature = "postgres")]
    pub fn returning(mut self, cols: &[&'static str]) -> Self {
        self.returning = Some(cols.to_vec());
        self
    }

    /// Append `RETURNING new.*` clause (PostgreSQL 18+).
    ///
    /// Returns the inserted row states (INSERT has no `old` state).
    #[cfg(feature = "postgres18")]
    pub fn returning_new_all(mut self) -> Self {
        self.returning_old_new = Some(super::ReturningOldNew::New);
        self
    }

    /// Append a `RETURNING` clause using typed [`Column`] references (PostgreSQL only).
    #[cfg(feature = "postgres")]
    pub fn returning_cols<T>(mut self, cols: &[Column<M, T>]) -> Self {
        self.returning = Some(cols.iter().map(|c| c.name).collect());
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

    /// Build a [`crate::BuiltQuery`] with `$N` placeholders and PostgreSQL upsert syntax (PostgreSQL only).
    ///
    /// Uses `build_with_dialect(Dialect::Postgres)` so that `ON CONFLICT … DO UPDATE SET`
    /// syntax is generated correctly, then rewrites `?` → `$N` once at build time.
    #[cfg(feature = "postgres")]
    pub fn build_pg(&self) -> crate::built_query::BuiltQuery {
        let (sql, params) = self.build_with_dialect(Dialect::Postgres);
        let pg_sql = rewrite_placeholders_pg(&sql);
        crate::built_query::BuiltQuery::new(pg_sql, params)
    }

    /// Build SQL for a specific [`Dialect`].
    ///
    /// # Panics
    ///
    /// Panics if `num_rows × num_cols` exceeds `dialect.max_params()`.
    /// Use [`try_build_with_dialect`](Self::try_build_with_dialect) to get a
    /// recoverable error, or [`build_chunked`](Self::build_chunked) to split
    /// the statement automatically.
    #[allow(unused_mut)]
    pub fn build_with_dialect(&self, dialect: Dialect) -> (String, Vec<Value>) {
        self.try_build_with_dialect(dialect)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    /// Build SQL for a specific [`Dialect`], returning [`crate::ParamLimitExceeded`]
    /// if the bind-parameter limit would be violated.
    #[allow(unused_mut)]
    pub fn try_build_with_dialect(
        &self,
        dialect: Dialect,
    ) -> Result<(String, Vec<Value>), ParamLimitExceeded> {
        let col_names = M::writable_column_names();
        let num_cols = col_names.len();
        let num_rows = self.rows.len();

        let requested = num_cols.saturating_mul(num_rows);
        let limit = dialect.max_params();
        if requested > limit {
            return Err(ParamLimitExceeded {
                dialect,
                limit,
                requested,
                num_cols,
                num_rows,
            });
        }

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

        #[cfg(feature = "postgres18")]
        if let Some(mode) = self.returning_old_new {
            super::write_returning_old_new(&mut sql, mode, M::table_name());
        }

        trace_query("insert_many", M::table_name(), &sql, &params);
        Ok((sql, params))
    }
}

// ── Chunked build methods ────────────────────────────────────────────

impl<M: Table> InsertManyBuilder<M> {
    /// Maximum rows per chunk for a given dialect, based on its parameter limit.
    ///
    /// Returns `usize::MAX` when the data fits in a single statement.
    fn rows_per_chunk(&self, dialect: Dialect) -> usize {
        let num_cols = M::writable_column_names().len();
        if num_cols == 0 {
            return usize::MAX;
        }
        dialect.max_params() / num_cols
    }

    /// Build one `(sql, params)` tuple for a contiguous slice of rows.
    #[allow(unused_mut)]
    fn build_chunk(&self, rows: &[Vec<Value>], dialect: Dialect) -> (String, Vec<Value>) {
        let col_names = M::writable_column_names();
        let num_cols = col_names.len();
        let num_rows = rows.len();

        let params: Vec<Value> = rows.iter().flat_map(|r| r.iter().cloned()).collect();

        let insert_kw = match (&self.on_conflict, dialect) {
            (Some(OnConflict::DoNothing), Dialect::Mysql) => "INSERT IGNORE",
            _ => "INSERT",
        };

        let mut sql = String::with_capacity(64 + num_cols * 3 + num_rows * (num_cols * 3 + 4));
        let _ = write!(sql, "{insert_kw} INTO {} (", qi(M::table_name()));
        write_joined(&mut sql, &col_names, ", ", |buf, c| buf.push_str(&qi(c)));
        sql.push_str(") VALUES ");

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

        write_on_conflict(&mut sql, &self.on_conflict, dialect);

        #[cfg(feature = "postgres")]
        write_returning(&mut sql, &self.returning);

        #[cfg(feature = "postgres18")]
        if let Some(mode) = self.returning_old_new {
            super::write_returning_old_new(&mut sql, mode, M::table_name());
        }

        trace_query("insert_many_chunk", M::table_name(), &sql, &params);
        (sql, params)
    }

    /// Split the rows into chunks that respect the dialect's parameter limit
    /// and build one `(sql, params)` pair per chunk.
    ///
    /// If all rows fit in a single statement, returns a `Vec` with one element
    /// (identical to calling [`build_with_dialect`](Self::build_with_dialect)).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let chunks = User::insert_many(&huge_vec).build_chunked(Dialect::Postgres);
    /// for (sql, params) in &chunks {
    ///     db.execute(sql, params).await?;
    /// }
    /// ```
    pub fn build_chunked(&self, dialect: Dialect) -> Vec<(String, Vec<Value>)> {
        let chunk_size = self.rows_per_chunk(dialect);
        if chunk_size >= self.rows.len() {
            return vec![self.build_with_dialect(dialect)];
        }
        self.rows
            .chunks(chunk_size)
            .map(|chunk| self.build_chunk(chunk, dialect))
            .collect()
    }

    /// Build chunked queries with PostgreSQL `$N` placeholders.
    ///
    /// Each chunk respects the 65 535 parameter limit. Returns one
    /// [`crate::BuiltQuery`] per chunk.
    ///
    /// # Performance note
    ///
    /// All full-size chunks share the same SQL shape, so only the first
    /// (and the last, if it differs in row count) require a placeholder
    /// rewrite scan. The rest reuse the first chunk's rewritten SQL directly.
    #[cfg(feature = "postgres")]
    pub fn build_chunked_pg(&self) -> Vec<crate::built_query::BuiltQuery> {
        let chunks = self.build_chunked(Dialect::Postgres);
        if chunks.is_empty() {
            return vec![];
        }

        // Single chunk — simple path.
        if chunks.len() == 1 {
            let (sql, params) = chunks.into_iter().next().unwrap();
            let pg_sql = rewrite_placeholders_pg(&sql);
            return vec![crate::built_query::BuiltQuery::new(pg_sql, params)];
        }

        let total_rows = self.rows.len();
        let chunk_size = self.rows_per_chunk(Dialect::Postgres);
        let last_chunk_rows = total_rows % chunk_size;
        // If evenly divisible, every chunk is full-size; otherwise the last one is partial.
        let last_is_partial = last_chunk_rows != 0;

        // Rewrite the first chunk once — all full-size chunks share this SQL.
        let first_pg_sql = rewrite_placeholders_pg(&chunks[0].0);
        let chunks_len = chunks.len();

        chunks
            .into_iter()
            .enumerate()
            .map(|(idx, (sql, params))| {
                let is_last = idx == chunks_len - 1;
                let pg_sql = if is_last && last_is_partial {
                    // Last chunk has fewer rows (and placeholders) — needs its own rewrite.
                    rewrite_placeholders_pg(&sql)
                } else {
                    // Full-size chunk — identical SQL shape as the first chunk.
                    first_pg_sql.clone()
                };
                crate::built_query::BuiltQuery::new(pg_sql, params)
            })
            .collect()
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

    /// Execute this batch INSERT … RETURNING new.* and return `OldNew<M>` results (PostgreSQL 18+).
    ///
    /// Requires `.returning_new_all()` to be called first.
    #[cfg(feature = "postgres18")]
    pub async fn fetch_new(
        &self,
        db: &impl crate::db::Database,
    ) -> Result<Vec<crate::db::OldNew<M>>, crate::db::DbError>
    where
        M: crate::db::FromRowPositional,
    {
        crate::db::insert_many_returning_new(db, self).await
    }

    /// Execute a batch INSERT, automatically chunking to stay within the
    /// database's bind-parameter limit.
    ///
    /// All chunks run inside a single transaction for atomicity. Returns
    /// the total number of affected rows across all chunks.
    ///
    /// ```ignore
    /// // Inserts 100k rows in ~8 chunks of ~8k rows each (5 cols × 8k ≈ 40k params)
    /// let affected = User::insert_many(&huge_vec)
    ///     .execute_chunked(&db)
    ///     .await?;
    /// ```
    pub async fn execute_chunked(
        &self,
        db: &impl crate::db::Database,
    ) -> Result<u64, crate::db::DbError> {
        #[cfg(feature = "postgres")]
        let chunks = self.build_chunked_pg();
        #[cfg(not(feature = "postgres"))]
        let chunks = self.build_chunked(Dialect::Generic);

        // Single chunk — no transaction wrapper needed.
        if chunks.len() == 1 {
            #[cfg(feature = "postgres")]
            return db.execute(&chunks[0].sql, &chunks[0].params).await;
            #[cfg(not(feature = "postgres"))]
            return db.execute(&chunks[0].0, &chunks[0].1).await;
        }

        // Multiple chunks — wrap in a transaction for atomicity.
        use crate::db::{Database, DynDatabase};
        let total = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let total_clone = total.clone();

        // We need to move chunks into the closure. Clone the data.
        #[cfg(feature = "postgres")]
        let owned_chunks: Vec<(String, Vec<Value>)> =
            chunks.into_iter().map(|q| (q.sql, q.params)).collect();
        #[cfg(not(feature = "postgres"))]
        let owned_chunks = chunks;

        Database::transaction(
            db,
            Box::new(move |tx: &dyn DynDatabase| {
                Box::pin(async move {
                    for (sql, params) in &owned_chunks {
                        let n = tx.execute(sql, params).await?;
                        total_clone.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
                    }
                    Ok(())
                })
            }),
        )
        .await?;

        Ok(total.load(std::sync::atomic::Ordering::Relaxed))
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, IndexDef};

    struct Row3 {
        a: i32,
        b: i32,
        c: i32,
    }

    impl Table for Row3 {
        fn table_name() -> &'static str {
            "rows"
        }
        fn column_names() -> &'static [&'static str] {
            &["a", "b", "c"]
        }
        fn as_values(&self) -> Vec<Value> {
            vec![Value::I32(self.a), Value::I32(self.b), Value::I32(self.c)]
        }
        fn column_defs() -> Vec<ColumnDef> {
            Vec::new()
        }
        fn indexes() -> Vec<IndexDef> {
            Vec::new()
        }
    }

    fn make_rows(n: usize) -> Vec<Row3> {
        (0..n)
            .map(|i| Row3 {
                a: i as i32,
                b: i as i32 * 10,
                c: i as i32 * 100,
            })
            .collect()
    }

    #[test]
    fn single_chunk_when_under_limit() {
        let rows = make_rows(10);
        let builder = InsertManyBuilder::new(&rows);
        // 10 rows × 3 cols = 30 params — well under any limit
        let chunks = builder.build_chunked(Dialect::Generic);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].1.len(), 30);
    }

    #[test]
    fn splits_into_correct_chunks() {
        // 3 cols, Generic limit = 32_766 → max 10_922 rows per chunk
        let rows = make_rows(25_000);
        let builder = InsertManyBuilder::new(&rows);
        let chunks = builder.build_chunked(Dialect::Generic);

        // 25_000 / 10_922 = 3 chunks (10_922 + 10_922 + 3_156)
        assert_eq!(chunks.len(), 3);

        let total_params: usize = chunks.iter().map(|(_, p)| p.len()).sum();
        assert_eq!(total_params, 25_000 * 3);

        // Each chunk must not exceed the limit
        for (_, params) in &chunks {
            assert!(params.len() <= Dialect::Generic.max_params());
        }
    }

    #[test]
    fn chunk_sql_is_valid() {
        let rows = make_rows(5);
        let builder = InsertManyBuilder::new(&rows);
        let chunks = builder.build_chunked(Dialect::Generic);
        assert_eq!(chunks.len(), 1);
        let (sql, _) = &chunks[0];
        assert!(sql.starts_with("INSERT INTO \"rows\""));
        // 5 rows → 5 value groups
        assert_eq!(sql.matches("(?, ?, ?)").count(), 5);
    }

    #[test]
    fn on_conflict_preserved_in_chunks() {
        let rows = make_rows(5);
        let builder = InsertManyBuilder::new(&rows).on_conflict_do_nothing();
        let chunks = builder.build_chunked(Dialect::Postgres);
        for (sql, _) in &chunks {
            assert!(
                sql.contains("ON CONFLICT DO NOTHING"),
                "missing ON CONFLICT in: {sql}"
            );
        }
    }

    #[test]
    fn rows_per_chunk_calculation() {
        let rows = make_rows(1);
        let builder = InsertManyBuilder::new(&rows);
        // 3 cols → 65_535 / 3 = 21_845 rows per chunk for Postgres
        assert_eq!(builder.rows_per_chunk(Dialect::Postgres), 21_845);
        // 3 cols → 32_766 / 3 = 10_922 rows per chunk for Generic
        assert_eq!(builder.rows_per_chunk(Dialect::Generic), 10_922);
    }

    // ── build_chunked_pg tests (PostgreSQL-only) ────────────────────

    #[test]
    #[cfg(feature = "postgres")]
    fn build_chunked_pg_single_chunk() {
        let rows = make_rows(5);
        let builder = InsertManyBuilder::new(&rows);
        let chunks = builder.build_chunked_pg();

        assert_eq!(chunks.len(), 1);
        let q = &chunks[0];
        // 5 rows × 3 cols = 15 params → $1 .. $15
        assert!(
            q.sql.contains("$1"),
            "SQL should contain $1 placeholders: {}",
            q.sql
        );
        assert!(
            q.sql.contains("$15"),
            "SQL should contain $15 placeholders: {}",
            q.sql
        );
        assert!(
            !q.sql.contains('?'),
            "SQL should not contain ? placeholders"
        );
        assert_eq!(q.params.len(), 15);
    }

    #[test]
    #[cfg(feature = "postgres")]
    fn build_chunked_pg_even_chunks_have_identical_sql() {
        // 2 full chunks: 21_845 × 2 = 43_690 rows
        let rows = make_rows(21_845 * 2);
        let builder = InsertManyBuilder::new(&rows);
        let chunks = builder.build_chunked_pg();

        assert_eq!(chunks.len(), 2);
        // Both chunks are full-size → same SQL shape, same placeholder count
        assert_eq!(chunks[0].sql, chunks[1].sql);

        // Each chunk starts at $1 (independent queries)
        assert!(chunks[0].sql.contains("$1"));
        assert!(chunks[1].sql.contains("$1"));

        // 21_845 rows × 3 cols = 65_535 params per chunk
        assert_eq!(chunks[0].params.len(), 65_535);
        assert_eq!(chunks[1].params.len(), 65_535);
    }

    #[test]
    #[cfg(feature = "postgres")]
    fn build_chunked_pg_partial_last_chunk() {
        // 2 full chunks + 1 partial row: 21_845 × 2 + 1 = 43_691 rows
        let rows = make_rows(21_845 * 2 + 1);
        let builder = InsertManyBuilder::new(&rows);
        let chunks = builder.build_chunked_pg();

        assert_eq!(chunks.len(), 3);

        // First two chunks are full-size → identical SQL
        assert_eq!(chunks[0].sql, chunks[1].sql);

        // Last chunk is partial (1 row × 3 cols = 3 params) → different SQL
        assert_ne!(chunks[0].sql, chunks[2].sql);

        // Verify placeholder counts by scanning SQL
        let count_dollars = |s: &str| s.matches("$").count();
        // Full chunks: 65_535 placeholders → 65_535 "$" occurrences
        assert_eq!(count_dollars(&chunks[0].sql), 65_535);
        assert_eq!(count_dollars(&chunks[1].sql), 65_535);
        // Partial chunk: only 3 placeholders
        assert_eq!(count_dollars(&chunks[2].sql), 3);

        // Params count
        assert_eq!(chunks[0].params.len(), 65_535);
        assert_eq!(chunks[1].params.len(), 65_535);
        assert_eq!(chunks[2].params.len(), 3);
    }

    #[test]
    #[cfg(feature = "postgres")]
    fn build_chunked_pg_params_partitioned_correctly() {
        // 3 chunks: 2 full + 1 partial (5 rows)
        let rows = make_rows(21_845 * 2 + 5);
        let builder = InsertManyBuilder::new(&rows);
        let chunks = builder.build_chunked_pg();

        assert_eq!(chunks.len(), 3);

        // First chunk params: rows 0..21_845
        assert_eq!(chunks[0].params[0], Value::I32(0));
        assert_eq!(chunks[0].params[1], Value::I32(0));
        assert_eq!(chunks[0].params[2], Value::I32(0));

        // Second chunk params: rows 21_845..43_690
        assert_eq!(chunks[1].params[0], Value::I32(21_845));
        assert_eq!(chunks[1].params[1], Value::I32(21_845 * 10));
        assert_eq!(chunks[1].params[2], Value::I32(21_845 * 100));

        // Third chunk params: rows 43_690..43_695
        assert_eq!(chunks[2].params[0], Value::I32(43_690));
        assert_eq!(chunks[2].params[1], Value::I32(43_690 * 10));
        assert_eq!(chunks[2].params[2], Value::I32(43_690 * 100));
    }
}
