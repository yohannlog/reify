use crate::table::Table;
use crate::value::Value;

// ── Row abstraction ─────────────────────────────────────────────────

/// A single row returned by a query.
///
/// Column lookup by name is O(1) via an internal index map built lazily on
/// the first call to [`get`](Row::get). Positional access via
/// [`get_idx`](Row::get_idx) is always O(1).
#[derive(Debug, Clone)]
pub struct Row {
    columns: Vec<String>,
    values: Vec<Value>,
    /// Lazily-built column-name → index map for O(1) named lookups.
    index: std::sync::OnceLock<std::collections::HashMap<String, usize>>,
}

impl Row {
    pub fn new(columns: Vec<String>, values: Vec<Value>) -> Self {
        Self {
            columns,
            values,
            index: std::sync::OnceLock::new(),
        }
    }

    /// Get a value by column name (O(1) after the first call).
    pub fn get(&self, column: &str) -> Option<&Value> {
        let idx_map = self.index.get_or_init(|| {
            self.columns
                .iter()
                .enumerate()
                .map(|(i, c)| (c.clone(), i))
                .collect()
        });
        idx_map.get(column).and_then(|&i| self.values.get(i))
    }

    /// Get a value by column index (always O(1)).
    pub fn get_idx(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Column names in this row.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// All values in this row.
    pub fn values(&self) -> &[Value] {
        &self.values
    }
}

// ── FromRow trait ───────────────────────────────────────────────────

/// Trait for types that can be constructed from a database row.
pub trait FromRow: Sized {
    fn from_row(row: &Row) -> Result<Self, DbError>;
}

// ── Error type ──────────────────────────────────────────────────────

/// Database error.
#[derive(Debug)]
pub enum DbError {
    /// Connection failed.
    Connection(String),
    /// Query execution failed.
    Query(String),
    /// Expected exactly one row, but found none.
    RecordNotFound,
    /// Expected at most one row, but found multiple.
    TooManyRows,
    /// Constraint violation (unique, foreign key, not-null, check, …).
    ///
    /// `message` is a human-readable description; `sqlstate` carries the
    /// five-character SQLSTATE code when the driver exposes it
    /// (e.g. `"23505"` for PostgreSQL unique-violation).
    Constraint {
        message: String,
        sqlstate: Option<String>,
    },
    /// Row conversion failed.
    Conversion(String),
    /// Other error.
    Other(String),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::Connection(msg) => write!(f, "connection error: {msg}"),
            DbError::Query(msg) => write!(f, "query error: {msg}"),
            DbError::RecordNotFound => write!(f, "expected 1 row, found 0"),
            DbError::TooManyRows => write!(f, "expected at most 1 row, found multiple"),
            DbError::Constraint {
                message,
                sqlstate: Some(code),
            } => {
                write!(f, "constraint violation [{code}]: {message}")
            }
            DbError::Constraint {
                message,
                sqlstate: None,
            } => {
                write!(f, "constraint violation: {message}")
            }
            DbError::Conversion(msg) => write!(f, "conversion error: {msg}"),
            DbError::Other(msg) => write!(f, "error: {msg}"),
        }
    }
}

impl std::error::Error for DbError {}

// ── SQLSTATE constants ──────────────────────────────────────────────

/// Standard SQLSTATE codes for constraint violations (class 23).
pub mod sqlstate {
    /// SQLSTATE class prefix for all integrity constraint violations.
    pub const CONSTRAINT_CLASS: &str = "23";
    /// 23000 — generic integrity constraint violation.
    pub const INTEGRITY_CONSTRAINT: &str = "23000";
    /// 23502 — NOT NULL violation.
    pub const NOT_NULL_VIOLATION: &str = "23502";
    /// 23503 — foreign key violation.
    pub const FOREIGN_KEY_VIOLATION: &str = "23503";
    /// 23505 — unique constraint violation.
    pub const UNIQUE_VIOLATION: &str = "23505";
    /// 23514 — CHECK constraint violation.
    pub const CHECK_VIOLATION: &str = "23514";

    /// Returns `true` if the given SQLSTATE code is a constraint violation.
    pub fn is_constraint_violation(code: &str) -> bool {
        code.starts_with(CONSTRAINT_CLASS)
    }
}

// ── Shared type aliases ─────────────────────────────────────────────

/// A boxed, `Send`-safe future returning `Result<T, DbError>`.
///
/// Used by [`DynDatabase`] (the dyn-compatible companion trait) to erase
/// the concrete future type behind a trait object.
pub type BoxFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, DbError>> + Send + 'a>>;

/// A boxed, `Send`-safe stream returning `Result<T, DbError>`.
pub type BoxStream<'a, T> =
    std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<T, DbError>> + Send + 'a>>;

/// The closure accepted by [`Database::transaction`].
///
/// Receives a `&dyn DynDatabase` representing the isolated transaction
/// connection and returns a [`BoxFuture`] that resolves when the
/// transaction body completes.
pub type TransactionFn<'a> =
    Box<dyn for<'c> FnOnce(&'c dyn DynDatabase) -> BoxFuture<'c, ()> + Send + 'a>;

// ── Database trait ──────────────────────────────────────────────────

/// Async database connection trait.
///
/// Implemented by each adapter (postgres, mysql/mariadb, sqlite).
///
/// Uses native `async fn` (AFIT) — zero-cost, no heap allocation per call.
/// Requires `Send + Sync` on implementors for use across threads.
///
/// The `transaction` method accepts a boxed closure whose inner connection
/// is typed as `&dyn DynDatabase` (the dyn-compatible companion trait) so
/// that the transaction wrapper can be passed as a trait object. This is the
/// only remaining allocation and is intentional (one per transaction, not per
/// query).
#[allow(async_fn_in_trait)]
pub trait Database: Send + Sync {
    /// Execute a statement (INSERT, UPDATE, DELETE). Returns rows affected.
    fn execute(
        &self,
        sql: &str,
        params: &[Value],
    ) -> impl std::future::Future<Output = Result<u64, DbError>> + Send;

    /// Execute a query (SELECT). Returns rows.
    fn query(
        &self,
        sql: &str,
        params: &[Value],
    ) -> impl std::future::Future<Output = Result<Vec<Row>, DbError>> + Send;

    /// Execute a query (SELECT) and return an asynchronous stream of rows.
    ///
    /// The default implementation executes `query` and streams the resulting `Vec<Row>`.
    /// Database adapters can override this to stream rows directly from the driver
    /// to avoid loading the entire result set into memory.
    fn query_stream<'a>(
        &'a self,
        sql: String,
        params: Vec<Value>,
    ) -> impl std::future::Future<Output = Result<BoxStream<'a, Row>, DbError>> + Send {
        use futures_util::StreamExt;
        async move {
            let rows = self.query(&sql, &params).await?;
            Ok(
                Box::pin(futures_util::stream::iter(rows.into_iter().map(Ok)))
                    as BoxStream<'a, Row>,
            )
        }
    }

    /// Execute a query and return a single row (e.g. COUNT).
    fn query_one(
        &self,
        sql: &str,
        params: &[Value],
    ) -> impl std::future::Future<Output = Result<Row, DbError>> + Send;

    /// Run a closure inside a transaction.
    ///
    /// The closure receives a `&dyn DynDatabase` that represents the
    /// **isolated transaction connection** — NOT the pool. All queries inside
    /// `f` MUST go through this reference to participate in the transaction.
    fn transaction<'a>(
        &'a self,
        f: TransactionFn<'a>,
    ) -> impl std::future::Future<Output = Result<(), DbError>> + Send;
}

// ── DynDatabase — dyn-compatible companion ──────────────────────────

/// Dyn-compatible version of [`Database`], used where a trait object
/// (`&dyn DynDatabase`) is required — primarily inside transaction closures.
///
/// A blanket impl automatically implements this for every `T: Database`.
/// You should not implement this trait manually.
pub trait DynDatabase: Send + Sync {
    fn execute<'a>(&'a self, sql: &'a str, params: &'a [Value]) -> BoxFuture<'a, u64>;

    fn query<'a>(&'a self, sql: &'a str, params: &'a [Value]) -> BoxFuture<'a, Vec<Row>>;

    fn query_stream<'a>(
        &'a self,
        sql: String,
        params: Vec<Value>,
    ) -> BoxFuture<'a, BoxStream<'a, Row>>;

    fn query_one<'a>(&'a self, sql: &'a str, params: &'a [Value]) -> BoxFuture<'a, Row>;

    fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> BoxFuture<'a, ()>;
}

impl<T: Database> DynDatabase for T {
    fn execute<'a>(&'a self, sql: &'a str, params: &'a [Value]) -> BoxFuture<'a, u64> {
        Box::pin(Database::execute(self, sql, params))
    }

    fn query<'a>(&'a self, sql: &'a str, params: &'a [Value]) -> BoxFuture<'a, Vec<Row>> {
        Box::pin(Database::query(self, sql, params))
    }

    fn query_stream<'a>(
        &'a self,
        sql: String,
        params: Vec<Value>,
    ) -> BoxFuture<'a, BoxStream<'a, Row>> {
        Box::pin(Database::query_stream(self, sql, params))
    }

    fn query_one<'a>(&'a self, sql: &'a str, params: &'a [Value]) -> BoxFuture<'a, Row> {
        Box::pin(Database::query_one(self, sql, params))
    }

    fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> BoxFuture<'a, ()> {
        Box::pin(Database::transaction(self, f))
    }
}

impl Database for Box<dyn DynDatabase> {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        DynDatabase::execute(self.as_ref(), sql, params).await
    }
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        DynDatabase::query(self.as_ref(), sql, params).await
    }
    async fn query_stream<'a>(
        &'a self,
        sql: String,
        params: Vec<Value>,
    ) -> Result<BoxStream<'a, Row>, DbError> {
        DynDatabase::query_stream(self.as_ref(), sql, params).await
    }
    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        DynDatabase::query_one(self.as_ref(), sql, params).await
    }
    async fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> Result<(), DbError> {
        DynDatabase::transaction(self.as_ref(), f).await
    }
}

impl Database for dyn DynDatabase + '_ {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        DynDatabase::execute(self, sql, params).await
    }
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        DynDatabase::query(self, sql, params).await
    }
    async fn query_stream<'a>(
        &'a self,
        sql: String,
        params: Vec<Value>,
    ) -> Result<BoxStream<'a, Row>, DbError> {
        DynDatabase::query_stream(self, sql, params).await
    }
    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        DynDatabase::query_one(self, sql, params).await
    }
    async fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> Result<(), DbError> {
        DynDatabase::transaction(self, f).await
    }
}

// ── Query execution helpers ─────────────────────────────────────────

// Extension methods on query builders for direct execution against a database.
// These are free functions to avoid orphan rule issues.

/// Execute a SELECT and return raw rows.
pub async fn fetch_all<M: Table>(
    db: &impl Database,
    builder: &crate::query::SelectBuilder<M>,
) -> Result<Vec<Row>, DbError> {
    #[cfg(feature = "postgres")]
    {
        let q = builder.build_pg();
        return db.query(&q.sql, &q.params).await;
    }
    #[cfg(not(feature = "postgres"))]
    {
        let (sql, params) = builder.build();
        db.query(&sql, &params).await
    }
}

/// Execute a SELECT and return an asynchronous stream of raw rows.
pub async fn fetch_all_stream<'a, M: Table>(
    db: &'a impl Database,
    builder: &crate::query::SelectBuilder<M>,
) -> Result<BoxStream<'a, Row>, DbError> {
    #[cfg(feature = "postgres")]
    {
        let q = builder.build_pg();
        return db.query_stream(q.sql, q.params).await;
    }
    #[cfg(not(feature = "postgres"))]
    {
        let (sql, params) = builder.build();
        db.query_stream(sql, params).await
    }
}

/// Execute a SELECT and return typed results.
pub async fn fetch<M: Table + FromRow>(
    db: &impl Database,
    builder: &crate::query::SelectBuilder<M>,
) -> Result<Vec<M>, DbError> {
    let rows = fetch_all(db, builder).await?;
    rows.iter().map(|r| M::from_row(r)).collect()
}

/// Execute a SELECT and return an asynchronous stream of typed results.
pub async fn fetch_stream<'a, M: Table + FromRow>(
    db: &'a impl Database,
    builder: &crate::query::SelectBuilder<M>,
) -> Result<BoxStream<'a, M>, DbError> {
    use futures_util::StreamExt;
    let stream = fetch_all_stream(db, builder).await?;
    Ok(Box::pin(
        stream.map(|res| res.and_then(|r| M::from_row(&r))),
    ))
}

/// Execute a SELECT and return exactly one typed result.
///
/// Returns an error if the query returns 0 or 2+ rows.
pub async fn fetch_one<M: Table + FromRow>(
    db: &impl Database,
    builder: &crate::query::SelectBuilder<M>,
) -> Result<M, DbError> {
    let rows = fetch(db, builder).await?;
    match rows.len() {
        1 => Ok(rows.into_iter().next().unwrap()),
        0 => Err(DbError::RecordNotFound),
        _ => Err(DbError::TooManyRows),
    }
}

/// Execute a SELECT and return 0 or 1 typed result.
///
/// Returns an error if the query returns 2+ rows.
pub async fn fetch_optional<M: Table + FromRow>(
    db: &impl Database,
    builder: &crate::query::SelectBuilder<M>,
) -> Result<Option<M>, DbError> {
    let rows = fetch(db, builder).await?;
    match rows.len() {
        0 => Ok(None),
        1 => Ok(Some(rows.into_iter().next().unwrap())),
        _ => Err(DbError::TooManyRows),
    }
}

/// Execute an INSERT.
pub async fn insert<M: Table>(
    db: &impl Database,
    builder: &crate::query::InsertBuilder<M>,
) -> Result<u64, DbError> {
    #[cfg(feature = "postgres")]
    {
        let q = builder.build_pg();
        return db.execute(&q.sql, &q.params).await;
    }
    #[cfg(not(feature = "postgres"))]
    {
        let (sql, params) = builder.build();
        db.execute(&sql, &params).await
    }
}

/// Execute a batch INSERT (multiple rows in one statement).
pub async fn insert_many<M: Table>(
    db: &impl Database,
    builder: &crate::query::InsertManyBuilder<M>,
) -> Result<u64, DbError> {
    #[cfg(feature = "postgres")]
    {
        let q = builder.build_pg();
        return db.execute(&q.sql, &q.params).await;
    }
    #[cfg(not(feature = "postgres"))]
    {
        let (sql, params) = builder.build();
        db.execute(&sql, &params).await
    }
}

/// Execute a batch INSERT … RETURNING and return typed results (PostgreSQL only).
#[cfg(feature = "postgres")]
pub async fn insert_many_returning<M: Table + FromRow>(
    db: &impl Database,
    builder: &crate::query::InsertManyBuilder<M>,
) -> Result<Vec<M>, DbError> {
    let q = builder.build_pg();
    let rows = db.query(&q.sql, &q.params).await?;
    rows.iter().map(|r| M::from_row(r)).collect()
}

/// Execute an INSERT … RETURNING and return typed results (PostgreSQL only).
#[cfg(feature = "postgres")]
pub async fn insert_returning<M: Table + FromRow>(
    db: &impl Database,
    builder: &crate::query::InsertBuilder<M>,
) -> Result<Vec<M>, DbError> {
    let q = builder.build_pg();
    let rows = db.query(&q.sql, &q.params).await?;
    rows.iter().map(|r| M::from_row(r)).collect()
}

/// Execute an UPDATE.
pub async fn update<M: Table>(
    db: &impl Database,
    builder: &crate::query::UpdateBuilder<M>,
) -> Result<u64, DbError> {
    #[cfg(feature = "postgres")]
    {
        let q = builder.build_pg();
        return db.execute(&q.sql, &q.params).await;
    }
    #[cfg(not(feature = "postgres"))]
    {
        let (sql, params) = builder.build();
        db.execute(&sql, &params).await
    }
}

/// Execute an UPDATE … RETURNING and return typed results (PostgreSQL only).
#[cfg(feature = "postgres")]
pub async fn update_returning<M: Table + FromRow>(
    db: &impl Database,
    builder: &crate::query::UpdateBuilder<M>,
) -> Result<Vec<M>, DbError> {
    let q = builder.build_pg();
    let rows = db.query(&q.sql, &q.params).await?;
    rows.iter().map(|r| M::from_row(r)).collect()
}

/// Execute a DELETE.
pub async fn delete<M: Table>(
    db: &impl Database,
    builder: &crate::query::DeleteBuilder<M>,
) -> Result<u64, DbError> {
    #[cfg(feature = "postgres")]
    {
        let q = builder.build_pg();
        return db.execute(&q.sql, &q.params).await;
    }
    #[cfg(not(feature = "postgres"))]
    {
        let (sql, params) = builder.build();
        db.execute(&sql, &params).await
    }
}

/// Execute a DELETE … RETURNING and return typed results (PostgreSQL only).
#[cfg(feature = "postgres")]
pub async fn delete_returning<M: Table + FromRow>(
    db: &impl Database,
    builder: &crate::query::DeleteBuilder<M>,
) -> Result<Vec<M>, DbError> {
    let q = builder.build_pg();
    let rows = db.query(&q.sql, &q.params).await?;
    rows.iter().map(|r| M::from_row(r)).collect()
}

// ── Raw SQL helpers ─────────────────────────────────────────────────

/// Execute a raw SQL statement (INSERT / UPDATE / DELETE / DDL).
///
/// Use `?` as the placeholder character; adapters rewrite it as needed.
/// Returns the number of rows affected.
///
/// ```ignore
/// let affected = raw_execute(db, "DELETE FROM sessions WHERE expires_at < ?", &[Value::Timestamptz(cutoff)]).await?;
/// ```
pub async fn raw_execute(db: &impl Database, sql: &str, params: &[Value]) -> Result<u64, DbError> {
    db.execute(sql, params).await
}

/// Execute a raw SQL SELECT and return untyped rows.
///
/// Use `?` as the placeholder character.
///
/// ```ignore
/// let rows = raw_query(db, "SELECT id, name FROM users WHERE active = ?", &[Value::Bool(true)]).await?;
/// ```
pub async fn raw_query(
    db: &impl Database,
    sql: &str,
    params: &[Value],
) -> Result<Vec<Row>, DbError> {
    db.query(sql, params).await
}

/// Execute a raw SQL SELECT and deserialize each row into `T`.
///
/// `T` must implement [`FromRow`]. Use `?` as the placeholder character.
///
/// ```ignore
/// let users: Vec<User> = raw_fetch::<User>(db, "SELECT * FROM users WHERE id = ?", &[Value::I64(1)]).await?;
/// ```
pub async fn raw_fetch<T: FromRow>(
    db: &impl Database,
    sql: &str,
    params: &[Value],
) -> Result<Vec<T>, DbError> {
    let rows = db.query(sql, params).await?;
    rows.iter().map(|r| T::from_row(r)).collect()
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Minimal in-memory Database stub ─────────────────────────────

    struct StubDb {
        /// Rows returned by every query call.
        rows: Vec<Row>,
        /// Affected-rows count returned by every execute call.
        affected: u64,
    }

    impl StubDb {
        fn with_rows(rows: Vec<Row>) -> Self {
            Self { rows, affected: 0 }
        }

        fn with_affected(n: u64) -> Self {
            Self {
                rows: vec![],
                affected: n,
            }
        }
    }

    impl Database for StubDb {
        async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<u64, DbError> {
            Ok(self.affected)
        }

        async fn query(&self, _sql: &str, _params: &[Value]) -> Result<Vec<Row>, DbError> {
            Ok(self.rows.clone())
        }

        async fn query_one(&self, _sql: &str, _params: &[Value]) -> Result<Row, DbError> {
            self.rows
                .first()
                .cloned()
                .ok_or_else(|| DbError::Query("no rows".into()))
        }

        fn transaction<'a>(
            &'a self,
            f: TransactionFn<'a>,
        ) -> impl std::future::Future<Output = Result<(), DbError>> + Send {
            async move { f(self).await }
        }
    }

    // ── FromRow stub ─────────────────────────────────────────────────

    #[derive(Debug, PartialEq)]
    struct UserRow {
        id: i64,
        name: String,
    }

    impl FromRow for UserRow {
        fn from_row(row: &Row) -> Result<Self, DbError> {
            let id = match row.get("id") {
                Some(Value::I64(v)) => *v,
                _ => return Err(DbError::Conversion("missing id".into())),
            };
            let name = match row.get("name") {
                Some(Value::String(v)) => v.clone(),
                _ => return Err(DbError::Conversion("missing name".into())),
            };
            Ok(UserRow { id, name })
        }
    }

    // ── raw_execute tests ────────────────────────────────────────────

    #[tokio::test]
    async fn raw_execute_returns_affected_rows() {
        let db = StubDb::with_affected(3);
        let affected = raw_execute(&db, "DELETE FROM t WHERE x = ?", &[Value::I32(1)])
            .await
            .unwrap();
        assert_eq!(affected, 3);
    }

    #[tokio::test]
    async fn raw_execute_empty_params() {
        let db = StubDb::with_affected(0);
        let affected = raw_execute(&db, "TRUNCATE TABLE t", &[]).await.unwrap();
        assert_eq!(affected, 0);
    }

    // ── raw_query tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn raw_query_returns_rows() {
        let row = Row::new(
            vec!["id".into(), "name".into()],
            vec![Value::I64(42), Value::String("alice".into())],
        );
        let db = StubDb::with_rows(vec![row]);
        let rows = raw_query(
            &db,
            "SELECT id, name FROM users WHERE id = ?",
            &[Value::I64(42)],
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&Value::I64(42)));
        assert_eq!(rows[0].get("name"), Some(&Value::String("alice".into())));
    }

    #[tokio::test]
    async fn raw_query_empty_result() {
        let db = StubDb::with_rows(vec![]);
        let rows = raw_query(&db, "SELECT 1 WHERE false", &[]).await.unwrap();
        assert!(rows.is_empty());
    }

    // ── raw_fetch tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn raw_fetch_deserializes_rows() {
        let rows = vec![
            Row::new(
                vec!["id".into(), "name".into()],
                vec![Value::I64(1), Value::String("bob".into())],
            ),
            Row::new(
                vec!["id".into(), "name".into()],
                vec![Value::I64(2), Value::String("carol".into())],
            ),
        ];
        let db = StubDb::with_rows(rows);
        let users: Vec<UserRow> = raw_fetch::<UserRow>(&db, "SELECT id, name FROM users", &[])
            .await
            .unwrap();
        assert_eq!(users.len(), 2);
        assert_eq!(
            users[0],
            UserRow {
                id: 1,
                name: "bob".into()
            }
        );
        assert_eq!(
            users[1],
            UserRow {
                id: 2,
                name: "carol".into()
            }
        );
    }

    #[tokio::test]
    async fn raw_fetch_propagates_conversion_error() {
        // Row missing the "name" column → FromRow returns Err
        let row = Row::new(vec!["id".into()], vec![Value::I64(99)]);
        let db = StubDb::with_rows(vec![row]);
        let result = raw_fetch::<UserRow>(&db, "SELECT id FROM users", &[]).await;
        assert!(matches!(result, Err(DbError::Conversion(_))));
    }

    // ── DbError display tests ────────────────────────────────────────

    #[test]
    fn dberror_display_constraint_with_sqlstate() {
        let e = DbError::Constraint {
            message: "duplicate key".into(),
            sqlstate: Some("23505".into()),
        };
        assert_eq!(e.to_string(), "constraint violation [23505]: duplicate key");
    }

    #[test]
    fn dberror_display_constraint_without_sqlstate() {
        let e = DbError::Constraint {
            message: "not null violation".into(),
            sqlstate: None,
        };
        assert_eq!(e.to_string(), "constraint violation: not null violation");
    }

    #[test]
    fn dberror_display_variants() {
        assert_eq!(
            DbError::Connection("refused".into()).to_string(),
            "connection error: refused"
        );
        assert_eq!(
            DbError::Query("syntax".into()).to_string(),
            "query error: syntax"
        );
        assert_eq!(
            DbError::Conversion("bad type".into()).to_string(),
            "conversion error: bad type"
        );
        assert_eq!(DbError::Other("oops".into()).to_string(), "error: oops");
    }

    // ── Row::get index cache ─────────────────────────────────────────

    #[test]
    fn row_get_by_name_returns_correct_value() {
        let row = Row::new(
            vec!["id".into(), "name".into(), "active".into()],
            vec![
                Value::I64(7),
                Value::String("alice".into()),
                Value::Bool(true),
            ],
        );
        assert_eq!(row.get("id"), Some(&Value::I64(7)));
        assert_eq!(row.get("name"), Some(&Value::String("alice".into())));
        assert_eq!(row.get("active"), Some(&Value::Bool(true)));
        assert_eq!(row.get("missing"), None);
    }

    #[test]
    fn row_get_is_idempotent_after_cache_build() {
        let row = Row::new(
            vec!["x".into(), "y".into()],
            vec![Value::I32(1), Value::I32(2)],
        );
        // Call twice — second call uses the cached index map.
        assert_eq!(row.get("x"), Some(&Value::I32(1)));
        assert_eq!(row.get("x"), Some(&Value::I32(1)));
        assert_eq!(row.get("y"), Some(&Value::I32(2)));
    }

    #[test]
    fn row_get_idx_returns_correct_value() {
        let row = Row::new(
            vec!["a".into(), "b".into()],
            vec![Value::I64(10), Value::I64(20)],
        );
        assert_eq!(row.get_idx(0), Some(&Value::I64(10)));
        assert_eq!(row.get_idx(1), Some(&Value::I64(20)));
        assert_eq!(row.get_idx(2), None);
    }

    #[test]
    fn row_columns_and_values_accessors() {
        let row = Row::new(vec!["col".into()], vec![Value::Bool(false)]);
        assert_eq!(row.columns(), &["col".to_string()]);
        assert_eq!(row.values(), &[Value::Bool(false)]);
    }
}

/// Execute an INSERT, calling `ModelHooks::before_insert` and `after_insert` if implemented.
///
/// Pass a mutable reference to the model so `before_insert` can mutate it
/// (e.g. set `created_at`).
pub async fn insert_with_hooks<M: Table + crate::hooks::ModelHooks>(
    db: &impl Database,
    model: &mut M,
    builder_fn: impl FnOnce(&M) -> crate::query::InsertBuilder<M>,
) -> Result<u64, DbError> {
    model.before_insert();
    let builder = builder_fn(model);
    #[cfg(feature = "postgres")]
    let result = {
        let q = builder.build_pg();
        db.execute(&q.sql, &q.params).await?
    };
    #[cfg(not(feature = "postgres"))]
    let result = {
        let (sql, params) = builder.build();
        db.execute(&sql, &params).await?
    };
    model.after_insert();
    Ok(result)
}

/// Execute an UPDATE, calling `ModelHooks::before_update` if implemented.
pub async fn update_with_hooks<M: Table + crate::hooks::ModelHooks>(
    db: &impl Database,
    model: &mut M,
    builder_fn: impl FnOnce(&M) -> crate::query::UpdateBuilder<M>,
) -> Result<u64, DbError> {
    model.before_update();
    let builder = builder_fn(model);
    #[cfg(feature = "postgres")]
    {
        let q = builder.build_pg();
        db.execute(&q.sql, &q.params).await
    }
    #[cfg(not(feature = "postgres"))]
    {
        let (sql, params) = builder.build();
        db.execute(&sql, &params).await
    }
}

/// Execute a DELETE, calling `ModelHooks::before_delete` if implemented.
pub async fn delete_with_hooks<M: Table + crate::hooks::ModelHooks>(
    db: &impl Database,
    model: &M,
    builder: &crate::query::DeleteBuilder<M>,
) -> Result<u64, DbError> {
    model.before_delete();
    #[cfg(feature = "postgres")]
    {
        let q = builder.build_pg();
        return db.execute(&q.sql, &q.params).await;
    }
    #[cfg(not(feature = "postgres"))]
    {
        let (sql, params) = builder.build();
        db.execute(&sql, &params).await
    }
}
