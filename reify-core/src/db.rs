use std::future::Future;
use std::pin::Pin;

use crate::table::Table;
use crate::value::Value;

// ── Row abstraction ─────────────────────────────────────────────────

/// A single row returned by a query.
#[derive(Debug, Clone)]
pub struct Row {
    columns: Vec<String>,
    values: Vec<Value>,
}

impl Row {
    pub fn new(columns: Vec<String>, values: Vec<Value>) -> Self {
        Self { columns, values }
    }

    /// Get a value by column name.
    pub fn get(&self, column: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|c| c == column)
            .map(|i| &self.values[i])
    }

    /// Get a value by column index.
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
            DbError::Constraint { message, sqlstate: Some(code) } => {
                write!(f, "constraint violation [{code}]: {message}")
            }
            DbError::Constraint { message, sqlstate: None } => {
                write!(f, "constraint violation: {message}")
            }
            DbError::Conversion(msg) => write!(f, "conversion error: {msg}"),
            DbError::Other(msg) => write!(f, "error: {msg}"),
        }
    }
}

impl std::error::Error for DbError {}

// ── Database trait ──────────────────────────────────────────────────

/// Async database connection trait.
///
/// Implemented by each adapter (postgres, mysql/mariadb).
pub trait Database: Send + Sync {
    /// Execute a statement (INSERT, UPDATE, DELETE). Returns rows affected.
    fn execute<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Value],
    ) -> Pin<Box<dyn Future<Output = Result<u64, DbError>> + Send + 'a>>;

    /// Execute a query (SELECT). Returns rows.
    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Value],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Row>, DbError>> + Send + 'a>>;

    /// Execute a query and return a single scalar value (e.g. COUNT).
    fn query_one<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Value],
    ) -> Pin<Box<dyn Future<Output = Result<Row, DbError>> + Send + 'a>>;

    /// Run a closure inside a transaction.
    fn transaction<'a>(
        &'a self,
        f: Box<
            dyn FnOnce(
                    &'a dyn Database,
                )
                    -> Pin<Box<dyn Future<Output = Result<(), DbError>> + Send + 'a>>
                + Send
                + 'a,
        >,
    ) -> Pin<Box<dyn Future<Output = Result<(), DbError>> + Send + 'a>>;
}

// ── Query execution helpers ─────────────────────────────────────────

/// Extension methods on query builders for direct execution against a database.
///
/// These are free functions to avoid orphan rule issues.

/// Execute a SELECT and return raw rows.
pub async fn fetch_all<M: Table>(
    db: &dyn Database,
    builder: &crate::query::SelectBuilder<M>,
) -> Result<Vec<Row>, DbError> {
    let (sql, params) = builder.build();
    db.query(&sql, &params).await
}

/// Execute a SELECT and return typed results.
pub async fn fetch<M: Table + FromRow>(
    db: &dyn Database,
    builder: &crate::query::SelectBuilder<M>,
) -> Result<Vec<M>, DbError> {
    let rows = fetch_all(db, builder).await?;
    rows.iter().map(|r| M::from_row(r)).collect()
}

/// Execute an INSERT.
pub async fn insert<M: Table>(
    db: &dyn Database,
    builder: &crate::query::InsertBuilder<M>,
) -> Result<u64, DbError> {
    let (sql, params) = builder.build();
    db.execute(&sql, &params).await
}

/// Execute a batch INSERT (multiple rows in one statement).
pub async fn insert_many<M: Table>(
    db: &dyn Database,
    builder: &crate::query::InsertManyBuilder<M>,
) -> Result<u64, DbError> {
    let (sql, params) = builder.build();
    db.execute(&sql, &params).await
}

/// Execute a batch INSERT … RETURNING and return typed results (PostgreSQL only).
#[cfg(feature = "postgres")]
pub async fn insert_many_returning<M: Table + FromRow>(
    db: &dyn Database,
    builder: &crate::query::InsertManyBuilder<M>,
) -> Result<Vec<M>, DbError> {
    let (sql, params) = builder.build();
    let rows = db.query(&sql, &params).await?;
    rows.iter().map(|r| M::from_row(r)).collect()
}

/// Execute an INSERT … RETURNING and return typed results (PostgreSQL only).
#[cfg(feature = "postgres")]
pub async fn insert_returning<M: Table + FromRow>(
    db: &dyn Database,
    builder: &crate::query::InsertBuilder<M>,
) -> Result<Vec<M>, DbError> {
    let (sql, params) = builder.build();
    let rows = db.query(&sql, &params).await?;
    rows.iter().map(|r| M::from_row(r)).collect()
}

/// Execute an UPDATE.
pub async fn update<M: Table>(
    db: &dyn Database,
    builder: &crate::query::UpdateBuilder<M>,
) -> Result<u64, DbError> {
    let (sql, params) = builder.build();
    db.execute(&sql, &params).await
}

/// Execute an UPDATE … RETURNING and return typed results (PostgreSQL only).
#[cfg(feature = "postgres")]
pub async fn update_returning<M: Table + FromRow>(
    db: &dyn Database,
    builder: &crate::query::UpdateBuilder<M>,
) -> Result<Vec<M>, DbError> {
    let (sql, params) = builder.build();
    let rows = db.query(&sql, &params).await?;
    rows.iter().map(|r| M::from_row(r)).collect()
}

/// Execute a DELETE.
pub async fn delete<M: Table>(
    db: &dyn Database,
    builder: &crate::query::DeleteBuilder<M>,
) -> Result<u64, DbError> {
    let (sql, params) = builder.build();
    db.execute(&sql, &params).await
}

/// Execute a DELETE … RETURNING and return typed results (PostgreSQL only).
#[cfg(feature = "postgres")]
pub async fn delete_returning<M: Table + FromRow>(
    db: &dyn Database,
    builder: &crate::query::DeleteBuilder<M>,
) -> Result<Vec<M>, DbError> {
    let (sql, params) = builder.build();
    let rows = db.query(&sql, &params).await?;
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
pub async fn raw_execute(
    db: &dyn Database,
    sql: &str,
    params: &[Value],
) -> Result<u64, DbError> {
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
    db: &dyn Database,
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
    db: &dyn Database,
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
            Self { rows: vec![], affected: n }
        }
    }

    impl Database for StubDb {
        fn execute<'a>(
            &'a self,
            _sql: &'a str,
            _params: &'a [Value],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, DbError>> + Send + 'a>>
        {
            let n = self.affected;
            Box::pin(async move { Ok(n) })
        }

        fn query<'a>(
            &'a self,
            _sql: &'a str,
            _params: &'a [Value],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<Row>, DbError>> + Send + 'a>>
        {
            let rows = self.rows.clone();
            Box::pin(async move { Ok(rows) })
        }

        fn query_one<'a>(
            &'a self,
            _sql: &'a str,
            _params: &'a [Value],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Row, DbError>> + Send + 'a>>
        {
            let row = self.rows.first().cloned();
            Box::pin(async move {
                row.ok_or_else(|| DbError::Query("no rows".into()))
            })
        }

        fn transaction<'a>(
            &'a self,
            f: Box<
                dyn FnOnce(
                        &'a dyn Database,
                    ) -> std::pin::Pin<
                        Box<dyn std::future::Future<Output = Result<(), DbError>> + Send + 'a>,
                    > + Send
                    + 'a,
            >,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DbError>> + Send + 'a>>
        {
            Box::pin(async move { f(self).await })
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
        let rows = raw_query(&db, "SELECT id, name FROM users WHERE id = ?", &[Value::I64(42)])
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
        let users: Vec<UserRow> =
            raw_fetch::<UserRow>(&db, "SELECT id, name FROM users", &[]).await.unwrap();
        assert_eq!(users.len(), 2);
        assert_eq!(users[0], UserRow { id: 1, name: "bob".into() });
        assert_eq!(users[1], UserRow { id: 2, name: "carol".into() });
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
        assert_eq!(DbError::Connection("refused".into()).to_string(), "connection error: refused");
        assert_eq!(DbError::Query("syntax".into()).to_string(), "query error: syntax");
        assert_eq!(DbError::Conversion("bad type".into()).to_string(), "conversion error: bad type");
        assert_eq!(DbError::Other("oops".into()).to_string(), "error: oops");
    }
}

/// Execute an INSERT, calling `ModelHooks::before_insert` and `after_insert` if implemented.
///
/// Pass a mutable reference to the model so `before_insert` can mutate it
/// (e.g. set `created_at`).
pub async fn insert_with_hooks<M: Table + crate::hooks::ModelHooks>(
    db: &dyn Database,
    model: &mut M,
    builder_fn: impl FnOnce(&M) -> crate::query::InsertBuilder<M>,
) -> Result<u64, DbError> {
    model.before_insert();
    let builder = builder_fn(model);
    let (sql, params) = builder.build();
    let result = db.execute(&sql, &params).await?;
    model.after_insert();
    Ok(result)
}

/// Execute an UPDATE, calling `ModelHooks::before_update` if implemented.
pub async fn update_with_hooks<M: Table + crate::hooks::ModelHooks>(
    db: &dyn Database,
    model: &mut M,
    builder_fn: impl FnOnce(&M) -> crate::query::UpdateBuilder<M>,
) -> Result<u64, DbError> {
    model.before_update();
    let builder = builder_fn(model);
    let (sql, params) = builder.build();
    db.execute(&sql, &params).await
}

/// Execute a DELETE, calling `ModelHooks::before_delete` if implemented.
pub async fn delete_with_hooks<M: Table + crate::hooks::ModelHooks>(
    db: &dyn Database,
    model: &M,
    builder: &crate::query::DeleteBuilder<M>,
) -> Result<u64, DbError> {
    model.before_delete();
    let (sql, params) = builder.build();
    db.execute(&sql, &params).await
}
