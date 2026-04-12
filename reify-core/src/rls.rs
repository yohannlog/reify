use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::condition::Condition;
use crate::db::{Database, DbError, Row};
use crate::value::Value;

// ── Policy context ─────────────────────────────────────────────────

/// Holds the current session context (user id, tenant id, role, …).
///
/// Build one per request and pass it to `Scoped::new()`.
///
/// ```ignore
/// let ctx = RlsContext::new()
///     .set("tenant_id", 42i64)
///     .set("role", "member");
/// ```
#[derive(Clone)]
pub struct RlsContext {
    values: HashMap<&'static str, Arc<dyn Any + Send + Sync>>,
}

impl RlsContext {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    /// Store a value in the context.
    pub fn set<T: Send + Sync + 'static>(mut self, key: &'static str, val: T) -> Self {
        self.values.insert(key, Arc::new(val));
        self
    }

    /// Retrieve a value from the context.
    pub fn get<T: Send + Sync + 'static>(&self, key: &'static str) -> Option<&T> {
        self.values.get(key).and_then(|v| v.downcast_ref::<T>())
    }
}

impl Default for RlsContext {
    fn default() -> Self {
        Self::new()
    }
}

// ── Policy trait ───────────────────────────────────────────────────

/// Row-level security policy for a table.
///
/// Implement this on your `Table` struct to restrict which rows are visible
/// and modifiable. The policy is enforced automatically by `Scoped`.
///
/// ```ignore
/// impl Policy for Post {
///     fn policy(ctx: &RlsContext) -> Option<Condition> {
///         let user_id = ctx.get::<i64>("user_id")?;
///         Some(Post::user_id.eq(*user_id))
///     }
/// }
/// ```
pub trait Policy: crate::table::Table {
    /// Return a `Condition` that will be injected into every SELECT, UPDATE,
    /// and DELETE query. Return `None` to allow unrestricted access (e.g. for admins).
    fn policy(ctx: &RlsContext) -> Option<Condition>;
}

// ── Scoped database wrapper ────────────────────────────────────────

/// A database wrapper that enforces row-level security policies.
///
/// Wraps any `Database` and automatically injects policy conditions
/// into queries built through the `scoped_*` helper functions.
///
/// ```ignore
/// let ctx = RlsContext::new().set("tenant_id", 42i64);
/// let scoped = Scoped::new(&db, ctx);
///
/// // Only returns posts where tenant_id = 42
/// let posts = scoped_fetch(&scoped, &Post::find()).await?;
/// ```
pub struct Scoped<'a> {
    inner: &'a dyn Database,
    ctx: RlsContext,
}

impl<'a> Scoped<'a> {
    pub fn new(db: &'a dyn Database, ctx: RlsContext) -> Self {
        Self { inner: db, ctx }
    }

    /// Access the RLS context.
    pub fn context(&self) -> &RlsContext {
        &self.ctx
    }
}

/// Delegate raw `Database` calls — policies are enforced at the builder level,
/// not at the SQL string level, so the raw trait just passes through.
impl Database for Scoped<'_> {
    fn execute<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Value],
    ) -> Pin<Box<dyn Future<Output = Result<u64, DbError>> + Send + 'a>> {
        self.inner.execute(sql, params)
    }

    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Value],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Row>, DbError>> + Send + 'a>> {
        self.inner.query(sql, params)
    }

    fn query_one<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Value],
    ) -> Pin<Box<dyn Future<Output = Result<Row, DbError>> + Send + 'a>> {
        self.inner.query_one(sql, params)
    }

    fn transaction<'a>(
        &'a self,
        f: Box<
            dyn FnOnce(
                    &'a dyn Database,
                ) -> Pin<Box<dyn Future<Output = Result<(), DbError>> + Send + 'a>>
                + Send
                + 'a,
        >,
    ) -> Pin<Box<dyn Future<Output = Result<(), DbError>> + Send + 'a>> {
        self.inner.transaction(f)
    }
}

// ── Scoped query helpers ───────────────────────────────────────────

/// Fetch all rows with RLS policy applied.
pub async fn scoped_fetch_all<M: crate::table::Table + Policy>(
    scoped: &Scoped<'_>,
    builder: crate::query::SelectBuilder<M>,
) -> Result<Vec<Row>, DbError> {
    let builder = match M::policy(scoped.context()) {
        Some(cond) => builder.filter(cond),
        None => builder,
    };
    let (sql, params) = builder.build();
    scoped.query(&sql, &params).await
}

/// Fetch typed results with RLS policy applied.
pub async fn scoped_fetch<M: crate::table::Table + Policy + crate::db::FromRow>(
    scoped: &Scoped<'_>,
    builder: crate::query::SelectBuilder<M>,
) -> Result<Vec<M>, DbError> {
    let rows = scoped_fetch_all(scoped, builder).await?;
    rows.iter().map(|r| M::from_row(r)).collect()
}

/// Execute an UPDATE with RLS policy applied to the WHERE clause.
pub async fn scoped_update<M: crate::table::Table + Policy>(
    scoped: &Scoped<'_>,
    builder: crate::query::UpdateBuilder<M>,
) -> Result<u64, DbError> {
    let builder = match M::policy(scoped.context()) {
        Some(cond) => builder.filter(cond),
        None => builder,
    };
    let (sql, params) = builder.build();
    scoped.execute(&sql, &params).await
}

/// Execute a DELETE with RLS policy applied to the WHERE clause.
pub async fn scoped_delete<M: crate::table::Table + Policy>(
    scoped: &Scoped<'_>,
    builder: crate::query::DeleteBuilder<M>,
) -> Result<u64, DbError> {
    let builder = match M::policy(scoped.context()) {
        Some(cond) => builder.filter(cond),
        None => builder,
    };
    let (sql, params) = builder.build();
    scoped.execute(&sql, &params).await
}
