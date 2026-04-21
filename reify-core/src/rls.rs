use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::sync::Arc;

use crate::condition::Condition;
use crate::db::{BoxFuture, Database, DbError, DynDatabase, Row, TransactionFn};
use crate::value::Value;

// ── Policy context ─────────────────────────────────────────────────

/// Error returned when an RLS context lookup fails.
///
/// Used by [`RlsContext::require`] to distinguish a missing key from a
/// type mismatch — a silent `None` on mismatch would let a policy that
/// expects (e.g.) `i64` but receives `i32` bypass row-level security
/// entirely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RlsError {
    /// The key is not present in the context.
    Missing { key: &'static str },
    /// The key exists but the stored value has a different type than
    /// the one requested. Contains the requested and stored type names.
    TypeMismatch {
        key: &'static str,
        expected: &'static str,
        found: &'static str,
    },
}

impl std::fmt::Display for RlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RlsError::Missing { key } => {
                write!(f, "RLS context key `{key}` is missing")
            }
            RlsError::TypeMismatch {
                key,
                expected,
                found,
            } => write!(
                f,
                "RLS context key `{key}` has type `{found}`, expected `{expected}`"
            ),
        }
    }
}

impl std::error::Error for RlsError {}

/// Internal record for a context entry — keeps the original type name so
/// that [`RlsContext::require`] can surface precise mismatch diagnostics.
struct Entry {
    value: Arc<dyn Any + Send + Sync>,
    type_id: TypeId,
    type_name: &'static str,
}

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
    values: HashMap<&'static str, Arc<Entry>>,
}

impl RlsContext {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    /// Store a value in the context.
    pub fn set<T: Send + Sync + 'static>(mut self, key: &'static str, val: T) -> Self {
        self.values.insert(
            key,
            Arc::new(Entry {
                value: Arc::new(val),
                type_id: TypeId::of::<T>(),
                type_name: type_name::<T>(),
            }),
        );
        self
    }

    /// Retrieve a value from the context.
    ///
    /// Returns `None` if the key is missing **or** the stored value has a
    /// different type than `T`. Prefer [`RlsContext::require`] in policies
    /// — a silent `None` on type mismatch would cause the policy to
    /// return no restriction and bypass RLS.
    pub fn get<T: Send + Sync + 'static>(&self, key: &'static str) -> Option<&T> {
        self.values
            .get(key)
            .and_then(|e| e.value.downcast_ref::<T>())
    }

    /// Retrieve a value from the context, failing loudly on type mismatch.
    ///
    /// Returns [`RlsError::Missing`] if the key is absent, or
    /// [`RlsError::TypeMismatch`] if the stored value has a different
    /// type than `T`. Use this in policies to guarantee that a mismatch
    /// (e.g. storing `i32` where the policy expects `i64`) is an error,
    /// not a silent bypass.
    ///
    /// ```ignore
    /// impl Policy for Post {
    ///     fn policy(ctx: &RlsContext) -> Option<Condition> {
    ///         let tenant_id = ctx.require::<i64>("tenant_id").ok()?;
    ///         Some(Post::tenant_id.eq(*tenant_id))
    ///     }
    /// }
    /// ```
    pub fn require<T: Send + Sync + 'static>(&self, key: &'static str) -> Result<&T, RlsError> {
        let entry = self.values.get(key).ok_or(RlsError::Missing { key })?;
        if entry.type_id != TypeId::of::<T>() {
            return Err(RlsError::TypeMismatch {
                key,
                expected: type_name::<T>(),
                found: entry.type_name,
            });
        }
        // SAFETY of downcast: TypeId equality guarantees the concrete type
        // matches `T`. `expect` is unreachable but kept as a defensive
        // assertion rather than `unwrap_unchecked`.
        entry
            .value
            .downcast_ref::<T>()
            .ok_or(RlsError::TypeMismatch {
                key,
                expected: type_name::<T>(),
                found: entry.type_name,
            })
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
    /// and DELETE query.
    ///
    /// Return [`PolicyDecision::Restrict`] to scope access, or
    /// [`PolicyDecision::Unrestricted`] to allow full access (e.g. for
    /// admins). There is **no implicit default** — you must choose
    /// explicitly. Forgetting to read a context key must not collapse
    /// into unrestricted access: propagate [`RlsError`] via `?` on
    /// [`RlsContext::require`] instead.
    ///
    /// ```ignore
    /// impl Policy for Post {
    ///     fn policy(ctx: &RlsContext) -> Result<PolicyDecision, RlsError> {
    ///         let tenant_id = ctx.require::<i64>("tenant_id")?;
    ///         Ok(PolicyDecision::Restrict(Post::tenant_id.eq(*tenant_id)))
    ///     }
    /// }
    /// ```
    fn policy(ctx: &RlsContext) -> Result<PolicyDecision, RlsError>;
}

/// The outcome of evaluating a [`Policy`] against an [`RlsContext`].
///
/// Every policy call must pick one explicitly — the previous
/// `Option<Condition>` shape silently treated `None` as "no
/// restriction", which meant a forgotten context key granted full
/// access. `PolicyDecision` forces a conscious choice between
/// restricting, opening, or refusing.
#[derive(Debug, Clone)]
pub enum PolicyDecision {
    /// Inject `Condition` into the WHERE clause of every scoped query.
    Restrict(Condition),
    /// Allow full, unrestricted access — must be chosen explicitly
    /// (e.g. an admin role) rather than reached by omission.
    Unrestricted,
}

/// The closure accepted by [`Scoped::scoped_transaction`].
///
/// Unlike [`TransactionFn`], this closure receives `&Scoped` so that
/// RLS helpers (`scoped_fetch_all`, `scoped_update`, `scoped_delete`)
/// remain usable inside the transaction body.
pub type ScopedTransactionFn<'a> =
    Box<dyn for<'c> FnOnce(Scoped<'c>) -> BoxFuture<'c, ()> + Send + 'a>;

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
    inner: &'a dyn DynDatabase,
    ctx: RlsContext,
    /// When `true`, a [`RlsError`] raised by a policy (e.g. missing
    /// context key) causes the query to be rejected rather than
    /// treated as unrestricted. Defaults to `true` — opting out
    /// requires an explicit call to [`Scoped::allow_on_policy_error`].
    deny_on_policy_error: bool,
}

impl<'a> Scoped<'a> {
    /// Create a new `Scoped` wrapper.
    ///
    /// Policy errors ([`RlsError::Missing`] / [`RlsError::TypeMismatch`])
    /// are treated as **deny** by default — a policy that cannot be
    /// evaluated must never fall back to unrestricted access.
    pub fn new(db: &'a dyn DynDatabase, ctx: RlsContext) -> Self {
        Self {
            inner: db,
            ctx,
            deny_on_policy_error: true,
        }
    }

    /// Opt out of deny-by-default policy-error handling.
    ///
    /// When set, a policy returning [`RlsError`] is propagated to the
    /// caller as a `DbError` instead of being silently treated as
    /// deny. The default (`deny_on_policy_error = true`) is safer —
    /// flip this only if the caller explicitly handles the error.
    pub fn allow_on_policy_error(mut self) -> Self {
        self.deny_on_policy_error = false;
        self
    }

    /// Access the RLS context.
    pub fn context(&self) -> &RlsContext {
        &self.ctx
    }

    /// Run a closure inside a transaction, passing `&Scoped` so that
    /// RLS helpers (`scoped_fetch_all`, `scoped_update`, `scoped_delete`)
    /// remain usable inside the transaction body.
    ///
    /// ```ignore
    /// scoped.scoped_transaction(|s| Box::pin(async move {
    ///     scoped_fetch_all::<Post>(s, Post::find()).await?;
    ///     Ok(())
    /// })).await?;
    /// ```
    pub fn scoped_transaction<'s>(
        &'s self,
        f: ScopedTransactionFn<'s>,
    ) -> impl std::future::Future<Output = Result<(), DbError>> + Send + 's {
        let ctx = self.ctx.clone();
        let deny = self.deny_on_policy_error;
        async move {
            self.inner
                .transaction(Box::new(move |tx_db| {
                    let mut scoped_tx = Scoped::new(tx_db, ctx);
                    scoped_tx.deny_on_policy_error = deny;
                    f(scoped_tx)
                }))
                .await
        }
    }
}

/// Evaluate a policy and apply its decision to `builder.filter`.
///
/// Centralises the deny-by-default handling so that every scoped
/// helper (`scoped_fetch_all`, `scoped_update`, `scoped_delete`)
/// behaves identically in the face of [`RlsError`].
fn apply_policy<M, B>(
    scoped: &Scoped<'_>,
    builder: B,
    filter: impl FnOnce(B, Condition) -> B,
) -> Result<B, DbError>
where
    M: crate::table::Table + Policy,
{
    match M::policy(scoped.context()) {
        Ok(PolicyDecision::Restrict(cond)) => Ok(filter(builder, cond)),
        Ok(PolicyDecision::Unrestricted) => Ok(builder),
        Err(e) => {
            if scoped.deny_on_policy_error {
                Err(DbError::Other(format!(
                    "RLS policy evaluation failed for {}: {e}; query denied (deny-by-default)",
                    M::table_name()
                )))
            } else {
                Err(DbError::Other(format!(
                    "RLS policy evaluation failed for {}: {e}",
                    M::table_name()
                )))
            }
        }
    }
}

/// Delegate raw `Database` calls — policies are enforced at the builder level,
/// not at the SQL string level, so the raw trait just passes through.
///
/// **`transaction()`** is intentionally blocked: calling `Database::transaction`
/// on a `Scoped` would hand the closure a raw `&dyn DynDatabase` without RLS
/// context, silently bypassing row-level security.  Use
/// [`Scoped::scoped_transaction`] instead — it passes `&Scoped` to the closure
/// so that `scoped_fetch_all`, `scoped_update`, and `scoped_delete` remain
/// available and RLS policies stay enforced.
impl Database for Scoped<'_> {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        self.inner.execute(sql, params).await
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        self.inner.query(sql, params).await
    }

    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Row, DbError> {
        self.inner.query_one(sql, params).await
    }

    /// **Blocked** — always returns an error.
    ///
    /// Calling `Database::transaction` on a `Scoped` wrapper would give the
    /// closure a raw `&dyn DynDatabase` that cannot enforce RLS policies.
    /// Use [`Scoped::scoped_transaction`] instead.
    async fn transaction<'a>(&'a self, _f: TransactionFn<'a>) -> Result<(), DbError> {
        Err(DbError::Other(
            "Database::transaction() called on a Scoped wrapper. \
             This would bypass RLS policies because the closure receives a raw \
             &dyn DynDatabase without the RLS context. \
             Use Scoped::scoped_transaction() instead."
                .into(),
        ))
    }
}

// ── Scoped query helpers ───────────────────────────────────────────

/// Fetch all rows with RLS policy applied.
pub async fn scoped_fetch_all<M: crate::table::Table + Policy>(
    scoped: &Scoped<'_>,
    builder: crate::query::SelectBuilder<M>,
) -> Result<Vec<Row>, DbError> {
    let builder = apply_policy::<M, _>(scoped, builder, |b, cond| b.filter(cond))?;
    #[cfg(feature = "postgres")]
    {
        let q = builder.build_pg();
        return Database::query(scoped, &q.sql, &q.params).await;
    }
    #[cfg(not(feature = "postgres"))]
    {
        let (sql, params) = builder.build();
        Database::query(scoped, &sql, &params).await
    }
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
    let builder = apply_policy::<M, _>(scoped, builder, |b, cond| b.filter(cond))?;
    #[cfg(feature = "postgres")]
    {
        let q = builder
            .try_build_pg()
            .map_err(|e| DbError::Other(e.to_string()))?;
        return Database::execute(scoped, &q.sql, &q.params).await;
    }
    #[cfg(not(feature = "postgres"))]
    {
        let (sql, params) = builder
            .try_build()
            .map_err(|e| DbError::Other(e.to_string()))?;
        Database::execute(scoped, &sql, &params).await
    }
}

/// Execute a DELETE with RLS policy applied to the WHERE clause.
pub async fn scoped_delete<M: crate::table::Table + Policy>(
    scoped: &Scoped<'_>,
    builder: crate::query::DeleteBuilder<M>,
) -> Result<u64, DbError> {
    let builder = apply_policy::<M, _>(scoped, builder, |b, cond| b.filter(cond))?;
    #[cfg(feature = "postgres")]
    {
        let q = builder
            .try_build_pg()
            .map_err(|e| DbError::Other(e.to_string()))?;
        return Database::execute(scoped, &q.sql, &q.params).await;
    }
    #[cfg(not(feature = "postgres"))]
    {
        let (sql, params) = builder
            .try_build()
            .map_err(|e| DbError::Other(e.to_string()))?;
        Database::execute(scoped, &sql, &params).await
    }
}

// ── Tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Database, DbError, Row, TransactionFn};
    use crate::value::Value;
    use std::sync::{Arc, Mutex};

    // ── Recording stub DB ───────────────────────────────────────────

    /// A stub database that records all SQL executed through it.
    struct RecordingDb {
        log: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingDb {
        fn new() -> (Self, Arc<Mutex<Vec<String>>>) {
            let log = Arc::new(Mutex::new(Vec::new()));
            (Self { log: log.clone() }, log)
        }
    }

    impl Database for RecordingDb {
        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<u64, DbError> {
            self.log.lock().unwrap().push(sql.to_string());
            Ok(0)
        }

        async fn query(&self, sql: &str, _params: &[Value]) -> Result<Vec<Row>, DbError> {
            self.log.lock().unwrap().push(sql.to_string());
            Ok(vec![])
        }

        async fn query_one(&self, sql: &str, _params: &[Value]) -> Result<Row, DbError> {
            self.log.lock().unwrap().push(sql.to_string());
            Err(DbError::Query("no rows".into()))
        }

        async fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> Result<(), DbError> {
            // Simulate a transaction by just calling f with self
            f(self).await
        }
    }

    // ── Minimal Table + Policy stub ─────────────────────────────────

    struct TenantPost;

    impl crate::table::Table for TenantPost {
        fn table_name() -> &'static str {
            "posts"
        }
        fn column_names() -> &'static [&'static str] {
            &["id", "tenant_id", "title"]
        }
        fn as_values(&self) -> Vec<Value> {
            vec![]
        }
    }

    impl Policy for TenantPost {
        fn policy(ctx: &RlsContext) -> Result<PolicyDecision, RlsError> {
            let tenant_id = ctx.require::<i64>("tenant_id")?;
            Ok(PolicyDecision::Restrict(Condition::Eq(
                "tenant_id",
                Value::I64(*tenant_id),
            )))
        }
    }

    // ── Tests ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn scoped_transaction_passes_queries_through() {
        let (db, log) = RecordingDb::new();
        let ctx = RlsContext::new().set("tenant_id", 42i64);
        let scoped = Scoped::new(&db, ctx);

        // Use scoped_transaction — the closure receives &Scoped so raw
        // queries still go through the inner connection.
        let result = scoped
            .scoped_transaction(Box::new(|s: Scoped<'_>| {
                Box::pin(async move {
                    Database::query(&s, "SELECT 1", &[]).await?;
                    Database::execute(&s, "UPDATE t SET x = 1", &[]).await?;
                    Ok(())
                })
            }))
            .await;

        assert!(result.is_ok());

        // Both queries should have been recorded
        let queries = log.lock().unwrap();
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0], "SELECT 1");
        assert_eq!(queries[1], "UPDATE t SET x = 1");
    }

    #[tokio::test]
    async fn scoped_query_outside_transaction_applies_rls() {
        let (db, log) = RecordingDb::new();
        let ctx = RlsContext::new().set("tenant_id", 7i64);
        let scoped = Scoped::new(&db, ctx);

        let builder = crate::query::SelectBuilder::<TenantPost>::new();
        let _ = scoped_fetch_all::<TenantPost>(&scoped, builder).await;

        let queries = log.lock().unwrap();
        assert_eq!(queries.len(), 1);
        assert!(
            queries[0].contains("\"tenant_id\" ="),
            "RLS filter should be applied, got: {}",
            queries[0]
        );
    }

    #[tokio::test]
    async fn scoped_transaction_applies_rls_inside_transaction() {
        let (db, log) = RecordingDb::new();
        let ctx = RlsContext::new().set("tenant_id", 55i64);
        let scoped = Scoped::new(&db, ctx);

        // Use scoped_transaction so the closure receives &Scoped and can
        // call scoped_fetch_all — the RLS filter must be injected even
        // inside the transaction body.
        let result = scoped
            .scoped_transaction(Box::new(|s: Scoped<'_>| {
                Box::pin(async move {
                    let builder = crate::query::SelectBuilder::<TenantPost>::new();
                    scoped_fetch_all::<TenantPost>(&s, builder).await?;
                    Ok(())
                })
            }))
            .await;

        assert!(result.is_ok());

        let queries = log.lock().unwrap();
        assert_eq!(queries.len(), 1, "expected exactly one query");
        assert!(
            queries[0].contains("\"tenant_id\" ="),
            "RLS filter must be applied inside transaction, got: {}",
            queries[0]
        );
    }

    #[tokio::test]
    async fn database_transaction_on_scoped_is_blocked() {
        let (db, _log) = RecordingDb::new();
        let ctx = RlsContext::new().set("tenant_id", 99i64);
        let scoped = Scoped::new(&db, ctx);

        // Calling Database::transaction on a Scoped must return an error
        // to prevent RLS bypass — the closure would receive a raw
        // &dyn DynDatabase without the RLS context.
        let result = Database::transaction(
            &scoped,
            Box::new(|_tx_db: &dyn DynDatabase| Box::pin(async { Ok(()) })),
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("scoped_transaction"),
            "error should mention scoped_transaction, got: {err}",
        );
    }

    #[tokio::test]
    async fn scoped_transaction_preserves_rls_context() {
        let (db, log) = RecordingDb::new();
        let ctx = RlsContext::new().set("tenant_id", 99i64);
        let scoped = Scoped::new(&db, ctx);

        // scoped_transaction passes &Scoped to the closure, so the RLS
        // context is available and scoped_* helpers work correctly.
        let flag = Arc::new(Mutex::new(false));
        let flag_clone = flag.clone();

        let result = scoped
            .scoped_transaction(Box::new(move |s: Scoped<'_>| {
                let flag = flag_clone.clone();
                Box::pin(async move {
                    // Verify the RLS context is propagated into the transaction.
                    let tid = s.context().get::<i64>("tenant_id");
                    assert_eq!(tid, Some(&99i64));

                    Database::query(&s, "SELECT 1", &[]).await?;
                    *flag.lock().unwrap() = true;
                    Ok(())
                })
            }))
            .await;

        assert!(result.is_ok());
        assert!(
            *flag.lock().unwrap(),
            "transaction closure should have executed"
        );
        assert_eq!(log.lock().unwrap().len(), 1);
    }
}
