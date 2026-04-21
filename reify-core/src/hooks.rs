/// Optional synchronous lifecycle hooks for model types.
///
/// Implement this trait on your model to run logic before/after database
/// operations. All methods have default no-op implementations so you only
/// override what you need.
///
/// For async hooks (await-able, short-circuit via `Err`) see [`AsyncModelHooks`].
///
/// # Example
/// ```ignore
/// impl ModelHooks for User {
///     fn before_insert(&mut self) {
///         self.created_at = chrono::Utc::now();
///     }
///     fn after_insert(&self) {
///         tracing::info!(id = self.id, "User inserted");
///     }
/// }
/// ```
pub trait ModelHooks {
    /// Called before an INSERT. Mutate `self` to set timestamps, defaults, etc.
    fn before_insert(&mut self) {}
    /// Called after a successful INSERT.
    fn after_insert(&self) {}
    /// Called before an UPDATE. Mutate `self` to set `updated_at`, etc.
    fn before_update(&mut self) {}
    /// Called before a DELETE.
    fn before_delete(&self) {}
}

// ── HookError ────────────────────────────────────────────────────────

/// Error returned by async lifecycle hooks.
///
/// - [`HookError::Reject`] — the operation is intentionally vetoed. The
///   message is surfaced to the caller as [`crate::db::DbError::Other`] with
///   a `"hook rejected: …"` prefix. No SQL is executed.
/// - [`HookError::Internal`] — an unexpected failure inside the hook (e.g. a
///   cache write error). Propagated as [`crate::db::DbError::Other`].
///
/// # Example
///
/// ```ignore
/// async fn before_insert(&self, model: &User) -> Result<(), HookError> {
///     if model.email.is_empty() {
///         return Err(HookError::Reject("email must not be empty".into()));
///     }
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookError {
    /// The hook intentionally vetoes the operation.
    ///
    /// The SQL statement is **not** executed. The error surfaces to the caller
    /// as `DbError::Other("hook rejected: <message>")`.
    Reject(String),
    /// An unexpected internal failure inside the hook body.
    ///
    /// Propagated as `DbError::Other("hook error: <message>")`.
    Internal(String),
}

impl std::fmt::Display for HookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HookError::Reject(msg) => write!(f, "hook rejected: {msg}"),
            HookError::Internal(msg) => write!(f, "hook error: {msg}"),
        }
    }
}

impl std::error::Error for HookError {}

impl From<HookError> for crate::db::DbError {
    fn from(e: HookError) -> Self {
        crate::db::DbError::Other(e.to_string())
    }
}

// ── AsyncModelHooks ──────────────────────────────────────────────────

/// Async lifecycle hooks for model types.
///
/// All methods are `async` and return `Result<(), HookError>`, allowing hooks
/// to perform I/O (cache writes, search indexing, audit logging) and to
/// short-circuit the operation via [`HookError::Reject`].
///
/// All methods have default no-op implementations — override only what you need.
///
/// Use with [`crate::db::insert_with_async_hooks`],
/// [`crate::db::update_with_async_hooks`], and
/// [`crate::db::delete_with_async_hooks`].
///
/// # Example
///
/// ```ignore
/// use reify_core::hooks::{AsyncModelHooks, HookError};
///
/// impl AsyncModelHooks for User {
///     async fn before_insert(&mut self) -> Result<(), HookError> {
///         if self.email.is_empty() {
///             return Err(HookError::Reject("email required".into()));
///         }
///         // Optionally mutate self — e.g. set created_at
///         Ok(())
///     }
///
///     async fn after_insert(&self, rows_affected: u64) -> Result<(), HookError> {
///         search_index.upsert(self.id, &self.email)
///             .await
///             .map_err(|e| HookError::Internal(e.to_string()))?;
///         Ok(())
///     }
///
///     async fn before_delete(&self) -> Result<(), HookError> {
///         if self.is_superadmin {
///             return Err(HookError::Reject("cannot delete superadmin".into()));
///         }
///         Ok(())
///     }
/// }
/// ```
#[allow(async_fn_in_trait)]
pub trait AsyncModelHooks: Sized {
    /// Called before an INSERT.
    ///
    /// `&mut self` allows mutating the model (e.g. setting `created_at`).
    /// Return `Err(HookError::Reject(_))` to abort without executing SQL.
    async fn before_insert(&mut self) -> Result<(), HookError> {
        Ok(())
    }

    /// Called after a successful INSERT.
    ///
    /// `rows_affected` is the count returned by the database driver.
    /// Return `Err` to signal a post-insert failure (e.g. cache write failed).
    async fn after_insert(&self, rows_affected: u64) -> Result<(), HookError> {
        let _ = rows_affected;
        Ok(())
    }

    /// Called before an UPDATE.
    ///
    /// `&mut self` allows mutating the model (e.g. bumping `updated_at`).
    /// Return `Err(HookError::Reject(_))` to abort without executing SQL.
    async fn before_update(&mut self) -> Result<(), HookError> {
        Ok(())
    }

    /// Called after a successful UPDATE.
    async fn after_update(&self, rows_affected: u64) -> Result<(), HookError> {
        let _ = rows_affected;
        Ok(())
    }

    /// Called before a DELETE.
    ///
    /// Return `Err(HookError::Reject(_))` to abort without executing SQL.
    async fn before_delete(&self) -> Result<(), HookError> {
        Ok(())
    }

    /// Called after a successful DELETE.
    async fn after_delete(&self, rows_affected: u64) -> Result<(), HookError> {
        let _ = rows_affected;
        Ok(())
    }
}
