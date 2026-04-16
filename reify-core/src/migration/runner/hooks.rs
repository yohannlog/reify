use crate::migration::error::MigrationError;
use crate::migration::plan::MigrationPlan;
use futures_core::future::BoxFuture;

/// Async hook called before or after each migration plan execution.
///
/// Receives a shared reference to the plan. Return `Err` from `before_each`
/// to abort the migration immediately (the plan will not be executed).
pub type MigrationHookFn = Box<
    dyn for<'a> Fn(&'a MigrationPlan) -> BoxFuture<'a, Result<(), MigrationError>> + Send + Sync,
>;

/// Async hook called when a migration plan fails.
///
/// Cannot cancel or modify the error — use it for logging, alerting, or
/// cleanup. The original error is propagated after the hook returns.
pub type MigrationErrorHookFn =
    Box<dyn for<'a> Fn(&'a MigrationPlan, &'a MigrationError) -> BoxFuture<'a, ()> + Send + Sync>;

/// Collection of lifecycle hooks for the migration runner.
///
/// Register hooks via the builder methods on [`MigrationRunner`]:
/// - [`on_before_each`](crate::migration::MigrationRunner::on_before_each)
/// - [`on_after_each`](crate::migration::MigrationRunner::on_after_each)
/// - [`on_migration_error`](crate::migration::MigrationRunner::on_migration_error)
#[derive(Default)]
pub struct MigrationHooks {
    /// Called before each plan's transaction begins.
    pub(super) before_each: Option<MigrationHookFn>,
    /// Called after each plan's transaction commits successfully.
    pub(super) after_each: Option<MigrationHookFn>,
    /// Called when a plan's transaction fails.
    pub(super) on_error: Option<MigrationErrorHookFn>,
}

impl MigrationHooks {
    /// Invoke `before_each` if registered. Returns its result directly.
    pub(super) async fn call_before(&self, plan: &MigrationPlan) -> Result<(), MigrationError> {
        if let Some(f) = &self.before_each {
            f(plan).await
        } else {
            Ok(())
        }
    }

    /// Invoke `after_each` if registered. Returns its result directly.
    pub(super) async fn call_after(&self, plan: &MigrationPlan) -> Result<(), MigrationError> {
        if let Some(f) = &self.after_each {
            f(plan).await
        } else {
            Ok(())
        }
    }

    /// Invoke `on_error` if registered. Never fails — errors in the hook are
    /// silently ignored to avoid masking the original migration error.
    pub(super) async fn call_error(&self, plan: &MigrationPlan, err: &MigrationError) {
        if let Some(f) = &self.on_error {
            f(plan, err).await;
        }
    }
}
