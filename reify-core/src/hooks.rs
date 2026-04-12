/// Optional lifecycle hooks for model types.
///
/// Implement this trait on your model to run logic before/after database
/// operations. All methods have default no-op implementations so you only
/// override what you need.
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

/// Marker: this type has no lifecycle hooks.
///
/// Automatically implemented for all types that do not implement `ModelHooks`.
/// Used by the hook-aware db helpers to avoid requiring `ModelHooks` bounds
/// on every model.
pub struct NoHooks;
