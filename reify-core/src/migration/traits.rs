use super::context::MigrationContext;

/// A single, versioned database migration.
///
/// Implement this trait for complex migrations that cannot be auto-detected
/// (renames, type changes, data migrations, etc.).
///
/// # Example
///
/// ```ignore
/// pub struct AddUserCity;
///
/// impl Migration for AddUserCity {
///     fn version(&self) -> &'static str { "20240320_000001_add_user_city" }
///     fn description(&self) -> &'static str { "Add city column to users" }
///
///     fn up(&self, ctx: &mut MigrationContext) {
///         ctx.add_column("users", "city", "TEXT NOT NULL DEFAULT ''");
///     }
///
///     fn down(&self, ctx: &mut MigrationContext) {
///         ctx.drop_column("users", "city");
///     }
/// }
/// ```
pub trait Migration: Send + Sync {
    /// Unique version string — used as the primary key in the tracking table.
    ///
    /// Convention: `YYYYMMDD_NNNNNN_snake_case_description`
    fn version(&self) -> &'static str;

    /// Human-readable description shown in `reify status` output.
    fn description(&self) -> &'static str;

    /// Apply the migration (forward direction).
    fn up(&self, ctx: &mut MigrationContext);

    /// Revert the migration (backward direction).
    ///
    /// Only called when `is_reversible()` returns `true`.
    /// Default implementation is a no-op (migration is irreversible).
    fn down(&self, ctx: &mut MigrationContext) {
        let _ = ctx;
    }

    /// Whether this migration can be rolled back via `down()`.
    ///
    /// Return `false` for destructive migrations (DROP TABLE, DROP COLUMN, …)
    /// where reversal is impossible or unsafe.
    fn is_reversible(&self) -> bool {
        true
    }
}
