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
///     fn comment(&self) -> Option<&'static str> {
///         Some("Needed for the regional billing feature (JIRA-42).")
///     }
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

    /// Optional free-text comment stored in the changelog.
    ///
    /// Use this to record the business reason, ticket reference, or any
    /// context that doesn't fit in the short `description`.
    /// Defaults to `None` (no comment).
    fn comment(&self) -> Option<&'static str> {
        None
    }

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

    /// Maximum wall-clock time allowed for this migration's transaction.
    ///
    /// When `Some(duration)`, the runner wraps the transaction in
    /// `tokio::time::timeout`. If the deadline is exceeded the transaction is
    /// rolled back, no tracking-table row is written (the migration remains
    /// pending), and `MigrationError::TimedOut` is returned.
    ///
    /// Use this for migrations that touch large tables and could lock rows for
    /// an unacceptable duration in production. A timed-out migration can be
    /// retried after the root cause (missing index, table size, etc.) is
    /// addressed.
    ///
    /// Defaults to `None` — no timeout.
    fn timeout(&self) -> Option<std::time::Duration> {
        None
    }
}
