//! Soft delete configuration and utilities.
//!
//! Soft delete allows marking rows as deleted without physically removing them from the database.
//! Instead of `DELETE FROM table WHERE ...`, a soft delete performs
//! `UPDATE table SET deleted_at = CURRENT_TIMESTAMP WHERE ...`.
//!
//! # Configuration
//!
//! By default, soft-deleted rows are **hidden** from queries. Use [`set_show_deleted`] to
//! change this globally, or use `.with_deleted()` / `.only_deleted()` on individual queries.
//!
//! ```ignore
//! // Global: show deleted rows by default
//! reify::soft_delete::set_show_deleted(true);
//!
//! // Per-query: include deleted rows
//! User::find().with_deleted().fetch(&db).await?;
//!
//! // Per-query: only deleted rows
//! User::find().only_deleted().fetch(&db).await?;
//! ```
//!
//! # Declaring a soft-delete column
//!
//! Mark a column with `#[column(soft_delete)]`:
//!
//! ```ignore
//! #[derive(Table)]
//! #[table(name = "users")]
//! pub struct User {
//!     #[column(primary_key, auto_increment)]
//!     pub id: i64,
//!     pub email: String,
//!     #[column(soft_delete)]
//!     pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
//! }
//! ```
//!
//! The column must be `Option<DateTime<Utc>>` or `Option<NaiveDateTime>`.
//!
//! # Behavior
//!
//! - `User::find()` → `SELECT * FROM users WHERE deleted_at IS NULL`
//! - `User::find().with_deleted()` → `SELECT * FROM users` (no filter)
//! - `User::find().only_deleted()` → `SELECT * FROM users WHERE deleted_at IS NOT NULL`
//! - `User::delete().filter(...)` → `UPDATE users SET deleted_at = CURRENT_TIMESTAMP WHERE ...`
//! - `User::delete().filter(...).force()` → `DELETE FROM users WHERE ...` (hard delete)

use std::sync::atomic::{AtomicBool, Ordering};

/// Global flag: whether to show soft-deleted rows by default.
///
/// - `false` (default): `Model::find()` auto-injects `WHERE deleted_at IS NULL`
/// - `true`: `Model::find()` returns all rows including deleted ones
static SHOW_DELETED: AtomicBool = AtomicBool::new(false);

/// Set the global default for showing soft-deleted rows.
///
/// When `true`, `Model::find()` will **not** auto-inject the soft-delete filter,
/// returning all rows including deleted ones. Individual queries can still use
/// `.with_deleted()` or `.only_deleted()` to override.
///
/// Default: `false` (deleted rows are hidden).
///
/// # Example
///
/// ```ignore
/// // Show deleted rows globally (e.g., for admin dashboards)
/// reify::soft_delete::set_show_deleted(true);
///
/// // All queries now include deleted rows by default
/// let all_users = User::find().fetch(&db).await?;
///
/// // Restore default behavior
/// reify::soft_delete::set_show_deleted(false);
/// ```
pub fn set_show_deleted(show: bool) {
    SHOW_DELETED.store(show, Ordering::SeqCst);
}

/// Get the current global default for showing soft-deleted rows.
///
/// Returns `true` if deleted rows are shown by default, `false` otherwise.
pub fn show_deleted() -> bool {
    SHOW_DELETED.load(Ordering::SeqCst)
}

/// Soft-delete filter mode for queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SoftDeleteFilter {
    /// Apply the default filter based on global config.
    ///
    /// - If `show_deleted()` is `false`: filter out deleted rows (`deleted_at IS NULL`)
    /// - If `show_deleted()` is `true`: no filter applied
    #[default]
    Default,
    /// Include all rows (deleted and non-deleted).
    WithDeleted,
    /// Include only deleted rows (`deleted_at IS NOT NULL`).
    OnlyDeleted,
}
