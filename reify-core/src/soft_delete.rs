//! Soft delete configuration and utilities.
//!
//! Soft delete allows marking rows as deleted without physically removing
//! them from the database. Instead of `DELETE FROM table WHERE …`, a soft
//! delete performs `UPDATE table SET deleted_at = CURRENT_TIMESTAMP WHERE …`.
//!
//! # Filtering policy
//!
//! By design, soft-deleted rows are **always** hidden from default queries.
//! There is no global "show deleted" toggle: a process-wide flag is unsafe
//! under concurrency (one task flipping it would leak deleted rows to
//! every other request running on the same runtime). Override per query:
//!
//! ```ignore
//! // Default: filter out deleted rows.
//! User::find().fetch(&db).await?;
//! // → SELECT … FROM users WHERE deleted_at IS NULL
//!
//! // Include deleted rows (admin / trash views).
//! User::find().with_deleted().fetch(&db).await?;
//! // → SELECT … FROM users
//!
//! // Show only deleted rows.
//! User::find().only_deleted().fetch(&db).await?;
//! // → SELECT … FROM users WHERE deleted_at IS NOT NULL
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
//! - `User::delete().filter(…)` → `UPDATE users SET deleted_at = CURRENT_TIMESTAMP WHERE …`
//! - `User::delete().filter(…).force()` → `DELETE FROM users WHERE …` (hard delete)

/// Soft-delete filter mode for queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SoftDeleteFilter {
    /// Apply the safe default: hide soft-deleted rows
    /// (`WHERE deleted_at IS NULL`).
    ///
    /// Override per query with [`SoftDeleteFilter::WithDeleted`] (no
    /// filter) or [`SoftDeleteFilter::OnlyDeleted`] (`IS NOT NULL`).
    #[default]
    Default,
    /// Include all rows (deleted and non-deleted).
    WithDeleted,
    /// Include only deleted rows (`deleted_at IS NOT NULL`).
    OnlyDeleted,
}
