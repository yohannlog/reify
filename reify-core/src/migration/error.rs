use crate::db::DbError;

/// Error type for migration operations.
#[derive(Debug)]
pub enum MigrationError {
    /// Underlying database error.
    Db(DbError),
    /// A migration is not reversible but rollback was requested.
    NotReversible(String),
    /// The stored checksum for an applied migration no longer matches the
    /// checksum computed from the current code — the migration was modified
    /// after being applied.
    ChecksumMismatch {
        /// Migration version string.
        version: String,
        /// SHA-256 hex digest stored in the database (applied previously).
        stored: String,
        /// SHA-256 hex digest computed from the current code.
        computed: String,
    },
    /// The migration lock is already held by another process.
    Locked {
        /// Identity of the lock holder, if available.
        locked_by: Option<String>,
        /// Timestamp when the lock was acquired, if available.
        locked_at: Option<String>,
    },
    /// A migration exceeded its declared timeout and was aborted.
    ///
    /// The transaction was rolled back by the database. No tracking-table row
    /// was inserted, so the migration is still pending and can be retried.
    TimedOut {
        /// Migration version string.
        version: String,
        /// The timeout that was exceeded, in seconds.
        timeout_secs: u64,
    },
    /// Generic migration error.
    Other(String),
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationError::Db(e) => write!(f, "database error: {e}"),
            MigrationError::NotReversible(v) => {
                write!(f, "migration '{v}' is not reversible")
            }
            MigrationError::ChecksumMismatch {
                version,
                stored,
                computed,
            } => {
                write!(
                    f,
                    "checksum mismatch for migration '{version}': \
                     stored={}, computed={} — migration was modified after being applied",
                    &stored[..stored.len().min(8)],
                    &computed[..computed.len().min(8)],
                )
            }
            MigrationError::Locked {
                locked_by,
                locked_at,
            } => match locked_by {
                Some(by) => write!(
                    f,
                    "migration lock is already held by '{by}'{}",
                    locked_at
                        .as_deref()
                        .map(|t| format!(" (since {t})"))
                        .unwrap_or_default()
                ),
                None => write!(f, "migration lock is already held by another process"),
            },
            MigrationError::TimedOut {
                version,
                timeout_secs,
            } => write!(
                f,
                "migration '{version}' timed out after {timeout_secs}s — \
                 transaction rolled back, migration is still pending"
            ),
            MigrationError::Other(msg) => write!(f, "migration error: {msg}"),
        }
    }
}

impl std::error::Error for MigrationError {}

impl From<DbError> for MigrationError {
    fn from(e: DbError) -> Self {
        MigrationError::Db(e)
    }
}
