use crate::db::DbError;

/// Error type for migration operations.
#[derive(Debug)]
pub enum MigrationError {
    /// Underlying database error.
    Db(DbError),
    /// A migration is not reversible but rollback was requested.
    NotReversible(String),
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
