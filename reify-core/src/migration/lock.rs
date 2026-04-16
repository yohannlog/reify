use super::error::MigrationError;
use crate::db::Database;
use crate::query::Dialect;
use crate::value::Value;

/// Stale-lock TTL: a lock held longer than this many minutes is considered
/// abandoned (e.g. the process was SIGKILL'd) and will be forcibly reclaimed.
const STALE_LOCK_MINUTES: u32 = 10;

/// DDL for the lock table, parameterised by dialect.
///
/// PostgreSQL uses `TIMESTAMPTZ` and `BOOLEAN`; MySQL uses `DATETIME` and `TINYINT(1)`.
fn create_lock_table_sql(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::Mysql => {
            "CREATE TABLE IF NOT EXISTS `_reify_migrations_lock` (\
             `id`        SMALLINT    NOT NULL DEFAULT 1 PRIMARY KEY,\
             `locked`    TINYINT(1)  NOT NULL DEFAULT 0,\
             `locked_by` TEXT,\
             `locked_at` DATETIME\
             );"
        }
        _ => {
            "CREATE TABLE IF NOT EXISTS \"_reify_migrations_lock\" (\
             \"id\"        SMALLINT  NOT NULL DEFAULT 1 PRIMARY KEY CHECK (\"id\" = 1),\
             \"locked\"    BOOLEAN   NOT NULL DEFAULT false,\
             \"locked_by\" TEXT,\
             \"locked_at\" TIMESTAMPTZ\
             );"
        }
    }
}

/// Build the acquire UPDATE, which also reclaims stale locks older than `STALE_LOCK_MINUTES`.
fn acquire_sql(dialect: Dialect) -> String {
    match dialect {
        Dialect::Mysql => format!(
            "UPDATE `_reify_migrations_lock` \
             SET `locked` = 1, `locked_by` = ?, `locked_at` = NOW() \
             WHERE `id` = 1 AND (`locked` = 0 OR `locked_at` < NOW() - INTERVAL {STALE_LOCK_MINUTES} MINUTE);"
        ),
        _ => format!(
            "UPDATE \"_reify_migrations_lock\" \
             SET \"locked\" = true, \"locked_by\" = ?, \"locked_at\" = NOW() \
             WHERE \"id\" = 1 AND (\"locked\" = false OR \"locked_at\" < NOW() - INTERVAL '{STALE_LOCK_MINUTES} minutes');"
        ),
    }
}

/// Build a human-readable lock-holder identifier: `reify@<hostname>:<pid>`.
fn lock_holder_id() -> String {
    let hostname = std::env::var("HOSTNAME")
        .or_else(|_| {
            // Fallback: read /etc/hostname on Linux
            std::fs::read_to_string("/etc/hostname").map(|s| s.trim().to_string())
        })
        .unwrap_or_else(|_| "unknown".to_string());
    format!("reify@{}:{}", hostname, std::process::id())
}

/// Distributed lock for the migration runner.
///
/// Prevents concurrent deployments from running migrations simultaneously.
/// Uses a single sentinel row (id=1) in `_reify_migrations_lock`.
///
/// Stale locks (held longer than [`STALE_LOCK_MINUTES`] minutes) are
/// automatically reclaimed by the next `acquire()` call, so a crashed process
/// never permanently blocks deployments.
pub struct MigrationLock;

impl MigrationLock {
    /// Ensure the lock table exists and the sentinel row is present.
    pub async fn ensure(db: &impl Database, dialect: Dialect) -> Result<(), MigrationError> {
        db.execute(create_lock_table_sql(dialect), &[]).await?;
        // Insert the sentinel row if it doesn't exist yet.
        // Ignore errors — the row may already exist on any DB.
        let insert = match dialect {
            Dialect::Mysql => {
                "INSERT IGNORE INTO `_reify_migrations_lock` (`id`, `locked`) VALUES (1, 0);"
            }
            _ => {
                "INSERT INTO \"_reify_migrations_lock\" (\"id\", \"locked\") VALUES (1, false) ON CONFLICT DO NOTHING;"
            }
        };
        let _ = db.execute(insert, &[]).await;
        Ok(())
    }

    /// Acquire the lock.
    ///
    /// Atomically sets `locked = true` where `locked = false` **or** where
    /// the lock has been held longer than [`STALE_LOCK_MINUTES`] minutes
    /// (stale-lock reclaim).
    ///
    /// Returns `Err(MigrationError::Locked)` if the lock is actively held by
    /// another process within the TTL window.
    pub async fn acquire(db: &impl Database, dialect: Dialect) -> Result<(), MigrationError> {
        let holder = lock_holder_id();
        let sql = acquire_sql(dialect);
        let rows_affected = db.execute(&sql, &[Value::String(holder)]).await?;

        if rows_affected == 0 {
            // Lock is actively held — fetch holder info for the error message.
            let lock_table = match dialect {
                Dialect::Mysql => "`_reify_migrations_lock`",
                _ => "\"_reify_migrations_lock\"",
            };
            let rows = db
                .query(
                    &format!(
                        "SELECT \"locked_by\", CAST(\"locked_at\" AS TEXT) AS \"locked_at\" \
                         FROM {lock_table} WHERE \"id\" = 1;"
                    ),
                    &[],
                )
                .await
                .map_err(MigrationError::Db)?;

            let (locked_by, locked_at) = rows
                .first()
                .map(|r| (r.get_string("locked_by"), r.get_string("locked_at")))
                .unwrap_or((None, None));

            return Err(MigrationError::Locked {
                locked_by,
                locked_at,
            });
        }

        Ok(())
    }

    /// Release the lock.
    pub async fn release(db: &impl Database) -> Result<(), MigrationError> {
        db.execute(
            "UPDATE \"_reify_migrations_lock\" \
             SET \"locked\" = false, \"locked_by\" = NULL, \"locked_at\" = NULL \
             WHERE \"id\" = 1;",
            &[],
        )
        .await?;
        Ok(())
    }
}
