use super::MigrationRunner;
use super::entries::upsert_migration_sql;
use crate::db::Database;
use crate::migration::context::MigrationContext;
use crate::migration::error::MigrationError;
use crate::migration::lock::MigrationLock;
use crate::migration::plan::compute_checksum;
use crate::query::Dialect;
use crate::value::Value;
use std::collections::HashSet;
use tokio::time::timeout as tokio_timeout;

impl MigrationRunner {
    /// Apply all pending migrations (auto-diff + manual + views) against the database.
    ///
    /// Creates the `_reify_migrations` tracking table if it doesn't exist.
    /// Acquires a distributed lock before running and releases it afterwards.
    ///
    /// The SQL dialect is auto-detected from the database connection. Use
    /// `with_dialect()` to override if needed.
    ///
    /// If a migration's checksum no longer matches what was stored when it was
    /// first applied, `MigrationError::ChecksumMismatch` is returned immediately
    /// (the migration was modified after being applied).
    ///
    /// Each migration is executed inside a transaction: the DDL statements and the
    /// tracking-table INSERT are committed atomically. On PostgreSQL this gives full
    /// ACID guarantees; on MySQL/MariaDB DDL statements cause an implicit commit, so
    /// the transaction boundary still protects the tracking INSERT from being lost.
    /// # Warning — additive-only auto-diff
    ///
    /// Auto-diff migrations registered via [`add_table`](MigrationRunner::add_table)
    /// are **additive only**: `CREATE TABLE` and `ADD COLUMN` are emitted
    /// automatically, but drops, renames, and type changes are **silently skipped**.
    ///
    /// Use [`diff`](MigrationRunner::diff) to inspect what the runner detected
    /// but will not apply, and write a manual migration for those changes.
    pub async fn run(&self, db: &impl Database) -> Result<(), MigrationError> {
        let dialect = self.resolve_dialect(db);
        self.ensure_tracking_table(db, dialect).await?;
        MigrationLock::ensure(db, dialect).await?;
        MigrationLock::acquire(db, dialect).await?;

        let result = self.run_inner(db, dialect).await;

        // Always release, even on error. Panics are not caught here — the
        // stale-lock TTL in acquire() handles crashed processes automatically.
        MigrationLock::release(db, dialect).await.ok();

        result
    }

    /// Inner run logic — called after the lock is acquired.
    async fn run_inner(&self, db: &impl Database, dialect: Dialect) -> Result<(), MigrationError> {
        let applied = self.applied_checksums(db, dialect).await?;
        let applied_versions: HashSet<String> = applied.keys().cloned().collect();

        // Verify checksums for already-applied manual migrations.
        // If the code was modified after the migration was applied, abort.
        for m in &self.manual {
            if let Some(stored) = applied.get(m.version()) {
                let mut ctx = MigrationContext::new();
                m.up(&mut ctx);
                let computed = compute_checksum(&ctx.into_statements());
                if computed != *stored {
                    return Err(MigrationError::ChecksumMismatch {
                        version: m.version().to_string(),
                        stored: stored.clone(),
                        computed,
                    });
                }
            }
        }

        // Collect all pending plans up-front (no DB side-effects yet).
        let auto_plans = self.auto_diff_plans(db, &applied_versions, dialect).await?;
        let view_plans = self.view_plans(&applied_versions);
        let mat_view_plans = self.mat_view_plans(&applied_versions);
        let manual_plans = self.manual_plans(&applied_versions);

        // Execute each plan atomically: DDL statements + tracking INSERT in one transaction.
        let all_plans = auto_plans
            .into_iter()
            .chain(view_plans)
            .chain(mat_view_plans)
            .chain(manual_plans);
        // Use upsert to handle edge cases where a migration is re-run
        // (e.g., after a partial failure or manual intervention).
        let upsert_sql = upsert_migration_sql(dialect);

        for plan in all_plans {
            self.hooks.call_before(&plan).await?;
            let stmts = plan.statements.clone();
            let version = plan.version.clone();
            let description = plan.description.clone();
            let comment = plan.comment.clone();
            let checksum = plan.checksum.clone();
            let upsert_sql = upsert_sql.clone();
            let result = {
                let fut = db.transaction(Box::new(move |txn| {
                    Box::pin(async move {
                        for stmt in &stmts {
                            txn.execute(stmt, &[]).await?;
                        }
                        txn.execute(
                            &upsert_sql,
                            &[
                                Value::String(version),
                                Value::String(description),
                                Value::String(checksum),
                                match comment {
                                    Some(c) => Value::String(c),
                                    None => Value::Null,
                                },
                            ],
                        )
                        .await?;
                        Ok(())
                    })
                }));
                match plan.timeout {
                    Some(dur) => tokio_timeout(dur, fut)
                        .await
                        .map_err(|_| MigrationError::TimedOut {
                            version: plan.version.clone(),
                            timeout_secs: dur.as_secs(),
                        })
                        .and_then(|r| r.map_err(MigrationError::Db)),
                    None => fut.await.map_err(MigrationError::Db),
                }
            };
            match result {
                Ok(()) => self.hooks.call_after(&plan).await?,
                Err(e) => {
                    self.hooks.call_error(&plan, &e).await;
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Apply all pending migrations that were registered **after** `since`.
    ///
    /// `since` is compared against the `applied_at` timestamp of already-applied
    /// migrations. Pending migrations (never applied) are always included.
    /// Format: any prefix of `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SS` — compared
    /// lexicographically against the stored timestamp string.
    pub async fn run_since(&self, db: &impl Database, since: &str) -> Result<(), MigrationError> {
        let dialect = self.resolve_dialect(db);
        self.ensure_tracking_table(db, dialect).await?;
        MigrationLock::ensure(db, dialect).await?;
        MigrationLock::acquire(db, dialect).await?;

        let result = self.run_since_inner(db, since, dialect).await;

        MigrationLock::release(db, dialect).await.ok();

        result
    }

    async fn run_since_inner(
        &self,
        db: &impl Database,
        since: &str,
        dialect: Dialect,
    ) -> Result<(), MigrationError> {
        let applied = self.applied_checksums(db, dialect).await?;
        let timestamps = self.applied_timestamps(db, dialect).await?;

        // Checksum verification for already-applied manual migrations.
        for m in &self.manual {
            if let Some(stored) = applied.get(m.version()) {
                let mut ctx = MigrationContext::new();
                m.up(&mut ctx);
                let computed = compute_checksum(&ctx.into_statements());
                if computed != *stored {
                    return Err(MigrationError::ChecksumMismatch {
                        version: m.version().to_string(),
                        stored: stored.clone(),
                        computed,
                    });
                }
            }
        }

        // Build the set of versions to skip: applied AND applied_at < since.
        // Versions applied at or after `since` are re-included (treated as pending).
        let skip: HashSet<String> = applied
            .keys()
            .filter(|v| {
                timestamps
                    .get(*v)
                    .map(|ts| ts.as_str() < since)
                    .unwrap_or(false)
            })
            .cloned()
            .collect();

        let auto_plans = self.auto_diff_plans(db, &skip, dialect).await?;
        let view_plans = self.view_plans(&skip);
        let mat_view_plans = self.mat_view_plans(&skip);
        let manual_plans = self.manual_plans(&skip);

        // Build the dialect-appropriate upsert SQL once, outside the loop.
        let upsert_sql = upsert_migration_sql(dialect);

        let all_plans = auto_plans
            .into_iter()
            .chain(view_plans)
            .chain(mat_view_plans)
            .chain(manual_plans);
        for plan in all_plans {
            self.hooks.call_before(&plan).await?;
            let stmts = plan.statements.clone();
            let version = plan.version.clone();
            let description = plan.description.clone();
            let comment = plan.comment.clone();
            let checksum = plan.checksum.clone();
            let upsert_sql = upsert_sql.clone();
            let result = {
                let fut = db.transaction(Box::new(move |txn| {
                    Box::pin(async move {
                        for stmt in &stmts {
                            txn.execute(stmt, &[]).await?;
                        }
                        txn.execute(
                            &upsert_sql,
                            &[
                                Value::String(version),
                                Value::String(description),
                                Value::String(checksum),
                                match comment {
                                    Some(c) => Value::String(c),
                                    None => Value::Null,
                                },
                            ],
                        )
                        .await?;
                        Ok(())
                    })
                }));
                match plan.timeout {
                    Some(dur) => tokio_timeout(dur, fut)
                        .await
                        .map_err(|_| MigrationError::TimedOut {
                            version: plan.version.clone(),
                            timeout_secs: dur.as_secs(),
                        })
                        .and_then(|r| r.map_err(MigrationError::Db)),
                    None => fut.await.map_err(MigrationError::Db),
                }
            };
            match result {
                Ok(()) => self.hooks.call_after(&plan).await?,
                Err(e) => {
                    self.hooks.call_error(&plan, &e).await;
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Inner interactive run logic — called after the lock is acquired.
    ///
    /// Same as `run_inner` but calls `confirm(&plan)` before each migration.
    /// Returns `UserAborted` if the callback returns `false`.
    pub(super) async fn run_interactive_inner<F>(
        &self,
        db: &impl Database,
        confirm: F,
        dialect: Dialect,
    ) -> Result<(), MigrationError>
    where
        F: Fn(&crate::migration::plan::MigrationPlan) -> bool + Send + Sync,
    {
        let applied = self.applied_checksums(db, dialect).await?;
        let applied_versions: HashSet<String> = applied.keys().cloned().collect();

        // Verify checksums for already-applied manual migrations.
        for m in &self.manual {
            if let Some(stored) = applied.get(m.version()) {
                let mut ctx = MigrationContext::new();
                m.up(&mut ctx);
                let computed = compute_checksum(&ctx.into_statements());
                if computed != *stored {
                    return Err(MigrationError::ChecksumMismatch {
                        version: m.version().to_string(),
                        stored: stored.clone(),
                        computed,
                    });
                }
            }
        }

        // Collect all pending plans up-front.
        let auto_plans = self.auto_diff_plans(db, &applied_versions, dialect).await?;
        let view_plans = self.view_plans(&applied_versions);
        let mat_view_plans = self.mat_view_plans(&applied_versions);
        let manual_plans = self.manual_plans(&applied_versions);

        let all_plans = auto_plans
            .into_iter()
            .chain(view_plans)
            .chain(mat_view_plans)
            .chain(manual_plans);

        let upsert_sql = upsert_migration_sql(dialect);

        for plan in all_plans {
            // Interactive confirmation — abort if user declines.
            if !confirm(&plan) {
                return Err(MigrationError::UserAborted {
                    version: plan.version.clone(),
                });
            }

            self.hooks.call_before(&plan).await?;
            let stmts = plan.statements.clone();
            let version = plan.version.clone();
            let description = plan.description.clone();
            let comment = plan.comment.clone();
            let checksum = plan.checksum.clone();
            let upsert_sql = upsert_sql.clone();
            let result = {
                let fut = db.transaction(Box::new(move |txn| {
                    Box::pin(async move {
                        for stmt in &stmts {
                            txn.execute(stmt, &[]).await?;
                        }
                        txn.execute(
                            &upsert_sql,
                            &[
                                Value::String(version),
                                Value::String(description),
                                Value::String(checksum),
                                match comment {
                                    Some(c) => Value::String(c),
                                    None => Value::Null,
                                },
                            ],
                        )
                        .await?;
                        Ok(())
                    })
                }));
                match plan.timeout {
                    Some(dur) => tokio_timeout(dur, fut)
                        .await
                        .map_err(|_| MigrationError::TimedOut {
                            version: plan.version.clone(),
                            timeout_secs: dur.as_secs(),
                        })
                        .and_then(|r| r.map_err(MigrationError::Db)),
                    None => fut.await.map_err(MigrationError::Db),
                }
            };
            match result {
                Ok(()) => self.hooks.call_after(&plan).await?,
                Err(e) => {
                    self.hooks.call_error(&plan, &e).await;
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}
