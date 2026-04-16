use super::entries::TRACKING_TABLE;
use super::MigrationRunner;
use crate::db::Database;
use crate::migration::context::MigrationContext;
use crate::migration::error::MigrationError;
use crate::migration::lock::MigrationLock;
use crate::migration::plan::{MigrationPlan, compute_checksum};
use crate::value::Value;

impl MigrationRunner {
    /// Roll back the last applied migration.
    ///
    /// Returns `MigrationError::NotReversible` if the migration declared
    /// `is_reversible() = false`.
    pub async fn rollback(&self, db: &impl Database) -> Result<(), MigrationError> {
        self.ensure_tracking_table(db).await?;
        MigrationLock::ensure(db, self.dialect).await?;
        MigrationLock::acquire(db, self.dialect).await?;

        // Find the most recently applied manual migration
        let rows = db
            .query(
                &format!(
                    "SELECT \"version\" FROM {TRACKING_TABLE} \
                     WHERE \"version\" NOT LIKE 'auto__%' \
                     ORDER BY \"applied_at\" DESC LIMIT 1;"
                ),
                &[],
            )
            .await?;

        let last_version = rows.first().and_then(|r| r.get_string("version"));

        let version = match last_version {
            Some(v) => v,
            None => {
                return Err(MigrationError::Other(
                    "no applied migrations to roll back".into(),
                ));
            }
        };

        // Find the matching Migration impl
        let migration = self
            .manual
            .iter()
            .find(|m| m.version() == version)
            .ok_or_else(|| {
                MigrationError::Other(format!(
                    "migration '{version}' is applied but not registered in this runner"
                ))
            })?;

        if !migration.is_reversible() {
            return Err(MigrationError::NotReversible(version));
        }

        let mut ctx = MigrationContext::new();
        migration.down(&mut ctx);
        let stmts = ctx.into_statements();
        let checksum = compute_checksum(&stmts);
        let plan = MigrationPlan {
            version: version.clone(),
            description: format!("Rollback: {}", migration.description()),
            comment: None,
            statements: stmts,
            checksum,
            schema_diff: None,
        };
        self.hooks.call_before(&plan).await?;
        let stmts_clone = plan.statements.clone();
        let version_clone = version.clone();
        let result = db
            .transaction(Box::new(move |txn| {
                Box::pin(async move {
                    for stmt in &stmts_clone {
                        txn.execute(stmt, &[]).await?;
                    }
                    txn.execute(
                        &format!("DELETE FROM {TRACKING_TABLE} WHERE \"version\" = ?;"),
                        &[Value::String(version_clone.into())],
                    )
                    .await?;
                    Ok(())
                })
            }))
            .await
            .map_err(MigrationError::Db);
        match result {
            Ok(()) => self.hooks.call_after(&plan).await?,
            Err(e) => {
                self.hooks.call_error(&plan, &e).await;
                MigrationLock::release(db).await.ok();
                return Err(e);
            }
        }

        MigrationLock::release(db).await.ok();
        Ok(())
    }

    /// Roll back all applied migrations up to (and including) `target_version`.
    pub async fn rollback_to(
        &self,
        db: &impl Database,
        target_version: &str,
    ) -> Result<(), MigrationError> {
        self.ensure_tracking_table(db).await?;
        MigrationLock::ensure(db, self.dialect).await?;
        MigrationLock::acquire(db, self.dialect).await?;

        let rows = db
            .query(
                &format!(
                    "SELECT \"version\" FROM {TRACKING_TABLE} \
                     WHERE \"version\" NOT LIKE 'auto__%' \
                     ORDER BY \"applied_at\" DESC;"
                ),
                &[],
            )
            .await?;

        let versions: Vec<String> =
            rows.into_iter().filter_map(|r| r.get_string("version")).collect();

        // Guard: ensure target_version is actually in the applied list before
        // starting any rollback — prevents silent no-ops on typos.
        if !versions.iter().any(|v| v == target_version) {
            return Err(MigrationError::Other(format!(
                "target version '{target_version}' is not in the list of applied migrations"
            )));
        }

        // Roll back from newest to target (inclusive), each in its own transaction.
        for version in &versions {
            let migration = self
                .manual
                .iter()
                .find(|m| m.version() == version)
                .ok_or_else(|| {
                    MigrationError::Other(format!(
                        "migration '{version}' is applied but not registered"
                    ))
                })?;

            if !migration.is_reversible() {
                return Err(MigrationError::NotReversible(version.clone()));
            }

            let mut ctx = MigrationContext::new();
            migration.down(&mut ctx);
            let stmts = ctx.into_statements();
            let checksum = compute_checksum(&stmts);
            let plan = MigrationPlan {
                version: version.clone(),
                description: format!("Rollback: {}", migration.description()),
                comment: None,
                statements: stmts,
                checksum,
                schema_diff: None,
            };
            self.hooks.call_before(&plan).await?;
            let stmts_clone = plan.statements.clone();
            let version_clone = version.clone();
            let result = db
                .transaction(Box::new(move |txn| {
                    Box::pin(async move {
                        for stmt in &stmts_clone {
                            txn.execute(stmt, &[]).await?;
                        }
                        txn.execute(
                            &format!("DELETE FROM {TRACKING_TABLE} WHERE \"version\" = ?;"),
                            &[Value::String(version_clone.into())],
                        )
                        .await?;
                        Ok(())
                    })
                }))
                .await
                .map_err(MigrationError::Db);
            match result {
                Ok(()) => self.hooks.call_after(&plan).await?,
                Err(e) => {
                    self.hooks.call_error(&plan, &e).await;
                    MigrationLock::release(db).await.ok();
                    return Err(e);
                }
            }

            if version == target_version {
                break;
            }
        }

        MigrationLock::release(db).await.ok();
        Ok(())
    }

    /// Roll back all manual migrations applied **at or after** `since`.
    ///
    /// Migrations are rolled back in reverse `applied_at` order (newest first).
    /// `since` is compared lexicographically against the stored timestamp string.
    /// Format: any prefix of `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SS`.
    pub async fn rollback_since(
        &self,
        db: &impl Database,
        since: &str,
    ) -> Result<(), MigrationError> {
        self.ensure_tracking_table(db).await?;
        MigrationLock::ensure(db, self.dialect).await?;
        MigrationLock::acquire(db, self.dialect).await?;

        // Fetch manual migrations applied at or after `since`, newest first.
        let rows = db
            .query(
                &format!(
                    "SELECT \"version\", CAST(\"applied_at\" AS TEXT) AS \"applied_at\" \
                     FROM {TRACKING_TABLE} \
                     WHERE \"version\" NOT LIKE 'auto__%' \
                       AND CAST(\"applied_at\" AS TEXT) >= ? \
                     ORDER BY \"applied_at\" DESC;"
                ),
                &[Value::String(since.into())],
            )
            .await?;

        let versions: Vec<String> =
            rows.into_iter().filter_map(|r| r.get_string("version")).collect();

        if versions.is_empty() {
            return Err(MigrationError::Other(format!(
                "no applied migrations found at or after '{since}'"
            )));
        }

        for version in &versions {
            let migration = self
                .manual
                .iter()
                .find(|m| m.version() == version)
                .ok_or_else(|| {
                    MigrationError::Other(format!(
                        "migration '{version}' is applied but not registered"
                    ))
                })?;

            if !migration.is_reversible() {
                return Err(MigrationError::NotReversible(version.clone()));
            }

            let mut ctx = MigrationContext::new();
            migration.down(&mut ctx);
            let stmts = ctx.into_statements();
            let checksum = compute_checksum(&stmts);
            let plan = MigrationPlan {
                version: version.clone(),
                description: format!("Rollback: {}", migration.description()),
                comment: None,
                statements: stmts,
                checksum,
                schema_diff: None,
            };
            self.hooks.call_before(&plan).await?;
            let stmts_clone = plan.statements.clone();
            let version_clone = version.clone();
            let result = db
                .transaction(Box::new(move |txn| {
                    Box::pin(async move {
                        for stmt in &stmts_clone {
                            txn.execute(stmt, &[]).await?;
                        }
                        txn.execute(
                            &format!("DELETE FROM {TRACKING_TABLE} WHERE \"version\" = ?;"),
                            &[Value::String(version_clone.into())],
                        )
                        .await?;
                        Ok(())
                    })
                }))
                .await
                .map_err(MigrationError::Db);
            match result {
                Ok(()) => self.hooks.call_after(&plan).await?,
                Err(e) => {
                    self.hooks.call_error(&plan, &e).await;
                    MigrationLock::release(db).await.ok();
                    return Err(e);
                }
            }
        }

        MigrationLock::release(db).await.ok();
        Ok(())
    }
}
