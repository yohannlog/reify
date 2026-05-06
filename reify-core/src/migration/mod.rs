mod codegen;
mod context;
mod ddl;
mod diff;
mod error;
mod lock;
mod plan;
mod runner;
mod traits;

#[cfg(test)]
mod mod_tests_diff;
#[cfg(test)]
mod mod_tests_views;
#[cfg(test)]
mod test_support;

pub use codegen::{
    generate_materialized_view_migration_file, generate_migration_file,
    generate_view_migration_file,
};
pub use context::MigrationContext;
pub use ddl::{
    MissingDefaultError, add_column_sql, create_table_sql, create_table_sql_with_checks,
    try_add_column_sql,
};
pub use diff::{ColumnDiff, DbColumnInfo, SchemaDiff, TableDiff, normalize_sql_type};
pub use error::MigrationError;
pub use lock::MigrationLock;
pub use plan::{MigrationPlan, MigrationStatus, compute_checksum};
pub use runner::MigrationHooks;
pub use runner::MigrationRunner;
pub use traits::Migration;
// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use crate::db::Row;
    use crate::value::Value;

    // ── Tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn dry_run_new_table_emits_create_table() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns users → absent

        let runner = MigrationRunner::new().add_table::<Users>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].version, "auto__users");
        assert!(plans[0].statements[0].contains("CREATE TABLE IF NOT EXISTS \"users\""));
    }

    #[tokio::test]
    async fn dry_run_existing_table_no_new_columns_emits_nothing() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        // existing_column_details: column metadata query
        let existing = vec![
            Row::new(
                vec![
                    "column_name".into(),
                    "data_type".into(),
                    "is_nullable".into(),
                    "column_default".into(),
                ],
                vec![
                    Value::String("id".into()),
                    Value::String("bigint".into()),
                    Value::String("NO".into()),
                    Value::Null,
                ],
            ),
            Row::new(
                vec![
                    "column_name".into(),
                    "data_type".into(),
                    "is_nullable".into(),
                    "column_default".into(),
                ],
                vec![
                    Value::String("email".into()),
                    Value::String("text".into()),
                    Value::String("NO".into()),
                    Value::Null,
                ],
            ),
            Row::new(
                vec![
                    "column_name".into(),
                    "data_type".into(),
                    "is_nullable".into(),
                    "column_default".into(),
                ],
                vec![
                    Value::String("role".into()),
                    Value::String("text".into()),
                    Value::String("NO".into()),
                    Value::Null,
                ],
            ),
        ];
        db.push_query_result(existing);
        db.push_query_result(vec![]); // unique constraints query
        db.push_query_result(vec![]); // existing_indexes query

        let runner = MigrationRunner::new().add_table::<Users>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert!(plans.is_empty());
    }

    #[tokio::test]
    async fn dry_run_existing_table_new_column_emits_add_column() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        // existing_column_details: Table exists but missing "role"
        let existing = vec![
            Row::new(
                vec![
                    "column_name".into(),
                    "data_type".into(),
                    "is_nullable".into(),
                    "column_default".into(),
                ],
                vec![
                    Value::String("id".into()),
                    Value::String("bigint".into()),
                    Value::String("NO".into()),
                    Value::Null,
                ],
            ),
            Row::new(
                vec![
                    "column_name".into(),
                    "data_type".into(),
                    "is_nullable".into(),
                    "column_default".into(),
                ],
                vec![
                    Value::String("email".into()),
                    Value::String("text".into()),
                    Value::String("NO".into()),
                    Value::Null,
                ],
            ),
        ];
        db.push_query_result(existing);
        db.push_query_result(vec![]); // unique constraints query
        db.push_query_result(vec![]); // existing_indexes query

        let runner = MigrationRunner::new().add_table::<Users>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        assert!(plans[0].statements[0].contains("ADD COLUMN \"role\""));
    }

    #[tokio::test]
    async fn dry_run_manual_migration_included() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        let runner = MigrationRunner::new().add(AddUserCity);
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].version, "20240320_000001_add_user_city");
        assert!(plans[0].statements[0].contains("ADD COLUMN \"city\""));
    }

    #[tokio::test]
    async fn dry_run_skips_already_applied_manual() {
        let db = MockDb::new();
        // applied_versions returns the manual migration as already applied
        let applied_row = Row::new(
            vec!["version".into()],
            vec![Value::String("20240320_000001_add_user_city".into())],
        );
        db.push_query_result(vec![applied_row]);

        let runner = MigrationRunner::new().add(AddUserCity);
        let plans = runner.dry_run(&db).await.unwrap();

        assert!(plans.is_empty());
    }

    #[tokio::test]
    async fn run_creates_tracking_table_and_executes_ddl() {
        let db = MockDb::new();
        // applied_checksums → empty (queried after lock acquire)
        db.push_query_result(vec![]);
        db.push_query_result(vec![]); // existing_columns users → absent

        let runner = MigrationRunner::new().add_table::<Users>();
        runner.run(&db).await.unwrap();

        let sql = db.executed_sql();
        // Sequence: CREATE tracking table, CREATE lock table, INSERT sentinel,
        // UPDATE acquire lock, then DDL, then INSERT tracking, then UPDATE release.
        assert!(
            sql.iter().any(|s| s.contains("_reify_migrations")),
            "tracking table not found: {sql:?}"
        );
        assert!(
            sql.iter()
                .any(|s| s.contains("CREATE TABLE IF NOT EXISTS \"users\"")),
            "CREATE TABLE users not found: {sql:?}"
        );
        assert!(
            sql.iter()
                .any(|s| s.contains("INSERT INTO \"_reify_migrations\"")),
            "INSERT tracking not found: {sql:?}"
        );
    }

    #[tokio::test]
    async fn run_manual_migration_executes_up_statements() {
        let db = MockDb::new();
        // applied_checksums → empty (queried after lock acquire)
        db.push_query_result(vec![]);

        let runner = MigrationRunner::new().add(AddUserCity);
        runner.run(&db).await.unwrap();

        let sql = db.executed_sql();
        let has_add_column = sql.iter().any(|s| s.contains("ADD COLUMN \"city\""));
        assert!(has_add_column);
    }

    #[tokio::test]
    async fn rollback_executes_down_and_removes_tracking_row() {
        let db = MockDb::new();
        // applied_versions for rollback query
        let applied_row = Row::new(
            vec!["version".into()],
            vec![Value::String("20240320_000001_add_user_city".into())],
        );
        db.push_query_result(vec![applied_row]); // last applied query

        let runner = MigrationRunner::new().add(AddUserCity);
        runner.rollback(&db).await.unwrap();

        let sql = db.executed_sql();
        let has_drop = sql.iter().any(|s| s.contains("DROP COLUMN \"city\""));
        let has_delete = sql
            .iter()
            .any(|s| s.contains("DELETE FROM \"_reify_migrations\""));
        assert!(has_drop, "expected DROP COLUMN \"city\" in: {sql:?}");
        assert!(has_delete, "expected DELETE FROM tracking in: {sql:?}");
    }

    #[tokio::test]
    async fn rollback_irreversible_returns_error() {
        let db = MockDb::new();
        let applied_row = Row::new(
            vec!["version".into()],
            vec![Value::String("20240321_000001_irreversible".into())],
        );
        db.push_query_result(vec![applied_row]);

        let runner = MigrationRunner::new().add(IrreversibleMigration);
        let result = runner.rollback(&db).await;

        assert!(matches!(result, Err(MigrationError::NotReversible(_))));
    }

    #[tokio::test]
    async fn migration_context_collects_statements() {
        let mut ctx = MigrationContext::new();
        ctx.add_column("users", "city", "TEXT NOT NULL");
        ctx.drop_column("users", "old_col");
        ctx.rename_column("users", "nm", "name");
        ctx.execute("UPDATE users SET city = 'Paris';");

        assert_eq!(ctx.statements().len(), 4);
        assert!(ctx.statements()[0].contains("ADD COLUMN \"city\""));
        assert!(ctx.statements()[1].contains("DROP COLUMN \"old_col\""));
        assert!(ctx.statements()[2].contains("RENAME COLUMN \"nm\" TO \"name\""));
        assert!(ctx.statements()[3].contains("UPDATE users"));
    }

    #[tokio::test]
    async fn migration_plan_display_format() {
        use super::plan::compute_checksum;
        let stmts = vec!["ALTER TABLE users ADD COLUMN city TEXT NOT NULL;".to_string()];
        let checksum = compute_checksum(&stmts);
        let plan = MigrationPlan {
            version: "20240320_000001_add_user_city".into(),
            description: "Add city column to users".into(),
            comment: None,
            statements: stmts,
            checksum,
            schema_diff: None,
            timeout: None,
        };
        let display = plan.display();
        assert!(display.contains("Would apply (up)"));
        assert!(display.contains("20240320_000001_add_user_city"));
        assert!(display.contains("ALTER TABLE users"));
        assert!(display.contains("SQL:"));
    }

    #[test]
    fn migration_plan_display_includes_schema_diff_when_present() {
        use super::plan::compute_checksum;
        let stmts = vec![
            "CREATE TABLE IF NOT EXISTS \"users\" (\"id\" BIGSERIAL PRIMARY KEY);".to_string(),
        ];
        let checksum = compute_checksum(&stmts);
        let plan = MigrationPlan {
            version: "auto__users".into(),
            description: "Create table users".into(),
            comment: None,
            statements: stmts,
            checksum,
            timeout: None,
            schema_diff: Some(SchemaDiff {
                tables: vec![TableDiff {
                    table_name: "users".into(),
                    is_new_table: true,
                    column_diffs: vec![
                        ColumnDiff::Added {
                            column: "id".into(),
                        },
                        ColumnDiff::Added {
                            column: "email".into(),
                        },
                    ],
                }],
            }),
        };
        let display = plan.display();
        assert!(display.contains("Would apply (up)"));
        assert!(
            display.contains("Schema diff:"),
            "missing Schema diff header: {display}"
        );
        assert!(
            display.contains("✚ table `users`"),
            "missing table symbol: {display}"
        );
        assert!(display.contains("✚ `id`"), "missing id column: {display}");
        assert!(
            display.contains("✚ `email`"),
            "missing email column: {display}"
        );
        assert!(display.contains("SQL:"), "missing SQL: label: {display}");
        assert!(
            display.contains("CREATE TABLE"),
            "missing SQL body: {display}"
        );
    }

    #[test]
    fn generate_migration_file_produces_valid_template() {
        let content = generate_migration_file("add_user_city", "20240320_000001_add_user_city");
        assert!(content.contains("struct AddUserCity"));
        assert!(content.contains("impl Migration for AddUserCity"));
        assert!(content.contains("20240320_000001_add_user_city"));
        assert!(content.contains("todo!(\"implement up migration\")"));
    }

    // ── Hook tests ───────────────────────────────────────────────────

    #[tokio::test]
    async fn hooks_before_each_called() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns users → absent

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let runner = MigrationRunner::new()
            .add_table::<Users>()
            .on_before_each(move |_plan| {
                let c = counter_clone.clone();
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            });

        runner.run(&db).await.unwrap();
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "before_each should be called once"
        );
    }

    #[tokio::test]
    async fn hooks_after_each_called() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns users → absent

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let runner = MigrationRunner::new()
            .add_table::<Users>()
            .on_after_each(move |_plan| {
                let c = counter_clone.clone();
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            });

        runner.run(&db).await.unwrap();
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "after_each should be called once after success"
        );
    }

    #[tokio::test]
    async fn hooks_before_each_can_abort() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns users → absent

        let runner = MigrationRunner::new()
            .add_table::<Users>()
            .on_before_each(|_plan| {
                Box::pin(async move { Err(MigrationError::Other("aborted by hook".into())) })
            });

        let result = runner.run(&db).await;
        assert!(
            matches!(result, Err(MigrationError::Other(ref msg)) if msg.contains("aborted by hook")),
            "run() should propagate the hook error: {result:?}"
        );
        // The user table DDL should NOT have been executed (tracking table setup runs before hooks).
        let sql = db.executed_sql();
        assert!(
            !sql.iter()
                .any(|s| s.contains("CREATE TABLE IF NOT EXISTS \"users\"")),
            "CREATE TABLE users should not run when before_each aborts: {sql:?}"
        );
    }

    // ── Dialect-aware SQL generation tests ────────────────────────────

    #[test]
    fn tracking_table_sql_uses_backticks_for_mysql() {
        use super::runner::entries::{quote_col, tracking_table};
        use crate::query::Dialect;

        assert_eq!(tracking_table(Dialect::Mysql), "`_reify_migrations`");
        assert_eq!(tracking_table(Dialect::Postgres), "\"_reify_migrations\"");
        assert_eq!(tracking_table(Dialect::Generic), "\"_reify_migrations\"");

        assert_eq!(quote_col("version", Dialect::Mysql), "`version`");
        assert_eq!(quote_col("version", Dialect::Postgres), "\"version\"");
    }

    #[test]
    fn select_versions_sql_uses_dialect_quoting() {
        use super::runner::entries::select_versions_sql;
        use crate::query::Dialect;

        let mysql_sql = select_versions_sql(Dialect::Mysql);
        assert!(mysql_sql.contains("`_reify_migrations`"));
        assert!(mysql_sql.contains("`version`"));

        let pg_sql = select_versions_sql(Dialect::Postgres);
        assert!(pg_sql.contains("\"_reify_migrations\""));
        assert!(pg_sql.contains("\"version\""));
    }

    #[test]
    fn insert_migration_sql_uses_dialect_quoting() {
        use super::runner::entries::insert_migration_sql;
        use crate::query::Dialect;

        let mysql_sql = insert_migration_sql(Dialect::Mysql);
        assert!(mysql_sql.contains("`_reify_migrations`"));
        assert!(mysql_sql.contains("`version`"));
        assert!(mysql_sql.contains("`description`"));

        let pg_sql = insert_migration_sql(Dialect::Postgres);
        assert!(pg_sql.contains("\"_reify_migrations\""));
        assert!(pg_sql.contains("\"version\""));
    }

    #[test]
    fn upsert_migration_sql_uses_dialect_syntax() {
        use super::runner::entries::upsert_migration_sql;
        use crate::query::Dialect;

        let mysql_sql = upsert_migration_sql(Dialect::Mysql);
        assert!(mysql_sql.contains("ON DUPLICATE KEY UPDATE"));
        assert!(mysql_sql.contains("`_reify_migrations`"));

        let pg_sql = upsert_migration_sql(Dialect::Postgres);
        assert!(pg_sql.contains("ON CONFLICT"));
        assert!(pg_sql.contains("EXCLUDED"));
        assert!(pg_sql.contains("\"_reify_migrations\""));
    }

    // ── Index DDL tests ───────────────────────────────────────────────

    #[test]
    fn create_index_sql_basic() {
        use crate::migration::ddl::create_index_sql;
        use crate::query::Dialect;
        use crate::schema::{IndexColumnDef, IndexDef, IndexKind};

        let idx = IndexDef {
            name: None,
            columns: vec![IndexColumnDef::asc("email")],
            unique: false,
            kind: IndexKind::BTree,
            predicate: None,
        };

        let sql = create_index_sql("users", &idx, Dialect::Postgres);
        assert!(sql.contains("CREATE INDEX"));
        assert!(sql.contains("\"idx_email\""));
        assert!(sql.contains("ON \"users\""));
        assert!(sql.contains("\"email\" ASC"));
        assert!(sql.contains("USING btree"));
    }

    #[test]
    fn create_index_sql_unique() {
        use crate::migration::ddl::create_index_sql;
        use crate::query::Dialect;
        use crate::schema::{IndexColumnDef, IndexDef, IndexKind};

        let idx = IndexDef {
            name: Some("users_email_unique".into()),
            columns: vec![IndexColumnDef::asc("email")],
            unique: true,
            kind: IndexKind::BTree,
            predicate: None,
        };

        let sql = create_index_sql("users", &idx, Dialect::Postgres);
        assert!(sql.contains("CREATE UNIQUE INDEX"));
        assert!(sql.contains("\"users_email_unique\""));
    }

    #[test]
    fn create_index_sql_composite() {
        use crate::migration::ddl::create_index_sql;
        use crate::query::Dialect;
        use crate::schema::{IndexColumnDef, IndexDef, IndexKind};

        let idx = IndexDef {
            name: None,
            columns: vec![
                IndexColumnDef::asc("tenant_id"),
                IndexColumnDef::desc("created_at"),
            ],
            unique: false,
            kind: IndexKind::BTree,
            predicate: None,
        };

        let sql = create_index_sql("orders", &idx, Dialect::Postgres);
        assert!(sql.contains("\"idx_tenant_id_created_at\""));
        assert!(sql.contains("\"tenant_id\" ASC"));
        assert!(sql.contains("\"created_at\" DESC"));
    }

    #[test]
    fn create_index_sql_partial() {
        use crate::migration::ddl::create_index_sql;
        use crate::query::Dialect;
        use crate::schema::{IndexColumnDef, IndexDef, IndexKind};

        let idx = IndexDef {
            name: Some("active_users_idx".into()),
            columns: vec![IndexColumnDef::asc("email")],
            unique: false,
            kind: IndexKind::BTree,
            predicate: Some("deleted_at IS NULL".into()),
        };

        let sql = create_index_sql("users", &idx, Dialect::Postgres);
        assert!(sql.contains("WHERE deleted_at IS NULL"));
    }

    #[test]
    fn create_index_sql_mysql_dialect() {
        use crate::migration::ddl::create_index_sql;
        use crate::query::Dialect;
        use crate::schema::{IndexColumnDef, IndexDef, IndexKind};

        let idx = IndexDef {
            name: None,
            columns: vec![IndexColumnDef::asc("email")],
            unique: false,
            kind: IndexKind::BTree,
            predicate: None,
        };

        let sql = create_index_sql("users", &idx, Dialect::Mysql);
        assert!(sql.contains("`idx_email`"));
        assert!(sql.contains("ON `users`"));
        assert!(sql.contains("`email` ASC"));
        assert!(sql.contains("USING BTREE"));
    }

    #[test]
    fn drop_index_sql_postgres() {
        use crate::migration::ddl::drop_index_sql;
        use crate::query::Dialect;

        let sql = drop_index_sql("users", "idx_email", Dialect::Postgres);
        assert_eq!(sql, "DROP INDEX IF EXISTS \"idx_email\";");
    }

    #[test]
    fn drop_index_sql_mysql() {
        use crate::migration::ddl::drop_index_sql;
        use crate::query::Dialect;

        let sql = drop_index_sql("users", "idx_email", Dialect::Mysql);
        assert_eq!(sql, "DROP INDEX `idx_email` ON `users`;");
    }

    // ── add_column_sql dialect test ───────────────────────────────────

    #[test]
    fn add_column_sql_uses_dialect() {
        use crate::migration::ddl::add_column_sql;
        use crate::query::Dialect;
        use crate::schema::{ColumnDef, SqlType, TimestampSource};

        let def = ColumnDef {
            name: "status",
            sql_type: SqlType::Text,
            primary_key: false,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: true,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: TimestampSource::Vm,
            check: None,
            foreign_key: None,
            soft_delete: false,
        };

        let pg_sql = add_column_sql("users", "status", Some(&def), Dialect::Postgres);
        assert!(pg_sql.contains("ALTER TABLE \"users\""));
        assert!(pg_sql.contains("ADD COLUMN \"status\""));

        let mysql_sql = add_column_sql("users", "status", Some(&def), Dialect::Mysql);
        assert!(mysql_sql.contains("ALTER TABLE `users`"));
        assert!(mysql_sql.contains("ADD COLUMN `status`"));
    }

    // ── Auto-diff index creation tests ─────────────────────────────────

    #[tokio::test]
    async fn dry_run_new_table_with_index_emits_create_index() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns → table absent

        let runner = MigrationRunner::new().add_table::<UsersWithIndex>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        let stmts = &plans[0].statements;
        assert!(
            stmts.iter().any(|s| s.contains("CREATE TABLE")),
            "expected CREATE TABLE in: {stmts:?}"
        );
        assert!(
            stmts.iter().any(|s| s.contains("CREATE INDEX")),
            "expected CREATE INDEX in: {stmts:?}"
        );
        assert!(
            stmts.iter().any(|s| s.contains("idx_email")),
            "expected idx_email in: {stmts:?}"
        );
    }

    #[tokio::test]
    async fn dry_run_existing_table_missing_index_emits_create_index() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        // existing_column_details: Table exists with all columns
        let existing = vec![
            Row::new(
                vec![
                    "column_name".into(),
                    "data_type".into(),
                    "is_nullable".into(),
                    "column_default".into(),
                ],
                vec![
                    Value::String("id".into()),
                    Value::String("bigint".into()),
                    Value::String("NO".into()),
                    Value::Null,
                ],
            ),
            Row::new(
                vec![
                    "column_name".into(),
                    "data_type".into(),
                    "is_nullable".into(),
                    "column_default".into(),
                ],
                vec![
                    Value::String("email".into()),
                    Value::String("text".into()),
                    Value::String("NO".into()),
                    Value::Null,
                ],
            ),
            Row::new(
                vec![
                    "column_name".into(),
                    "data_type".into(),
                    "is_nullable".into(),
                    "column_default".into(),
                ],
                vec![
                    Value::String("role".into()),
                    Value::String("text".into()),
                    Value::String("NO".into()),
                    Value::Null,
                ],
            ),
        ];
        db.push_query_result(existing);
        db.push_query_result(vec![]); // unique constraints query
        db.push_query_result(vec![]); // existing_indexes → no indexes

        let runner = MigrationRunner::new().add_table::<UsersWithIndex>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        let stmts = &plans[0].statements;
        assert!(
            stmts.iter().any(|s| s.contains("CREATE INDEX")),
            "expected CREATE INDEX in: {stmts:?}"
        );
        assert!(
            stmts.iter().any(|s| s.contains("idx_email")),
            "expected idx_email in: {stmts:?}"
        );
        // Should NOT contain CREATE TABLE since table exists
        assert!(
            !stmts.iter().any(|s| s.contains("CREATE TABLE")),
            "unexpected CREATE TABLE in: {stmts:?}"
        );
    }

    #[tokio::test]
    async fn dry_run_existing_table_with_index_emits_nothing() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        // existing_column_details: Table exists with all columns
        let existing = vec![
            Row::new(
                vec![
                    "column_name".into(),
                    "data_type".into(),
                    "is_nullable".into(),
                    "column_default".into(),
                ],
                vec![
                    Value::String("id".into()),
                    Value::String("bigint".into()),
                    Value::String("NO".into()),
                    Value::Null,
                ],
            ),
            Row::new(
                vec![
                    "column_name".into(),
                    "data_type".into(),
                    "is_nullable".into(),
                    "column_default".into(),
                ],
                vec![
                    Value::String("email".into()),
                    Value::String("text".into()),
                    Value::String("NO".into()),
                    Value::Null,
                ],
            ),
            Row::new(
                vec![
                    "column_name".into(),
                    "data_type".into(),
                    "is_nullable".into(),
                    "column_default".into(),
                ],
                vec![
                    Value::String("role".into()),
                    Value::String("text".into()),
                    Value::String("NO".into()),
                    Value::Null,
                ],
            ),
        ];
        db.push_query_result(existing);
        db.push_query_result(vec![]); // unique constraints query

        // existing_indexes → index already exists
        let idx_row = Row::new(
            vec!["indexname".into()],
            vec![Value::String("idx_email".into())],
        );
        db.push_query_result(vec![idx_row]);

        let runner = MigrationRunner::new().add_table::<UsersWithIndex>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert!(
            plans.is_empty(),
            "expected no plans when table and index exist: {plans:?}"
        );
    }

    // ── Dialect-aware CREATE TABLE tests ───────────────────────────────

    #[tokio::test]
    async fn dry_run_mysql_dialect_uses_backticks() {
        use crate::query::Dialect;

        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns → table absent

        let runner = MigrationRunner::new()
            .with_dialect(Dialect::Mysql)
            .add_table::<Users>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        let create_stmt = &plans[0].statements[0];
        assert!(
            create_stmt.contains("`users`"),
            "expected backtick-quoted table name in MySQL: {create_stmt}"
        );
        assert!(
            create_stmt.contains("`id`"),
            "expected backtick-quoted column name in MySQL: {create_stmt}"
        );
        // Should NOT contain double quotes
        assert!(
            !create_stmt.contains("\"users\""),
            "unexpected double-quoted table name in MySQL: {create_stmt}"
        );
    }

    #[tokio::test]
    async fn dry_run_postgres_dialect_uses_double_quotes() {
        use crate::query::Dialect;

        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns → table absent

        let runner = MigrationRunner::new()
            .with_dialect(Dialect::Postgres)
            .add_table::<Users>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        let create_stmt = &plans[0].statements[0];
        assert!(
            create_stmt.contains("\"users\""),
            "expected double-quoted table name in Postgres: {create_stmt}"
        );
        assert!(
            create_stmt.contains("\"id\""),
            "expected double-quoted column name in Postgres: {create_stmt}"
        );
    }

    #[test]
    fn create_table_sql_named_with_checks_uses_dialect() {
        use crate::migration::ddl::create_table_sql_named_with_checks;
        use crate::query::Dialect;
        use crate::schema::{ColumnDef, SqlType, TimestampSource};

        let defs = vec![ColumnDef {
            name: "id",
            sql_type: SqlType::BigInt,
            primary_key: true,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: false,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: TimestampSource::Vm,
            check: None,
            foreign_key: None,
            soft_delete: false,
        }];
        let checks = vec!["id > 0".to_string()];

        let pg_sql = create_table_sql_named_with_checks("test", &defs, &checks, Dialect::Postgres);
        assert!(pg_sql.contains("\"test\""));
        assert!(pg_sql.contains("\"id\""));
        assert!(pg_sql.contains("CHECK (id > 0)"));

        let mysql_sql = create_table_sql_named_with_checks("test", &defs, &checks, Dialect::Mysql);
        assert!(mysql_sql.contains("`test`"));
        assert!(mysql_sql.contains("`id`"));
        assert!(mysql_sql.contains("CHECK (id > 0)"));
    }

    // ── Interactive mode tests ────────────────────────────────────────

    #[tokio::test]
    async fn run_interactive_aborts_on_reject() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns users → absent

        let runner = MigrationRunner::new().add_table::<Users>();

        // Confirm callback always returns false → user rejects
        let result = runner.run_interactive(&db, |_plan| false).await;

        assert!(
            matches!(result, Err(MigrationError::UserAborted { ref version }) if version == "auto__users"),
            "expected UserAborted for auto__users, got: {result:?}"
        );

        // The CREATE TABLE should NOT have been executed
        let sql = db.executed_sql();
        assert!(
            !sql.iter()
                .any(|s| s.contains("CREATE TABLE IF NOT EXISTS \"users\"")),
            "CREATE TABLE users should not run when user aborts: {sql:?}"
        );
    }

    #[tokio::test]
    async fn run_interactive_applies_on_accept() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns users → absent

        let runner = MigrationRunner::new().add_table::<Users>();

        // Confirm callback always returns true → user accepts
        let result = runner.run_interactive(&db, |_plan| true).await;

        assert!(result.is_ok(), "expected Ok, got: {result:?}");

        // The CREATE TABLE should have been executed
        let sql = db.executed_sql();
        assert!(
            sql.iter()
                .any(|s| s.contains("CREATE TABLE IF NOT EXISTS \"users\"")),
            "CREATE TABLE users should run when user accepts: {sql:?}"
        );
    }

    #[tokio::test]
    async fn run_interactive_stops_at_first_reject() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns users → absent

        let runner = MigrationRunner::new().add_table::<Users>().add(AddUserCity);

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        // Accept first, reject second
        let result = runner
            .run_interactive(&db, move |_plan| {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                count == 0 // true for first, false for second
            })
            .await;

        // Should abort on the second migration (AddUserCity)
        assert!(
            matches!(result, Err(MigrationError::UserAborted { ref version }) if version == "20240320_000001_add_user_city"),
            "expected UserAborted for manual migration, got: {result:?}"
        );

        // Confirm was called twice (once for each pending migration)
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }
}
