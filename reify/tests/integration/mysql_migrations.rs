//! Integration tests for `MigrationRunner` with the `mysql` feature.
//!
//! Covers:
//! - First run creates the table and the tracking table
//! - Second run is idempotent (tracking row count unchanged)
//! - Schema evolution: adding a new table on the second run
//! - Manual migration: `ADD COLUMN` via `MigrationContext`
//! - `dry_run` previews pending plans without applying them

#![cfg(feature = "integration-tests")]

use reify::mysql_async::Opts;
use reify::{
    Dialect, Migration, MigrationContext, MigrationRunner, MysqlDb, Table, raw_execute, raw_query,
};

use crate::mysql_url;

// ── Fixtures ──────────────────────────────────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "mysql_mig_users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
    #[column(nullable)]
    pub role: Option<String>,
}

#[derive(Table, Debug, Clone)]
#[table(name = "mysql_mig_posts")]
pub struct Post {
    #[column(primary_key)]
    pub id: i64,
    pub user_id: i64,
    pub title: String,
}

// ── Manual migration fixture ──────────────────────────────────────────

struct AddUserCity;
impl Migration for AddUserCity {
    fn version(&self) -> &'static str {
        "20240320_mysql_000001_add_user_city"
    }
    fn description(&self) -> &'static str {
        "Add city column to mysql_mig_users"
    }
    fn up(&self, ctx: &mut MigrationContext) {
        ctx.add_column("mysql_mig_users", "city", "VARCHAR(255) NOT NULL DEFAULT ''");
    }
    fn down(&self, ctx: &mut MigrationContext) {
        ctx.drop_column("mysql_mig_users", "city");
    }
}

// ── Helpers ───────────────────────────────────────────────────────────

async fn connect() -> Option<MysqlDb> {
    let url = mysql_url()?;
    let opts = Opts::from_url(&url).expect("invalid MYSQL_URL");
    Some(MysqlDb::connect(opts).await.expect("mysql connect"))
}

async fn cleanup(db: &MysqlDb) {
    for table in &[
        "mysql_mig_posts",
        "mysql_mig_users",
        "_reify_migrations",
    ] {
        raw_execute(db, &format!("DROP TABLE IF EXISTS `{table}`"), &[])
            .await
            .unwrap_or_else(|e| panic!("drop {table}: {e}"));
    }
}

fn runner() -> MigrationRunner {
    MigrationRunner::new().with_dialect(Dialect::Mysql)
}

// ── Tests ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn mysql_migration_first_run_creates_table() {
    let Some(db) = connect().await else { return };
    cleanup(&db).await;

    runner()
        .add_table::<User>()
        .run(&db)
        .await
        .expect("first run");

    // Table must exist and be queryable
    let rows = raw_query(&db, "SELECT COUNT(*) FROM `mysql_mig_users`", &[])
        .await
        .expect("query users");
    assert!(!rows.is_empty());

    // Tracking table must have been created and populated
    let tracking = raw_query(&db, "SELECT COUNT(*) FROM `_reify_migrations`", &[])
        .await
        .expect("query tracking");
    assert!(!tracking.is_empty());

    cleanup(&db).await;
}

#[tokio::test]
async fn mysql_migration_second_run_is_idempotent() {
    let Some(db) = connect().await else { return };
    cleanup(&db).await;

    runner()
        .add_table::<User>()
        .run(&db)
        .await
        .expect("first run");

    let count_before = raw_query(&db, "SELECT COUNT(*) FROM `_reify_migrations`", &[])
        .await
        .expect("count before");

    runner()
        .add_table::<User>()
        .run(&db)
        .await
        .expect("second run");

    let count_after = raw_query(&db, "SELECT COUNT(*) FROM `_reify_migrations`", &[])
        .await
        .expect("count after");

    assert_eq!(
        count_before[0].get_idx(0),
        count_after[0].get_idx(0),
        "tracking table row count must be unchanged on idempotent run"
    );

    cleanup(&db).await;
}

#[tokio::test]
async fn mysql_migration_schema_evolution_adds_new_table() {
    let Some(db) = connect().await else { return };
    cleanup(&db).await;

    runner()
        .add_table::<User>()
        .run(&db)
        .await
        .expect("first run");

    // posts must not exist yet
    let posts_exists = raw_query(
        &db,
        "SELECT 1 FROM information_schema.tables \
         WHERE table_name = 'mysql_mig_posts' AND table_schema = DATABASE()",
        &[],
    )
    .await
    .expect("check posts before");
    assert!(
        posts_exists.is_empty(),
        "mysql_mig_posts must not exist after first run"
    );

    runner()
        .add_table::<User>()
        .add_table::<Post>()
        .run(&db)
        .await
        .expect("second run with Post");

    let posts_exists = raw_query(
        &db,
        "SELECT 1 FROM information_schema.tables \
         WHERE table_name = 'mysql_mig_posts' AND table_schema = DATABASE()",
        &[],
    )
    .await
    .expect("check posts after");
    assert!(
        !posts_exists.is_empty(),
        "mysql_mig_posts must exist after second run"
    );

    cleanup(&db).await;
}

#[tokio::test]
async fn mysql_migration_manual_add_column() {
    let Some(db) = connect().await else { return };
    cleanup(&db).await;

    // First: create the base table
    runner()
        .add_table::<User>()
        .run(&db)
        .await
        .expect("create users");

    // Second: apply manual migration that adds `city`
    runner()
        .add_table::<User>()
        .add(AddUserCity)
        .run(&db)
        .await
        .expect("add city column");

    // Verify the column exists
    let col_exists = raw_query(
        &db,
        "SELECT 1 FROM information_schema.columns \
         WHERE table_name = 'mysql_mig_users' \
           AND column_name = 'city' \
           AND table_schema = DATABASE()",
        &[],
    )
    .await
    .expect("check city column");
    assert!(
        !col_exists.is_empty(),
        "city column must exist after manual migration"
    );

    cleanup(&db).await;
}

#[tokio::test]
async fn mysql_migration_dry_run_previews_without_applying() {
    let Some(db) = connect().await else { return };
    cleanup(&db).await;

    let plans = runner()
        .add_table::<User>()
        .dry_run(&db)
        .await
        .expect("dry_run");

    // dry_run must report a pending plan
    assert!(!plans.is_empty(), "expected at least one pending plan");
    assert!(
        plans[0].statements[0].contains("mysql_mig_users"),
        "plan must reference mysql_mig_users: {:?}",
        plans[0].statements
    );

    // Table must NOT have been created (dry_run is read-only)
    let table_exists = raw_query(
        &db,
        "SELECT 1 FROM information_schema.tables \
         WHERE table_name = 'mysql_mig_users' AND table_schema = DATABASE()",
        &[],
    )
    .await
    .expect("check table after dry_run");
    assert!(
        table_exists.is_empty(),
        "dry_run must not create the table"
    );

    cleanup(&db).await;
}
