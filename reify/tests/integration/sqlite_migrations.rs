//! Integration tests for `MigrationRunner` with the SQLite adapter.
//!
//! SQLite is in-memory here: every test opens a fresh database, so
//! isolation is free and there is no cross-test pollution risk.

#![cfg(feature = "sqlite-integration-tests")]

use reify::{
    Dialect, Migration, MigrationContext, MigrationRunner, SqliteDb, Table, raw_execute, raw_query,
};

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
    pub role: Option<String>,
}

#[derive(Table, Debug, Clone)]
#[table(name = "posts")]
pub struct Post {
    #[column(primary_key)]
    pub id: i64,
    pub user_id: i64,
    pub title: String,
}

/// Manual migration that adds / drops a `city` column on `users`.
struct AddUserCity;
impl Migration for AddUserCity {
    fn version(&self) -> &'static str {
        "20240320_sqlite_000001_add_user_city"
    }
    fn description(&self) -> &'static str {
        "Add city column to users"
    }
    fn up(&self, ctx: &mut MigrationContext) {
        ctx.add_column("users", "city", "TEXT NOT NULL DEFAULT ''");
    }
    fn down(&self, ctx: &mut MigrationContext) {
        ctx.drop_column("users", "city");
    }
}

fn runner() -> MigrationRunner {
    MigrationRunner::new().with_dialect(Dialect::Sqlite)
}

#[tokio::test]
async fn sqlite_migration_first_run_creates_table() {
    let db = SqliteDb::open_in_memory().expect("open db");

    runner()
        .add_table::<User>()
        .run(&db)
        .await
        .expect("first run");

    // Table must be queryable and the tracking table must exist.
    let rows = raw_query(&db, "SELECT COUNT(*) FROM users", &[])
        .await
        .expect("query users");
    assert!(!rows.is_empty());
    let tracking = raw_query(&db, "SELECT COUNT(*) FROM _reify_migrations", &[])
        .await
        .expect("query tracking");
    assert!(!tracking.is_empty());
}

#[tokio::test]
async fn sqlite_migration_second_run_is_idempotent() {
    let db = SqliteDb::open_in_memory().expect("open db");

    runner()
        .add_table::<User>()
        .run(&db)
        .await
        .expect("first run");

    let before = raw_query(&db, "SELECT COUNT(*) FROM _reify_migrations", &[])
        .await
        .expect("count before");

    runner()
        .add_table::<User>()
        .run(&db)
        .await
        .expect("second run");

    let after = raw_query(&db, "SELECT COUNT(*) FROM _reify_migrations", &[])
        .await
        .expect("count after");

    let before_v = before[0].get_idx(0).expect("count must carry a value");
    let after_v = after[0].get_idx(0).expect("count must carry a value");
    assert_eq!(
        before_v, after_v,
        "tracking table row count must be unchanged on idempotent run"
    );
}

#[tokio::test]
async fn sqlite_migration_schema_evolution_adds_new_table() {
    let db = SqliteDb::open_in_memory().expect("open db");

    runner()
        .add_table::<User>()
        .run(&db)
        .await
        .expect("first run");

    let exists = raw_query(
        &db,
        "SELECT name FROM sqlite_master WHERE type='table' AND name='posts'",
        &[],
    )
    .await
    .expect("check posts");
    assert!(exists.is_empty(), "posts must not exist after first run");

    runner()
        .add_table::<User>()
        .add_table::<Post>()
        .run(&db)
        .await
        .expect("second run with Post");

    let exists = raw_query(
        &db,
        "SELECT name FROM sqlite_master WHERE type='table' AND name='posts'",
        &[],
    )
    .await
    .expect("check posts after");
    assert!(!exists.is_empty(), "posts must exist after second run");
}

#[tokio::test]
async fn sqlite_migration_manual_add_column() {
    let db = SqliteDb::open_in_memory().expect("open db");

    runner()
        .add_table::<User>()
        .run(&db)
        .await
        .expect("create users");

    runner()
        .add_table::<User>()
        .add(AddUserCity)
        .run(&db)
        .await
        .expect("add city column");

    let exists = raw_query(
        &db,
        "SELECT name FROM pragma_table_info('users') WHERE name='city'",
        &[],
    )
    .await
    .expect("check city");
    assert!(!exists.is_empty(), "city column must exist after up()");
}

#[tokio::test]
async fn sqlite_migration_dry_run_previews_without_applying() {
    let db = SqliteDb::open_in_memory().expect("open db");

    let plans = runner()
        .add_table::<User>()
        .dry_run(&db)
        .await
        .expect("dry_run");
    assert!(!plans.is_empty(), "expected at least one pending plan");
    assert!(
        plans[0].statements.iter().any(|s| s.contains("users")),
        "plan must reference users: {:?}",
        plans[0].statements
    );

    let exists = raw_query(
        &db,
        "SELECT name FROM sqlite_master WHERE type='table' AND name='users'",
        &[],
    )
    .await
    .expect("check users after dry_run");
    assert!(exists.is_empty(), "dry_run must not create the table");
}

/// `up()` → `down()` round-trip: the column must be gone after the
/// manual rollback.
#[tokio::test]
async fn sqlite_migration_manual_down_rollback() {
    let db = SqliteDb::open_in_memory().expect("open db");

    runner()
        .add_table::<User>()
        .add(AddUserCity)
        .run(&db)
        .await
        .expect("apply up");

    let exists = raw_query(
        &db,
        "SELECT name FROM pragma_table_info('users') WHERE name='city'",
        &[],
    )
    .await
    .expect("check after up");
    assert!(!exists.is_empty(), "city must exist after up()");

    // Execute `down()` statements by hand — `MigrationRunner` does
    // not yet expose a public `.rollback()` helper.
    let mut ctx = MigrationContext::new();
    AddUserCity.down(&mut ctx);
    for stmt in ctx.statements() {
        raw_execute(&db, stmt, &[])
            .await
            .unwrap_or_else(|e| panic!("run_down failed for {stmt:?}: {e}"));
    }

    let exists = raw_query(
        &db,
        "SELECT name FROM pragma_table_info('users') WHERE name='city'",
        &[],
    )
    .await
    .expect("check after down");
    assert!(exists.is_empty(), "city must be gone after down()");
}
