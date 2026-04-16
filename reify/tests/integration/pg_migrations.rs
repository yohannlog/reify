#![cfg(feature = "integration-tests")]

use reify::{NoTls, PostgresDb, Table, migration::MigrationRunner, raw_execute, raw_query};

use crate::{pg_config_from_url, pg_url};

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
    #[column(nullable)]
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

async fn connect() -> Option<PostgresDb> {
    let url = pg_url()?;
    let cfg = pg_config_from_url(&url);
    Some(PostgresDb::connect(cfg, NoTls).await.expect("pg connect"))
}

async fn cleanup(db: &PostgresDb) {
    for table in &["posts", "users", "_reify_migrations"] {
        raw_execute(db, &format!("DROP TABLE IF EXISTS {table}"), &[])
            .await
            .expect("drop table");
    }
}

#[tokio::test]
async fn pg_migration_first_run_creates_table() {
    let Some(db) = connect().await else { return };
    cleanup(&db).await;

    MigrationRunner::new()
        .add_table::<User>()
        .run(&db)
        .await
        .expect("first run");

    let rows = raw_query(&db, "SELECT COUNT(*) FROM users", &[])
        .await
        .expect("query users");
    assert!(!rows.is_empty());

    let tracking = raw_query(&db, "SELECT COUNT(*) FROM _reify_migrations", &[])
        .await
        .expect("query tracking");
    assert!(!tracking.is_empty());

    cleanup(&db).await;
}

#[tokio::test]
async fn pg_migration_second_run_is_idempotent() {
    let Some(db) = connect().await else { return };
    cleanup(&db).await;

    MigrationRunner::new()
        .add_table::<User>()
        .run(&db)
        .await
        .expect("first run");

    let count_before = raw_query(&db, "SELECT COUNT(*) FROM _reify_migrations", &[])
        .await
        .expect("count before");

    MigrationRunner::new()
        .add_table::<User>()
        .run(&db)
        .await
        .expect("second run");

    let count_after = raw_query(&db, "SELECT COUNT(*) FROM _reify_migrations", &[])
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
async fn pg_migration_schema_evolution_adds_new_table() {
    let Some(db) = connect().await else { return };
    cleanup(&db).await;

    MigrationRunner::new()
        .add_table::<User>()
        .run(&db)
        .await
        .expect("first run");

    let posts_exists = raw_query(
        &db,
        "SELECT 1 FROM information_schema.tables WHERE table_name = 'posts'",
        &[],
    )
    .await
    .expect("check posts");
    assert!(
        posts_exists.is_empty(),
        "posts table must not exist after first run"
    );

    MigrationRunner::new()
        .add_table::<User>()
        .add_table::<Post>()
        .run(&db)
        .await
        .expect("second run with Post");

    let posts_exists = raw_query(
        &db,
        "SELECT 1 FROM information_schema.tables WHERE table_name = 'posts'",
        &[],
    )
    .await
    .expect("check posts after");
    assert!(
        !posts_exists.is_empty(),
        "posts table must exist after second run"
    );

    cleanup(&db).await;
}
