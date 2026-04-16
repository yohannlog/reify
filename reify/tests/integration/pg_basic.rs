#![cfg(feature = "integration-tests")]

use reify::{
    Database, DbError, NoTls, PostgresDb, Table, Value, delete, fetch, insert, raw_execute, update,
};

use crate::{pg_config_from_url, pg_url};

#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
    #[column(nullable)]
    pub role: Option<String>,
}

async fn connect() -> Option<PostgresDb> {
    let url = pg_url()?;
    let cfg = pg_config_from_url(&url);
    Some(PostgresDb::connect(cfg, NoTls).await.expect("pg connect"))
}

async fn setup(db: &PostgresDb) {
    raw_execute(
        db,
        "CREATE TABLE IF NOT EXISTS users (
            id    BIGINT PRIMARY KEY,
            email TEXT   NOT NULL UNIQUE,
            role  TEXT
        )",
        &[],
    )
    .await
    .expect("create table");
}

async fn teardown(db: &PostgresDb) {
    raw_execute(db, "DROP TABLE IF EXISTS users", &[])
        .await
        .expect("drop table");
}

#[tokio::test]
async fn pg_insert_and_select() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let user = User {
        id: 1,
        email: "alice@example.com".into(),
        role: Some("admin".into()),
    };
    insert(&db, &User::insert(&user)).await.expect("insert");

    let rows = fetch::<User>(&db, &User::find().filter(User::id.eq(1i64)))
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], user);

    teardown(&db).await;
}

#[tokio::test]
async fn pg_update_with_filter() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let user = User {
        id: 2,
        email: "bob@example.com".into(),
        role: None,
    };
    insert(&db, &User::insert(&user)).await.expect("insert");

    update(
        &db,
        &User::update()
            .set(User::email, "bob2@example.com")
            .filter(User::id.eq(2i64)),
    )
    .await
    .expect("update");

    let rows = fetch::<User>(&db, &User::find().filter(User::id.eq(2i64)))
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].email, "bob2@example.com");

    teardown(&db).await;
}

#[tokio::test]
async fn pg_delete_with_filter() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let user = User {
        id: 3,
        email: "carol@example.com".into(),
        role: None,
    };
    insert(&db, &User::insert(&user)).await.expect("insert");

    delete(&db, &User::delete().filter(User::id.eq(3i64)))
        .await
        .expect("delete");

    let rows = fetch::<User>(&db, &User::find().filter(User::id.eq(3i64)))
        .await
        .expect("fetch");
    assert!(rows.is_empty());

    teardown(&db).await;
}

#[tokio::test]
async fn pg_transaction_rollback() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let result = db
        .transaction(Box::new(|tx| {
            Box::pin(async move {
                tx.execute(
                    "INSERT INTO users (id, email, role) VALUES (?, ?, ?)",
                    &[
                        Value::I64(5),
                        Value::String("eve@example.com".into()),
                        Value::Null,
                    ],
                )
                .await?;
                Err::<(), DbError>(DbError::Other("forced rollback".into()))
            })
        }))
        .await;

    assert!(result.is_err());

    let rows = fetch::<User>(&db, &User::find().filter(User::id.eq(5i64)))
        .await
        .expect("fetch");
    assert!(rows.is_empty(), "row must be absent after rollback");

    teardown(&db).await;
}

#[tokio::test]
async fn pg_null_round_trip() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let user = User {
        id: 6,
        email: "frank@example.com".into(),
        role: None,
    };
    insert(&db, &User::insert(&user)).await.expect("insert");

    let rows = fetch::<User>(&db, &User::find().filter(User::id.eq(6i64)))
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].role, None);

    teardown(&db).await;
}

#[tokio::test]
async fn pg_unique_constraint_error() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let user = User {
        id: 7,
        email: "grace@example.com".into(),
        role: None,
    };
    insert(&db, &User::insert(&user))
        .await
        .expect("first insert");

    let result = insert(&db, &User::insert(&user)).await;
    assert!(
        matches!(result, Err(DbError::Constraint { .. })),
        "expected Constraint error, got: {result:?}"
    );

    teardown(&db).await;
}

#[tokio::test]
async fn pg_ilike_filter() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let user = User {
        id: 8,
        email: "Heidi@Example.COM".into(),
        role: None,
    };
    insert(&db, &User::insert(&user)).await.expect("insert");

    let rows = fetch::<User>(&db, &User::find().filter(User::email.ilike("%example%")))
        .await
        .expect("fetch");
    assert!(!rows.is_empty(), "ilike should match case-insensitively");

    teardown(&db).await;
}
