#![cfg(feature = "integration-tests")]

use reify::mysql_async::Opts;
use reify::{Database, DbError, MysqlDb, Table, Value, delete, fetch, insert, raw_execute, update};

use crate::mysql_url;

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

async fn connect() -> Option<MysqlDb> {
    let url = mysql_url()?;
    let opts = Opts::from_url(&url).expect("invalid MYSQL_URL");
    Some(MysqlDb::connect(opts).await.expect("mysql connect"))
}

async fn setup(db: &MysqlDb) {
    raw_execute(
        db,
        "CREATE TABLE IF NOT EXISTS users (
            id    BIGINT       PRIMARY KEY,
            email VARCHAR(255) NOT NULL UNIQUE,
            role  VARCHAR(255)
        )",
        &[],
    )
    .await
    .expect("create table");
}

async fn teardown(db: &MysqlDb) {
    raw_execute(db, "DROP TABLE IF EXISTS users", &[])
        .await
        .expect("drop table");
}

#[tokio::test]
async fn mysql_insert_and_select() {
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
async fn mysql_update_with_filter() {
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
async fn mysql_delete_with_filter() {
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
async fn mysql_transaction_rollback() {
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
async fn mysql_null_round_trip() {
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
async fn mysql_unique_constraint_error() {
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
