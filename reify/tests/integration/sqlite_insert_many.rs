//! Batch insert (`insert_many`) for the SQLite adapter.
//!
//! SQLite does not need a Docker container; these tests run in the
//! default `cargo test --features sqlite-integration-tests` matrix
//! in milliseconds.

#![cfg(feature = "sqlite-integration-tests")]

use reify::{SqliteDb, Table, fetch, insert_many, raw_execute};

#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
    pub role: Option<String>,
}

async fn setup(db: &SqliteDb) {
    raw_execute(
        db,
        "CREATE TABLE users (
            id    INTEGER PRIMARY KEY,
            email TEXT    NOT NULL UNIQUE,
            role  TEXT
        )",
        &[],
    )
    .await
    .expect("create table");
}

#[tokio::test]
async fn sqlite_insert_many_returns_row_count() {
    let db = SqliteDb::open_in_memory().expect("open db");
    setup(&db).await;

    let users: Vec<User> = (0..10)
        .map(|i| User {
            id: i,
            email: format!("u{i}@example.com"),
            role: if i % 2 == 0 {
                Some("admin".into())
            } else {
                None
            },
        })
        .collect();

    let affected = insert_many(&db, &User::insert_many(&users))
        .await
        .expect("insert_many");
    assert_eq!(affected, 10, "expected 10 rows inserted");

    let rows = fetch::<User>(&db, &User::find()).await.expect("fetch");
    assert_eq!(rows.len(), 10);
}

/// Inserting an empty batch at the SQL-gen layer panics, so we do
/// not test that path here. The `MockDb` unit tests already cover
/// the `InsertManyBuilder::try_new(&[])` error branch.
#[tokio::test]
async fn sqlite_insert_many_single_row() {
    let db = SqliteDb::open_in_memory().expect("open db");
    setup(&db).await;

    let user = User {
        id: 1,
        email: "only@example.com".into(),
        role: None,
    };
    let affected = insert_many(&db, &User::insert_many(std::slice::from_ref(&user)))
        .await
        .expect("insert_many one");
    assert_eq!(affected, 1);

    let rows = fetch::<User>(&db, &User::find()).await.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], user);
}
