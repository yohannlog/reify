//! SQLite upsert integration tests.
//!
//! SQLite accepts both `INSERT OR IGNORE` and `INSERT … ON CONFLICT
//! (col) DO UPDATE SET …`. The builder API exposes them as
//! `on_conflict_do_nothing()` / `on_conflict_do_update(&[col], &[col])`
//! and this file asserts the semantics round-trip against the engine.

#![cfg(feature = "sqlite-integration-tests")]

use reify::{SqliteDb, Table, fetch, insert, raw_execute};

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
async fn sqlite_upsert_do_nothing() {
    let db = SqliteDb::open_in_memory().expect("open db");
    setup(&db).await;

    let user = User {
        id: 1,
        email: "a@example.com".into(),
        role: None,
    };
    insert(&db, &User::insert(&user))
        .await
        .expect("first insert");

    // Re-inserting the exact same row with ON CONFLICT DO NOTHING
    // must succeed and leave the table unchanged.
    insert(&db, &User::insert(&user).on_conflict_do_nothing())
        .await
        .expect("on_conflict_do_nothing");

    let rows = fetch::<User>(&db, &User::find())
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1, "no new row must be inserted");
    assert_eq!(rows[0].role, None, "original row must be untouched");
}

#[tokio::test]
async fn sqlite_upsert_do_update() {
    let db = SqliteDb::open_in_memory().expect("open db");
    setup(&db).await;

    let user = User {
        id: 1,
        email: "a@example.com".into(),
        role: None,
    };
    insert(&db, &User::insert(&user))
        .await
        .expect("first insert");

    let updated = User {
        id: 1,
        email: "a@example.com".into(),
        role: Some("admin".into()),
    };
    insert(
        &db,
        &User::insert(&updated).on_conflict_do_update(&["email"], &["role"]),
    )
    .await
    .expect("on_conflict_do_update");

    let rows = fetch::<User>(&db, &User::find())
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].role,
        Some("admin".into()),
        "role must be updated by upsert"
    );
}
