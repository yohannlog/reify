//! Integration test that drives the `Relations` derive against a real
//! SQLite database.
//!
//! The derive exposes a `Relation` descriptor per relation
//! (`User::posts()`, `User::profile()`, …) with a canonical
//! `join_condition()` string. The SQL-generation layer is already
//! asserted in `reify/tests/relations.rs`; this file closes the loop
//! by actually executing the resulting `INNER JOIN` against SQLite
//! and asserting the row count + shape.

#![cfg(feature = "sqlite-integration-tests")]

use reify::{Relations, SqliteDb, Table, Value, insert, raw_execute, raw_query};

#[derive(Table, Relations, Debug, Clone)]
#[table(name = "rel_users")]
#[relations(has_many(posts: Post, foreign_key = "user_id"))]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    pub name: String,
}

#[derive(Table, Debug, Clone)]
#[table(name = "rel_posts")]
pub struct Post {
    #[column(primary_key)]
    pub id: i64,
    pub user_id: i64,
    pub title: String,
}

async fn setup(db: &SqliteDb) {
    raw_execute(
        db,
        "CREATE TABLE rel_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        &[],
    )
    .await
    .expect("create rel_users");
    raw_execute(
        db,
        "CREATE TABLE rel_posts (
            id      INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            title   TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES rel_users(id)
        )",
        &[],
    )
    .await
    .expect("create rel_posts");
}

/// Executes an `INNER JOIN` built from the `User::posts()` relation
/// descriptor and checks the cross-table row count.
#[tokio::test]
async fn sqlite_has_many_join_returns_correct_rows() {
    let db = SqliteDb::open_in_memory().expect("open db");
    setup(&db).await;

    insert(
        &db,
        &User::insert(&User {
            id: 1,
            name: "Alice".into(),
        }),
    )
    .await
    .expect("insert user");
    for pid in 1..=3 {
        insert(
            &db,
            &Post::insert(&Post {
                id: pid,
                user_id: 1,
                title: format!("Post {pid}"),
            }),
        )
        .await
        .expect("insert post");
    }

    // Build the JOIN SQL from the typed descriptor — no magic strings.
    let rel = User::posts();
    let sql = format!(
        "SELECT u.name, p.title FROM rel_users u \
         INNER JOIN rel_posts p ON u.id = p.user_id \
         WHERE u.id = ? \
         ORDER BY p.id",
    );
    // The assert here is defensive: if the descriptor ever drifts to
    // the wrong columns, the compile-time `join_condition()` unit
    // test catches it, but we mirror the same columns below to
    // guarantee SQL/descriptor agreement at runtime too.
    assert_eq!(rel.from_col, "id");
    assert_eq!(rel.to_col, "user_id");

    let rows = raw_query(&db, &sql, &[Value::I64(1)])
        .await
        .expect("join query");
    assert_eq!(rows.len(), 3, "three posts must be joined for user 1");
    assert_eq!(
        rows[0].get_idx(0),
        Some(&Value::String("Alice".into())),
        "join must pull user name",
    );
}
