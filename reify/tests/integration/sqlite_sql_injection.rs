//! End-to-end proof that parameter binding neutralises SQL injection
//! attempts against SQLite.
//!
//! `reify/tests/sql_injection.rs` only checks identifier quoting at
//! the SQL-gen layer. This file commits a **hostile string** through
//! the real bind path and asserts that (a) the table still exists
//! after the statement and (b) the raw string is recovered intact.

#![cfg(feature = "sqlite-integration-tests")]

use reify::{SqliteDb, Table, fetch, insert, raw_execute, raw_query};

#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "sqli_users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    pub name: String,
}

#[tokio::test]
async fn sqlite_parameter_binding_neutralises_injection() {
    let db = SqliteDb::open_in_memory().expect("open db");
    raw_execute(
        &db,
        "CREATE TABLE sqli_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        &[],
    )
    .await
    .expect("create");

    let hostile = "'); DROP TABLE sqli_users; --".to_string();
    insert(
        &db,
        &User::insert(&User {
            id: 1,
            name: hostile.clone(),
        }),
    )
    .await
    .expect("insert");

    // Table must still exist after the injection attempt.
    let still_there = raw_query(
        &db,
        "SELECT name FROM sqlite_master WHERE type='table' AND name='sqli_users'",
        &[],
    )
    .await
    .expect("check schema");
    assert!(!still_there.is_empty(), "table must survive the payload");

    // Value must round-trip byte-for-byte.
    let rows = fetch::<User>(&db, &User::find().filter(User::id.eq(1i64)))
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, hostile);
}
