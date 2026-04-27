//! End-to-end round-trip for `#[derive(DbEnum)]` against SQLite.
//!
//! The existing `reify/tests/derive_enum.rs` unit tests assert:
//! - `Role::variants()`, `as_str()`, `from_str()`
//! - `IntoValue` emits `Value::String("admin")`
//! - `enum_from_value` roundtrip at the `Value` layer
//!
//! This file checks that inserting a struct holding an enum field
//! into a `TEXT` column and reading it back yields the same variant.

#![cfg(feature = "sqlite-integration-tests")]

use reify::{DbEnum, SqliteDb, Table, Value, enum_from_value, insert, raw_execute, raw_query};

#[derive(DbEnum, Debug, Clone, Copy, PartialEq)]
pub enum Role {
    Admin,
    Member,
    Guest,
}

/// Keep the enum out of the `Table` derive (it doesn't implement
/// `FromRow` / `IntoValue` for model-level use) and persist the
/// string representation by hand via `raw_execute`.
#[derive(Table, Debug, Clone)]
#[table(name = "enum_users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    pub name: String,
    pub role: String,
}

async fn setup(db: &SqliteDb) {
    raw_execute(
        db,
        "CREATE TABLE enum_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, role TEXT NOT NULL)",
        &[],
    )
    .await
    .expect("create enum_users");
}

#[tokio::test]
async fn sqlite_enum_round_trip() {
    let db = SqliteDb::open_in_memory().expect("open db");
    setup(&db).await;

    // Persist one row per variant — the role is stored as its
    // `as_str()` representation, which is the contract the macro
    // publishes.
    for (id, r) in [(1i64, Role::Admin), (2, Role::Member), (3, Role::Guest)] {
        insert(
            &db,
            &User::insert(&User {
                id,
                name: format!("user-{id}"),
                role: r.as_str().to_string(),
            }),
        )
        .await
        .expect("insert user");
    }

    let rows = raw_query(&db, "SELECT role FROM enum_users ORDER BY id", &[])
        .await
        .expect("fetch roles");
    assert_eq!(rows.len(), 3);

    let variants: Vec<Role> = rows
        .iter()
        .map(|r| {
            let v = r.get_idx(0).expect("role column must be present").clone();
            enum_from_value::<Role>(&v).expect("decode enum")
        })
        .collect();
    assert_eq!(variants, vec![Role::Admin, Role::Member, Role::Guest]);

    // Unknown variant must fail the decode with a clear error.
    let bogus = Value::String("superadmin".into());
    let err = enum_from_value::<Role>(&bogus);
    assert!(err.is_err(), "unknown variant must not decode");
}
