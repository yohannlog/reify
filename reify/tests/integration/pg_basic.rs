//! Basic integration tests for the PostgreSQL adapter.
//!
//! Covers the round-trip of `insert` / `update` / `delete` / `find`,
//! transactional paths (commit, rollback, nested savepoint), null /
//! unique constraint handling, case-insensitive `ILIKE`, and foreign
//! key violations.
//!
//! The model maps to a **dedicated** table name (`pg_basic_users`)
//! so the file no longer collides with `pg_migrations.rs::users`.
//! This means the suite no longer requires `--test-threads=1`.

#![cfg(feature = "pg-integration-tests")]

use reify::{
    Database, DbError, Table, Value, delete, fetch, insert, insert_many, raw_execute, update,
};

use crate::PgFixture;

#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "pg_basic_users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
    pub role: Option<String>,
}

/// Create the `pg_basic_users` table fresh. DROP is unconditional —
/// we never rely on `CREATE TABLE IF NOT EXISTS`, which would mask
/// schema drift if the `User` struct changes between runs.
async fn create_users(db: &reify::PostgresDb) {
    raw_execute(
        db,
        "CREATE TABLE pg_basic_users (
            id    BIGINT PRIMARY KEY,
            email TEXT   NOT NULL UNIQUE,
            role  TEXT
        )",
        &[],
    )
    .await
    .expect("create pg_basic_users");
}

/// Open a fixture that owns the tables exercised in this file.
/// `pg_basic_users` is always dropped; `pg_basic_parents` /
/// `pg_basic_children` are used by the FK-violation test only but
/// listing them here makes cleanup robust to panics.
async fn fixture() -> Option<PgFixture> {
    PgFixture::new(&["pg_basic_children", "pg_basic_parents", "pg_basic_users"]).await
}

#[tokio::test]
async fn pg_insert_and_select() {
    let Some(fx) = fixture().await else { return };
    create_users(&fx.db).await;

    let user = User {
        id: 1,
        email: "alice@example.com".into(),
        role: Some("admin".into()),
    };
    insert(&fx.db, &User::insert(&user)).await.expect("insert");

    let rows = fetch::<User>(&fx.db, &User::find().filter(User::id.eq(1i64)))
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], user);

    fx.teardown().await;
}

#[tokio::test]
async fn pg_update_with_filter() {
    let Some(fx) = fixture().await else { return };
    create_users(&fx.db).await;

    let user = User {
        id: 2,
        email: "bob@example.com".into(),
        role: None,
    };
    insert(&fx.db, &User::insert(&user)).await.expect("insert");

    update(
        &fx.db,
        &User::update()
            .set(User::email, "bob2@example.com")
            .filter(User::id.eq(2i64)),
    )
    .await
    .expect("update");

    let rows = fetch::<User>(&fx.db, &User::find().filter(User::id.eq(2i64)))
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].email, "bob2@example.com");

    fx.teardown().await;
}

#[tokio::test]
async fn pg_delete_with_filter() {
    let Some(fx) = fixture().await else { return };
    create_users(&fx.db).await;

    let user = User {
        id: 3,
        email: "carol@example.com".into(),
        role: None,
    };
    insert(&fx.db, &User::insert(&user)).await.expect("insert");

    delete(&fx.db, &User::delete().filter(User::id.eq(3i64)))
        .await
        .expect("delete");

    let rows = fetch::<User>(&fx.db, &User::find().filter(User::id.eq(3i64)))
        .await
        .expect("fetch");
    assert!(rows.is_empty());

    fx.teardown().await;
}

#[tokio::test]
async fn pg_transaction_rollback() {
    let Some(fx) = fixture().await else { return };
    create_users(&fx.db).await;

    let result = fx
        .db
        .transaction(Box::new(|tx| {
            Box::pin(async move {
                let affected = tx
                    .execute(
                        "INSERT INTO pg_basic_users (id, email, role) VALUES (?, ?, ?)",
                        &[
                            Value::I64(5),
                            Value::String("eve@example.com".into()),
                            Value::Null,
                        ],
                    )
                    .await?;
                assert_eq!(affected, 1, "INSERT inside tx must report 1 affected row");
                Err::<(), DbError>(DbError::Other("forced rollback".into()))
            })
        }))
        .await;

    assert!(result.is_err());

    let rows = fetch::<User>(&fx.db, &User::find().filter(User::id.eq(5i64)))
        .await
        .expect("fetch");
    assert!(rows.is_empty(), "row must be absent after rollback");

    fx.teardown().await;
}

/// Transaction that returns `Ok(())` — the insert must be committed
/// and visible after the transaction closes. This closes the
/// "commit-path" parity gap with the MySQL suite.
#[tokio::test]
async fn pg_transaction_commit() {
    let Some(fx) = fixture().await else { return };
    create_users(&fx.db).await;

    fx.db
        .transaction(Box::new(|tx| {
            Box::pin(async move {
                tx.execute(
                    "INSERT INTO pg_basic_users (id, email, role) VALUES (?, ?, ?)",
                    &[
                        Value::I64(50),
                        Value::String("commit@example.com".into()),
                        Value::Null,
                    ],
                )
                .await?;
                Ok(())
            })
        }))
        .await
        .expect("commit transaction");

    let rows = fetch::<User>(&fx.db, &User::find().filter(User::id.eq(50i64)))
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1, "row must be committed");
    assert_eq!(rows[0].email, "commit@example.com");

    fx.teardown().await;
}

/// Outer transaction commits an insert; the nested savepoint inserts
/// another row and rolls back. Only the outer insert must remain.
#[tokio::test]
async fn pg_nested_transaction_savepoint() {
    let Some(fx) = fixture().await else { return };
    create_users(&fx.db).await;

    fx.db
        .transaction(Box::new(|outer| {
            Box::pin(async move {
                outer
                    .execute(
                        "INSERT INTO pg_basic_users (id, email, role) VALUES (?, ?, ?)",
                        &[
                            Value::I64(60),
                            Value::String("outer@example.com".into()),
                            Value::Null,
                        ],
                    )
                    .await?;

                // Capture the inner result to assert the savepoint
                // actually reported the error (not a silent Ok).
                let inner_res = outer
                    .transaction(Box::new(|inner| {
                        Box::pin(async move {
                            inner
                                .execute(
                                    "INSERT INTO pg_basic_users (id, email, role) VALUES (?, ?, ?)",
                                    &[
                                        Value::I64(61),
                                        Value::String("inner@example.com".into()),
                                        Value::Null,
                                    ],
                                )
                                .await?;
                            Err::<(), DbError>(DbError::Other("inner rollback".into()))
                        })
                    }))
                    .await;
                assert!(
                    inner_res.is_err(),
                    "inner savepoint must bubble up the error (got: {inner_res:?})"
                );

                Ok(())
            })
        }))
        .await
        .expect("outer transaction");

    let rows = fetch::<User>(&fx.db, &User::find().filter(User::id.eq(60i64)))
        .await
        .expect("fetch outer");
    assert_eq!(rows.len(), 1, "outer insert must be committed");

    let rows = fetch::<User>(&fx.db, &User::find().filter(User::id.eq(61i64)))
        .await
        .expect("fetch inner");
    assert!(rows.is_empty(), "inner insert must be rolled back");

    fx.teardown().await;
}

#[tokio::test]
async fn pg_null_round_trip() {
    let Some(fx) = fixture().await else { return };
    create_users(&fx.db).await;

    let user = User {
        id: 6,
        email: "frank@example.com".into(),
        role: None,
    };
    insert(&fx.db, &User::insert(&user)).await.expect("insert");

    let rows = fetch::<User>(&fx.db, &User::find().filter(User::id.eq(6i64)))
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].role, None);

    fx.teardown().await;
}

#[tokio::test]
async fn pg_unique_constraint_error() {
    let Some(fx) = fixture().await else { return };
    create_users(&fx.db).await;

    let user = User {
        id: 7,
        email: "grace@example.com".into(),
        role: None,
    };
    insert(&fx.db, &User::insert(&user))
        .await
        .expect("first insert");

    let result = insert(&fx.db, &User::insert(&user)).await;
    assert!(
        matches!(result, Err(DbError::Constraint { .. })),
        "expected Constraint error, got: {result:?}"
    );

    fx.teardown().await;
}

#[tokio::test]
async fn pg_ilike_filter() {
    let Some(fx) = fixture().await else { return };
    create_users(&fx.db).await;

    let user = User {
        id: 8,
        email: "Heidi@Example.COM".into(),
        role: None,
    };
    insert(&fx.db, &User::insert(&user)).await.expect("insert");

    let rows = fetch::<User>(&fx.db, &User::find().filter(User::email.ilike("%example%")))
        .await
        .expect("fetch");
    assert!(!rows.is_empty(), "ilike should match case-insensitively");

    fx.teardown().await;
}

/// Batch insert — closes the `insert_many` parity gap with MySQL.
#[tokio::test]
async fn pg_insert_many() {
    let Some(fx) = fixture().await else { return };
    create_users(&fx.db).await;

    let users = vec![
        User {
            id: 20,
            email: "u20@example.com".into(),
            role: None,
        },
        User {
            id: 21,
            email: "u21@example.com".into(),
            role: Some("editor".into()),
        },
        User {
            id: 22,
            email: "u22@example.com".into(),
            role: None,
        },
    ];

    let affected = insert_many(&fx.db, &User::insert_many(&users))
        .await
        .expect("insert_many");
    assert_eq!(affected, 3, "expected 3 rows inserted");

    let rows = fetch::<User>(
        &fx.db,
        &User::find().filter(User::id.gte(20i64).and(User::id.lte(22i64))),
    )
    .await
    .expect("fetch");
    assert_eq!(rows.len(), 3);

    fx.teardown().await;
}

/// Postgres-flavoured upsert: `ON CONFLICT (col) DO NOTHING`.
#[tokio::test]
async fn pg_upsert_do_nothing() {
    let Some(fx) = fixture().await else { return };
    create_users(&fx.db).await;

    let user = User {
        id: 30,
        email: "upsert_ignore@example.com".into(),
        role: None,
    };
    insert(&fx.db, &User::insert(&user))
        .await
        .expect("first insert");

    // Same email (unique), different PK — DO NOTHING must succeed and
    // report 0 affected rows.
    let conflict = User {
        id: 31,
        email: "upsert_ignore@example.com".into(),
        role: Some("admin".into()),
    };
    let affected = insert(&fx.db, &User::insert(&conflict).on_conflict_do_nothing())
        .await
        .expect("upsert do nothing");
    assert_eq!(
        affected, 0,
        "ON CONFLICT DO NOTHING on duplicate must affect 0 rows"
    );

    let rows = fetch::<User>(
        &fx.db,
        &User::find().filter(User::email.eq("upsert_ignore@example.com")),
    )
    .await
    .expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].role, None, "existing role must be preserved");

    fx.teardown().await;
}

/// Postgres-flavoured upsert: `ON CONFLICT (col) DO UPDATE SET …`.
#[tokio::test]
async fn pg_upsert_do_update() {
    let Some(fx) = fixture().await else { return };
    create_users(&fx.db).await;

    let user = User {
        id: 40,
        email: "upsert_update@example.com".into(),
        role: None,
    };
    insert(&fx.db, &User::insert(&user))
        .await
        .expect("first insert");

    let updated = User {
        id: 40,
        email: "upsert_update@example.com".into(),
        role: Some("admin".into()),
    };
    insert(
        &fx.db,
        &User::insert(&updated).on_conflict_do_update(&["email"], &["role"]),
    )
    .await
    .expect("upsert do update");

    let rows = fetch::<User>(&fx.db, &User::find().filter(User::id.eq(40i64)))
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].role,
        Some("admin".into()),
        "role must be updated by upsert"
    );

    fx.teardown().await;
}

/// Foreign-key violation: inserting a child that references a
/// non-existent parent must return `DbError::Constraint`.
#[tokio::test]
async fn pg_foreign_key_violation() {
    let Some(fx) = fixture().await else { return };
    raw_execute(
        &fx.db,
        "CREATE TABLE pg_basic_parents (id BIGINT PRIMARY KEY)",
        &[],
    )
    .await
    .expect("create parents");
    raw_execute(
        &fx.db,
        "CREATE TABLE pg_basic_children (
            id        BIGINT PRIMARY KEY,
            parent_id BIGINT NOT NULL REFERENCES pg_basic_parents(id)
        )",
        &[],
    )
    .await
    .expect("create children");

    let result = raw_execute(
        &fx.db,
        "INSERT INTO pg_basic_children (id, parent_id) VALUES (?, ?)",
        &[Value::I64(1), Value::I64(999)],
    )
    .await;
    assert!(
        matches!(result, Err(DbError::Constraint { .. })),
        "expected FK Constraint error, got: {result:?}"
    );

    // Valid FK must succeed.
    raw_execute(
        &fx.db,
        "INSERT INTO pg_basic_parents (id) VALUES (?)",
        &[Value::I64(1)],
    )
    .await
    .expect("insert parent");
    raw_execute(
        &fx.db,
        "INSERT INTO pg_basic_children (id, parent_id) VALUES (?, ?)",
        &[Value::I64(1), Value::I64(1)],
    )
    .await
    .expect("insert child with valid FK");

    fx.teardown().await;
}
