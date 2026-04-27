//! End-to-end round-trip of PostgreSQL array columns.
//!
//! The `Value::ArrayI64` / `Value::ArrayString` variants are
//! serialised by the adapter's `ToSql` impl in `reify-postgres/src/lib.rs`.
//! Existing unit tests assert the `IntoValue` path; this file
//! closes the loop by inserting arrays through the adapter,
//! reading them back, and asserting element-level equality.

#![cfg(feature = "pg-integration-tests")]

use reify::{Value, raw_execute, raw_query};

use crate::PgFixture;

#[tokio::test]
async fn pg_array_i64_round_trip() {
    let Some(fx) = PgFixture::new(&["pg_array_rows"]).await else {
        return;
    };

    raw_execute(
        &fx.db,
        "CREATE TABLE pg_array_rows (id BIGINT PRIMARY KEY, tags BIGINT[] NOT NULL)",
        &[],
    )
    .await
    .expect("create");

    let values = vec![1i64, 2, 3, 1_000_000_000_000];
    raw_execute(
        &fx.db,
        "INSERT INTO pg_array_rows (id, tags) VALUES (?, ?)",
        &[Value::I64(1), Value::ArrayI64(values.clone())],
    )
    .await
    .expect("insert array");

    let rows = raw_query(
        &fx.db,
        "SELECT tags FROM pg_array_rows WHERE id = ?",
        &[Value::I64(1)],
    )
    .await
    .expect("fetch array");
    assert_eq!(rows.len(), 1);
    match rows[0].get_idx(0) {
        Some(Value::ArrayI64(v)) => assert_eq!(v, &values, "array must round-trip exactly"),
        other => panic!("expected ArrayI64, got {other:?}"),
    }

    fx.teardown().await;
}

#[tokio::test]
async fn pg_array_text_round_trip() {
    let Some(fx) = PgFixture::new(&["pg_array_text_rows"]).await else {
        return;
    };

    raw_execute(
        &fx.db,
        "CREATE TABLE pg_array_text_rows (id BIGINT PRIMARY KEY, labels TEXT[] NOT NULL)",
        &[],
    )
    .await
    .expect("create");

    let labels = vec![
        "rust".to_string(),
        "go".to_string(),
        "it's 'ok'".to_string(),
    ];
    raw_execute(
        &fx.db,
        "INSERT INTO pg_array_text_rows (id, labels) VALUES (?, ?)",
        &[Value::I64(1), Value::ArrayString(labels.clone())],
    )
    .await
    .expect("insert text array");

    let rows = raw_query(
        &fx.db,
        "SELECT labels FROM pg_array_text_rows WHERE id = ?",
        &[Value::I64(1)],
    )
    .await
    .expect("fetch text array");
    match rows[0].get_idx(0) {
        Some(Value::ArrayString(v)) => assert_eq!(v, &labels, "text array must round-trip"),
        other => panic!("expected ArrayString, got {other:?}"),
    }

    fx.teardown().await;
}
