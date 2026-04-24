//! Integration tests for `DbError` variants other than `Constraint`.
//!
//! The existing suite already asserts that a UNIQUE / FK violation is
//! surfaced as `DbError::Constraint`. This file closes the other
//! half of the error-mapping contract: a malformed SQL statement
//! must produce `DbError::Query(_)`, not a panic nor a silent `Ok`.

#![cfg(feature = "pg-integration-tests")]

use reify::{DbError, raw_execute, raw_query};

use crate::PgFixture;

async fn fixture() -> Option<PgFixture> {
    // No tables owned by this file — error tests run against the
    // server's system catalogue or parser.
    PgFixture::new(&[]).await
}

/// A syntactically invalid query must produce `DbError::Query`.
#[tokio::test]
async fn pg_invalid_sql_maps_to_query_error() {
    let Some(fx) = fixture().await else { return };

    let result = raw_query(&fx.db, "SELECT from_nowhere", &[]).await;
    assert!(
        matches!(result, Err(DbError::Query(_))),
        "expected DbError::Query for malformed SQL, got: {result:?}"
    );
}

/// Referencing an unknown column/table must also map to
/// `DbError::Query` (PostgreSQL reports it as a "undefined_column" or
/// "undefined_table" SQLSTATE in the 42xxx class).
#[tokio::test]
async fn pg_unknown_table_maps_to_query_error() {
    let Some(fx) = fixture().await else { return };

    let result = raw_execute(
        &fx.db,
        "INSERT INTO _pg_this_table_does_not_exist VALUES (1)",
        &[],
    )
    .await;
    assert!(
        matches!(result, Err(DbError::Query(_))),
        "expected DbError::Query for unknown table, got: {result:?}"
    );
}
