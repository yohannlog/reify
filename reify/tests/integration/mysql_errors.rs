//! MySQL counterpart to `pg_errors.rs`: asserts that malformed SQL
//! produces `DbError::Query(_)` (not `DbError::Constraint`, which is
//! already covered by the existing unique/FK tests).

#![cfg(feature = "mysql-integration-tests")]

use reify::{DbError, raw_execute, raw_query};

use crate::MysqlFixture;

async fn fixture() -> Option<MysqlFixture> {
    MysqlFixture::new(&[]).await
}

#[tokio::test]
async fn mysql_invalid_sql_maps_to_query_error() {
    let Some(fx) = fixture().await else { return };

    let result = raw_query(&fx.db, "SELECT from_nowhere", &[]).await;
    assert!(
        matches!(result, Err(DbError::Query(_))),
        "expected DbError::Query for malformed SQL, got: {result:?}"
    );
}

#[tokio::test]
async fn mysql_unknown_table_maps_to_query_error() {
    let Some(fx) = fixture().await else { return };

    let result = raw_execute(
        &fx.db,
        "INSERT INTO `_mysql_this_table_does_not_exist` VALUES (1)",
        &[],
    )
    .await;
    assert!(
        matches!(result, Err(DbError::Query(_))),
        "expected DbError::Query for unknown table, got: {result:?}"
    );
}
