//! PostgreSQL counterpart to `sqlite_sql_injection.rs`.

#![cfg(feature = "pg-integration-tests")]

use reify::{Table, fetch, insert, raw_execute, raw_query};

use crate::PgFixture;

#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "pg_sqli_users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    pub name: String,
}

#[tokio::test]
async fn pg_parameter_binding_neutralises_injection() {
    let Some(fx) = PgFixture::new(&["pg_sqli_users"]).await else {
        return;
    };

    raw_execute(
        &fx.db,
        "CREATE TABLE pg_sqli_users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
        &[],
    )
    .await
    .expect("create");

    let hostile = "'); DROP TABLE pg_sqli_users; --".to_string();
    insert(
        &fx.db,
        &User::insert(&User {
            id: 1,
            name: hostile.clone(),
        }),
    )
    .await
    .expect("insert");

    let exists = raw_query(
        &fx.db,
        "SELECT 1 FROM information_schema.tables WHERE table_name = 'pg_sqli_users'",
        &[],
    )
    .await
    .expect("check table");
    assert!(!exists.is_empty(), "table must survive the payload");

    let rows = fetch::<User>(&fx.db, &User::find().filter(User::id.eq(1i64)))
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, hostile);

    fx.teardown().await;
}
