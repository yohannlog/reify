//! MySQL counterpart to `sqlite_sql_injection.rs`.

#![cfg(feature = "mysql-integration-tests")]

use reify::{Table, fetch, insert, raw_execute, raw_query};

use crate::MysqlFixture;

#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "mysql_sqli_users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    pub name: String,
}

#[tokio::test]
async fn mysql_parameter_binding_neutralises_injection() {
    let Some(fx) = MysqlFixture::new(&["mysql_sqli_users"]).await else {
        return;
    };

    raw_execute(
        &fx.db,
        "CREATE TABLE mysql_sqli_users (id BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL)",
        &[],
    )
    .await
    .expect("create");

    let hostile = "'); DROP TABLE mysql_sqli_users; --".to_string();
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
        "SELECT 1 FROM information_schema.tables \
         WHERE table_name = 'mysql_sqli_users' AND table_schema = DATABASE()",
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
