#![cfg(feature = "integration-tests")]

use reify::mysql_async::Opts;
use reify::{Database, DbError, MysqlDb, Table, Value, delete, fetch, insert, raw_execute, update};

use crate::mysql_url;

#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
    #[column(nullable)]
    pub role: Option<String>,
}

async fn connect() -> Option<MysqlDb> {
    let url = mysql_url()?;
    let opts = Opts::from_url(&url).expect("invalid MYSQL_URL");
    Some(MysqlDb::connect(opts).await.expect("mysql connect"))
}

async fn setup(db: &MysqlDb) {
    raw_execute(
        db,
        "CREATE TABLE IF NOT EXISTS users (
            id    BIGINT       PRIMARY KEY,
            email VARCHAR(255) NOT NULL UNIQUE,
            role  VARCHAR(255)
        )",
        &[],
    )
    .await
    .expect("create table");
}

async fn teardown(db: &MysqlDb) {
    raw_execute(db, "DROP TABLE IF EXISTS users", &[])
        .await
        .expect("drop table");
}

#[tokio::test]
async fn mysql_insert_and_select() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let user = User {
        id: 1,
        email: "alice@example.com".into(),
        role: Some("admin".into()),
    };
    insert(&db, &User::insert(&user)).await.expect("insert");

    let rows = fetch::<User>(&db, &User::find().filter(User::id.eq(1i64)))
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], user);

    teardown(&db).await;
}

#[tokio::test]
async fn mysql_update_with_filter() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let user = User {
        id: 2,
        email: "bob@example.com".into(),
        role: None,
    };
    insert(&db, &User::insert(&user)).await.expect("insert");

    update(
        &db,
        &User::update()
            .set(User::email, "bob2@example.com")
            .filter(User::id.eq(2i64)),
    )
    .await
    .expect("update");

    let rows = fetch::<User>(&db, &User::find().filter(User::id.eq(2i64)))
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].email, "bob2@example.com");

    teardown(&db).await;
}

#[tokio::test]
async fn mysql_delete_with_filter() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let user = User {
        id: 3,
        email: "carol@example.com".into(),
        role: None,
    };
    insert(&db, &User::insert(&user)).await.expect("insert");

    delete(&db, &User::delete().filter(User::id.eq(3i64)))
        .await
        .expect("delete");

    let rows = fetch::<User>(&db, &User::find().filter(User::id.eq(3i64)))
        .await
        .expect("fetch");
    assert!(rows.is_empty());

    teardown(&db).await;
}

#[tokio::test]
async fn mysql_transaction_rollback() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let result = db
        .transaction(Box::new(|tx| {
            Box::pin(async move {
                tx.execute(
                    "INSERT INTO users (id, email, role) VALUES (?, ?, ?)",
                    &[
                        Value::I64(5),
                        Value::String("eve@example.com".into()),
                        Value::Null,
                    ],
                )
                .await?;
                Err::<(), DbError>(DbError::Other("forced rollback".into()))
            })
        }))
        .await;

    assert!(result.is_err());

    let rows = fetch::<User>(&db, &User::find().filter(User::id.eq(5i64)))
        .await
        .expect("fetch");
    assert!(rows.is_empty(), "row must be absent after rollback");

    teardown(&db).await;
}

#[tokio::test]
async fn mysql_null_round_trip() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let user = User {
        id: 6,
        email: "frank@example.com".into(),
        role: None,
    };
    insert(&db, &User::insert(&user)).await.expect("insert");

    let rows = fetch::<User>(&db, &User::find().filter(User::id.eq(6i64)))
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].role, None);

    teardown(&db).await;
}

#[tokio::test]
async fn mysql_unique_constraint_error() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let user = User {
        id: 7,
        email: "grace@example.com".into(),
        role: None,
    };
    insert(&db, &User::insert(&user))
        .await
        .expect("first insert");

    let result = insert(&db, &User::insert(&user)).await;
    assert!(
        matches!(result, Err(DbError::Constraint { .. })),
        "expected Constraint error, got: {result:?}"
    );

    teardown(&db).await;
}

#[tokio::test]
async fn mysql_nested_transaction_savepoint() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    // Outer transaction inserts id=10, inner (savepoint) inserts id=11 then rolls back.
    // After commit: id=10 must exist, id=11 must be absent.
    db.transaction(Box::new(|outer| {
        Box::pin(async move {
            outer
                .execute(
                    "INSERT INTO users (id, email, role) VALUES (?, ?, ?)",
                    &[
                        Value::I64(10),
                        Value::String("outer@example.com".into()),
                        Value::Null,
                    ],
                )
                .await?;

            // Nested savepoint — should roll back only the inner insert.
            let _ = outer
                .transaction(Box::new(|inner| {
                    Box::pin(async move {
                        inner
                            .execute(
                                "INSERT INTO users (id, email, role) VALUES (?, ?, ?)",
                                &[
                                    Value::I64(11),
                                    Value::String("inner@example.com".into()),
                                    Value::Null,
                                ],
                            )
                            .await?;
                        Err::<(), DbError>(DbError::Other("inner rollback".into()))
                    })
                }))
                .await;

            Ok(())
        })
    }))
    .await
    .expect("outer transaction");

    let rows = fetch::<User>(&db, &User::find().filter(User::id.eq(10i64)))
        .await
        .expect("fetch outer");
    assert_eq!(rows.len(), 1, "outer insert must be committed");

    let rows = fetch::<User>(&db, &User::find().filter(User::id.eq(11i64)))
        .await
        .expect("fetch inner");
    assert!(rows.is_empty(), "inner insert must be rolled back");

    teardown(&db).await;
}

#[tokio::test]
async fn mysql_temporal_round_trip() {
    let Some(db) = connect().await else { return };

    raw_execute(
        &db,
        "CREATE TABLE IF NOT EXISTS temporal_test (\
            id   INT PRIMARY KEY,\
            dt   DATETIME,\
            d    DATE,\
            t    TIME\
        )",
        &[],
    )
    .await
    .expect("create temporal_test");

    raw_execute(
        &db,
        "INSERT INTO temporal_test (id, dt, d, t) VALUES (?, ?, ?, ?)",
        &[
            Value::I64(1),
            Value::Timestamp(
                chrono::NaiveDateTime::parse_from_str("2024-06-15 12:30:45", "%Y-%m-%d %H:%M:%S")
                    .unwrap(),
            ),
            Value::Date(chrono::NaiveDate::parse_from_str("2024-06-15", "%Y-%m-%d").unwrap()),
            Value::Time(chrono::NaiveTime::parse_from_str("12:30:45", "%H:%M:%S").unwrap()),
        ],
    )
    .await
    .expect("insert temporal");

    let rows = reify::raw_query(
        &db,
        "SELECT dt, d, t FROM temporal_test WHERE id = ?",
        &[Value::I64(1)],
    )
    .await
    .expect("select temporal");

    assert_eq!(rows.len(), 1);
    assert!(
        matches!(rows[0].get_idx(0), Some(Value::Timestamp(_))),
        "dt must deserialize as Value::Timestamp, got: {:?}",
        rows[0].get_idx(0)
    );
    assert!(
        matches!(rows[0].get_idx(1), Some(Value::Date(_))),
        "d must deserialize as Value::Date, got: {:?}",
        rows[0].get_idx(1)
    );
    assert!(
        matches!(rows[0].get_idx(2), Some(Value::Time(_))),
        "t must deserialize as Value::Time, got: {:?}",
        rows[0].get_idx(2)
    );

    raw_execute(&db, "DROP TABLE IF EXISTS temporal_test", &[])
        .await
        .expect("drop temporal_test");
}
