#![cfg(feature = "pg-integration-tests")]

use reify::{Database, DbError, NoTls, PostgresDb, Table, Value, fetch, raw_execute};

use crate::{PgFixture, pg_config_from_url, pg_url};

#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "copy_products")]
pub struct Product {
    #[column(primary_key)]
    pub id: i64,
    pub name: String,
    pub price: f64,
    pub in_stock: bool,
    pub category: Option<String>,
}

async fn connect() -> Option<PostgresDb> {
    let url = pg_url()?;
    let cfg = pg_config_from_url(&url);
    Some(PostgresDb::connect(cfg, NoTls).await.expect("pg connect"))
}

async fn setup(db: &PostgresDb) {
    raw_execute(
        db,
        "CREATE TABLE IF NOT EXISTS copy_products (
            id       BIGINT PRIMARY KEY,
            name     TEXT    NOT NULL,
            price    DOUBLE PRECISION NOT NULL,
            in_stock BOOLEAN NOT NULL,
            category TEXT
        )",
        &[],
    )
    .await
    .expect("create table");
}

async fn teardown(db: &PostgresDb) {
    raw_execute(db, "DROP TABLE IF EXISTS copy_products", &[])
        .await
        .expect("drop table");
}

#[tokio::test]
async fn pg_copy_in_basic() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let products = vec![
        Product {
            id: 1,
            name: "Widget".into(),
            price: 9.99,
            in_stock: true,
            category: Some("Hardware".into()),
        },
        Product {
            id: 2,
            name: "Gadget".into(),
            price: 19.99,
            in_stock: false,
            category: None,
        },
        Product {
            id: 3,
            name: "Thingama* jig".into(),
            price: 29.99,
            in_stock: true,
            category: Some("Software".into()),
        },
    ];

    let rows_affected = db.copy_in(&products).await.expect("copy_in");
    assert_eq!(rows_affected, 3);

    let fetched = fetch::<Product>(&db, &Product::find().order_by(Product::id.asc()))
        .await
        .expect("fetch");

    assert_eq!(fetched.len(), 3);
    assert_eq!(fetched[0], products[0]);
    assert_eq!(fetched[1], products[1]);
    assert_eq!(fetched[2], products[2]);

    teardown(&db).await;
}

#[tokio::test]
async fn pg_copy_in_empty_returns_zero() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let rows_affected = db.copy_in::<Product>(&[]).await.expect("copy_in empty");
    assert_eq!(rows_affected, 0);

    teardown(&db).await;
}

#[tokio::test]
async fn pg_copy_in_large_batch() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let products: Vec<Product> = (1..=1000)
        .map(|i| Product {
            id: i,
            name: format!("Product-{i}"),
            price: i as f64 * 0.99,
            in_stock: i % 2 == 0,
            category: if i % 3 == 0 {
                Some("Category-A".into())
            } else {
                Some("Category-B".into())
            },
        })
        .collect();

    let rows_affected = db.copy_in(&products).await.expect("copy_in large batch");
    assert_eq!(rows_affected, 1000);

    let count = db
        .query_one("SELECT COUNT(*)::bigint AS c FROM copy_products", &[])
        .await
        .expect("count");
    assert_eq!(count.get("c"), Some(&Value::I64(1000)));

    teardown(&db).await;
}

#[tokio::test]
async fn pg_copy_in_unique_constraint_error() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    let products = vec![
        Product {
            id: 1,
            name: "Widget".into(),
            price: 9.99,
            in_stock: true,
            category: None,
        },
        Product {
            id: 1,
            name: "Duplicate".into(),
            price: 1.00,
            in_stock: false,
            category: None,
        },
    ];

    let result = db.copy_in(&products).await;
    assert!(
        matches!(result, Err(DbError::Constraint { .. })),
        "expected Constraint error for duplicate PK, got: {result:?}"
    );

    teardown(&db).await;
}

// ── Advanced-type round-trip via COPY ────────────────────────────────
//
// `copy_in` drives the PostgreSQL binary-COPY protocol, which has its
// own type encoders per `Value` variant. The tests above cover the
// scalar variants (`I64`, `String`, `F64`, `Bool`, `Option<String>`);
// this final test exercises the less common variants —
// `Uuid`, `Timestamptz`, `Jsonb`, and `ArrayI64` — in a single model
// to keep the fixture small.

use uuid::Uuid;

#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "copy_events")]
pub struct CopyEvent {
    #[column(primary_key)]
    pub id: Uuid,
    pub happened_at: chrono::DateTime<chrono::Utc>,
    pub payload: serde_json::Value,
    pub tags: Vec<i64>,
}

#[tokio::test]
async fn pg_copy_in_advanced_types() {
    let Some(fx) = PgFixture::new(&["copy_events"]).await else {
        return;
    };

    raw_execute(
        &fx.db,
        "CREATE TABLE copy_events (
            id          UUID        PRIMARY KEY,
            happened_at TIMESTAMPTZ NOT NULL,
            payload     JSONB       NOT NULL,
            tags        BIGINT[]    NOT NULL
        )",
        &[],
    )
    .await
    .expect("create copy_events");

    let id = Uuid::new_v4();
    let ts = chrono::Utc::now();
    let event = CopyEvent {
        id,
        happened_at: ts,
        payload: serde_json::json!({"k": "v", "n": 42}),
        tags: vec![1, 2, 3],
    };

    let affected = fx.db.copy_in(&[event.clone()]).await.expect("copy_in");
    assert_eq!(affected, 1);

    let rows = fetch::<CopyEvent>(&fx.db, &CopyEvent::find().filter(CopyEvent::id.eq(id)))
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1);
    // `TIMESTAMPTZ` comparisons: tolerate sub-microsecond drift on
    // `copy_in` by comparing timestamps to microsecond precision.
    assert_eq!(rows[0].id, event.id);
    assert_eq!(rows[0].payload, event.payload);
    assert_eq!(rows[0].tags, event.tags);
    let delta = (rows[0].happened_at - event.happened_at)
        .num_microseconds()
        .map(i64::abs)
        .unwrap_or(i64::MAX);
    assert!(
        delta <= 1,
        "happened_at must round-trip within 1 µs (delta: {delta} µs)"
    );

    fx.teardown().await;
}
