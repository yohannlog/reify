//! Offset- and cursor-pagination integration tests against SQLite.
//!
//! Closes the SQL-gen ↔ DB round-trip gap for `.paginate()`
//! (offset-based) and `.after()` / `.before()` (cursor-based). The
//! existing unit tests in `reify/tests/cursor_pagination.rs` only
//! exercise the SQL builder.

#![cfg(feature = "sqlite-integration-tests")]

use reify::{Page, SqliteDb, Table, Value, insert, raw_execute, raw_query};

#[derive(Table, Debug, Clone)]
#[table(name = "paginate_users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    pub name: String,
}

async fn seed(db: &SqliteDb, count: i64) {
    raw_execute(
        db,
        "CREATE TABLE paginate_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        &[],
    )
    .await
    .expect("create paginate_users");
    for i in 1..=count {
        insert(
            db,
            &User::insert(&User {
                id: i,
                name: format!("user-{i}"),
            }),
        )
        .await
        .expect("insert");
    }
}

/// Execute the offset-paginated query against SQLite, decode the
/// `COUNT(*)` result, build a `Page`, then assert the navigation
/// flags + data length for each corner of a 60-row dataset.
#[tokio::test]
async fn sqlite_offset_pagination_page_info_is_correct() {
    let db = SqliteDb::open_in_memory().expect("open db");
    seed(&db, 60).await;

    let paginated = User::find().paginate(2, 20);
    let (data_sql, count_sql, params) = paginated.build();

    let data_rows = raw_query(&db, &data_sql, &params)
        .await
        .expect("data query");
    let count_rows = raw_query(&db, &count_sql, &params)
        .await
        .expect("count query");

    // COUNT(*) is returned as INTEGER → `Value::I64` in SQLite.
    let total_items = match count_rows[0].get_idx(0) {
        Some(Value::I64(n)) => *n as u64,
        other => panic!("expected COUNT to be Value::I64, got {other:?}"),
    };

    let page = Page::<User>::new(2, 20, total_items);
    assert_eq!(data_rows.len(), 20, "page 2 of 60 / 20 must hold 20 rows");
    assert_eq!(page.total_items, 60);
    assert_eq!(page.total_pages, 3);
    assert!(page.has_next, "page 2 of 3 must have a next page");
    assert!(page.has_prev, "page 2 of 3 must have a previous page");
}

/// Last-page navigation: `has_next` must be `false`.
#[tokio::test]
async fn sqlite_offset_pagination_last_page() {
    let db = SqliteDb::open_in_memory().expect("open db");
    seed(&db, 60).await;

    let paginated = User::find().paginate(3, 20);
    let (data_sql, count_sql, params) = paginated.build();
    let data_rows = raw_query(&db, &data_sql, &params)
        .await
        .expect("data");
    let count_rows = raw_query(&db, &count_sql, &params)
        .await
        .expect("count");
    let total_items = match count_rows[0].get_idx(0) {
        Some(Value::I64(n)) => *n as u64,
        other => panic!("count: {other:?}"),
    };

    let page = Page::<User>::new(3, 20, total_items);
    assert_eq!(data_rows.len(), 20);
    assert!(!page.has_next, "last page must not have a next page");
    assert!(page.has_prev);
}

/// Cursor pagination (forward): `.after(col, value, limit)` must
/// return exactly `limit` rows (or fewer at the tail) and the
/// builder's `has_more` helper must reflect whether a next page
/// exists.
#[tokio::test]
async fn sqlite_cursor_pagination_forward() {
    let db = SqliteDb::open_in_memory().expect("open db");
    seed(&db, 10).await;

    let page = User::find().after(User::id, 3i64, 4);
    let (sql, params) = page.build();
    let rows = raw_query(&db, &sql, &params).await.expect("cursor query");

    // `.build()` fetches `limit + 1` rows to detect more pages —
    // expect 5 returned (IDs 4..=8) → `has_more(5) == true`.
    assert_eq!(rows.len(), 5);
    assert!(page.has_more(rows.len() as u64), "should report has_more");

    // First returned row's id must be > cursor value (3).
    let first_id = match rows[0].get_idx(0) {
        Some(Value::I64(n)) => *n,
        other => panic!("expected I64 id, got {other:?}"),
    };
    assert!(first_id > 3, "cursor pagination must skip ids <= 3");
}

/// Cursor pagination at the tail: fewer rows than `limit + 1` → `has_more` is false.
#[tokio::test]
async fn sqlite_cursor_pagination_tail() {
    let db = SqliteDb::open_in_memory().expect("open db");
    seed(&db, 10).await;

    let page = User::find().after(User::id, 8i64, 5);
    let (sql, params) = page.build();
    let rows = raw_query(&db, &sql, &params).await.expect("cursor query");

    // Only ids 9 and 10 remain, so `has_more(2)` must be false.
    assert_eq!(rows.len(), 2);
    assert!(!page.has_more(rows.len() as u64));
}
