use reify::{Cursor, Row, Table, Value};

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
    pub score: f64,
}

#[derive(Table, Debug, Clone)]
#[table(name = "posts")]
pub struct Post {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub user_id: i64,
    pub title: String,
}

// ═══════════════════════════════════════════════════════════════════
// ── Cursor encoding / decoding ─────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

#[test]
fn cursor_encode_decode_i64() {
    let cursor = Cursor::encode(&[("id", &Value::I64(42))]);
    let decoded = Cursor::decode(cursor.as_str()).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].0, "id");
    assert_eq!(decoded[0].1, Value::I64(42));
}

#[test]
fn cursor_encode_decode_string() {
    let cursor = Cursor::encode(&[("email", &Value::String("alice@test.com".into()))]);
    let decoded = Cursor::decode(cursor.as_str()).unwrap();
    assert_eq!(decoded[0].0, "email");
    assert_eq!(decoded[0].1, Value::String("alice@test.com".into()));
}

#[test]
fn cursor_encode_decode_multi_column() {
    let cursor = Cursor::encode(&[("score", &Value::F64(99.5)), ("id", &Value::I64(7))]);
    let decoded = Cursor::decode(cursor.as_str()).unwrap();
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0], ("score".to_string(), Value::F64(99.5)));
    assert_eq!(decoded[1], ("id".to_string(), Value::I64(7)));
}

#[test]
fn cursor_encode_decode_all_int_types() {
    let cursor = Cursor::encode(&[
        ("a", &Value::I16(1)),
        ("b", &Value::I32(2)),
        ("c", &Value::I64(3)),
    ]);
    let decoded = Cursor::decode(cursor.as_str()).unwrap();
    assert_eq!(decoded[0].1, Value::I16(1));
    assert_eq!(decoded[1].1, Value::I32(2));
    assert_eq!(decoded[2].1, Value::I64(3));
}

#[test]
fn cursor_encode_decode_float_types() {
    let cursor = Cursor::encode(&[("a", &Value::F32(1.5)), ("b", &Value::F64(2.5))]);
    let decoded = Cursor::decode(cursor.as_str()).unwrap();
    assert_eq!(decoded[0].1, Value::F32(1.5));
    assert_eq!(decoded[1].1, Value::F64(2.5));
}

#[test]
fn cursor_encode_decode_bool() {
    let cursor = Cursor::encode(&[("active", &Value::Bool(true))]);
    let decoded = Cursor::decode(cursor.as_str()).unwrap();
    assert_eq!(decoded[0].1, Value::Bool(true));
}

#[test]
fn cursor_decode_invalid_returns_none() {
    assert!(Cursor::decode("!!!invalid!!!").is_none());
    assert!(Cursor::decode("").is_none());
}

#[test]
fn cursor_is_opaque() {
    let cursor = Cursor::encode(&[("id", &Value::I64(42))]);
    // Should not contain raw column names or values in plain text
    assert!(!cursor.as_str().contains("id"));
    assert!(!cursor.as_str().contains("42"));
}

#[test]
fn cursor_roundtrip_display() {
    let cursor = Cursor::encode(&[("id", &Value::I64(100))]);
    let s = cursor.to_string();
    let decoded = Cursor::decode(&s).unwrap();
    assert_eq!(decoded[0].1, Value::I64(100));
}

// ═══════════════════════════════════════════════════════════════════
// ── CursorBuilder — single column, forward ─────────────────────────
// ═══════════════════════════════════════════════════════════════════

#[test]
fn cursor_builder_first_page_asc() {
    let builder = User::find().cursor(User::id).first(10);
    let (sql, params) = builder.build();
    assert_eq!(sql, "SELECT * FROM users ORDER BY id ASC LIMIT 11");
    assert!(params.is_empty());
}

#[test]
fn cursor_builder_first_page_desc() {
    let builder = User::find().cursor_desc(User::id).first(10);
    let (sql, params) = builder.build();
    assert_eq!(sql, "SELECT * FROM users ORDER BY id DESC LIMIT 11");
    assert!(params.is_empty());
}

#[test]
fn cursor_builder_after_cursor_asc() {
    // Encode a cursor for id=42
    let cursor = Cursor::encode(&[("id", &Value::I64(42))]);
    let builder = User::find()
        .cursor(User::id)
        .first(25)
        .after_cursor(cursor.as_str());
    let (sql, params) = builder.build();
    assert_eq!(
        sql,
        "SELECT * FROM users WHERE id > ? ORDER BY id ASC LIMIT 26"
    );
    assert_eq!(params, vec![Value::I64(42)]);
}

#[test]
fn cursor_builder_after_cursor_desc() {
    let cursor = Cursor::encode(&[("id", &Value::I64(100))]);
    let builder = User::find()
        .cursor_desc(User::id)
        .first(20)
        .after_cursor(cursor.as_str());
    let (sql, params) = builder.build();
    // DESC + forward → LT
    assert_eq!(
        sql,
        "SELECT * FROM users WHERE id < ? ORDER BY id DESC LIMIT 21"
    );
    assert_eq!(params, vec![Value::I64(100)]);
}

// ═══════════════════════════════════════════════════════════════════
// ── CursorBuilder — single column, backward ────────────────────────
// ═══════════════════════════════════════════════════════════════════

#[test]
fn cursor_builder_before_cursor_asc() {
    let cursor = Cursor::encode(&[("id", &Value::I64(50))]);
    let builder = User::find()
        .cursor(User::id)
        .last(10)
        .before_cursor(cursor.as_str());
    let (sql, params) = builder.build();
    // ASC + backward → LT, order flips to DESC
    assert_eq!(
        sql,
        "SELECT * FROM users WHERE id < ? ORDER BY id DESC LIMIT 11"
    );
    assert_eq!(params, vec![Value::I64(50)]);
}

#[test]
fn cursor_builder_last_without_cursor() {
    let builder = User::find().cursor(User::id).last(5);
    let (sql, params) = builder.build();
    // backward with no cursor → just reversed order
    assert_eq!(sql, "SELECT * FROM users ORDER BY id DESC LIMIT 6");
    assert!(params.is_empty());
}

// ═══════════════════════════════════════════════════════════════════
// ── CursorBuilder — with filters ───────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

#[test]
fn cursor_builder_with_filter() {
    let cursor = Cursor::encode(&[("id", &Value::I64(10))]);
    let builder = User::find()
        .filter(User::email.starts_with("admin"))
        .cursor(User::id)
        .first(25)
        .after_cursor(cursor.as_str());
    let (sql, params) = builder.build();
    assert_eq!(
        sql,
        "SELECT * FROM users WHERE email LIKE ? AND id > ? ORDER BY id ASC LIMIT 26"
    );
    assert_eq!(params, vec![Value::String("admin%".into()), Value::I64(10)]);
}

// ═══════════════════════════════════════════════════════════════════
// ── CursorBuilder — multi-column cursor ────────────────────────────
// ═══════════════════════════════════════════════════════════════════

#[test]
fn cursor_builder_multi_column_first_page() {
    let builder = Post::find()
        .cursor_by(vec![Post::user_id.desc_cursor(), Post::id.desc_cursor()])
        .first(20);
    let (sql, params) = builder.build();
    assert_eq!(
        sql,
        "SELECT * FROM posts ORDER BY user_id DESC, id DESC LIMIT 21"
    );
    assert!(params.is_empty());
}

#[test]
fn cursor_builder_multi_column_after_cursor() {
    let cursor = Cursor::encode(&[("user_id", &Value::I64(5)), ("id", &Value::I64(100))]);
    let builder = Post::find()
        .cursor_by(vec![Post::user_id.desc_cursor(), Post::id.desc_cursor()])
        .first(20)
        .after_cursor(cursor.as_str());
    let (sql, params) = builder.build();
    // DESC + forward → LT for row-value comparison
    assert_eq!(
        sql,
        "SELECT * FROM posts WHERE (user_id, id) < (?, ?) ORDER BY user_id DESC, id DESC LIMIT 21"
    );
    assert_eq!(params, vec![Value::I64(5), Value::I64(100)]);
}

#[test]
fn cursor_builder_multi_column_before_cursor() {
    let cursor = Cursor::encode(&[("user_id", &Value::I64(5)), ("id", &Value::I64(100))]);
    let builder = Post::find()
        .cursor_by(vec![Post::user_id.desc_cursor(), Post::id.desc_cursor()])
        .last(20)
        .before_cursor(cursor.as_str());
    let (sql, params) = builder.build();
    // DESC + backward → GT, order flips to ASC
    assert_eq!(
        sql,
        "SELECT * FROM posts WHERE (user_id, id) > (?, ?) ORDER BY user_id ASC, id ASC LIMIT 21"
    );
    assert_eq!(params, vec![Value::I64(5), Value::I64(100)]);
}

// ═══════════════════════════════════════════════════════════════════
// ── CursorBuilder — with_total_count ───────────────────────────────
// ═══════════════════════════════════════════════════════════════════

#[test]
fn cursor_builder_with_count() {
    let cursor = Cursor::encode(&[("id", &Value::I64(42))]);
    let builder = User::find()
        .filter(User::score.gt(50.0f64))
        .cursor(User::id)
        .first(10)
        .after_cursor(cursor.as_str())
        .with_total_count();
    let (data_sql, count_sql, data_params, count_params) = builder.build_with_count();
    assert_eq!(
        data_sql,
        "SELECT * FROM users WHERE score > ? AND id > ? ORDER BY id ASC LIMIT 11"
    );
    assert_eq!(count_sql, "SELECT COUNT(*) FROM users WHERE score > ?");
    assert_eq!(data_params, vec![Value::F64(50.0), Value::I64(42)]);
    assert_eq!(count_params, vec![Value::F64(50.0)]);
}

// ═══════════════════════════════════════════════════════════════════
// ── CursorBuilder — into_page (result processing) ──────────────────
// ═══════════════════════════════════════════════════════════════════

fn make_row(id: i64, email: &str) -> Row {
    Row::new(
        vec!["id".into(), "email".into()],
        vec![Value::I64(id), Value::String(email.into())],
    )
}

#[test]
fn into_page_first_page_has_next() {
    let builder = User::find().cursor(User::id).first(2);

    // 3 rows returned (limit+1) → has_next_page = true
    let rows = vec![
        make_row(1, "a@test.com"),
        make_row(2, "b@test.com"),
        make_row(3, "c@test.com"), // extra probe row
    ];

    let page = builder.into_page(&rows, |row| vec![("id", row.get("id").unwrap().clone())]);

    assert_eq!(page.edges.len(), 2); // only 2, not 3
    assert!(page.page_info.has_next_page);
    assert!(!page.page_info.has_previous_page); // no cursor → first page
    assert!(page.page_info.start_cursor.is_some());
    assert!(page.page_info.end_cursor.is_some());

    // Verify cursors decode correctly
    let start = Cursor::decode(page.page_info.start_cursor.as_ref().unwrap().as_str()).unwrap();
    assert_eq!(start[0].1, Value::I64(1));
    let end = Cursor::decode(page.page_info.end_cursor.as_ref().unwrap().as_str()).unwrap();
    assert_eq!(end[0].1, Value::I64(2));
}

#[test]
fn into_page_last_page_no_next() {
    let cursor = Cursor::encode(&[("id", &Value::I64(0))]);
    let builder = User::find()
        .cursor(User::id)
        .first(5)
        .after_cursor(cursor.as_str());

    // Only 2 rows returned (< limit+1) → no more pages
    let rows = vec![make_row(1, "a@test.com"), make_row(2, "b@test.com")];

    let page = builder.into_page(&rows, |row| vec![("id", row.get("id").unwrap().clone())]);

    assert_eq!(page.edges.len(), 2);
    assert!(!page.page_info.has_next_page);
    assert!(page.page_info.has_previous_page); // has cursor → not first page
}

#[test]
fn into_page_empty_result() {
    let builder = User::find().cursor(User::id).first(10);
    let rows: Vec<Row> = vec![];

    let page = builder.into_page(&rows, |row| vec![("id", row.get("id").unwrap().clone())]);

    assert!(page.edges.is_empty());
    assert!(!page.page_info.has_next_page);
    assert!(!page.page_info.has_previous_page);
    assert!(page.page_info.start_cursor.is_none());
    assert!(page.page_info.end_cursor.is_none());
}

#[test]
fn into_page_backward_reverses_order() {
    let cursor = Cursor::encode(&[("id", &Value::I64(50))]);
    let builder = User::find()
        .cursor(User::id)
        .last(3)
        .before_cursor(cursor.as_str());

    // DB returns rows in DESC order (backward query)
    let rows = vec![
        make_row(49, "x@test.com"),
        make_row(48, "y@test.com"),
        make_row(47, "z@test.com"),
    ];

    let page = builder.into_page(&rows, |row| vec![("id", row.get("id").unwrap().clone())]);

    // Edges should be reversed to natural (ASC) order
    assert_eq!(page.edges.len(), 3);
    let first = Cursor::decode(page.edges[0].cursor.as_str()).unwrap();
    let last = Cursor::decode(page.edges[2].cursor.as_str()).unwrap();
    assert_eq!(first[0].1, Value::I64(47)); // smallest first
    assert_eq!(last[0].1, Value::I64(49)); // largest last

    assert!(page.page_info.has_next_page); // cursor was provided → has_next
    assert!(!page.page_info.has_previous_page); // 3 rows ≤ limit → no more before
}

// ═══════════════════════════════════════════════════════════════════
// ── Column cursor helpers ──────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

#[test]
fn asc_cursor_helper() {
    let col = User::id.asc_cursor();
    assert_eq!(col.name, "id");
    assert!(!col.descending);
}

#[test]
fn desc_cursor_helper() {
    let col = User::id.desc_cursor();
    assert_eq!(col.name, "id");
    assert!(col.descending);
}

// ═══════════════════════════════════════════════════════════════════
// ── End-to-end: full pagination flow ───────────────────────────────
// ═══════════════════════════════════════════════════════════════════

#[test]
fn full_pagination_flow() {
    // Page 1: no cursor
    let builder = User::find().cursor(User::id).first(2);
    let (sql, _) = builder.build();
    assert_eq!(sql, "SELECT * FROM users ORDER BY id ASC LIMIT 3");

    // Simulate: got 3 rows → has_next
    let rows = vec![
        make_row(1, "a@test.com"),
        make_row(2, "b@test.com"),
        make_row(3, "c@test.com"),
    ];
    let page1 = builder.into_page(&rows, |r| vec![("id", r.get("id").unwrap().clone())]);
    assert!(page1.page_info.has_next_page);
    let end_cursor = page1.page_info.end_cursor.unwrap();

    // Page 2: use end_cursor from page 1
    let builder2 = User::find()
        .cursor(User::id)
        .first(2)
        .after_cursor(end_cursor.as_str());
    let (sql2, params2) = builder2.build();
    assert_eq!(
        sql2,
        "SELECT * FROM users WHERE id > ? ORDER BY id ASC LIMIT 3"
    );
    assert_eq!(params2, vec![Value::I64(2)]);

    // Simulate: got only 2 rows → no more
    let rows2 = vec![make_row(3, "c@test.com"), make_row(4, "d@test.com")];
    let page2 = builder2.into_page(&rows2, |r| vec![("id", r.get("id").unwrap().clone())]);
    assert!(!page2.page_info.has_next_page);
    assert!(page2.page_info.has_previous_page);
}
