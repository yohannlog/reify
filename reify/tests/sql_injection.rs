//! Tests for SQL injection prevention via identifier quoting and LIKE escaping.

use reify::{Table, Value};

#[derive(Table, Debug, Clone)]
#[table(name = "order")]
pub struct Order {
    #[column(primary_key)]
    pub id: i64,
    pub group: Option<String>,
    pub user: String,
    pub select: String,
}

// ── 1.1: Identifier quoting — reserved words ────────────────────────

#[test]
fn reserved_word_table_name_is_quoted() {
    let (sql, _) = Order::find().build();
    assert!(
        sql.contains("\"order\""),
        "table name 'order' must be quoted: {sql}"
    );
}

#[test]
fn reserved_word_column_names_are_quoted() {
    let (sql, _) = Order::find().filter(Order::group.is_null()).build();
    assert!(
        sql.contains("\"group\" IS NULL"),
        "column 'group' must be quoted: {sql}"
    );
}

#[test]
fn reserved_word_in_insert() {
    let o = Order {
        id: 1,
        group: Some("admin".into()),
        user: "alice".into(),
        select: "all".into(),
    };
    let (sql, _) = Order::insert(&o).build();
    assert!(sql.contains("\"order\""), "table: {sql}");
    assert!(sql.contains("\"group\""), "column group: {sql}");
    assert!(sql.contains("\"user\""), "column user: {sql}");
    assert!(sql.contains("\"select\""), "column select: {sql}");
}

#[test]
fn reserved_word_in_update() {
    let (sql, _) = Order::update()
        .set(Order::user, "bob")
        .filter(Order::id.eq(1i64))
        .build();
    assert!(sql.contains("UPDATE \"order\""), "table: {sql}");
    assert!(sql.contains("\"user\" = ?"), "SET clause: {sql}");
    assert!(sql.contains("\"id\" = ?"), "WHERE clause: {sql}");
}

#[test]
fn reserved_word_in_delete() {
    let (sql, _) = Order::delete().filter(Order::id.eq(1i64)).build();
    assert!(sql.contains("DELETE FROM \"order\""), "table: {sql}");
    assert!(sql.contains("\"id\" = ?"), "WHERE clause: {sql}");
}

#[test]
fn reserved_word_in_order_by() {
    let (sql, _) = Order::find()
        .order_by(reify::query::Order::Asc("select"))
        .build();
    assert!(
        sql.contains("ORDER BY \"select\" ASC"),
        "ORDER BY must quote 'select': {sql}"
    );
}

// ── 1.1: Identifier quoting — special characters ────────────────────

#[test]
fn quote_ident_escapes_double_quotes() {
    use reify::Dialect;
    use reify_core::ident::quote_ident;

    // A column name containing a double quote
    let quoted = quote_ident("col\"evil", Dialect::Generic);
    assert_eq!(quoted, "\"col\"\"evil\"");
}

#[test]
fn quote_ident_escapes_backtick_mysql() {
    use reify::Dialect;
    use reify_core::ident::quote_ident;

    let quoted = quote_ident("col`evil", Dialect::Mysql);
    assert_eq!(quoted, "`col``evil`");
}

#[test]
fn validate_ident_rejects_semicolons() {
    use reify_core::ident::validate_ident;
    assert!(validate_ident("users; DROP TABLE users").is_err());
}

#[test]
fn validate_ident_rejects_comment_markers() {
    use reify_core::ident::validate_ident;
    assert!(validate_ident("users--evil").is_err());
    assert!(validate_ident("users/*evil").is_err());
}

#[test]
fn validate_ident_accepts_normal_names() {
    use reify_core::ident::validate_ident;
    assert!(validate_ident("users").is_ok());
    assert!(validate_ident("user_roles").is_ok());
    assert!(validate_ident("order").is_ok());
    assert!(validate_ident("group").is_ok());
}

// ── 1.1: Migration context quotes identifiers ──────────────────────

#[test]
fn migration_context_add_column_quotes_identifiers() {
    use reify::MigrationContext;
    let mut ctx = MigrationContext::new();
    ctx.add_column("order", "group", "TEXT NOT NULL DEFAULT ''");
    assert!(
        ctx.statements()[0].contains("\"order\""),
        "table must be quoted: {}",
        ctx.statements()[0]
    );
    assert!(
        ctx.statements()[0].contains("\"group\""),
        "column must be quoted: {}",
        ctx.statements()[0]
    );
}

// ── 1.2: LIKE wildcard escaping ─────────────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "products")]
pub struct Product {
    #[column(primary_key)]
    pub id: i64,
    pub name: String,
    pub description: String,
}

#[test]
fn contains_escapes_percent() {
    let (sql, params) = Product::find()
        .filter(Product::name.contains("50%"))
        .build();
    assert!(
        sql.contains("ESCAPE"),
        "LIKE must have ESCAPE clause: {sql}"
    );
    assert_eq!(
        params,
        vec![Value::String("%50\\%%".into())],
        "percent in user input must be escaped"
    );
}

#[test]
fn contains_escapes_underscore() {
    let (_, params) = Product::find()
        .filter(Product::name.contains("test_value"))
        .build();
    assert_eq!(
        params,
        vec![Value::String("%test\\_value%".into())],
        "underscore in user input must be escaped"
    );
}

#[test]
fn starts_with_escapes_wildcards() {
    let (_, params) = Product::find()
        .filter(Product::name.starts_with("test_"))
        .build();
    assert_eq!(
        params,
        vec![Value::String("test\\_%".into())],
        "underscore must be escaped in starts_with"
    );
}

#[test]
fn ends_with_escapes_wildcards() {
    let (_, params) = Product::find()
        .filter(Product::name.ends_with("100%"))
        .build();
    assert_eq!(
        params,
        vec![Value::String("%100\\%".into())],
        "percent must be escaped in ends_with"
    );
}

#[test]
fn like_does_not_escape_wildcards() {
    // Raw like() should NOT escape — user controls the pattern
    let (_, params) = Product::find().filter(Product::name.like("%test%")).build();
    assert_eq!(
        params,
        vec![Value::String("%test%".into())],
        "raw like() must not escape wildcards"
    );
}

#[test]
fn contains_escapes_backslash() {
    let (_, params) = Product::find()
        .filter(Product::name.contains("C:\\path"))
        .build();
    assert_eq!(
        params,
        vec![Value::String("%C:\\\\path%".into())],
        "backslash in user input must be escaped"
    );
}

#[test]
fn like_sql_has_escape_clause() {
    let (sql, _) = Product::find().filter(Product::name.like("%test%")).build();
    assert!(
        sql.contains("LIKE ? ESCAPE '\\'"),
        "LIKE must include ESCAPE clause: {sql}"
    );
}
