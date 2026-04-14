use reify::{Table, Value};

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    pub name: String,
    pub role: String,
}

#[test]
fn distinct_star() {
    let (sql, params) = User::find().distinct().build();
    assert_eq!(sql, "SELECT DISTINCT * FROM \"users\"");
    assert!(params.is_empty());
}

#[test]
fn distinct_with_column_filter() {
    let (sql, params) = User::find().distinct().select(&["name"]).build();
    assert_eq!(sql, "SELECT DISTINCT \"name\" FROM \"users\"");
    assert!(params.is_empty());
}

#[test]
fn distinct_with_filter() {
    let (sql, params) = User::find()
        .distinct()
        .filter(User::role.eq("admin".to_string()))
        .build();
    assert_eq!(sql, "SELECT DISTINCT * FROM \"users\" WHERE \"role\" = ?");
    assert_eq!(params, vec![Value::String("admin".into())]);
}

#[test]
fn distinct_with_order_and_limit() {
    let (sql, params) = User::find()
        .distinct()
        .order_by(reify::query::Order::Asc("name"))
        .limit(10)
        .build();
    assert_eq!(
        sql,
        "SELECT DISTINCT * FROM \"users\" ORDER BY \"name\" ASC LIMIT 10"
    );
    assert!(params.is_empty());
}

#[test]
fn no_distinct_regression() {
    let (sql, params) = User::find().build();
    assert_eq!(sql, "SELECT * FROM \"users\"");
    assert!(params.is_empty());
}

#[test]
fn distinct_idempotent() {
    let (sql, params) = User::find().distinct().distinct().build();
    assert_eq!(sql, "SELECT DISTINCT * FROM \"users\"");
    assert!(params.is_empty());
}

#[test]
fn distinct_count_query_has_no_distinct() {
    let ast = User::find().distinct().build_ast();
    let count_ast = ast.to_count_query();
    let mut params = Vec::new();
    let sql = count_ast.render(&mut params);
    assert_eq!(sql, "SELECT COUNT(*) FROM \"users\"");
    assert!(!sql.contains("DISTINCT"));
}
