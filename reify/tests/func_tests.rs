use reify::{func, Expr, Table, Value};

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub name: String,
    pub score: f64,
    #[column(nullable)]
    pub bio: Option<String>,
}

// ── Aggregate functions ────────────────────────────────────────────

#[test]
fn func_count_column() {
    let (sql, params) = User::find()
        .select_expr(&[func::count(User::id)])
        .build();
    assert_eq!(sql, "SELECT COUNT(id) FROM users");
    assert!(params.is_empty());
}

#[test]
fn func_count_all() {
    let (sql, params) = User::find()
        .select_expr(&[func::count_all()])
        .build();
    assert_eq!(sql, "SELECT COUNT(*) FROM users");
    assert!(params.is_empty());
}

#[test]
fn func_count_distinct() {
    let (sql, params) = User::find()
        .select_expr(&[func::count_distinct(User::name)])
        .build();
    assert_eq!(sql, "SELECT COUNT(DISTINCT name) FROM users");
    assert!(params.is_empty());
}

#[test]
fn func_sum() {
    let (sql, params) = User::find()
        .select_expr(&[func::sum(User::score)])
        .build();
    assert_eq!(sql, "SELECT SUM(score) FROM users");
    assert!(params.is_empty());
}

#[test]
fn func_avg() {
    let (sql, params) = User::find()
        .select_expr(&[func::avg(User::score)])
        .build();
    assert_eq!(sql, "SELECT AVG(score) FROM users");
    assert!(params.is_empty());
}

#[test]
fn func_min() {
    let (sql, params) = User::find()
        .select_expr(&[func::min(User::score)])
        .build();
    assert_eq!(sql, "SELECT MIN(score) FROM users");
    assert!(params.is_empty());
}

#[test]
fn func_max() {
    let (sql, params) = User::find()
        .select_expr(&[func::max(User::id)])
        .build();
    assert_eq!(sql, "SELECT MAX(id) FROM users");
    assert!(params.is_empty());
}

// ── String functions ───────────────────────────────────────────────

#[test]
fn func_upper() {
    let (sql, params) = User::find()
        .select_expr(&[func::upper(User::name)])
        .build();
    assert_eq!(sql, "SELECT UPPER(name) FROM users");
    assert!(params.is_empty());
}

#[test]
fn func_lower() {
    let (sql, params) = User::find()
        .select_expr(&[func::lower(User::name)])
        .build();
    assert_eq!(sql, "SELECT LOWER(name) FROM users");
    assert!(params.is_empty());
}

#[test]
fn func_length() {
    let (sql, params) = User::find()
        .select_expr(&[func::length(User::name)])
        .build();
    assert_eq!(sql, "SELECT LENGTH(name) FROM users");
    assert!(params.is_empty());
}

// ── Numeric functions ──────────────────────────────────────────────

#[test]
fn func_abs() {
    let (sql, params) = User::find()
        .select_expr(&[func::abs(User::score)])
        .build();
    assert_eq!(sql, "SELECT ABS(score) FROM users");
    assert!(params.is_empty());
}

#[test]
fn func_round() {
    let (sql, params) = User::find()
        .select_expr(&[func::round(User::score)])
        .build();
    assert_eq!(sql, "SELECT ROUND(score) FROM users");
    assert!(params.is_empty());
}

#[test]
fn func_round_to_precision() {
    let (sql, params) = User::find()
        .select_expr(&[func::round_to(User::score, 2)])
        .build();
    assert_eq!(sql, "SELECT ROUND(score, 2) FROM users");
    assert!(params.is_empty());
}

// ── General functions ──────────────────────────────────────────────

#[test]
fn func_coalesce_nullable() {
    let (sql, params) = User::find()
        .select_expr(&[func::coalesce(User::bio, "N/A")])
        .build();
    assert_eq!(sql, "SELECT COALESCE(bio, 'N/A') FROM users");
    assert!(params.is_empty());
}

#[test]
fn func_coalesce_numeric_default() {
    let (sql, params) = User::find()
        .select_expr(&[func::coalesce_val(User::score, 0.0f64)])
        .build();
    assert_eq!(sql, "SELECT COALESCE(score, 0) FROM users");
    assert!(params.is_empty());
}

// ── Composition: multiple functions in one select ──────────────────

#[test]
fn func_multiple_in_select() {
    let (sql, params) = User::find()
        .select_expr(&[
            func::count(User::id),
            func::max(User::score),
            func::lower(User::name),
        ])
        .build();
    assert_eq!(
        sql,
        "SELECT COUNT(id), MAX(score), LOWER(name) FROM users"
    );
    assert!(params.is_empty());
}

// ── Composition with GROUP BY and HAVING ───────────────────────────

#[test]
fn func_with_group_by_and_having() {
    let (sql, params) = User::find()
        .select_expr(&[Expr::Col("name"), func::count(User::id)])
        .group_by(&["name"])
        .having(func::count(User::id).gt(5i64))
        .build();
    assert_eq!(
        sql,
        "SELECT name, COUNT(id) FROM users GROUP BY name HAVING COUNT(id) > ?"
    );
    assert_eq!(params, vec![Value::I64(5)]);
}

#[test]
fn func_count_distinct_with_having() {
    let (sql, params) = User::find()
        .select_expr(&[func::count_distinct(User::name)])
        .having(func::count_distinct(User::name).gte(3i64))
        .build();
    assert_eq!(
        sql,
        "SELECT COUNT(DISTINCT name) FROM users HAVING COUNT(DISTINCT name) >= ?"
    );
    assert_eq!(params, vec![Value::I64(3)]);
}
