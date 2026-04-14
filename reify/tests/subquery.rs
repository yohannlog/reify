use reify::{Table, Value};

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    pub name: String,
    pub role: String,
}

#[derive(Table, Debug, Clone)]
#[table(name = "orders")]
pub struct Order {
    #[column(primary_key)]
    pub id: i64,
    pub user_id: i64,
    pub amount: f64,
}

#[test]
fn in_subquery_basic() {
    let sub = Order::find().select(&["user_id"]);
    let (sql, params) = User::find().filter(User::id.in_subquery(sub)).build();
    assert_eq!(
        sql,
        "SELECT * FROM \"users\" WHERE \"id\" IN (SELECT \"user_id\" FROM \"orders\")"
    );
    assert!(params.is_empty());
}

#[test]
fn in_subquery_with_filter() {
    let sub = Order::find()
        .select(&["user_id"])
        .filter(Order::amount.gt(100.0f64));
    let (sql, params) = User::find().filter(User::id.in_subquery(sub)).build();
    assert_eq!(
        sql,
        "SELECT * FROM \"users\" WHERE \"id\" IN (SELECT \"user_id\" FROM \"orders\" WHERE \"amount\" > ?)"
    );
    assert_eq!(params, vec![Value::F64(100.0)]);
}

#[test]
fn in_subquery_combined_with_other_filter() {
    let sub = Order::find().select(&["user_id"]);
    let (sql, params) = User::find()
        .filter(User::role.eq("admin".to_string()))
        .filter(User::id.in_subquery(sub))
        .build();
    assert_eq!(
        sql,
        "SELECT * FROM \"users\" WHERE \"role\" = ? AND \"id\" IN (SELECT \"user_id\" FROM \"orders\")"
    );
    assert_eq!(params, vec![Value::String("admin".into())]);
}

#[test]
fn in_subquery_with_subquery_params_and_outer_params() {
    let sub = Order::find()
        .select(&["user_id"])
        .filter(Order::amount.gt(50.0f64));
    let (sql, params) = User::find()
        .filter(User::role.eq("member".to_string()))
        .filter(User::id.in_subquery(sub))
        .build();
    assert_eq!(
        sql,
        "SELECT * FROM \"users\" WHERE \"role\" = ? AND \"id\" IN (SELECT \"user_id\" FROM \"orders\" WHERE \"amount\" > ?)"
    );
    assert_eq!(
        params,
        vec![Value::String("member".into()), Value::F64(50.0)]
    );
}
