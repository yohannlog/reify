use reify::{Expr, Table, Value, count_all};

#[derive(Table, Debug, Clone)]
#[table(name = "orders")]
pub struct Order {
    #[column(primary_key)]
    pub id: i64,
    pub customer_id: i64,
    pub amount: f64,
    pub status: String,
}

#[test]
fn group_by_single_column() {
    let (sql, params) = Order::find()
        .select_expr(&[Expr::Col("status"), count_all()])
        .group_by(&["status"])
        .build();
    assert_eq!(
        sql,
        "SELECT \"status\", COUNT(*) FROM \"orders\" GROUP BY \"status\""
    );
    assert!(params.is_empty());
}

#[test]
fn group_by_multiple_columns() {
    let (sql, params) = Order::find()
        .select_expr(&[Expr::Col("customer_id"), Expr::Col("status"), count_all()])
        .group_by(&["customer_id", "status"])
        .build();
    assert_eq!(
        sql,
        "SELECT \"customer_id\", \"status\", COUNT(*) FROM \"orders\" GROUP BY \"customer_id\", \"status\""
    );
    assert!(params.is_empty());
}

#[test]
fn group_by_with_having_gt() {
    let (sql, params) = Order::find()
        .select_expr(&[Expr::Col("status"), count_all()])
        .group_by(&["status"])
        .having(count_all().gt(5i64))
        .build();
    assert_eq!(
        sql,
        "SELECT \"status\", COUNT(*) FROM \"orders\" GROUP BY \"status\" HAVING COUNT(*) > ?"
    );
    assert_eq!(params, vec![Value::I64(5)]);
}

#[test]
fn group_by_with_having_gte() {
    let (sql, params) = Order::find()
        .select_expr(&[Expr::Col("status"), count_all()])
        .group_by(&["status"])
        .having(count_all().gte(10i64))
        .build();
    assert_eq!(
        sql,
        "SELECT \"status\", COUNT(*) FROM \"orders\" GROUP BY \"status\" HAVING COUNT(*) >= ?"
    );
    assert_eq!(params, vec![Value::I64(10)]);
}

#[test]
fn group_by_with_having_lt() {
    let (sql, params) = Order::find()
        .select_expr(&[Expr::Col("customer_id"), count_all()])
        .group_by(&["customer_id"])
        .having(count_all().lt(3i64))
        .build();
    assert_eq!(
        sql,
        "SELECT \"customer_id\", COUNT(*) FROM \"orders\" GROUP BY \"customer_id\" HAVING COUNT(*) < ?"
    );
    assert_eq!(params, vec![Value::I64(3)]);
}

#[test]
fn group_by_with_having_eq() {
    let (sql, params) = Order::find()
        .select_expr(&[Expr::Col("status"), count_all()])
        .group_by(&["status"])
        .having(count_all().eq(1i64))
        .build();
    assert_eq!(
        sql,
        "SELECT \"status\", COUNT(*) FROM \"orders\" GROUP BY \"status\" HAVING COUNT(*) = ?"
    );
    assert_eq!(params, vec![Value::I64(1)]);
}

#[test]
fn sum_aggregate() {
    let (sql, params) = Order::find()
        .select_expr(&[Expr::Col("customer_id"), Order::amount.sum()])
        .group_by(&["customer_id"])
        .build();
    assert_eq!(
        sql,
        "SELECT \"customer_id\", SUM(\"amount\") FROM \"orders\" GROUP BY \"customer_id\""
    );
    assert!(params.is_empty());
}

#[test]
fn avg_aggregate() {
    let (sql, params) = Order::find()
        .select_expr(&[Expr::Col("status"), Order::amount.avg()])
        .group_by(&["status"])
        .build();
    assert_eq!(
        sql,
        "SELECT \"status\", AVG(\"amount\") FROM \"orders\" GROUP BY \"status\""
    );
    assert!(params.is_empty());
}

#[test]
fn min_max_aggregate() {
    let (sql, params) = Order::find()
        .select_expr(&[
            Expr::Col("status"),
            Order::amount.min_expr(),
            Order::amount.max_expr(),
        ])
        .group_by(&["status"])
        .build();
    assert_eq!(
        sql,
        "SELECT \"status\", MIN(\"amount\"), MAX(\"amount\") FROM \"orders\" GROUP BY \"status\""
    );
    assert!(params.is_empty());
}

#[test]
fn count_column() {
    let (sql, params) = Order::find()
        .select_expr(&[Expr::Col("status"), Order::id.count()])
        .group_by(&["status"])
        .build();
    assert_eq!(
        sql,
        "SELECT \"status\", COUNT(\"id\") FROM \"orders\" GROUP BY \"status\""
    );
    assert!(params.is_empty());
}

#[test]
fn group_by_with_where_and_having() {
    let (sql, params) = Order::find()
        .select_expr(&[Expr::Col("customer_id"), Order::amount.sum()])
        .filter(Order::status.eq("completed".to_string()))
        .group_by(&["customer_id"])
        .having(Order::amount.sum().gt(100.0f64))
        .build();
    assert_eq!(
        sql,
        "SELECT \"customer_id\", SUM(\"amount\") FROM \"orders\" WHERE \"status\" = ? GROUP BY \"customer_id\" HAVING SUM(\"amount\") > ?"
    );
    assert_eq!(
        params,
        vec![Value::String("completed".into()), Value::F64(100.0)]
    );
}

#[test]
fn group_by_with_order_and_limit() {
    let (sql, params) = Order::find()
        .select_expr(&[Expr::Col("customer_id"), count_all()])
        .group_by(&["customer_id"])
        .having(count_all().gte(2i64))
        .order_by(reify::query::Order::Desc("customer_id"))
        .limit(10)
        .build();
    assert_eq!(
        sql,
        "SELECT \"customer_id\", COUNT(*) FROM \"orders\" GROUP BY \"customer_id\" HAVING COUNT(*) >= ? ORDER BY \"customer_id\" DESC LIMIT 10"
    );
    assert_eq!(params, vec![Value::I64(2)]);
}

#[test]
fn multiple_having_conditions() {
    let (sql, params) = Order::find()
        .select_expr(&[Expr::Col("status"), count_all(), Order::amount.sum()])
        .group_by(&["status"])
        .having(count_all().gt(1i64))
        .having(Order::amount.sum().lt(1000.0f64))
        .build();
    assert_eq!(
        sql,
        "SELECT \"status\", COUNT(*), SUM(\"amount\") FROM \"orders\" GROUP BY \"status\" HAVING COUNT(*) > ? AND SUM(\"amount\") < ?"
    );
    assert_eq!(params, vec![Value::I64(1), Value::F64(1000.0)]);
}
