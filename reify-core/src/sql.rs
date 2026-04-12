use crate::condition::{Condition, LogicalOp};
use crate::value::Value;

/// Trait to render a type into a SQL fragment + bound parameters.
pub trait ToSql {
    fn to_sql(&self, params: &mut Vec<Value>) -> String;
}

impl ToSql for Condition {
    fn to_sql(&self, params: &mut Vec<Value>) -> String {
        match self {
            Condition::Eq(col, val) => {
                params.push(val.clone());
                format!("{col} = ?")
            }
            Condition::Neq(col, val) => {
                params.push(val.clone());
                format!("{col} != ?")
            }
            Condition::Gt(col, val) => {
                params.push(val.clone());
                format!("{col} > ?")
            }
            Condition::Lt(col, val) => {
                params.push(val.clone());
                format!("{col} < ?")
            }
            Condition::Gte(col, val) => {
                params.push(val.clone());
                format!("{col} >= ?")
            }
            Condition::Lte(col, val) => {
                params.push(val.clone());
                format!("{col} <= ?")
            }
            Condition::Between(col, a, b) => {
                params.push(a.clone());
                params.push(b.clone());
                format!("{col} BETWEEN ? AND ?")
            }
            Condition::Like(col, pattern) => {
                params.push(Value::String(pattern.clone()));
                format!("{col} LIKE ?")
            }
            #[cfg(feature = "postgres")]
            Condition::ILike(col, pattern) => {
                params.push(Value::String(pattern.clone()));
                format!("{col} ILIKE ?")
            }
            Condition::In(col, vals) => {
                let placeholders: Vec<&str> = vals
                    .iter()
                    .map(|v| {
                        params.push(v.clone());
                        "?"
                    })
                    .collect();
                format!("{col} IN ({})", placeholders.join(", "))
            }
            Condition::IsNull(col) => format!("{col} IS NULL"),
            Condition::IsNotNull(col) => format!("{col} IS NOT NULL"),
            #[cfg(feature = "postgres")]
            Condition::RangeContains(col, val) => {
                params.push(val.clone());
                format!("{col} @> ?")
            }
            #[cfg(feature = "postgres")]
            Condition::RangeContainedBy(col, val) => {
                params.push(val.clone());
                format!("{col} <@ ?")
            }
            #[cfg(feature = "postgres")]
            Condition::RangeOverlaps(col, val) => {
                params.push(val.clone());
                format!("{col} && ?")
            }
            #[cfg(feature = "postgres")]
            Condition::RangeLeftOf(col, val) => {
                params.push(val.clone());
                format!("{col} << ?")
            }
            #[cfg(feature = "postgres")]
            Condition::RangeRightOf(col, val) => {
                params.push(val.clone());
                format!("{col} >> ?")
            }
            #[cfg(feature = "postgres")]
            Condition::RangeAdjacent(col, val) => {
                params.push(val.clone());
                format!("{col} -|- ?")
            }
            #[cfg(feature = "postgres")]
            Condition::RangeIsEmpty(col) => format!("isempty({col})"),
            #[cfg(feature = "postgres")]
            Condition::JsonGet(col, key) => {
                params.push(Value::String(key.clone()));
                format!("{col}->?")
            }
            #[cfg(feature = "postgres")]
            Condition::JsonContains(col, val) => {
                params.push(val.clone());
                format!("{col} @> ?")
            }
            #[cfg(feature = "postgres")]
            Condition::JsonHasKey(col, key) => {
                params.push(Value::String(key.clone()));
                format!("jsonb_exists({col}, ?)")
            }
            #[cfg(feature = "postgres")]
            Condition::ArrayContains(col, val) => {
                params.push(val.clone());
                format!("{col} @> ?")
            }
            #[cfg(feature = "postgres")]
            Condition::ArrayContainedBy(col, val) => {
                params.push(val.clone());
                format!("{col} <@ ?")
            }
            #[cfg(feature = "postgres")]
            Condition::ArrayOverlaps(col, val) => {
                params.push(val.clone());
                format!("{col} && ?")
            }
            Condition::AggregateGt(expr, val) => {
                params.push(val.clone());
                format!("{} > ?", expr.to_sql_fragment())
            }
            Condition::AggregateLt(expr, val) => {
                params.push(val.clone());
                format!("{} < ?", expr.to_sql_fragment())
            }
            Condition::AggregateGte(expr, val) => {
                params.push(val.clone());
                format!("{} >= ?", expr.to_sql_fragment())
            }
            Condition::AggregateLte(expr, val) => {
                params.push(val.clone());
                format!("{} <= ?", expr.to_sql_fragment())
            }
            Condition::AggregateEq(expr, val) => {
                params.push(val.clone());
                format!("{} = ?", expr.to_sql_fragment())
            }
            Condition::InSubquery(col, sub_sql, sub_params) => {
                params.extend(sub_params.iter().cloned());
                format!("{col} IN ({sub_sql})")
            }
            Condition::Logical(op) => match op {
                LogicalOp::And(conds) => {
                    let parts: Vec<String> = conds.iter().map(|c| c.to_sql(params)).collect();
                    format!("({})", parts.join(" AND "))
                }
                LogicalOp::Or(conds) => {
                    let parts: Vec<String> = conds.iter().map(|c| c.to_sql(params)).collect();
                    format!("({})", parts.join(" OR "))
                }
            },
        }
    }
}
