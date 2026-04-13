use std::fmt::Write;

use crate::condition::{Condition, LogicalOp};
use crate::query::JoinKind;
use crate::value::Value;

/// Trait to render a type into a SQL fragment + bound parameters.
pub trait ToSql {
    /// Write the SQL fragment directly into `buf`, appending bound params.
    ///
    /// This is the primary method — implement this to avoid per-call
    /// `String` allocations.
    fn write_sql(&self, buf: &mut String, params: &mut Vec<Value>);

    /// Convenience wrapper that allocates and returns a `String`.
    fn to_sql(&self, params: &mut Vec<Value>) -> String {
        let mut buf = String::new();
        self.write_sql(&mut buf, params);
        buf
    }
}

/// Write `items` into `buf` separated by `sep`, using `write_fn` for each item.
///
/// Avoids the `collect::<Vec<String>>().join()` pattern that allocates a
/// temporary `Vec` and one `String` per element.
#[inline]
pub(crate) fn write_joined<T>(
    buf: &mut String,
    items: &[T],
    sep: &str,
    mut write_fn: impl FnMut(&mut String, &T),
) {
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            buf.push_str(sep);
        }
        write_fn(buf, item);
    }
}

// ── SQL AST ─────────────────────────────────────────────────────────

/// Column ordering direction.
#[derive(Debug, Clone)]
pub struct OrderFragment {
    pub column: String,
    pub descending: bool,
}

/// A single JOIN clause in the AST.
#[derive(Debug, Clone)]
pub struct JoinFragment {
    pub kind: JoinKind,
    pub table: String,
    pub on_condition: String,
}

/// Mini SQL AST — structured representation of a query.
///
/// Produced by `SelectBuilder::build_ast()` and consumed by pagination
/// helpers so they can manipulate query structure without parsing text.
#[derive(Debug, Clone)]
pub enum SqlFragment {
    /// A structured SELECT query.
    Select {
        columns: Vec<String>,
        from: String,
        joins: Vec<JoinFragment>,
        conditions: Vec<Condition>,
        group_by: Vec<String>,
        having: Vec<Condition>,
        order_by: Vec<OrderFragment>,
        limit: Option<u64>,
        offset: Option<u64>,
    },
    /// Raw SQL string with bound parameters (escape hatch).
    Raw(String, Vec<Value>),
}

impl SqlFragment {
    /// Render this fragment into a SQL string, appending bound params.
    ///
    /// Uses a single pre-allocated buffer — no intermediate `Vec<String>`
    /// or per-clause `String` allocations.
    pub fn render(&self, params: &mut Vec<Value>) -> String {
        match self {
            SqlFragment::Select {
                columns,
                from,
                joins,
                conditions,
                group_by,
                having,
                order_by,
                limit,
                offset,
            } => {
                // Rough capacity: "SELECT * FROM table WHERE …" is typically 64-256 bytes.
                let mut sql = String::with_capacity(128);

                sql.push_str("SELECT ");
                if columns.is_empty() {
                    sql.push('*');
                } else {
                    write_joined(&mut sql, columns, ", ", |buf, c| buf.push_str(c));
                }
                sql.push_str(" FROM ");
                sql.push_str(from);

                for join in joins {
                    let _ = write!(
                        sql,
                        " {} {} ON {}",
                        join.kind.sql_keyword(),
                        join.table,
                        join.on_condition
                    );
                }

                if !conditions.is_empty() {
                    sql.push_str(" WHERE ");
                    write_joined(&mut sql, conditions, " AND ", |buf, c| {
                        c.write_sql(buf, params)
                    });
                }

                if !group_by.is_empty() {
                    sql.push_str(" GROUP BY ");
                    write_joined(&mut sql, group_by, ", ", |buf, c| buf.push_str(c));
                }

                if !having.is_empty() {
                    sql.push_str(" HAVING ");
                    write_joined(&mut sql, having, " AND ", |buf, c| c.write_sql(buf, params));
                }

                if !order_by.is_empty() {
                    sql.push_str(" ORDER BY ");
                    write_joined(&mut sql, order_by, ", ", |buf, o| {
                        buf.push_str(&o.column);
                        buf.push_str(if o.descending { " DESC" } else { " ASC" });
                    });
                }

                if let Some(lim) = limit {
                    let _ = write!(sql, " LIMIT {lim}");
                }

                if let Some(off) = offset {
                    let _ = write!(sql, " OFFSET {off}");
                }

                sql
            }
            SqlFragment::Raw(sql, raw_params) => {
                params.extend(raw_params.iter().cloned());
                sql.clone()
            }
        }
    }

    /// Create a count query from this fragment.
    ///
    /// For `Select` variants: replaces columns with `COUNT(*)` and strips
    /// ORDER BY, LIMIT, OFFSET. For `Raw`: falls back to text manipulation.
    pub fn to_count_query(&self) -> SqlFragment {
        match self {
            SqlFragment::Select {
                from,
                joins,
                conditions,
                group_by,
                having,
                ..
            } => SqlFragment::Select {
                columns: vec!["COUNT(*)".to_string()],
                from: from.clone(),
                joins: joins.clone(),
                conditions: conditions.clone(),
                group_by: group_by.clone(),
                having: having.clone(),
                order_by: vec![],
                limit: None,
                offset: None,
            },
            SqlFragment::Raw(sql, p) => {
                // Fallback: text-based count query
                let upper = sql.to_uppercase();
                if let Some(from_idx) = upper.find(" FROM ") {
                    let rest = &sql[from_idx..];
                    SqlFragment::Raw(format!("SELECT COUNT(*){rest}"), p.clone())
                } else {
                    SqlFragment::Raw(sql.clone(), p.clone())
                }
            }
        }
    }

    /// Strip LIMIT and OFFSET from this fragment.
    pub fn without_limit_offset(&self) -> SqlFragment {
        match self {
            SqlFragment::Select {
                columns,
                from,
                joins,
                conditions,
                group_by,
                having,
                order_by,
                ..
            } => SqlFragment::Select {
                columns: columns.clone(),
                from: from.clone(),
                joins: joins.clone(),
                conditions: conditions.clone(),
                group_by: group_by.clone(),
                having: having.clone(),
                order_by: order_by.clone(),
                limit: None,
                offset: None,
            },
            raw => raw.clone(),
        }
    }

    /// Strip ORDER BY from this fragment.
    pub fn without_order_by(&self) -> SqlFragment {
        match self {
            SqlFragment::Select {
                columns,
                from,
                joins,
                conditions,
                group_by,
                having,
                limit,
                offset,
                ..
            } => SqlFragment::Select {
                columns: columns.clone(),
                from: from.clone(),
                joins: joins.clone(),
                conditions: conditions.clone(),
                group_by: group_by.clone(),
                having: having.clone(),
                order_by: vec![],
                limit: *limit,
                offset: *offset,
            },
            raw => raw.clone(),
        }
    }
}

impl ToSql for Condition {
    fn write_sql(&self, buf: &mut String, params: &mut Vec<Value>) {
        match self {
            Condition::Eq(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} = ?");
            }
            Condition::Neq(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} != ?");
            }
            Condition::Gt(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} > ?");
            }
            Condition::Lt(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} < ?");
            }
            Condition::Gte(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} >= ?");
            }
            Condition::Lte(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} <= ?");
            }
            Condition::Between(col, a, b) => {
                params.push(a.clone());
                params.push(b.clone());
                let _ = write!(buf, "{col} BETWEEN ? AND ?");
            }
            Condition::Like(col, pattern) => {
                params.push(Value::String(pattern.clone()));
                let _ = write!(buf, "{col} LIKE ?");
            }
            #[cfg(feature = "postgres")]
            Condition::ILike(col, pattern) => {
                params.push(Value::String(pattern.clone()));
                let _ = write!(buf, "{col} ILIKE ?");
            }
            Condition::In(col, vals) => {
                let _ = write!(buf, "{col} IN (");
                for (i, v) in vals.iter().enumerate() {
                    if i > 0 {
                        buf.push_str(", ");
                    }
                    buf.push('?');
                    params.push(v.clone());
                }
                buf.push(')');
            }
            Condition::IsNull(col) => {
                let _ = write!(buf, "{col} IS NULL");
            }
            Condition::IsNotNull(col) => {
                let _ = write!(buf, "{col} IS NOT NULL");
            }
            #[cfg(feature = "postgres")]
            Condition::RangeContains(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} @> ?");
            }
            #[cfg(feature = "postgres")]
            Condition::RangeContainedBy(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} <@ ?");
            }
            #[cfg(feature = "postgres")]
            Condition::RangeOverlaps(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} && ?");
            }
            #[cfg(feature = "postgres")]
            Condition::RangeLeftOf(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} << ?");
            }
            #[cfg(feature = "postgres")]
            Condition::RangeRightOf(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} >> ?");
            }
            #[cfg(feature = "postgres")]
            Condition::RangeAdjacent(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} -|- ?");
            }
            #[cfg(feature = "postgres")]
            Condition::RangeIsEmpty(col) => {
                let _ = write!(buf, "isempty({col})");
            }
            #[cfg(feature = "postgres")]
            Condition::JsonGet(col, key) => {
                params.push(Value::String(key.clone()));
                let _ = write!(buf, "{col}->?");
            }
            #[cfg(feature = "postgres")]
            Condition::JsonContains(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} @> ?");
            }
            #[cfg(feature = "postgres")]
            Condition::JsonHasKey(col, key) => {
                params.push(Value::String(key.clone()));
                let _ = write!(buf, "jsonb_exists({col}, ?)");
            }
            #[cfg(feature = "postgres")]
            Condition::ArrayContains(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} @> ?");
            }
            #[cfg(feature = "postgres")]
            Condition::ArrayContainedBy(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} <@ ?");
            }
            #[cfg(feature = "postgres")]
            Condition::ArrayOverlaps(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{col} && ?");
            }
            Condition::AggregateGt(expr, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} > ?", expr.to_sql_fragment());
            }
            Condition::AggregateLt(expr, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} < ?", expr.to_sql_fragment());
            }
            Condition::AggregateGte(expr, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} >= ?", expr.to_sql_fragment());
            }
            Condition::AggregateLte(expr, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} <= ?", expr.to_sql_fragment());
            }
            Condition::AggregateEq(expr, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} = ?", expr.to_sql_fragment());
            }
            Condition::InSubquery(col, sub_sql, sub_params) => {
                params.extend(sub_params.iter().cloned());
                let _ = write!(buf, "{col} IN ({sub_sql})");
            }
            Condition::Raw(sql, raw_params) => {
                params.extend(raw_params.iter().cloned());
                buf.push_str(sql);
            }
            Condition::Logical(op) => match op {
                LogicalOp::And(conds) => {
                    buf.push('(');
                    write_joined(buf, conds, " AND ", |b, c| c.write_sql(b, params));
                    buf.push(')');
                }
                LogicalOp::Or(conds) => {
                    buf.push('(');
                    write_joined(buf, conds, " OR ", |b, c| c.write_sql(b, params));
                    buf.push(')');
                }
            },
        }
    }
}
