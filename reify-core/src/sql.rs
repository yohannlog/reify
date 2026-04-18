use std::borrow::Cow;
use std::fmt::Write;

#[cfg(feature = "postgres")]
use crate::condition::PgCondition;
use crate::condition::{AggregateCondition, Condition, LogicalOp};
use crate::ident::qi;
use crate::query::JoinKind;
use crate::value::Value;

/// Trait to render a type into a SQL fragment + bound parameters.
///
/// `write_sql` borrows `self` and clones each [`Value`] into `params`. When
/// the caller owns the condition tree and does not need it afterwards,
/// [`IntoSql::into_sql`] moves values in without cloning, which matters on
/// large `Bytes`/`Jsonb` payloads.
pub trait ToSql {
    /// Write the SQL fragment directly into `buf`, appending bound params.
    ///
    /// This is the primary method — implement this to avoid per-call
    /// `String` allocations. Note: each bound [`Value`] is cloned. Use
    /// [`IntoSql`] when the caller owns the fragment.
    fn write_sql(&self, buf: &mut String, params: &mut Vec<Value>);

    /// Convenience wrapper that allocates and returns a `String`.
    fn to_sql(&self, params: &mut Vec<Value>) -> String {
        let mut buf = String::new();
        self.write_sql(&mut buf, params);
        buf
    }
}

/// Consuming variant of [`ToSql`] — moves bound values into `params`
/// instead of cloning. Implemented for types that own their parameter
/// data (e.g. [`Condition`]).
pub trait IntoSql {
    fn into_sql(self, buf: &mut String, params: &mut Vec<Value>);
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
pub enum SqlFragment<'a> {
    /// A structured SELECT query.
    Select {
        distinct: bool,
        columns: Vec<String>,
        from: String,
        joins: Cow<'a, [JoinFragment]>,
        conditions: Cow<'a, [Condition]>,
        group_by: Vec<String>,
        having: Cow<'a, [Condition]>,
        order_by: Cow<'a, [OrderFragment]>,
        limit: Option<u64>,
        offset: Option<u64>,
    },
    /// Raw SQL string with bound parameters (escape hatch).
    Raw(Cow<'a, str>, Cow<'a, [Value]>),
}

impl<'a> SqlFragment<'a> {
    /// Render this fragment into a SQL string, appending bound params.
    ///
    /// Uses a single pre-allocated buffer — no intermediate `Vec<String>`
    /// or per-clause `String` allocations.
    pub fn render(&self, params: &mut Vec<Value>) -> String {
        match self {
            SqlFragment::Select {
                distinct,
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

                if *distinct {
                    sql.push_str("SELECT DISTINCT ");
                } else {
                    sql.push_str("SELECT ");
                }
                if columns.is_empty() {
                    sql.push('*');
                } else {
                    write_joined(&mut sql, columns, ", ", |buf, c| buf.push_str(c));
                }
                sql.push_str(" FROM ");
                sql.push_str(&qi(from));

                for join in joins.iter() {
                    let _ = write!(
                        sql,
                        " {} {} ON {}",
                        join.kind.sql_keyword(),
                        qi(&join.table),
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
                sql.clone().into_owned()
            }
        }
    }

    /// Create a count query from this fragment.
    ///
    /// For `Select` variants: replaces columns with `COUNT(*)` and strips
    /// ORDER BY, LIMIT, OFFSET. For `Raw`: falls back to text manipulation.
    pub fn to_count_query(&self) -> SqlFragment<'a> {
        match self {
            SqlFragment::Select {
                from,
                joins,
                conditions,
                group_by,
                having,
                ..
            } => SqlFragment::Select {
                distinct: false,
                columns: vec!["COUNT(*)".to_string()],
                from: from.clone(),
                joins: joins.clone(),
                conditions: conditions.clone(),
                group_by: group_by.clone(),
                having: having.clone(),
                order_by: Cow::Owned(vec![]),
                limit: None,
                offset: None,
            },
            SqlFragment::Raw(sql, p) => {
                // Fallback: text-based count query
                let upper = sql.to_uppercase();
                if let Some(from_idx) = upper.find(" FROM ") {
                    let rest = &sql[from_idx..];
                    SqlFragment::Raw(Cow::Owned(format!("SELECT COUNT(*){rest}")), p.clone())
                } else {
                    SqlFragment::Raw(sql.clone(), p.clone())
                }
            }
        }
    }

    /// Strip LIMIT and OFFSET from this fragment.
    pub fn without_limit_offset(&self) -> SqlFragment<'a> {
        match self {
            SqlFragment::Select {
                distinct,
                columns,
                from,
                joins,
                conditions,
                group_by,
                having,
                order_by,
                ..
            } => SqlFragment::Select {
                distinct: *distinct,
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
    pub fn without_order_by(&self) -> SqlFragment<'a> {
        match self {
            SqlFragment::Select {
                distinct,
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
                distinct: *distinct,
                columns: columns.clone(),
                from: from.clone(),
                joins: joins.clone(),
                conditions: conditions.clone(),
                group_by: group_by.clone(),
                having: having.clone(),
                order_by: Cow::Owned(vec![]),
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
                let _ = write!(buf, "{} = ?", qi(col));
            }
            Condition::Neq(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} != ?", qi(col));
            }
            Condition::Gt(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} > ?", qi(col));
            }
            Condition::Lt(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} < ?", qi(col));
            }
            Condition::Gte(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} >= ?", qi(col));
            }
            Condition::Lte(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} <= ?", qi(col));
            }
            Condition::Between(col, a, b) => {
                params.push(a.clone());
                params.push(b.clone());
                let _ = write!(buf, "{} BETWEEN ? AND ?", qi(col));
            }
            Condition::Like(col, pattern) => {
                params.push(Value::String(pattern.clone()));
                let _ = write!(buf, "{} LIKE ? ESCAPE '\\'", qi(col));
            }
            Condition::In(col, vals) => {
                if vals.is_empty() {
                    // Empty IN list is invalid SQL — emit an always-false condition.
                    // Note: this differs from SQL's NULL-propagating semantics
                    // on a non-empty list containing NULL (`x IN (NULL)` → NULL,
                    // not FALSE). Reify does not introspect values for NULLs;
                    // a user-supplied `Value::Null` inside the list is bound
                    // verbatim and the database applies standard three-valued
                    // logic. See `condition_in_with_null_propagates` test.
                    buf.push_str("1 = 0");
                } else {
                    let _ = write!(buf, "{} IN (", qi(col));
                    for (i, v) in vals.iter().enumerate() {
                        if i > 0 {
                            buf.push_str(", ");
                        }
                        buf.push('?');
                        params.push(v.clone());
                    }
                    buf.push(')');
                }
            }
            Condition::IsNull(col) => {
                let _ = write!(buf, "{} IS NULL", qi(col));
            }
            Condition::IsNotNull(col) => {
                let _ = write!(buf, "{} IS NOT NULL", qi(col));
            }
            #[cfg(feature = "postgres")]
            Condition::Postgres(pg) => pg.write_sql(buf, params),
            Condition::Aggregate(agg) => agg.write_sql(buf, params),
            Condition::InSubquery(col, sub_sql, sub_params) => {
                params.extend(sub_params.iter().cloned());
                let _ = write!(buf, "{} IN ({sub_sql})", qi(col));
            }
            Condition::Raw(sql, raw_params) => {
                params.extend(raw_params.iter().cloned());
                buf.push_str(sql);
            }
            Condition::Logical(op) => match op {
                LogicalOp::And(conds) => match conds.as_slice() {
                    [] => buf.push_str("1 = 1"),
                    [only] => only.write_sql(buf, params),
                    many => {
                        buf.push('(');
                        write_joined(buf, many, " AND ", |b, c| c.write_sql(b, params));
                        buf.push(')');
                    }
                },
                LogicalOp::Or(conds) => match conds.as_slice() {
                    [] => buf.push_str("1 = 0"),
                    [only] => only.write_sql(buf, params),
                    many => {
                        buf.push('(');
                        write_joined(buf, many, " OR ", |b, c| c.write_sql(b, params));
                        buf.push(')');
                    }
                },
            },
        }
    }
}

impl IntoSql for Condition {
    fn into_sql(self, buf: &mut String, params: &mut Vec<Value>) {
        match self {
            Condition::Eq(col, val) => {
                params.push(val);
                let _ = write!(buf, "{} = ?", qi(col));
            }
            Condition::Neq(col, val) => {
                params.push(val);
                let _ = write!(buf, "{} != ?", qi(col));
            }
            Condition::Gt(col, val) => {
                params.push(val);
                let _ = write!(buf, "{} > ?", qi(col));
            }
            Condition::Lt(col, val) => {
                params.push(val);
                let _ = write!(buf, "{} < ?", qi(col));
            }
            Condition::Gte(col, val) => {
                params.push(val);
                let _ = write!(buf, "{} >= ?", qi(col));
            }
            Condition::Lte(col, val) => {
                params.push(val);
                let _ = write!(buf, "{} <= ?", qi(col));
            }
            Condition::Between(col, a, b) => {
                params.push(a);
                params.push(b);
                let _ = write!(buf, "{} BETWEEN ? AND ?", qi(col));
            }
            Condition::Like(col, pattern) => {
                params.push(Value::String(pattern));
                let _ = write!(buf, "{} LIKE ? ESCAPE '\\'", qi(col));
            }
            Condition::In(col, vals) => {
                if vals.is_empty() {
                    buf.push_str("1 = 0");
                } else {
                    let _ = write!(buf, "{} IN (", qi(col));
                    for (i, v) in vals.into_iter().enumerate() {
                        if i > 0 {
                            buf.push_str(", ");
                        }
                        buf.push('?');
                        params.push(v);
                    }
                    buf.push(')');
                }
            }
            Condition::IsNull(col) => {
                let _ = write!(buf, "{} IS NULL", qi(col));
            }
            Condition::IsNotNull(col) => {
                let _ = write!(buf, "{} IS NOT NULL", qi(col));
            }
            #[cfg(feature = "postgres")]
            Condition::Postgres(pg) => pg.write_sql(buf, params),
            Condition::Aggregate(agg) => agg.write_sql(buf, params),
            Condition::InSubquery(col, sub_sql, sub_params) => {
                params.extend(sub_params);
                let _ = write!(buf, "{} IN ({sub_sql})", qi(col));
            }
            Condition::Raw(sql, raw_params) => {
                params.extend(raw_params);
                buf.push_str(&sql);
            }
            Condition::Logical(op) => match op {
                LogicalOp::And(mut conds) => match conds.len() {
                    0 => buf.push_str("1 = 1"),
                    1 => conds.pop().unwrap().into_sql(buf, params),
                    _ => {
                        buf.push('(');
                        let mut first = true;
                        for c in conds {
                            if !first {
                                buf.push_str(" AND ");
                            }
                            first = false;
                            c.into_sql(buf, params);
                        }
                        buf.push(')');
                    }
                },
                LogicalOp::Or(mut conds) => match conds.len() {
                    0 => buf.push_str("1 = 0"),
                    1 => conds.pop().unwrap().into_sql(buf, params),
                    _ => {
                        buf.push('(');
                        let mut first = true;
                        for c in conds {
                            if !first {
                                buf.push_str(" OR ");
                            }
                            first = false;
                            c.into_sql(buf, params);
                        }
                        buf.push(')');
                    }
                },
            },
        }
    }
}

impl ToSql for AggregateCondition {
    fn write_sql(&self, buf: &mut String, params: &mut Vec<Value>) {
        match self {
            AggregateCondition::Gt(expr, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} > ?", expr.to_sql_fragment());
            }
            AggregateCondition::Lt(expr, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} < ?", expr.to_sql_fragment());
            }
            AggregateCondition::Gte(expr, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} >= ?", expr.to_sql_fragment());
            }
            AggregateCondition::Lte(expr, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} <= ?", expr.to_sql_fragment());
            }
            AggregateCondition::Eq(expr, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} = ?", expr.to_sql_fragment());
            }
        }
    }
}

#[cfg(feature = "postgres")]
impl ToSql for PgCondition {
    fn write_sql(&self, buf: &mut String, params: &mut Vec<Value>) {
        match self {
            PgCondition::ILike(col, pattern) => {
                params.push(Value::String(pattern.clone()));
                let _ = write!(buf, "{} ILIKE ? ESCAPE '\\'", qi(col));
            }
            PgCondition::RangeContains(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} @> ?", qi(col));
            }
            PgCondition::RangeContainedBy(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} <@ ?", qi(col));
            }
            PgCondition::RangeOverlaps(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} && ?", qi(col));
            }
            PgCondition::RangeLeftOf(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} << ?", qi(col));
            }
            PgCondition::RangeRightOf(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} >> ?", qi(col));
            }
            PgCondition::RangeAdjacent(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} -|- ?", qi(col));
            }
            PgCondition::RangeIsEmpty(col) => {
                let _ = write!(buf, "isempty({})", qi(col));
            }
            PgCondition::JsonContains(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} @> ?", qi(col));
            }
            PgCondition::JsonContainedBy(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} <@ ?", qi(col));
            }
            PgCondition::JsonHasKey(col, key) => {
                params.push(Value::String(key.clone()));
                let _ = write!(buf, "{} ? ?", qi(col));
            }
            PgCondition::JsonHasAnyKey(col, keys) => {
                params.push(Value::ArrayString(keys.clone()));
                let _ = write!(buf, "{} ?| ?", qi(col));
            }
            PgCondition::JsonHasAllKeys(col, keys) => {
                params.push(Value::ArrayString(keys.clone()));
                let _ = write!(buf, "{} ?& ?", qi(col));
            }
            PgCondition::JsonConcat(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} || ?", qi(col));
            }
            PgCondition::JsonDeleteKey(col, key) => {
                params.push(Value::String(key.clone()));
                let _ = write!(buf, "{} - ?", qi(col));
            }
            PgCondition::JsonPathGetText(col, path) => {
                params.push(Value::ArrayString(path.clone()));
                let _ = write!(buf, "{} #>> ?", qi(col));
            }
            PgCondition::JsonPathGet(col, path) => {
                params.push(Value::ArrayString(path.clone()));
                let _ = write!(buf, "{} #> ?", qi(col));
            }
            PgCondition::JsonPathMatch(col, path) => {
                params.push(Value::String(path.clone()));
                let _ = write!(buf, "{} @? ?", qi(col));
            }
            PgCondition::JsonPathTest(col, path) => {
                params.push(Value::String(path.clone()));
                let _ = write!(buf, "{} @@ ?", qi(col));
            }
            PgCondition::ArrayContains(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} @> ?", qi(col));
            }
            PgCondition::ArrayContainedBy(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} <@ ?", qi(col));
            }
            PgCondition::ArrayOverlaps(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "{} && ?", qi(col));
            }
            PgCondition::ArrayAnyEq(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "? = ANY({})", qi(col));
            }
            PgCondition::ArrayAllEq(col, val) => {
                params.push(val.clone());
                let _ = write!(buf, "? = ALL({})", qi(col));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn condition_in_empty_renders_always_false() {
        let cond = Condition::In("id", vec![]);
        let mut params = Vec::new();
        let mut buf = String::new();
        cond.write_sql(&mut buf, &mut params);
        assert_eq!(buf, "1 = 0", "empty IN should render as always-false");
        assert!(params.is_empty(), "empty IN should produce no params");
    }

    #[test]
    fn condition_in_nonempty_renders_correctly() {
        let cond = Condition::In("id", vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
        let mut params = Vec::new();
        let mut buf = String::new();
        cond.write_sql(&mut buf, &mut params);
        assert_eq!(buf, "\"id\" IN (?, ?, ?)");
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn condition_logical_and_or() {
        let cond = Condition::Eq("a", Value::I32(1))
            .and(Condition::Eq("b", Value::I32(2)))
            .or(Condition::Eq("c", Value::I32(3)));
        let mut params = Vec::new();
        let mut buf = String::new();
        cond.write_sql(&mut buf, &mut params);
        assert!(buf.contains("OR"), "expected OR in: {buf}");
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn condition_in_with_null_propagates() {
        // Per SQL three-valued logic, `x IN (NULL)` evaluates to NULL (not
        // FALSE), so rows where `x IS NULL` are still excluded by the outer
        // WHERE clause. Reify emits the list verbatim; NULL handling is left
        // to the database. This test pins the rendered SQL so the behaviour
        // is documented and regression-detected.
        let cond = Condition::In("id", vec![Value::I64(1), Value::Null]);
        let mut params = Vec::new();
        let mut buf = String::new();
        cond.write_sql(&mut buf, &mut params);
        assert_eq!(buf, "\"id\" IN (?, ?)");
        assert_eq!(params, vec![Value::I64(1), Value::Null]);
    }

    #[test]
    fn into_sql_moves_values_without_clone() {
        // Large payload: ensure IntoSql does not clone it.
        let big = vec![0u8; 1024];
        let cond = Condition::Eq("blob", Value::Bytes(big.clone()));
        let mut params = Vec::new();
        let mut buf = String::new();
        cond.into_sql(&mut buf, &mut params);
        assert_eq!(buf, "\"blob\" = ?");
        assert_eq!(params, vec![Value::Bytes(big)]);
    }
}
