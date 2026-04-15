use crate::condition::{AggregateCondition, Condition};
use crate::ident::qi;
use crate::sql::{ToSql, write_joined};
use crate::table::Table;
use crate::value::Value;
use std::fmt::Write;
use std::marker::PhantomData;
use tracing::debug;

mod delete;
mod insert;
mod join;
mod select;
mod update;
mod with;

pub use delete::DeleteBuilder;
pub use insert::{InsertBuilder, InsertManyBuilder};
pub use join::{JoinClause, JoinKind};
pub use select::SelectBuilder;
pub use update::UpdateBuilder;
pub use with::WithBuilder;

// ── BuildError ──────────────────────────────────────────────────────

/// Error returned by `try_build()` and `try_new()` when a builder is
/// used incorrectly (missing WHERE clause, empty insert, etc.).
///
/// Use `try_build()` in production code to avoid panics. The regular
/// `build()` method still panics for backward compatibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildError {
    /// UPDATE or DELETE without a WHERE clause.
    ///
    /// Use `.filter()` to add conditions, or `.unfiltered()` to explicitly
    /// opt into a full-table operation.
    MissingFilter { operation: &'static str },
    /// `InsertManyBuilder` created with zero rows.
    EmptyInsert,
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::MissingFilter { operation } => {
                write!(
                    f,
                    "{operation} without WHERE is forbidden. Use .filter() or .unfiltered() explicitly."
                )
            }
            BuildError::EmptyInsert => {
                write!(f, "insert_many requires at least one row")
            }
        }
    }
}

impl std::error::Error for BuildError {}

// ── Dialect ─────────────────────────────────────────────────────────

/// SQL dialect — controls syntax differences between backends.
///
/// Pass to `InsertBuilder::build_with_dialect` /
/// `InsertManyBuilder::build_with_dialect` when you need dialect-specific
/// SQL (upsert syntax, placeholder style, …).
///
/// The default `build()` method emits portable SQL with `?` placeholders
/// and no dialect-specific extensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Dialect {
    /// Generic SQL — `?` placeholders, no vendor extensions. Default.
    #[default]
    Generic,
    /// PostgreSQL — `ON CONFLICT … DO UPDATE SET` upsert syntax.
    Postgres,
    /// MySQL / MariaDB — `ON DUPLICATE KEY UPDATE` upsert syntax.
    Mysql,
}

impl Dialect {
    /// Maximum number of bind parameters allowed in a single statement.
    ///
    /// - **PostgreSQL**: 65 535 (`$1` … `$65535`)
    /// - **MySQL**: 65 535 (practical limit)
    /// - **Generic / SQLite**: 32 766 (conservative; SQLite default is 999
    ///   but can be compiled up to 32 766)
    pub const fn max_params(self) -> usize {
        match self {
            Dialect::Postgres => 65_535,
            Dialect::Mysql => 65_535,
            Dialect::Generic => 32_766,
        }
    }
}

// ── OnConflict ──────────────────────────────────────────────────────

/// Conflict-resolution strategy for INSERT statements.
#[derive(Debug, Clone)]
pub enum OnConflict {
    /// `INSERT … ON CONFLICT DO NOTHING` (PostgreSQL) /
    /// `INSERT IGNORE …` (MySQL).
    DoNothing,
    /// Upsert: on conflict on `target_cols`, update `updates`.
    ///
    /// - PostgreSQL: `ON CONFLICT (col, …) DO UPDATE SET col = EXCLUDED.col, …`
    /// - MySQL: `ON DUPLICATE KEY UPDATE col = VALUES(col), …`
    ///
    /// `target_cols` is only used by PostgreSQL (MySQL infers the conflict
    /// target from the unique key that triggered the violation).
    DoUpdate {
        /// Columns that form the conflict target (PostgreSQL `ON CONFLICT (…)`).
        target_cols: Vec<&'static str>,
        /// Columns to update on conflict.
        updates: Vec<&'static str>,
    },
}

pub(crate) fn trace_query(operation: &str, table: &'static str, sql: &str, params: &[Value]) {
    debug!(
        target: "reify::query",
        operation,
        table,
        sql = %sql,
        params = ?params,
        "Built SQL query"
    );
}

/// Append an `ON CONFLICT` clause to `sql` based on the conflict strategy and dialect.
pub(crate) fn write_on_conflict(
    sql: &mut String,
    on_conflict: &Option<OnConflict>,
    dialect: Dialect,
) {
    match (on_conflict, dialect) {
        (Some(OnConflict::DoNothing), Dialect::Postgres) => {
            sql.push_str(" ON CONFLICT DO NOTHING");
        }
        (
            Some(OnConflict::DoUpdate {
                target_cols,
                updates,
            }),
            Dialect::Postgres,
        ) => {
            sql.push_str(" ON CONFLICT (");
            write_joined(sql, target_cols, ", ", |buf, c| buf.push_str(&qi(c)));
            sql.push_str(") DO UPDATE SET ");
            write_joined(sql, updates, ", ", |buf, c| {
                let _ = write!(buf, "{} = EXCLUDED.{}", qi(c), qi(c));
            });
        }
        (Some(OnConflict::DoUpdate { updates, .. }), Dialect::Mysql) => {
            sql.push_str(" ON DUPLICATE KEY UPDATE ");
            write_joined(sql, updates, ", ", |buf, c| {
                let _ = write!(buf, "{} = VALUES({})", qi(c), qi(c));
            });
        }
        _ => {}
    }
}

/// Append a `RETURNING` clause to `sql` (PostgreSQL only).
#[cfg(feature = "postgres")]
pub(crate) fn write_returning(sql: &mut String, returning: &Option<Vec<&'static str>>) {
    if let Some(ret_cols) = returning {
        sql.push_str(" RETURNING ");
        write_joined(sql, ret_cols, ", ", |buf, c| buf.push_str(&qi(c)));
    }
}

/// Rewrite `?` placeholders to PostgreSQL-style `$1, $2, …` positional params.
///
/// Call this on the SQL string returned by `build()` when targeting PostgreSQL.
/// This is a pure string transformation with a single allocation.
#[cfg(feature = "postgres")]
pub fn rewrite_placeholders_pg(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len() + 16);
    let mut idx = 1u32;
    for ch in sql.chars() {
        if ch == '?' {
            let _ = write!(result, "${idx}");
            idx += 1;
        } else {
            result.push(ch);
        }
    }
    result
}

// ── Aggregate expressions ───────────────────────────────────────────

/// A SQL expression that can appear in a SELECT list.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A plain column reference: `col`.
    Col(&'static str),
    /// `COUNT(col)` or `COUNT(*)`.
    Count(Option<&'static str>),
    /// `COUNT(DISTINCT col)`.
    CountDistinct(&'static str),
    /// `SUM(col)`.
    Sum(&'static str),
    /// `AVG(col)`.
    Avg(&'static str),
    /// `MIN(col)`.
    Min(&'static str),
    /// `MAX(col)`.
    Max(&'static str),
    /// `UPPER(col)`.
    Upper(&'static str),
    /// `LOWER(col)`.
    Lower(&'static str),
    /// `LENGTH(col)`.
    Length(&'static str),
    /// `ABS(col)`.
    Abs(&'static str),
    /// `ROUND(col)` or `ROUND(col, precision)`.
    Round(&'static str, Option<i32>),
    /// `COALESCE(col, default)`.
    Coalesce(&'static str, Box<Value>),
}

impl Expr {
    /// Render the expression to a SQL fragment.
    pub fn to_sql_fragment(&self) -> String {
        match self {
            Expr::Col(c) => qi(c),
            Expr::Count(None) => "COUNT(*)".to_string(),
            Expr::Count(Some(c)) => format!("COUNT({})", qi(c)),
            Expr::CountDistinct(c) => format!("COUNT(DISTINCT {})", qi(c)),
            Expr::Sum(c) => format!("SUM({})", qi(c)),
            Expr::Avg(c) => format!("AVG({})", qi(c)),
            Expr::Min(c) => format!("MIN({})", qi(c)),
            Expr::Max(c) => format!("MAX({})", qi(c)),
            Expr::Upper(c) => format!("UPPER({})", qi(c)),
            Expr::Lower(c) => format!("LOWER({})", qi(c)),
            Expr::Length(c) => format!("LENGTH({})", qi(c)),
            Expr::Abs(c) => format!("ABS({})", qi(c)),
            Expr::Round(c, None) => format!("ROUND({})", qi(c)),
            Expr::Round(c, Some(p)) => format!("ROUND({}, {p})", qi(c)),
            Expr::Coalesce(c, default) => {
                format!("COALESCE({}, {})", qi(c), default.to_sql_literal())
            }
        }
    }
}

impl Expr {
    /// `expr > val` — for use in HAVING clauses.
    pub fn gt(self, val: impl crate::value::IntoValue) -> Condition {
        Condition::Aggregate(AggregateCondition::Gt(self, val.into_value()))
    }
    /// `expr < val`
    pub fn lt(self, val: impl crate::value::IntoValue) -> Condition {
        Condition::Aggregate(AggregateCondition::Lt(self, val.into_value()))
    }
    /// `expr >= val`
    pub fn gte(self, val: impl crate::value::IntoValue) -> Condition {
        Condition::Aggregate(AggregateCondition::Gte(self, val.into_value()))
    }
    /// `expr <= val`
    pub fn lte(self, val: impl crate::value::IntoValue) -> Condition {
        Condition::Aggregate(AggregateCondition::Lte(self, val.into_value()))
    }
    /// `expr = val`
    pub fn eq(self, val: impl crate::value::IntoValue) -> Condition {
        Condition::Aggregate(AggregateCondition::Eq(self, val.into_value()))
    }
}

// ── Ordering ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Order {
    /// Sort ascending: `ORDER BY col ASC`.
    Asc(&'static str),
    /// Sort descending: `ORDER BY col DESC`.
    Desc(&'static str),
}

/// Helper returned by `Column` — lets you write `User::id.asc()`.
pub struct OrderExpr {
    pub col: &'static str,
}

impl OrderExpr {
    /// Wrap this column in an ascending [`Order`] expression.
    pub fn asc(self) -> Order {
        Order::Asc(self.col)
    }
    /// Wrap this column in a descending [`Order`] expression.
    pub fn desc(self) -> Order {
        Order::Desc(self.col)
    }
}
