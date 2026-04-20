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
pub use insert::{InsertBuilder, InsertManyBuilder, ParamLimitExceeded};
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
    crate::telemetry::record_query_built(operation, table, sql, params.len());
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
///
/// ## Implementation
///
/// Operates on raw bytes: `?` is ASCII (0x3F) so it can never appear as a
/// continuation byte of a multi-byte UTF-8 sequence. We scan bytes directly,
/// copy non-`?` runs in bulk with `extend_from_slice`, and only format the
/// `$N` token when we hit a placeholder. The output capacity is pre-computed
/// from the placeholder count to avoid reallocations.
#[cfg(feature = "postgres")]
pub fn rewrite_placeholders_pg(sql: &str) -> String {
    let bytes = sql.as_bytes();

    // Count placeholders to pre-size the output buffer.
    // Each `?` (1 byte) is replaced by `$N` (2–11 bytes); reserve for `$N` up
    // to u32::MAX but in practice SQL never has more than a few hundred params.
    let n_placeholders = bytecount_question_marks(bytes);
    // Worst case: every `?` becomes `$4294967295` (11 chars). In practice
    // params are small numbers, so this slightly over-allocates but avoids
    // any reallocation.
    let extra = n_placeholders.saturating_mul(10); // `$N` adds at most 10 extra bytes
    let mut result = String::with_capacity(sql.len() + extra);

    let mut idx = 1u32;
    let mut start = 0usize;

    for (i, &b) in bytes.iter().enumerate() {
        if b == b'?' {
            // SAFETY: `start..i` is a valid UTF-8 sub-slice because:
            // 1. `sql` is valid UTF-8.
            // 2. `?` (0x3F) is ASCII and cannot be a UTF-8 continuation byte,
            //    so splitting at any `?` position always lands on a char boundary.
            debug_assert!(std::str::from_utf8(&bytes[start..i]).is_ok());
            result.push_str(unsafe { std::str::from_utf8_unchecked(&bytes[start..i]) });
            let _ = write!(result, "${idx}");
            idx += 1;
            start = i + 1; // skip the `?` byte
        }
    }
    // Append the tail after the last placeholder (or the whole string if none).
    debug_assert!(std::str::from_utf8(&bytes[start..]).is_ok());
    result.push_str(unsafe { std::str::from_utf8_unchecked(&bytes[start..]) });
    result
}

/// Count the number of `?` bytes in a byte slice.
///
/// Kept as a separate function so it can be inlined and auto-vectorised by
/// the compiler independently of the main rewrite loop.
#[cfg(feature = "postgres")]
#[inline]
fn bytecount_question_marks(bytes: &[u8]) -> usize {
    bytes.iter().filter(|&&b| b == b'?').count()
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
