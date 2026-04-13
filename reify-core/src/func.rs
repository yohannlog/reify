//! Type-safe SQL function wrappers.
//!
//! These free functions accept typed `Column<M, T>` references and return
//! [`Expr`] values that can be used in `select_expr()`, `having()`, etc.
//!
//! # Example
//!
//! ```ignore
//! use reify::func;
//!
//! let (sql, _) = User::find()
//!     .select_expr(&[
//!         func::count(User::id),
//!         func::max(User::created_at),
//!         func::avg(User::score),
//!     ])
//!     .build();
//! // SELECT COUNT(id), MAX(created_at), AVG(score) FROM users
//! ```

use crate::column::{Column, Numeric};
use crate::query::Expr;
use crate::value::IntoValue;

// ── Aggregate functions ────────────────────────────────────────────

/// `COUNT(col)` — count non-NULL values in a column.
pub fn count<M: 'static, T: 'static>(col: Column<M, T>) -> Expr {
    Expr::Count(Some(col.name))
}

/// `COUNT(*)` — count all rows.
pub fn count_all() -> Expr {
    Expr::Count(None)
}

/// `COUNT(DISTINCT col)` — count distinct non-NULL values.
pub fn count_distinct<M: 'static, T: 'static>(col: Column<M, T>) -> Expr {
    Expr::CountDistinct(col.name)
}

/// `SUM(col)` — sum of numeric column values.
pub fn sum<M: 'static, T: Numeric + 'static>(col: Column<M, T>) -> Expr {
    Expr::Sum(col.name)
}

/// `AVG(col)` — average of numeric column values.
pub fn avg<M: 'static, T: Numeric + 'static>(col: Column<M, T>) -> Expr {
    Expr::Avg(col.name)
}

/// `MIN(col)` — minimum value.
pub fn min<M: 'static, T: 'static>(col: Column<M, T>) -> Expr {
    Expr::Min(col.name)
}

/// `MAX(col)` — maximum value.
pub fn max<M: 'static, T: 'static>(col: Column<M, T>) -> Expr {
    Expr::Max(col.name)
}

// ── String functions ───────────────────────────────────────────────

/// `UPPER(col)` — convert to uppercase.
pub fn upper<M: 'static>(col: Column<M, String>) -> Expr {
    Expr::Upper(col.name)
}

/// `LOWER(col)` — convert to lowercase.
pub fn lower<M: 'static>(col: Column<M, String>) -> Expr {
    Expr::Lower(col.name)
}

/// `LENGTH(col)` — string length.
pub fn length<M: 'static>(col: Column<M, String>) -> Expr {
    Expr::Length(col.name)
}

// ── Numeric functions ──────────────────────────────────────────────

/// `ABS(col)` — absolute value.
pub fn abs<M: 'static, T: Numeric + 'static>(col: Column<M, T>) -> Expr {
    Expr::Abs(col.name)
}

/// `ROUND(col)` — round to nearest integer.
pub fn round<M: 'static, T: Numeric + 'static>(col: Column<M, T>) -> Expr {
    Expr::Round(col.name, None)
}

/// `ROUND(col, precision)` — round to `precision` decimal places.
pub fn round_to<M: 'static, T: Numeric + 'static>(col: Column<M, T>, precision: i32) -> Expr {
    Expr::Round(col.name, Some(precision))
}

// ── General functions ──────────────────────────────────────────────

/// `COALESCE(col, default)` — return first non-NULL value.
pub fn coalesce<M: 'static, T: IntoValue + 'static>(
    col: Column<M, Option<T>>,
    default: impl IntoValue,
) -> Expr {
    Expr::Coalesce(col.name, Box::new(default.into_value()))
}

/// `COALESCE(col, default)` — variant for non-nullable columns.
pub fn coalesce_val<M: 'static, T: IntoValue + 'static>(
    col: Column<M, T>,
    default: impl IntoValue,
) -> Expr {
    Expr::Coalesce(col.name, Box::new(default.into_value()))
}
