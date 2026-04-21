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

/// `VARIANCE(col)` — population variance (aggregate function).
///
/// Rendered per dialect:
/// - PostgreSQL: `VARIANCE(col)` (note: this is `VAR_SAMP` in PostgreSQL)
/// - MySQL/MariaDB: `VARIANCE(col)` (population variance, equivalent to `VAR_POP`)
///
/// ⚠️ **Cross-database warning**: PostgreSQL's `VARIANCE` computes sample variance,
/// while MySQL's computes population variance. For consistent behavior across
/// databases, use raw SQL with explicit `VAR_POP` or `VAR_SAMP`.
///
/// # Availability
///
/// This function is only available when the `postgres` or `mysql` feature is enabled.
/// SQLite does not support `VARIANCE`.
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub fn variance<M: 'static, T: Numeric + 'static>(col: Column<M, T>) -> Expr {
    Expr::Variance(col.name)
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

// ── Date/time extraction functions ────────────────────────────────

use crate::query::{DatePart, TrimWhere};

/// `YEAR(col)` / `EXTRACT(YEAR FROM col)` / `strftime('%Y', col)` — extract year.
///
/// Rendered per dialect:
/// - MySQL/MariaDB: `YEAR(col)`
/// - PostgreSQL: `EXTRACT(YEAR FROM col)`
/// - SQLite: `CAST(strftime('%Y', col) AS INTEGER)`
pub fn year<M: 'static, T: 'static>(col: Column<M, T>) -> Expr {
    Expr::Extract(DatePart::Year, col.name)
}

/// `MONTH(col)` / `EXTRACT(MONTH FROM col)` / `strftime('%m', col)` — extract month (1-12).
pub fn month<M: 'static, T: 'static>(col: Column<M, T>) -> Expr {
    Expr::Extract(DatePart::Month, col.name)
}

/// `DAY(col)` / `EXTRACT(DAY FROM col)` / `strftime('%d', col)` — extract day of month (1-31).
pub fn day<M: 'static, T: 'static>(col: Column<M, T>) -> Expr {
    Expr::Extract(DatePart::Day, col.name)
}

/// `HOUR(col)` / `EXTRACT(HOUR FROM col)` / `strftime('%H', col)` — extract hour (0-23).
pub fn hour<M: 'static, T: 'static>(col: Column<M, T>) -> Expr {
    Expr::Extract(DatePart::Hour, col.name)
}

/// `MINUTE(col)` / `EXTRACT(MINUTE FROM col)` / `strftime('%M', col)` — extract minute (0-59).
pub fn minute<M: 'static, T: 'static>(col: Column<M, T>) -> Expr {
    Expr::Extract(DatePart::Minute, col.name)
}

/// `SECOND(col)` / `EXTRACT(SECOND FROM col)` / `strftime('%S', col)` — extract second (0-59).
pub fn second<M: 'static, T: 'static>(col: Column<M, T>) -> Expr {
    Expr::Extract(DatePart::Second, col.name)
}

// ── String trimming functions ────────────────────────────────────────

/// `TRIM(col)` — remove whitespace from both ends.
///
/// Rendered per dialect:
/// - PostgreSQL/MySQL: `TRIM(BOTH FROM col)`
/// - SQLite: `TRIM(col)`
pub fn trim<M: 'static>(col: Column<M, String>) -> Expr {
    Expr::Trim(col.name, None, TrimWhere::Both)
}

/// `TRIM(BOTH chars FROM col)` — remove specified characters from both ends.
///
/// Rendered per dialect:
/// - PostgreSQL/MySQL: `TRIM(BOTH 'chars' FROM col)`
/// - SQLite: `TRIM(col, 'chars')`
pub fn trim_chars<M: 'static>(col: Column<M, String>, chars: impl Into<String>) -> Expr {
    Expr::Trim(col.name, Some(chars.into()), TrimWhere::Both)
}

/// `LTRIM(col)` / `TRIM(LEADING FROM col)` — remove whitespace from start.
///
/// Rendered per dialect:
/// - PostgreSQL/MySQL: `TRIM(LEADING FROM col)`
/// - SQLite: `TRIM(col)` (SQLite TRIM removes from both ends; use `ltrim_chars` for precise control)
pub fn ltrim<M: 'static>(col: Column<M, String>) -> Expr {
    Expr::Trim(col.name, None, TrimWhere::Leading)
}

/// `LTRIM(col, chars)` / `TRIM(LEADING chars FROM col)` — remove specified characters from start.
///
/// Rendered per dialect:
/// - PostgreSQL/MySQL: `TRIM(LEADING 'chars' FROM col)`
/// - SQLite: `LTRIM(col, 'chars')`
pub fn ltrim_chars<M: 'static>(col: Column<M, String>, chars: impl Into<String>) -> Expr {
    Expr::Trim(col.name, Some(chars.into()), TrimWhere::Leading)
}

/// `RTRIM(col)` / `TRIM(TRAILING FROM col)` — remove whitespace from end.
///
/// Rendered per dialect:
/// - PostgreSQL/MySQL: `TRIM(TRAILING FROM col)`
/// - SQLite: `TRIM(col)` (SQLite TRIM removes from both ends; use `rtrim_chars` for precise control)
pub fn rtrim<M: 'static>(col: Column<M, String>) -> Expr {
    Expr::Trim(col.name, None, TrimWhere::Trailing)
}

/// `RTRIM(col, chars)` / `TRIM(TRAILING chars FROM col)` — remove specified characters from end.
///
/// Rendered per dialect:
/// - PostgreSQL/MySQL: `TRIM(TRAILING 'chars' FROM col)`
/// - SQLite: `RTRIM(col, 'chars')`
pub fn rtrim_chars<M: 'static>(col: Column<M, String>, chars: impl Into<String>) -> Expr {
    Expr::Trim(col.name, Some(chars.into()), TrimWhere::Trailing)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Dialect;

    // Mock model for testing
    struct TestModel;

    #[test]
    fn func_year_creates_extract_expr() {
        let col: Column<TestModel, String> = Column::new("created_at");
        let expr = year(col);
        assert_eq!(
            expr.to_sql_fragment_dialect(Dialect::Mysql),
            "YEAR(\"created_at\")"
        );
    }

    #[test]
    fn func_month_creates_extract_expr() {
        let col: Column<TestModel, String> = Column::new("date_col");
        let expr = month(col);
        assert_eq!(
            expr.to_sql_fragment_dialect(Dialect::Postgres),
            "EXTRACT(MONTH FROM \"date_col\")"
        );
    }

    #[test]
    fn func_day_creates_extract_expr() {
        let col: Column<TestModel, String> = Column::new("ts");
        let expr = day(col);
        assert_eq!(
            expr.to_sql_fragment_dialect(Dialect::Generic),
            "CAST(strftime('%d', \"ts\") AS INTEGER)"
        );
    }

    #[test]
    fn func_hour_creates_extract_expr() {
        let col: Column<TestModel, String> = Column::new("timestamp");
        let expr = hour(col);
        assert_eq!(
            expr.to_sql_fragment_dialect(Dialect::Mysql),
            "HOUR(\"timestamp\")"
        );
    }

    #[test]
    fn func_minute_creates_extract_expr() {
        let col: Column<TestModel, String> = Column::new("time_col");
        let expr = minute(col);
        assert_eq!(
            expr.to_sql_fragment_dialect(Dialect::Postgres),
            "EXTRACT(MINUTE FROM \"time_col\")"
        );
    }

    #[test]
    fn func_second_creates_extract_expr() {
        let col: Column<TestModel, String> = Column::new("ts");
        let expr = second(col);
        assert_eq!(
            expr.to_sql_fragment_dialect(Dialect::Generic),
            "CAST(strftime('%S', \"ts\") AS INTEGER)"
        );
    }

    #[test]
    fn all_date_funcs_produce_correct_date_parts() {
        let col: Column<TestModel, String> = Column::new("c");

        // Verify each function produces the correct DatePart variant
        assert!(matches!(year(col), Expr::Extract(DatePart::Year, "c")));
        assert!(matches!(month(col), Expr::Extract(DatePart::Month, "c")));
        assert!(matches!(day(col), Expr::Extract(DatePart::Day, "c")));
        assert!(matches!(hour(col), Expr::Extract(DatePart::Hour, "c")));
        assert!(matches!(minute(col), Expr::Extract(DatePart::Minute, "c")));
        assert!(matches!(second(col), Expr::Extract(DatePart::Second, "c")));
    }

    // ── Trim function tests ─────────────────────────────────────────────

    #[test]
    fn func_trim_creates_trim_expr() {
        let col: Column<TestModel, String> = Column::new("name");
        let expr = trim(col);
        assert!(matches!(expr, Expr::Trim("name", None, TrimWhere::Both)));
    }

    #[test]
    fn func_trim_chars_creates_trim_expr_with_chars() {
        let col: Column<TestModel, String> = Column::new("name");
        let expr = trim_chars(col, "x");
        assert!(matches!(expr, Expr::Trim("name", Some(_), TrimWhere::Both)));
        assert_eq!(
            expr.to_sql_fragment_dialect(Dialect::Mysql),
            "TRIM(BOTH 'x' FROM \"name\")"
        );
    }

    #[test]
    fn func_ltrim_creates_leading_trim() {
        let col: Column<TestModel, String> = Column::new("col");
        let expr = ltrim(col);
        assert!(matches!(expr, Expr::Trim("col", None, TrimWhere::Leading)));
        assert_eq!(
            expr.to_sql_fragment_dialect(Dialect::Postgres),
            "TRIM(LEADING FROM \"col\")"
        );
    }

    #[test]
    fn func_ltrim_chars_creates_leading_trim_with_chars() {
        let col: Column<TestModel, String> = Column::new("col");
        let expr = ltrim_chars(col, "0");
        assert_eq!(
            expr.to_sql_fragment_dialect(Dialect::Generic),
            "LTRIM(\"col\", '0')"
        );
    }

    #[test]
    fn func_rtrim_creates_trailing_trim() {
        let col: Column<TestModel, String> = Column::new("col");
        let expr = rtrim(col);
        assert!(matches!(expr, Expr::Trim("col", None, TrimWhere::Trailing)));
        assert_eq!(
            expr.to_sql_fragment_dialect(Dialect::Mysql),
            "TRIM(TRAILING FROM \"col\")"
        );
    }

    #[test]
    fn func_rtrim_chars_creates_trailing_trim_with_chars() {
        let col: Column<TestModel, String> = Column::new("col");
        let expr = rtrim_chars(col, "!");
        assert_eq!(
            expr.to_sql_fragment_dialect(Dialect::Generic),
            "RTRIM(\"col\", '!')"
        );
    }

    #[test]
    fn all_trim_funcs_produce_correct_variants() {
        let col: Column<TestModel, String> = Column::new("c");

        assert!(matches!(trim(col), Expr::Trim("c", None, TrimWhere::Both)));
        assert!(matches!(
            trim_chars(col, "x"),
            Expr::Trim("c", Some(_), TrimWhere::Both)
        ));
        assert!(matches!(
            ltrim(col),
            Expr::Trim("c", None, TrimWhere::Leading)
        ));
        assert!(matches!(
            ltrim_chars(col, "x"),
            Expr::Trim("c", Some(_), TrimWhere::Leading)
        ));
        assert!(matches!(
            rtrim(col),
            Expr::Trim("c", None, TrimWhere::Trailing)
        ));
        assert!(matches!(
            rtrim_chars(col, "x"),
            Expr::Trim("c", Some(_), TrimWhere::Trailing)
        ));
    }

    // ── Variance function tests ─────────────────────────────────────────

    #[test]
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    fn func_variance_creates_variance_expr() {
        let col: Column<TestModel, f64> = Column::new("score");
        let expr = variance(col);
        assert!(matches!(expr, Expr::Variance("score")));
    }

    #[test]
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    fn func_variance_renders_per_dialect() {
        let col: Column<TestModel, f64> = Column::new("val");
        let expr = variance(col);

        assert_eq!(
            expr.to_sql_fragment_dialect(Dialect::Postgres),
            "VARIANCE(\"val\")"
        );
        assert_eq!(
            expr.to_sql_fragment_dialect(Dialect::Mysql),
            "VARIANCE(\"val\")"
        );
    }
}
