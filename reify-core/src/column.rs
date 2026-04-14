use std::marker::PhantomData;

use crate::condition::Condition;
#[cfg(feature = "postgres")]
use crate::condition::PgCondition;
use crate::query::{Expr, SelectBuilder};
use crate::table::Table;
use crate::value::IntoValue;

/// A typed column reference: `Column<Model, FieldType>`.
///
/// Generated as associated constants by `#[derive(Table)]`.
/// Provides type-safe filter methods that depend on `T`.
///
/// `Copy + Clone` because the struct only holds a `&'static str` and
/// `PhantomData` — this lets free functions in [`crate::func`] accept
/// columns by value without requiring explicit `.clone()`.
pub struct Column<M, T> {
    pub name: &'static str,
    _model: PhantomData<M>,
    _type: PhantomData<T>,
}

impl<M, T> Clone for Column<M, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<M, T> Copy for Column<M, T> {}

impl<M, T> Column<M, T> {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            _model: PhantomData,
            _type: PhantomData,
        }
    }

    /// `COUNT(col)` aggregate expression.
    pub fn count(&self) -> Expr {
        Expr::Count(Some(self.name))
    }

    /// `MIN(col)` aggregate expression.
    pub fn min_expr(&self) -> Expr {
        Expr::Min(self.name)
    }

    /// `MAX(col)` aggregate expression.
    pub fn max_expr(&self) -> Expr {
        Expr::Max(self.name)
    }
}

// ── Numeric aggregate helpers ──────────────────────────────────────

impl<M: 'static, T: Numeric + 'static> Column<M, T> {
    /// `SUM(col)` aggregate expression (numeric columns only).
    pub fn sum(&self) -> Expr {
        Expr::Sum(self.name)
    }

    /// `AVG(col)` aggregate expression (numeric columns only).
    pub fn avg(&self) -> Expr {
        Expr::Avg(self.name)
    }
}

// ── Operators available on all columns ──────────────────────────────

impl<M: 'static, T: IntoValue + 'static> Column<M, T> {
    pub fn eq(&self, val: impl IntoValue) -> Condition {
        Condition::Eq(self.name, val.into_value())
    }

    pub fn neq(&self, val: impl IntoValue) -> Condition {
        Condition::Neq(self.name, val.into_value())
    }

    pub fn in_list(&self, vals: Vec<impl IntoValue>) -> Condition {
        Condition::In(
            self.name,
            vals.into_iter().map(|v| v.into_value()).collect(),
        )
    }

    /// `col IN (SELECT ...)` — filter using a subquery.
    pub fn in_subquery<S: Table>(&self, sub: SelectBuilder<S>) -> Condition {
        let (sql, params) = sub.build();
        Condition::InSubquery(self.name, sql, params)
    }
}

// ── Numeric operators ───────────────────────────────────────────────

/// Trait marker for numeric column types.
pub trait Numeric: IntoValue {}
impl Numeric for i16 {}
impl Numeric for i32 {}
impl Numeric for i64 {}
impl Numeric for f32 {}
impl Numeric for f64 {}

impl<M: 'static, T: Numeric + 'static> Column<M, T> {
    pub fn gt(&self, val: impl IntoValue) -> Condition {
        Condition::Gt(self.name, val.into_value())
    }

    pub fn lt(&self, val: impl IntoValue) -> Condition {
        Condition::Lt(self.name, val.into_value())
    }

    pub fn gte(&self, val: impl IntoValue) -> Condition {
        Condition::Gte(self.name, val.into_value())
    }

    pub fn lte(&self, val: impl IntoValue) -> Condition {
        Condition::Lte(self.name, val.into_value())
    }

    pub fn between(&self, a: impl IntoValue, b: impl IntoValue) -> Condition {
        Condition::Between(self.name, a.into_value(), b.into_value())
    }
}

// ── String operators ────────────────────────────────────────────────

/// Escape LIKE/ILIKE wildcard characters in user input.
///
/// Escapes `\`, `%`, and `_` so they are treated as literal characters
/// in a LIKE pattern. The corresponding SQL uses `ESCAPE '\\'` to
/// declare the escape character.
fn escape_like(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

impl<M: 'static> Column<M, String> {
    /// Raw LIKE pattern — wildcards in `pattern` are **not** escaped.
    ///
    /// Use this when you want to pass your own `%` / `_` wildcards.
    /// The generated SQL includes `ESCAPE '\\'` so you can still use
    /// `\\%` and `\\_` for literal matches.
    pub fn like(&self, pattern: &str) -> Condition {
        Condition::Like(self.name, pattern.to_owned())
    }

    /// `col LIKE '%sub%'` — user input is escaped so `%` and `_` in
    /// `sub` are treated as literals.
    pub fn contains(&self, sub: &str) -> Condition {
        let escaped = escape_like(sub);
        Condition::Like(self.name, format!("%{escaped}%"))
    }

    /// `col LIKE 'prefix%'` — user input is escaped.
    pub fn starts_with(&self, prefix: &str) -> Condition {
        let escaped = escape_like(prefix);
        Condition::Like(self.name, format!("{escaped}%"))
    }

    /// `col LIKE '%suffix'` — user input is escaped.
    pub fn ends_with(&self, suffix: &str) -> Condition {
        let escaped = escape_like(suffix);
        Condition::Like(self.name, format!("%{escaped}"))
    }

    /// Raw case-insensitive LIKE (PostgreSQL `ILIKE`) — wildcards are **not** escaped.
    #[cfg(feature = "postgres")]
    pub fn ilike(&self, pattern: &str) -> Condition {
        Condition::Postgres(PgCondition::ILike(self.name, pattern.to_owned()))
    }

    /// Case-insensitive contains (PostgreSQL `ILIKE '%sub%'`) — user input is escaped.
    #[cfg(feature = "postgres")]
    pub fn icontains(&self, sub: &str) -> Condition {
        let escaped = escape_like(sub);
        Condition::Postgres(PgCondition::ILike(self.name, format!("%{escaped}%")))
    }

    /// Case-insensitive starts-with (PostgreSQL `ILIKE 'prefix%'`) — user input is escaped.
    #[cfg(feature = "postgres")]
    pub fn istarts_with(&self, prefix: &str) -> Condition {
        let escaped = escape_like(prefix);
        Condition::Postgres(PgCondition::ILike(self.name, format!("{escaped}%")))
    }

    /// Case-insensitive ends-with (PostgreSQL `ILIKE '%suffix'`) — user input is escaped.
    #[cfg(feature = "postgres")]
    pub fn iends_with(&self, suffix: &str) -> Condition {
        let escaped = escape_like(suffix);
        Condition::Postgres(PgCondition::ILike(self.name, format!("%{escaped}")))
    }
}

// ── Temporal operators (PostgreSQL & MySQL) ────────────────────────

/// Trait marker for temporal column types.
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub trait Temporal: IntoValue {}

#[cfg(any(feature = "postgres", feature = "mysql"))]
impl Temporal for chrono::NaiveDateTime {}
#[cfg(any(feature = "postgres", feature = "mysql"))]
impl Temporal for chrono::NaiveDate {}
#[cfg(any(feature = "postgres", feature = "mysql"))]
impl Temporal for chrono::NaiveTime {}
#[cfg(feature = "postgres")]
impl Temporal for chrono::DateTime<chrono::Utc> {}

#[cfg(any(feature = "postgres", feature = "mysql"))]
impl<M: 'static, T: Temporal + 'static> Column<M, T> {
    pub fn before(&self, val: impl IntoValue) -> Condition {
        Condition::Lt(self.name, val.into_value())
    }

    pub fn after(&self, val: impl IntoValue) -> Condition {
        Condition::Gt(self.name, val.into_value())
    }

    pub fn between_times(&self, a: impl IntoValue, b: impl IntoValue) -> Condition {
        Condition::Between(self.name, a.into_value(), b.into_value())
    }
}

// ── Range operators (PostgreSQL) ────────────────────────────────────

#[cfg(feature = "postgres")]
impl<M: 'static, T: crate::range::RangeElement + 'static> Column<M, crate::range::Range<T>>
where
    crate::range::Range<T>: IntoValue,
{
    /// Range contains an element: `@>` operator.
    ///
    /// ```ignore
    /// Event::duration.contains_element(5i32)
    /// // → duration @> 5
    /// ```
    pub fn contains_element(&self, val: impl IntoValue) -> Condition {
        Condition::Postgres(PgCondition::RangeContains(self.name, val.into_value()))
    }

    /// Range contains another range: `@>` operator.
    ///
    /// ```ignore
    /// Event::duration.contains_range(Range::closed(5, 10))
    /// // → duration @> '[5,10]'
    /// ```
    pub fn contains_range(&self, val: crate::range::Range<T>) -> Condition {
        Condition::Postgres(PgCondition::RangeContains(self.name, val.into_value()))
    }

    /// Range is contained by another range: `<@` operator.
    pub fn contained_by(&self, val: crate::range::Range<T>) -> Condition {
        Condition::Postgres(PgCondition::RangeContainedBy(self.name, val.into_value()))
    }

    /// Ranges overlap: `&&` operator.
    pub fn overlaps(&self, val: crate::range::Range<T>) -> Condition {
        Condition::Postgres(PgCondition::RangeOverlaps(self.name, val.into_value()))
    }

    /// Range is strictly left of: `<<` operator.
    pub fn left_of(&self, val: crate::range::Range<T>) -> Condition {
        Condition::Postgres(PgCondition::RangeLeftOf(self.name, val.into_value()))
    }

    /// Range is strictly right of: `>>` operator.
    pub fn right_of(&self, val: crate::range::Range<T>) -> Condition {
        Condition::Postgres(PgCondition::RangeRightOf(self.name, val.into_value()))
    }

    /// Range is adjacent to: `-|-` operator.
    pub fn adjacent(&self, val: crate::range::Range<T>) -> Condition {
        Condition::Postgres(PgCondition::RangeAdjacent(self.name, val.into_value()))
    }

    /// Range is empty: `isempty(column)`.
    pub fn is_empty_range(&self) -> Condition {
        Condition::Postgres(PgCondition::RangeIsEmpty(self.name))
    }
}

// ── JSONB operators (PostgreSQL) ───────────────────────────────────

/// A JSONB field access expression: `column->>'key'`.
///
/// Returned by [`Column<M, serde_json::Value>::json_get()`]. This is an
/// *expression* (it returns a value), not a condition. Use the methods
/// on this struct to build actual WHERE conditions:
///
/// ```ignore
/// User::metadata.json_get("role").eq("admin")
/// // → metadata->>'role' = ?
///
/// User::metadata.json_get("bio").is_null()
/// // → metadata->>'bio' IS NULL
/// ```
#[cfg(feature = "postgres")]
pub struct JsonExpr {
    column: &'static str,
    key: String,
}

#[cfg(feature = "postgres")]
impl JsonExpr {
    /// `column->>'key' = value`
    pub fn eq(&self, val: impl IntoValue) -> Condition {
        Condition::Raw(
            format!("{}->>'{}' = ?", crate::ident::qi(self.column), self.key),
            vec![val.into_value()],
        )
    }

    /// `column->>'key' != value`
    pub fn neq(&self, val: impl IntoValue) -> Condition {
        Condition::Raw(
            format!("{}->>'{}' != ?", crate::ident::qi(self.column), self.key),
            vec![val.into_value()],
        )
    }

    /// `column->>'key' IS NULL`
    pub fn is_null(&self) -> Condition {
        Condition::Raw(
            format!("{}->>'{}' IS NULL", crate::ident::qi(self.column), self.key),
            vec![],
        )
    }

    /// `column->>'key' IS NOT NULL`
    pub fn is_not_null(&self) -> Condition {
        Condition::Raw(
            format!(
                "{}->>'{}' IS NOT NULL",
                crate::ident::qi(self.column),
                self.key
            ),
            vec![],
        )
    }

    /// `column->>'key' LIKE pattern` — raw pattern, wildcards not escaped.
    pub fn like(&self, pattern: &str) -> Condition {
        Condition::Raw(
            format!(
                "{}->>'{}' LIKE ? ESCAPE '\\'",
                crate::ident::qi(self.column),
                self.key
            ),
            vec![crate::value::Value::String(pattern.to_owned())],
        )
    }

    /// `column->>'key' LIKE '%sub%'` — user input is escaped.
    pub fn contains(&self, sub: &str) -> Condition {
        let escaped = escape_like(sub);
        Condition::Raw(
            format!(
                "{}->>'{}' LIKE ? ESCAPE '\\'",
                crate::ident::qi(self.column),
                self.key
            ),
            vec![crate::value::Value::String(format!("%{escaped}%"))],
        )
    }
}

#[cfg(feature = "postgres")]
impl<M: 'static> Column<M, serde_json::Value> {
    /// JSONB field access as text: `column->>'key'`.
    ///
    /// Returns a [`JsonExpr`] — use `.eq()`, `.neq()`, `.is_null()`, etc.
    /// to build a WHERE condition.
    ///
    /// ```ignore
    /// User::metadata.json_get("role").eq("admin")
    /// // → metadata->>'role' = ?
    /// ```
    pub fn json_get(&self, key: &str) -> JsonExpr {
        JsonExpr {
            column: self.name,
            key: key.to_owned(),
        }
    }

    /// JSONB contains: `column @> value`.
    ///
    /// ```ignore
    /// User::metadata.json_contains(serde_json::json!({"active": true}))
    /// // → metadata @> '{"active": true}'
    /// ```
    pub fn json_contains(&self, val: impl crate::value::IntoValue) -> Condition {
        Condition::Postgres(PgCondition::JsonContains(self.name, val.into_value()))
    }

    /// JSONB key exists: `column ? key`.
    ///
    /// ```ignore
    /// User::metadata.json_has_key("email")
    /// // → metadata ? 'email'
    /// ```
    pub fn json_has_key(&self, key: &str) -> Condition {
        Condition::Postgres(PgCondition::JsonHasKey(self.name, key.to_owned()))
    }
}

// ── Array operators (PostgreSQL) ────────────────────────────────────

#[cfg(feature = "postgres")]
impl<M: 'static, T: IntoValue + Clone + 'static> Column<M, Vec<T>>
where
    Vec<T>: IntoValue,
{
    /// Array contains element: `@>` operator.
    ///
    /// ```ignore
    /// Post::tags.contains(vec!["rust".to_string()])
    /// // → tags @> ARRAY['rust']
    /// ```
    pub fn contains(&self, val: Vec<T>) -> Condition {
        Condition::Postgres(PgCondition::ArrayContains(self.name, val.into_value()))
    }

    /// Array is contained by: `<@` operator.
    ///
    /// ```ignore
    /// Post::tags.contained_by(vec!["rust".to_string(), "python".to_string()])
    /// // → tags <@ ARRAY['rust','python']
    /// ```
    pub fn contained_by(&self, val: Vec<T>) -> Condition {
        Condition::Postgres(PgCondition::ArrayContainedBy(self.name, val.into_value()))
    }

    /// Arrays overlap: `&&` operator.
    ///
    /// ```ignore
    /// Post::tags.overlaps(vec!["rust".to_string(), "go".to_string()])
    /// // → tags && ARRAY['rust','go']
    /// ```
    pub fn overlaps(&self, val: Vec<T>) -> Condition {
        Condition::Postgres(PgCondition::ArrayOverlaps(self.name, val.into_value()))
    }
}

// ── Option (nullable) operators ─────────────────────────────────────

impl<M: 'static, T: 'static> Column<M, Option<T>> {
    pub fn is_null(&self) -> Condition {
        Condition::IsNull(self.name)
    }

    pub fn is_not_null(&self) -> Condition {
        Condition::IsNotNull(self.name)
    }
}
