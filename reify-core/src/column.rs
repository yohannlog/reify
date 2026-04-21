use std::marker::PhantomData;

use crate::condition::Condition;
#[cfg(feature = "postgres")]
use crate::condition::PgCondition;
use crate::query::{Expr, Order, SelectBuilder};
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

    /// Ascending sort expression: `ORDER BY col ASC`.
    pub fn asc(self) -> Order {
        Order::Asc(self.name)
    }

    /// Descending sort expression: `ORDER BY col DESC`.
    pub fn desc(self) -> Order {
        Order::Desc(self.name)
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

/// Validate that a JSONB key is safe to use.
///
/// Keys are passed as bound parameters, but we still reject null bytes
/// and excessively long strings as a defence-in-depth measure.
#[cfg(feature = "postgres")]
fn validate_json_key(key: &str) -> Result<&str, crate::db::DbError> {
    if key.contains('\0') {
        return Err(crate::db::DbError::Other(
            "JSON key must not contain null bytes".into(),
        ));
    }
    if key.len() > 512 {
        return Err(crate::db::DbError::Other(
            "JSON key too long (max 512 chars)".into(),
        ));
    }
    Ok(key)
}

/// A JSONB single-key access expression: `column ->> $key`.
///
/// Returned by [`Column<M, serde_json::Value>::json_get()`]. The key is
/// **always passed as a bound parameter** — never interpolated into SQL —
/// so user-supplied keys are safe.
///
/// ```ignore
/// User::metadata.json_get("role").eq("admin")
/// // → "metadata" ->> $1 = $2
///
/// User::metadata.json_get("bio").is_null()
/// // → "metadata" ->> $1 IS NULL
/// ```
#[cfg(feature = "postgres")]
pub struct JsonExpr {
    column: &'static str,
    /// Borrowed when the key is a `&'static str` literal, owned when built
    /// from user input. Avoids a `String` clone per operator call.
    key: std::borrow::Cow<'static, str>,
}

#[cfg(feature = "postgres")]
impl JsonExpr {
    /// Build a `Raw` condition `"col" ->> ? <op> ?` with `[key, val]` params.
    fn raw_binop(&self, op: &str, val: crate::value::Value) -> Condition {
        Condition::Raw(
            format!("{} ->> ? {op} ?", crate::ident::qi(self.column)),
            vec![
                crate::value::Value::String(self.key.as_ref().to_owned()),
                val,
            ],
        )
    }

    /// Build a `Raw` condition `"col" ->> ? <suffix>` with `[key]` params only.
    fn raw_unary(&self, suffix: &str) -> Condition {
        Condition::Raw(
            format!("{} ->> ? {suffix}", crate::ident::qi(self.column)),
            vec![crate::value::Value::String(self.key.as_ref().to_owned())],
        )
    }

    /// Build a `Raw` LIKE/ILIKE condition with escape clause + a pattern param.
    fn raw_like(&self, op: &str, pattern: String, escape: bool) -> Condition {
        let sql = if escape {
            format!("{} ->> ? {op} ? ESCAPE '\\'", crate::ident::qi(self.column))
        } else {
            format!("{} ->> ? {op} ?", crate::ident::qi(self.column))
        };
        Condition::Raw(
            sql,
            vec![
                crate::value::Value::String(self.key.as_ref().to_owned()),
                crate::value::Value::String(pattern),
            ],
        )
    }

    /// `"column" ->> ? = ?`
    pub fn eq(&self, val: impl IntoValue) -> Condition {
        self.raw_binop("=", val.into_value())
    }

    /// `"column" ->> ? != ?`
    pub fn neq(&self, val: impl IntoValue) -> Condition {
        self.raw_binop("!=", val.into_value())
    }

    /// `"column" ->> ? IS NULL`
    pub fn is_null(&self) -> Condition {
        self.raw_unary("IS NULL")
    }

    /// `"column" ->> ? IS NOT NULL`
    pub fn is_not_null(&self) -> Condition {
        self.raw_unary("IS NOT NULL")
    }

    /// `"column" ->> ? LIKE ?` — raw pattern, wildcards not escaped.
    pub fn like(&self, pattern: &str) -> Condition {
        self.raw_like("LIKE", pattern.to_owned(), true)
    }

    /// `"column" ->> ? LIKE '%sub%'` — user input is escaped.
    pub fn contains(&self, sub: &str) -> Condition {
        let escaped = escape_like(sub);
        self.raw_like("LIKE", format!("%{escaped}%"), true)
    }

    /// `"column" ->> ? ILIKE ?` — raw pattern, wildcards are **not** escaped.
    pub fn ilike(&self, pattern: &str) -> Condition {
        self.raw_like("ILIKE", pattern.to_owned(), false)
    }

    /// `"column" ->> ? ILIKE '%sub%'` — case-insensitive contains, user input is escaped.
    pub fn icontains(&self, sub: &str) -> Condition {
        let escaped = escape_like(sub);
        self.raw_like("ILIKE", format!("%{escaped}%"), false)
    }

    /// `"column" ->> ? ILIKE 'prefix%'` — case-insensitive starts-with, user input is escaped.
    pub fn istarts_with(&self, prefix: &str) -> Condition {
        let escaped = escape_like(prefix);
        self.raw_like("ILIKE", format!("{escaped}%"), false)
    }

    /// `"column" ->> ? ILIKE '%suffix'` — case-insensitive ends-with, user input is escaped.
    pub fn iends_with(&self, suffix: &str) -> Condition {
        let escaped = escape_like(suffix);
        self.raw_like("ILIKE", format!("%{escaped}"), false)
    }
}

#[cfg(feature = "postgres")]
impl<M: 'static> Column<M, serde_json::Value> {
    /// JSONB single-key access as text: `"column" ->> ?`.
    ///
    /// The key is passed as a **bound parameter** — safe for user-supplied input.
    /// Returns a [`JsonExpr`] — chain `.eq()`, `.neq()`, `.is_null()`, etc.
    ///
    /// Returns `None` if `key` contains null bytes or exceeds 512 characters.
    ///
    /// ```ignore
    /// User::metadata.json_get("role")?.eq("admin")
    /// // → "metadata" ->> $1 = $2
    /// ```
    pub fn json_get(&self, key: &str) -> Option<JsonExpr> {
        Some(JsonExpr {
            column: self.name,
            key: std::borrow::Cow::Owned(validate_json_key(key).ok()?.to_owned()),
        })
    }

    /// Same as [`json_get`](Self::json_get) but takes a `&'static str`
    /// literal, avoiding the key allocation entirely. Prefer this when
    /// the key is known at compile time.
    ///
    /// Returns `None` if `key` contains null bytes or exceeds 512 characters
    /// (in practice impossible for compile-time literals, but kept consistent).
    pub fn json_get_static(&self, key: &'static str) -> Option<JsonExpr> {
        Some(JsonExpr {
            column: self.name,
            key: std::borrow::Cow::Borrowed(validate_json_key(key).ok()?),
        })
    }

    /// JSONB nested path access as text: `"column" #>> ARRAY[...]`.
    ///
    /// ```ignore
    /// User::metadata.json_get_path(&["address", "city"]).eq("Paris")
    /// // → "metadata" #>> $1 = $2  (where $1 = ARRAY['address','city'])
    /// ```
    pub fn json_get_path(&self, path: &[&str]) -> JsonPathExpr {
        JsonPathExpr {
            column: self.name,
            path: path.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// JSONB contains: `column @> value`.
    ///
    /// ```ignore
    /// User::metadata.json_contains(serde_json::json!({"active": true}))
    /// // → "metadata" @> $1
    /// ```
    pub fn json_contains(&self, val: impl crate::value::IntoValue) -> Condition {
        Condition::Postgres(PgCondition::JsonContains(self.name, val.into_value()))
    }

    /// JSONB is contained by: `column <@ value`.
    ///
    /// ```ignore
    /// User::metadata.json_contained_by(serde_json::json!({"role": "admin", "active": true}))
    /// // → "metadata" <@ $1
    /// ```
    pub fn json_contained_by(&self, val: impl crate::value::IntoValue) -> Condition {
        Condition::Postgres(PgCondition::JsonContainedBy(self.name, val.into_value()))
    }

    /// JSONB key exists: `column ? key`.
    ///
    /// ```ignore
    /// User::metadata.json_has_key("email")
    /// // → "metadata" ? $1
    /// ```
    pub fn json_has_key(&self, key: &str) -> Condition {
        Condition::Postgres(PgCondition::JsonHasKey(self.name, key.to_owned()))
    }

    /// JSONB any key exists: `column ?| keys`.
    ///
    /// ```ignore
    /// User::metadata.json_has_any_key(&["email", "phone"])
    /// // → "metadata" ?| $1
    /// ```
    pub fn json_has_any_key(&self, keys: &[&str]) -> Condition {
        Condition::Postgres(PgCondition::JsonHasAnyKey(
            self.name,
            keys.iter().map(|s| s.to_string()).collect(),
        ))
    }

    /// JSONB all keys exist: `column ?& keys`.
    ///
    /// ```ignore
    /// User::metadata.json_has_all_keys(&["name", "email"])
    /// // → "metadata" ?& $1
    /// ```
    pub fn json_has_all_keys(&self, keys: &[&str]) -> Condition {
        Condition::Postgres(PgCondition::JsonHasAllKeys(
            self.name,
            keys.iter().map(|s| s.to_string()).collect(),
        ))
    }

    /// JSONB path match: `column @? path`.
    ///
    /// Tests whether the jsonpath expression returns any item.
    ///
    /// ```ignore
    /// User::metadata.json_path_match("$.tags[*] ? (@ == \"rust\")")
    /// // → "metadata" @? $1
    /// ```
    pub fn json_path_match(&self, path: &str) -> Condition {
        Condition::Postgres(PgCondition::JsonPathMatch(self.name, path.to_owned()))
    }

    /// JSONB path predicate: `column @@ path`.
    ///
    /// Tests whether the jsonpath predicate holds for the whole document.
    ///
    /// ```ignore
    /// User::metadata.json_path_test("$.active == true")
    /// // → "metadata" @@ $1
    /// ```
    pub fn json_path_test(&self, path: &str) -> Condition {
        Condition::Postgres(PgCondition::JsonPathTest(self.name, path.to_owned()))
    }
}

/// A JSONB nested path access expression: `"column" #>> ARRAY[...]`.
///
/// Returned by [`Column<M, serde_json::Value>::json_get_path()`].
/// The path is passed as a bound parameter array — safe for user-supplied input.
///
/// ```ignore
/// User::metadata.json_get_path(&["address", "city"]).eq("Paris")
/// // → "metadata" #>> $1 = $2
/// ```
#[cfg(feature = "postgres")]
pub struct JsonPathExpr {
    column: &'static str,
    path: Vec<String>,
}

#[cfg(feature = "postgres")]
impl JsonPathExpr {
    /// `"column" #>> ? = ?`
    pub fn eq(&self, val: impl IntoValue) -> Condition {
        Condition::Raw(
            format!("{} #>> ? = ?", crate::ident::qi(self.column)),
            vec![
                crate::value::Value::ArrayString(self.path.clone()),
                val.into_value(),
            ],
        )
    }

    /// `"column" #>> ? != ?`
    pub fn neq(&self, val: impl IntoValue) -> Condition {
        Condition::Raw(
            format!("{} #>> ? != ?", crate::ident::qi(self.column)),
            vec![
                crate::value::Value::ArrayString(self.path.clone()),
                val.into_value(),
            ],
        )
    }

    /// `"column" #>> ? IS NULL`
    pub fn is_null(&self) -> Condition {
        Condition::Raw(
            format!("{} #>> ? IS NULL", crate::ident::qi(self.column)),
            vec![crate::value::Value::ArrayString(self.path.clone())],
        )
    }

    /// `"column" #>> ? IS NOT NULL`
    pub fn is_not_null(&self) -> Condition {
        Condition::Raw(
            format!("{} #>> ? IS NOT NULL", crate::ident::qi(self.column)),
            vec![crate::value::Value::ArrayString(self.path.clone())],
        )
    }

    /// `"column" #>> ? LIKE ?` — raw pattern, wildcards not escaped.
    pub fn like(&self, pattern: &str) -> Condition {
        Condition::Raw(
            format!("{} #>> ? LIKE ? ESCAPE '\\'", crate::ident::qi(self.column)),
            vec![
                crate::value::Value::ArrayString(self.path.clone()),
                crate::value::Value::String(pattern.to_owned()),
            ],
        )
    }

    /// `"column" #>> ? ILIKE ?` — case-insensitive, raw pattern.
    pub fn ilike(&self, pattern: &str) -> Condition {
        Condition::Raw(
            format!("{} #>> ? ILIKE ?", crate::ident::qi(self.column)),
            vec![
                crate::value::Value::ArrayString(self.path.clone()),
                crate::value::Value::String(pattern.to_owned()),
            ],
        )
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

    /// Scalar equals any array element: `val = ANY(column)`.
    ///
    /// ```ignore
    /// Post::scores.array_any_eq(10i32)
    /// // → $1 = ANY("scores")
    /// ```
    pub fn array_any_eq(&self, val: impl IntoValue) -> Condition {
        Condition::Postgres(PgCondition::ArrayAnyEq(self.name, val.into_value()))
    }

    /// Scalar equals all array elements: `val = ALL(column)`.
    ///
    /// ```ignore
    /// Post::scores.array_all_eq(10i32)
    /// // → $1 = ALL("scores")
    /// ```
    pub fn array_all_eq(&self, val: impl IntoValue) -> Condition {
        Condition::Postgres(PgCondition::ArrayAllEq(self.name, val.into_value()))
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
