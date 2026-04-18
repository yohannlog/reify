use crate::query::Expr;
use crate::value::Value;

/// A single filter condition, produced by `Column` methods.
///
/// Universal operators live directly on this enum for ergonomics.
/// PostgreSQL-specific and aggregate conditions are nested in sub-enums
/// to keep the top-level variants manageable.
#[derive(Debug, Clone)]
pub enum Condition {
    // ── Universal operators ─────────────────────────────────────────
    Eq(&'static str, Value),
    Neq(&'static str, Value),
    Gt(&'static str, Value),
    Lt(&'static str, Value),
    Gte(&'static str, Value),
    Lte(&'static str, Value),
    Between(&'static str, Value, Value),
    Like(&'static str, String),
    In(&'static str, Vec<Value>),
    IsNull(&'static str),
    IsNotNull(&'static str),
    /// `col IN (SELECT ...)` — subquery filter.
    InSubquery(&'static str, String, Vec<Value>),

    // ── Logical combinators ─────────────────────────────────────────
    Logical(LogicalOp),

    // ── Aggregate conditions (HAVING) ───────────────────────────────
    Aggregate(AggregateCondition),

    // ── PostgreSQL-specific ─────────────────────────────────────────
    #[cfg(feature = "postgres")]
    Postgres(PgCondition),

    // ── Escape hatch ────────────────────────────────────────────────
    /// Raw SQL condition with bound parameters.
    ///
    /// **Do not construct this variant directly from user code** —
    /// `#[non_exhaustive]` blocks external tuple construction, so an
    /// attacker-controlled `String` cannot reach the SQL verbatim. The
    /// safe public entry point is [`Condition::raw`], which only
    /// accepts a [`RawFragment`] built from a `&'static str` literal.
    ///
    /// Used internally for row-value comparisons in cursor pagination
    /// (e.g. `(created_at, id) < (?, ?)`) and for JSON path operators.
    #[non_exhaustive]
    Raw(String, Vec<Value>),
}

/// Statically-known SQL fragment with runtime-bound parameters.
///
/// The only public path to [`Condition::raw`]. By forcing `sql:
/// &'static str`, runtime SQL string interpolation becomes impossible
/// at the type level — user input can only travel through `params`,
/// which are bound as prepared-statement arguments, never spliced into
/// the SQL text.
///
/// ```no_run
/// use reify_core::condition::{Condition, RawFragment};
/// use reify_core::value::Value;
///
/// // OK — SQL literal is &'static str.
/// let frag = RawFragment::new("\"created_at\" < NOW() - INTERVAL '1 day'", vec![]);
/// let cond = Condition::raw(frag);
/// # let _ = cond;
///
/// // Compile error — runtime-built String does not coerce to &'static str.
/// // let evil = format!("1=1; DROP TABLE users; --");
/// // RawFragment::new(&evil, vec![]); // ❌
/// ```
#[derive(Debug, Clone)]
pub struct RawFragment {
    sql: &'static str,
    params: Vec<Value>,
}

impl RawFragment {
    /// Build a fragment from a compile-time SQL literal and runtime
    /// parameters. `sql` is `&'static str`, so only string literals (or
    /// explicit `Box::leak`ed statics chosen by the programmer) can be
    /// passed — runtime-formatted strings do not coerce.
    pub const fn new(sql: &'static str, params: Vec<Value>) -> Self {
        Self { sql, params }
    }

    /// The static SQL template.
    pub fn sql(&self) -> &'static str {
        self.sql
    }

    /// The bound parameters.
    pub fn params(&self) -> &[Value] {
        &self.params
    }
}

/// Aggregate comparison for HAVING clauses.
#[derive(Debug, Clone)]
pub enum AggregateCondition {
    /// `expr > val`
    Gt(Expr, Value),
    /// `expr < val`
    Lt(Expr, Value),
    /// `expr >= val`
    Gte(Expr, Value),
    /// `expr <= val`
    Lte(Expr, Value),
    /// `expr = val`
    Eq(Expr, Value),
}

/// PostgreSQL-specific conditions (ILIKE, range ops, JSONB, arrays).
#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
pub enum PgCondition {
    /// Case-insensitive LIKE (`ILIKE`).
    ILike(&'static str, String),
    /// Range contains element: `column @> value`.
    RangeContains(&'static str, Value),
    /// Range is contained by another range: `column <@ value`.
    RangeContainedBy(&'static str, Value),
    /// Ranges overlap: `column && value`.
    RangeOverlaps(&'static str, Value),
    /// Range strictly left of: `column << value`.
    RangeLeftOf(&'static str, Value),
    /// Range strictly right of: `column >> value`.
    RangeRightOf(&'static str, Value),
    /// Range is adjacent to: `column -|- value`.
    RangeAdjacent(&'static str, Value),
    /// Range is not empty: `isempty(column)`.
    RangeIsEmpty(&'static str),
    /// JSONB contains: `column @> value`.
    JsonContains(&'static str, Value),
    /// JSONB is contained by: `column <@ value`.
    JsonContainedBy(&'static str, Value),
    /// JSONB key exists: `column ? key`.
    JsonHasKey(&'static str, String),
    /// JSONB any key exists: `column ?| keys`.
    JsonHasAnyKey(&'static str, Vec<String>),
    /// JSONB all keys exist: `column ?& keys`.
    JsonHasAllKeys(&'static str, Vec<String>),
    /// JSONB concatenation: `column || value`.
    JsonConcat(&'static str, Value),
    /// JSONB key deletion: `column - key`.
    JsonDeleteKey(&'static str, String),
    /// JSONB path access as text: `column #>> path`.
    JsonPathGetText(&'static str, Vec<String>),
    /// JSONB path access as jsonb: `column #> path`.
    JsonPathGet(&'static str, Vec<String>),
    /// JSONB path match: `column @? path`.
    JsonPathMatch(&'static str, String),
    /// JSONB path predicate: `column @@ path`.
    JsonPathTest(&'static str, String),
    /// Array contains element: `column @> ARRAY[value]`.
    ArrayContains(&'static str, Value),
    /// Array is contained by: `column <@ ARRAY[...]`.
    ArrayContainedBy(&'static str, Value),
    /// Arrays overlap: `column && ARRAY[...]`.
    ArrayOverlaps(&'static str, Value),
    /// Scalar equals any array element: `? = ANY(column)`.
    ArrayAnyEq(&'static str, Value),
    /// Scalar equals all array elements: `? = ALL(column)`.
    ArrayAllEq(&'static str, Value),
}

#[derive(Debug, Clone)]
pub enum LogicalOp {
    And(Vec<Condition>),
    Or(Vec<Condition>),
}

impl Condition {
    /// Combine two conditions with `AND`, flattening nested `And` chains.
    ///
    /// `A.and(B).and(C)` produces `And([A, B, C])` (a single flat list),
    /// not `And([And([A, B]), C])`. This keeps the rendered SQL shallow
    /// (`(A AND B AND C)`) and avoids one `Vec` allocation per chained call.
    pub fn and(self, other: Condition) -> Condition {
        match (self, other) {
            (Condition::Logical(LogicalOp::And(mut a)), Condition::Logical(LogicalOp::And(b))) => {
                a.extend(b);
                Condition::Logical(LogicalOp::And(a))
            }
            (Condition::Logical(LogicalOp::And(mut a)), other) => {
                a.push(other);
                Condition::Logical(LogicalOp::And(a))
            }
            (lhs, Condition::Logical(LogicalOp::And(b))) => {
                let mut v = Vec::with_capacity(b.len() + 1);
                v.push(lhs);
                v.extend(b);
                Condition::Logical(LogicalOp::And(v))
            }
            (lhs, rhs) => Condition::Logical(LogicalOp::And(vec![lhs, rhs])),
        }
    }

    /// Combine two conditions with `OR`, flattening nested `Or` chains.
    ///
    /// Mirrors [`and`](Self::and): `A.or(B).or(C)` → `Or([A, B, C])`.
    pub fn or(self, other: Condition) -> Condition {
        match (self, other) {
            (Condition::Logical(LogicalOp::Or(mut a)), Condition::Logical(LogicalOp::Or(b))) => {
                a.extend(b);
                Condition::Logical(LogicalOp::Or(a))
            }
            (Condition::Logical(LogicalOp::Or(mut a)), other) => {
                a.push(other);
                Condition::Logical(LogicalOp::Or(a))
            }
            (lhs, Condition::Logical(LogicalOp::Or(b))) => {
                let mut v = Vec::with_capacity(b.len() + 1);
                v.push(lhs);
                v.extend(b);
                Condition::Logical(LogicalOp::Or(v))
            }
            (lhs, rhs) => Condition::Logical(LogicalOp::Or(vec![lhs, rhs])),
        }
    }

    /// Build a raw-SQL condition from a [`RawFragment`].
    ///
    /// The **only** safe public constructor for [`Condition::Raw`].
    /// The fragment carries a `&'static str` SQL template, so no
    /// runtime-built string can reach the variant — user input is
    /// confined to the bound parameters.
    pub fn raw(frag: RawFragment) -> Condition {
        Condition::Raw(frag.sql.to_owned(), frag.params)
    }
}
