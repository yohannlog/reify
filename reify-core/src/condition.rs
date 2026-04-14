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
    /// Used internally for row-value comparisons in cursor pagination
    /// (e.g. `(created_at, id) < (?, ?)`).
    Raw(String, Vec<Value>),
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
    /// JSONB key exists: `column ? key`.
    JsonHasKey(&'static str, String),
    /// Array contains element: `column @> ARRAY[value]`.
    ArrayContains(&'static str, Value),
    /// Array is contained by: `column <@ ARRAY[...]`.
    ArrayContainedBy(&'static str, Value),
    /// Arrays overlap: `column && ARRAY[...]`.
    ArrayOverlaps(&'static str, Value),
}

#[derive(Debug, Clone)]
pub enum LogicalOp {
    And(Vec<Condition>),
    Or(Vec<Condition>),
}

impl Condition {
    pub fn and(self, other: Condition) -> Condition {
        Condition::Logical(LogicalOp::And(vec![self, other]))
    }

    pub fn or(self, other: Condition) -> Condition {
        Condition::Logical(LogicalOp::Or(vec![self, other]))
    }
}
