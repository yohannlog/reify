use crate::query::Expr;
use crate::value::Value;

/// A single filter condition, produced by `Column` methods.
#[derive(Debug, Clone)]
pub enum Condition {
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
    /// Case-insensitive LIKE (PostgreSQL `ILIKE`).
    #[cfg(feature = "postgres")]
    ILike(&'static str, String),
    /// Range contains element: `column @> value` (PostgreSQL).
    #[cfg(feature = "postgres")]
    RangeContains(&'static str, Value),
    /// Range is contained by another range: `column <@ value` (PostgreSQL).
    #[cfg(feature = "postgres")]
    RangeContainedBy(&'static str, Value),
    /// Ranges overlap: `column && value` (PostgreSQL).
    #[cfg(feature = "postgres")]
    RangeOverlaps(&'static str, Value),
    /// Range strictly left of: `column << value` (PostgreSQL).
    #[cfg(feature = "postgres")]
    RangeLeftOf(&'static str, Value),
    /// Range strictly right of: `column >> value` (PostgreSQL).
    #[cfg(feature = "postgres")]
    RangeRightOf(&'static str, Value),
    /// Range is adjacent to: `column -|- value` (PostgreSQL).
    #[cfg(feature = "postgres")]
    RangeAdjacent(&'static str, Value),
    /// Range is not empty: `NOT isempty(column)` (PostgreSQL).
    #[cfg(feature = "postgres")]
    RangeIsEmpty(&'static str),
    /// JSONB field access: `column->key` (PostgreSQL).
    #[cfg(feature = "postgres")]
    JsonGet(&'static str, String),
    /// JSONB contains: `column @> value` (PostgreSQL).
    #[cfg(feature = "postgres")]
    JsonContains(&'static str, Value),
    /// JSONB key exists: `column ? key` (PostgreSQL).
    #[cfg(feature = "postgres")]
    JsonHasKey(&'static str, String),
    /// Array contains element: `column @> ARRAY[value]` (PostgreSQL).
    #[cfg(feature = "postgres")]
    ArrayContains(&'static str, Value),
    /// Array is contained by: `column <@ ARRAY[...]` (PostgreSQL).
    #[cfg(feature = "postgres")]
    ArrayContainedBy(&'static str, Value),
    /// Arrays overlap: `column && ARRAY[...]` (PostgreSQL).
    #[cfg(feature = "postgres")]
    ArrayOverlaps(&'static str, Value),
    Logical(LogicalOp),
    /// Aggregate comparison for HAVING: e.g. `COUNT(*) > 5`.
    AggregateGt(Expr, Value),
    /// `COUNT(*) < 5`
    AggregateLt(Expr, Value),
    /// `COUNT(*) >= 5`
    AggregateGte(Expr, Value),
    /// `COUNT(*) <= 5`
    AggregateLte(Expr, Value),
    /// `COUNT(*) = 5`
    AggregateEq(Expr, Value),
    /// `col IN (SELECT ...)` — subquery filter.
    InSubquery(&'static str, String, Vec<Value>),
    /// Raw SQL condition with bound parameters.
    ///
    /// Used internally for row-value comparisons in cursor pagination
    /// (e.g. `(created_at, id) < (?, ?)`).
    Raw(String, Vec<Value>),
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
