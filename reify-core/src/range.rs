//! PostgreSQL range type support.
//!
//! Maps to `int4range`, `int8range`, `tsrange`, `tstzrange`, `daterange`.
//!
//! ```ignore
//! use reify::range::{Range, Bound};
//!
//! // [10, 20)
//! let r = Range::new(Bound::Inclusive(10i32), Bound::Exclusive(20i32));
//!
//! // Unbounded lower: (,100]
//! let r = Range::new(Bound::Unbounded, Bound::Inclusive(100i64));
//!
//! // Empty range
//! let r = Range::<i32>::empty();
//! ```

use std::fmt;

use crate::value::{IntoValue, Value};

/// A bound of a range (lower or upper).
#[derive(Debug, Clone, PartialEq)]
pub enum Bound<T> {
    /// `[value` or `value]`
    Inclusive(T),
    /// `(value` or `value)`
    Exclusive(T),
    /// Unbounded (extends to infinity).
    Unbounded,
}

/// A PostgreSQL range value, generic over the element type.
#[derive(Debug, Clone, PartialEq)]
pub enum Range<T> {
    /// An empty range (contains no elements).
    Empty,
    /// A non-empty range with lower and upper bounds.
    Nonempty(Bound<T>, Bound<T>),
}

impl<T> Range<T> {
    /// Create a non-empty range with the given bounds.
    pub fn new(lower: Bound<T>, upper: Bound<T>) -> Self {
        Range::Nonempty(lower, upper)
    }

    /// Create an empty range.
    pub fn empty() -> Self {
        Range::Empty
    }

    /// `[lower, upper)` — the most common PostgreSQL canonical form.
    pub fn closed_open(lower: T, upper: T) -> Self {
        Range::Nonempty(Bound::Inclusive(lower), Bound::Exclusive(upper))
    }

    /// `[lower, upper]`
    pub fn closed(lower: T, upper: T) -> Self {
        Range::Nonempty(Bound::Inclusive(lower), Bound::Inclusive(upper))
    }

    /// `(lower, upper)`
    pub fn open(lower: T, upper: T) -> Self {
        Range::Nonempty(Bound::Exclusive(lower), Bound::Exclusive(upper))
    }

    /// `[lower, ∞)`
    pub fn from(lower: T) -> Self {
        Range::Nonempty(Bound::Inclusive(lower), Bound::Unbounded)
    }

    /// `(∞, upper]`
    pub fn to(upper: T) -> Self {
        Range::Nonempty(Bound::Unbounded, Bound::Inclusive(upper))
    }

    /// Returns `true` if this is an empty range.
    pub fn is_empty(&self) -> bool {
        matches!(self, Range::Empty)
    }

    /// Get the lower bound, if non-empty.
    pub fn lower(&self) -> Option<&Bound<T>> {
        match self {
            Range::Nonempty(lower, _) => Some(lower),
            Range::Empty => None,
        }
    }

    /// Get the upper bound, if non-empty.
    pub fn upper(&self) -> Option<&Bound<T>> {
        match self {
            Range::Nonempty(_, upper) => Some(upper),
            Range::Empty => None,
        }
    }
}

impl<T: fmt::Display> fmt::Display for Range<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Range::Empty => write!(f, "empty"),
            Range::Nonempty(lower, upper) => {
                match lower {
                    Bound::Inclusive(v) => write!(f, "[{v}")?,
                    Bound::Exclusive(v) => write!(f, "({v}")?,
                    Bound::Unbounded => write!(f, "(")?,
                }
                write!(f, ",")?;
                match upper {
                    Bound::Inclusive(v) => write!(f, "{v}]"),
                    Bound::Exclusive(v) => write!(f, "{v})"),
                    Bound::Unbounded => write!(f, ")"),
                }
            }
        }
    }
}

// ── IntoValue impls ────────────────────────────────────────────────

/// Marker trait for types that can be used as range element types.
pub trait RangeElement: IntoValue + Clone + fmt::Debug + fmt::Display + 'static {}

impl RangeElement for i32 {}
impl RangeElement for i64 {}

#[cfg(feature = "postgres")]
impl RangeElement for chrono::NaiveDateTime {}
#[cfg(feature = "postgres")]
impl RangeElement for chrono::DateTime<chrono::Utc> {}
#[cfg(any(feature = "postgres", feature = "mysql"))]
impl RangeElement for chrono::NaiveDate {}

// Without the postgres feature, ranges serialize to their text representation.
// With postgres, each concrete type maps to its native Value variant.

#[cfg(not(feature = "postgres"))]
impl<T: RangeElement> IntoValue for Range<T> {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for Range<i32> {
    fn into_value(self) -> Value {
        Value::Int4Range(self)
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for Range<i64> {
    fn into_value(self) -> Value {
        Value::Int8Range(self)
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for Range<chrono::NaiveDateTime> {
    fn into_value(self) -> Value {
        Value::TsRange(self)
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for Range<chrono::DateTime<chrono::Utc>> {
    fn into_value(self) -> Value {
        Value::TstzRange(self)
    }
}

#[cfg(feature = "postgres")]
impl IntoValue for Range<chrono::NaiveDate> {
    fn into_value(self) -> Value {
        Value::DateRange(self)
    }
}
