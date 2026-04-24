//! Time interval type.

use std::fmt;
use std::ops::{Add, Neg, Sub};

/// A time interval representing a duration.
///
/// Maps to PostgreSQL `INTERVAL` type. Internally stored as months, days,
/// and microseconds to match PostgreSQL's representation.
///
/// # Examples
///
/// ```
/// use reify_core::types::Interval;
///
/// let duration = Interval::days(30) + Interval::hours(12);
/// assert_eq!(duration.days(), 30);
/// assert_eq!(duration.microseconds(), 12 * 3600 * 1_000_000);
///
/// let period = Interval::months(6);
/// assert_eq!(period.months(), 6);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Interval {
    /// Number of months (can be negative).
    months: i32,
    /// Number of days (can be negative).
    days: i32,
    /// Number of microseconds (can be negative).
    microseconds: i64,
}

impl Interval {
    /// Create a new interval from components.
    #[inline]
    pub const fn new(months: i32, days: i32, microseconds: i64) -> Self {
        Self {
            months,
            days,
            microseconds,
        }
    }

    /// Create a zero interval.
    #[inline]
    pub const fn zero() -> Self {
        Self {
            months: 0,
            days: 0,
            microseconds: 0,
        }
    }

    /// Create an interval of N years.
    #[inline]
    pub const fn years(n: i32) -> Self {
        Self {
            months: n * 12,
            days: 0,
            microseconds: 0,
        }
    }

    /// Create an interval of N months.
    #[inline]
    pub const fn from_months(n: i32) -> Self {
        Self {
            months: n,
            days: 0,
            microseconds: 0,
        }
    }

    /// Create an interval of N days.
    #[inline]
    pub const fn from_days(n: i32) -> Self {
        Self {
            months: 0,
            days: n,
            microseconds: 0,
        }
    }

    /// Create an interval of N hours.
    #[inline]
    pub const fn hours(n: i64) -> Self {
        Self {
            months: 0,
            days: 0,
            microseconds: n * 3_600_000_000,
        }
    }

    /// Create an interval of N minutes.
    #[inline]
    pub const fn minutes(n: i64) -> Self {
        Self {
            months: 0,
            days: 0,
            microseconds: n * 60_000_000,
        }
    }

    /// Create an interval of N seconds.
    #[inline]
    pub const fn seconds(n: i64) -> Self {
        Self {
            months: 0,
            days: 0,
            microseconds: n * 1_000_000,
        }
    }

    /// Create an interval of N milliseconds.
    #[inline]
    pub const fn milliseconds(n: i64) -> Self {
        Self {
            months: 0,
            days: 0,
            microseconds: n * 1_000,
        }
    }

    /// Create an interval of N microseconds.
    #[inline]
    pub const fn from_microseconds(n: i64) -> Self {
        Self {
            months: 0,
            days: 0,
            microseconds: n,
        }
    }

    /// Get the months component.
    #[inline]
    pub const fn months(&self) -> i32 {
        self.months
    }

    /// Get the days component.
    #[inline]
    pub const fn days(&self) -> i32 {
        self.days
    }

    /// Get the microseconds component.
    #[inline]
    pub const fn microseconds(&self) -> i64 {
        self.microseconds
    }

    /// Returns `true` if this interval is zero.
    #[inline]
    pub const fn is_zero(&self) -> bool {
        self.months == 0 && self.days == 0 && self.microseconds == 0
    }

    /// Negate the interval.
    #[inline]
    pub const fn negate(&self) -> Self {
        Self {
            months: -self.months,
            days: -self.days,
            microseconds: -self.microseconds,
        }
    }
}

impl Add for Interval {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            months: self.months + rhs.months,
            days: self.days + rhs.days,
            microseconds: self.microseconds + rhs.microseconds,
        }
    }
}

impl Sub for Interval {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            months: self.months - rhs.months,
            days: self.days - rhs.days,
            microseconds: self.microseconds - rhs.microseconds,
        }
    }
}

impl Neg for Interval {
    type Output = Self;

    fn neg(self) -> Self::Output {
        self.negate()
    }
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();

        if self.months != 0 {
            let years = self.months / 12;
            let months = self.months % 12;
            if years != 0 {
                parts.push(format!(
                    "{} year{}",
                    years,
                    if years.abs() == 1 { "" } else { "s" }
                ));
            }
            if months != 0 {
                parts.push(format!(
                    "{} mon{}",
                    months,
                    if months.abs() == 1 { "" } else { "s" }
                ));
            }
        }

        if self.days != 0 {
            parts.push(format!(
                "{} day{}",
                self.days,
                if self.days.abs() == 1 { "" } else { "s" }
            ));
        }

        if self.microseconds != 0 || parts.is_empty() {
            let total_secs = self.microseconds / 1_000_000;
            let micros = (self.microseconds % 1_000_000).abs();
            let hours = total_secs / 3600;
            let mins = (total_secs % 3600) / 60;
            let secs = total_secs % 60;

            if micros == 0 {
                parts.push(format!("{:02}:{:02}:{:02}", hours, mins.abs(), secs.abs()));
            } else {
                parts.push(format!(
                    "{:02}:{:02}:{:02}.{:06}",
                    hours,
                    mins.abs(),
                    secs.abs(),
                    micros
                ));
            }
        }

        write!(f, "{}", parts.join(" "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_constructors() {
        assert_eq!(Interval::years(2).months(), 24);
        assert_eq!(Interval::from_months(6).months(), 6);
        assert_eq!(Interval::from_days(30).days(), 30);
        assert_eq!(Interval::hours(2).microseconds(), 2 * 3_600_000_000);
        assert_eq!(Interval::minutes(30).microseconds(), 30 * 60_000_000);
        assert_eq!(Interval::seconds(45).microseconds(), 45_000_000);
    }

    #[test]
    fn test_interval_zero() {
        let zero = Interval::zero();
        assert!(zero.is_zero());
        assert_eq!(zero.months(), 0);
        assert_eq!(zero.days(), 0);
        assert_eq!(zero.microseconds(), 0);
    }

    #[test]
    fn test_interval_add() {
        let a = Interval::from_days(10) + Interval::hours(5);
        assert_eq!(a.days(), 10);
        assert_eq!(a.microseconds(), 5 * 3_600_000_000);

        let b = Interval::from_months(3) + Interval::from_days(15);
        assert_eq!(b.months(), 3);
        assert_eq!(b.days(), 15);
    }

    #[test]
    fn test_interval_sub() {
        let a = Interval::from_days(30) - Interval::from_days(10);
        assert_eq!(a.days(), 20);
    }

    #[test]
    fn test_interval_neg() {
        let a = Interval::from_days(10);
        let b = -a;
        assert_eq!(b.days(), -10);
    }

    #[test]
    fn test_interval_display() {
        assert_eq!(Interval::zero().to_string(), "00:00:00");
        assert_eq!(Interval::from_days(5).to_string(), "5 days");
        assert_eq!(Interval::hours(2).to_string(), "02:00:00");
        assert_eq!(Interval::years(1).to_string(), "1 year");
        assert_eq!(Interval::from_months(14).to_string(), "1 year 2 mons");
    }

    #[test]
    fn test_interval_new() {
        let i = Interval::new(2, 5, 3_600_000_000);
        assert_eq!(i.months(), 2);
        assert_eq!(i.days(), 5);
        assert_eq!(i.microseconds(), 3_600_000_000);
    }
}
