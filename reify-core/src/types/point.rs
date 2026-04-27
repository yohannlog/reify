//! 2D geometric point type.

use std::fmt;
use std::str::FromStr;

/// A 2D point with `x` and `y` coordinates.
///
/// # Adapter support
///
/// - **PostgreSQL** — native `POINT` type (two `float8` in network byte
///   order), round-trips losslessly via the binary protocol.
/// - **MySQL** — bound as a `POINT(x y)` WKT string; reads currently
///   require explicit deserialisation (no auto-mapping from MySQL
///   `POINT` geometry).
/// - **SQLite** — bound as `TEXT` (no native geometry type).
///
/// The `Value::Point` variant is gated on the `postgres` feature; the
/// MySQL adapter encodes via `ST_GeomFromText`-compatible text so a
/// shared model can still bind a `Point` parameter when targeting
/// MySQL, but the read path cannot reconstruct a `Point` automatically.
///
/// # Examples
///
/// ```
/// use reify_core::types::Point;
///
/// let p = Point::new(3.0, 4.0);
/// assert_eq!(p.x(), 3.0);
/// assert_eq!(p.y(), 4.0);
///
/// let origin = Point::origin();
/// assert_eq!(origin.distance_to(&p), 5.0);
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    x: f64,
    y: f64,
}

impl Point {
    /// Create a new point at `(x, y)`.
    #[inline]
    pub const fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    /// Create a point at the origin `(0, 0)`.
    #[inline]
    pub const fn origin() -> Self {
        Self { x: 0.0, y: 0.0 }
    }

    /// Get the x coordinate.
    #[inline]
    pub const fn x(&self) -> f64 {
        self.x
    }

    /// Get the y coordinate.
    #[inline]
    pub const fn y(&self) -> f64 {
        self.y
    }

    /// Calculate the Euclidean distance to another point.
    #[inline]
    pub fn distance_to(&self, other: &Point) -> f64 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        (dx * dx + dy * dy).sqrt()
    }

    /// Translate the point by `(dx, dy)`.
    #[inline]
    pub const fn translate(&self, dx: f64, dy: f64) -> Self {
        Self {
            x: self.x + dx,
            y: self.y + dy,
        }
    }
}

impl Default for Point {
    fn default() -> Self {
        Self::origin()
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({},{})", self.x, self.y)
    }
}

/// Parse error for Point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsePointError(String);

impl fmt::Display for ParsePointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid point format: {}", self.0)
    }
}

impl std::error::Error for ParsePointError {}

impl FromStr for Point {
    type Err = ParsePointError;

    /// Parse a point from PostgreSQL format: `(x,y)` or `x,y`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let s = s.strip_prefix('(').unwrap_or(s);
        let s = s.strip_suffix(')').unwrap_or(s);

        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() != 2 {
            return Err(ParsePointError(format!(
                "expected 2 coordinates, got {}",
                parts.len()
            )));
        }

        let x = parts[0]
            .trim()
            .parse::<f64>()
            .map_err(|e| ParsePointError(format!("invalid x: {e}")))?;
        let y = parts[1]
            .trim()
            .parse::<f64>()
            .map_err(|e| ParsePointError(format!("invalid y: {e}")))?;

        Ok(Point::new(x, y))
    }
}

impl From<(f64, f64)> for Point {
    fn from((x, y): (f64, f64)) -> Self {
        Self::new(x, y)
    }
}

impl From<Point> for (f64, f64) {
    fn from(p: Point) -> Self {
        (p.x, p.y)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_new() {
        let p = Point::new(1.5, 2.5);
        assert_eq!(p.x(), 1.5);
        assert_eq!(p.y(), 2.5);
    }

    #[test]
    fn test_point_origin() {
        let p = Point::origin();
        assert_eq!(p.x(), 0.0);
        assert_eq!(p.y(), 0.0);
    }

    #[test]
    fn test_point_distance() {
        let a = Point::origin();
        let b = Point::new(3.0, 4.0);
        assert!((a.distance_to(&b) - 5.0).abs() < 1e-10);
    }

    #[test]
    fn test_point_translate() {
        let p = Point::new(1.0, 2.0);
        let q = p.translate(3.0, 4.0);
        assert_eq!(q.x(), 4.0);
        assert_eq!(q.y(), 6.0);
    }

    #[test]
    fn test_point_display() {
        let p = Point::new(1.5, 2.5);
        assert_eq!(p.to_string(), "(1.5,2.5)");
    }

    #[test]
    fn test_point_parse() {
        assert_eq!("(1.5,2.5)".parse::<Point>().unwrap(), Point::new(1.5, 2.5));
        assert_eq!("1.5,2.5".parse::<Point>().unwrap(), Point::new(1.5, 2.5));
        assert_eq!(
            "( 1.5 , 2.5 )".parse::<Point>().unwrap(),
            Point::new(1.5, 2.5)
        );
    }

    #[test]
    fn test_point_from_tuple() {
        let p: Point = (1.0, 2.0).into();
        assert_eq!(p, Point::new(1.0, 2.0));

        let t: (f64, f64) = p.into();
        assert_eq!(t, (1.0, 2.0));
    }
}
