use super::select::SelectBuilder;
use crate::table::Table;

// ── Join types ──────────────────────────────────────────────────────

/// Kind of SQL JOIN.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
}

impl JoinKind {
    /// Returns the SQL keyword string for this join type (e.g. `"INNER JOIN"`).
    pub fn sql_keyword(self) -> &'static str {
        match self {
            JoinKind::Inner => "INNER JOIN",
            JoinKind::Left => "LEFT JOIN",
            JoinKind::Right => "RIGHT JOIN",
        }
    }
}

/// A single JOIN clause: kind + target table + ON condition.
#[derive(Debug, Clone)]
pub struct JoinClause {
    pub kind: JoinKind,
    /// SQL table name of the joined table.
    pub table: &'static str,
    /// Raw ON condition string, e.g. `"users.id = posts.user_id"`.
    pub on: String,
}

/// Backward-compatible type alias.
///
/// `JoinedSelectBuilder` has been unified into `SelectBuilder` — joins are
/// now a first-class field on `SelectBuilder`. This alias exists so that
/// downstream code mentioning the type still compiles.
#[deprecated(note = "Use SelectBuilder directly — joins are now built-in")]
pub type JoinedSelectBuilder<M> = SelectBuilder<M>;
