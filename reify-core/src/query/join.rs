use super::select::SelectBuilder;

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
#[allow(dead_code)]
pub type JoinedSelectBuilder<M> = SelectBuilder<M>;

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_keyword_inner() {
        assert_eq!(JoinKind::Inner.sql_keyword(), "INNER JOIN");
    }

    #[test]
    fn sql_keyword_left() {
        assert_eq!(JoinKind::Left.sql_keyword(), "LEFT JOIN");
    }

    #[test]
    fn sql_keyword_right() {
        assert_eq!(JoinKind::Right.sql_keyword(), "RIGHT JOIN");
    }

    #[test]
    fn join_kind_is_copy_and_eq() {
        let k = JoinKind::Left;
        let k2 = k; // Copy
        assert_eq!(k, k2);
        assert_ne!(JoinKind::Inner, JoinKind::Right);
    }

    #[test]
    fn join_clause_clone_preserves_fields() {
        let c = JoinClause {
            kind: JoinKind::Left,
            table: "posts",
            on: r#""users"."id" = "posts"."user_id""#.to_string(),
        };
        let c2 = c.clone();
        assert_eq!(c2.kind, JoinKind::Left);
        assert_eq!(c2.table, "posts");
        assert!(c2.on.contains("posts"));
    }
}
