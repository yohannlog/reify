use crate::value::Value;

/// A built SQL query with its parameter list.
///
/// Returned by `build()` methods on query builders. Provides convenience
/// methods for debugging, dialect rewriting, and destructuring.
///
/// Implements `From<BuiltQuery> for (String, Vec<Value>)` for backward
/// compatibility with code that expects a tuple.
#[derive(Debug, Clone)]
pub struct BuiltQuery {
    /// The SQL string with `?` placeholders.
    pub sql: String,
    /// Ordered parameter values matching the `?` placeholders.
    pub params: Vec<Value>,
}

impl BuiltQuery {
    /// Create a new `BuiltQuery`.
    pub fn new(sql: String, params: Vec<Value>) -> Self {
        Self { sql, params }
    }

    /// Rewrite `?` placeholders to PostgreSQL-style `$1, $2, …`.
    #[cfg(feature = "postgres")]
    pub fn rewrite_pg(&self) -> BuiltQuery {
        let sql = crate::query::rewrite_placeholders_pg(&self.sql);
        BuiltQuery {
            sql,
            params: self.params.clone(),
        }
    }

    /// Debug-friendly display with params inlined (for logging, not execution).
    ///
    /// Forward single-pass substitution: O(n + m) in the length of the SQL
    /// and the number of parameters. The previous `rfind`-per-param approach
    /// was O(n · m) and noticeably slow on large queries; this version scans
    /// the SQL once and replaces `?` placeholders in order.
    ///
    /// Intended for logging only — the output is not safe to execute
    /// (parameter literals are subject to the caveats of
    /// [`Value::to_sql_literal`]).
    pub fn display_debug(&self) -> String {
        let bytes = self.sql.as_bytes();
        let mut out = String::with_capacity(self.sql.len() + self.params.len() * 8);
        let mut start = 0usize;
        let mut param_idx = 0usize;
        for (i, &b) in bytes.iter().enumerate() {
            if b == b'?' {
                // SAFETY: `?` (0x3F) is ASCII and cannot be a UTF-8 continuation
                // byte, so splitting at `i` always lands on a char boundary.
                out.push_str(unsafe { std::str::from_utf8_unchecked(&bytes[start..i]) });
                if let Some(param) = self.params.get(param_idx) {
                    out.push_str(&param.to_sql_literal());
                    param_idx += 1;
                } else {
                    out.push('?');
                }
                start = i + 1;
            }
        }
        out.push_str(unsafe { std::str::from_utf8_unchecked(&bytes[start..]) });
        out
    }

    /// Destructure into a tuple (backward compat).
    pub fn into_parts(self) -> (String, Vec<Value>) {
        (self.sql, self.params)
    }
}

impl From<BuiltQuery> for (String, Vec<Value>) {
    fn from(q: BuiltQuery) -> Self {
        (q.sql, q.params)
    }
}

impl From<(String, Vec<Value>)> for BuiltQuery {
    fn from((sql, params): (String, Vec<Value>)) -> Self {
        Self { sql, params }
    }
}

impl std::fmt::Display for BuiltQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.sql)
    }
}
