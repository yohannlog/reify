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
    pub fn rewrite_pg(&self) -> BuiltQuery {
        let sql = crate::query::rewrite_placeholders_pg(&self.sql);
        BuiltQuery {
            sql,
            params: self.params.clone(),
        }
    }

    /// Debug-friendly display with params inlined (for logging, not execution).
    pub fn display_debug(&self) -> String {
        let mut result = self.sql.clone();
        for param in self.params.iter().rev() {
            if let Some(pos) = result.rfind('?') {
                let literal = param.to_sql_literal();
                result.replace_range(pos..pos + 1, &literal);
            }
        }
        result
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
