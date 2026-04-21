//! Shared utilities used by every database adapter
//! (`reify-postgres`, `reify-mysql`, `reify-sqlite`).
//!
//! Factoring these out of the adapter crates keeps dialect-specific rewriting
//! and savepoint naming in one place so fixes (e.g. quote-awareness) apply
//! uniformly.

use std::sync::atomic::{AtomicU64, Ordering};

// ── Savepoint naming ────────────────────────────────────────────────

/// Monotonically-increasing counter yielding unique `sp_<n>` names for
/// nested transactions on a shared connection. All three adapters use the
/// same pattern; this factors it out.
#[derive(Debug, Default)]
pub struct SavepointCounter(AtomicU64);

impl SavepointCounter {
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Return the next unique savepoint identifier, e.g. `"sp_0"`, `"sp_1"`, …
    pub fn next_name(&self) -> String {
        let n = self.0.fetch_add(1, Ordering::Relaxed);
        format!("sp_{n}")
    }
}

// ── Quote-aware SQL rewriting ───────────────────────────────────────

/// Copy a single-quoted SQL string literal from `bytes` starting at `i` into
/// `out`, honouring `''` escapes. Returns the index after the closing quote.
///
/// Panics if `i` does not point to a `'` byte.
fn copy_string_literal(bytes: &[u8], mut i: usize, out: &mut String) -> usize {
    debug_assert!(bytes[i] == b'\'');
    out.push('\'');
    i += 1;
    while i < bytes.len() {
        let b = bytes[i];
        out.push(b as char);
        if b == b'\'' {
            i += 1;
            if i < bytes.len() && bytes[i] == b'\'' {
                out.push('\'');
                i += 1;
            } else {
                break; // end of literal
            }
        } else {
            i += 1;
        }
    }
    i
}

/// Rewrite PostgreSQL-style `$N` placeholders to MySQL/SQLite `?`
/// placeholders, leaving `'…'` string literals untouched.
///
/// Operates on raw bytes: all characters we care about (`$`, `'`, digits and
/// `?`) are ASCII, so byte-level scanning is safe and avoids the overhead
/// of `chars()` / UTF-8 decoding.
pub fn rewrite_placeholders_to_question(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            i = copy_string_literal(bytes, i, &mut out);
        } else if b == b'$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            // consume `$` + digits
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            out.push('?');
        } else {
            out.push(b as char);
            i += 1;
        }
    }
    out
}

/// Rewrite ANSI-style `"ident"` double-quoted identifiers to MySQL backtick
/// style `` `ident` ``. Single-quoted string literals are left untouched, so
/// a literal like `'"abc"'` is preserved intact.
///
/// Escaped double quotes (`""`) inside an identifier become a single `"`
/// inside backticks.
pub fn rewrite_double_quoted_idents_to_backticks(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            i = copy_string_literal(bytes, i, &mut out);
        } else if b == b'"' {
            out.push('`');
            i += 1;
            while i < bytes.len() {
                let b2 = bytes[i];
                if b2 == b'"' {
                    i += 1;
                    if i < bytes.len() && bytes[i] == b'"' {
                        out.push('"');
                        i += 1;
                    } else {
                        out.push('`');
                        break;
                    }
                } else {
                    out.push(b2 as char);
                    i += 1;
                }
            }
        } else {
            out.push(b as char);
            i += 1;
        }
    }
    out
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholders_rewritten_outside_strings() {
        let sql = "SELECT $1, $2 FROM t WHERE x = $3";
        assert_eq!(
            rewrite_placeholders_to_question(sql),
            "SELECT ?, ? FROM t WHERE x = ?"
        );
    }

    #[test]
    fn placeholders_preserved_inside_string_literal() {
        let sql = "SELECT '$1', $2 FROM t";
        assert_eq!(
            rewrite_placeholders_to_question(sql),
            "SELECT '$1', ? FROM t"
        );
    }

    #[test]
    fn escaped_single_quote_handled() {
        let sql = "SELECT 'it''s $1', $2 FROM t";
        assert_eq!(
            rewrite_placeholders_to_question(sql),
            "SELECT 'it''s $1', ? FROM t"
        );
    }

    #[test]
    fn quotes_rewritten_outside_strings() {
        let sql = r#"SELECT "id", "name" FROM "users""#;
        assert_eq!(
            rewrite_double_quoted_idents_to_backticks(sql),
            "SELECT `id`, `name` FROM `users`"
        );
    }

    #[test]
    fn quotes_preserved_inside_string_literal() {
        let sql = r#"SELECT "id", '"abc"' FROM "t""#;
        assert_eq!(
            rewrite_double_quoted_idents_to_backticks(sql),
            "SELECT `id`, '\"abc\"' FROM `t`"
        );
    }

    #[test]
    fn doubled_quote_inside_identifier_preserved() {
        let sql = r#"SELECT "na""me" FROM t"#;
        assert_eq!(
            rewrite_double_quoted_idents_to_backticks(sql),
            "SELECT `na\"me` FROM t"
        );
    }

    #[test]
    fn savepoint_counter_is_monotonic() {
        let c = SavepointCounter::new();
        assert_eq!(c.next_name(), "sp_0");
        assert_eq!(c.next_name(), "sp_1");
        assert_eq!(c.next_name(), "sp_2");
    }
}
