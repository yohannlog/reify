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

/// Scan `sql` and invoke `on_code(byte, output)` for every byte that is part
/// of ordinary SQL code (i.e. *not* inside a `'single-quoted'` string
/// literal), forwarding string-literal bytes verbatim.
///
/// Used by the MySQL adapter to rewrite `$N → ?` and `"ident" → `` `ident` ``
/// without corrupting literals like `'$1'` or `'"abc"'`.
///
/// PostgreSQL-style `E'…'` escape strings and `$tag$…$tag$` dollar-quoted
/// strings are not supported here because the MySQL adapter will never see
/// them — `build_pg()` only emits plain single-quoted string literals.
fn scan_with_string_awareness<F>(sql: &str, mut on_code: F) -> String
where
    F: FnMut(&str, &mut String, &mut std::str::Chars<'_>),
{
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars();
    while let Some(ch) = chars.clone().next() {
        if ch == '\'' {
            // Copy the string literal verbatim, honouring `''` escapes.
            chars.next();
            out.push('\'');
            loop {
                match chars.next() {
                    None => return out,
                    Some('\'') => {
                        // `''` inside a string literal is an escaped quote.
                        if chars.clone().next() == Some('\'') {
                            chars.next();
                            out.push_str("''");
                        } else {
                            out.push('\'');
                            break;
                        }
                    }
                    Some(other) => out.push(other),
                }
            }
        } else {
            // Slice from the current position of `chars` and let the callback
            // decide how many chars to consume via its own `Chars` iterator.
            let rest = chars.as_str();
            on_code(rest, &mut out, &mut chars);
        }
    }
    out
}

/// Rewrite PostgreSQL-style `$N` placeholders to MySQL/SQLite `?`
/// placeholders, leaving `'…'` string literals untouched.
pub fn rewrite_placeholders_to_question(sql: &str) -> String {
    scan_with_string_awareness(sql, |rest, out, chars| {
        let mut it = rest.chars();
        let ch = it.next().unwrap();
        if ch == '$' && it.clone().next().is_some_and(|c| c.is_ascii_digit()) {
            // consume `$` + digits
            chars.next();
            while let Some(c) = chars.clone().next() {
                if c.is_ascii_digit() {
                    chars.next();
                } else {
                    break;
                }
            }
            out.push('?');
        } else {
            chars.next();
            out.push(ch);
        }
    })
}

/// Rewrite ANSI-style `"ident"` double-quoted identifiers to MySQL backtick
/// style `` `ident` ``. Single-quoted string literals are left untouched, so
/// a literal like `'"abc"'` is preserved intact.
pub fn rewrite_double_quoted_idents_to_backticks(sql: &str) -> String {
    scan_with_string_awareness(sql, |rest, out, chars| {
        let mut it = rest.chars();
        let ch = it.next().unwrap();
        if ch == '"' {
            chars.next();
            out.push('`');
            loop {
                match chars.next() {
                    None => break,
                    Some('"') => {
                        // `""` inside an identifier is an escaped quote.
                        if chars.clone().next() == Some('"') {
                            chars.next();
                            out.push('"');
                        } else {
                            out.push('`');
                            break;
                        }
                    }
                    Some(inner) => out.push(inner),
                }
            }
        } else {
            chars.next();
            out.push(ch);
        }
    })
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
