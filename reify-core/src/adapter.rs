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
//
// All rewriters here scan the SQL as raw bytes — the structural
// characters we look at (`'`, `"`, `$`, `?`, ASCII digits) are all
// ASCII (< 0x80) and therefore can never appear as UTF-8 continuation
// bytes. Non-ASCII bytes are forwarded verbatim via bulk
// `push_str(from_utf8_unchecked(...))` of the surrounding run, which
// preserves the original UTF-8 encoding byte-for-byte.

/// Append the byte range `bytes[start..end]` to `out`.
///
/// The caller guarantees that `start` and `end` sit on UTF-8 char
/// boundaries — every split point used by the rewriters in this module
/// (and in `crate::query::rewrite_placeholders_pg`) is at an ASCII byte
/// (`'`, `"`, `$`, `?`) or at the end of input.
#[inline]
pub(crate) fn push_run(out: &mut String, bytes: &[u8], start: usize, end: usize) {
    debug_assert!(std::str::from_utf8(&bytes[start..end]).is_ok());
    // SAFETY: see module-level comment; split points are ASCII.
    out.push_str(unsafe { std::str::from_utf8_unchecked(&bytes[start..end]) });
}

/// Copy a single-quoted SQL string literal from `bytes` starting at `i`
/// (which must point at the opening `'`) into `out`, honouring `''`
/// escapes. Returns the index after the closing quote, or end-of-input
/// for an unterminated literal.
///
/// Non-ASCII bytes inside the literal are preserved byte-perfect.
///
/// Panics in debug builds if `bytes[i]` is not `'`.
pub(crate) fn copy_string_literal(bytes: &[u8], i: usize, out: &mut String) -> usize {
    debug_assert!(bytes[i] == b'\'');
    let start = i;
    let mut j = i + 1;
    while j < bytes.len() {
        if bytes[j] == b'\'' {
            // `''` is an escaped quote inside the literal — consume both
            // and keep scanning.
            if j + 1 < bytes.len() && bytes[j + 1] == b'\'' {
                j += 2;
                continue;
            }
            // Closing quote — include it in the copied range and stop.
            j += 1;
            break;
        }
        j += 1;
    }
    push_run(out, bytes, start, j);
    j
}

/// Rewrite PostgreSQL-style `$N` placeholders to MySQL/SQLite `?`
/// placeholders, leaving `'…'` string literals untouched.
pub fn rewrite_placeholders_to_question(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let mut run_start = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            push_run(&mut out, bytes, run_start, i);
            i = copy_string_literal(bytes, i, &mut out);
            run_start = i;
        } else if b == b'$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            push_run(&mut out, bytes, run_start, i);
            // consume `$` + digits
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            out.push('?');
            run_start = i;
        } else {
            i += 1;
        }
    }
    push_run(&mut out, bytes, run_start, bytes.len());
    out
}

/// Rewrite ANSI-style `"ident"` double-quoted identifiers to MySQL backtick
/// style `` `ident` ``. Single-quoted string literals are left untouched, so
/// a literal like `'"abc"'` is preserved intact.
///
/// Escaped double quotes (`""`) inside an identifier become a single `"`
/// inside backticks. Non-ASCII identifier characters are preserved
/// byte-perfect.
pub fn rewrite_double_quoted_idents_to_backticks(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let mut run_start = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            push_run(&mut out, bytes, run_start, i);
            i = copy_string_literal(bytes, i, &mut out);
            run_start = i;
        } else if b == b'"' {
            push_run(&mut out, bytes, run_start, i);
            out.push('`');
            i += 1;
            let mut body_start = i;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        // `""` escape inside identifier → emit a single `"`.
                        push_run(&mut out, bytes, body_start, i);
                        out.push('"');
                        i += 2;
                        body_start = i;
                    } else {
                        // Closing quote.
                        push_run(&mut out, bytes, body_start, i);
                        out.push('`');
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            run_start = i;
        } else {
            i += 1;
        }
    }
    push_run(&mut out, bytes, run_start, bytes.len());
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

    // ── UTF-8 preservation ──────────────────────────────────────────
    // The previous byte-by-byte `push(b as char)` pattern re-encoded
    // any byte ≥ 0x80 as a 2-byte UTF-8 sequence, corrupting non-ASCII
    // text (e.g. `'café'` → `'cafÃ©'`). These regression tests pin the
    // bulk-copy fix.

    #[test]
    fn non_ascii_in_string_literal_preserved_to_question() {
        let sql = "SELECT $1 WHERE name = 'café'";
        assert_eq!(
            rewrite_placeholders_to_question(sql),
            "SELECT ? WHERE name = 'café'"
        );
    }

    #[test]
    fn non_ascii_outside_literals_preserved_to_question() {
        // Postgres allows Unicode in unquoted identifiers when client
        // encoding is UTF-8.
        let sql = "SELECT é FROM tâble WHERE x = $1";
        assert_eq!(
            rewrite_placeholders_to_question(sql),
            "SELECT é FROM tâble WHERE x = ?"
        );
    }

    #[test]
    fn non_ascii_in_double_quoted_ident_preserved() {
        let sql = r#"SELECT "café" FROM "tâble""#;
        assert_eq!(
            rewrite_double_quoted_idents_to_backticks(sql),
            "SELECT `café` FROM `tâble`"
        );
    }

    #[test]
    fn non_ascii_in_string_with_doubled_quote_preserved() {
        let sql = "SELECT 'l''été $1' FROM t";
        assert_eq!(
            rewrite_placeholders_to_question(sql),
            "SELECT 'l''été $1' FROM t"
        );
    }

    #[test]
    fn unterminated_literal_does_not_panic() {
        let sql = "SELECT 'unterminated";
        // We only check the call doesn't panic and consumes the rest of
        // the input. Behaviour on malformed SQL is best-effort.
        let out = rewrite_placeholders_to_question(sql);
        assert_eq!(out, sql);
    }
}
