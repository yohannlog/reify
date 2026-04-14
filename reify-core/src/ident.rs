use crate::query::Dialect;

/// Quote a SQL identifier using ANSI double-quote style (Generic dialect).
///
/// Convenience shorthand for `quote_ident(name, Dialect::Generic)` — used
/// throughout the core rendering paths that are dialect-agnostic.
#[inline]
pub fn qi(name: &str) -> String {
    quote_ident(name, Dialect::Generic)
}

/// Quote a SQL identifier according to the dialect.
///
/// - PostgreSQL / SQLite / Generic: `"name"` (double quotes, `"` → `""`)
/// - MySQL: `` `name` `` (backticks, `` ` `` → ` `` `)
///
/// Always quotes — even when the name is a simple identifier — so that
/// reserved words (`order`, `group`, `user`, `select`, …) are handled
/// uniformly without maintaining a keyword list.
///
/// # Examples
///
/// ```ignore
/// assert_eq!(quote_ident("user", Dialect::Postgres), "\"user\"");
/// assert_eq!(quote_ident("user", Dialect::Mysql),    "`user`");
/// assert_eq!(quote_ident("col\"x", Dialect::Generic), "\"col\"\"x\"");
/// ```
pub fn quote_ident(name: &str, dialect: Dialect) -> String {
    match dialect {
        Dialect::Mysql => {
            let escaped = name.replace('`', "``");
            format!("`{escaped}`")
        }
        // PostgreSQL, SQLite, Generic all use double-quote quoting.
        _ => {
            let escaped = name.replace('"', "\"\"");
            format!("\"{escaped}\"")
        }
    }
}

/// Validate that a SQL identifier does not contain dangerous characters.
///
/// Intended as a compile-time guard for names coming from proc-macros
/// (`&'static str`). Returns `Ok(())` if the name is safe, or an error
/// message describing the problem.
///
/// Rules:
/// - Must not be empty.
/// - Must not contain null bytes.
/// - Must not contain semicolons (statement terminators).
/// - Must not contain comment markers (`--`, `/*`).
/// - Must be ≤ 128 characters (reasonable limit for identifiers).
pub fn validate_ident(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("identifier must not be empty".into());
    }
    if name.len() > 128 {
        return Err(format!(
            "identifier too long ({} chars, max 128): {:.32}…",
            name.len(),
            name
        ));
    }
    if name.contains('\0') {
        return Err(format!("identifier contains null byte: {name:?}"));
    }
    if name.contains(';') {
        return Err(format!("identifier contains semicolon: {name:?}"));
    }
    if name.contains("--") {
        return Err(format!("identifier contains comment marker '--': {name:?}"));
    }
    if name.contains("/*") {
        return Err(format!("identifier contains comment marker '/*': {name:?}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── quote_ident ─────────────────────────────────────────────────

    #[test]
    fn quotes_simple_name_generic() {
        assert_eq!(quote_ident("users", Dialect::Generic), "\"users\"");
    }

    #[test]
    fn quotes_simple_name_postgres() {
        assert_eq!(quote_ident("users", Dialect::Postgres), "\"users\"");
    }

    #[test]
    fn quotes_simple_name_mysql() {
        assert_eq!(quote_ident("users", Dialect::Mysql), "`users`");
    }

    #[test]
    fn quotes_reserved_word() {
        assert_eq!(quote_ident("order", Dialect::Generic), "\"order\"");
        assert_eq!(quote_ident("group", Dialect::Generic), "\"group\"");
        assert_eq!(quote_ident("user", Dialect::Generic), "\"user\"");
        assert_eq!(quote_ident("select", Dialect::Generic), "\"select\"");
    }

    #[test]
    fn escapes_double_quote_in_name() {
        assert_eq!(quote_ident("col\"x", Dialect::Generic), "\"col\"\"x\"");
        assert_eq!(quote_ident("col\"x", Dialect::Postgres), "\"col\"\"x\"");
    }

    #[test]
    fn escapes_backtick_in_mysql() {
        assert_eq!(quote_ident("col`x", Dialect::Mysql), "`col``x`");
    }

    // ── validate_ident ──────────────────────────────────────────────

    #[test]
    fn valid_identifiers() {
        assert!(validate_ident("users").is_ok());
        assert!(validate_ident("user_roles").is_ok());
        assert!(validate_ident("order").is_ok());
        assert!(validate_ident("CamelCase").is_ok());
    }

    #[test]
    fn rejects_empty() {
        assert!(validate_ident("").is_err());
    }

    #[test]
    fn rejects_null_byte() {
        assert!(validate_ident("foo\0bar").is_err());
    }

    #[test]
    fn rejects_semicolon() {
        assert!(validate_ident("users; DROP TABLE users").is_err());
    }

    #[test]
    fn rejects_comment_markers() {
        assert!(validate_ident("users--evil").is_err());
        assert!(validate_ident("users/*evil").is_err());
    }

    #[test]
    fn rejects_too_long() {
        let long = "a".repeat(129);
        assert!(validate_ident(&long).is_err());
    }
}
