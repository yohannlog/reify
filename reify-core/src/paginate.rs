use std::fmt::Write;
use std::marker::PhantomData;

use crate::column::Column;
use crate::condition::Condition;
use crate::ident::qi;
use crate::query::SelectBuilder;
use crate::sql::{OrderFragment, SqlFragment};
use crate::table::Table;
use crate::value::{IntoValue, Value};

// ── Page result (offset-based) ─────────────────────────────────────

/// A page of results with metadata for navigation.
#[derive(Debug, Clone)]
pub struct Page<M> {
    /// Current page number (1-indexed).
    pub page: u64,
    /// Items per page.
    pub per_page: u64,
    /// Total number of items (from COUNT query).
    pub total_items: u64,
    /// Total number of pages.
    pub total_pages: u64,
    /// Whether there is a next page.
    pub has_next: bool,
    /// Whether there is a previous page.
    pub has_prev: bool,
    _model: PhantomData<M>,
}

impl<M> Page<M> {
    pub fn new(page: u64, per_page: u64, total_items: u64) -> Self {
        let total_pages = if total_items == 0 {
            1
        } else {
            (total_items + per_page - 1) / per_page
        };
        Self {
            page,
            per_page,
            total_items,
            total_pages,
            has_next: page < total_pages,
            has_prev: page > 1,
            _model: PhantomData,
        }
    }
}

// ── Offset-based pagination ─────────────────────────────────────────

/// Builder for offset-based pagination (classic `LIMIT/OFFSET`).
///
/// ```ignore
/// let paginated = User::find()
///     .filter(User::role.is_not_null())
///     .paginate(3, 25);  // page 3, 25 per page
///
/// let (data_sql, count_sql, params) = paginated.build();
/// ```
pub struct Paginated<M: Table> {
    builder: SelectBuilder<M>,
    page: u64,
    per_page: u64,
}

impl<M: Table> Paginated<M> {
    pub fn new(builder: SelectBuilder<M>, page: u64, per_page: u64) -> Self {
        assert!(page >= 1, "Page number must be >= 1");
        assert!(per_page >= 1, "Per-page must be >= 1");
        Self {
            builder,
            page,
            per_page,
        }
    }

    /// Build both the data query and the count query.
    ///
    /// Returns `(data_sql, count_sql, params)`.
    /// - `data_sql`: SELECT with LIMIT/OFFSET applied
    /// - `count_sql`: SELECT COUNT(*) with the same WHERE clause
    /// - `params`: shared parameters (used by both queries)
    pub fn build(&self) -> (String, String, Vec<Value>) {
        let ast = self.builder.build_ast();
        let offset = (self.page - 1) * self.per_page;

        // Data query: set LIMIT/OFFSET on the AST
        let data_ast = match ast {
            SqlFragment::Select {
                distinct,
                columns,
                from,
                joins,
                conditions,
                group_by,
                having,
                order_by,
                ..
            } => SqlFragment::Select {
                distinct,
                columns,
                from,
                joins,
                conditions,
                group_by,
                having,
                order_by,
                limit: Some(self.per_page),
                offset: Some(offset),
            },
            raw => raw,
        };

        let mut data_params = Vec::new();
        let data_sql = data_ast.render(&mut data_params);

        // Count query: from the AST
        let count_ast = self.builder.build_ast().to_count_query();
        let mut count_params = Vec::new();
        let count_sql = count_ast.render(&mut count_params);

        // Use data_params as the shared params (count params are identical)
        (data_sql, count_sql, data_params)
    }

    /// Create a `Page` metadata object from a known total count.
    pub fn page_info(&self, total_items: u64) -> Page<M> {
        Page::new(self.page, self.per_page, total_items)
    }
}

// ── Simple cursor-based pagination (single column) ──────────────────

/// Builder for cursor-based pagination (keyset pagination).
///
/// More performant than offset for large datasets — uses WHERE instead of OFFSET.
///
/// ```ignore
/// let page = User::find()
///     .filter(User::role.is_not_null())
///     .after(User::id, 150, 25);  // 25 items after id=150
///
/// let (sql, params) = page.build();
/// ```
pub struct CursorPaginated<M: Table> {
    builder: SelectBuilder<M>,
    cursor_column: &'static str,
    cursor_value: Option<Value>,
    direction: CursorDirection,
    limit: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorDirection {
    Forward,
    Backward,
}

impl<M: Table> CursorPaginated<M> {
    pub fn new(
        builder: SelectBuilder<M>,
        cursor_column: &'static str,
        cursor_value: Option<Value>,
        direction: CursorDirection,
        limit: u64,
    ) -> Self {
        assert!(limit >= 1, "Limit must be >= 1");
        Self {
            builder,
            cursor_column,
            cursor_value,
            direction,
            limit,
        }
    }

    /// Build the cursor-paginated query.
    ///
    /// Requests `limit + 1` rows to detect if there are more results.
    pub fn build(&self) -> (String, Vec<Value>) {
        let ast = self.builder.build_ast();

        // Operate on the AST: strip limit/offset, add cursor condition + ordering
        let ast = match ast {
            SqlFragment::Select {
                distinct,
                columns,
                from,
                joins,
                mut conditions,
                group_by,
                having,
                ..
            } => {
                // Add cursor condition
                if let Some(ref val) = self.cursor_value {
                    let cursor_cond = match self.direction {
                        CursorDirection::Forward => Condition::Gt(self.cursor_column, val.clone()),
                        CursorDirection::Backward => Condition::Lt(self.cursor_column, val.clone()),
                    };
                    conditions.to_mut().push(cursor_cond);
                }

                // Replace ORDER BY with cursor ordering
                let descending = self.direction == CursorDirection::Backward;
                let order_by = vec![OrderFragment {
                    column: qi(self.cursor_column),
                    descending,
                }];

                SqlFragment::Select {
                    distinct,
                    columns,
                    from,
                    joins,
                    conditions,
                    group_by,
                    having,
                    order_by: std::borrow::Cow::Owned(order_by),
                    limit: Some(self.limit + 1), // fetch one extra to detect has_next
                    offset: None,
                }
            }
            raw => raw,
        };

        let mut params = Vec::new();
        let sql = ast.render(&mut params);
        (sql, params)
    }

    /// Check if there are more results based on the number of rows returned.
    ///
    /// Pass the actual row count from your query result.
    /// If `row_count > limit`, there are more pages — trim the last row from your results.
    pub fn has_more(&self, row_count: u64) -> bool {
        row_count > self.limit
    }
}

// ═══════════════════════════════════════════════════════════════════
// ── Relay-style cursor pagination ──────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

// ── Opaque cursor encoding ─────────────────────────────────────────

/// An opaque, base64-encoded cursor string safe for use in REST/GraphQL APIs.
///
/// Internally encodes one or more `Value`s as `col:type:value` pairs separated
/// by `|`. The encoding is intentionally simple and deterministic.
///
/// ```ignore
/// let cursor = Cursor::encode(&[("id", &Value::I64(42))]);
/// assert_eq!(cursor.to_string(), "aWQ6aTY0OjQy");  // base64
///
/// let values = Cursor::decode(&cursor.to_string()).unwrap();
/// assert_eq!(values, vec![("id".to_string(), Value::I64(42))]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor(pub String);

impl Cursor {
    /// Encode column values into an opaque (**unsigned**) cursor string.
    ///
    /// # Security
    ///
    /// Unsigned cursors can be forged by any client that understands the
    /// encoding. They are fine for internal callers and for values that
    /// cannot grant privilege escalation (e.g. stable public IDs in a
    /// read-only list). For any API that exposes the cursor to untrusted
    /// clients, use [`encode_signed`](Self::encode_signed) instead.
    pub fn encode(fields: &[(&str, &Value)]) -> Self {
        Cursor(base64_encode(encode_raw(fields).as_bytes()))
    }

    /// Decode an (**unsigned**) opaque cursor string back into column
    /// name + value pairs.
    ///
    /// Returns `None` if the cursor is malformed. See
    /// [`decode_signed`](Self::decode_signed) for the authenticated variant.
    pub fn decode(cursor: &str) -> Option<Vec<(String, Value)>> {
        let bytes = base64_decode(cursor)?;
        let raw = std::str::from_utf8(&bytes).ok()?;
        decode_raw(raw)
    }

    /// Encode column values into a **signed** opaque cursor.
    ///
    /// The format is `<base64(payload)>.<base64(mac)>`, where `mac` is the
    /// first 16 bytes of `HMAC-SHA256(key, payload)`. 128-bit truncation is
    /// the NIST SP 800-107 minimum for HMAC authentication tags and keeps
    /// the cursor short.
    ///
    /// Reuse the same HMAC key as the audit log
    /// ([`crate::audit::AuditContext::with_integrity`]) so operators have
    /// one secret to rotate, or use a distinct key — the API does not
    /// enforce one choice.
    ///
    /// # Panics
    ///
    /// Does not panic — `hmac::Hmac<Sha256>::new_from_slice` accepts any
    /// key length. An empty key is accepted but provides no integrity.
    pub fn encode_signed(fields: &[(&str, &Value)], key: &[u8]) -> Self {
        let raw = encode_raw(fields);
        let mac = crate::audit::hmac_sha256(key, raw.as_bytes());
        let payload = base64_encode(raw.as_bytes());
        let tag = base64_encode(&mac[..MAC_BYTES]);
        Cursor(format!("{payload}.{tag}"))
    }

    /// Decode and **verify** a signed opaque cursor.
    ///
    /// Returns `None` if the cursor is malformed, the signature is
    /// missing, or the MAC does not match (constant-time comparison).
    /// The key must be the same one that signed the cursor.
    pub fn decode_signed(cursor: &str, key: &[u8]) -> Option<Vec<(String, Value)>> {
        let (payload_b64, tag_b64) = cursor.split_once('.')?;
        let payload = base64_decode(payload_b64)?;
        let provided = base64_decode(tag_b64)?;
        if provided.len() != MAC_BYTES {
            return None;
        }
        let expected = crate::audit::hmac_sha256(key, &payload);
        if !constant_time_eq(&provided, &expected[..MAC_BYTES]) {
            return None;
        }
        let raw = std::str::from_utf8(&payload).ok()?;
        decode_raw(raw)
    }

    /// The raw opaque string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Length of the truncated HMAC tag carried in signed cursors
/// (16 bytes = 128 bits, NIST SP 800-107 minimum).
const MAC_BYTES: usize = 16;

/// Build the unsigned `col:type:value|...` payload shared by both
/// `encode` and `encode_signed`.
fn encode_raw(fields: &[(&str, &Value)]) -> String {
    let mut raw = String::new();
    for (i, (col, val)) in fields.iter().enumerate() {
        if i > 0 {
            raw.push('|');
        }
        let _ = write!(raw, "{}:{}", col, encode_value(val));
    }
    raw
}

/// Parse the unsigned `col:type:value|...` payload into `(col, value)`
/// pairs. Returns `None` when the payload is malformed or empty.
fn decode_raw(raw: &str) -> Option<Vec<(String, Value)>> {
    let mut result = Vec::new();
    for part in split_cursor_fields(raw) {
        let (col, val) = decode_field(&part)?;
        result.push((col, val));
    }
    if result.is_empty() {
        return None;
    }
    Some(result)
}

/// Constant-time byte-slice comparison. Returns `false` for length
/// mismatch without short-circuiting on content.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

impl std::fmt::Display for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Escape `|` and `\` in a string so it is safe to embed in the
/// `col:type:value` cursor format (which uses `|` as field separator).
fn escape_cursor_str(s: &str) -> String {
    s.replace('\\', "\\\\").replace('|', "\\|")
}

/// Unescape a string that was escaped by `escape_cursor_str`.
fn unescape_cursor_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.peek() {
                Some('|') => {
                    out.push('|');
                    chars.next();
                }
                Some('\\') => {
                    out.push('\\');
                    chars.next();
                }
                _ => out.push(c),
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn encode_value(val: &Value) -> String {
    match val {
        Value::I16(v) => format!("i16:{v}"),
        Value::I32(v) => format!("i32:{v}"),
        Value::I64(v) => format!("i64:{v}"),
        Value::F32(v) => format!("f32:{v}"),
        Value::F64(v) => format!("f64:{v}"),
        Value::String(s) => format!("str:{}", escape_cursor_str(s)),
        Value::Bool(b) => format!("bool:{b}"),
        #[cfg(feature = "postgres")]
        Value::Uuid(u) => format!("uuid:{u}"),
        #[cfg(feature = "postgres")]
        Value::Timestamptz(t) => format!("tstz:{}", t.to_rfc3339()),
        #[cfg(any(feature = "postgres", feature = "mysql"))]
        Value::Timestamp(t) => format!("ts:{t}"),
        #[cfg(any(feature = "postgres", feature = "mysql"))]
        Value::Date(d) => format!("date:{d}"),
        #[cfg(any(feature = "postgres", feature = "mysql"))]
        Value::Time(t) => format!("time:{t}"),
        _ => "null:".to_string(),
    }
}

fn decode_field(part: &str) -> Option<(String, Value)> {
    // Format: "col_name:type:value"
    let colon1 = part.find(':')?;
    let col = &part[..colon1];
    let rest = &part[colon1 + 1..];
    let colon2 = rest.find(':')?;
    let typ = &rest[..colon2];
    let raw = &rest[colon2 + 1..];

    let val = match typ {
        "i16" => Value::I16(raw.parse().ok()?),
        "i32" => Value::I32(raw.parse().ok()?),
        "i64" => Value::I64(raw.parse().ok()?),
        "f32" => Value::F32(raw.parse().ok()?),
        "f64" => Value::F64(raw.parse().ok()?),
        "str" => Value::String(unescape_cursor_str(raw)),
        "bool" => Value::Bool(raw.parse().ok()?),
        #[cfg(feature = "postgres")]
        "uuid" => Value::Uuid(raw.parse().ok()?),
        #[cfg(feature = "postgres")]
        "tstz" => Value::Timestamptz(
            chrono::DateTime::parse_from_rfc3339(raw)
                .ok()?
                .with_timezone(&chrono::Utc),
        ),
        #[cfg(any(feature = "postgres", feature = "mysql"))]
        "ts" => Value::Timestamp(raw.parse().ok()?),
        #[cfg(any(feature = "postgres", feature = "mysql"))]
        "date" => Value::Date(raw.parse().ok()?),
        #[cfg(any(feature = "postgres", feature = "mysql"))]
        "time" => Value::Time(raw.parse().ok()?),
        _ => return None,
    };
    Some((col.to_string(), val))
}

// ── Minimal base64 (no external dependency) ────────────────────────

const B64_CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity((input.len() + 2) / 3 * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64_CHARS[((triple >> 18) & 0x3F) as usize] as char);
        out.push(B64_CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64_CHARS[((triple >> 6) & 0x3F) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(B64_CHARS[(triple & 0x3F) as usize] as char);
        }
    }
    out
}

fn base64_decode(input: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let remaining = bytes.len() - i;
        // Need at least 2 chars to decode 1 byte
        if remaining < 2 {
            return None;
        }
        let b0 = b64_val(bytes[i])?;
        let b1 = b64_val(bytes[i + 1])?;
        let b2 = if i + 2 < bytes.len() {
            b64_val(bytes[i + 2])?
        } else {
            0
        };
        let b3 = if i + 3 < bytes.len() {
            b64_val(bytes[i + 3])?
        } else {
            0
        };
        let triple = (b0 << 18) | (b1 << 12) | (b2 << 6) | b3;
        // Always emit the first byte
        out.push(((triple >> 16) & 0xFF) as u8);
        // Emit second byte if we had at least 3 chars in this group
        if remaining >= 3 {
            out.push(((triple >> 8) & 0xFF) as u8);
        }
        // Emit third byte only if we had a full group of 4
        if remaining >= 4 {
            out.push((triple & 0xFF) as u8);
        }
        i += 4;
    }
    Some(out)
}

/// Split a cursor raw string on unescaped `|` separators.
/// `\|` is treated as a literal pipe inside a field value.
fn split_cursor_fields(s: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.peek() {
                Some('|') => {
                    current.push('\\');
                    current.push('|');
                    chars.next();
                }
                Some('\\') => {
                    current.push('\\');
                    current.push('\\');
                    chars.next();
                }
                _ => current.push(c),
            }
        } else if c == '|' {
            fields.push(current.clone());
            current.clear();
        } else {
            current.push(c);
        }
    }
    fields.push(current);
    fields
}

fn b64_val(c: u8) -> Option<u32> {
    match c {
        b'A'..=b'Z' => Some((c - b'A') as u32),
        b'a'..=b'z' => Some((c - b'a' + 26) as u32),
        b'0'..=b'9' => Some((c - b'0' + 52) as u32),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    }
}

// ── CursorPage — Relay Connection result ───────────────────────────

/// A single edge in a cursor-paginated result.
///
/// Pairs a row's cursor with its position in the result set.
/// The `node` field is not included — the caller maps rows to edges
/// using [`CursorPage::from_rows`].
#[derive(Debug, Clone)]
pub struct Edge {
    /// Opaque cursor for this row.
    pub cursor: Cursor,
}

/// Relay Connection-style page info.
///
/// Returned by [`CursorPage`] after processing query results.
///
/// ```ignore
/// // After executing the query:
/// let page = cursor_builder.into_page(&rows, |row| {
///     vec![("id", row.get("id").unwrap().clone())]
/// });
///
/// if page.page_info.has_next_page {
///     let next_cursor = page.page_info.end_cursor.as_ref().unwrap();
///     // Use next_cursor in the next request
/// }
/// ```
#[derive(Debug, Clone)]
pub struct PageInfo {
    /// Whether there are more items after the last edge.
    pub has_next_page: bool,
    /// Whether there are more items before the first edge.
    pub has_previous_page: bool,
    /// Cursor of the first edge (if any).
    pub start_cursor: Option<Cursor>,
    /// Cursor of the last edge (if any).
    pub end_cursor: Option<Cursor>,
}

/// A complete cursor-paginated result set (Relay Connection-compatible).
///
/// Contains edges with opaque cursors and page navigation info.
///
/// # Usage
///
/// ```ignore
/// // 1. Build the query
/// let builder = User::find()
///     .filter(User::role.eq("admin"))
///     .cursor(User::id)
///     .first(25)
///     .after_cursor("aWQ6aTY0OjQy");
///
/// let (sql, params) = builder.build();
///
/// // 2. Execute the query (your DB adapter)
/// let rows = db.query(&sql, &params).await?;
///
/// // 3. Process into a CursorPage
/// let page = builder.into_page(&rows);
///
/// // 4. Use the result
/// println!("has_next: {}", page.page_info.has_next_page);
/// for (i, edge) in page.edges.iter().enumerate() {
///     println!("  row {i}: cursor={}", edge.cursor);
/// }
/// if let Some(end) = &page.page_info.end_cursor {
///     println!("next page: after={end}");
/// }
/// ```
#[derive(Debug, Clone)]
pub struct CursorPage {
    /// Edges with opaque cursors (one per result row, excluding the extra probe row).
    pub edges: Vec<Edge>,
    /// Navigation metadata.
    pub page_info: PageInfo,
}

// ── CursorBuilder — Relay-style fluent API ─────────────────────────

/// A cursor column definition: column name + sort direction.
#[derive(Debug, Clone)]
pub struct CursorCol {
    pub name: &'static str,
    pub descending: bool,
}

/// Fluent builder for Relay-style cursor pagination.
///
/// Supports single and multi-column cursors, forward (`first`/`after`) and
/// backward (`last`/`before`) pagination, and optional total count.
///
/// # Single-column cursor
///
/// ```ignore
/// let builder = User::find()
///     .cursor(User::id)
///     .first(25);
///
/// let (sql, params) = builder.build();
/// // SELECT * FROM users ORDER BY id ASC LIMIT 26
/// ```
///
/// # Multi-column cursor (composite key)
///
/// ```ignore
/// let builder = Post::find()
///     .cursor_by(vec![
///         Post::created_at.desc_cursor(),
///         Post::id.desc_cursor(),
///     ])
///     .first(20)
///     .after_cursor("Y3JlYXRlZF9hdDp0czo...");
///
/// let (sql, params) = builder.build();
/// // SELECT * FROM posts
/// //   WHERE (created_at, id) < (?, ?)
/// //   ORDER BY created_at DESC, id DESC
/// //   LIMIT 21
/// ```
///
/// # With total count
///
/// ```ignore
/// let (data_sql, count_sql, params) = builder.build_with_count();
/// ```
pub struct CursorBuilder<M: Table> {
    inner: SelectBuilder<M>,
    columns: Vec<CursorCol>,
    limit: u64,
    backward: bool,
    cursor: Option<String>,
    with_count: bool,
    /// When set, incoming cursors are verified with this HMAC-SHA256 key
    /// and emitted cursors are signed with it. Untrusted callers
    /// (REST/GraphQL clients) should always go through this path.
    signing_key: Option<Vec<u8>>,
}

impl<M: Table> CursorBuilder<M> {
    fn new(inner: SelectBuilder<M>, columns: Vec<CursorCol>) -> Self {
        assert!(!columns.is_empty(), "cursor requires at least one column");
        Self {
            inner,
            columns,
            limit: 25,
            backward: false,
            cursor: None,
            with_count: false,
            signing_key: None,
        }
    }

    /// Require incoming cursors to be HMAC-signed, and sign emitted cursors.
    ///
    /// Use the same key as [`crate::audit::AuditContext::with_integrity`] to
    /// keep secret rotation centralised, or a separate key per surface — both
    /// are acceptable.
    ///
    /// When set, any unsigned or tampered cursor passed to
    /// [`after_cursor`](Self::after_cursor) / [`before_cursor`](Self::before_cursor)
    /// is silently ignored (treated as "no cursor"). Emitted cursors in
    /// `CursorPage::edges`, `page_info.start_cursor`, and `page_info.end_cursor`
    /// are signed.
    pub fn signed(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.signing_key = Some(key.into());
        self
    }

    /// Set the page size (forward direction). Default: 25.
    pub fn first(mut self, n: u64) -> Self {
        assert!(n >= 1, "first must be >= 1");
        self.limit = n;
        self.backward = false;
        self
    }

    /// Set the page size (backward direction).
    pub fn last(mut self, n: u64) -> Self {
        assert!(n >= 1, "last must be >= 1");
        self.limit = n;
        self.backward = true;
        self
    }

    /// Paginate after the given opaque cursor (forward).
    pub fn after_cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self.backward = false;
        self
    }

    /// Paginate before the given opaque cursor (backward).
    pub fn before_cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self.backward = true;
        self
    }

    /// Also generate a COUNT query (for UIs that need total count).
    pub fn with_total_count(mut self) -> Self {
        self.with_count = true;
        self
    }

    /// Build the cursor-paginated SQL query.
    ///
    /// Fetches `limit + 1` rows to detect whether more pages exist.
    pub fn build(&self) -> (String, Vec<Value>) {
        let ast = self.apply_cursor(self.inner.build_ast());
        let mut params = Vec::new();
        let sql = ast.render(&mut params);
        (sql, params)
    }

    /// Build both the data query and a COUNT query.
    ///
    /// Returns `(data_sql, count_sql, data_params, count_params)`.
    pub fn build_with_count(&self) -> (String, String, Vec<Value>, Vec<Value>) {
        let count_ast = self.inner.build_ast().to_count_query();
        let mut count_params = Vec::new();
        let count_sql = count_ast.render(&mut count_params);

        let data_ast = self.apply_cursor(self.inner.build_ast());
        let mut data_params = Vec::new();
        let data_sql = data_ast.render(&mut data_params);

        (data_sql, count_sql, data_params, count_params)
    }

    fn apply_cursor<'a>(&self, ast: SqlFragment<'a>) -> SqlFragment<'a> {
        match ast {
            SqlFragment::Select {
                distinct,
                columns,
                from,
                joins,
                mut conditions,
                group_by,
                having,
                ..
            } => {
                // Decode cursor and add WHERE condition. When a signing key
                // is configured, only signed cursors are accepted; unsigned
                // or tampered input is silently treated as "no cursor".
                if let Some(ref cursor_str) = self.cursor {
                    let decoded = match self.signing_key.as_deref() {
                        Some(key) => Cursor::decode_signed(cursor_str, key),
                        None => Cursor::decode(cursor_str),
                    };
                    if let Some(decoded) = decoded {
                        let cond = build_cursor_condition(&self.columns, &decoded, self.backward);
                        if let Some(c) = cond {
                            conditions.to_mut().push(c);
                        }
                    }
                }

                // Build ORDER BY from cursor columns
                let order_by: Vec<OrderFragment> = self
                    .columns
                    .iter()
                    .map(|c| {
                        let descending = if self.backward {
                            !c.descending
                        } else {
                            c.descending
                        };
                        OrderFragment {
                            column: qi(c.name),
                            descending,
                        }
                    })
                    .collect();

                SqlFragment::Select {
                    distinct,
                    columns,
                    from,
                    joins,
                    conditions,
                    group_by,
                    having,
                    order_by: std::borrow::Cow::Owned(order_by),
                    limit: Some(self.limit + 1),
                    offset: None,
                }
            }
            raw => raw,
        }
    }

    /// Process raw database rows into a [`CursorPage`].
    ///
    /// Call this after executing the query returned by [`build()`](Self::build).
    /// The `cursor_extractor` closure extracts cursor column values from each row.
    ///
    /// ```ignore
    /// let page = builder.into_page(&rows, |row| {
    ///     vec![("id", row.get("id").unwrap().clone())]
    /// });
    /// ```
    pub fn into_page(
        &self,
        rows: &[crate::db::Row],
        cursor_extractor: impl Fn(&crate::db::Row) -> Vec<(&str, Value)>,
    ) -> CursorPage {
        let has_extra = rows.len() as u64 > self.limit;
        let actual_rows = if has_extra {
            &rows[..self.limit as usize]
        } else {
            rows
        };

        let mut edges: Vec<Edge> = actual_rows
            .iter()
            .map(|row| {
                let fields = cursor_extractor(row);
                let refs: Vec<(&str, &Value)> = fields.iter().map(|(k, v)| (*k, v)).collect();
                let cursor = match self.signing_key.as_deref() {
                    Some(key) => Cursor::encode_signed(&refs, key),
                    None => Cursor::encode(&refs),
                };
                Edge { cursor }
            })
            .collect();

        // For backward pagination, reverse to restore natural order
        if self.backward {
            edges.reverse();
        }

        let (has_next_page, has_previous_page) = if self.backward {
            (self.cursor.is_some(), has_extra)
        } else {
            (has_extra, self.cursor.is_some())
        };

        let start_cursor = edges.first().map(|e| e.cursor.clone());
        let end_cursor = edges.last().map(|e| e.cursor.clone());

        CursorPage {
            edges,
            page_info: PageInfo {
                has_next_page,
                has_previous_page,
                start_cursor,
                end_cursor,
            },
        }
    }
}

/// Build a multi-column cursor WHERE condition.
///
/// For a single column `(id)` with forward direction:
///   `id > ?`
///
/// For two columns `(created_at DESC, id DESC)` with forward direction:
///   `(created_at, id) < (?, ?)`
///
/// The comparison operator flips based on the sort direction of the
/// *first* cursor column and whether we're going backward.
fn build_cursor_condition(
    columns: &[CursorCol],
    decoded: &[(String, Value)],
    backward: bool,
) -> Option<Condition> {
    if columns.len() != decoded.len() {
        return None;
    }

    if columns.len() == 1 {
        // Single-column: simple comparison
        let col = &columns[0];
        let (_, val) = &decoded[0];
        // Forward + ASC → GT, Forward + DESC → LT
        // Backward + ASC → LT, Backward + DESC → GT
        let use_gt = col.descending == backward;
        if use_gt {
            Some(Condition::Gt(col.name, val.clone()))
        } else {
            Some(Condition::Lt(col.name, val.clone()))
        }
    } else {
        // Multi-column: row-value comparison `(a, b) < (?, ?)`
        // Determine operator from first column's direction
        let first_desc = columns[0].descending;
        let use_gt = first_desc == backward;
        let op = if use_gt { ">" } else { "<" };

        // Quote each column name to handle reserved words and mixed-case identifiers.
        let col_list: Vec<String> = columns.iter().map(|c| qi(c.name)).collect();
        let placeholders: Vec<&str> = vec!["?"; columns.len()];

        let raw_sql = format!(
            "({}) {} ({})",
            col_list.join(", "),
            op,
            placeholders.join(", ")
        );

        let raw_params: Vec<Value> = decoded.iter().map(|(_, v)| v.clone()).collect();

        Some(Condition::Raw(raw_sql, raw_params))
    }
}

// ── Column cursor helpers ──────────────────────────────────────────

impl<M: 'static, T: 'static> Column<M, T> {
    /// Create an ascending cursor column definition.
    pub fn asc_cursor(&self) -> CursorCol {
        CursorCol {
            name: self.name,
            descending: false,
        }
    }

    /// Create a descending cursor column definition.
    pub fn desc_cursor(&self) -> CursorCol {
        CursorCol {
            name: self.name,
            descending: true,
        }
    }
}

// ── SelectBuilder integration ───────────────────────────────────────

impl<M: Table> SelectBuilder<M> {
    /// Offset-based pagination: page N with `per_page` items.
    ///
    /// ```ignore
    /// let paginated = User::find()
    ///     .filter(User::role.is_not_null())
    ///     .paginate(3, 25);
    /// let (data_sql, count_sql, params) = paginated.build();
    /// ```
    pub fn paginate(self, page: u64, per_page: u64) -> Paginated<M> {
        Paginated::new(self, page, per_page)
    }

    /// Cursor-based pagination: `limit` items after the given cursor value.
    ///
    /// ```ignore
    /// let page = User::find()
    ///     .after(User::id, 150i64, 25);
    /// let (sql, params) = page.build();
    /// ```
    pub fn after<T: IntoValue>(
        self,
        cursor: Column<M, T>,
        value: impl IntoValue,
        limit: u64,
    ) -> CursorPaginated<M> {
        CursorPaginated::new(
            self,
            cursor.name,
            Some(value.into_value()),
            CursorDirection::Forward,
            limit,
        )
    }

    /// Cursor-based pagination: `limit` items before the given cursor value.
    ///
    /// ```ignore
    /// let page = User::find()
    ///     .before(User::id, 100i64, 25);
    /// let (sql, params) = page.build();
    /// ```
    pub fn before<T: IntoValue>(
        self,
        cursor: Column<M, T>,
        value: impl IntoValue,
        limit: u64,
    ) -> CursorPaginated<M> {
        CursorPaginated::new(
            self,
            cursor.name,
            Some(value.into_value()),
            CursorDirection::Backward,
            limit,
        )
    }

    /// Start building a Relay-style cursor-paginated query on a single column.
    ///
    /// ```ignore
    /// let builder = User::find()
    ///     .cursor(User::id)
    ///     .first(25)
    ///     .after_cursor("aWQ6aTY0OjQy");
    ///
    /// let (sql, params) = builder.build();
    /// ```
    pub fn cursor<T: 'static>(self, col: Column<M, T>) -> CursorBuilder<M> {
        CursorBuilder::new(
            self,
            vec![CursorCol {
                name: col.name,
                descending: false,
            }],
        )
    }

    /// Start building a Relay-style cursor-paginated query on a single column
    /// with descending order.
    ///
    /// ```ignore
    /// let builder = Post::find()
    ///     .cursor_desc(Post::created_at)
    ///     .first(20);
    /// ```
    pub fn cursor_desc<T: 'static>(self, col: Column<M, T>) -> CursorBuilder<M> {
        CursorBuilder::new(
            self,
            vec![CursorCol {
                name: col.name,
                descending: true,
            }],
        )
    }

    /// Start building a Relay-style cursor-paginated query with multiple
    /// cursor columns (composite key).
    ///
    /// ```ignore
    /// let builder = Post::find()
    ///     .cursor_by(vec![
    ///         Post::created_at.desc_cursor(),
    ///         Post::id.desc_cursor(),
    ///     ])
    ///     .first(20);
    /// ```
    pub fn cursor_by(self, columns: Vec<CursorCol>) -> CursorBuilder<M> {
        CursorBuilder::new(self, columns)
    }
}

// Text-based helpers removed — pagination now operates on SqlFragment AST.
// See SqlFragment::to_count_query(), without_limit_offset(), without_order_by().

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    // ── Cursor encode/decode round-trips ────────────────────────────

    #[test]
    fn cursor_roundtrip_simple_i64() {
        let fields = [("id", &Value::I64(42))];
        let cursor = Cursor::encode(&fields);
        let decoded = Cursor::decode(cursor.as_str()).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, "id");
        assert_eq!(decoded[0].1, Value::I64(42));
    }

    #[test]
    fn cursor_roundtrip_string_with_pipe() {
        // A string value containing `|` must survive encode/decode without
        // being split into multiple fields.
        let s = Value::String("foo|bar|baz".to_string());
        let fields = [("name", &s)];
        let cursor = Cursor::encode(&fields);
        let decoded = Cursor::decode(cursor.as_str()).unwrap();
        assert_eq!(decoded.len(), 1, "pipe in value must not split fields");
        assert_eq!(decoded[0].0, "name");
        assert_eq!(decoded[0].1, Value::String("foo|bar|baz".to_string()));
    }

    #[test]
    fn cursor_roundtrip_string_with_backslash() {
        let s = Value::String("back\\slash".to_string());
        let fields = [("path", &s)];
        let cursor = Cursor::encode(&fields);
        let decoded = Cursor::decode(cursor.as_str()).unwrap();
        assert_eq!(decoded[0].1, Value::String("back\\slash".to_string()));
    }

    #[test]
    fn cursor_roundtrip_multi_field() {
        let a = Value::I64(99);
        let b = Value::String("hello|world".to_string());
        let fields = [("id", &a), ("name", &b)];
        let cursor = Cursor::encode(&fields);
        let decoded = Cursor::decode(cursor.as_str()).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].1, Value::I64(99));
        assert_eq!(decoded[1].1, Value::String("hello|world".to_string()));
    }

    #[test]
    fn cursor_decode_invalid_returns_none() {
        assert!(Cursor::decode("!!!not-base64!!!").is_none());
    }

    // ── base64 unpadded round-trips ─────────────────────────────────

    #[test]
    fn base64_roundtrip_various_lengths() {
        for len in 0usize..=9 {
            let input: Vec<u8> = (0..len as u8).collect();
            let encoded = base64_encode(&input);
            let decoded = base64_decode(&encoded).unwrap_or_else(|| {
                panic!("base64_decode failed for len={len}, encoded={encoded:?}")
            });
            assert_eq!(decoded, input, "roundtrip failed for len={len}");
        }
    }

    #[test]
    fn base64_known_values() {
        // "Man" → "TWFu" (standard base64, same chars in URL-safe alphabet)
        assert_eq!(base64_encode(b"Man"), "TWFu");
        let decoded = base64_decode("TWFu").unwrap();
        assert_eq!(decoded, b"Man");
    }

    // ── escape_cursor_str / unescape_cursor_str ─────────────────────

    #[test]
    fn escape_unescape_roundtrip() {
        let cases = ["plain", "with|pipe", "back\\slash", "both|and\\mixed", ""];
        for s in &cases {
            let escaped = escape_cursor_str(s);
            let unescaped = unescape_cursor_str(&escaped);
            assert_eq!(&unescaped, s, "roundtrip failed for: {s:?}");
        }
    }

    // ── Signed cursor tests ─────────────────────────────────────────

    #[test]
    fn signed_cursor_roundtrip() {
        let key = b"test-hmac-secret-key-32-bytes!!!";
        let fields = [("id", &Value::I64(42))];
        let cursor = Cursor::encode_signed(&fields, key);
        let decoded = Cursor::decode_signed(cursor.as_str(), key).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].1, Value::I64(42));
    }

    #[test]
    fn signed_cursor_rejects_wrong_key() {
        let key = b"correct-key-xxxxxxxxxxxxxxxxxxxx";
        let wrong = b"wrong-key-yyyyyyyyyyyyyyyyyyyyyy";
        let fields = [("id", &Value::I64(7))];
        let cursor = Cursor::encode_signed(&fields, key);
        assert!(Cursor::decode_signed(cursor.as_str(), wrong).is_none());
    }

    #[test]
    fn signed_cursor_rejects_tampered_payload() {
        let key = b"test-hmac-secret-key-32-bytes!!!";
        let fields = [("id", &Value::I64(1))];
        let cursor = Cursor::encode_signed(&fields, key);
        // Flip the last byte of the payload (before the `.`), keep tag intact.
        let s = cursor.as_str().to_string();
        let dot = s.rfind('.').unwrap();
        let mut bytes = s.into_bytes();
        bytes[dot - 1] ^= 0x01;
        let tampered = std::str::from_utf8(&bytes).unwrap();
        assert!(Cursor::decode_signed(tampered, key).is_none());
    }

    #[test]
    fn signed_cursor_rejects_unsigned_input() {
        let key = b"test-hmac-secret-key-32-bytes!!!";
        let fields = [("id", &Value::I64(1))];
        // Plain unsigned cursor — no `.` separator.
        let unsigned = Cursor::encode(&fields);
        assert!(Cursor::decode_signed(unsigned.as_str(), key).is_none());
    }

    #[test]
    fn unsigned_decode_rejects_signed_input() {
        // A signed cursor has a `.` in it and therefore decodes as invalid
        // base64 when fed through the legacy unsigned path.
        let key = b"test-hmac-secret-key-32-bytes!!!";
        let fields = [("id", &Value::I64(1))];
        let signed = Cursor::encode_signed(&fields, key);
        assert!(Cursor::decode(signed.as_str()).is_none());
    }

    #[test]
    fn constant_time_eq_handles_length_mismatch() {
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
    }

    #[test]
    fn escape_pipes_are_preceded_by_backslash() {
        let escaped = escape_cursor_str("a|b|c");
        // Every `|` in the escaped output must be preceded by `\`.
        let bytes = escaped.as_bytes();
        for i in 0..bytes.len() {
            if bytes[i] == b'|' {
                assert!(
                    i > 0 && bytes[i - 1] == b'\\',
                    "bare pipe at position {i} in escaped string: {escaped:?}"
                );
            }
        }
        // And the roundtrip must recover the original.
        assert_eq!(unescape_cursor_str(&escaped), "a|b|c");
    }
}
