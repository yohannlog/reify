use crate::db::{Database, DbError, FromRow};
use crate::ident::qi;
use crate::query::{DeleteBuilder, InsertBuilder, UpdateBuilder};
use crate::schema::{ColumnDef, SqlType, TimestampSource};
use crate::table::Table;

// `changed_at` is generated app-side (RFC 3339 UTC) so the value is bound
// as a parameter and signed by the HMAC at INSERT time — closes the
// antedating window where the DB-side `NOW()` was unknown to the signer.
#[cfg(any(feature = "postgres", feature = "mysql"))]
use chrono::SecondsFormat;

// ── SHA-256 / HMAC-SHA256 (audited crates) ───────────────────────────
//
// The hash and MAC primitives used to be hand-rolled. They are now thin
// wrappers over the RustCrypto `sha2` and `hmac` crates, which are
// audited, constant-time over secret inputs, and wipe their internal
// buffers on drop. The wrappers preserve the crate-private signatures
// so the rest of the module (and the RFC 4231 / NIST KAT tests) needs
// no change.

use hmac::{Hmac, Mac};
#[cfg(test)]
use sha2::Digest;
use sha2::Sha256;

/// Compute SHA-256 over arbitrary bytes. Returns a 32-byte digest.
///
/// Kept test-only because the production code paths go through
/// `hmac_sha256`; the standalone hash is retained for RFC KAT tests
/// (`test_sha256_empty`, `test_sha256_abc`).
#[inline]
#[cfg(test)]
pub(crate) fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// HMAC-SHA256. Returns a 32-byte MAC.
///
/// Delegates to `hmac::Hmac<Sha256>` (RFC 2104). The crate handles key
/// derivation (hash when longer than the block size, zero-pad otherwise)
/// and zeroizes internal state on drop.
///
/// `pub(crate)` — shared with the `paginate` module for cursor signing.
#[inline]
pub(crate) fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key)
        .expect("Hmac<Sha256> accepts keys of any length");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

/// Encode a byte slice as a lowercase hex string.
pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            use std::fmt::Write;
            let _ = write!(s, "{b:02x}");
            s
        })
}

// ── ZeroOnDrop ───────────────────────────────────────────────────────

/// RAII guard that zeroizes a `Vec<u8>` on drop via `write_volatile` +
/// `compiler_fence(SeqCst)`, preventing the compiler from eliding the wipe.
///
/// `Clone` is implemented explicitly so that secret material handed off
/// to transaction closures is never stored in a bare `Vec<u8>` — every
/// copy carries its own drop-time wipe.
pub(crate) struct ZeroOnDrop(pub Vec<u8>);

impl Clone for ZeroOnDrop {
    /// Deep-clone the secret into a fresh `ZeroOnDrop`. The clone owns
    /// its own buffer and is wiped independently on drop, so passing
    /// secret material into a transaction closure never leaves a
    /// non-zeroized copy behind.
    fn clone(&self) -> Self {
        ZeroOnDrop(self.0.clone())
    }
}

impl Drop for ZeroOnDrop {
    fn drop(&mut self) {
        for byte in self.0.iter_mut() {
            unsafe { std::ptr::write_volatile(byte, 0) };
        }
        std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
    }
}

// ── AuditOperation ───────────────────────────────────────────────────

/// Operation kind recorded in the audit log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditOperation {
    Insert,
    Update,
    Delete,
}

impl AuditOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            AuditOperation::Insert => "insert",
            AuditOperation::Update => "update",
            AuditOperation::Delete => "delete",
        }
    }
}

// ── ActorId ──────────────────────────────────────────────────────────

/// Identifies the actor who triggered an audited operation.
///
/// Supports numeric IDs (`i64`), UUID strings, arbitrary strings, or anonymous
/// (`None`). The actor representation is included in the HMAC message so that
/// any post-hoc modification of `actor_id` is detectable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorId {
    /// A numeric user/session ID (most common case).
    Int(i64),
    /// A UUID or any other string identifier.
    String(String),
    /// No actor — anonymous or system-initiated operation.
    None,
}

impl ActorId {
    /// Render the actor as the string embedded in the HMAC message and stored
    /// in the `actor_id` column (as TEXT when not an integer).
    pub fn as_hmac_str(&self) -> std::borrow::Cow<'_, str> {
        match self {
            ActorId::Int(id) => std::borrow::Cow::Owned(id.to_string()),
            ActorId::String(s) => std::borrow::Cow::Borrowed(s.as_str()),
            ActorId::None => std::borrow::Cow::Borrowed("null"),
        }
    }

    /// Convert to the `Value` stored in the `actor_id` column.
    ///
    /// `Int` → `Value::I64`, `String` → `Value::String`, `None` → `Value::Null`.
    pub fn to_value(&self) -> crate::value::Value {
        match self {
            ActorId::Int(id) => crate::value::Value::I64(*id),
            ActorId::String(s) => crate::value::Value::String(s.clone()),
            ActorId::None => crate::value::Value::Null,
        }
    }
}

impl From<i64> for ActorId {
    fn from(id: i64) -> Self {
        ActorId::Int(id)
    }
}
impl From<Option<i64>> for ActorId {
    fn from(opt: Option<i64>) -> Self {
        match opt {
            Some(id) => ActorId::Int(id),
            None => ActorId::None,
        }
    }
}
impl From<String> for ActorId {
    fn from(s: String) -> Self {
        ActorId::String(s)
    }
}
impl From<&str> for ActorId {
    fn from(s: &str) -> Self {
        ActorId::String(s.to_string())
    }
}

// ── SecretError ──────────────────────────────────────────────────────

/// Error returned by [`AuditContext::with_integrity`] when the HMAC secret
/// does not meet the minimum security requirements.
///
/// NIST SP 800-107 recommends that HMAC keys be at least as long as the hash
/// output (32 bytes for SHA-256). Shorter keys are technically valid per
/// RFC 2104 but provide reduced security margins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretError {
    /// The secret is empty. An empty key produces a deterministic HMAC that
    /// provides no integrity guarantee whatsoever.
    Empty,
}

impl std::fmt::Display for SecretError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecretError::Empty => f.write_str(
                "HMAC secret must not be empty; use at least 32 bytes (NIST SP 800-107)",
            ),
        }
    }
}

impl std::error::Error for SecretError {}

/// Minimum recommended HMAC-SHA256 key length (NIST SP 800-107).
pub const HMAC_MIN_KEY_BYTES: usize = 32;

// ── AuditContext ─────────────────────────────────────────────────────

/// Context passed to audited operations (who triggered the change).
///
/// # Fields (private — use constructors)
///
/// - `actor`: who triggered the change (see [`ActorId`])
/// - `hmac_secret`: when set, every audit row receives a `row_hash` column
///   containing an HMAC-SHA256 digest of the operation, actor, timestamp, and
///   row data, encoded as a lowercase hex string. This makes tampering with
///   **any** field (including `changed_at`) detectable.
/// - `dialect`: SQL dialect used for placeholder rewriting in audit INSERTs.
pub struct AuditContext {
    actor: ActorId,
    /// Optional HMAC-SHA256 secret. Zeroized on drop.
    hmac_secret: Option<ZeroOnDrop>,
    /// SQL dialect — controls `?` vs `$N` placeholders in audit INSERTs.
    dialect: crate::query::Dialect,
}

impl AuditContext {
    /// Create a context without integrity protection.
    ///
    /// `actor` accepts anything that converts to [`ActorId`]: `i64`,
    /// `Option<i64>`, `&str`, `String`, or `ActorId::None`.
    pub fn new(actor: impl Into<ActorId>) -> Self {
        Self {
            actor: actor.into(),
            hmac_secret: None,
            dialect: crate::query::Dialect::Generic,
        }
    }

    /// Create a context with HMAC-SHA256 integrity protection.
    ///
    /// Returns [`SecretError::Empty`] when `secret` is empty — an empty key
    /// provides no integrity guarantee.
    ///
    /// A `tracing::warn!` is emitted when the secret is shorter than
    /// [`HMAC_MIN_KEY_BYTES`] (32 bytes, per NIST SP 800-107) but still
    /// accepted, so callers can detect misconfiguration in logs without
    /// breaking existing deployments that use shorter keys.
    ///
    /// # Errors
    ///
    /// Returns [`SecretError::Empty`] if `secret` is zero bytes.
    pub fn with_integrity(
        actor: impl Into<ActorId>,
        secret: impl Into<Vec<u8>>,
    ) -> Result<Self, SecretError> {
        let secret = secret.into();
        if secret.is_empty() {
            return Err(SecretError::Empty);
        }
        if secret.len() < HMAC_MIN_KEY_BYTES {
            tracing::warn!(
                secret_len = secret.len(),
                min_recommended = HMAC_MIN_KEY_BYTES,
                "HMAC secret is shorter than the NIST-recommended {} bytes; \
                 consider using a longer key",
                HMAC_MIN_KEY_BYTES,
            );
        }
        Ok(Self {
            actor: actor.into(),
            hmac_secret: Some(ZeroOnDrop(secret)),
            dialect: crate::query::Dialect::Generic,
        })
    }

    /// Override the SQL dialect (default: `Generic` / `?` placeholders).
    ///
    /// Set to `Dialect::Postgres` when using a PostgreSQL backend so that
    /// audit INSERT statements use `$1, $2, …` placeholders.
    pub fn with_dialect(mut self, dialect: crate::query::Dialect) -> Self {
        self.dialect = dialect;
        self
    }

    /// Read the actor.
    pub fn actor(&self) -> &ActorId {
        &self.actor
    }

    /// `true` when an HMAC secret is configured.
    pub fn has_integrity(&self) -> bool {
        self.hmac_secret.is_some()
    }

    /// Compute the HMAC-SHA256 hex digest for an audit row, or `None` if no
    /// secret is configured.
    ///
    /// The signed message uses length-prefixed fields to prevent ambiguity:
    /// `"<op_len>:<op><actor_len>:<actor><ts_len>:<changed_at><data_len>:<row_data>"`
    ///
    /// `changed_at` is included so that antedating an audit row is detectable.
    pub fn compute_hash(
        &self,
        operation: &str,
        changed_at: &str,
        row_data: &str,
    ) -> Option<String> {
        let secret = self.hmac_secret.as_ref().map(|z| z.0.as_slice())?;
        let actor = self.actor.as_hmac_str();
        let message = build_hmac_message(operation, &actor, changed_at, row_data);
        Some(hex_encode(&hmac_sha256(secret, &message)))
    }
}

/// Return the current timestamp as an RFC 3339 UTC string, e.g.
/// `"2024-01-15T10:30:00.123456789Z"`.
///
/// Generated app-side so that `changed_at` can be bound as a parameter at
/// INSERT time and therefore covered by the HMAC signature. A DB-side
/// `NOW()` would only be known post-INSERT, leaving a window where an
/// attacker with DB write access could backdate the row and recompute
/// the hash over an empty timestamp field.
#[cfg(any(feature = "postgres", feature = "mysql"))]
fn current_changed_at() -> String {
    chrono::Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true)
}

/// Fallback timestamp when neither `postgres` nor `mysql` features are
/// enabled. Uses `SystemTime` since `UNIX_EPOCH` formatted as an
/// RFC 3339-like UTC string so it can still be signed.
#[cfg(not(any(feature = "postgres", feature = "mysql")))]
fn current_changed_at() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    // `secs.nanos` since epoch — deterministic and monotonic enough for
    // signing purposes; callers that need a real timestamp should enable
    // the `chrono`-backed features.
    format!("{}.{:09}Z", d.as_secs(), d.subsec_nanos())
}

/// Build an unambiguous HMAC message from the audit fields.
///
/// Binary length-prefixed encoding: each field is serialised as a big-endian
/// `u64` byte length followed by the raw UTF-8 bytes. There are no textual
/// delimiters, so no field value can forge a field boundary regardless of the
/// bytes it contains.
///
/// Layout (concatenated):
///
/// ```text
///   op_len_be_u64    || op_bytes
/// || actor_len_be_u64 || actor_bytes
/// || ts_len_be_u64    || changed_at_bytes
/// || data_len_be_u64  || row_data_bytes
/// ```
///
/// `changed_at` is included so that antedating a row is detectable even when
/// `row_data` and `operation` are unchanged.
fn build_hmac_message(operation: &str, actor: &str, changed_at: &str, row_data: &str) -> Vec<u8> {
    let op = operation.as_bytes();
    let ac = actor.as_bytes();
    let ts = changed_at.as_bytes();
    let rd = row_data.as_bytes();
    let mut out = Vec::with_capacity(32 + op.len() + ac.len() + ts.len() + rd.len());
    out.extend_from_slice(&(op.len() as u64).to_be_bytes());
    out.extend_from_slice(op);
    out.extend_from_slice(&(ac.len() as u64).to_be_bytes());
    out.extend_from_slice(ac);
    out.extend_from_slice(&(ts.len() as u64).to_be_bytes());
    out.extend_from_slice(ts);
    out.extend_from_slice(&(rd.len() as u64).to_be_bytes());
    out.extend_from_slice(rd);
    out
}

// ── Auditable trait ──────────────────────────────────────────────────

/// Implemented automatically by `#[table(audit)]`.
pub trait Auditable: Table {
    /// Name of the audit table (e.g. `"users_audit"`).
    fn audit_table_name() -> &'static str;
    /// Fixed column defs for the audit table.
    fn audit_column_defs() -> Vec<ColumnDef>;
}

// ── Fixed audit column defs ──────────────────────────────────────────

/// Returns the fixed column definitions for any audit table.
///
/// Columns (in order):
/// 1. `audit_id`   — `BIGSERIAL PRIMARY KEY`
/// 2. `operation`  — `TEXT NOT NULL CHECK (operation IN ('insert','update','delete'))`
/// 3. `actor_id`   — `TEXT NULL` (stores `i64`, UUID, or any string actor)
/// 4. `changed_at` — `TIMESTAMPTZ NOT NULL DEFAULT NOW()` (DB-side timestamp)
/// 5. `row_data`   — `JSONB NOT NULL` (before-image for UPDATE/DELETE, full row for INSERT)
/// 6. `row_hash`   — `TEXT NULL` (HMAC-SHA256 hex; NULL when no secret is configured)
///
/// **Column order is stable** — the HMAC covers `row_data` by position, so
/// changing this order would invalidate existing hashes.
pub fn audit_column_defs_for(table_name: &str) -> Vec<ColumnDef> {
    let _ = table_name; // reserved for future per-table customisation
    vec![
        ColumnDef {
            name: "audit_id",
            sql_type: SqlType::BigSerial,
            primary_key: true,
            auto_increment: true,
            unique: false,
            index: false,
            nullable: false,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: TimestampSource::Vm,
            check: None,
            foreign_key: None,
        },
        ColumnDef {
            name: "operation",
            sql_type: SqlType::Text,
            primary_key: false,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: false,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: TimestampSource::Vm,
            // Restrict to known operations — prevents arbitrary values being
            // inserted directly into the audit table.
            check: Some("operation IN ('insert','update','delete')".to_string()),
            foreign_key: None,
        },
        // actor_id is TEXT to support i64, UUID, or any string identifier.
        ColumnDef {
            name: "actor_id",
            sql_type: SqlType::Text,
            primary_key: false,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: true,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: TimestampSource::Vm,
            check: None,
            foreign_key: None,
        },
        // `changed_at` is bound by the application (RFC 3339 UTC) so it is
        // covered by the HMAC signature. The `NOW()` default remains so
        // legacy rows inserted without an explicit value still get a
        // server-side timestamp — but `audited_*` always passes one in.
        ColumnDef {
            name: "changed_at",
            sql_type: SqlType::Timestamptz,
            primary_key: false,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: false,
            default: Some(crate::schema::DefaultValue::Expr("NOW()")),
            computed: None,
            timestamp_kind: None,
            timestamp_source: TimestampSource::Db,
            check: None,
            foreign_key: None,
        },
        ColumnDef {
            name: "row_data",
            sql_type: SqlType::Jsonb,
            primary_key: false,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: false,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: TimestampSource::Vm,
            check: None,
            foreign_key: None,
        },
        // Integrity column — NULL when no HMAC secret is configured.
        ColumnDef {
            name: "row_hash",
            sql_type: SqlType::Text,
            primary_key: false,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: true,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: TimestampSource::Vm,
            check: None,
            foreign_key: None,
        },
    ]
}

// ── Integrity verification ────────────────────────────────────────────

/// Verify that a stored `row_hash` matches the expected HMAC-SHA256 for the
/// given audit row fields.
///
/// Returns `Some(true)` when the hash is valid, `Some(false)` when it has been
/// tampered with. When `stored_hash` is `None`:
/// - If `integrity_expected` is `true`, returns `Some(false)` (a missing hash
///   is treated as tampering — an attacker may have nullified it).
/// - If `integrity_expected` is `false`, returns `None` (integrity was never
///   enabled for this row, so there is nothing to verify).
///
/// `changed_at` must be the **exact string** stored in the `changed_at` column
/// (e.g. `"2024-01-15T10:30:00Z"`). It is included in the HMAC so that
/// antedating a row is detectable.
///
/// # Example
/// ```
/// use reify_core::audit::{AuditContext, ActorId, verify_audit_row};
///
/// let ctx = AuditContext::with_integrity(ActorId::Int(7), b"secret").unwrap();
/// let hash = ctx.compute_hash("delete", "2024-01-15T10:30:00Z", r#"{"id":1}"#).unwrap();
/// assert_eq!(
///     verify_audit_row(b"secret", "delete", "7", "2024-01-15T10:30:00Z", r#"{"id":1}"#, Some(&hash), false),
///     Some(true)
/// );
/// assert_eq!(
///     verify_audit_row(b"secret", "delete", "7", "2024-01-15T10:30:00Z", r#"{"id":1}"#, None, false),
///     None
/// );
/// ```
pub fn verify_audit_row(
    secret: &[u8],
    operation: &str,
    actor: &str,
    changed_at: &str,
    row_data: &str,
    stored_hash: Option<&str>,
    integrity_expected: bool,
) -> Option<bool> {
    match stored_hash {
        Some(stored) => {
            let message = build_hmac_message(operation, actor, changed_at, row_data);
            let expected = hex_encode(&hmac_sha256(secret, &message));
            // Constant-time comparison to prevent timing attacks.
            Some(constant_time_eq(expected.as_bytes(), stored.as_bytes()))
        }
        None if integrity_expected => Some(false),
        None => None,
    }
}

/// Constant-time byte-slice equality — prevents timing side-channels when
/// comparing MAC values.
///
/// Length-agnostic: a mismatch in length is folded into the accumulator
/// alongside the per-byte XOR so the running time depends only on
/// `max(a.len(), b.len())`, not on *where* the inputs diverge. Safe to
/// use with variable-length MACs (e.g. truncated HMAC, SHA-512) without
/// leaking the length of the secret value.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let len = a.len().max(b.len());
    let mut diff: u8 = (a.len() ^ b.len()) as u8
        | (((a.len() ^ b.len()) >> 8) as u8)
        | (((a.len() ^ b.len()) >> 16) as u8)
        | (((a.len() ^ b.len()) >> 24) as u8);
    for i in 0..len {
        let x = *a.get(i).unwrap_or(&0);
        let y = *b.get(i).unwrap_or(&0);
        diff |= x ^ y;
    }
    diff == 0
}

// ── JSON serialisation helper ────────────────────────────────────────

/// Escape a string for safe embedding in a JSON string literal per RFC 8259.
///
/// Escapes: `"`, `\`, and all C0 control characters (U+0000–U+001F).
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

/// Serialise column-value pairs to a JSON object string without any external crate.
///
/// ```
/// use reify_core::audit::values_to_json_string;
/// use reify_core::Value;
/// let json = values_to_json_string(&["id", "name"], &[Value::I64(1), Value::String("alice".into())]);
/// assert_eq!(json, r#"{"id":1,"name":"alice"}"#);
/// ```
pub fn values_to_json_string(cols: &[&str], vals: &[crate::value::Value]) -> String {
    use crate::value::Value;

    let mut out = String::from("{");
    for (i, (col, val)) in cols.iter().zip(vals.iter()).enumerate() {
        if i > 0 {
            out.push(',');
        }
        // key
        out.push('"');
        out.push_str(&json_escape(col));
        out.push_str("\":");
        // value
        match val {
            Value::Null => out.push_str("null"),
            Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Value::I16(n) => out.push_str(&n.to_string()),
            Value::I32(n) => out.push_str(&n.to_string()),
            Value::I64(n) => out.push_str(&n.to_string()),
            // Non-finite floats (NaN, ±Infinity) are not representable in
            // strict JSON — emit them as tagged string literals so no evidence
            // is silently discarded from the audit log. Consumers can parse
            // these back when integrity-verifying a row.
            Value::F32(f) => {
                if f.is_finite() {
                    out.push_str(&f.to_string());
                } else if f.is_nan() {
                    out.push_str("\"NaN\"");
                } else if *f > 0.0 {
                    out.push_str("\"Infinity\"");
                } else {
                    out.push_str("\"-Infinity\"");
                }
            }
            Value::F64(f) => {
                if f.is_finite() {
                    out.push_str(&f.to_string());
                } else if f.is_nan() {
                    out.push_str("\"NaN\"");
                } else if *f > 0.0 {
                    out.push_str("\"Infinity\"");
                } else {
                    out.push_str("\"-Infinity\"");
                }
            }
            Value::String(s) => {
                out.push('"');
                out.push_str(&json_escape(s));
                out.push('"');
            }
            Value::Bytes(b) => {
                // hex encoding for binary data
                out.push('"');
                for byte in b {
                    out.push_str(&format!("{byte:02x}"));
                }
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::Uuid(u) => {
                out.push('"');
                out.push_str(&u.to_string());
                out.push('"');
            }
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            Value::Timestamp(ts) => {
                out.push('"');
                out.push_str(&ts.to_string());
                out.push('"');
            }
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            Value::Date(d) => {
                out.push('"');
                out.push_str(&d.to_string());
                out.push('"');
            }
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            Value::Time(t) => {
                out.push('"');
                out.push_str(&t.to_string());
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::Timestamptz(ts) => {
                out.push('"');
                out.push_str(&ts.to_string());
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::Jsonb(j) => out.push_str(&j.to_string()),
            // Range and array types: serialize as quoted string representation
            #[cfg(feature = "postgres")]
            Value::Int4Range(r) => {
                out.push('"');
                out.push_str(&format!("{r:?}"));
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::Int8Range(r) => {
                out.push('"');
                out.push_str(&format!("{r:?}"));
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::TsRange(r) => {
                out.push('"');
                out.push_str(&format!("{r:?}"));
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::TstzRange(r) => {
                out.push('"');
                out.push_str(&format!("{r:?}"));
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::DateRange(r) => {
                out.push('"');
                out.push_str(&format!("{r:?}"));
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayBool(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 {
                        out.push(',');
                    }
                    out.push_str(if *v { "true" } else { "false" });
                }
                out.push(']');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayI16(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 {
                        out.push(',');
                    }
                    out.push_str(&v.to_string());
                }
                out.push(']');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayI32(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 {
                        out.push(',');
                    }
                    out.push_str(&v.to_string());
                }
                out.push(']');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayI64(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 {
                        out.push(',');
                    }
                    out.push_str(&v.to_string());
                }
                out.push(']');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayF32(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 {
                        out.push(',');
                    }
                    out.push_str(&v.to_string());
                }
                out.push(']');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayF64(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 {
                        out.push(',');
                    }
                    out.push_str(&v.to_string());
                }
                out.push(']');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayString(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 {
                        out.push(',');
                    }
                    out.push('"');
                    out.push_str(&json_escape(v));
                    out.push('"');
                }
                out.push(']');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayUuid(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 {
                        out.push(',');
                    }
                    out.push('"');
                    out.push_str(&v.to_string());
                    out.push('"');
                }
                out.push(']');
            }
        }
    }
    out.push('}');
    out
}

// ── audited_insert ───────────────────────────────────────────────────

/// Execute an INSERT and write an audit row atomically inside a transaction.
///
/// The full row (all writable columns) is serialised as JSON and stored in
/// `row_data`. When [`AuditContext::has_integrity`] is `true`, a `row_hash`
/// HMAC-SHA256 digest is also stored.
pub async fn audited_insert<M: Auditable + crate::db::FromRow>(
    db: &impl Database,
    builder: InsertBuilder<M>,
    ctx: &AuditContext,
) -> Result<u64, DbError> {
    let (insert_sql, insert_params) = builder.build();
    let audit_table = M::audit_table_name();
    let col_names: Vec<&'static str> = M::writable_column_names().to_vec();
    let actor = ctx.actor.clone();
    // Clone the secret into a ZeroOnDrop *immediately* so every copy is
    // wiped on drop — including the intermediate binding that lives
    // between here and the closure body.
    let secret_guard = ctx.hmac_secret.clone();
    let dialect = ctx.dialect;

    let affected = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let affected_clone = affected.clone();

    db.transaction(Box::new(move |tx| {
        // Move the ZeroOnDrop-wrapped secret into the future; its Drop
        // impl zeroizes on completion or panic.
        let secret_guard = secret_guard.clone();
        Box::pin(async move {
            use crate::db::DynDatabase;

            // 1. Execute the INSERT.
            let n = DynDatabase::execute(tx, &insert_sql, &insert_params).await?;
            affected_clone.store(n, std::sync::atomic::Ordering::Relaxed);

            // 2. Serialize the inserted values as JSON.
            let col_refs: Vec<&str> = col_names.iter().map(|s| *s).collect();
            let row_data = values_to_json_string(
                &col_refs,
                &insert_params.iter().cloned().collect::<Vec<_>>(),
            );

            // 3. Generate changed_at app-side so it is covered by the HMAC.
            //    A DB-side NOW() would only be known post-INSERT and therefore
            //    unsignable — an attacker with DB access could backdate the
            //    row and recompute the hash over an empty timestamp.
            let changed_at = current_changed_at();

            // 4. Compute HMAC over the bound changed_at value.
            let row_hash = if let Some(ref secret) = secret_guard {
                let actor_str = actor.as_hmac_str();
                let message = build_hmac_message(
                    AuditOperation::Insert.as_str(),
                    &actor_str,
                    &changed_at,
                    &row_data,
                );
                Some(hex_encode(&hmac_sha256(&secret.0, &message)))
            } else {
                None
            };

            // 5. Insert audit row with explicit changed_at parameter.
            let (audit_sql, audit_params) = build_audit_insert(
                audit_table,
                AuditOperation::Insert.as_str(),
                actor.to_value(),
                &changed_at,
                row_data,
                row_hash,
                dialect,
            );
            DynDatabase::execute(tx, &audit_sql, &audit_params).await?;
            Ok(())
        })
    }))
    .await?;

    Ok(affected.load(std::sync::atomic::Ordering::Relaxed))
}

// ── audited_update ───────────────────────────────────────────────────

/// Execute an UPDATE and write an audit row atomically inside a transaction.
///
/// Rows matching the WHERE clause are locked with `SELECT … FOR UPDATE`,
/// serialised as a **before-image**, then the UPDATE is applied. One audit row
/// per matched record is written containing both the before-image and the
/// after-image under the key `"before"` / `"after"` in `row_data`.
///
/// When [`AuditContext::has_integrity`] is `true`, each audit row receives a
/// `row_hash` HMAC-SHA256 digest covering the operation, actor, timestamp
/// placeholder, and row data — detectable via [`verify_audit_row`].
pub async fn audited_update<M: Auditable + crate::db::FromRow>(
    db: &impl Database,
    builder: UpdateBuilder<M>,
    ctx: &AuditContext,
) -> Result<u64, DbError> {
    // Build a SELECT … FOR UPDATE from the same WHERE conditions.
    let select = builder.to_select();
    let (select_sql_base, select_params) = select.build();
    let select_sql = format!("{select_sql_base} FOR UPDATE");

    let (update_sql, update_params) = builder.build();
    let audit_table = M::audit_table_name();
    let col_names: Vec<&'static str> = M::column_names().to_vec();
    let actor = ctx.actor.clone();
    // Clone the secret into a ZeroOnDrop *immediately* so every copy is
    // wiped on drop — including the intermediate binding that lives
    // between here and the closure body.
    let secret_guard = ctx.hmac_secret.clone();
    let dialect = ctx.dialect;

    let affected = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let affected_clone = affected.clone();

    db.transaction(Box::new(move |tx| {
        // Move the ZeroOnDrop-wrapped secret into the future; its Drop
        // impl zeroizes on completion or panic.
        let secret_guard = secret_guard.clone();
        Box::pin(async move {
            use crate::db::DynDatabase;

            // 1. Read matching rows inside the transaction (locked FOR UPDATE).
            let old_rows = DynDatabase::query(tx, &select_sql, &select_params).await?;

            // 2. Serialize before-images.
            let col_refs: Vec<&str> = col_names.iter().map(|s| *s).collect();
            let mut before_images: Vec<String> = Vec::with_capacity(old_rows.len());
            for row in &old_rows {
                let vals: Vec<crate::value::Value> = col_names
                    .iter()
                    .map(|c| row.get(c).cloned().unwrap_or(crate::value::Value::Null))
                    .collect();
                before_images.push(values_to_json_string(&col_refs, &vals));
            }

            // 3. Apply the UPDATE.
            let n = DynDatabase::execute(tx, &update_sql, &update_params).await?;
            affected_clone.store(n, std::sync::atomic::Ordering::Relaxed);

            // 4. Read after-images for the same rows.
            let after_rows = DynDatabase::query(tx, &select_sql_base, &select_params).await?;

            // 5. Build combined row_data {"before":{...},"after":{...}} and compute HMACs.
            //    changed_at is generated app-side so it is covered by the signature.
            let mut entries: Vec<(String, String, Option<String>)> =
                Vec::with_capacity(before_images.len());
            for (before, after_row) in before_images.iter().zip(after_rows.iter()) {
                let after_vals: Vec<crate::value::Value> = col_names
                    .iter()
                    .map(|c| {
                        after_row
                            .get(c)
                            .cloned()
                            .unwrap_or(crate::value::Value::Null)
                    })
                    .collect();
                let after = values_to_json_string(&col_refs, &after_vals);
                let row_data = format!("{{\"before\":{before},\"after\":{after}}}");
                let changed_at = current_changed_at();

                let row_hash = if let Some(ref secret) = secret_guard {
                    let actor_str = actor.as_hmac_str();
                    let message = build_hmac_message(
                        AuditOperation::Update.as_str(),
                        &actor_str,
                        &changed_at,
                        &row_data,
                    );
                    Some(hex_encode(&hmac_sha256(&secret.0, &message)))
                } else {
                    None
                };
                entries.push((changed_at, row_data, row_hash));
            }

            // 6. Insert one audit row per before+after pair.
            for (changed_at, row_data, row_hash) in entries {
                let (audit_sql, audit_params) = build_audit_insert(
                    audit_table,
                    AuditOperation::Update.as_str(),
                    actor.to_value(),
                    &changed_at,
                    row_data,
                    row_hash,
                    dialect,
                );
                DynDatabase::execute(tx, &audit_sql, &audit_params).await?;
            }
            Ok(())
        })
    }))
    .await?;

    Ok(affected.load(std::sync::atomic::Ordering::Relaxed))
}

// ── audited_delete ───────────────────────────────────────────────────

/// SELECT matching rows, DELETE them, and write one audit row per deleted record —
/// all inside a single transaction.
///
/// The SELECT (`FOR UPDATE`), DELETE, and audit INSERTs all execute within the
/// same transaction to prevent TOCTOU races. When [`AuditContext::has_integrity`]
/// is `true`, each audit row receives a `row_hash` HMAC-SHA256 digest.
pub async fn audited_delete<M: Auditable + FromRow>(
    db: &impl Database,
    builder: DeleteBuilder<M>,
    ctx: &AuditContext,
) -> Result<u64, DbError> {
    let select = builder.to_select();
    let (select_sql_base, select_params) = select.build();
    // Lock rows for the duration of the transaction.
    let select_sql = format!("{select_sql_base} FOR UPDATE");
    let (delete_sql, delete_params) = builder.build();
    let audit_table = M::audit_table_name();
    let col_names: Vec<&'static str> = M::column_names().to_vec();
    let actor = ctx.actor.clone();
    // Clone the secret into a ZeroOnDrop *immediately* so every copy is
    // wiped on drop — including the intermediate binding that lives
    // between here and the closure body.
    let secret_guard = ctx.hmac_secret.clone();
    let dialect = ctx.dialect;

    let affected = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let affected_clone = affected.clone();

    db.transaction(Box::new(move |tx| {
        // Move the ZeroOnDrop-wrapped secret into the future; its Drop
        // impl zeroizes on completion or panic.
        let secret_guard = secret_guard.clone();
        Box::pin(async move {
            use crate::db::DynDatabase;

            // 1. Read matching rows inside the transaction (locked FOR UPDATE).
            let old_rows = DynDatabase::query(tx, &select_sql, &select_params).await?;

            // 2. Serialize rows and compute HMACs. changed_at is generated
            //    app-side so it is covered by the signature.
            let col_refs: Vec<&str> = col_names.iter().map(|s| *s).collect();
            let mut entries: Vec<(String, String, Option<String>)> =
                Vec::with_capacity(old_rows.len());
            for row in &old_rows {
                let vals: Vec<crate::value::Value> = col_names
                    .iter()
                    .map(|c| row.get(c).cloned().unwrap_or(crate::value::Value::Null))
                    .collect();
                let row_data = values_to_json_string(&col_refs, &vals);
                let changed_at = current_changed_at();
                let row_hash = if let Some(ref secret) = secret_guard {
                    let actor_str = actor.as_hmac_str();
                    let message = build_hmac_message(
                        AuditOperation::Delete.as_str(),
                        &actor_str,
                        &changed_at,
                        &row_data,
                    );
                    Some(hex_encode(&hmac_sha256(&secret.0, &message)))
                } else {
                    None
                };
                entries.push((changed_at, row_data, row_hash));
            }

            // 3. Delete the rows.
            let n = DynDatabase::execute(tx, &delete_sql, &delete_params).await?;
            affected_clone.store(n, std::sync::atomic::Ordering::Relaxed);

            // 4. Insert audit rows.
            for (changed_at, row_data, row_hash) in entries {
                let (audit_sql, audit_params) = build_audit_insert(
                    audit_table,
                    AuditOperation::Delete.as_str(),
                    actor.to_value(),
                    &changed_at,
                    row_data,
                    row_hash,
                    dialect,
                );
                DynDatabase::execute(tx, &audit_sql, &audit_params).await?;
            }
            Ok(())
        })
    }))
    .await?;

    Ok(affected.load(std::sync::atomic::Ordering::Relaxed))
}

// ── Internal helpers ─────────────────────────────────────────────────

/// Build the audit INSERT SQL and parameter list.
///
/// When `row_hash` is `Some`, includes the `row_hash` column; otherwise omits
/// it so that existing audit tables without the column still work.
///
/// The `dialect` parameter controls placeholder style: `?` for Generic/MySQL,
/// `$1, $2, …` for PostgreSQL.
fn build_audit_insert(
    audit_table: &str,
    operation: &str,
    actor_val: crate::value::Value,
    changed_at: &str,
    row_data: String,
    row_hash: Option<String>,
    dialect: crate::query::Dialect,
) -> (String, Vec<crate::value::Value>) {
    // `changed_at` is bound by the caller (RFC 3339 UTC) so it is covered
    // by the HMAC signature at INSERT time. This closes the antedating
    // window where the DB-side `NOW()` was only known post-INSERT and
    // therefore not signed.
    let (sql, params) = if let Some(hash) = row_hash {
        let sql = format!(
            "INSERT INTO {} (\"operation\", \"actor_id\", \"changed_at\", \"row_data\", \"row_hash\") VALUES (?, ?, ?, ?, ?)",
            qi(audit_table)
        );
        let params = vec![
            crate::value::Value::String(operation.into()),
            actor_val,
            crate::value::Value::String(changed_at.to_owned()),
            crate::value::Value::String(row_data),
            crate::value::Value::String(hash),
        ];
        (sql, params)
    } else {
        let sql = format!(
            "INSERT INTO {} (\"operation\", \"actor_id\", \"changed_at\", \"row_data\") VALUES (?, ?, ?, ?)",
            qi(audit_table)
        );
        let params = vec![
            crate::value::Value::String(operation.into()),
            actor_val,
            crate::value::Value::String(changed_at.to_owned()),
            crate::value::Value::String(row_data),
        ];
        (sql, params)
    };
    // Rewrite ? placeholders to $N for PostgreSQL.
    if dialect == crate::query::Dialect::Postgres {
        use std::fmt::Write as _;
        let mut rewritten = String::with_capacity(sql.len() + 16);
        let mut idx = 1u32;
        for ch in sql.chars() {
            if ch == '?' {
                let _ = write!(rewritten, "${idx}");
                idx += 1;
            } else {
                rewritten.push(ch);
            }
        }
        return (rewritten, params);
    }
    (sql, params)
}

// ── Unit tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn test_values_to_json_string_basic() {
        let json = values_to_json_string(
            &["id", "name", "active"],
            &[
                Value::I64(42),
                Value::String("alice".into()),
                Value::Bool(true),
            ],
        );
        assert_eq!(json, r#"{"id":42,"name":"alice","active":true}"#);
    }

    #[test]
    fn test_values_to_json_string_null() {
        let json = values_to_json_string(&["x"], &[Value::Null]);
        assert_eq!(json, r#"{"x":null}"#);
    }

    #[test]
    fn test_values_to_json_string_empty() {
        let json = values_to_json_string(&[], &[]);
        assert_eq!(json, "{}");
    }

    #[test]
    fn test_values_to_json_string_escaping() {
        let json = values_to_json_string(&["msg"], &[Value::String(r#"say "hi""#.into())]);
        assert_eq!(json, r#"{"msg":"say \"hi\""}"#);
    }

    #[test]
    fn test_values_to_json_string_non_finite_f32() {
        let json = values_to_json_string(
            &["a", "b", "c"],
            &[
                Value::F32(f32::INFINITY),
                Value::F32(f32::NEG_INFINITY),
                Value::F32(f32::NAN),
            ],
        );
        // Non-finite floats are tagged string literals so the audit log
        // preserves the evidence instead of silently dropping it to `null`.
        assert_eq!(json, r#"{"a":"Infinity","b":"-Infinity","c":"NaN"}"#);
    }

    #[test]
    fn test_values_to_json_string_non_finite_f64() {
        let json = values_to_json_string(
            &["x", "y"],
            &[Value::F64(f64::INFINITY), Value::F64(f64::NAN)],
        );
        assert_eq!(json, r#"{"x":"Infinity","y":"NaN"}"#);
    }

    #[test]
    fn test_values_to_json_string_finite_floats() {
        let json = values_to_json_string(
            &["f32", "f64"],
            &[Value::F32(1.5), Value::F64(std::f64::consts::PI)],
        );
        assert!(json.contains("1.5"), "expected 1.5 in: {json}");
        assert!(json.contains("3.14"), "expected 3.14 in: {json}");
    }

    #[test]
    fn test_audit_column_defs_count() {
        let defs = audit_column_defs_for("users");
        assert_eq!(defs.len(), 6);
        assert_eq!(defs[0].name, "audit_id");
        assert_eq!(defs[1].name, "operation");
        assert_eq!(defs[2].name, "actor_id");
        assert_eq!(defs[3].name, "changed_at");
        assert_eq!(defs[4].name, "row_data");
        assert_eq!(defs[5].name, "row_hash");
    }

    #[test]
    fn test_audit_column_defs_types() {
        let defs = audit_column_defs_for("users");
        assert_eq!(defs[0].sql_type, SqlType::BigSerial);
        assert!(defs[0].primary_key);
        assert_eq!(defs[1].sql_type, SqlType::Text);
        // operation has a CHECK constraint
        assert!(defs[1].check.as_deref().unwrap_or("").contains("insert"));
        // actor_id is now TEXT (supports i64, UUID, string)
        assert_eq!(defs[2].sql_type, SqlType::Text);
        assert!(defs[2].nullable);
        assert_eq!(defs[3].sql_type, SqlType::Timestamptz);
        assert_eq!(defs[3].default, Some(crate::schema::DefaultValue::Expr("NOW()")));
        assert_eq!(defs[4].sql_type, SqlType::Jsonb);
        assert_eq!(defs[5].sql_type, SqlType::Text);
        assert!(defs[5].nullable);
    }

    // ── SHA-256 / HMAC-SHA256 ────────────────────────────────────────

    #[test]
    fn test_actor_id_as_hmac_str() {
        assert_eq!(ActorId::Int(42).as_hmac_str(), "42");
        assert_eq!(ActorId::String("uuid-abc".into()).as_hmac_str(), "uuid-abc");
        assert_eq!(ActorId::None.as_hmac_str(), "null");
    }

    #[test]
    fn test_actor_id_to_value() {
        use crate::value::Value;
        assert_eq!(ActorId::Int(7).to_value(), Value::I64(7));
        assert_eq!(
            ActorId::String("x".into()).to_value(),
            Value::String("x".into())
        );
        assert_eq!(ActorId::None.to_value(), Value::Null);
    }

    #[test]
    fn test_audit_context_private_fields() {
        // Constructors work; fields are not directly accessible.
        let ctx = AuditContext::new(ActorId::Int(1));
        assert!(!ctx.has_integrity());
        assert_eq!(ctx.actor(), &ActorId::Int(1));

        let ctx2 = AuditContext::with_integrity(ActorId::None, b"s").unwrap();
        assert!(ctx2.has_integrity());
    }

    #[test]
    fn test_audit_context_dialect() {
        use crate::query::Dialect;
        let ctx = AuditContext::new(ActorId::None).with_dialect(Dialect::Postgres);
        assert_eq!(ctx.dialect, Dialect::Postgres);
    }

    // ── SecretError / with_integrity validation ───────────────────────────

    #[test]
    fn test_with_integrity_empty_secret_is_error() {
        let result = AuditContext::with_integrity(ActorId::Int(1), b"".to_vec());
        assert_eq!(result.err(), Some(SecretError::Empty));
    }

    #[test]
    fn test_with_integrity_empty_secret_display() {
        let msg = SecretError::Empty.to_string();
        assert!(
            msg.contains("empty"),
            "display should mention 'empty': {msg}"
        );
        assert!(msg.contains("32"), "display should mention 32 bytes: {msg}");
    }

    #[test]
    fn test_with_integrity_short_secret_is_ok() {
        // Secrets shorter than HMAC_MIN_KEY_BYTES are accepted (only a warning
        // is emitted via tracing). The returned context must be functional.
        let ctx = AuditContext::with_integrity(ActorId::Int(1), b"short").unwrap();
        assert!(ctx.has_integrity());
        // Must still produce a valid 64-char hex hash.
        let h = ctx.compute_hash("insert", "", "{}").unwrap();
        assert_eq!(h.len(), 64);
    }

    #[test]
    fn test_with_integrity_exact_min_length_is_ok() {
        let secret = vec![0xabu8; HMAC_MIN_KEY_BYTES]; // exactly 32 bytes
        let ctx = AuditContext::with_integrity(ActorId::Int(1), secret).unwrap();
        assert!(ctx.has_integrity());
    }

    #[test]
    fn test_with_integrity_long_secret_is_ok() {
        let secret = vec![0x42u8; 64]; // 64 bytes — above threshold
        let ctx = AuditContext::with_integrity(ActorId::None, secret).unwrap();
        assert!(ctx.has_integrity());
    }

    #[test]
    fn test_secret_error_is_std_error() {
        // Ensure SecretError implements std::error::Error (compile-time check).
        fn assert_error<E: std::error::Error>(_: &E) {}
        assert_error(&SecretError::Empty);
    }

    /// Known-answer test: SHA-256(\"\") = e3b0c44298fc1c149afb...
    #[test]
    fn test_sha256_empty() {
        let digest = sha256(b"");
        let hex = hex_encode(&digest);
        assert_eq!(
            hex,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    /// Known-answer test: SHA-256("abc") = ba7816bf8f01cfea414140...
    #[test]
    fn test_sha256_abc() {
        let digest = sha256(b"abc");
        let hex = hex_encode(&digest);
        assert_eq!(
            hex,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    /// HMAC-SHA256 known-answer from RFC 4231 test vector #1.
    /// Key  = 0x0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b (20 bytes)
    /// Data = "Hi There"
    /// Expected = b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7
    #[test]
    fn test_hmac_sha256_rfc4231_vector1() {
        let key = [0x0bu8; 20];
        let mac = hmac_sha256(&key, b"Hi There");
        let hex = hex_encode(&mac);
        assert_eq!(
            hex,
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    /// HMAC-SHA256 RFC 4231 test vector #2.
    /// Key  = "Jefe"
    /// Data = "what do ya want for nothing?"
    /// Expected = 5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964a72424
    #[test]
    fn test_hmac_sha256_rfc4231_vector2() {
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        let hex = hex_encode(&mac);
        assert_eq!(
            hex,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    // ── AuditContext::compute_hash ────────────────────────────────────

    #[test]
    fn test_compute_hash_none_when_no_secret() {
        let ctx = AuditContext::new(ActorId::Int(1));
        assert!(ctx.compute_hash("update", "", "{}").is_none());
    }

    #[test]
    fn test_compute_hash_deterministic() {
        let ctx = AuditContext::with_integrity(ActorId::Int(42), b"my-secret").unwrap();
        let h1 = ctx
            .compute_hash("delete", "2024-01-01T00:00:00Z", r#"{"id":1}"#)
            .unwrap();
        let h2 = ctx
            .compute_hash("delete", "2024-01-01T00:00:00Z", r#"{"id":1}"#)
            .unwrap();
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64); // 32 bytes → 64 hex chars
    }

    #[test]
    fn test_compute_hash_differs_on_operation() {
        let ctx = AuditContext::with_integrity(ActorId::Int(1), b"secret").unwrap();
        let h_update = ctx.compute_hash("update", "", "{}").unwrap();
        let h_delete = ctx.compute_hash("delete", "", "{}").unwrap();
        assert_ne!(h_update, h_delete);
    }

    #[test]
    fn test_compute_hash_differs_on_actor() {
        let ctx1 = AuditContext::with_integrity(ActorId::Int(1), b"secret").unwrap();
        let ctx2 = AuditContext::with_integrity(ActorId::Int(2), b"secret").unwrap();
        assert_ne!(
            ctx1.compute_hash("update", "", "{}").unwrap(),
            ctx2.compute_hash("update", "", "{}").unwrap()
        );
    }

    #[test]
    fn test_compute_hash_differs_on_changed_at() {
        let ctx = AuditContext::with_integrity(ActorId::Int(1), b"secret").unwrap();
        let h1 = ctx
            .compute_hash("delete", "2024-01-01T00:00:00Z", "{}")
            .unwrap();
        let h2 = ctx
            .compute_hash("delete", "2024-06-01T00:00:00Z", "{}")
            .unwrap();
        assert_ne!(h1, h2, "changed_at must affect the hash");
    }

    #[test]
    fn test_compute_hash_null_actor() {
        let ctx = AuditContext::with_integrity(ActorId::None, b"secret").unwrap();
        let h = ctx.compute_hash("update", "", "{}");
        assert!(h.is_some());
    }

    #[test]
    fn test_compute_hash_string_actor() {
        let ctx =
            AuditContext::with_integrity(ActorId::String("uuid-abc".into()), b"secret").unwrap();
        let h = ctx.compute_hash("update", "", "{}");
        assert!(h.is_some());
        // Must differ from Int actor with same string representation
        let ctx2 = AuditContext::with_integrity(ActorId::Int(0), b"secret").unwrap();
        assert_ne!(h.unwrap(), ctx2.compute_hash("update", "", "{}").unwrap());
    }

    // ── verify_audit_row ─────────────────────────────────────────────

    const TS: &str = "2024-01-15T10:30:00Z";

    #[test]
    fn test_verify_audit_row_valid() {
        let ctx = AuditContext::with_integrity(ActorId::Int(7), b"secret").unwrap();
        let hash = ctx.compute_hash("delete", TS, r#"{"id":1}"#).unwrap();
        assert_eq!(
            verify_audit_row(
                b"secret",
                "delete",
                "7",
                TS,
                r#"{"id":1}"#,
                Some(&hash),
                false
            ),
            Some(true)
        );
    }

    #[test]
    fn test_verify_audit_row_tampered_data() {
        let ctx = AuditContext::with_integrity(ActorId::Int(7), b"secret").unwrap();
        let hash = ctx.compute_hash("delete", TS, r#"{"id":1}"#).unwrap();
        assert_eq!(
            verify_audit_row(
                b"secret",
                "delete",
                "7",
                TS,
                r#"{"id":99}"#,
                Some(&hash),
                false
            ),
            Some(false)
        );
    }

    #[test]
    fn test_verify_audit_row_tampered_operation() {
        let ctx = AuditContext::with_integrity(ActorId::Int(7), b"secret").unwrap();
        let hash = ctx.compute_hash("delete", TS, r#"{"id":1}"#).unwrap();
        assert_eq!(
            verify_audit_row(
                b"secret",
                "update",
                "7",
                TS,
                r#"{"id":1}"#,
                Some(&hash),
                false
            ),
            Some(false)
        );
    }

    #[test]
    fn test_verify_audit_row_tampered_actor() {
        let ctx = AuditContext::with_integrity(ActorId::Int(7), b"secret").unwrap();
        let hash = ctx.compute_hash("delete", TS, r#"{"id":1}"#).unwrap();
        assert_eq!(
            verify_audit_row(
                b"secret",
                "delete",
                "99",
                TS,
                r#"{"id":1}"#,
                Some(&hash),
                false
            ),
            Some(false)
        );
    }

    #[test]
    fn test_verify_audit_row_tampered_changed_at() {
        let ctx = AuditContext::with_integrity(ActorId::Int(7), b"secret").unwrap();
        let hash = ctx.compute_hash("delete", TS, r#"{"id":1}"#).unwrap();
        assert_eq!(
            verify_audit_row(
                b"secret",
                "delete",
                "7",
                "2099-01-01T00:00:00Z",
                r#"{"id":1}"#,
                Some(&hash),
                false
            ),
            Some(false)
        );
    }

    #[test]
    fn test_verify_audit_row_no_hash_stored() {
        assert_eq!(
            verify_audit_row(b"secret", "delete", "7", TS, r#"{"id":1}"#, None, false),
            None
        );
    }

    #[test]
    fn test_verify_audit_row_wrong_secret() {
        let ctx = AuditContext::with_integrity(ActorId::Int(7), b"correct-secret").unwrap();
        let hash = ctx.compute_hash("delete", TS, r#"{"id":1}"#).unwrap();
        assert_eq!(
            verify_audit_row(
                b"wrong-secret",
                "delete",
                "7",
                TS,
                r#"{"id":1}"#,
                Some(&hash),
                false
            ),
            Some(false)
        );
    }

    // ── constant_time_eq ─────────────────────────────────────────────

    #[test]
    fn test_constant_time_eq_equal() {
        assert!(constant_time_eq(b"hello", b"hello"));
    }

    #[test]
    fn test_constant_time_eq_different() {
        assert!(!constant_time_eq(b"hello", b"world"));
    }

    #[test]
    fn test_constant_time_eq_different_lengths() {
        // In debug builds, this triggers a debug_assert (panic).
        // In release, unequal lengths return false.
        let result = std::panic::catch_unwind(|| constant_time_eq(b"hi", b"hello"));
        match result {
            Ok(false) => {} // release mode: returns false
            Err(_) => {}    // debug mode: debug_assert panics
            other => panic!("unexpected result: {other:?}"),
        }
    }

    // ── integrity_expected flag ───────────────────────────────────────

    #[test]
    fn test_verify_audit_row_null_hash_with_integrity_expected() {
        assert_eq!(
            verify_audit_row(b"secret", "delete", "7", TS, r#"{"id":1}"#, None, true),
            Some(false)
        );
    }

    #[test]
    fn test_verify_audit_row_null_hash_without_integrity_expected() {
        assert_eq!(
            verify_audit_row(b"secret", "delete", "7", TS, r#"{"id":1}"#, None, false),
            None
        );
    }

    // ── hex_encode ────────────────────────────────────────────────────────

    #[test]
    fn test_hex_encode_empty() {
        assert_eq!(hex_encode(&[]), "");
    }

    #[test]
    fn test_hex_encode_single_byte_boundaries() {
        assert_eq!(hex_encode(&[0x00]), "00");
        assert_eq!(hex_encode(&[0x0f]), "0f");
        assert_eq!(hex_encode(&[0xff]), "ff");
    }

    #[test]
    fn test_hex_encode_known_bytes() {
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }

    #[test]
    fn test_hex_encode_all_zeros() {
        assert_eq!(hex_encode(&[0u8; 32]), "0".repeat(64));
    }

    #[test]
    fn test_hex_encode_all_ones() {
        assert_eq!(hex_encode(&[0xffu8; 32]), "f".repeat(64));
    }

    #[test]
    fn test_hex_encode_large_input() {
        // 1 MiB of cycling bytes — verifies no allocation panic and correct length.
        let data: Vec<u8> = (0u8..=255).cycle().take(1024 * 1024).collect();
        let hex = hex_encode(&data);
        assert_eq!(hex.len(), data.len() * 2);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(&hex[..2], "00");
        assert_eq!(&hex[hex.len() - 2..], "ff");
    }

    #[test]
    fn test_hex_encode_output_is_lowercase() {
        // Uppercase input bytes must produce lowercase hex digits.
        assert_eq!(hex_encode(&[0xAB, 0xCD, 0xEF]), "abcdef");
    }

    // ── AuditOperation ────────────────────────────────────────────────────

    #[test]
    fn test_audit_operation_as_str_all_variants() {
        assert_eq!(AuditOperation::Insert.as_str(), "insert");
        assert_eq!(AuditOperation::Update.as_str(), "update");
        assert_eq!(AuditOperation::Delete.as_str(), "delete");
    }

    #[test]
    fn test_audit_operation_as_str_matches_check_constraint() {
        // Every variant must appear in the CHECK constraint of the `operation` column.
        let defs = audit_column_defs_for("t");
        let check = defs[1].check.as_deref().unwrap_or("");
        for op in [
            AuditOperation::Insert,
            AuditOperation::Update,
            AuditOperation::Delete,
        ] {
            assert!(
                check.contains(op.as_str()),
                "CHECK constraint missing '{}': {check}",
                op.as_str()
            );
        }
    }

    #[test]
    fn test_audit_operation_eq_and_clone() {
        assert_eq!(AuditOperation::Insert, AuditOperation::Insert);
        assert_ne!(AuditOperation::Insert, AuditOperation::Delete);
        let op = AuditOperation::Update;
        assert_eq!(op, op.clone());
    }

    #[test]
    fn test_audit_operation_debug() {
        assert_eq!(format!("{:?}", AuditOperation::Insert), "Insert");
        assert_eq!(format!("{:?}", AuditOperation::Update), "Update");
        assert_eq!(format!("{:?}", AuditOperation::Delete), "Delete");
    }

    // ── constant_time_eq (extended) ───────────────────────────────────────

    #[test]
    fn test_constant_time_eq_empty_slices() {
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn test_constant_time_eq_single_byte_equal() {
        assert!(constant_time_eq(b"x", b"x"));
    }

    #[test]
    fn test_constant_time_eq_single_byte_different() {
        assert!(!constant_time_eq(b"a", b"b"));
    }

    #[test]
    fn test_constant_time_eq_differs_only_in_last_byte() {
        let a = b"hello world!";
        let mut b = *a;
        b[b.len() - 1] ^= 0x01;
        assert!(!constant_time_eq(a, &b));
    }

    #[test]
    fn test_constant_time_eq_differs_only_in_first_byte() {
        let a = b"hello world!";
        let mut b = *a;
        b[0] ^= 0x01;
        assert!(!constant_time_eq(a, &b));
    }

    #[test]
    fn test_constant_time_eq_all_zeros_vs_one_bit() {
        let a = [0u8; 32];
        let mut b = [0u8; 32];
        b[15] = 1;
        assert!(!constant_time_eq(&a, &b));
    }

    #[test]
    fn test_constant_time_eq_hmac_hex_length() {
        // Real comparison path: 64-char hex strings produced by hex_encode(hmac_sha256(...)).
        let mac_a = hex_encode(&[0xabu8; 32]);
        let mac_b = hex_encode(&[0xabu8; 32]);
        let mac_c = hex_encode(&[0xcdu8; 32]);
        assert!(constant_time_eq(mac_a.as_bytes(), mac_b.as_bytes()));
        assert!(!constant_time_eq(mac_a.as_bytes(), mac_c.as_bytes()));
    }

    // ── JSON escaping ─────────────────────────────────────────────────

    #[test]
    fn test_values_to_json_string_control_chars() {
        let json = values_to_json_string(
            &["newline", "tab", "null_byte"],
            &[
                Value::String("hello\nworld".into()),
                Value::String("a\tb".into()),
                Value::String("x\x00y".into()),
            ],
        );
        assert_eq!(
            json,
            r#"{"newline":"hello\nworld","tab":"a\tb","null_byte":"x\u0000y"}"#
        );
    }
}
