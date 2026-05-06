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

use hmac::{Hmac, KeyInit, Mac};
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
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(key)
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
    /// Optional external append-only log destination. When set, every
    /// audit row is mirrored to the sink **inside** the audited
    /// transaction; a sink failure rolls back the whole transaction so
    /// no DML row exists in the database without a corresponding sink
    /// entry. See [`AuditSink`] for the contract.
    sink: Option<std::sync::Arc<dyn AuditSink>>,
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
            sink: None,
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
            sink: None,
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

    /// Mirror every audit row to an external append-only log.
    ///
    /// The sink is invoked **inside** the audited transaction, after the
    /// audit row has been inserted in the DB. A sink error is propagated
    /// as a `DbError`, which rolls back the transaction — the audited
    /// DML and the audit row are both undone, so the database can never
    /// commit a row that the sink rejected. This is the strongest
    /// SOC2 / PCI-DSS guarantee, traded against holding the DB
    /// connection while the sink runs (slow remote sinks effectively
    /// gate write throughput).
    ///
    /// Pass `Arc<MyBackend>` for cheap cloning across the request path.
    /// See [`AuditSink`] and [`NoopAuditSink`].
    pub fn with_sink(mut self, sink: std::sync::Arc<dyn AuditSink>) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Currently configured external sink, if any.
    pub fn sink(&self) -> Option<&std::sync::Arc<dyn AuditSink>> {
        self.sink.as_ref()
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
        self.compute_hash_chained(operation, changed_at, row_data, None)
    }

    /// Compute a chained HMAC-SHA256 over the audit fields and an optional
    /// previous-row hash.
    ///
    /// When `prev_hash` is `None`, the resulting hash matches
    /// [`AuditContext::compute_hash`] (legacy 4-field layout), so chains can
    /// start from rows written before chaining was enabled. When `Some`, the
    /// previous-row hash is folded into the signed message — corrupting the
    /// previous row, deleting it, or skipping a chain link all break
    /// verification.
    pub fn compute_hash_chained(
        &self,
        operation: &str,
        changed_at: &str,
        row_data: &str,
        prev_hash: Option<&str>,
    ) -> Option<String> {
        let secret = self.hmac_secret.as_ref().map(|z| z.0.as_slice())?;
        let actor = self.actor.as_hmac_str();
        let message =
            build_hmac_message_chained(operation, &actor, changed_at, prev_hash, row_data);
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
///
/// This is the legacy 4-field layout — kept for backward compatibility with
/// audit rows written before hash chaining was added. New rows use
/// [`build_hmac_message_chained`] when a previous-row hash is available.
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

/// Build an HMAC message that chains to a previous row's `row_hash`.
///
/// Layout (concatenated, length-prefixed):
///
/// ```text
///   op_len_be_u64    || op_bytes
/// || actor_len_be_u64 || actor_bytes
/// || ts_len_be_u64    || changed_at_bytes
/// || prev_len_be_u64  || prev_hash_bytes
/// || data_len_be_u64  || row_data_bytes
/// ```
///
/// When `prev_hash` is `None`, falls back to the legacy 4-field layout in
/// [`build_hmac_message`] so audit rows pre-dating the chain feature continue
/// to verify with their original signatures. The first row of a chain
/// always has `prev_hash = None`.
fn build_hmac_message_chained(
    operation: &str,
    actor: &str,
    changed_at: &str,
    prev_hash: Option<&str>,
    row_data: &str,
) -> Vec<u8> {
    let prev = match prev_hash {
        // No previous row — preserve the legacy layout so existing
        // signatures continue to verify.
        None => return build_hmac_message(operation, actor, changed_at, row_data),
        Some(p) => p,
    };
    let op = operation.as_bytes();
    let ac = actor.as_bytes();
    let ts = changed_at.as_bytes();
    let pv = prev.as_bytes();
    let rd = row_data.as_bytes();
    let mut out = Vec::with_capacity(40 + op.len() + ac.len() + ts.len() + pv.len() + rd.len());
    out.extend_from_slice(&(op.len() as u64).to_be_bytes());
    out.extend_from_slice(op);
    out.extend_from_slice(&(ac.len() as u64).to_be_bytes());
    out.extend_from_slice(ac);
    out.extend_from_slice(&(ts.len() as u64).to_be_bytes());
    out.extend_from_slice(ts);
    out.extend_from_slice(&(pv.len() as u64).to_be_bytes());
    out.extend_from_slice(pv);
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

// ── AuditSink — append-only external audit log ───────────────────────

/// One entry sent to an [`AuditSink`].
///
/// Mirrors the audit row that was just inserted in the database, with
/// the same byte-perfect `row_data` and `row_hash` fields so a downstream
/// verifier can reconstruct the HMAC signature without round-tripping
/// the database.
///
/// `actor` is exposed by reference (no clone). Borrowed `&'static str`
/// for `table` matches what the audited table contributes.
#[derive(Debug)]
pub struct AuditEvent<'a> {
    /// SQL name of the audited (parent) table — e.g. `"users"`, not
    /// `"users_audit"`.
    pub table: &'static str,
    /// What changed.
    pub operation: AuditOperation,
    /// Actor identity (matches the value bound into the `actor_id`
    /// column of the audit row).
    pub actor: &'a ActorId,
    /// RFC 3339 UTC timestamp string, byte-identical to the
    /// `changed_at` column.
    pub changed_at: &'a str,
    /// JSON serialisation of the row, byte-identical to `row_data`.
    pub row_data: &'a str,
    /// HMAC-SHA256 hex digest of this row, or `None` when integrity
    /// is not configured on the [`AuditContext`].
    pub row_hash: Option<&'a str>,
    /// Hash chain link to the previous audit row's `row_hash`, or
    /// `None` for the first row of the chain / pre-chaining rows.
    pub prev_hash: Option<&'a str>,
}

/// Error returned by [`AuditSink::write`].
///
/// Variant boundaries mirror the most common failure modes: I/O for
/// file/network sinks, serialization for JSON/protobuf encoders,
/// `Backend` for everything else (status-code rejection, queue full,
/// signing failure, …).
#[derive(Debug)]
pub enum AuditSinkError {
    /// I/O error from a file/network/socket backend.
    Io(std::io::Error),
    /// Failure encoding the event to the wire format.
    Serialization(String),
    /// Catch-all for backend-specific failures (HTTP non-2xx, S3 access
    /// denied, Kafka broker unavailable, …).
    Backend(String),
}

impl std::fmt::Display for AuditSinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuditSinkError::Io(e) => write!(f, "audit sink io error: {e}"),
            AuditSinkError::Serialization(m) => {
                write!(f, "audit sink serialization error: {m}")
            }
            AuditSinkError::Backend(m) => write!(f, "audit sink backend error: {m}"),
        }
    }
}

impl std::error::Error for AuditSinkError {}

impl From<std::io::Error> for AuditSinkError {
    fn from(e: std::io::Error) -> Self {
        AuditSinkError::Io(e)
    }
}

/// Boxed future returned by [`AuditSink::write`].
///
/// Type-erased so the trait stays dyn-safe. Project convention mirrors
/// [`crate::db::BoxFuture`] but with a different `Err` variant since
/// audit-sink failures are not `DbError`s.
pub type AuditSinkFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), AuditSinkError>> + Send + 'a>>;

/// Append-only audit log destination outside the application database.
///
/// Implementations are free to write to a local file, an HTTP endpoint,
/// an S3 bucket with Object Lock, a Kafka topic, etc. The contract is:
///
/// - **Append-only**: every successful call appends exactly one event.
/// - **Atomic with the DB transaction**: the audit code calls
///   `write` *inside* the same transaction that wrote the audit row in
///   the database. If `write` returns `Err`, the audit code propagates
///   it as a `DbError`, the transaction is rolled back, and **neither**
///   the underlying DML nor the audit row are committed. This trades
///   database connection time for the strongest durability guarantee:
///   a row only exists in the DB if it also reached the sink.
/// - **Synchronous failure**: the function must not silently retain
///   events in a background buffer. If a sink wants async batching,
///   it must finalise (or fail) before returning.
///
/// The trait is dyn-safe via [`BoxFuture`](crate::db::BoxFuture). Most
/// implementations look like:
///
/// ```ignore
/// impl AuditSink for MySink {
///     fn write<'a>(
///         &'a self,
///         event: &'a AuditEvent<'a>,
///     ) -> BoxFuture<'a, Result<(), AuditSinkError>> {
///         Box::pin(async move {
///             // … push to backend …
///             Ok(())
///         })
///     }
/// }
/// ```
pub trait AuditSink: Send + Sync {
    fn write<'a>(&'a self, event: &'a AuditEvent<'a>) -> AuditSinkFuture<'a>;
}

/// No-op sink — accepts every event and discards it.
///
/// Useful as a default when no external sink is configured, and as a
/// stand-in in tests where the sink itself is not under test.
pub struct NoopAuditSink;

impl AuditSink for NoopAuditSink {
    fn write<'a>(&'a self, _event: &'a AuditEvent<'a>) -> AuditSinkFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

// ── FileAuditSink (feature: audit-file) ──────────────────────────────

/// Append-only newline-delimited JSON sink to a local file.
///
/// Each successful `write` appends one JSON object plus a newline. The
/// file is opened in append mode with `O_CREAT`, so concurrent writers
/// pointing at the same path are serialised by the OS at the syscall
/// level (POSIX `O_APPEND` guarantees atomic appends ≤ `PIPE_BUF` on
/// local filesystems). An internal `tokio::sync::Mutex` further
/// serialises writes within a single process so a single audit event
/// always lands as one contiguous line.
///
/// **Durability**: `flush` is called after every line. For ultimate
/// durability against power loss callers can wrap or extend this sink
/// to call `sync_all` explicitly — at the cost of a syscall per event.
///
/// Enabled by the `audit-file` feature.
#[cfg(feature = "audit-file")]
pub struct FileAuditSink {
    /// Locked for the duration of one write so concurrent calls don't
    /// interleave half a line. Tokio's async mutex is intentional —
    /// holding a `std::sync::Mutex` across an await would block the
    /// runtime.
    file: tokio::sync::Mutex<tokio::fs::File>,
}

#[cfg(feature = "audit-file")]
impl FileAuditSink {
    /// Open the file at `path` in append mode, creating it if missing.
    pub async fn open(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Self {
            file: tokio::sync::Mutex::new(file),
        })
    }

    /// Build from an already-opened `tokio::fs::File`. Useful for tests
    /// or when the caller wants custom open flags (e.g. `O_DSYNC`).
    pub fn from_file(file: tokio::fs::File) -> Self {
        Self {
            file: tokio::sync::Mutex::new(file),
        }
    }
}

#[cfg(feature = "audit-file")]
impl AuditSink for FileAuditSink {
    fn write<'a>(&'a self, event: &'a AuditEvent<'a>) -> AuditSinkFuture<'a> {
        Box::pin(async move {
            use tokio::io::AsyncWriteExt;
            let mut line = audit_event_to_jsonl(event);
            line.push('\n');
            let mut guard = self.file.lock().await;
            guard.write_all(line.as_bytes()).await?;
            guard.flush().await?;
            Ok(())
        })
    }
}

/// Serialise an [`AuditEvent`] to a single JSON object string suitable
/// for newline-delimited logs (no embedded newlines).
///
/// Public so tests in this crate can pin the format. The
/// representation is **stable** — adding new fields is allowed,
/// renaming or removing existing ones is a breaking change.
#[cfg(feature = "audit-file")]
pub fn audit_event_to_jsonl(event: &AuditEvent<'_>) -> String {
    use std::fmt::Write as _;

    let mut out = String::with_capacity(256 + event.row_data.len());
    out.push('{');
    let _ = write!(
        &mut out,
        "\"table\":\"{}\",\"operation\":\"{}\",\"actor\":{},\"changed_at\":\"{}\",\"row_data\":{}",
        event.table,
        event.operation.as_str(),
        actor_to_json(event.actor),
        event.changed_at,
        event.row_data,
    );
    if let Some(h) = event.row_hash {
        let _ = write!(&mut out, ",\"row_hash\":\"{h}\"");
    }
    if let Some(p) = event.prev_hash {
        let _ = write!(&mut out, ",\"prev_hash\":\"{p}\"");
    }
    out.push('}');
    out
}

#[cfg(feature = "audit-file")]
fn actor_to_json(a: &ActorId) -> String {
    match a {
        ActorId::None => "null".to_string(),
        ActorId::Int(n) => n.to_string(),
        ActorId::String(s) => format!("\"{}\"", json_escape(s)),
    }
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
/// 7. `prev_hash`  — `TEXT NULL` (hash chain link to the previous audit row;
///    `NULL` for the first row of the chain or rows written before chaining was enabled)
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
            soft_delete: false,
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
            soft_delete: false,
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
            soft_delete: false,
        },
        // `changed_at` is bound by the application (RFC 3339 UTC) so it is
        // covered by the HMAC signature. The `CURRENT_TIMESTAMP` default
        // remains so legacy rows inserted without an explicit value still
        // get a server-side timestamp — but `audited_*` always passes one
        // in. We use the SQL-standard `CURRENT_TIMESTAMP` (rather than the
        // PG/MySQL `NOW()` synonym) so the same DDL works on SQLite too;
        // PG and MySQL both accept `CURRENT_TIMESTAMP` identically.
        ColumnDef {
            name: "changed_at",
            sql_type: SqlType::Timestamptz,
            primary_key: false,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: false,
            default: Some(crate::schema::DefaultValue::Expr("CURRENT_TIMESTAMP")),
            computed: None,
            timestamp_kind: None,
            timestamp_source: TimestampSource::Db,
            check: None,
            foreign_key: None,
            soft_delete: false,
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
            soft_delete: false,
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
            soft_delete: false,
        },
        // Chain link — references the `row_hash` of the previous audit row in
        // this table. NULL for the first row of the chain, or for any row
        // written before hash chaining was introduced. A `verify_audit_chain`
        // walk detects deletions by spotting a `prev_hash` that does not
        // match the previous row's `row_hash`.
        ColumnDef {
            name: "prev_hash",
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
            soft_delete: false,
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
    verify_audit_row_chained(
        secret,
        operation,
        actor,
        changed_at,
        None,
        row_data,
        stored_hash,
        integrity_expected,
    )
}

/// Verify an audit row's `row_hash` against a chained HMAC-SHA256.
///
/// Like [`verify_audit_row`], but also folds an optional `prev_hash` into
/// the signed message. When `prev_hash` is `None`, the message format is
/// identical to the legacy 4-field layout, so rows written before chaining
/// was added continue to verify under this entry point.
///
/// Use [`verify_audit_chain`] to verify both the per-row signature **and**
/// the chain continuity in one pass.
#[allow(clippy::too_many_arguments)]
pub fn verify_audit_row_chained(
    secret: &[u8],
    operation: &str,
    actor: &str,
    changed_at: &str,
    prev_hash: Option<&str>,
    row_data: &str,
    stored_hash: Option<&str>,
    integrity_expected: bool,
) -> Option<bool> {
    match stored_hash {
        Some(stored) => {
            let message =
                build_hmac_message_chained(operation, actor, changed_at, prev_hash, row_data);
            let expected = hex_encode(&hmac_sha256(secret, &message));
            // Constant-time comparison to prevent timing attacks.
            Some(constant_time_eq(expected.as_bytes(), stored.as_bytes()))
        }
        None if integrity_expected => Some(false),
        None => None,
    }
}

// ── Chain verification ────────────────────────────────────────────────

/// Result of verifying a single audit row during a chain walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditChainCheck {
    /// Row signature and chain link both verify.
    Ok,
    /// Row HMAC does not match the stored `row_hash`.
    BadHash,
    /// `row_hash` is `NULL` but integrity was expected.
    MissingHash,
    /// Row signature verifies, but `prev_hash` does not match the previous
    /// row's `row_hash` (or one of the two is `NULL` while the other is
    /// not). A deletion of a chain link surfaces here.
    BrokenChain {
        expected: Option<String>,
        found: Option<String>,
    },
}

/// One row's verdict in a chain walk: the `audit_id` of the row and its
/// individual check result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditChainRowResult {
    pub audit_id: i64,
    pub check: AuditChainCheck,
}

/// Walk every row of an audit table in `audit_id` order and verify both
/// the per-row HMAC signature **and** the integrity of the `prev_hash`
/// chain.
///
/// A deleted row is surfaced as a [`AuditChainCheck::BrokenChain`] on the
/// row that follows it: `prev_hash` of row N+1 still references the
/// deleted row's `row_hash`, but row N (now the row before N+1) has a
/// different `row_hash`.
///
/// `integrity_expected` mirrors the parameter of [`verify_audit_row`]: pass
/// `true` when every row of the chain was supposed to carry an HMAC, and
/// the function will flag rows missing a `row_hash` as
/// [`AuditChainCheck::MissingHash`]. Pass `false` to allow a chain that
/// was bootstrapped without integrity to pass through.
pub async fn verify_audit_chain(
    db: &impl Database,
    audit_table: &str,
    secret: &[u8],
    integrity_expected: bool,
) -> Result<Vec<AuditChainRowResult>, DbError> {
    let sql = format!(
        "SELECT \"audit_id\", \"operation\", \"actor_id\", \"changed_at\", \"row_data\", \"row_hash\", \"prev_hash\" FROM {} ORDER BY \"audit_id\" ASC",
        qi(audit_table)
    );
    let rows = db.query(&sql, &[]).await?;
    let mut results = Vec::with_capacity(rows.len());
    let mut last_row_hash: Option<String> = None;

    for row in &rows {
        let audit_id = match row.get("audit_id") {
            Some(crate::value::Value::I64(n)) => *n,
            Some(crate::value::Value::I32(n)) => *n as i64,
            _ => 0,
        };
        let operation = row_get_string(row, "operation");
        // actor_id is nullable: render NULL as "" so it matches the
        // `as_hmac_str()` behaviour of `ActorId::None`.
        let actor = row_get_string(row, "actor_id");
        let changed_at = row_get_string(row, "changed_at");
        let row_data = row_get_string(row, "row_data");
        let stored_hash = row_get_optional_string(row, "row_hash");
        let prev_hash = row_get_optional_string(row, "prev_hash");

        let check = match verify_audit_row_chained(
            secret,
            &operation,
            &actor,
            &changed_at,
            prev_hash.as_deref(),
            &row_data,
            stored_hash.as_deref(),
            integrity_expected,
        ) {
            Some(true) => {
                // Row signature verified — now check the chain link.
                if prev_hash == last_row_hash {
                    AuditChainCheck::Ok
                } else {
                    AuditChainCheck::BrokenChain {
                        expected: last_row_hash.clone(),
                        found: prev_hash.clone(),
                    }
                }
            }
            Some(false) => {
                if stored_hash.is_none() {
                    AuditChainCheck::MissingHash
                } else {
                    AuditChainCheck::BadHash
                }
            }
            None => AuditChainCheck::Ok, // integrity not expected for this row
        };

        results.push(AuditChainRowResult { audit_id, check });
        // Advance the chain tracker even on errors so the *next* row's
        // verdict reflects what is actually in the table — chasing a
        // bad-hash row with a "broken chain" verdict on the next row would
        // be redundant noise.
        last_row_hash = stored_hash;
    }

    Ok(results)
}

fn row_get_string(row: &crate::db::Row, col: &str) -> String {
    match row.get(col) {
        Some(crate::value::Value::String(s)) => s.clone(),
        _ => String::new(),
    }
}

fn row_get_optional_string(row: &crate::db::Row, col: &str) -> Option<String> {
    match row.get(col) {
        Some(crate::value::Value::String(s)) => Some(s.clone()),
        _ => None,
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
            // u64 values above 2^53 lose precision in JSON's number type
            // (which IEEE 754 double-precision floats implicitly truncate).
            // Audit integrity matters more than parser convenience: emit as
            // a quoted decimal string so consumers can losslessly round-trip
            // any u64. The HMAC chain spans the exact bytes we write here,
            // so a downstream verifier must use the same encoding.
            Value::U64(n) => {
                out.push('"');
                out.push_str(&n.to_string());
                out.push('"');
            }
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
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            Value::Duration(d) => {
                // Use the canonical MySQL TIME formatting so audit verifiers
                // can compare against the database's native rendering.
                out.push('"');
                out.push_str(&crate::value::format_mysql_time(*d));
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
            // Complex types — serialize as quoted string representation
            #[cfg(feature = "postgres")]
            Value::Point(p) => {
                out.push('"');
                out.push_str(&json_escape(&p.to_string()));
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::Inet(i) => {
                out.push('"');
                out.push_str(&i.to_string());
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::Cidr(c) => {
                out.push('"');
                out.push_str(&c.to_string());
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::MacAddr(m) => {
                out.push('"');
                out.push_str(&m.to_string());
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::Interval(i) => {
                out.push('"');
                out.push_str(&json_escape(&i.to_string()));
                out.push('"');
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
    let parent_table = M::table_name();
    let col_names: Vec<&'static str> = M::writable_column_names().to_vec();
    let actor = ctx.actor.clone();
    // Clone the secret into a ZeroOnDrop *immediately* so every copy is
    // wiped on drop — including the intermediate binding that lives
    // between here and the closure body.
    let secret_guard = ctx.hmac_secret.clone();
    let dialect = ctx.dialect;
    let sink = ctx.sink.clone();

    let affected = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let affected_clone = affected.clone();

    db.transaction(Box::new(move |tx| {
        // Move the ZeroOnDrop-wrapped secret into the future; its Drop
        // impl zeroizes on completion or panic.
        let secret_guard = secret_guard.clone();
        let sink = sink.clone();
        Box::pin(async move {
            use crate::db::DynDatabase;

            // 1. Execute the INSERT.
            let n = DynDatabase::execute(tx, &insert_sql, &insert_params).await?;
            affected_clone.store(n, std::sync::atomic::Ordering::Relaxed);

            // 2. Serialize the inserted values as JSON.
            let col_refs: Vec<&str> = col_names.to_vec();
            let row_data = values_to_json_string(&col_refs, &insert_params.to_vec());

            // 3. Generate changed_at app-side so it is covered by the HMAC.
            //    A DB-side NOW() would only be known post-INSERT and therefore
            //    unsignable — an attacker with DB access could backdate the
            //    row and recompute the hash over an empty timestamp.
            let changed_at = current_changed_at();

            // 4. Read the chain tip inside the transaction (locked when the
            //    dialect supports it) and chain the new row's HMAC to it.
            let prev_hash = if secret_guard.is_some() {
                read_chain_tip(tx, audit_table, dialect).await?
            } else {
                None
            };

            // 5. Compute HMAC over the bound changed_at value, optionally
            //    chained to the previous row's hash.
            let row_hash = if let Some(ref secret) = secret_guard {
                let actor_str = actor.as_hmac_str();
                let message = build_hmac_message_chained(
                    AuditOperation::Insert.as_str(),
                    &actor_str,
                    &changed_at,
                    prev_hash.as_deref(),
                    &row_data,
                );
                Some(hex_encode(&hmac_sha256(&secret.0, &message)))
            } else {
                None
            };

            // 6. Insert audit row with explicit changed_at parameter.
            let (audit_sql, audit_params) = build_audit_insert(
                audit_table,
                AuditOperation::Insert.as_str(),
                actor.to_value(),
                &changed_at,
                row_data.clone(),
                row_hash.clone(),
                prev_hash.clone(),
                dialect,
            );
            DynDatabase::execute(tx, &audit_sql, &audit_params).await?;

            // 7. Mirror to the external sink if one is configured.
            //    A sink failure is bubbled as `DbError::Other` which causes
            //    the whole transaction to roll back — the DML and the audit
            //    row both go away, preserving the "never DB without sink"
            //    invariant.
            if let Some(sink) = sink.as_ref() {
                let event = AuditEvent {
                    table: parent_table,
                    operation: AuditOperation::Insert,
                    actor: &actor,
                    changed_at: &changed_at,
                    row_data: &row_data,
                    row_hash: row_hash.as_deref(),
                    prev_hash: prev_hash.as_deref(),
                };
                sink.write(&event)
                    .await
                    .map_err(|e| DbError::Other(format!("audit sink rejected event: {e}")))?;
            }
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
    let parent_table = M::table_name();
    let col_names: Vec<&'static str> = M::column_names().to_vec();
    let actor = ctx.actor.clone();
    // Clone the secret into a ZeroOnDrop *immediately* so every copy is
    // wiped on drop — including the intermediate binding that lives
    // between here and the closure body.
    let secret_guard = ctx.hmac_secret.clone();
    let dialect = ctx.dialect;
    let sink = ctx.sink.clone();

    let affected = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let affected_clone = affected.clone();

    db.transaction(Box::new(move |tx| {
        // Move the ZeroOnDrop-wrapped secret into the future; its Drop
        // impl zeroizes on completion or panic.
        let secret_guard = secret_guard.clone();
        let sink = sink.clone();
        Box::pin(async move {
            use crate::db::DynDatabase;

            // 1. Read matching rows inside the transaction (locked FOR UPDATE).
            let old_rows = DynDatabase::query(tx, &select_sql, &select_params).await?;

            // 2. Serialize before-images.
            let col_refs: Vec<&str> = col_names.to_vec();
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

            // 5. Read the chain tip inside the transaction. The walking
            //    `prev_hash` is updated after each row so that N audit rows
            //    written by a multi-row UPDATE form a contiguous chain.
            let mut prev_hash = if secret_guard.is_some() {
                read_chain_tip(tx, audit_table, dialect).await?
            } else {
                None
            };

            // 6. For each (before, after) pair: build the combined row_data,
            //    compute the chained HMAC, insert the audit row, then
            //    advance `prev_hash` to the row we just signed.
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
                    let message = build_hmac_message_chained(
                        AuditOperation::Update.as_str(),
                        &actor_str,
                        &changed_at,
                        prev_hash.as_deref(),
                        &row_data,
                    );
                    Some(hex_encode(&hmac_sha256(&secret.0, &message)))
                } else {
                    None
                };

                let (audit_sql, audit_params) = build_audit_insert(
                    audit_table,
                    AuditOperation::Update.as_str(),
                    actor.to_value(),
                    &changed_at,
                    row_data.clone(),
                    row_hash.clone(),
                    prev_hash.clone(),
                    dialect,
                );
                DynDatabase::execute(tx, &audit_sql, &audit_params).await?;

                // Mirror to the external sink, propagating failures so
                // the transaction rolls back if the sink rejects.
                if let Some(sink) = sink.as_ref() {
                    let event = AuditEvent {
                        table: parent_table,
                        operation: AuditOperation::Update,
                        actor: &actor,
                        changed_at: &changed_at,
                        row_data: &row_data,
                        row_hash: row_hash.as_deref(),
                        prev_hash: prev_hash.as_deref(),
                    };
                    sink.write(&event)
                        .await
                        .map_err(|e| DbError::Other(format!("audit sink rejected event: {e}")))?;
                }

                // Advance the chain tip so the next iteration links to the
                // row we just inserted, not the original tip.
                if row_hash.is_some() {
                    prev_hash = row_hash;
                }
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
    let parent_table = M::table_name();
    let col_names: Vec<&'static str> = M::column_names().to_vec();
    let actor = ctx.actor.clone();
    // Clone the secret into a ZeroOnDrop *immediately* so every copy is
    // wiped on drop — including the intermediate binding that lives
    // between here and the closure body.
    let secret_guard = ctx.hmac_secret.clone();
    let dialect = ctx.dialect;
    let sink = ctx.sink.clone();

    let affected = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let affected_clone = affected.clone();

    db.transaction(Box::new(move |tx| {
        // Move the ZeroOnDrop-wrapped secret into the future; its Drop
        // impl zeroizes on completion or panic.
        let secret_guard = secret_guard.clone();
        let sink = sink.clone();
        Box::pin(async move {
            use crate::db::DynDatabase;

            // 1. Read matching rows inside the transaction (locked FOR UPDATE).
            let old_rows = DynDatabase::query(tx, &select_sql, &select_params).await?;

            // 2. Serialise rows and compute chained HMACs. The walking
            //    `prev_hash` starts at the audit-table chain tip (read in
            //    the same transaction) and advances after each iteration so
            //    multi-row deletes produce a contiguous chain segment.
            let col_refs: Vec<&str> = col_names.to_vec();
            let mut prev_hash = if secret_guard.is_some() {
                read_chain_tip(tx, audit_table, dialect).await?
            } else {
                None
            };
            let mut entries: Vec<(String, String, Option<String>, Option<String>)> =
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
                    let message = build_hmac_message_chained(
                        AuditOperation::Delete.as_str(),
                        &actor_str,
                        &changed_at,
                        prev_hash.as_deref(),
                        &row_data,
                    );
                    Some(hex_encode(&hmac_sha256(&secret.0, &message)))
                } else {
                    None
                };
                let prev_for_row = prev_hash.clone();
                if row_hash.is_some() {
                    prev_hash = row_hash.clone();
                }
                entries.push((changed_at, row_data, row_hash, prev_for_row));
            }

            // 3. Delete the rows.
            let n = DynDatabase::execute(tx, &delete_sql, &delete_params).await?;
            affected_clone.store(n, std::sync::atomic::Ordering::Relaxed);

            // 4. Insert audit rows + mirror to sink.
            for (changed_at, row_data, row_hash, prev_for_row) in entries {
                let (audit_sql, audit_params) = build_audit_insert(
                    audit_table,
                    AuditOperation::Delete.as_str(),
                    actor.to_value(),
                    &changed_at,
                    row_data.clone(),
                    row_hash.clone(),
                    prev_for_row.clone(),
                    dialect,
                );
                DynDatabase::execute(tx, &audit_sql, &audit_params).await?;

                if let Some(sink) = sink.as_ref() {
                    let event = AuditEvent {
                        table: parent_table,
                        operation: AuditOperation::Delete,
                        actor: &actor,
                        changed_at: &changed_at,
                        row_data: &row_data,
                        row_hash: row_hash.as_deref(),
                        prev_hash: prev_for_row.as_deref(),
                    };
                    sink.write(&event)
                        .await
                        .map_err(|e| DbError::Other(format!("audit sink rejected event: {e}")))?;
                }
            }
            Ok(())
        })
    }))
    .await?;

    Ok(affected.load(std::sync::atomic::Ordering::Relaxed))
}

// ── Internal helpers ─────────────────────────────────────────────────

/// Read the most recent `row_hash` from the audit table — the "tip" of the
/// hash chain — locking it when the dialect supports `FOR UPDATE` so that
/// two concurrent transactions cannot both produce rows that point at the
/// same predecessor.
///
/// Returns `Ok(None)` when the audit table is empty or the latest row has
/// `row_hash = NULL` (integrity was not configured for that row). The
/// caller treats both cases identically: the next row is the start of a
/// fresh chain segment.
async fn read_chain_tip(
    tx: &dyn crate::db::DynDatabase,
    audit_table: &str,
    dialect: crate::query::Dialect,
) -> Result<Option<String>, DbError> {
    use crate::db::DynDatabase;

    // SQLite does not support row-level `FOR UPDATE`; its WAL+IMMEDIATE
    // locking already serialises writes per database.
    let lock = matches!(
        dialect,
        crate::query::Dialect::Postgres | crate::query::Dialect::Mysql
    );
    let sql = if lock {
        format!(
            "SELECT \"row_hash\" FROM {} ORDER BY \"audit_id\" DESC LIMIT 1 FOR UPDATE",
            qi(audit_table)
        )
    } else {
        format!(
            "SELECT \"row_hash\" FROM {} ORDER BY \"audit_id\" DESC LIMIT 1",
            qi(audit_table)
        )
    };
    let rows = DynDatabase::query(tx, &sql, &[]).await?;
    if let Some(row) = rows.into_iter().next() {
        match row.get("row_hash") {
            Some(crate::value::Value::String(s)) => Ok(Some(s.clone())),
            _ => Ok(None),
        }
    } else {
        Ok(None)
    }
}

/// Build the audit INSERT SQL and parameter list.
///
/// When `row_hash` is `Some`, includes the `row_hash` column; otherwise omits
/// it so that existing audit tables without the column still work.
///
/// The `dialect` parameter controls placeholder style: `?` for Generic/MySQL,
/// `$1, $2, …` for PostgreSQL.
#[allow(clippy::too_many_arguments)]
fn build_audit_insert(
    audit_table: &str,
    operation: &str,
    actor_val: crate::value::Value,
    changed_at: &str,
    row_data: String,
    row_hash: Option<String>,
    prev_hash: Option<String>,
    dialect: crate::query::Dialect,
) -> (String, Vec<crate::value::Value>) {
    // `changed_at` is bound by the caller (RFC 3339 UTC) so it is covered
    // by the HMAC signature at INSERT time. This closes the antedating
    // window where the DB-side `NOW()` was only known post-INSERT and
    // therefore not signed.
    //
    // The INSERT shape varies with whether a `row_hash` and `prev_hash` are
    // present, so the audit table can be queried by adapters that only know
    // about the legacy schema. Concretely:
    //   * row_hash = None              → 4-column legacy INSERT
    //   * row_hash = Some, prev = None → 5-column INSERT (no chain link)
    //   * both Some                    → 6-column chained INSERT
    let (sql, params) = match (row_hash, prev_hash) {
        (Some(hash), Some(prev)) => {
            let sql = format!(
                "INSERT INTO {} (\"operation\", \"actor_id\", \"changed_at\", \"row_data\", \"row_hash\", \"prev_hash\") VALUES (?, ?, ?, ?, ?, ?)",
                qi(audit_table)
            );
            let params = vec![
                crate::value::Value::String(operation.into()),
                actor_val,
                crate::value::Value::String(changed_at.to_owned()),
                crate::value::Value::String(row_data),
                crate::value::Value::String(hash),
                crate::value::Value::String(prev),
            ];
            (sql, params)
        }
        (Some(hash), None) => {
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
        }
        (None, _) => {
            // No HMAC — chain is irrelevant without integrity, so prev_hash
            // is dropped silently.
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
        }
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
        assert_eq!(defs.len(), 7);
        assert_eq!(defs[0].name, "audit_id");
        assert_eq!(defs[1].name, "operation");
        assert_eq!(defs[2].name, "actor_id");
        assert_eq!(defs[3].name, "changed_at");
        assert_eq!(defs[4].name, "row_data");
        assert_eq!(defs[5].name, "row_hash");
        assert_eq!(defs[6].name, "prev_hash");
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
        assert_eq!(
            defs[3].default,
            Some(crate::schema::DefaultValue::Expr("CURRENT_TIMESTAMP"))
        );
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

    // ── Hash chaining ────────────────────────────────────────────────

    #[test]
    fn audit_column_defs_includes_prev_hash() {
        let defs = audit_column_defs_for("anything");
        assert_eq!(
            defs.len(),
            7,
            "expected 7 audit columns including prev_hash"
        );
        let prev = defs
            .iter()
            .find(|d| d.name == "prev_hash")
            .expect("prev_hash column missing");
        assert!(prev.nullable, "prev_hash must be nullable");
        assert_eq!(prev.sql_type, crate::schema::SqlType::Text);
    }

    #[test]
    fn build_hmac_message_chained_without_prev_matches_legacy() {
        let legacy = build_hmac_message("insert", "actor", "ts", "data");
        let chained = build_hmac_message_chained("insert", "actor", "ts", None, "data");
        assert_eq!(legacy, chained);
    }

    #[test]
    fn build_hmac_message_chained_with_prev_differs_from_none() {
        let none = build_hmac_message_chained("insert", "actor", "ts", None, "data");
        let some = build_hmac_message_chained("insert", "actor", "ts", Some("abc"), "data");
        assert_ne!(none, some, "prev_hash must influence the signed message");
    }

    #[test]
    fn build_hmac_message_chained_sensitive_to_prev_value() {
        let a = build_hmac_message_chained("insert", "x", "t", Some("aaa"), "d");
        let b = build_hmac_message_chained("insert", "x", "t", Some("bbb"), "d");
        assert_ne!(a, b);
    }

    #[test]
    fn compute_hash_chained_matches_compute_hash_when_no_prev() {
        let ctx = AuditContext::with_integrity(ActorId::Int(1), b"key").unwrap();
        let h1 = ctx.compute_hash("insert", "ts", "data").unwrap();
        let h2 = ctx
            .compute_hash_chained("insert", "ts", "data", None)
            .unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn compute_hash_chained_changes_with_prev() {
        let ctx = AuditContext::with_integrity(ActorId::Int(1), b"key").unwrap();
        let h1 = ctx
            .compute_hash_chained("insert", "ts", "data", None)
            .unwrap();
        let h2 = ctx
            .compute_hash_chained("insert", "ts", "data", Some("aaa"))
            .unwrap();
        assert_ne!(h1, h2);
    }

    #[test]
    fn verify_audit_row_chained_round_trip() {
        let ctx = AuditContext::with_integrity(ActorId::Int(7), b"secret").unwrap();
        let h = ctx
            .compute_hash_chained(
                "delete",
                "2024-01-15T10:30:00Z",
                r#"{"id":1}"#,
                Some("prev"),
            )
            .unwrap();
        assert_eq!(
            verify_audit_row_chained(
                b"secret",
                "delete",
                "7",
                "2024-01-15T10:30:00Z",
                Some("prev"),
                r#"{"id":1}"#,
                Some(&h),
                false,
            ),
            Some(true),
        );
    }

    #[test]
    fn verify_audit_row_chained_detects_prev_hash_tampering() {
        let ctx = AuditContext::with_integrity(ActorId::Int(7), b"secret").unwrap();
        let h = ctx
            .compute_hash_chained("delete", "ts", "data", Some("prev"))
            .unwrap();
        assert_eq!(
            verify_audit_row_chained(
                b"secret",
                "delete",
                "7",
                "ts",
                Some("forged_prev"),
                "data",
                Some(&h),
                false,
            ),
            Some(false),
        );
    }

    #[test]
    fn verify_audit_row_legacy_still_verifies_unchained_rows() {
        let ctx = AuditContext::with_integrity(ActorId::Int(1), b"k").unwrap();
        let legacy_hash = ctx.compute_hash("insert", "ts", "row").unwrap();
        assert_eq!(
            verify_audit_row(b"k", "insert", "1", "ts", "row", Some(&legacy_hash), false),
            Some(true),
        );
    }

    // ── verify_audit_chain ───────────────────────────────────────────

    struct MockChainDb {
        rows: Vec<crate::db::Row>,
    }

    impl crate::db::Database for MockChainDb {
        async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<u64, DbError> {
            Ok(0)
        }
        async fn query(
            &self,
            _sql: &str,
            _params: &[Value],
        ) -> Result<Vec<crate::db::Row>, DbError> {
            Ok(self.rows.clone())
        }
        async fn query_one(
            &self,
            _sql: &str,
            _params: &[Value],
        ) -> Result<crate::db::Row, DbError> {
            Err(DbError::RecordNotFound)
        }
        async fn transaction<'a>(
            &'a self,
            _f: crate::db::TransactionFn<'a>,
        ) -> Result<(), DbError> {
            Ok(())
        }
    }

    fn make_audit_row(
        id: i64,
        operation: &str,
        actor: &str,
        changed_at: &str,
        row_data: &str,
        row_hash: Option<&str>,
        prev_hash: Option<&str>,
    ) -> crate::db::Row {
        let columns = vec![
            "audit_id".to_string(),
            "operation".to_string(),
            "actor_id".to_string(),
            "changed_at".to_string(),
            "row_data".to_string(),
            "row_hash".to_string(),
            "prev_hash".to_string(),
        ];
        let values = vec![
            Value::I64(id),
            Value::String(operation.into()),
            Value::String(actor.into()),
            Value::String(changed_at.into()),
            Value::String(row_data.into()),
            row_hash
                .map(|s| Value::String(s.into()))
                .unwrap_or(Value::Null),
            prev_hash
                .map(|s| Value::String(s.into()))
                .unwrap_or(Value::Null),
        ];
        crate::db::Row::new(columns, values)
    }

    fn build_chain(secret: &[u8], actor: &str, entries: &[(&str, &str)]) -> Vec<crate::db::Row> {
        let mut rows = Vec::with_capacity(entries.len());
        let mut prev: Option<String> = None;
        for (i, (op, data)) in entries.iter().enumerate() {
            let ts = format!("2024-01-15T10:30:0{i}Z");
            let msg = build_hmac_message_chained(op, actor, &ts, prev.as_deref(), data);
            let hash = hex_encode(&hmac_sha256(secret, &msg));
            rows.push(make_audit_row(
                (i + 1) as i64,
                op,
                actor,
                &ts,
                data,
                Some(&hash),
                prev.as_deref(),
            ));
            prev = Some(hash);
        }
        rows
    }

    #[tokio::test]
    async fn verify_audit_chain_accepts_well_formed_chain() {
        let rows = build_chain(
            b"secret",
            "1",
            &[
                ("insert", r#"{"id":1}"#),
                ("update", r#"{"id":1,"v":2}"#),
                ("delete", r#"{"id":1,"v":2}"#),
            ],
        );
        let db = MockChainDb { rows };
        let results = verify_audit_chain(&db, "users_audit", b"secret", true)
            .await
            .unwrap();
        assert_eq!(results.len(), 3);
        for r in &results {
            assert_eq!(r.check, AuditChainCheck::Ok, "row {} failed", r.audit_id);
        }
    }

    #[tokio::test]
    async fn verify_audit_chain_detects_deleted_middle_row() {
        let mut rows = build_chain(
            b"k",
            "1",
            &[("insert", "a"), ("update", "b"), ("delete", "c")],
        );
        rows.remove(1);
        let db = MockChainDb { rows };
        let results = verify_audit_chain(&db, "x_audit", b"k", true)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].check, AuditChainCheck::Ok);
        match &results[1].check {
            AuditChainCheck::BrokenChain { .. } => {}
            other => panic!("expected BrokenChain, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn verify_audit_chain_detects_tampered_row_data() {
        let rows = build_chain(b"k", "1", &[("insert", "a"), ("update", "b")]);
        // Replace row 2 with a tampered version: same row_hash, different
        // row_data — verifies that the per-row HMAC catches data forgery.
        let original_hash = match rows[1].get("row_hash") {
            Some(Value::String(s)) => s.clone(),
            _ => unreachable!(),
        };
        let original_prev = match rows[1].get("prev_hash") {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        };
        let mut tampered = rows;
        tampered[1] = make_audit_row(
            2,
            "update",
            "1",
            "2024-01-15T10:30:01Z",
            "forged",
            Some(&original_hash),
            original_prev.as_deref(),
        );
        let db = MockChainDb { rows: tampered };
        let results = verify_audit_chain(&db, "x_audit", b"k", true)
            .await
            .unwrap();
        assert_eq!(results[0].check, AuditChainCheck::Ok);
        assert_eq!(results[1].check, AuditChainCheck::BadHash);
    }

    #[tokio::test]
    async fn verify_audit_chain_flags_missing_hash_when_integrity_expected() {
        let rows = vec![make_audit_row(1, "insert", "1", "ts", "data", None, None)];
        let db = MockChainDb { rows };
        let results = verify_audit_chain(&db, "x_audit", b"k", true)
            .await
            .unwrap();
        assert_eq!(results[0].check, AuditChainCheck::MissingHash);
    }

    #[tokio::test]
    async fn verify_audit_chain_handles_empty_table() {
        let db = MockChainDb { rows: vec![] };
        let results = verify_audit_chain(&db, "x_audit", b"k", true)
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    // ── AuditSink ────────────────────────────────────────────────────

    #[tokio::test]
    async fn noop_audit_sink_accepts_every_event() {
        let sink = NoopAuditSink;
        let actor = ActorId::Int(1);
        let event = AuditEvent {
            table: "users",
            operation: AuditOperation::Insert,
            actor: &actor,
            changed_at: "2026-05-07T10:00:00Z",
            row_data: r#"{"id":1}"#,
            row_hash: None,
            prev_hash: None,
        };
        sink.write(&event).await.unwrap();
    }

    #[cfg(feature = "audit-file")]
    #[test]
    fn audit_event_to_jsonl_minimal_shape() {
        let actor = ActorId::Int(42);
        let event = AuditEvent {
            table: "users",
            operation: AuditOperation::Insert,
            actor: &actor,
            changed_at: "2026-05-07T10:00:00Z",
            row_data: r#"{"id":1}"#,
            row_hash: None,
            prev_hash: None,
        };
        let line = audit_event_to_jsonl(&event);
        assert_eq!(
            line,
            r#"{"table":"users","operation":"insert","actor":42,"changed_at":"2026-05-07T10:00:00Z","row_data":{"id":1}}"#
        );
    }

    #[cfg(feature = "audit-file")]
    #[test]
    fn audit_event_to_jsonl_includes_hashes_when_present() {
        let actor = ActorId::None;
        let event = AuditEvent {
            table: "users",
            operation: AuditOperation::Update,
            actor: &actor,
            changed_at: "2026-05-07T10:00:00Z",
            row_data: r#"{"id":1}"#,
            row_hash: Some("abc"),
            prev_hash: Some("xyz"),
        };
        let line = audit_event_to_jsonl(&event);
        assert!(line.contains(r#""actor":null"#));
        assert!(line.contains(r#""row_hash":"abc""#));
        assert!(line.contains(r#""prev_hash":"xyz""#));
        // Single-line: JSON has no embedded newlines.
        assert!(!line.contains('\n'));
    }

    #[cfg(feature = "audit-file")]
    #[test]
    fn audit_event_to_jsonl_escapes_string_actor() {
        let actor = ActorId::String("alice\"quoted".into());
        let event = AuditEvent {
            table: "x",
            operation: AuditOperation::Delete,
            actor: &actor,
            changed_at: "ts",
            row_data: "{}",
            row_hash: None,
            prev_hash: None,
        };
        let line = audit_event_to_jsonl(&event);
        assert!(
            line.contains(r#""actor":"alice\"quoted""#),
            "expected escaped quote, got: {line}"
        );
    }

    #[cfg(feature = "audit-file")]
    #[tokio::test]
    async fn file_audit_sink_appends_one_line_per_event() {
        // Use a deterministic temp path under the system tmp dir to avoid
        // pulling `tempfile` as a dev-dependency just for one test.
        let path = std::env::temp_dir().join(format!(
            "reify_audit_sink_test_{}.ndjson",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);

        let sink = FileAuditSink::open(&path).await.unwrap();

        let actor = ActorId::Int(1);
        let event1 = AuditEvent {
            table: "users",
            operation: AuditOperation::Insert,
            actor: &actor,
            changed_at: "2026-05-07T10:00:00Z",
            row_data: r#"{"id":1}"#,
            row_hash: None,
            prev_hash: None,
        };
        let event2 = AuditEvent {
            table: "users",
            operation: AuditOperation::Update,
            actor: &actor,
            changed_at: "2026-05-07T10:00:01Z",
            row_data: r#"{"id":1,"v":2}"#,
            row_hash: Some("aaa"),
            prev_hash: None,
        };
        sink.write(&event1).await.unwrap();
        sink.write(&event2).await.unwrap();

        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains(r#""operation":"insert""#));
        assert!(lines[1].contains(r#""operation":"update""#));
        assert!(lines[1].contains(r#""row_hash":"aaa""#));

        // Each line is itself parsable JSON (no embedded newlines).
        // Manual sanity check: balanced braces.
        for line in &lines {
            assert!(line.starts_with('{'));
            assert!(line.ends_with('}'));
        }

        // Reopen + append a 3rd event; previous content must be preserved.
        let sink2 = FileAuditSink::open(&path).await.unwrap();
        sink2.write(&event1).await.unwrap();
        let contents2 = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(contents2.lines().count(), 3);

        // Cleanup.
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(feature = "audit-file")]
    #[tokio::test]
    async fn file_audit_sink_serialises_concurrent_writes_atomically() {
        // 8 concurrent tasks each writing one event. The internal mutex
        // must ensure no line is interleaved with another. Verified by
        // reading back the file and checking each line is a complete
        // JSON object with balanced braces.
        let path = std::env::temp_dir().join(format!(
            "reify_audit_sink_concurrency_{}.ndjson",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let sink = std::sync::Arc::new(FileAuditSink::open(&path).await.unwrap());

        let mut handles = Vec::new();
        for i in 0..8 {
            let s = sink.clone();
            handles.push(tokio::spawn(async move {
                let actor = ActorId::Int(i);
                let row = format!(r#"{{"id":{i}}}"#);
                let event = AuditEvent {
                    table: "users",
                    operation: AuditOperation::Insert,
                    actor: &actor,
                    changed_at: "2026-05-07T10:00:00Z",
                    row_data: &row,
                    row_hash: None,
                    prev_hash: None,
                };
                s.write(&event).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        drop(sink); // release any held file handles before reading

        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 8, "8 events, 8 lines");
        for line in &lines {
            // Brace balance proves no interleaving.
            let opens = line.matches('{').count();
            let closes = line.matches('}').count();
            assert_eq!(opens, closes, "unbalanced line: {line}");
            assert!(line.starts_with('{') && line.ends_with('}'));
        }
        let _ = std::fs::remove_file(&path);
    }
}
