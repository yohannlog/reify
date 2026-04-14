use crate::db::{Database, DbError, FromRow};
use crate::ident::qi;
use crate::query::{DeleteBuilder, UpdateBuilder};
use crate::schema::{ColumnDef, SqlType, TimestampSource};
use crate::table::Table;

// ── SHA-256 / HMAC-SHA256 (pure std, zero deps) ──────────────────────

/// SHA-256 constants: first 32 bits of the fractional parts of the cube
/// roots of the first 64 primes.
#[rustfmt::skip]
const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
    0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
    0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
    0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
    0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
    0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/// Initial hash values: first 32 bits of the fractional parts of the
/// square roots of the first 8 primes.
const H0: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
    0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

/// Compute SHA-256 over arbitrary bytes. Returns a 32-byte digest.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = H0;

    // Pre-processing: pad to a multiple of 512 bits (64 bytes).
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut msg: Vec<u8> = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0x00);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    // Process each 512-bit (64-byte) block.
    for block in msg.chunks(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes(block[i * 4..i * 4 + 4].try_into().unwrap());
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

/// HMAC-SHA256. Returns a 32-byte MAC.
///
/// Implements RFC 2104: `HMAC(K, m) = H((K' ⊕ opad) ∥ H((K' ⊕ ipad) ∥ m))`
/// where `K'` is the key zero-padded (or hashed) to the block size (64 bytes).
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    // Derive block-sized key.
    let mut k = [0u8; 64];
    if key.len() > 64 {
        let hashed = sha256(key);
        k[..32].copy_from_slice(&hashed);
    } else {
        k[..key.len()].copy_from_slice(key);
    }

    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }

    // inner = H(ipad ∥ message)
    let mut inner_input = Vec::with_capacity(64 + message.len());
    inner_input.extend_from_slice(&ipad);
    inner_input.extend_from_slice(message);
    let inner = sha256(&inner_input);

    // outer = H(opad ∥ inner)
    let mut outer_input = [0u8; 96]; // 64 + 32
    outer_input[..64].copy_from_slice(&opad);
    outer_input[64..].copy_from_slice(&inner);
    sha256(&outer_input)
}

/// Encode a byte slice as a lowercase hex string.
pub fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
        s
    })
}

// ── AuditOperation ───────────────────────────────────────────────────

/// Operation kind recorded in the audit log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditOperation {
    Update,
    Delete,
}

impl AuditOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            AuditOperation::Update => "update",
            AuditOperation::Delete => "delete",
        }
    }
}

// ── AuditContext ─────────────────────────────────────────────────────

/// Context passed to audited operations (who triggered the change).
///
/// When `hmac_secret` is set, every audit row receives a `row_hash` column
/// containing `HMAC-SHA256(secret, "<operation>|<actor_id>|<row_data>")` encoded
/// as a lowercase hex string. This makes tampering with any field detectable.
pub struct AuditContext {
    pub actor_id: Option<i64>,
    /// Optional HMAC-SHA256 secret. When present, a `row_hash` is computed and
    /// stored alongside every audit row, enabling tamper detection.
    pub hmac_secret: Option<Vec<u8>>,
}

impl AuditContext {
    /// Create a context without integrity protection (backward-compatible).
    pub fn new(actor_id: Option<i64>) -> Self {
        Self { actor_id, hmac_secret: None }
    }

    /// Create a context with HMAC-SHA256 integrity protection.
    pub fn with_integrity(actor_id: Option<i64>, secret: impl Into<Vec<u8>>) -> Self {
        Self { actor_id, hmac_secret: Some(secret.into()) }
    }

    /// Compute the HMAC-SHA256 hex digest for an audit row, or `None` if no
    /// secret is configured.
    ///
    /// The signed message is: `"<operation>|<actor_id_or_null>|<row_data>"`
    pub fn compute_hash(&self, operation: &str, row_data: &str) -> Option<String> {
        let secret = self.hmac_secret.as_deref()?;
        let actor = match self.actor_id {
            Some(id) => id.to_string(),
            None => "null".to_string(),
        };
        let message = format!("{operation}|{actor}|{row_data}");
        Some(hex_encode(&hmac_sha256(secret, message.as_bytes())))
    }
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
/// Columns: `audit_id`, `operation`, `actor_id`, `changed_at`, `row_data`,
/// and `row_hash` (nullable `TEXT` — populated only when an HMAC secret is
/// configured on [`AuditContext`]).
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
            check: None,
            foreign_key: None,
        },
        ColumnDef {
            name: "actor_id",
            sql_type: SqlType::BigInt,
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
        ColumnDef {
            name: "changed_at",
            sql_type: SqlType::Timestamptz,
            primary_key: false,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: false,
            default: Some("NOW()".to_string()),
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
/// Returns `true` when the hash is valid, `false` when it has been tampered
/// with, and `None` when `stored_hash` is `None` (integrity not enabled).
///
/// # Example
/// ```
/// use reify_core::audit::{AuditContext, verify_audit_row};
///
/// let ctx = AuditContext::with_integrity(Some(7), b"secret");
/// let hash = ctx.compute_hash("delete", r#"{"id":1}"#).unwrap();
/// assert_eq!(verify_audit_row(b"secret", "delete", Some(7), r#"{"id":1}"#, Some(&hash)), Some(true));
/// assert_eq!(verify_audit_row(b"secret", "delete", Some(7), r#"{"id":1}"#, None), None);
/// assert_eq!(verify_audit_row(b"secret", "delete", Some(7), r#"{"id":2}"#, Some(&hash)), Some(false));
/// ```
pub fn verify_audit_row(
    secret: &[u8],
    operation: &str,
    actor_id: Option<i64>,
    row_data: &str,
    stored_hash: Option<&str>,
) -> Option<bool> {
    let stored = stored_hash?;
    let actor = match actor_id {
        Some(id) => id.to_string(),
        None => "null".to_string(),
    };
    let message = format!("{operation}|{actor}|{row_data}");
    let expected = hex_encode(&hmac_sha256(secret, message.as_bytes()));
    // Constant-time comparison to prevent timing attacks.
    Some(constant_time_eq(expected.as_bytes(), stored.as_bytes()))
}

/// Constant-time byte-slice equality — prevents timing side-channels when
/// comparing MAC values.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

// ── JSON serialisation helper ────────────────────────────────────────

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
        out.push_str(col);
        out.push_str("\":");
        // value
        match val {
            Value::Null => out.push_str("null"),
            Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Value::I16(n) => out.push_str(&n.to_string()),
            Value::I32(n) => out.push_str(&n.to_string()),
            Value::I64(n) => out.push_str(&n.to_string()),
            Value::F32(f) => out.push_str(&f.to_string()),
            Value::F64(f) => out.push_str(&f.to_string()),
            Value::String(s) => {
                out.push('"');
                out.push_str(&s.replace('\\', "\\\\").replace('"', "\\\""));
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
                    out.push_str(&v.replace('\\', "\\\\").replace('"', "\\\""));
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

// ── audited_update ───────────────────────────────────────────────────

/// Execute an UPDATE and write an audit row atomically inside a transaction.
///
/// The audit row captures the operation kind (`"update"`) and the actor id.
/// Because the old values are not fetched (UPDATE does not return old data
/// without a RETURNING clause), `row_data` is set to `"{}"` for UPDATE — use
/// `audited_delete` when you need the full old snapshot.
///
/// When [`AuditContext::hmac_secret`] is set, a `row_hash` column is also
/// written with `HMAC-SHA256(secret, "update|<actor_id>|{}")` so that any
/// post-hoc modification of the audit row is detectable via [`verify_audit_row`].
pub async fn audited_update<M: Auditable>(
    db: &impl Database,
    builder: UpdateBuilder<M>,
    ctx: &AuditContext,
) -> Result<u64, DbError> {
    let (update_sql, update_params) = builder.build();
    let audit_table = M::audit_table_name();
    let actor_id = ctx.actor_id;
    let row_data = "{}".to_string();
    let row_hash = ctx.compute_hash("update", &row_data);

    let actor_val = match actor_id {
        Some(id) => crate::value::Value::I64(id),
        None => crate::value::Value::Null,
    };

    let (audit_sql, audit_params) = build_audit_insert(audit_table, "update", actor_val, row_data, row_hash);

    let affected = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let affected_clone = affected.clone();

    db.transaction(Box::new(move |tx| {
        Box::pin(async move {
            let n = tx.execute(&update_sql, &update_params).await?;
            affected_clone.store(n, std::sync::atomic::Ordering::Relaxed);
            tx.execute(&audit_sql, &audit_params).await?;
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
/// When [`AuditContext::hmac_secret`] is set, each audit row receives a
/// `row_hash` column with `HMAC-SHA256(secret, "delete|<actor_id>|<row_data>")`
/// so that any post-hoc modification is detectable via [`verify_audit_row`].
pub async fn audited_delete<M: Auditable + FromRow>(
    db: &impl Database,
    builder: DeleteBuilder<M>,
    ctx: &AuditContext,
) -> Result<u64, DbError> {
    // 1. Capture old rows before deletion (outside the transaction — read-only).
    let select = builder.to_select();
    let (select_sql, select_params) = select.build();
    let old_rows = db.query(&select_sql, &select_params).await?;

    // Serialize each row to JSON and pre-compute its HMAC (before the closure).
    let col_names: Vec<&'static str> = M::column_names().to_vec();
    let mut entries: Vec<(String, Option<String>)> = Vec::with_capacity(old_rows.len());
    for row in &old_rows {
        let vals: Vec<crate::value::Value> = col_names
            .iter()
            .map(|c| row.get(c).cloned().unwrap_or(crate::value::Value::Null))
            .collect();
        let col_refs: Vec<&str> = col_names.iter().map(|s| *s).collect();
        let row_data = values_to_json_string(&col_refs, &vals);
        let row_hash = ctx.compute_hash("delete", &row_data);
        entries.push((row_data, row_hash));
    }

    let (delete_sql, delete_params) = builder.build();
    let audit_table = M::audit_table_name();
    let actor_id = ctx.actor_id;

    let affected = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let affected_clone = affected.clone();

    db.transaction(Box::new(move |tx| {
        Box::pin(async move {
            let n = tx.execute(&delete_sql, &delete_params).await?;
            affected_clone.store(n, std::sync::atomic::Ordering::Relaxed);
            for (row_data, row_hash) in entries {
                let actor_val = match actor_id {
                    Some(id) => crate::value::Value::I64(id),
                    None => crate::value::Value::Null,
                };
                let (audit_sql, audit_params) =
                    build_audit_insert(audit_table, "delete", actor_val, row_data, row_hash);
                tx.execute(&audit_sql, &audit_params).await?;
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
fn build_audit_insert(
    audit_table: &str,
    operation: &str,
    actor_val: crate::value::Value,
    row_data: String,
    row_hash: Option<String>,
) -> (String, Vec<crate::value::Value>) {
    if let Some(hash) = row_hash {
        let sql = format!(
            "INSERT INTO {} (\"operation\", \"actor_id\", \"row_data\", \"row_hash\") VALUES (?, ?, ?, ?)",
            qi(audit_table)
        );
        let params = vec![
            crate::value::Value::String(operation.into()),
            actor_val,
            crate::value::Value::String(row_data),
            crate::value::Value::String(hash),
        ];
        (sql, params)
    } else {
        let sql = format!(
            "INSERT INTO {} (\"operation\", \"actor_id\", \"row_data\") VALUES (?, ?, ?)",
            qi(audit_table)
        );
        let params = vec![
            crate::value::Value::String(operation.into()),
            actor_val,
            crate::value::Value::String(row_data),
        ];
        (sql, params)
    }
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
        assert_eq!(defs[2].sql_type, SqlType::BigInt);
        assert!(defs[2].nullable);
        assert_eq!(defs[3].sql_type, SqlType::Timestamptz);
        assert_eq!(defs[3].default, Some("NOW()".to_string()));
        assert_eq!(defs[4].sql_type, SqlType::Jsonb);
        assert_eq!(defs[5].sql_type, SqlType::Text);
        assert!(defs[5].nullable);
    }

    // ── SHA-256 / HMAC-SHA256 ────────────────────────────────────────

    /// Known-answer test: SHA-256("") = e3b0c44298fc1c149afb...
    #[test]
    fn test_sha256_empty() {
        let digest = sha256(b"");
        let hex = hex_encode(&digest);
        assert_eq!(hex, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    /// Known-answer test: SHA-256("abc") = ba7816bf8f01cfea414140...
    #[test]
    fn test_sha256_abc() {
        let digest = sha256(b"abc");
        let hex = hex_encode(&digest);
        assert_eq!(hex, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
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
        assert_eq!(hex, "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");
    }

    /// HMAC-SHA256 RFC 4231 test vector #2.
    /// Key  = "Jefe"
    /// Data = "what do ya want for nothing?"
    /// Expected = 5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964a72424
    #[test]
    fn test_hmac_sha256_rfc4231_vector2() {
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        let hex = hex_encode(&mac);
        assert_eq!(hex, "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
    }

    // ── AuditContext::compute_hash ────────────────────────────────────

    #[test]
    fn test_compute_hash_none_when_no_secret() {
        let ctx = AuditContext::new(Some(1));
        assert!(ctx.compute_hash("update", "{}").is_none());
    }

    #[test]
    fn test_compute_hash_deterministic() {
        let ctx = AuditContext::with_integrity(Some(42), b"my-secret");
        let h1 = ctx.compute_hash("delete", r#"{"id":1}"#).unwrap();
        let h2 = ctx.compute_hash("delete", r#"{"id":1}"#).unwrap();
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64); // 32 bytes → 64 hex chars
    }

    #[test]
    fn test_compute_hash_differs_on_operation() {
        let ctx = AuditContext::with_integrity(Some(1), b"secret");
        let h_update = ctx.compute_hash("update", "{}").unwrap();
        let h_delete = ctx.compute_hash("delete", "{}").unwrap();
        assert_ne!(h_update, h_delete);
    }

    #[test]
    fn test_compute_hash_differs_on_actor() {
        let ctx1 = AuditContext::with_integrity(Some(1), b"secret");
        let ctx2 = AuditContext::with_integrity(Some(2), b"secret");
        assert_ne!(
            ctx1.compute_hash("update", "{}").unwrap(),
            ctx2.compute_hash("update", "{}").unwrap()
        );
    }

    #[test]
    fn test_compute_hash_null_actor() {
        let ctx = AuditContext::with_integrity(None, b"secret");
        let h = ctx.compute_hash("update", "{}");
        assert!(h.is_some());
    }

    // ── verify_audit_row ─────────────────────────────────────────────

    #[test]
    fn test_verify_audit_row_valid() {
        let ctx = AuditContext::with_integrity(Some(7), b"secret");
        let hash = ctx.compute_hash("delete", r#"{"id":1}"#).unwrap();
        assert_eq!(
            verify_audit_row(b"secret", "delete", Some(7), r#"{"id":1}"#, Some(&hash)),
            Some(true)
        );
    }

    #[test]
    fn test_verify_audit_row_tampered_data() {
        let ctx = AuditContext::with_integrity(Some(7), b"secret");
        let hash = ctx.compute_hash("delete", r#"{"id":1}"#).unwrap();
        // row_data changed after the fact
        assert_eq!(
            verify_audit_row(b"secret", "delete", Some(7), r#"{"id":99}"#, Some(&hash)),
            Some(false)
        );
    }

    #[test]
    fn test_verify_audit_row_tampered_operation() {
        let ctx = AuditContext::with_integrity(Some(7), b"secret");
        let hash = ctx.compute_hash("delete", r#"{"id":1}"#).unwrap();
        assert_eq!(
            verify_audit_row(b"secret", "update", Some(7), r#"{"id":1}"#, Some(&hash)),
            Some(false)
        );
    }

    #[test]
    fn test_verify_audit_row_tampered_actor() {
        let ctx = AuditContext::with_integrity(Some(7), b"secret");
        let hash = ctx.compute_hash("delete", r#"{"id":1}"#).unwrap();
        assert_eq!(
            verify_audit_row(b"secret", "delete", Some(99), r#"{"id":1}"#, Some(&hash)),
            Some(false)
        );
    }

    #[test]
    fn test_verify_audit_row_no_hash_stored() {
        // No hash stored → integrity not enabled for this row → None
        assert_eq!(
            verify_audit_row(b"secret", "delete", Some(7), r#"{"id":1}"#, None),
            None
        );
    }

    #[test]
    fn test_verify_audit_row_wrong_secret() {
        let ctx = AuditContext::with_integrity(Some(7), b"correct-secret");
        let hash = ctx.compute_hash("delete", r#"{"id":1}"#).unwrap();
        assert_eq!(
            verify_audit_row(b"wrong-secret", "delete", Some(7), r#"{"id":1}"#, Some(&hash)),
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
        assert!(!constant_time_eq(b"hi", b"hello"));
    }
}
