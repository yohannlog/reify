//! # Audit logging example
//!
//! Demonstrates the full audit system: tamper-evident HMAC-SHA256 row hashes,
//! all three audited operations (INSERT / UPDATE / DELETE), and hash verification.
//!
//! Run with:
//! ```text
//! cargo run --example audit
//! ```
//!
//! ## HMAC secret rotation
//!
//! The `row_hash` column stores `HMAC-SHA256(secret, message)` for every audit
//! row. When you rotate the secret you **must not** re-hash old rows with the new
//! key — doing so would destroy the tamper-evidence of the historical log.
//!
//! ### Recommended rotation procedure
//!
//! 1. **Add a `key_version` column** to your audit table (e.g. `SMALLINT NOT NULL
//!    DEFAULT 1`). Stamp every new row with the current key version.
//!
//! 2. **Keep old keys** in a versioned store (environment variables, a secrets
//!    manager, or an encrypted config file). Never delete a key while rows signed
//!    with it still exist.
//!
//! 3. **Verify with the matching key**: when calling [`verify_audit_row`] look up
//!    the key by `key_version` rather than always using the latest secret.
//!
//! 4. **Rotate at a quiet moment** (low write traffic). After the cutover, all
//!    new [`AuditContext`] instances use the new secret; old rows remain verifiable
//!    with the old secret.
//!
//! 5. **Retire old keys** only after you are certain no unverified rows signed
//!    with them remain (e.g. after a full audit sweep).
//!
//! ### Minimal example
//!
//! ```rust,ignore
//! // secrets.rs — versioned key store
//! fn secret_for_version(v: u16) -> &'static [u8] {
//!     match v {
//!         1 => b"old-secret-keep-forever",
//!         2 => b"new-secret-2025",
//!         _ => panic!("unknown key version {v}"),
//!     }
//! }
//!
//! // Writing new rows — always use the current version.
//! let ctx = AuditContext::with_integrity(actor, secret_for_version(CURRENT_VERSION));
//!
//! // Verifying an old row — use the version stored alongside the row.
//! let ok = verify_audit_row(
//!     secret_for_version(row.key_version),
//!     &row.operation,
//!     &row.actor_id,
//!     &row.changed_at,
//!     &row.row_data,
//!     row.row_hash.as_deref(),
//!     true,
//! );
//! ```

use std::sync::{Arc, Mutex};

use reify::{
    ActorId, AuditContext, Database, DbError, MigrationRunner, Row, TransactionFn, Value,
    audit::verify_audit_row,
};

// ── Schema ────────────────────────────────────────────────────────────────────

/// A user record with full audit logging enabled via `#[table(audit)]`.
///
/// Reify automatically generates:
/// - An `Auditable` impl pointing to the `users_audit` shadow table.
/// - `audit_column_defs()` with the six fixed audit columns.
#[derive(reify::Table, Debug, Clone)]
#[table(name = "users", audit)]
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
    pub role: Option<String>,
}

// ── In-memory mock database ───────────────────────────────────────────────────

/// Captures every SQL statement and its parameters so we can inspect them
/// without a real database connection.
#[derive(Clone)]
#[allow(clippy::type_complexity)] // example scaffolding, expressive shape > alias
struct MockDb {
    log: Arc<Mutex<Vec<(String, Vec<Value>)>>>,
    /// Pre-loaded query results consumed in FIFO order.
    results: Arc<Mutex<Vec<Vec<Row>>>>,
}

impl MockDb {
    fn new() -> Self {
        Self {
            log: Arc::new(Mutex::new(Vec::new())),
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn push_rows(&self, rows: Vec<Row>) {
        self.results.lock().unwrap().push(rows);
    }

    fn statements(&self) -> Vec<String> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .map(|(s, _)| s.clone())
            .collect()
    }

    fn params_for(&self, idx: usize) -> Vec<Value> {
        self.log.lock().unwrap()[idx].1.clone()
    }
}

impl Database for MockDb {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        self.log
            .lock()
            .unwrap()
            .push((sql.to_string(), params.to_vec()));
        Ok(1)
    }

    async fn query(&self, _sql: &str, _params: &[Value]) -> Result<Vec<Row>, DbError> {
        let mut q = self.results.lock().unwrap();
        Ok(if q.is_empty() { vec![] } else { q.remove(0) })
    }

    async fn query_one(&self, _sql: &str, _params: &[Value]) -> Result<Row, DbError> {
        Err(DbError::Query("no rows".into()))
    }

    #[allow(clippy::manual_async_fn)] // matches the trait signature (+ Send)
    fn transaction<'a>(
        &'a self,
        f: TransactionFn<'a>,
    ) -> impl Future<Output = Result<(), DbError>> + Send {
        async move { f(self).await }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn make_user_row(id: i64, email: &str) -> Row {
    Row::new(
        vec!["id".into(), "email".into(), "role".into()],
        vec![Value::I64(id), Value::String(email.into()), Value::Null],
    )
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    // ── 1. Schema migration ───────────────────────────────────────────────────
    //
    // `add_audited_table` registers both `users` and `users_audit` in a single
    // call. The audit table gets the six fixed columns automatically.
    println!("=== Schema (dry-run) ===");
    {
        let db = MockDb::new();
        db.push_rows(vec![]); // no existing columns for `users`
        db.push_rows(vec![]); // no existing columns for `users_audit`

        let plans = MigrationRunner::new()
            .add_audited_table::<User>()
            .dry_run(&db)
            .await
            .unwrap();

        for plan in &plans {
            for stmt in &plan.statements {
                println!("  {stmt}");
            }
        }
    }

    // ── 2. Audited INSERT ─────────────────────────────────────────────────────
    //
    // `audited_insert` wraps the INSERT and the audit-row INSERT in a single
    // transaction. The `row_hash` column is populated when a secret is provided.
    println!("\n=== Audited INSERT ===");
    let secret = b"super-secret-hmac-key";
    {
        let db = MockDb::new();
        let ctx = AuditContext::with_integrity(ActorId::Int(42), secret.as_slice()).unwrap();

        let user = User {
            id: 0,
            email: "alice@example.com".into(),
            role: None,
        };
        reify::audited_insert(&db, User::insert(&user), &ctx)
            .await
            .unwrap();

        let stmts = db.statements();
        println!("  [1] {}", stmts[0]);
        println!("  [2] {}", stmts[1]);

        let audit_params = db.params_for(1);
        println!("  operation : {:?}", audit_params[0]);
        println!("  actor_id  : {:?}", audit_params[1]);
        println!("  row_data  : {:?}", audit_params[2]);
        println!("  row_hash  : {:?}", audit_params[3]);
    }

    // ── 3. Audited UPDATE ─────────────────────────────────────────────────────
    //
    // `audited_update` captures a before-image (SELECT … FOR UPDATE), applies
    // the UPDATE, then reads the after-image. The audit row stores both under
    // `{"before":{...},"after":{...}}`.
    println!("\n=== Audited UPDATE ===");
    {
        let db = MockDb::new();
        db.push_rows(vec![make_user_row(1, "alice@example.com")]); // before-image
        db.push_rows(vec![make_user_row(1, "alice@newdomain.com")]); // after-image

        let ctx =
            AuditContext::with_integrity(ActorId::String("svc-worker".into()), secret.as_slice())
                .unwrap();
        let builder = User::update()
            .set(User::email, "alice@newdomain.com")
            .filter(User::id.eq(1i64));

        reify::audited_update(&db, builder, &ctx).await.unwrap();

        let stmts = db.statements();
        println!("  [1] {}", stmts[0]); // UPDATE
        println!("  [2] {}", stmts[1]); // audit INSERT

        let row_data = match &db.params_for(1)[2] {
            Value::String(s) => s.clone(),
            v => panic!("unexpected: {v:?}"),
        };
        println!("  row_data  : {row_data}");
    }

    // ── 4. Audited DELETE ─────────────────────────────────────────────────────
    //
    // `audited_delete` captures the rows before deletion. One audit row is
    // written per deleted record.
    println!("\n=== Audited DELETE ===");
    {
        let db = MockDb::new();
        db.push_rows(vec![make_user_row(1, "alice@example.com")]);

        let ctx = AuditContext::with_integrity(ActorId::Int(42), secret.as_slice()).unwrap();
        let builder = User::delete().filter(User::id.eq(1i64));

        reify::audited_delete::<User>(&db, builder, &ctx)
            .await
            .unwrap();

        let stmts = db.statements();
        println!("  [1] {}", stmts[0]); // DELETE
        println!("  [2] {}", stmts[1]); // audit INSERT
    }

    // ── 5. Hash verification ──────────────────────────────────────────────────
    //
    // After reading an audit row back from the database, call `verify_audit_row`
    // to confirm it has not been tampered with. Pass `integrity_expected = true`
    // when your application always writes hashes — a NULL hash is then treated
    // as evidence of tampering rather than "integrity not configured".
    println!("\n=== Hash verification ===");
    {
        let ts = "2025-06-01T12:00:00Z";
        let row_data = r#"{"id":1,"email":"alice@example.com","role":null}"#;

        let ctx = AuditContext::with_integrity(ActorId::Int(42), secret.as_slice()).unwrap();
        let hash = ctx.compute_hash("delete", ts, row_data).unwrap();

        // Valid row.
        let result = verify_audit_row(secret, "delete", "42", ts, row_data, Some(&hash), true);
        println!("  valid row          : {result:?}"); // Some(true)

        // Tampered row_data.
        let tampered = verify_audit_row(
            secret,
            "delete",
            "42",
            ts,
            r#"{"id":1,"email":"attacker@evil.com","role":null}"#,
            Some(&hash),
            true,
        );
        println!("  tampered row_data  : {tampered:?}"); // Some(false)

        // Antedated timestamp.
        let antedated = verify_audit_row(
            secret,
            "delete",
            "42",
            "2020-01-01T00:00:00Z",
            row_data,
            Some(&hash),
            true,
        );
        println!("  antedated ts       : {antedated:?}"); // Some(false)

        // Missing hash when integrity is expected (e.g. attacker nullified it).
        let nullified = verify_audit_row(secret, "delete", "42", ts, row_data, None, true);
        println!("  nullified hash     : {nullified:?}"); // Some(false)

        // Missing hash when integrity was never configured for this row.
        let no_integrity = verify_audit_row(secret, "delete", "42", ts, row_data, None, false);
        println!("  no integrity       : {no_integrity:?}"); // None
    }

    println!("\nDone.");
}
