//! Integration tests for the audit hash chain against a real SQLite database.
//!
//! Mirrors the unit tests in `audit::tests` (which use a `MockChainDb`) but
//! exercises `audited_insert` / `audited_update` / `audited_delete` end-to-end
//! against the live SQLite adapter, confirming that:
//!
//! 1. The chain is contiguous across multiple `audited_*` calls.
//! 2. Verifying a clean chain returns `Ok` for every row.
//! 3. Manually deleting an audit row is detected as `BrokenChain` on the
//!    row that followed it.
//! 4. Tampering with `row_data` of a row whose hash is left in place is
//!    detected as `BadHash`.
//!
//! SQLite is in-memory per test — every test opens a fresh database, so
//! audit_id always starts at 1 and there is no cross-test bleed.

#![cfg(feature = "sqlite-integration-tests")]

use reify::{
    ActorId, AuditChainCheck, AuditContext, Dialect, MigrationRunner, SqliteDb, Value,
    audited_insert, raw_execute, verify_audit_chain,
};

// 32 bytes — meets the NIST minimum and silences the short-secret warning.
const SECRET: &[u8] = b"00112233445566778899aabbccddeeff";

#[derive(reify::Table, Debug, Clone)]
#[table(name = "users", audit)]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    pub email: String,
}

async fn setup(db: &SqliteDb) {
    // Use the runner so the audit table is created with the same shape
    // production code expects (7 columns including `prev_hash`).
    MigrationRunner::new()
        .with_dialect(Dialect::Sqlite)
        .add_audited_table::<User>()
        .run(db)
        .await
        .expect("migration");
}

fn ctx() -> AuditContext {
    AuditContext::with_integrity(ActorId::Int(1), SECRET).expect("ctx")
}

async fn insert_n(db: &SqliteDb, n: i64) {
    for i in 1..=n {
        let u = User {
            id: i,
            email: format!("u{i}@example.com"),
        };
        audited_insert::<User>(db, User::insert(&u), &ctx())
            .await
            .expect("audited_insert");
    }
}

#[tokio::test]
async fn sqlite_audit_chain_round_trip_ok() {
    let db = SqliteDb::open_in_memory().expect("open db");
    setup(&db).await;

    insert_n(&db, 3).await;

    let results = verify_audit_chain(&db, "users_audit", SECRET, true)
        .await
        .expect("verify_audit_chain");

    assert_eq!(results.len(), 3);
    for r in &results {
        assert_eq!(
            r.check,
            AuditChainCheck::Ok,
            "row {} unexpectedly failed: {:?}",
            r.audit_id,
            r.check,
        );
    }
}

#[tokio::test]
async fn sqlite_audit_chain_first_row_has_null_prev_hash() {
    // The chain head must have `prev_hash IS NULL` — pinning this
    // guarantees an attacker cannot forge a fake "row 0" upstream and
    // still produce a valid chain on top of it.
    let db = SqliteDb::open_in_memory().expect("open db");
    setup(&db).await;

    insert_n(&db, 1).await;

    let rows = reify::raw_query(
        &db,
        "SELECT prev_hash FROM users_audit ORDER BY audit_id",
        &[],
    )
    .await
    .expect("query");
    assert_eq!(rows.len(), 1);
    let prev = rows[0].get("prev_hash").cloned().unwrap_or(Value::Null);
    assert!(
        matches!(prev, Value::Null),
        "first audit row's prev_hash must be NULL, got: {prev:?}",
    );
}

#[tokio::test]
async fn sqlite_audit_chain_detects_deleted_middle_row() {
    let db = SqliteDb::open_in_memory().expect("open db");
    setup(&db).await;

    insert_n(&db, 3).await;

    // Manually delete the middle audit row — this is the attacker
    // scenario that pre-chaining audit logs could not detect.
    raw_execute(&db, "DELETE FROM users_audit WHERE audit_id = 2", &[])
        .await
        .expect("delete");

    let results = verify_audit_chain(&db, "users_audit", SECRET, true)
        .await
        .expect("verify_audit_chain");

    assert_eq!(results.len(), 2, "row 2 deleted, two rows remain");
    assert_eq!(results[0].check, AuditChainCheck::Ok, "row 1 must verify");
    match &results[1].check {
        AuditChainCheck::BrokenChain { expected, found } => {
            // `expected` is row 1's row_hash (now the predecessor); `found`
            // is row 3's `prev_hash`, which still points at row 2's hash.
            assert!(expected.is_some());
            assert!(found.is_some());
            assert_ne!(expected, found, "the whole point of the chain");
        }
        other => panic!("expected BrokenChain on row 3, got {other:?}"),
    }
}

#[tokio::test]
async fn sqlite_audit_chain_detects_tampered_row_data() {
    let db = SqliteDb::open_in_memory().expect("open db");
    setup(&db).await;

    insert_n(&db, 2).await;

    // Forge `row_data` on row 2 while leaving its `row_hash` intact —
    // simulates an in-place attacker editing the audit log.
    raw_execute(
        &db,
        "UPDATE users_audit SET row_data = ? WHERE audit_id = 2",
        &[Value::String(r#"{"forged":true}"#.into())],
    )
    .await
    .expect("update");

    let results = verify_audit_chain(&db, "users_audit", SECRET, true)
        .await
        .expect("verify_audit_chain");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].check, AuditChainCheck::Ok);
    assert_eq!(
        results[1].check,
        AuditChainCheck::BadHash,
        "tampered row 2 must surface BadHash",
    );
}

#[tokio::test]
async fn sqlite_audit_chain_extends_across_subsequent_inserts() {
    // Insert 2 rows in one "batch", then 2 more in a second batch, and
    // confirm the chain is contiguous across the boundary — i.e. row 3's
    // prev_hash points at row 2 (not at NULL again).
    let db = SqliteDb::open_in_memory().expect("open db");
    setup(&db).await;

    insert_n(&db, 2).await;
    insert_n(&db, 0).await; // no-op
    let u = User {
        id: 3,
        email: "u3@example.com".into(),
    };
    audited_insert::<User>(&db, User::insert(&u), &ctx())
        .await
        .expect("audited_insert 3");
    let u = User {
        id: 4,
        email: "u4@example.com".into(),
    };
    audited_insert::<User>(&db, User::insert(&u), &ctx())
        .await
        .expect("audited_insert 4");

    let rows = reify::raw_query(
        &db,
        "SELECT audit_id, row_hash, prev_hash FROM users_audit ORDER BY audit_id",
        &[],
    )
    .await
    .expect("query");
    assert_eq!(rows.len(), 4);

    // Chain pointer of row N+1 must equal row_hash of row N.
    for i in 1..rows.len() {
        let prev = rows[i].get("prev_hash").cloned().unwrap_or(Value::Null);
        let predecessor_hash = rows[i - 1].get("row_hash").cloned().unwrap_or(Value::Null);
        assert_eq!(
            prev,
            predecessor_hash,
            "row {} prev_hash must equal row {} row_hash",
            i + 1,
            i,
        );
    }

    let results = verify_audit_chain(&db, "users_audit", SECRET, true)
        .await
        .expect("verify_audit_chain");
    for r in &results {
        assert_eq!(r.check, AuditChainCheck::Ok);
    }
}
