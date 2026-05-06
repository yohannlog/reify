//! Integration tests for the audit hash chain against a real MySQL.
//!
//! Mirrors `pg_audit_chain.rs` and `sqlite_audit_chain.rs`. MySQL also
//! uses `SELECT … FOR UPDATE` to serialise concurrent chain extensions.
//!
//! Tables are prefixed with `mysql_audit_` so the suite can run
//! alongside the rest of the integration matrix without collisions.

#![cfg(feature = "mysql-integration-tests")]

use reify::{
    ActorId, AuditChainCheck, AuditContext, Dialect, MigrationRunner, Value, audited_insert,
    raw_execute, raw_query, verify_audit_chain,
};

use crate::MysqlFixture;

const SECRET: &[u8] = b"00112233445566778899aabbccddeeff";

#[derive(reify::Table, Debug, Clone)]
#[table(name = "mysql_audit_users", audit)]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    pub email: String,
}

async fn setup(db: &reify::MysqlDb) {
    MigrationRunner::new()
        .with_dialect(Dialect::Mysql)
        .add_audited_table::<User>()
        .run(db)
        .await
        .expect("migration");
}

fn ctx() -> AuditContext {
    AuditContext::with_integrity(ActorId::Int(1), SECRET)
        .expect("ctx")
        .with_dialect(Dialect::Mysql)
}

async fn insert_n(db: &reify::MysqlDb, n: i64) {
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
async fn mysql_audit_chain_round_trip_ok() {
    let Some(fx) = MysqlFixture::new(&["mysql_audit_users", "mysql_audit_users_audit"]).await
    else {
        return;
    };
    setup(&fx.db).await;

    insert_n(&fx.db, 3).await;

    let results = verify_audit_chain(&fx.db, "mysql_audit_users_audit", SECRET, true)
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
    fx.teardown().await;
}

#[tokio::test]
async fn mysql_audit_chain_detects_deleted_middle_row() {
    let Some(fx) = MysqlFixture::new(&["mysql_audit_users", "mysql_audit_users_audit"]).await
    else {
        return;
    };
    setup(&fx.db).await;

    insert_n(&fx.db, 3).await;

    let rows = raw_query(
        &fx.db,
        "SELECT audit_id FROM mysql_audit_users_audit ORDER BY audit_id ASC",
        &[],
    )
    .await
    .expect("query audit_ids");
    assert_eq!(rows.len(), 3);
    let middle_id = match rows[1].get("audit_id").cloned().unwrap_or(Value::Null) {
        Value::I64(n) => n,
        other => panic!("audit_id not i64: {other:?}"),
    };

    raw_execute(
        &fx.db,
        "DELETE FROM mysql_audit_users_audit WHERE audit_id = ?",
        &[Value::I64(middle_id)],
    )
    .await
    .expect("delete");

    let results = verify_audit_chain(&fx.db, "mysql_audit_users_audit", SECRET, true)
        .await
        .expect("verify_audit_chain");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].check, AuditChainCheck::Ok);
    assert!(
        matches!(results[1].check, AuditChainCheck::BrokenChain { .. }),
        "expected BrokenChain, got {:?}",
        results[1].check,
    );
    fx.teardown().await;
}

#[tokio::test]
async fn mysql_audit_chain_detects_tampered_row_data() {
    let Some(fx) = MysqlFixture::new(&["mysql_audit_users", "mysql_audit_users_audit"]).await
    else {
        return;
    };
    setup(&fx.db).await;

    insert_n(&fx.db, 2).await;

    let rows = raw_query(
        &fx.db,
        "SELECT audit_id FROM mysql_audit_users_audit ORDER BY audit_id ASC",
        &[],
    )
    .await
    .expect("query audit_ids");
    let target_id = match rows[1].get("audit_id").cloned().unwrap_or(Value::Null) {
        Value::I64(n) => n,
        other => panic!("audit_id not i64: {other:?}"),
    };

    raw_execute(
        &fx.db,
        "UPDATE mysql_audit_users_audit SET row_data = ? WHERE audit_id = ?",
        &[
            Value::String(r#"{"forged":true}"#.into()),
            Value::I64(target_id),
        ],
    )
    .await
    .expect("forge row_data");

    let results = verify_audit_chain(&fx.db, "mysql_audit_users_audit", SECRET, true)
        .await
        .expect("verify_audit_chain");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].check, AuditChainCheck::Ok);
    assert_eq!(
        results[1].check,
        AuditChainCheck::BadHash,
        "tampered row 2 must surface BadHash",
    );
    fx.teardown().await;
}
