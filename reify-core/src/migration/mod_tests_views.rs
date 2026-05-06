//! View and materialized-view migration tests, extracted from
//! `migration/mod.rs` to keep that file under 1000 LOC.

#![cfg(test)]

use super::test_support::*;
use super::*;
use crate::db::Row;
use crate::table::Table;
use crate::value::Value;

// ── View migration tests ────────────────────────────────────────

#[test]
fn migration_context_create_view() {
    let mut ctx = MigrationContext::new();
    ctx.create_view(
        "active_users",
        "SELECT id, email FROM users WHERE deleted_at IS NULL",
    );
    assert_eq!(ctx.statements().len(), 1);
    assert!(ctx.statements()[0].contains("CREATE OR REPLACE VIEW \"active_users\""));
    assert!(ctx.statements()[0].contains("SELECT id, email FROM users"));
}

#[test]
fn migration_context_drop_view() {
    let mut ctx = MigrationContext::new();
    ctx.drop_view("active_users");
    assert_eq!(ctx.statements().len(), 1);
    assert!(ctx.statements()[0].contains("DROP VIEW IF EXISTS \"active_users\""));
}

// Minimal View impl for tests
struct TestView;
impl Table for TestView {
    fn table_name() -> &'static str {
        "active_users"
    }
    fn column_names() -> &'static [&'static str] {
        &["id", "email"]
    }
    fn as_values(&self) -> Vec<Value> {
        vec![]
    }
}
impl crate::view::View for TestView {
    fn view_name() -> &'static str {
        "active_users"
    }
    fn view_query() -> crate::view::ViewQuery {
        crate::view::ViewQuery::Raw("SELECT id, email FROM users WHERE deleted_at IS NULL".into())
    }
}

#[tokio::test]
async fn dry_run_view_emits_create_view() {
    let db = MockDb::new();
    db.push_query_result(vec![]); // applied_versions → empty

    let runner = MigrationRunner::new().add_view::<TestView>();
    let plans = runner.dry_run(&db).await.unwrap();

    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].version, "auto_view__active_users");
    assert!(plans[0].statements[0].contains("CREATE OR REPLACE VIEW \"active_users\""));
}

#[tokio::test]
async fn dry_run_view_skips_already_applied() {
    let db = MockDb::new();
    let applied_row = Row::new(
        vec!["version".into()],
        vec![Value::String("auto_view__active_users".into())],
    );
    db.push_query_result(vec![applied_row]);

    let runner = MigrationRunner::new().add_view::<TestView>();
    let plans = runner.dry_run(&db).await.unwrap();

    assert!(plans.is_empty());
}

#[tokio::test]
async fn run_view_executes_create_view() {
    let db = MockDb::new();
    db.push_query_result(vec![]); // applied_versions → empty

    let runner = MigrationRunner::new().add_view::<TestView>();
    runner.run(&db).await.unwrap();

    let sql = db.executed_sql();
    let has_create_view = sql
        .iter()
        .any(|s| s.contains("CREATE OR REPLACE VIEW \"active_users\""));
    assert!(has_create_view, "expected CREATE VIEW in: {sql:?}");
}

#[test]
fn generate_view_migration_file_produces_valid_template() {
    let content = generate_view_migration_file("active_users", "20240320_000001_active_users");
    assert!(content.contains("struct ActiveUsers"));
    assert!(content.contains("impl Migration for ActiveUsers"));
    assert!(content.contains("ctx.create_view(\"active_users\""));
    assert!(content.contains("ctx.drop_view(\"active_users\""));
}

// ── Materialized view migration tests ──────────────────────────────

#[test]
fn migration_context_create_materialized_view() {
    let mut ctx = MigrationContext::new();
    ctx.create_materialized_view(
        "sales_summary",
        "SELECT seller_no, invoice_date, sum(invoice_amt) FROM invoice GROUP BY 1, 2",
    );
    assert_eq!(ctx.statements().len(), 1);
    assert!(
        ctx.statements()[0].contains("CREATE MATERIALIZED VIEW IF NOT EXISTS \"sales_summary\"")
    );
    assert!(ctx.statements()[0].contains("WITH DATA"));
}

#[test]
fn migration_context_create_materialized_view_no_data() {
    let mut ctx = MigrationContext::new();
    ctx.create_materialized_view_no_data("sales_summary", "SELECT 1");
    assert_eq!(ctx.statements().len(), 1);
    assert!(ctx.statements()[0].contains("WITH NO DATA"));
}

#[test]
fn migration_context_drop_materialized_view() {
    let mut ctx = MigrationContext::new();
    ctx.drop_materialized_view("sales_summary");
    assert_eq!(ctx.statements().len(), 1);
    assert!(ctx.statements()[0].contains("DROP MATERIALIZED VIEW IF EXISTS \"sales_summary\""));
}

#[test]
fn migration_context_refresh_materialized_view_blocking() {
    let mut ctx = MigrationContext::new();
    ctx.refresh_materialized_view("sales_summary", false);
    assert_eq!(ctx.statements().len(), 1);
    assert_eq!(
        ctx.statements()[0],
        "REFRESH MATERIALIZED VIEW \"sales_summary\";"
    );
}

#[test]
fn migration_context_refresh_materialized_view_concurrently() {
    let mut ctx = MigrationContext::new();
    ctx.refresh_materialized_view("sales_summary", true);
    assert_eq!(ctx.statements().len(), 1);
    assert_eq!(
        ctx.statements()[0],
        "REFRESH MATERIALIZED VIEW CONCURRENTLY \"sales_summary\";"
    );
}

// Minimal View impl for materialized view tests
struct TestMatView;
impl Table for TestMatView {
    fn table_name() -> &'static str {
        "sales_summary"
    }
    fn column_names() -> &'static [&'static str] {
        &["seller_no", "sales_amt"]
    }
    fn as_values(&self) -> Vec<Value> {
        vec![]
    }
}
impl crate::view::View for TestMatView {
    fn view_name() -> &'static str {
        "sales_summary"
    }
    fn view_query() -> crate::view::ViewQuery {
        crate::view::ViewQuery::Raw(
            "SELECT seller_no, sum(invoice_amt) FROM invoice GROUP BY seller_no".into(),
        )
    }
}

#[tokio::test]
async fn dry_run_materialized_view_emits_create_materialized_view() {
    let db = MockDb::new();
    db.push_query_result(vec![]); // applied_versions → empty

    let runner = MigrationRunner::new().add_materialized_view::<TestMatView>();
    let plans = runner.dry_run(&db).await.unwrap();

    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].version, "auto_matview__sales_summary");
    assert!(
        plans[0].statements[0].contains("CREATE MATERIALIZED VIEW IF NOT EXISTS \"sales_summary\"")
    );
    assert!(plans[0].statements[0].contains("WITH DATA"));
}

#[tokio::test]
async fn dry_run_materialized_view_skips_already_applied() {
    let db = MockDb::new();
    let applied_row = Row::new(
        vec!["version".into()],
        vec![Value::String("auto_matview__sales_summary".into())],
    );
    db.push_query_result(vec![applied_row]);

    let runner = MigrationRunner::new().add_materialized_view::<TestMatView>();
    let plans = runner.dry_run(&db).await.unwrap();

    assert!(plans.is_empty());
}

#[tokio::test]
async fn run_materialized_view_executes_create_materialized_view() {
    let db = MockDb::new();
    db.push_query_result(vec![]); // applied_versions → empty

    let runner = MigrationRunner::new().add_materialized_view::<TestMatView>();
    runner.run(&db).await.unwrap();

    let sql = db.executed_sql();
    let has_create = sql
        .iter()
        .any(|s| s.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS \"sales_summary\""));
    assert!(has_create, "expected CREATE MATERIALIZED VIEW in: {sql:?}");
}

#[test]
fn generate_materialized_view_migration_file_produces_valid_template() {
    let content =
        generate_materialized_view_migration_file("sales_summary", "20240320_000001_sales_summary");
    assert!(content.contains("struct SalesSummary"));
    assert!(content.contains("impl Migration for SalesSummary"));
    assert!(content.contains("ctx.create_materialized_view(\"sales_summary\""));
    assert!(content.contains("ctx.drop_materialized_view(\"sales_summary\""));
}
