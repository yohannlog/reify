//! Unit tests for soft delete and custom SQL functionality.

use chrono::{DateTime, Utc};
use reify::{Table, Value};

// ── Model with soft delete ──────────────────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "articles")]
pub struct Article {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub title: String,
    #[column(soft_delete)]
    pub deleted_at: Option<DateTime<Utc>>,
}

// ── Model without soft delete ───────────────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "comments")]
pub struct Comment {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub body: String,
}

// ── Model with custom SQL (Hibernate-style @SQLDelete/@SQLUpdate/@SQLInsert) ──

#[derive(Table, Debug, Clone)]
#[table(
    name = "audit_users",
    sql_delete = "UPDATE audit_users SET deleted_at = NOW(), deleted_by = current_user WHERE id = ?",
    sql_update = "CALL update_audit_user(?, ?, ?)",
    sql_insert = "CALL insert_audit_user(?, ?, ?)"
)]
pub struct AuditUser {
    #[column(primary_key)]
    pub id: i64,
    pub name: String,
    pub email: String,
}

// ── Immutable model (Hibernate-style @Immutable) ──

#[derive(Table, Debug, Clone)]
#[table(name = "audit_log", immutable)]
pub struct AuditLog {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub event: String,
    pub payload: String,
}

// ── Table trait tests ───────────────────────────────────────────────

#[test]
fn soft_delete_column_returns_column_name() {
    assert_eq!(Article::soft_delete_column(), Some("deleted_at"));
}

#[test]
fn soft_delete_column_returns_none_for_regular_model() {
    assert_eq!(Comment::soft_delete_column(), None);
}

#[test]
fn column_defs_include_soft_delete_flag() {
    let defs = Article::column_defs();
    let deleted_at_def = defs.iter().find(|d| d.name == "deleted_at").unwrap();
    assert!(deleted_at_def.soft_delete);
    assert!(deleted_at_def.nullable);

    // Other columns should not have soft_delete flag
    let id_def = defs.iter().find(|d| d.name == "id").unwrap();
    assert!(!id_def.soft_delete);
}

// ── SelectBuilder tests ─────────────────────────────────────────────

#[test]
fn select_auto_filters_deleted_rows() {
    // Default: hide deleted rows
    reify::soft_delete::set_show_deleted(false);

    let (sql, params) = Article::find().build();
    assert!(
        sql.contains("WHERE \"deleted_at\" IS NULL"),
        "Expected soft delete filter, got: {sql}"
    );
    assert!(params.is_empty());
}

#[test]
fn select_with_deleted_includes_all_rows() {
    reify::soft_delete::set_show_deleted(false);

    let (sql, _) = Article::find().with_deleted().build();
    assert!(
        !sql.contains("deleted_at"),
        "with_deleted() should not filter: {sql}"
    );
}

#[test]
fn select_only_deleted_filters_to_deleted_rows() {
    reify::soft_delete::set_show_deleted(false);

    let (sql, _) = Article::find().only_deleted().build();
    assert!(
        sql.contains("WHERE \"deleted_at\" IS NOT NULL"),
        "only_deleted() should filter to deleted rows: {sql}"
    );
}

#[test]
fn select_respects_global_show_deleted_true() {
    reify::soft_delete::set_show_deleted(true);

    let (sql, _) = Article::find().build();
    assert!(
        !sql.contains("deleted_at"),
        "Global show_deleted=true should not filter: {sql}"
    );

    // Restore default
    reify::soft_delete::set_show_deleted(false);
}

#[test]
fn select_combines_soft_delete_with_user_filters() {
    reify::soft_delete::set_show_deleted(false);

    let (sql, params) = Article::find().filter(Article::title.eq("Hello")).build();

    // Should have both soft delete filter AND user filter
    assert!(
        sql.contains("\"deleted_at\" IS NULL"),
        "Missing soft delete filter: {sql}"
    );
    assert!(sql.contains("\"title\" = ?"), "Missing user filter: {sql}");
    assert_eq!(params.len(), 1);
    assert_eq!(params[0], Value::String("Hello".into()));
}

#[test]
fn select_no_soft_delete_filter_for_regular_model() {
    let (sql, _) = Comment::find().build();
    assert!(
        !sql.contains("deleted_at"),
        "Regular model should not have soft delete filter: {sql}"
    );
}

// ── DeleteBuilder tests ─────────────────────────────────────────────

#[test]
fn delete_uses_update_for_soft_delete_model() {
    let (sql, _) = Article::delete().filter(Article::id.eq(42i64)).build();

    assert!(
        sql.starts_with("UPDATE"),
        "Soft delete should emit UPDATE: {sql}"
    );
    assert!(
        sql.contains("SET \"deleted_at\" = CURRENT_TIMESTAMP"),
        "Should set deleted_at: {sql}"
    );
    assert!(
        sql.contains("WHERE \"id\" = ?"),
        "Should have WHERE clause: {sql}"
    );
}

#[test]
fn delete_force_uses_real_delete() {
    let (sql, _) = Article::delete()
        .filter(Article::id.eq(42i64))
        .force()
        .build();

    assert!(
        sql.starts_with("DELETE FROM"),
        "force() should emit DELETE: {sql}"
    );
    assert!(
        !sql.contains("deleted_at"),
        "force() should not reference deleted_at: {sql}"
    );
}

#[test]
fn delete_regular_model_uses_delete() {
    let (sql, _) = Comment::delete().filter(Comment::id.eq(1i64)).build();

    assert!(
        sql.starts_with("DELETE FROM"),
        "Regular model should use DELETE: {sql}"
    );
}

#[test]
fn is_soft_delete_returns_correct_value() {
    let soft_builder = Article::delete().filter(Article::id.eq(1i64));
    assert!(soft_builder.is_soft_delete());

    let force_builder = Article::delete().filter(Article::id.eq(1i64)).force();
    assert!(!force_builder.is_soft_delete());

    let regular_builder = Comment::delete().filter(Comment::id.eq(1i64));
    assert!(!regular_builder.is_soft_delete());
}

// ── Global config tests ─────────────────────────────────────────────

#[test]
fn global_config_get_set() {
    // Save current state
    let original = reify::soft_delete::show_deleted();

    reify::soft_delete::set_show_deleted(true);
    assert!(reify::soft_delete::show_deleted());

    reify::soft_delete::set_show_deleted(false);
    assert!(!reify::soft_delete::show_deleted());

    // Restore
    reify::soft_delete::set_show_deleted(original);
}

// ── Custom SQL tests (Hibernate-style @SQLDelete/@SQLUpdate/@SQLInsert) ──

#[test]
fn custom_sql_delete_uses_provided_sql() {
    let (sql, _) = AuditUser::delete().filter(AuditUser::id.eq(1i64)).build();

    assert!(
        sql.starts_with("UPDATE audit_users SET deleted_at = NOW()"),
        "Should use custom sql_delete: {sql}"
    );
    assert!(
        sql.contains("deleted_by = current_user"),
        "Should include deleted_by: {sql}"
    );
    // Filter should be appended with AND since custom SQL has WHERE
    assert!(
        sql.contains("AND \"id\" = ?"),
        "Should append filter with AND: {sql}"
    );
}

#[test]
fn custom_sql_update_uses_provided_sql() {
    let (sql, _) = AuditUser::update()
        .set(AuditUser::name, "New Name")
        .filter(AuditUser::id.eq(1i64))
        .build();

    assert!(
        sql.starts_with("CALL update_audit_user"),
        "Should use custom sql_update: {sql}"
    );
}

#[test]
fn custom_sql_insert_uses_provided_sql() {
    let user = AuditUser {
        id: 1,
        name: "Alice".into(),
        email: "alice@example.com".into(),
    };
    let (sql, params) = AuditUser::insert(&user).build();

    assert!(
        sql.starts_with("CALL insert_audit_user"),
        "Should use custom sql_insert: {sql}"
    );
    // Values should still be collected
    assert_eq!(params.len(), 3);
}

#[test]
fn trait_methods_return_custom_sql() {
    assert_eq!(
        AuditUser::sql_delete(),
        Some("UPDATE audit_users SET deleted_at = NOW(), deleted_by = current_user WHERE id = ?")
    );
    assert_eq!(
        AuditUser::sql_update(),
        Some("CALL update_audit_user(?, ?, ?)")
    );
    assert_eq!(
        AuditUser::sql_insert(),
        Some("CALL insert_audit_user(?, ?, ?)")
    );

    // Regular models return None
    assert_eq!(Article::sql_delete(), None);
    assert_eq!(Article::sql_update(), None);
    assert_eq!(Article::sql_insert(), None);
}

// ── Immutable entity tests ──────────────────────────────────────────

#[test]
fn immutable_model_has_find_and_insert() {
    // These should compile and work
    let _ = AuditLog::find();

    let log = AuditLog {
        id: 0,
        event: "user_login".into(),
        payload: "{}".into(),
    };
    let (sql, params) = AuditLog::insert(&log).build();

    assert!(
        sql.starts_with("INSERT INTO"),
        "Should generate INSERT: {sql}"
    );
    assert_eq!(params.len(), 3); // id, event, payload (id is auto_increment but still in writable_values)
}

// Note: AuditLog::update() and AuditLog::delete() do not exist at compile time.
// This is verified by the compile_fail test: immutable_no_update_delete.rs
