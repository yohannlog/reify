//! Unit tests for soft delete functionality.

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

    let (sql, params) = Article::find()
        .filter(Article::title.eq("Hello"))
        .build();

    // Should have both soft delete filter AND user filter
    assert!(sql.contains("\"deleted_at\" IS NULL"), "Missing soft delete filter: {sql}");
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
    let (sql, _) = Article::delete()
        .filter(Article::id.eq(42i64))
        .build();

    assert!(
        sql.starts_with("UPDATE"),
        "Soft delete should emit UPDATE: {sql}"
    );
    assert!(
        sql.contains("SET \"deleted_at\" = CURRENT_TIMESTAMP"),
        "Should set deleted_at: {sql}"
    );
    assert!(sql.contains("WHERE \"id\" = ?"), "Should have WHERE clause: {sql}");
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
    let (sql, _) = Comment::delete()
        .filter(Comment::id.eq(1i64))
        .build();

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
