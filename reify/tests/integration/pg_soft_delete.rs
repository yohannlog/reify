//! Integration tests for soft delete with PostgreSQL.
//!
//! Covers:
//! - Soft delete emits UPDATE instead of DELETE
//! - `find()` auto-filters deleted rows
//! - `find().with_deleted()` includes deleted rows
//! - `find().only_deleted()` returns only deleted rows
//! - `delete().force()` performs hard DELETE

#![cfg(feature = "pg-integration-tests")]

use chrono::{DateTime, Utc};
use reify::{Table, delete, fetch, insert, raw_execute, update};

use crate::PgFixture;

#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "pg_soft_delete_articles")]
pub struct Article {
    #[column(primary_key)]
    pub id: i64,
    pub title: String,
    #[column(soft_delete)]
    pub deleted_at: Option<DateTime<Utc>>,
}

async fn create_table(db: &reify::PostgresDb) {
    raw_execute(
        db,
        "CREATE TABLE pg_soft_delete_articles (
            id         BIGINT PRIMARY KEY,
            title      TEXT NOT NULL,
            deleted_at TIMESTAMPTZ
        )",
        &[],
    )
    .await
    .expect("create pg_soft_delete_articles");
}

async fn fixture() -> Option<PgFixture> {
    PgFixture::new(&["pg_soft_delete_articles"]).await
}

#[tokio::test]
async fn soft_delete_emits_update() {
    let Some(fx) = fixture().await else { return };
    create_table(&fx.db).await;

    // Insert a row
    let article = Article {
        id: 1,
        title: "Hello World".into(),
        deleted_at: None,
    };
    insert(&fx.db, &Article::insert(&article))
        .await
        .expect("insert");

    // Soft delete it
    delete(&fx.db, &Article::delete().filter(Article::id.eq(1i64)))
        .await
        .expect("soft delete");

    // Row should still exist in DB with deleted_at set
    let rows: Vec<Article> = fetch(
        &fx.db,
        &Article::find().filter(Article::id.eq(1i64)).with_deleted(),
    )
    .await
    .expect("fetch with_deleted");

    assert_eq!(rows.len(), 1);
    assert!(rows[0].deleted_at.is_some(), "deleted_at should be set");

    fx.teardown().await;
}

#[tokio::test]
async fn find_auto_filters_deleted_rows() {
    let Some(fx) = fixture().await else { return };
    create_table(&fx.db).await;

    // Insert two rows
    insert(
        &fx.db,
        &Article::insert(&Article {
            id: 1,
            title: "Active".into(),
            deleted_at: None,
        }),
    )
    .await
    .expect("insert active");

    insert(
        &fx.db,
        &Article::insert(&Article {
            id: 2,
            title: "To Delete".into(),
            deleted_at: None,
        }),
    )
    .await
    .expect("insert to_delete");

    // Soft delete one
    delete(&fx.db, &Article::delete().filter(Article::id.eq(2i64)))
        .await
        .expect("soft delete");

    // Default find() should only return active row
    let rows: Vec<Article> = fetch(&fx.db, &Article::find()).await.expect("fetch");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].title, "Active");

    fx.teardown().await;
}

#[tokio::test]
async fn find_with_deleted_includes_all() {
    let Some(fx) = fixture().await else { return };
    create_table(&fx.db).await;

    insert(
        &fx.db,
        &Article::insert(&Article {
            id: 1,
            title: "Active".into(),
            deleted_at: None,
        }),
    )
    .await
    .expect("insert");

    insert(
        &fx.db,
        &Article::insert(&Article {
            id: 2,
            title: "Deleted".into(),
            deleted_at: None,
        }),
    )
    .await
    .expect("insert");

    delete(&fx.db, &Article::delete().filter(Article::id.eq(2i64)))
        .await
        .expect("soft delete");

    // with_deleted() should return both
    let rows: Vec<Article> = fetch(&fx.db, &Article::find().with_deleted())
        .await
        .expect("fetch");

    assert_eq!(rows.len(), 2);

    fx.teardown().await;
}

#[tokio::test]
async fn find_only_deleted_returns_deleted_only() {
    let Some(fx) = fixture().await else { return };
    create_table(&fx.db).await;

    insert(
        &fx.db,
        &Article::insert(&Article {
            id: 1,
            title: "Active".into(),
            deleted_at: None,
        }),
    )
    .await
    .expect("insert");

    insert(
        &fx.db,
        &Article::insert(&Article {
            id: 2,
            title: "Deleted".into(),
            deleted_at: None,
        }),
    )
    .await
    .expect("insert");

    delete(&fx.db, &Article::delete().filter(Article::id.eq(2i64)))
        .await
        .expect("soft delete");

    // only_deleted() should return only the deleted row
    let rows: Vec<Article> = fetch(&fx.db, &Article::find().only_deleted())
        .await
        .expect("fetch");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].title, "Deleted");

    fx.teardown().await;
}

#[tokio::test]
async fn force_delete_performs_hard_delete() {
    let Some(fx) = fixture().await else { return };
    create_table(&fx.db).await;

    insert(
        &fx.db,
        &Article::insert(&Article {
            id: 1,
            title: "To Hard Delete".into(),
            deleted_at: None,
        }),
    )
    .await
    .expect("insert");

    // Force hard delete
    delete(
        &fx.db,
        &Article::delete().filter(Article::id.eq(1i64)).force(),
    )
    .await
    .expect("force delete");

    // Row should be completely gone
    let rows: Vec<Article> = fetch(&fx.db, &Article::find().with_deleted())
        .await
        .expect("fetch");

    assert_eq!(rows.len(), 0, "row should be hard deleted");

    fx.teardown().await;
}

#[tokio::test]
async fn default_find_filters_deleted_with_deleted_overrides() {
    // Pins the post-removal-of-`set_show_deleted` semantics: default
    // `find()` always hides soft-deleted rows, and the only way to see
    // them is the per-query `.with_deleted()` opt-in.
    let Some(fx) = fixture().await else { return };
    create_table(&fx.db).await;

    insert(
        &fx.db,
        &Article::insert(&Article {
            id: 1,
            title: "Active".into(),
            deleted_at: None,
        }),
    )
    .await
    .expect("insert");

    insert(
        &fx.db,
        &Article::insert(&Article {
            id: 2,
            title: "Deleted".into(),
            deleted_at: None,
        }),
    )
    .await
    .expect("insert");

    delete(&fx.db, &Article::delete().filter(Article::id.eq(2i64)))
        .await
        .expect("soft delete");

    let active_only: Vec<Article> = fetch(&fx.db, &Article::find())
        .await
        .expect("fetch default");
    assert_eq!(
        active_only.len(),
        1,
        "default find() must hide deleted rows"
    );
    assert_eq!(active_only[0].id, 1);

    let all: Vec<Article> = fetch(&fx.db, &Article::find().with_deleted())
        .await
        .expect("fetch with_deleted");
    assert_eq!(all.len(), 2, ".with_deleted() must include deleted rows");

    fx.teardown().await;
}

#[tokio::test]
async fn restore_soft_deleted_row() {
    let Some(fx) = fixture().await else { return };
    create_table(&fx.db).await;

    insert(
        &fx.db,
        &Article::insert(&Article {
            id: 1,
            title: "Restorable".into(),
            deleted_at: None,
        }),
    )
    .await
    .expect("insert");

    // Soft delete
    delete(&fx.db, &Article::delete().filter(Article::id.eq(1i64)))
        .await
        .expect("soft delete");

    // Verify it's deleted
    let rows: Vec<Article> = fetch(&fx.db, &Article::find()).await.expect("fetch");
    assert_eq!(rows.len(), 0);

    // Restore by setting deleted_at to NULL
    update(
        &fx.db,
        &Article::update()
            .set(Article::deleted_at, None::<DateTime<Utc>>)
            .filter(Article::id.eq(1i64)),
    )
    .await
    .expect("restore");

    // Should be visible again
    let rows: Vec<Article> = fetch(&fx.db, &Article::find()).await.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].title, "Restorable");

    fx.teardown().await;
}
