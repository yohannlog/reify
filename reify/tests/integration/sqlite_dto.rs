//! SQLite DTO integration test.
//!
//! The SQLite adapter only accepts a narrow set of `Value` variants:
//! `Null`, `Bool`, `I16/I32/I64`, `F32/F64`, `String`, `Bytes`. Any
//! chrono temporal variant (`Timestamp`, `Date`, `Time`, `Timestamptz`),
//! `Uuid`, `Jsonb`, arrays and ranges are rejected with a
//! `DbError::Conversion` by `reify-sqlite/src/lib.rs::value_to_sqlite`.
//!
//! This test therefore exercises the DTO code path with the scalar
//! subset SQLite accepts:
//! - Full-model round-trip through a `Table` + primary-key `i64`.
//! - `#[table(dto(skip = "…"))]` removes the field from the DTO
//!   (`ArticleDto::column_names()`).
//! - DTO's `table_name()` delegates to the parent model.
//!
//! Temporal round-trip lives in `pg_dto.rs` / `mysql_dto.rs`.

#![cfg(all(feature = "sqlite-integration-tests", feature = "dto"))]

use reify::{SqliteDb, Table, fetch, insert, raw_execute};

#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "sqlt_dto_articles", dto(skip = "slug"))]
pub struct Article {
    #[column(primary_key)]
    pub id: i64,
    pub title: String,
    pub slug: String,
    pub body: String,
}

async fn setup(db: &SqliteDb) {
    raw_execute(
        db,
        "CREATE TABLE sqlt_dto_articles (
            id    INTEGER PRIMARY KEY,
            title TEXT    NOT NULL,
            slug  TEXT    NOT NULL,
            body  TEXT    NOT NULL
        )",
        &[],
    )
    .await
    .expect("create sqlt_dto_articles");
}

#[tokio::test]
async fn sqlite_dto_round_trip_and_skip() {
    let db = SqliteDb::open_in_memory().expect("open db");
    setup(&db).await;

    let article = Article {
        id: 1,
        title: "Title".into(),
        slug: "title-1".into(),
        body: "Body text".into(),
    };
    insert(&db, &Article::insert(&article))
        .await
        .expect("insert article");

    let rows = fetch::<Article>(&db, &Article::find().filter(Article::id.eq(1i64)))
        .await
        .expect("fetch article");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], article);

    // `dto(skip = "slug")` drops `slug` from the generated DTO.
    assert_eq!(ArticleDto::column_names(), &["id", "title", "body"]);
    assert_eq!(
        <ArticleDto as reify::Table>::table_name(),
        "sqlt_dto_articles"
    );
}
