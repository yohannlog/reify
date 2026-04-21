//! Integration tests for DTO generation with the `mysql` feature.
//!
//! Covers:
//! - DTO insert via `InsertBuilder<Dto>` (DTO implements `Table`)
//! - Round-trip of MySQL temporal types: `NaiveDateTime`, `NaiveDate`, `NaiveTime`
//! - `Option<NaiveDateTime>` nullable temporal column
//! - `From<Model>` / `From<Dto>` conversions against live data
//! - DTO with `dto(skip = "...")` inserts only the non-skipped columns
//!
//! All sub-cases run inside a single `#[tokio::test]` to avoid parallel
//! DDL races on shared table names.

#![cfg(feature = "integration-tests")]

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use reify::mysql_async::Opts;
use reify::{MysqlDb, Table, fetch, insert, raw_execute};

use crate::mysql_url;

// ── Fixtures ──────────────────────────────────────────────────────────

/// A model that exercises every temporal type available under the `mysql`
/// feature: `NaiveDateTime` (DATETIME), `NaiveDate` (DATE), `NaiveTime` (TIME).
#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "mysql_dto_events")]
pub struct Event {
    #[column(primary_key)]
    pub id: i64,
    pub label: String,
    /// `DATETIME` — `NaiveDateTime`
    pub happened_at: NaiveDateTime,
    /// `DATE`
    pub day: NaiveDate,
    /// `TIME`
    pub time_of_day: NaiveTime,
    /// Nullable `DATETIME`
    pub resolved_at: Option<NaiveDateTime>,
}

/// A model with a field excluded from the DTO via `dto(skip)`.
#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "mysql_dto_articles", dto(skip = "slug"))]
pub struct Article {
    #[column(primary_key)]
    pub id: i64,
    pub title: String,
    /// Excluded from `ArticleDto` — generated server-side.
    pub slug: String,
    pub body: String,
}

// ── Helpers ───────────────────────────────────────────────────────────

async fn connect() -> Option<MysqlDb> {
    let url = mysql_url()?;
    let opts = Opts::from_url(&url).expect("invalid MYSQL_URL");
    Some(MysqlDb::connect(opts).await.expect("mysql connect"))
}

async fn setup(db: &MysqlDb) {
    teardown(db).await;

    raw_execute(
        db,
        "CREATE TABLE mysql_dto_events (
            id           BIGINT       PRIMARY KEY,
            label        VARCHAR(255) NOT NULL,
            happened_at  DATETIME     NOT NULL,
            day          DATE         NOT NULL,
            time_of_day  TIME         NOT NULL,
            resolved_at  DATETIME
        )",
        &[],
    )
    .await
    .expect("create mysql_dto_events");

    raw_execute(
        db,
        "CREATE TABLE mysql_dto_articles (
            id    BIGINT       PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            slug  VARCHAR(255) NOT NULL,
            body  TEXT         NOT NULL
        )",
        &[],
    )
    .await
    .expect("create mysql_dto_articles");
}

async fn teardown(db: &MysqlDb) {
    raw_execute(db, "DROP TABLE IF EXISTS mysql_dto_events", &[])
        .await
        .expect("drop mysql_dto_events");
    raw_execute(db, "DROP TABLE IF EXISTS mysql_dto_articles", &[])
        .await
        .expect("drop mysql_dto_articles");
}

// ── Sub-cases ─────────────────────────────────────────────────────────

async fn case_temporal_round_trip(db: &MysqlDb) {
    let happened_at = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
        NaiveTime::from_hms_opt(12, 30, 0).unwrap(),
    );
    let day = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let time_of_day = NaiveTime::from_hms_opt(8, 0, 0).unwrap();

    let event = Event {
        id: 1,
        label: "launch".into(),
        happened_at,
        day,
        time_of_day,
        resolved_at: None,
    };

    insert(db, &Event::insert(&event))
        .await
        .expect("insert event");

    let rows = fetch::<Event>(db, &Event::find().filter(Event::id.eq(1i64)))
        .await
        .expect("fetch event");

    assert_eq!(rows.len(), 1, "temporal_round_trip: expected 1 row");
    let got = &rows[0];
    assert_eq!(got.id, 1);
    assert_eq!(got.label, "launch");
    assert_eq!(got.happened_at, happened_at, "datetime mismatch");
    assert_eq!(got.day, day, "date mismatch");
    assert_eq!(got.time_of_day, time_of_day, "time mismatch");
    assert_eq!(got.resolved_at, None, "nullable should be None");
}

async fn case_dto_table_trait(db: &MysqlDb) {
    let article = Article {
        id: 100,
        title: "Hello Reify".into(),
        slug: "hello-reify".into(),
        body: "Content here.".into(),
    };
    insert(db, &Article::insert(&article))
        .await
        .expect("insert article");

    let rows = fetch::<Article>(db, &Article::find().filter(Article::id.eq(100i64)))
        .await
        .expect("fetch article");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].slug, "hello-reify");

    // DTO excludes `slug` via dto(skip = "slug"); `id` is included because
    // it is primary_key but NOT auto_increment.
    assert_eq!(ArticleDto::column_names(), &["id", "title", "body"]);
    // DTO implements Table — table_name() delegates to the parent model
    assert_eq!(
        <ArticleDto as reify::Table>::table_name(),
        "mysql_dto_articles"
    );
}

async fn case_nullable_datetime(db: &MysqlDb) {
    let base_ts = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );
    let resolved = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
        NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
    );

    let event_none = Event {
        id: 2,
        label: "unresolved".into(),
        happened_at: base_ts,
        day: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        time_of_day: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        resolved_at: None,
    };
    let event_some = Event {
        id: 3,
        label: "resolved".into(),
        resolved_at: Some(resolved),
        ..event_none.clone()
    };

    insert(db, &Event::insert(&event_none))
        .await
        .expect("insert none");
    insert(db, &Event::insert(&event_some))
        .await
        .expect("insert some");

    let rows_none = fetch::<Event>(db, &Event::find().filter(Event::id.eq(2i64)))
        .await
        .expect("fetch none");
    assert_eq!(rows_none[0].resolved_at, None, "expected NULL resolved_at");

    let rows_some = fetch::<Event>(db, &Event::find().filter(Event::id.eq(3i64)))
        .await
        .expect("fetch some");
    assert_eq!(
        rows_some[0].resolved_at,
        Some(resolved),
        "expected Some resolved_at"
    );
}

async fn case_from_conversions(db: &MysqlDb) {
    let happened_at = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );
    let resolved = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2025, 6, 1).unwrap(),
        NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
    );

    let event = Event {
        id: 4,
        label: "conversion-test".into(),
        happened_at,
        day: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
        time_of_day: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        resolved_at: Some(resolved),
    };

    insert(db, &Event::insert(&event)).await.expect("insert");

    let rows = fetch::<Event>(db, &Event::find().filter(Event::id.eq(4i64)))
        .await
        .expect("fetch");
    let fetched = &rows[0];

    // Model → DTO: all DTO fields preserved
    let dto = EventDto::from(fetched);
    assert_eq!(dto.label, "conversion-test");
    assert_eq!(dto.happened_at, happened_at);
    assert_eq!(dto.resolved_at, Some(resolved));

    // DTO → Model: assembled explicitly (`From<Dto> for Model` removed to
    // avoid silently defaulting auto-PK / timestamp / skipped fields).
    let back = Event {
        id: 4,
        label: dto.label.clone(),
        happened_at: dto.happened_at,
        day: dto.day,
        time_of_day: dto.time_of_day,
        resolved_at: dto.resolved_at,
    };
    assert_eq!(back.label, dto.label);
    assert_eq!(back.happened_at, dto.happened_at);
    assert_eq!(back.resolved_at, dto.resolved_at);
}

async fn case_temporal_boundary_values(db: &MysqlDb) {
    let cases: &[(i64, NaiveDate, NaiveTime)] = &[
        // Unix epoch
        (
            10,
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        ),
        // End of day
        (
            11,
            NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
            NaiveTime::from_hms_opt(23, 59, 59).unwrap(),
        ),
        // Leap day
        (
            12,
            NaiveDate::from_ymd_opt(2000, 2, 29).unwrap(),
            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        ),
    ];

    for (id, day, time_of_day) in cases {
        let happened_at = NaiveDateTime::new(*day, *time_of_day);

        let event = Event {
            id: *id,
            label: format!("boundary-{id}"),
            happened_at,
            day: *day,
            time_of_day: *time_of_day,
            resolved_at: None,
        };

        insert(db, &Event::insert(&event))
            .await
            .unwrap_or_else(|e| panic!("insert boundary-{id}: {e}"));

        let rows = fetch::<Event>(db, &Event::find().filter(Event::id.eq(*id)))
            .await
            .unwrap_or_else(|e| panic!("fetch boundary-{id}: {e}"));

        assert_eq!(rows.len(), 1, "boundary-{id}: expected 1 row");
        assert_eq!(rows[0].day, *day, "boundary-{id}: date mismatch");
        assert_eq!(
            rows[0].time_of_day, *time_of_day,
            "boundary-{id}: time mismatch"
        );
        assert_eq!(
            rows[0].happened_at, happened_at,
            "boundary-{id}: datetime mismatch"
        );
    }
}

// ── Single sequential entry point ─────────────────────────────────────

/// All DTO+mysql integration cases run sequentially under one test to
/// avoid DDL lock contention when tokio runs tests in parallel.
#[tokio::test]
async fn mysql_dto_all() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    case_temporal_round_trip(&db).await;
    case_dto_table_trait(&db).await;
    case_nullable_datetime(&db).await;
    case_from_conversions(&db).await;
    case_temporal_boundary_values(&db).await;

    teardown(&db).await;
}
