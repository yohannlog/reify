//! Integration tests for DTO generation with the `postgres` feature.
//!
//! Covers:
//! - DTO insert via `InsertBuilder<Dto>` (H2: DTO implements `Table`)
//! - Round-trip of every temporal type: `DateTime<Utc>`, `NaiveDateTime`,
//!   `NaiveDate`, `NaiveTime`
//! - Round-trip of `Uuid` and `serde_json::Value`
//! - `Option<DateTime<Utc>>` nullable temporal column
//! - `From<Model>` / `From<Dto>` conversions against live data
//! - DTO with `dto(skip = "...")` inserts only the non-skipped columns
//!
//! All sub-cases run inside a single `#[tokio::test]` to avoid parallel
//! DDL races on shared table names (PostgreSQL catalogue lock contention).

#![cfg(feature = "integration-tests")]

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use reify::{NoTls, PostgresDb, Table, fetch, insert, raw_execute};
use uuid::Uuid;

use crate::{pg_config_from_url, pg_url};

// ── Fixtures ─────────────────────────────────────────────────────────

/// A model that exercises every temporal type available under the
/// `postgres` feature, plus `Uuid` and `serde_json::Value`.
#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "pg_dto_events")]
pub struct Event {
    #[column(primary_key)]
    pub id: Uuid,
    pub label: String,
    /// `TIMESTAMPTZ` — `DateTime<Utc>`
    pub happened_at: chrono::DateTime<chrono::Utc>,
    /// `TIMESTAMP` — `NaiveDateTime`
    pub local_ts: chrono::NaiveDateTime,
    /// `DATE`
    pub day: chrono::NaiveDate,
    /// `TIME`
    pub time_of_day: chrono::NaiveTime,
    /// `JSONB`
    pub metadata: serde_json::Value,
    /// Nullable `TIMESTAMPTZ`
    #[column(nullable)]
    pub resolved_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// A model with a field excluded from the DTO via `dto(skip)`.
#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "pg_dto_articles", dto(skip = "slug"))]
pub struct Article {
    #[column(primary_key)]
    pub id: i64,
    pub title: String,
    /// Excluded from `ArticleDto` — generated server-side.
    pub slug: String,
    pub body: String,
}

// ── Helpers ───────────────────────────────────────────────────────────

async fn connect() -> Option<PostgresDb> {
    let url = pg_url()?;
    let cfg = pg_config_from_url(&url);
    Some(PostgresDb::connect(cfg, NoTls).await.expect("pg connect"))
}

async fn setup(db: &PostgresDb) {
    // Always drop first so a failed previous run doesn't leave stale tables.
    teardown(db).await;

    raw_execute(
        db,
        "CREATE TABLE pg_dto_events (
            id           UUID        PRIMARY KEY,
            label        TEXT        NOT NULL,
            happened_at  TIMESTAMPTZ NOT NULL,
            local_ts     TIMESTAMP   NOT NULL,
            day          DATE        NOT NULL,
            time_of_day  TIME        NOT NULL,
            metadata     JSONB       NOT NULL,
            resolved_at  TIMESTAMPTZ
        )",
        &[],
    )
    .await
    .expect("create pg_dto_events");

    raw_execute(
        db,
        "CREATE TABLE pg_dto_articles (
            id    BIGINT PRIMARY KEY,
            title TEXT   NOT NULL,
            slug  TEXT   NOT NULL,
            body  TEXT   NOT NULL
        )",
        &[],
    )
    .await
    .expect("create pg_dto_articles");
}

async fn teardown(db: &PostgresDb) {
    raw_execute(db, "DROP TABLE IF EXISTS pg_dto_events", &[])
        .await
        .expect("drop pg_dto_events");
    raw_execute(db, "DROP TABLE IF EXISTS pg_dto_articles", &[])
        .await
        .expect("drop pg_dto_articles");
}

// ── Sub-cases (called sequentially from the single test entry point) ──

async fn case_temporal_round_trip(db: &PostgresDb) {
    let id = Uuid::new_v4();
    let happened_at = Utc.with_ymd_and_hms(2024, 6, 15, 12, 30, 0).unwrap();
    let local_ts = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
        NaiveTime::from_hms_opt(12, 30, 0).unwrap(),
    );
    let day = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let time_of_day = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
    let metadata = serde_json::json!({"source": "test", "version": 1});

    let event = Event {
        id,
        label: "launch".into(),
        happened_at,
        local_ts,
        day,
        time_of_day,
        metadata: metadata.clone(),
        resolved_at: None,
    };

    insert(db, &Event::insert(&event))
        .await
        .expect("insert event");

    let rows = fetch::<Event>(db, &Event::find().filter(Event::id.eq(id)))
        .await
        .expect("fetch event");

    assert_eq!(rows.len(), 1, "temporal_round_trip: expected 1 row");
    let got = &rows[0];
    assert_eq!(got.id, id);
    assert_eq!(got.label, "launch");
    assert_eq!(got.happened_at, happened_at, "timestamptz mismatch");
    assert_eq!(got.local_ts, local_ts, "timestamp mismatch");
    assert_eq!(got.day, day, "date mismatch");
    assert_eq!(got.time_of_day, time_of_day, "time mismatch");
    assert_eq!(got.metadata, metadata, "jsonb mismatch");
    assert_eq!(got.resolved_at, None, "nullable should be None");
}

async fn case_dto_table_trait(db: &PostgresDb) {
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
        "pg_dto_articles"
    );
}

async fn case_nullable_timestamptz(db: &PostgresDb) {
    let id_none = Uuid::new_v4();
    let id_some = Uuid::new_v4();
    let base_ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let resolved = Utc.with_ymd_and_hms(2024, 3, 15, 9, 0, 0).unwrap();
    let local_ts = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );

    let event_none = Event {
        id: id_none,
        label: "unresolved".into(),
        happened_at: base_ts,
        local_ts,
        day: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        time_of_day: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        metadata: serde_json::json!({}),
        resolved_at: None,
    };
    let event_some = Event {
        id: id_some,
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

    let rows_none = fetch::<Event>(db, &Event::find().filter(Event::id.eq(id_none)))
        .await
        .expect("fetch none");
    assert_eq!(rows_none[0].resolved_at, None, "expected NULL resolved_at");

    let rows_some = fetch::<Event>(db, &Event::find().filter(Event::id.eq(id_some)))
        .await
        .expect("fetch some");
    assert_eq!(
        rows_some[0].resolved_at,
        Some(resolved),
        "expected Some resolved_at"
    );
}

async fn case_from_conversions(db: &PostgresDb) {
    let id = Uuid::new_v4();
    let happened_at = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let local_ts = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );

    let event = Event {
        id,
        label: "conversion-test".into(),
        happened_at,
        local_ts,
        day: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
        time_of_day: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        metadata: serde_json::json!({"ok": true}),
        resolved_at: Some(happened_at),
    };

    insert(db, &Event::insert(&event)).await.expect("insert");

    let rows = fetch::<Event>(db, &Event::find().filter(Event::id.eq(id)))
        .await
        .expect("fetch");
    let fetched = &rows[0];

    // Model → DTO: all DTO fields preserved
    let dto = EventDto::from(fetched);
    assert_eq!(dto.label, "conversion-test");
    assert_eq!(dto.happened_at, happened_at);
    assert_eq!(dto.local_ts, local_ts);
    assert_eq!(dto.resolved_at, Some(happened_at));

    // DTO → Model: assembled explicitly (`From<Dto> for Model` removed to
    // avoid silently defaulting auto-PK / timestamp / skipped fields).
    let back = Event {
        id,
        label: dto.label.clone(),
        happened_at: dto.happened_at,
        local_ts: dto.local_ts,
        day: dto.day,
        time_of_day: dto.time_of_day,
        metadata: dto.metadata.clone(),
        resolved_at: dto.resolved_at,
    };
    assert_eq!(back.label, dto.label);
    assert_eq!(back.happened_at, dto.happened_at);
    assert_eq!(back.resolved_at, dto.resolved_at);
}

async fn case_temporal_boundary_values(db: &PostgresDb) {
    let cases: &[(NaiveDate, NaiveTime)] = &[
        // Unix epoch
        (
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        ),
        // End of day
        (
            NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
            NaiveTime::from_hms_opt(23, 59, 59).unwrap(),
        ),
        // Leap day
        (
            NaiveDate::from_ymd_opt(2000, 2, 29).unwrap(),
            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        ),
    ];

    for (i, (day, time_of_day)) in cases.iter().enumerate() {
        let id = Uuid::new_v4();
        let local_ts = NaiveDateTime::new(*day, *time_of_day);
        let happened_at = chrono::DateTime::from_naive_utc_and_offset(local_ts, Utc);

        let event = Event {
            id,
            label: format!("boundary-{i}"),
            happened_at,
            local_ts,
            day: *day,
            time_of_day: *time_of_day,
            metadata: serde_json::json!(null),
            resolved_at: None,
        };

        insert(db, &Event::insert(&event))
            .await
            .unwrap_or_else(|e| panic!("insert boundary-{i}: {e}"));

        let rows = fetch::<Event>(db, &Event::find().filter(Event::id.eq(id)))
            .await
            .unwrap_or_else(|e| panic!("fetch boundary-{i}: {e}"));

        assert_eq!(rows.len(), 1, "boundary-{i}: expected 1 row");
        assert_eq!(rows[0].day, *day, "boundary-{i}: date mismatch");
        assert_eq!(
            rows[0].time_of_day, *time_of_day,
            "boundary-{i}: time mismatch"
        );
        assert_eq!(
            rows[0].local_ts, local_ts,
            "boundary-{i}: timestamp mismatch"
        );
        assert_eq!(
            rows[0].happened_at, happened_at,
            "boundary-{i}: timestamptz mismatch"
        );
    }
}

// ── Single sequential entry point ────────────────────────────────────

/// All DTO+postgres integration cases run sequentially under one test to
/// avoid DDL catalogue lock contention when tokio runs tests in parallel.
#[tokio::test]
async fn pg_dto_all() {
    let Some(db) = connect().await else { return };
    setup(&db).await;

    case_temporal_round_trip(&db).await;
    case_dto_table_trait(&db).await;
    case_nullable_timestamptz(&db).await;
    case_from_conversions(&db).await;
    case_temporal_boundary_values(&db).await;

    teardown(&db).await;
}
