use reify::{Schema, Table, TimestampKind, TimestampSource, Value};

// ── VM-source timestamps (default) ─────────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "articles")]
pub struct Article {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub title: String,
    #[column(creation_timestamp)]
    pub created_at: chrono::DateTime<chrono::Utc>,
    #[column(update_timestamp)]
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

// ── DB-source timestamps ────────────────────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "events")]
pub struct Event {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub name: String,
    #[column(creation_timestamp, source = "db")]
    pub created_at: chrono::DateTime<chrono::Utc>,
    #[column(update_timestamp, source = "db")]
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

// ── ColumnDef metadata (via Schema) ─────────────────────────────────

#[test]
fn vm_timestamp_column_defs() {
    let defs = Article::column_defs();

    let created = defs.iter().find(|d| d.name == "created_at").unwrap();
    assert_eq!(created.timestamp_kind, Some(TimestampKind::Creation));
    assert_eq!(created.timestamp_source, TimestampSource::Vm);

    let updated = defs.iter().find(|d| d.name == "updated_at").unwrap();
    assert_eq!(updated.timestamp_kind, Some(TimestampKind::Update));
    assert_eq!(updated.timestamp_source, TimestampSource::Vm);
}

#[test]
fn db_timestamp_column_defs() {
    let defs = Event::column_defs();

    let created = defs.iter().find(|d| d.name == "created_at").unwrap();
    assert_eq!(created.timestamp_kind, Some(TimestampKind::Creation));
    assert_eq!(created.timestamp_source, TimestampSource::Db);

    let updated = defs.iter().find(|d| d.name == "updated_at").unwrap();
    assert_eq!(updated.timestamp_kind, Some(TimestampKind::Update));
    assert_eq!(updated.timestamp_source, TimestampSource::Db);
}

// ── update_timestamp_columns() ──────────────────────────────────────

#[test]
fn update_timestamp_columns_vm() {
    let cols = Article::update_timestamp_columns();
    assert_eq!(cols, vec!["updated_at"]);
}

#[test]
fn update_timestamp_columns_db_excluded() {
    // DB-source update timestamps should NOT appear (DB handles them)
    let cols = Event::update_timestamp_columns();
    assert!(cols.is_empty());
}

// ── writable_column_names excludes DB-source timestamps ─────────────

#[test]
fn writable_columns_include_vm_timestamps() {
    let cols = Article::writable_column_names();
    assert!(cols.contains(&"created_at"));
    assert!(cols.contains(&"updated_at"));
}

#[test]
fn writable_columns_exclude_db_timestamps() {
    let cols = Event::writable_column_names();
    assert!(!cols.contains(&"created_at"));
    assert!(!cols.contains(&"updated_at"));
    // id is auto_increment → also excluded
    assert_eq!(cols, vec!["name"]);
}

// ── into_values() injects Utc::now() for VM-source timestamps ──────

#[test]
fn vm_timestamp_into_values_produces_timestamptz() {
    let article = Article {
        id: 0,
        title: "Test".into(),
        created_at: chrono::DateTime::default(),
        updated_at: chrono::DateTime::default(),
    };
    let values = article.into_values();
    // created_at and updated_at should be recent Utc::now(), not the default epoch
    assert!(matches!(values[2], Value::Timestamptz(_)));
    assert!(matches!(values[3], Value::Timestamptz(_)));

    // Verify the injected timestamps are recent (within last 5 seconds)
    if let Value::Timestamptz(ts) = &values[2] {
        let diff = chrono::Utc::now() - *ts;
        assert!(diff.num_seconds() < 5, "created_at should be recent");
    }
}

// ── INSERT SQL for DB-source excludes timestamp columns ─────────────

#[test]
fn insert_sql_excludes_db_timestamps() {
    let event = Event {
        id: 0,
        name: "launch".into(),
        created_at: chrono::DateTime::default(),
        updated_at: chrono::DateTime::default(),
    };
    let (sql, params) = Event::insert(&event).build();
    assert!(!sql.contains("created_at"));
    assert!(!sql.contains("updated_at"));
    assert_eq!(params.len(), 1); // only name (id is auto_increment, timestamps are db-source)
}

// ── UPDATE SQL auto-injects VM-source update_timestamp ──────────────

#[cfg(feature = "postgres")]
#[test]
fn update_builder_auto_injects_update_timestamp() {
    let (sql, params) = Article::update()
        .set(Article::title, "New Title")
        .filter(Article::id.eq(1i64))
        .build();

    // Should contain both the explicit set AND the auto-injected updated_at
    assert!(sql.contains("\"title\" = ?"));
    assert!(sql.contains("\"updated_at\" = ?"));
    // params: title value, updated_at timestamp, id filter value
    assert_eq!(params.len(), 3);
    assert!(matches!(params[1], Value::Timestamptz(_)));
}

#[cfg(feature = "postgres")]
#[test]
fn update_builder_skips_if_already_set() {
    let now = chrono::Utc::now();
    let (sql, params) = Article::update()
        .set(Article::title, "New Title")
        .set(Article::updated_at, now)
        .filter(Article::id.eq(1i64))
        .build();

    // updated_at should appear only once (user-provided)
    let count = sql.matches("updated_at").count();
    assert_eq!(count, 1);
    assert_eq!(params.len(), 3); // title, updated_at, id
}

// ── DDL generation ──────────────────────────────────────────────────

#[test]
fn ddl_db_source_postgres_default_now() {
    let schema = Event::schema();
    let sql = reify::create_table_sql(
        Event::table_name(),
        &schema.columns,
        reify::Dialect::Postgres,
    );
    assert!(
        sql.contains("DEFAULT NOW()"),
        "DDL should contain DEFAULT NOW(): {sql}"
    );
}

#[test]
fn ddl_db_source_mysql_current_timestamp() {
    let schema = Event::schema();
    let sql = reify::create_table_sql(Event::table_name(), &schema.columns, reify::Dialect::Mysql);
    assert!(
        sql.contains("DEFAULT CURRENT_TIMESTAMP"),
        "DDL should contain DEFAULT CURRENT_TIMESTAMP: {sql}"
    );
    assert!(
        sql.contains("ON UPDATE CURRENT_TIMESTAMP"),
        "DDL should contain ON UPDATE CURRENT_TIMESTAMP for update_timestamp: {sql}"
    );
}
