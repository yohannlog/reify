//! Demonstrates `creation_timestamp` and `update_timestamp` attributes
//! with both VM (Rust-side) and DB (database-side) sources.
//!
//! Run: `cargo run --example timestamps`

use reify::{Schema, Table, query::Order};

// ── VM-source (default): Rust generates Utc::now() ─────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "articles")]
pub struct Article {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub title: String,
    /// Automatically set to `Utc::now()` on INSERT.
    #[column(creation_timestamp)]
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Automatically set to `Utc::now()` on INSERT and every UPDATE.
    #[column(update_timestamp)]
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

// ── DB-source: database provides the value via DEFAULT NOW() ────────

#[derive(Table, Debug, Clone)]
#[table(name = "events")]
pub struct Event {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub name: String,
    /// DB handles this via `DEFAULT NOW()` — excluded from INSERT params.
    #[column(creation_timestamp, source = "db")]
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// DB handles this via `DEFAULT NOW()` + `ON UPDATE CURRENT_TIMESTAMP` (MySQL).
    #[column(update_timestamp, source = "db")]
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

fn main() {
    println!("=== VM-source timestamps (Article) ===\n");

    // INSERT — created_at and updated_at are auto-injected with Utc::now()
    let article = Article {
        id: 0,
        title: "Hello Reify".into(),
        created_at: chrono::DateTime::default(), // ignored — macro injects Utc::now()
        updated_at: chrono::DateTime::default(), // ignored — macro injects Utc::now()
    };
    let (sql, params) = Article::insert(&article).build();
    println!("INSERT:\n  {sql}\n  params: {params:?}\n");

    // UPDATE — updated_at is auto-injected by UpdateBuilder
    let (sql, params) = Article::update()
        .set(Article::title, "Updated Title")
        .filter(Article::id.eq(1i64))
        .build();
    println!("UPDATE (auto-injects updated_at):\n  {sql}\n  params: {params:?}\n");

    // SELECT — normal query, timestamps are just regular columns
    let (sql, params) = Article::find()
        .order_by(Order::Desc("created_at"))
        .limit(10)
        .build();
    println!("SELECT:\n  {sql}\n  params: {params:?}\n");

    println!("\n=== DB-source timestamps (Event) ===\n");

    // INSERT — created_at and updated_at are excluded (DB provides them)
    let event = Event {
        id: 0,
        name: "launch".into(),
        created_at: chrono::DateTime::default(),
        updated_at: chrono::DateTime::default(),
    };
    let (sql, params) = Event::insert(&event).build();
    println!("INSERT (timestamps excluded):\n  {sql}\n  params: {params:?}\n");

    // DDL — shows DEFAULT NOW() for Postgres
    let schema = Event::schema();
    let ddl = reify::create_table_sql(
        Event::table_name(),
        &schema.columns,
        reify::Dialect::Postgres,
    );
    println!("DDL (Postgres):\n{ddl}\n");

    // DDL — shows DEFAULT CURRENT_TIMESTAMP + ON UPDATE for MySQL
    let ddl = reify::create_table_sql(Event::table_name(), &schema.columns, reify::Dialect::Mysql);
    println!("DDL (MySQL):\n{ddl}");
}
