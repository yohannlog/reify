/// Returns the PostgreSQL connection URL from the environment, or `None` to skip.
pub fn pg_url() -> Option<String> {
    std::env::var("PG_URL").ok()
}

/// Returns the MySQL connection URL from the environment, or `None` to skip.
pub fn mysql_url() -> Option<String> {
    std::env::var("MYSQL_URL").ok()
}

#[path = "integration/sqlite_basic.rs"]
mod sqlite_basic;

#[path = "integration/pg_basic.rs"]
mod pg_basic;

#[path = "integration/pg_migrations.rs"]
mod pg_migrations;

#[path = "integration/mysql_basic.rs"]
mod mysql_basic;
