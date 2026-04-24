//! Shared helpers for integration tests.
//!
//! - Factorises connection boilerplate for PostgreSQL / MySQL so each
//!   test file does not duplicate `connect()` / `setup()` / `teardown()`.
//! - Provides `PgFixture` / `MysqlFixture` helpers that **drop**
//!   the requested tables up-front, so a previous panicked run
//!   cannot leave stale state behind. Teardown is still an explicit
//!   `.teardown().await` call — doing it in `Drop` would require
//!   blocking on an async runtime from a `Drop` impl, which is not
//!   reliably available to us (`PostgresDb` / `MysqlDb` are not `Clone`).
//!   The up-front drop is the robustness property that matters: a
//!   panicking test still leaves the next run clean.
//! - Surfaces skipped tests via `eprintln!` so a missing
//!   `PG_URL` / `MYSQL_URL` is visible rather than silently green.
//!
//! NOTE: this module is compiled once per integration test binary
//!       (it is included via `#[path = "integration/common.rs"]`
//!       in `tests/integration.rs`). Items only used by a subset of
//!       the test files would emit dead-code warnings, hence
//!       `#[allow(dead_code)]`.

#![cfg(any(
    feature = "pg-integration-tests",
    feature = "mysql-integration-tests",
    feature = "sqlite-integration-tests",
))]
#![allow(dead_code)]

// ── Env helpers ───────────────────────────────────────────────────────

/// Returns the PostgreSQL connection URL from the environment, or
/// `None` (with a visible `SKIP:` log) to skip.
pub fn pg_url() -> Option<String> {
    match std::env::var("PG_URL") {
        Ok(v) => Some(v),
        Err(_) => {
            eprintln!("SKIP: PG_URL not set — skipping PostgreSQL integration test");
            None
        }
    }
}

/// Returns the MySQL connection URL from the environment, or `None`
/// (with a visible `SKIP:` log) to skip.
pub fn mysql_url() -> Option<String> {
    match std::env::var("MYSQL_URL") {
        Ok(v) => Some(v),
        Err(_) => {
            eprintln!("SKIP: MYSQL_URL not set — skipping MySQL integration test");
            None
        }
    }
}

// ── PostgreSQL helpers ────────────────────────────────────────────────

/// Parse a `postgres://user:pass@host:port/dbname` URL into a
/// `deadpool_postgres::Config`.
///
/// Uses `tokio_postgres::Config::from_str` for correctness (handles
/// URL-encoded passwords, `?sslmode=…`, etc.) and then translates the
/// relevant fields into deadpool's config.
#[cfg(feature = "pg-integration-tests")]
pub fn pg_config_from_url(url: &str) -> reify::deadpool_postgres::Config {
    use reify::deadpool_postgres::Config as DpConfig;
    use std::str::FromStr;

    let parsed = reify::tokio_postgres::Config::from_str(url)
        .unwrap_or_else(|e| panic!("invalid PG_URL {url:?}: {e}"));

    let mut cfg = DpConfig::new();
    if let Some(user) = parsed.get_user() {
        cfg.user = Some(user.to_string());
    }
    if let Some(pw) = parsed.get_password() {
        cfg.password = Some(String::from_utf8_lossy(pw).into_owned());
    }
    if let Some(dbname) = parsed.get_dbname() {
        cfg.dbname = Some(dbname.to_string());
    }
    // tokio-postgres can hold multiple hosts; use the first TCP one.
    if let Some(host) = parsed.get_hosts().iter().find_map(|h| match h {
        reify::tokio_postgres::config::Host::Tcp(h) => Some(h.clone()),
        _ => None,
    }) {
        cfg.host = Some(host);
    }
    if let Some(port) = parsed.get_ports().first().copied() {
        cfg.port = Some(port);
    }
    cfg
}

/// Connect to PostgreSQL, skipping (visibly) if `PG_URL` is not set.
#[cfg(feature = "pg-integration-tests")]
pub async fn pg_connect() -> Option<reify::PostgresDb> {
    let url = pg_url()?;
    let cfg = pg_config_from_url(&url);
    Some(
        reify::PostgresDb::connect(cfg, reify::NoTls)
            .await
            .expect("pg connect"),
    )
}

/// Fixture for a PostgreSQL test.
///
/// On construction, drops every `table` listed so that a previous
/// panicked run cannot leak stale tables. Callers should then run
/// their own `CREATE TABLE` and call `.teardown().await` at the end.
#[cfg(feature = "pg-integration-tests")]
pub struct PgFixture {
    pub db: reify::PostgresDb,
    tables: Vec<&'static str>,
}

#[cfg(feature = "pg-integration-tests")]
impl PgFixture {
    /// Connect, then DROP each of the given tables.
    ///
    /// Returns `None` (with a visible `SKIP:` log) if `PG_URL` is unset.
    pub async fn new(tables: &[&'static str]) -> Option<Self> {
        let db = pg_connect().await?;
        drop_pg_tables(&db, tables).await;
        Some(Self {
            db,
            tables: tables.to_vec(),
        })
    }

    /// Explicit teardown — drops every table registered on construction.
    pub async fn teardown(&self) {
        drop_pg_tables(&self.db, &self.tables).await;
    }
}

#[cfg(feature = "pg-integration-tests")]
async fn drop_pg_tables(db: &reify::PostgresDb, tables: &[&'static str]) {
    for t in tables {
        let _ = reify::raw_execute(db, &format!("DROP TABLE IF EXISTS {t} CASCADE"), &[]).await;
    }
}

// ── MySQL helpers ─────────────────────────────────────────────────────

/// Connect to MySQL, skipping (visibly) if `MYSQL_URL` is not set.
#[cfg(feature = "mysql-integration-tests")]
pub async fn mysql_connect() -> Option<reify::MysqlDb> {
    use reify::mysql_async::Opts;
    let url = mysql_url()?;
    let opts = Opts::from_url(&url).expect("invalid MYSQL_URL");
    Some(reify::MysqlDb::connect(opts).await.expect("mysql connect"))
}

/// Fixture for a MySQL test — same semantics as [`PgFixture`].
#[cfg(feature = "mysql-integration-tests")]
pub struct MysqlFixture {
    pub db: reify::MysqlDb,
    tables: Vec<&'static str>,
}

#[cfg(feature = "mysql-integration-tests")]
impl MysqlFixture {
    pub async fn new(tables: &[&'static str]) -> Option<Self> {
        let db = mysql_connect().await?;
        drop_mysql_tables(&db, tables).await;
        Some(Self {
            db,
            tables: tables.to_vec(),
        })
    }

    pub async fn teardown(&self) {
        drop_mysql_tables(&self.db, &self.tables).await;
    }
}

#[cfg(feature = "mysql-integration-tests")]
async fn drop_mysql_tables(db: &reify::MysqlDb, tables: &[&'static str]) {
    for t in tables {
        let _ = reify::raw_execute(db, &format!("DROP TABLE IF EXISTS `{t}`"), &[]).await;
    }
}
