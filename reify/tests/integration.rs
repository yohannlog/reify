/// Returns the PostgreSQL connection URL from the environment, or `None` to skip.
pub fn pg_url() -> Option<String> {
    std::env::var("PG_URL").ok()
}

/// Returns the MySQL connection URL from the environment, or `None` to skip.
pub fn mysql_url() -> Option<String> {
    std::env::var("MYSQL_URL").ok()
}

/// Parse a `postgres://user:pass@host:port/dbname` URL into a `deadpool_postgres::Config`.
#[cfg(feature = "integration-tests")]
pub fn pg_config_from_url(url: &str) -> reify::deadpool_postgres::Config {
    use reify::deadpool_postgres::Config as DpConfig;
    let url = url
        .trim_start_matches("postgres://")
        .trim_start_matches("postgresql://");
    let mut cfg = DpConfig::new();
    let (userinfo, rest) = url.split_once('@').unwrap_or(("", url));
    let (host_port, dbname) = rest.split_once('/').unwrap_or((rest, ""));
    if !userinfo.is_empty() {
        if let Some((user, pass)) = userinfo.split_once(':') {
            cfg.user = Some(user.to_string());
            cfg.password = Some(pass.to_string());
        } else {
            cfg.user = Some(userinfo.to_string());
        }
    }
    if let Some((host, port)) = host_port.split_once(':') {
        cfg.host = Some(host.to_string());
        cfg.port = port.parse().ok();
    } else {
        cfg.host = Some(host_port.to_string());
    }
    if !dbname.is_empty() {
        cfg.dbname = Some(dbname.to_string());
    }
    cfg
}

#[path = "integration/sqlite_basic.rs"]
mod sqlite_basic;

#[path = "integration/pg_basic.rs"]
mod pg_basic;

#[path = "integration/pg_migrations.rs"]
mod pg_migrations;

#[path = "integration/pg_dto.rs"]
mod pg_dto;

#[path = "integration/mysql_basic.rs"]
mod mysql_basic;

#[path = "integration/mysql_dto.rs"]
mod mysql_dto;

#[path = "integration/mysql_migrations.rs"]
mod mysql_migrations;
