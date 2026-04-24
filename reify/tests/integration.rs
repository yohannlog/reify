//! Integration-test harness.
//!
//! All actual test files live under `reify/tests/integration/` and are
//! mounted here via `#[path = …] mod name;`. Each file gates itself
//! behind the correct per-adapter feature flag (for example
//! `pg-integration-tests`), so `cargo test --features
//! sqlite-integration-tests` only compiles the SQLite suite.
//!
//! Shared fixtures, env helpers and URL parsing live in `common.rs`
//! so each adapter-specific file stays focused on its scenarios.

#[path = "integration/common.rs"]
mod common;

// Re-export the helpers that test files import via `use crate::{…};`.
#[cfg(feature = "pg-integration-tests")]
#[allow(unused_imports)]
pub use common::{PgFixture, pg_config_from_url, pg_connect, pg_url};

#[cfg(feature = "mysql-integration-tests")]
#[allow(unused_imports)]
pub use common::{MysqlFixture, mysql_connect, mysql_url};

// ── SQLite ───────────────────────────────────────────────────────────

#[cfg(feature = "sqlite-integration-tests")]
#[path = "integration/sqlite_basic.rs"]
mod sqlite_basic;

#[cfg(feature = "sqlite-integration-tests")]
#[path = "integration/sqlite_dto.rs"]
mod sqlite_dto;

#[cfg(feature = "sqlite-integration-tests")]
#[path = "integration/sqlite_migrations.rs"]
mod sqlite_migrations;

#[cfg(feature = "sqlite-integration-tests")]
#[path = "integration/sqlite_upsert.rs"]
mod sqlite_upsert;

#[cfg(feature = "sqlite-integration-tests")]
#[path = "integration/sqlite_insert_many.rs"]
mod sqlite_insert_many;

#[cfg(feature = "sqlite-integration-tests")]
#[path = "integration/sqlite_relations.rs"]
mod sqlite_relations;

#[cfg(feature = "sqlite-integration-tests")]
#[path = "integration/sqlite_pagination.rs"]
mod sqlite_pagination;

#[cfg(feature = "sqlite-integration-tests")]
#[path = "integration/sqlite_enum.rs"]
mod sqlite_enum;

#[cfg(feature = "sqlite-integration-tests")]
#[path = "integration/sqlite_timestamps.rs"]
mod sqlite_timestamps;

#[cfg(feature = "sqlite-integration-tests")]
#[path = "integration/sqlite_sql_injection.rs"]
mod sqlite_sql_injection;

// ── PostgreSQL ───────────────────────────────────────────────────────

#[cfg(feature = "pg-integration-tests")]
#[path = "integration/pg_basic.rs"]
mod pg_basic;

#[cfg(feature = "pg-integration-tests")]
#[path = "integration/pg_migrations.rs"]
mod pg_migrations;

#[cfg(feature = "pg-integration-tests")]
#[path = "integration/pg_dto.rs"]
mod pg_dto;

#[cfg(feature = "pg-integration-tests")]
#[path = "integration/pg_copy.rs"]
mod pg_copy;

#[cfg(feature = "pg-integration-tests")]
#[path = "integration/pg_errors.rs"]
mod pg_errors;

#[cfg(feature = "pg-integration-tests")]
#[path = "integration/pg_arrays.rs"]
mod pg_arrays;

#[cfg(feature = "pg-integration-tests")]
#[path = "integration/pg_sql_injection.rs"]
mod pg_sql_injection;

// ── MySQL ────────────────────────────────────────────────────────────

#[cfg(feature = "mysql-integration-tests")]
#[path = "integration/mysql_basic.rs"]
mod mysql_basic;

#[cfg(feature = "mysql-integration-tests")]
#[path = "integration/mysql_dto.rs"]
mod mysql_dto;

#[cfg(feature = "mysql-integration-tests")]
#[path = "integration/mysql_migrations.rs"]
mod mysql_migrations;

#[cfg(feature = "mysql-integration-tests")]
#[path = "integration/mysql_errors.rs"]
mod mysql_errors;

#[cfg(feature = "mysql-integration-tests")]
#[path = "integration/mysql_sql_injection.rs"]
mod mysql_sql_injection;
