//! Shared fixtures for the migration test suite.
//!
//! `MockDb`, the `Users` / `UsersWithIndex` table impls, and the manual
//! migration fixtures (`AddUserCity`, `IrreversibleMigration`) used to be
//! inlined at the top of `migration/mod.rs`'s `mod tests` block, which
//! made that file 1946 LOC. They are now extracted here so they can be
//! reused by the topic-specific test modules (`mod_tests_diff`,
//! `mod_tests_views`).

#![cfg(test)]

use crate::db::{Database, DbError, Row};
use crate::migration::{Migration, MigrationContext};
use crate::table::Table;
use crate::value::Value;
use std::sync::{Arc, Mutex};

// ── Mock Database ────────────────────────────────────────────────

/// Captures all SQL executed and returns configurable query results.
#[derive(Clone)]
pub(super) struct MockDb {
    executed: Arc<Mutex<Vec<String>>>,
    query_rows: Arc<Mutex<Vec<Vec<Row>>>>,
}

impl MockDb {
    pub(super) fn new() -> Self {
        Self {
            executed: Arc::new(Mutex::new(Vec::new())),
            query_rows: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Pre-load rows to be returned by successive `query()` calls.
    pub(super) fn push_query_result(&self, rows: Vec<Row>) {
        self.query_rows.lock().unwrap().push(rows);
    }

    pub(super) fn executed_sql(&self) -> Vec<String> {
        self.executed.lock().unwrap().clone()
    }
}

impl Database for MockDb {
    async fn execute(&self, sql: &str, _params: &[Value]) -> Result<u64, DbError> {
        self.executed.lock().unwrap().push(sql.to_string());
        Ok(1)
    }

    async fn query(&self, _sql: &str, _params: &[Value]) -> Result<Vec<Row>, DbError> {
        let rows = {
            let mut q = self.query_rows.lock().unwrap();
            if q.is_empty() { vec![] } else { q.remove(0) }
        };
        Ok(rows)
    }

    async fn query_one(&self, _sql: &str, _params: &[Value]) -> Result<Row, DbError> {
        Err(DbError::Query("no rows".into()))
    }

    #[allow(clippy::manual_async_fn)] // matches the trait signature (+ Send)
    fn transaction<'a>(
        &'a self,
        f: crate::db::TransactionFn<'a>,
    ) -> impl std::future::Future<Output = Result<(), DbError>> + Send {
        async move { f(self).await }
    }
}

// ── Minimal Table impl for tests ─────────────────────────────────

pub(super) struct Users;
impl Table for Users {
    fn table_name() -> &'static str {
        "users"
    }
    fn column_names() -> &'static [&'static str] {
        &["id", "email", "role"]
    }
    fn as_values(&self) -> Vec<Value> {
        vec![]
    }
    fn column_defs() -> Vec<crate::schema::ColumnDef> {
        vec![
            crate::schema::ColumnDef {
                name: "id",
                sql_type: crate::schema::SqlType::BigInt,
                primary_key: true,
                auto_increment: false,
                unique: false,
                index: false,
                nullable: false,
                default: None,
                computed: None,
                timestamp_kind: None,
                timestamp_source: crate::schema::TimestampSource::Vm,
                check: None,
                foreign_key: None,
                soft_delete: false,
            },
            crate::schema::ColumnDef {
                name: "email",
                sql_type: crate::schema::SqlType::Text,
                primary_key: false,
                auto_increment: false,
                unique: false,
                index: false,
                nullable: false,
                default: None,
                computed: None,
                timestamp_kind: None,
                timestamp_source: crate::schema::TimestampSource::Vm,
                check: None,
                foreign_key: None,
                soft_delete: false,
            },
            crate::schema::ColumnDef {
                name: "role",
                sql_type: crate::schema::SqlType::Text,
                primary_key: false,
                auto_increment: false,
                unique: false,
                index: false,
                nullable: false,
                default: None,
                computed: None,
                timestamp_kind: None,
                timestamp_source: crate::schema::TimestampSource::Vm,
                check: None,
                foreign_key: None,
                soft_delete: false,
            },
        ]
    }
}

/// Users table with an index on email for testing index creation.
pub(super) struct UsersWithIndex;
impl Table for UsersWithIndex {
    fn table_name() -> &'static str {
        "users"
    }
    fn column_names() -> &'static [&'static str] {
        &["id", "email", "role"]
    }
    fn as_values(&self) -> Vec<Value> {
        vec![]
    }
    fn column_defs() -> Vec<crate::schema::ColumnDef> {
        Users::column_defs()
    }
    fn indexes() -> Vec<crate::schema::IndexDef> {
        vec![crate::schema::IndexDef {
            name: None,
            columns: vec![crate::schema::IndexColumnDef::asc("email")],
            unique: false,
            kind: crate::schema::IndexKind::BTree,
            predicate: None,
        }]
    }
}

// ── Manual migration fixture ─────────────────────────────────────

pub(super) struct AddUserCity;
impl Migration for AddUserCity {
    fn version(&self) -> &'static str {
        "20240320_000001_add_user_city"
    }
    fn description(&self) -> &'static str {
        "Add city column to users"
    }
    fn up(&self, ctx: &mut MigrationContext) {
        ctx.add_column("users", "city", "TEXT NOT NULL DEFAULT ''");
    }
    fn down(&self, ctx: &mut MigrationContext) {
        ctx.drop_column("users", "city");
    }
}

pub(super) struct IrreversibleMigration;
impl Migration for IrreversibleMigration {
    fn version(&self) -> &'static str {
        "20240321_000001_irreversible"
    }
    fn description(&self) -> &'static str {
        "Drop old table"
    }
    fn is_reversible(&self) -> bool {
        false
    }
    fn up(&self, ctx: &mut MigrationContext) {
        ctx.execute("DROP TABLE old_table;");
    }
}
