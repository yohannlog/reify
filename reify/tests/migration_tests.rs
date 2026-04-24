//! Integration tests for the migration system.
//!
//! Uses a MockDb that captures executed SQL and returns configurable query results.

use std::sync::{Arc, Mutex};

use reify::{
    Database, DbError, Row, TransactionFn, Value,
    migration::{
        Migration, MigrationContext, MigrationError, MigrationPlan, MigrationRunner,
        compute_checksum, generate_migration_file,
    },
};

// ── MockDb ───────────────────────────────────────────────────────────

#[derive(Clone)]
struct MockDb {
    executed: Arc<Mutex<Vec<(String, Vec<Value>)>>>,
    query_results: Arc<Mutex<Vec<Vec<Row>>>>,
    /// Pre-loaded return values for `execute()`. When non-empty, the front
    /// value is popped and returned; otherwise defaults to `Ok(1)`.
    execute_results: Arc<Mutex<Vec<u64>>>,
}

impl MockDb {
    fn new() -> Self {
        Self {
            executed: Arc::new(Mutex::new(Vec::new())),
            query_results: Arc::new(Mutex::new(Vec::new())),
            execute_results: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn push_rows(&self, rows: Vec<Row>) {
        self.query_results.lock().unwrap().push(rows);
    }

    /// Pre-load a rows_affected value to be returned by the next `execute()` call.
    fn push_execute_result(&self, n: u64) {
        self.execute_results.lock().unwrap().push(n);
    }

    fn executed_sql(&self) -> Vec<String> {
        self.executed
            .lock()
            .unwrap()
            .iter()
            .map(|(s, _)| s.clone())
            .collect()
    }
}

impl Database for MockDb {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        self.executed
            .lock()
            .unwrap()
            .push((sql.to_string(), params.to_vec()));
        let rows_affected = {
            let mut q = self.execute_results.lock().unwrap();
            if q.is_empty() { 1 } else { q.remove(0) }
        };
        Ok(rows_affected)
    }

    async fn query(&self, _sql: &str, _params: &[Value]) -> Result<Vec<Row>, DbError> {
        let rows = {
            let mut q = self.query_results.lock().unwrap();
            if q.is_empty() { vec![] } else { q.remove(0) }
        };
        Ok(rows)
    }

    async fn query_one(&self, _sql: &str, _params: &[Value]) -> Result<Row, DbError> {
        Err(DbError::Query("no rows".into()))
    }

    fn transaction<'a>(
        &'a self,
        f: TransactionFn<'a>,
    ) -> impl std::future::Future<Output = Result<(), DbError>> + Send {
        async move { f(self).await }
    }
}

// ── Table fixtures ───────────────────────────────────────────────────

#[derive(reify::Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
    pub role: Option<String>,
}

#[derive(reify::Table, Debug, Clone)]
#[table(name = "posts")]
pub struct Post {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub user_id: i64,
    pub title: String,
}

// ── Migration fixtures ───────────────────────────────────────────────

struct AddUserCity;
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

struct AddPostSlug;
impl Migration for AddPostSlug {
    fn version(&self) -> &'static str {
        "20240321_000001_add_post_slug"
    }
    fn description(&self) -> &'static str {
        "Add slug column to posts"
    }
    fn up(&self, ctx: &mut MigrationContext) {
        ctx.add_column("posts", "slug", "TEXT NOT NULL DEFAULT ''");
        ctx.execute("UPDATE posts SET slug = title;");
    }
    fn down(&self, ctx: &mut MigrationContext) {
        ctx.drop_column("posts", "slug");
    }
}

struct IrreversibleDrop;
impl Migration for IrreversibleDrop {
    fn version(&self) -> &'static str {
        "20240322_000001_drop_old"
    }
    fn description(&self) -> &'static str {
        "Drop legacy table"
    }
    fn is_reversible(&self) -> bool {
        false
    }
    fn up(&self, ctx: &mut MigrationContext) {
        ctx.execute("DROP TABLE IF EXISTS legacy;");
    }
}

// ── Tests ────────────────────────────────────────────────────────────

/// Build a Row with version + checksum columns (for applied_checksums mock).
fn applied_row_with_checksum(version: &str, checksum: &str) -> Row {
    Row::new(
        vec!["version".into(), "checksum".into()],
        vec![
            Value::String(version.into()),
            Value::String(checksum.into()),
        ],
    )
}

#[tokio::test]
async fn runner_creates_tracking_table_on_first_run() {
    let db = MockDb::new();
    db.push_rows(vec![]); // applied_checksums → empty
    db.push_rows(vec![]); // existing_columns users → absent
    db.push_rows(vec![]); // existing_columns posts → absent

    MigrationRunner::new()
        .add_table::<User>()
        .add_table::<Post>()
        .run(&db)
        .await
        .unwrap();

    let sql = db.executed_sql();
    assert!(
        sql.iter().any(|s| s.contains("_reify_migrations")),
        "tracking table not created: {sql:?}"
    );
}

#[tokio::test]
async fn runner_emits_create_table_for_new_tables() {
    let db = MockDb::new();
    db.push_rows(vec![]); // applied_checksums → empty
    db.push_rows(vec![]); // users columns → absent

    MigrationRunner::new()
        .add_table::<User>()
        .run(&db)
        .await
        .unwrap();

    let sql = db.executed_sql();
    assert!(
        sql.iter()
            .any(|s| s.contains("CREATE TABLE IF NOT EXISTS \"users\"")),
        "CREATE TABLE not found: {sql:?}"
    );
}

#[tokio::test]
async fn runner_emits_add_column_for_new_fields() {
    let db = MockDb::new();
    db.push_rows(vec![]); // applied_checksums → empty
    // existing_column_details: users table exists but missing "role"
    db.push_rows(vec![
        Row::new(
            vec!["column_name".into(), "data_type".into(), "is_nullable".into(), "column_default".into()],
            vec![Value::String("id".into()), Value::String("bigint".into()), Value::String("NO".into()), Value::Null],
        ),
        Row::new(
            vec!["column_name".into(), "data_type".into(), "is_nullable".into(), "column_default".into()],
            vec![Value::String("email".into()), Value::String("text".into()), Value::String("NO".into()), Value::Null],
        ),
    ]);
    db.push_rows(vec![]); // unique constraints query
    db.push_rows(vec![]); // existing_indexes query

    MigrationRunner::new()
        .add_table::<User>()
        .run(&db)
        .await
        .unwrap();

    let sql = db.executed_sql();
    assert!(
        sql.iter().any(|s| s.contains("ADD COLUMN \"role\"")),
        "ADD COLUMN \"role\" not found: {sql:?}"
    );
}

#[tokio::test]
async fn runner_skips_table_when_all_columns_present() {
    let db = MockDb::new();
    db.push_rows(vec![]); // applied_checksums → empty
    // existing_column_details: all columns present
    db.push_rows(vec![
        Row::new(
            vec!["column_name".into(), "data_type".into(), "is_nullable".into(), "column_default".into()],
            vec![Value::String("id".into()), Value::String("bigint".into()), Value::String("NO".into()), Value::Null],
        ),
        Row::new(
            vec!["column_name".into(), "data_type".into(), "is_nullable".into(), "column_default".into()],
            vec![Value::String("email".into()), Value::String("text".into()), Value::String("NO".into()), Value::Null],
        ),
        Row::new(
            vec!["column_name".into(), "data_type".into(), "is_nullable".into(), "column_default".into()],
            vec![Value::String("role".into()), Value::String("text".into()), Value::String("NO".into()), Value::Null],
        ),
    ]);
    db.push_rows(vec![]); // unique constraints query
    db.push_rows(vec![]); // existing_indexes query

    MigrationRunner::new()
        .add_table::<User>()
        .run(&db)
        .await
        .unwrap();

    let sql = db.executed_sql();
    // Only the tracking table CREATE should be executed, no DDL for users
    assert!(
        !sql.iter()
            .any(|s| s.contains("CREATE TABLE IF NOT EXISTS \"users\"")),
        "unexpected CREATE TABLE: {sql:?}"
    );
    assert!(
        !sql.iter().any(|s| s.contains("ADD COLUMN")),
        "unexpected ADD COLUMN: {sql:?}"
    );
}

#[tokio::test]
async fn dry_run_returns_plans_without_executing_ddl() {
    let db = MockDb::new();
    db.push_rows(vec![]); // applied versions (dry_run reads version only)
    db.push_rows(vec![]); // users columns → absent

    let plans = MigrationRunner::new()
        .add_table::<User>()
        .add(AddUserCity)
        .dry_run(&db)
        .await
        .unwrap();

    // Plans returned
    assert!(!plans.is_empty());
    // No DDL executed (only tracking table CREATE + applied_versions query)
    let sql = db.executed_sql();
    assert!(
        !sql.iter()
            .any(|s| s.contains("CREATE TABLE IF NOT EXISTS \"users\"")),
        "dry_run must not execute DDL: {sql:?}"
    );
}

#[tokio::test]
async fn dry_run_includes_manual_migration_statements() {
    let db = MockDb::new();
    db.push_rows(vec![]); // applied versions

    let plans = MigrationRunner::new()
        .add(AddUserCity)
        .dry_run(&db)
        .await
        .unwrap();

    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].version, "20240320_000001_add_user_city");
    assert!(plans[0].statements[0].contains("ADD COLUMN \"city\""));
}

#[tokio::test]
async fn dry_run_skips_already_applied_migrations() {
    let db = MockDb::new();
    // Both migrations already applied (dry_run queries version only)
    db.push_rows(vec![
        Row::new(
            vec!["version".into()],
            vec![Value::String("20240320_000001_add_user_city".into())],
        ),
        Row::new(
            vec!["version".into()],
            vec![Value::String("20240321_000001_add_post_slug".into())],
        ),
    ]);

    let plans = MigrationRunner::new()
        .add(AddUserCity)
        .add(AddPostSlug)
        .dry_run(&db)
        .await
        .unwrap();

    assert!(
        plans.is_empty(),
        "expected no pending plans, got: {plans:?}"
    );
}

#[tokio::test]
async fn manual_migration_up_executes_all_statements() {
    let db = MockDb::new();
    db.push_rows(vec![]); // applied_checksums → empty

    MigrationRunner::new()
        .add(AddPostSlug)
        .run(&db)
        .await
        .unwrap();

    let sql = db.executed_sql();
    assert!(sql.iter().any(|s| s.contains("ADD COLUMN \"slug\"")));
    assert!(sql.iter().any(|s| s.contains("UPDATE posts SET slug")));
}

#[tokio::test]
async fn rollback_executes_down_and_removes_tracking_entry() {
    let db = MockDb::new();
    // Last applied migration
    db.push_rows(vec![Row::new(
        vec!["version".into()],
        vec![Value::String("20240320_000001_add_user_city".into())],
    )]);

    MigrationRunner::new()
        .add(AddUserCity)
        .rollback(&db)
        .await
        .unwrap();

    let sql = db.executed_sql();
    assert!(
        sql.iter().any(|s| s.contains("DROP COLUMN \"city\"")),
        "expected DROP COLUMN \"city\": {sql:?}"
    );
    assert!(
        sql.iter()
            .any(|s| s.contains("DELETE FROM \"_reify_migrations\"")),
        "expected DELETE FROM tracking: {sql:?}"
    );
}

#[tokio::test]
async fn rollback_irreversible_returns_error() {
    let db = MockDb::new();
    db.push_rows(vec![Row::new(
        vec!["version".into()],
        vec![Value::String("20240322_000001_drop_old".into())],
    )]);

    let result = MigrationRunner::new()
        .add(IrreversibleDrop)
        .rollback(&db)
        .await;

    assert!(
        matches!(result, Err(MigrationError::NotReversible(_))),
        "expected NotReversible error"
    );
}

#[tokio::test]
async fn status_lists_applied_and_pending() {
    let db = MockDb::new();
    // AddUserCity is applied, AddPostSlug is pending
    db.push_rows(vec![Row::new(
        vec!["version".into(), "applied_at".into()],
        vec![
            Value::String("20240320_000001_add_user_city".into()),
            Value::String("2024-03-20T00:00:00Z".into()),
        ],
    )]);

    let statuses = MigrationRunner::new()
        .add(AddUserCity)
        .add(AddPostSlug)
        .status(&db)
        .await
        .unwrap();

    assert_eq!(statuses.len(), 2);
    let city = statuses
        .iter()
        .find(|s| s.version.contains("add_user_city"))
        .unwrap();
    let slug = statuses
        .iter()
        .find(|s| s.version.contains("add_post_slug"))
        .unwrap();
    assert!(city.applied);
    assert!(!slug.applied);
}

#[test]
fn generate_migration_file_produces_correct_struct_name() {
    let content = generate_migration_file("add_user_city", "20240320_000001_add_user_city");
    assert!(content.contains("struct AddUserCity"));
    assert!(content.contains("impl Migration for AddUserCity"));
    assert!(content.contains("20240320_000001_add_user_city"));
}

#[test]
fn migration_plan_display_shows_version_and_sql() {
    let stmts = vec!["ALTER TABLE users ADD COLUMN city TEXT NOT NULL;".to_string()];
    let checksum = compute_checksum(&stmts);
    let plan = MigrationPlan {
        version: "20240320_000001_add_user_city".into(),
        description: "Add city column to users".into(),
        comment: None,
        statements: stmts,
        checksum,
        schema_diff: None,
        timeout: None,
    };
    let d = plan.display();
    assert!(d.contains("Would apply (up)"));
    assert!(d.contains("20240320_000001_add_user_city"));
    assert!(d.contains("ALTER TABLE users"));
}

// ── Parameterized type normalization ────────────────────────────────

#[test]
fn normalize_sql_type_parameterized() {
    use reify::migration::normalize_sql_type;

    // VARCHAR preserves length
    assert_eq!(normalize_sql_type("VARCHAR(255)"), "varchar(255)");
    assert_eq!(normalize_sql_type("varchar(100)"), "varchar(100)");
    assert_eq!(normalize_sql_type("CHARACTER VARYING(255)"), "varchar(255)");

    // CHAR preserves length
    assert_eq!(normalize_sql_type("CHAR(36)"), "char(36)");
    assert_eq!(normalize_sql_type("CHARACTER(3)"), "char(3)");

    // DECIMAL/NUMERIC both normalize to numeric, preserving precision
    assert_eq!(normalize_sql_type("DECIMAL(10,2)"), "numeric(10,2)");
    assert_eq!(normalize_sql_type("NUMERIC(10,2)"), "numeric(10,2)");
    assert_eq!(normalize_sql_type("numeric(8, 4)"), "numeric(8, 4)");

    // Without params, still works
    assert_eq!(normalize_sql_type("VARCHAR"), "varchar");
    assert_eq!(normalize_sql_type("NUMERIC"), "numeric");
    assert_eq!(normalize_sql_type("DECIMAL"), "numeric");
}

// ── Metadata-based DDL tests ────────────────────────────────────────

#[test]
fn create_table_uses_metadata_types_not_heuristics() {
    use reify::Dialect;
    use reify::migration::create_table_sql;
    use reify::schema::{ColumnDef, SqlType};

    // Build column defs with explicit types matching User's columns
    let defs = vec![
        ColumnDef {
            name: "id",
            sql_type: SqlType::BigSerial,
            primary_key: true,
            auto_increment: true,
            unique: false,
            index: false,
            nullable: false,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: reify::TimestampSource::Vm,
            check: None,
            foreign_key: None,
            soft_delete: false,
        },
        ColumnDef {
            name: "email",
            sql_type: SqlType::Uuid,
            primary_key: false,
            auto_increment: false,
            unique: true,
            index: false,
            nullable: false,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: reify::TimestampSource::Vm,
            check: None,
            foreign_key: None,
            soft_delete: false,
        },
        ColumnDef {
            name: "role",
            sql_type: SqlType::Text,
            primary_key: false,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: true,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: reify::TimestampSource::Vm,
            check: None,
            foreign_key: None,
            soft_delete: false,
        },
    ];

    let sql = create_table_sql::<User>(&defs, Dialect::Postgres);
    assert!(sql.contains("BIGSERIAL"), "expected BIGSERIAL, got: {sql}");
    // email uses Uuid type → should render as UUID for Postgres, not TEXT
    assert!(
        sql.contains("UUID"),
        "expected UUID (from metadata, not name heuristic), got: {sql}"
    );
    assert!(sql.contains("TEXT"), "expected TEXT for role, got: {sql}");
    assert!(
        sql.contains("PRIMARY KEY"),
        "expected PRIMARY KEY, got: {sql}"
    );
    assert!(sql.contains("UNIQUE"), "expected UNIQUE, got: {sql}");
}

#[test]
fn column_defs_from_derive_macro_have_correct_types() {
    use reify::Table;
    use reify::schema::SqlType;

    let defs = User::column_defs();
    assert_eq!(defs.len(), 3);

    // id: i64 + primary_key + auto_increment → BigSerial
    let id = &defs[0];
    assert_eq!(id.sql_type, SqlType::BigSerial);
    assert!(id.primary_key);
    assert!(id.auto_increment);

    // email: String → Text + unique
    let email = &defs[1];
    assert_eq!(email.sql_type, SqlType::Text);
    assert!(email.unique);

    // role: Option<String> → Text + nullable
    let role = &defs[2];
    assert_eq!(role.sql_type, SqlType::Text);
    assert!(role.nullable);
}

#[tokio::test]
async fn auto_migration_uses_metadata_types_in_create_table() {
    let db = MockDb::new();
    db.push_rows(vec![]); // applied_checksums → empty
    db.push_rows(vec![]); // existing_columns users → absent
    db.push_rows(vec![]); // existing_columns posts → absent

    MigrationRunner::new()
        .add_table::<User>()
        .add_table::<Post>()
        .run(&db)
        .await
        .unwrap();

    let sql = db.executed_sql();
    let create_users = sql
        .iter()
        .find(|s| s.contains("CREATE TABLE IF NOT EXISTS \"users\""));
    assert!(
        create_users.is_some(),
        "expected CREATE TABLE users: {sql:?}"
    );

    let create_sql = create_users.unwrap();
    // Should use metadata types, not name-based heuristics
    // id has BigSerial → renders as "INTEGER" in Generic dialect
    assert!(
        create_sql.contains("PRIMARY KEY"),
        "expected PRIMARY KEY in: {create_sql}"
    );
}

// ── CHECK constraint DDL rendering ──────────────────────────────────

#[test]
fn create_table_with_column_check() {
    use reify::Dialect;
    use reify::migration::create_table_sql;
    use reify::schema::{ColumnDef, SqlType};

    // Use column names matching User::column_names() = ["id", "email", "role"]
    let defs = vec![
        ColumnDef {
            name: "id",
            sql_type: SqlType::BigSerial,
            primary_key: true,
            auto_increment: true,
            unique: false,
            index: false,
            nullable: false,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: reify::TimestampSource::Vm,
            check: None,
            foreign_key: None,
            soft_delete: false,
        },
        ColumnDef {
            name: "email",
            sql_type: SqlType::Text,
            primary_key: false,
            auto_increment: false,
            unique: true,
            index: false,
            nullable: false,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: reify::TimestampSource::Vm,
            check: Some("length(email) > 0".to_string()),
            foreign_key: None,
            soft_delete: false,
        },
        ColumnDef {
            name: "role",
            sql_type: SqlType::Text,
            primary_key: false,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: true,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: reify::TimestampSource::Vm,
            check: None,
            foreign_key: None,
            soft_delete: false,
        },
    ];

    let sql = create_table_sql::<User>(&defs, Dialect::Postgres);
    assert!(
        sql.contains("CHECK (length(email) > 0)"),
        "expected CHECK (length(email) > 0) in: {sql}"
    );
    // CHECK should be on the email line
    let email_line = sql.lines().find(|l| l.contains("email")).unwrap();
    assert!(
        email_line.contains("CHECK (length(email) > 0)"),
        "CHECK should be inline with email column: {email_line}"
    );
    // role should NOT have a CHECK
    let role_line = sql.lines().find(|l| l.contains("role")).unwrap();
    assert!(
        !role_line.contains("CHECK"),
        "role should not have CHECK: {role_line}"
    );
}

#[test]
fn create_table_with_table_level_checks() {
    use reify::Dialect;
    use reify::migration::create_table_sql_with_checks;
    use reify::schema::{ColumnDef, SqlType};

    // Use column names matching User::column_names() = ["id", "email", "role"]
    let defs = vec![
        ColumnDef {
            name: "id",
            sql_type: SqlType::BigSerial,
            primary_key: true,
            auto_increment: true,
            unique: false,
            index: false,
            nullable: false,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: reify::TimestampSource::Vm,
            check: None,
            foreign_key: None,
            soft_delete: false,
        },
        ColumnDef {
            name: "email",
            sql_type: SqlType::Text,
            primary_key: false,
            auto_increment: false,
            unique: true,
            index: false,
            nullable: false,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: reify::TimestampSource::Vm,
            check: None,
            foreign_key: None,
            soft_delete: false,
        },
        ColumnDef {
            name: "role",
            sql_type: SqlType::Text,
            primary_key: false,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: true,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: reify::TimestampSource::Vm,
            check: None,
            foreign_key: None,
            soft_delete: false,
        },
    ];

    let checks = vec!["length(email) > 5".to_string()];
    let sql = create_table_sql_with_checks::<User>(&defs, &checks, Dialect::Postgres);
    assert!(
        sql.contains("CHECK (length(email) > 5)"),
        "expected table-level CHECK in: {sql}"
    );
    // Table-level CHECK should be a separate line, not on a column line
    let check_line = sql
        .lines()
        .find(|l| l.contains("length(email) > 5"))
        .unwrap();
    assert!(
        !check_line.contains("TEXT"),
        "table-level CHECK should be on its own line: {check_line}"
    );
}

#[test]
fn create_table_with_no_checks_matches_base() {
    use reify::Dialect;
    use reify::migration::{create_table_sql, create_table_sql_with_checks};
    use reify::schema::{ColumnDef, SqlType};

    let defs = vec![ColumnDef {
        name: "id",
        sql_type: SqlType::BigSerial,
        primary_key: true,
        auto_increment: true,
        unique: false,
        index: false,
        nullable: false,
        default: None,
        computed: None,
        timestamp_kind: None,
        timestamp_source: reify::TimestampSource::Vm,
        check: None,
        foreign_key: None,
        soft_delete: false,
    }];

    let base = create_table_sql::<User>(&defs, Dialect::Generic);
    let with_empty = create_table_sql_with_checks::<User>(&defs, &[], Dialect::Generic);
    assert_eq!(base, with_empty);
}

// ── Checksum + Lock tests ────────────────────────────────────────────

#[tokio::test]
async fn checksum_mismatch_returns_error() {
    let db = MockDb::new();
    // applied_checksums returns AddUserCity with a wrong checksum
    db.push_rows(vec![applied_row_with_checksum(
        "20240320_000001_add_user_city",
        "deadbeef_wrong_hash_that_will_never_match",
    )]);
    // lock acquire → 1 row affected (success) — default execute returns Ok(1)

    let result = MigrationRunner::new().add(AddUserCity).run(&db).await;

    assert!(
        matches!(result, Err(MigrationError::ChecksumMismatch { .. })),
        "expected ChecksumMismatch, got: {result:?}"
    );
}

#[tokio::test]
async fn checksum_match_does_not_rerun_migration() {
    // Compute the real checksum for AddUserCity so the stored value matches.
    let mut ctx = reify::migration::MigrationContext::new();
    AddUserCity.up(&mut ctx);
    let real_checksum = compute_checksum(ctx.statements());

    let db = MockDb::new();
    // applied_checksums returns AddUserCity with the correct checksum
    db.push_rows(vec![applied_row_with_checksum(
        "20240320_000001_add_user_city",
        &real_checksum,
    )]);
    // lock acquire → 1 row affected (default)

    MigrationRunner::new()
        .add(AddUserCity)
        .run(&db)
        .await
        .unwrap();

    let sql = db.executed_sql();
    // The migration was already applied with matching checksum — no ADD COLUMN
    assert!(
        !sql.iter().any(|s| s.contains("ADD COLUMN \"city\"")),
        "migration should not re-run when checksum matches: {sql:?}"
    );
}

#[tokio::test]
async fn lock_already_acquired_returns_error() {
    let db = MockDb::new();
    // acquire lock → 0 rows affected (lock already held)
    db.push_execute_result(0); // CREATE tracking table → ok (default would be 1, but we need to
    // place this result for the acquire UPDATE specifically)
    // We need to skip the CREATE tracking + CREATE lock + INSERT sentinel calls first.
    // Those are execute() calls that consume from the queue if present.
    // Strategy: pre-fill the queue so the acquire UPDATE gets 0.
    // CREATE tracking table → 1 (default, queue empty at that point)
    // CREATE lock table     → 1 (default)
    // INSERT sentinel       → 1 (default)
    // UPDATE acquire        → 0 (our pre-loaded value)
    // But execute_results is consumed in order, so we push 0 and let the first
    // three calls consume the default (queue empty → 1).
    // Reset: push_execute_result(0) was already called above — that will be
    // consumed by the FIRST execute call (CREATE tracking table), not acquire.
    // We need to push 3 × 1 then 1 × 0.

    // Re-create db cleanly:
    let db = MockDb::new();
    db.push_execute_result(1); // CREATE tracking table
    db.push_execute_result(1); // CREATE lock table
    db.push_execute_result(1); // INSERT sentinel (ignored error path, but execute is called)
    db.push_execute_result(0); // UPDATE acquire → lock held

    // SELECT to fetch locked_by/locked_at after failed acquire
    db.push_rows(vec![Row::new(
        vec!["locked_by".into(), "locked_at".into()],
        vec![
            Value::String("reify".into()),
            Value::String("2024-01-01T00:00:00Z".into()),
        ],
    )]);

    let result = MigrationRunner::new().add(AddUserCity).run(&db).await;

    assert!(
        matches!(result, Err(MigrationError::Locked { .. })),
        "expected Locked error, got: {result:?}"
    );
}
