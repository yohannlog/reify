mod codegen;
mod context;
mod ddl;
mod diff;
mod error;
mod lock;
mod plan;
mod runner;
mod traits;

pub use codegen::{
    generate_materialized_view_migration_file, generate_migration_file,
    generate_view_migration_file,
};
pub use context::MigrationContext;
pub(crate) use ddl::create_table_sql_named;
pub use ddl::{
    MissingDefaultError, add_column_sql, create_table_sql, create_table_sql_with_checks,
    try_add_column_sql,
};
pub use diff::{ColumnDiff, DbColumnInfo, SchemaDiff, TableDiff, normalize_sql_type};
pub use error::MigrationError;
pub use lock::MigrationLock;
pub use plan::{MigrationPlan, MigrationStatus, compute_checksum};
pub use runner::MigrationHooks;
pub use runner::MigrationRunner;
pub use traits::Migration;
// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Database, DbError, DynDatabase, Row};
    use crate::table::Table;
    use crate::value::Value;
    use std::sync::{Arc, Mutex};

    // ── Mock Database ────────────────────────────────────────────────

    /// Captures all SQL executed and returns configurable query results.
    #[derive(Clone)]
    struct MockDb {
        executed: Arc<Mutex<Vec<String>>>,
        query_rows: Arc<Mutex<Vec<Vec<Row>>>>,
    }

    impl MockDb {
        fn new() -> Self {
            Self {
                executed: Arc::new(Mutex::new(Vec::new())),
                query_rows: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Pre-load rows to be returned by successive `query()` calls.
        fn push_query_result(&self, rows: Vec<Row>) {
            self.query_rows.lock().unwrap().push(rows);
        }

        fn executed_sql(&self) -> Vec<String> {
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

        fn transaction<'a>(
            &'a self,
            f: crate::db::TransactionFn<'a>,
        ) -> impl std::future::Future<Output = Result<(), DbError>> + Send {
            async move { f(self).await }
        }
    }

    // ── Minimal Table impl for tests ─────────────────────────────────

    struct Users;
    impl Table for Users {
        fn table_name() -> &'static str {
            "users"
        }
        fn column_names() -> &'static [&'static str] {
            &["id", "email", "role"]
        }
        fn into_values(&self) -> Vec<Value> {
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
                },
            ]
        }
    }

    // ── Manual migration fixture ─────────────────────────────────────

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

    struct IrreversibleMigration;
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

    // ── Tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn dry_run_new_table_emits_create_table() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns users → absent

        let runner = MigrationRunner::new().add_table::<Users>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].version, "auto__users");
        assert!(plans[0].statements[0].contains("CREATE TABLE IF NOT EXISTS \"users\""));
    }

    #[tokio::test]
    async fn dry_run_existing_table_no_new_columns_emits_nothing() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        // existing_columns returns all three columns → no diff
        let existing = vec![
            Row::new(vec!["column_name".into()], vec![Value::String("id".into())]),
            Row::new(
                vec!["column_name".into()],
                vec![Value::String("email".into())],
            ),
            Row::new(
                vec!["column_name".into()],
                vec![Value::String("role".into())],
            ),
        ];
        db.push_query_result(existing);

        let runner = MigrationRunner::new().add_table::<Users>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert!(plans.is_empty());
    }

    #[tokio::test]
    async fn dry_run_existing_table_new_column_emits_add_column() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        // Table exists but missing "role"
        let existing = vec![
            Row::new(vec!["column_name".into()], vec![Value::String("id".into())]),
            Row::new(
                vec!["column_name".into()],
                vec![Value::String("email".into())],
            ),
        ];
        db.push_query_result(existing);

        let runner = MigrationRunner::new().add_table::<Users>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        assert!(plans[0].statements[0].contains("ADD COLUMN \"role\""));
    }

    #[tokio::test]
    async fn dry_run_manual_migration_included() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        let runner = MigrationRunner::new().add(AddUserCity);
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].version, "20240320_000001_add_user_city");
        assert!(plans[0].statements[0].contains("ADD COLUMN \"city\""));
    }

    #[tokio::test]
    async fn dry_run_skips_already_applied_manual() {
        let db = MockDb::new();
        // applied_versions returns the manual migration as already applied
        let applied_row = Row::new(
            vec!["version".into()],
            vec![Value::String("20240320_000001_add_user_city".into())],
        );
        db.push_query_result(vec![applied_row]);

        let runner = MigrationRunner::new().add(AddUserCity);
        let plans = runner.dry_run(&db).await.unwrap();

        assert!(plans.is_empty());
    }

    #[tokio::test]
    async fn run_creates_tracking_table_and_executes_ddl() {
        let db = MockDb::new();
        // applied_checksums → empty (queried after lock acquire)
        db.push_query_result(vec![]);
        db.push_query_result(vec![]); // existing_columns users → absent

        let runner = MigrationRunner::new().add_table::<Users>();
        runner.run(&db).await.unwrap();

        let sql = db.executed_sql();
        // Sequence: CREATE tracking table, CREATE lock table, INSERT sentinel,
        // UPDATE acquire lock, then DDL, then INSERT tracking, then UPDATE release.
        assert!(
            sql.iter().any(|s| s.contains("_reify_migrations")),
            "tracking table not found: {sql:?}"
        );
        assert!(
            sql.iter()
                .any(|s| s.contains("CREATE TABLE IF NOT EXISTS \"users\"")),
            "CREATE TABLE users not found: {sql:?}"
        );
        assert!(
            sql.iter()
                .any(|s| s.contains("INSERT INTO \"_reify_migrations\"")),
            "INSERT tracking not found: {sql:?}"
        );
    }

    #[tokio::test]
    async fn run_manual_migration_executes_up_statements() {
        let db = MockDb::new();
        // applied_checksums → empty (queried after lock acquire)
        db.push_query_result(vec![]);

        let runner = MigrationRunner::new().add(AddUserCity);
        runner.run(&db).await.unwrap();

        let sql = db.executed_sql();
        let has_add_column = sql.iter().any(|s| s.contains("ADD COLUMN \"city\""));
        assert!(has_add_column);
    }

    #[tokio::test]
    async fn rollback_executes_down_and_removes_tracking_row() {
        let db = MockDb::new();
        // applied_versions for rollback query
        let applied_row = Row::new(
            vec!["version".into()],
            vec![Value::String("20240320_000001_add_user_city".into())],
        );
        db.push_query_result(vec![applied_row]); // last applied query

        let runner = MigrationRunner::new().add(AddUserCity);
        runner.rollback(&db).await.unwrap();

        let sql = db.executed_sql();
        let has_drop = sql.iter().any(|s| s.contains("DROP COLUMN \"city\""));
        let has_delete = sql
            .iter()
            .any(|s| s.contains("DELETE FROM \"_reify_migrations\""));
        assert!(has_drop, "expected DROP COLUMN \"city\" in: {sql:?}");
        assert!(has_delete, "expected DELETE FROM tracking in: {sql:?}");
    }

    #[tokio::test]
    async fn rollback_irreversible_returns_error() {
        let db = MockDb::new();
        let applied_row = Row::new(
            vec!["version".into()],
            vec![Value::String("20240321_000001_irreversible".into())],
        );
        db.push_query_result(vec![applied_row]);

        let runner = MigrationRunner::new().add(IrreversibleMigration);
        let result = runner.rollback(&db).await;

        assert!(matches!(result, Err(MigrationError::NotReversible(_))));
    }

    #[tokio::test]
    async fn migration_context_collects_statements() {
        let mut ctx = MigrationContext::new();
        ctx.add_column("users", "city", "TEXT NOT NULL");
        ctx.drop_column("users", "old_col");
        ctx.rename_column("users", "nm", "name");
        ctx.execute("UPDATE users SET city = 'Paris';");

        assert_eq!(ctx.statements().len(), 4);
        assert!(ctx.statements()[0].contains("ADD COLUMN \"city\""));
        assert!(ctx.statements()[1].contains("DROP COLUMN \"old_col\""));
        assert!(ctx.statements()[2].contains("RENAME COLUMN \"nm\" TO \"name\""));
        assert!(ctx.statements()[3].contains("UPDATE users"));
    }

    #[tokio::test]
    async fn migration_plan_display_format() {
        use super::plan::compute_checksum;
        let stmts = vec!["ALTER TABLE users ADD COLUMN city TEXT NOT NULL;".to_string()];
        let checksum = compute_checksum(&stmts);
        let plan = MigrationPlan {
            version: "20240320_000001_add_user_city".into(),
            description: "Add city column to users".into(),
            comment: None,
            statements: stmts,
            checksum,
            schema_diff: None,
        };
        let display = plan.display();
        assert!(display.contains("Would apply (up)"));
        assert!(display.contains("20240320_000001_add_user_city"));
        assert!(display.contains("ALTER TABLE users"));
        assert!(display.contains("SQL:"));
    }

    #[test]
    fn migration_plan_display_includes_schema_diff_when_present() {
        use super::plan::compute_checksum;
        let stmts = vec![
            "CREATE TABLE IF NOT EXISTS \"users\" (\"id\" BIGSERIAL PRIMARY KEY);".to_string(),
        ];
        let checksum = compute_checksum(&stmts);
        let plan = MigrationPlan {
            version: "auto__users".into(),
            description: "Create table users".into(),
            comment: None,
            statements: stmts,
            checksum,
            schema_diff: Some(SchemaDiff {
                tables: vec![TableDiff {
                    table_name: "users".into(),
                    is_new_table: true,
                    column_diffs: vec![
                        ColumnDiff::Added {
                            column: "id".into(),
                        },
                        ColumnDiff::Added {
                            column: "email".into(),
                        },
                    ],
                }],
            }),
        };
        let display = plan.display();
        assert!(display.contains("Would apply (up)"));
        assert!(
            display.contains("Schema diff:"),
            "missing Schema diff header: {display}"
        );
        assert!(
            display.contains("✚ table `users`"),
            "missing table symbol: {display}"
        );
        assert!(display.contains("✚ `id`"), "missing id column: {display}");
        assert!(
            display.contains("✚ `email`"),
            "missing email column: {display}"
        );
        assert!(display.contains("SQL:"), "missing SQL: label: {display}");
        assert!(
            display.contains("CREATE TABLE"),
            "missing SQL body: {display}"
        );
    }

    #[test]
    fn generate_migration_file_produces_valid_template() {
        let content = generate_migration_file("add_user_city", "20240320_000001_add_user_city");
        assert!(content.contains("struct AddUserCity"));
        assert!(content.contains("impl Migration for AddUserCity"));
        assert!(content.contains("20240320_000001_add_user_city"));
        assert!(content.contains("todo!(\"implement up migration\")"));
    }

    // ── Schema diff / DbColumnInfo tests ────────────────────────────

    #[test]
    fn normalize_sql_type_aliases() {
        assert_eq!(normalize_sql_type("BIGSERIAL"), "bigint");
        assert_eq!(normalize_sql_type("bigserial"), "bigint");
        assert_eq!(normalize_sql_type("serial"), "integer");
        assert_eq!(normalize_sql_type("smallserial"), "smallint");
        assert_eq!(normalize_sql_type("int"), "integer");
        assert_eq!(normalize_sql_type("INT4"), "integer");
        assert_eq!(normalize_sql_type("int8"), "bigint");
        assert_eq!(normalize_sql_type("CHARACTER VARYING"), "varchar");
        assert_eq!(normalize_sql_type("varchar(255)"), "varchar(255)");
        assert_eq!(normalize_sql_type("bool"), "boolean");
        assert_eq!(normalize_sql_type("float4"), "real");
        assert_eq!(normalize_sql_type("float8"), "double precision");
        assert_eq!(
            normalize_sql_type("timestamp without time zone"),
            "timestamp"
        );
        assert_eq!(
            normalize_sql_type("timestamp with time zone"),
            "timestamptz"
        );
        assert_eq!(normalize_sql_type("TIMESTAMPTZ"), "timestamptz");
        // Unknown types pass through lowercased
        assert_eq!(normalize_sql_type("JSONB"), "jsonb");
        assert_eq!(normalize_sql_type("uuid"), "uuid");
        // Array types
        assert_eq!(normalize_sql_type("integer[]"), "integer[]");
        assert_eq!(normalize_sql_type("_int4"), "integer[]");
        assert_eq!(normalize_sql_type("text[]"), "text[]");
        assert_eq!(normalize_sql_type("_text"), "text[]");
    }

    #[test]
    fn column_diff_display_variants() {
        assert!(
            ColumnDiff::Added {
                column: "city".into()
            }
            .display()
            .contains("✚ `city`")
        );
        assert!(
            ColumnDiff::Removed {
                column: "old".into()
            }
            .display()
            .contains("✖ `old`")
        );
        assert!(
            ColumnDiff::TypeChanged {
                column: "age".into(),
                from: "integer".into(),
                to: "bigint".into(),
            }
            .display()
            .contains("integer → bigint")
        );
        assert!(
            ColumnDiff::NullableChanged {
                column: "email".into(),
                from: false,
                to: true,
            }
            .display()
            .contains("not null → nullable")
        );
        assert!(
            ColumnDiff::UniqueChanged {
                column: "slug".into(),
                from: false,
                to: true,
            }
            .display()
            .contains("non-unique → unique")
        );
        assert!(
            ColumnDiff::DefaultChanged {
                column: "role".into(),
                from: None,
                to: Some("'member'".into()),
            }
            .display()
            .contains("none → 'member'")
        );
    }

    #[test]
    fn schema_diff_is_empty_and_display() {
        let empty = SchemaDiff { tables: vec![] };
        assert!(empty.is_empty());
        assert!(empty.display().contains("no schema differences"));

        let diff = SchemaDiff {
            tables: vec![TableDiff {
                table_name: "users".into(),
                is_new_table: true,
                column_diffs: vec![],
            }],
        };
        assert!(!diff.is_empty());
        assert!(diff.display().contains("✚ table `users`"));
    }

    #[test]
    fn table_diff_is_empty() {
        let empty = TableDiff {
            table_name: "posts".into(),
            is_new_table: false,
            column_diffs: vec![],
        };
        assert!(empty.is_empty());

        let non_empty = TableDiff {
            table_name: "posts".into(),
            is_new_table: false,
            column_diffs: vec![ColumnDiff::Added {
                column: "slug".into(),
            }],
        };
        assert!(!non_empty.is_empty());
    }

    #[tokio::test]
    async fn existing_column_details_returns_none_for_absent_table() {
        let db = MockDb::new();
        // Both queries return empty → table absent
        db.push_query_result(vec![]);
        db.push_query_result(vec![]);

        let runner = MigrationRunner::new();
        let result = runner
            .existing_column_details(&db, "missing_table")
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn existing_column_details_parses_rows() {
        let db = MockDb::new();

        // Column metadata rows
        let col_rows = vec![
            Row::new(
                vec![
                    "column_name".into(),
                    "data_type".into(),
                    "is_nullable".into(),
                    "column_default".into(),
                ],
                vec![
                    Value::String("id".into()),
                    Value::String("bigint".into()),
                    Value::String("NO".into()),
                    Value::Null,
                ],
            ),
            Row::new(
                vec![
                    "column_name".into(),
                    "data_type".into(),
                    "is_nullable".into(),
                    "column_default".into(),
                ],
                vec![
                    Value::String("email".into()),
                    Value::String("CHARACTER VARYING".into()),
                    Value::String("YES".into()),
                    Value::String("''".into()),
                ],
            ),
        ];
        db.push_query_result(col_rows);

        // Unique constraint rows — only "email" is unique
        let unique_rows = vec![Row::new(
            vec!["column_name".into()],
            vec![Value::String("email".into())],
        )];
        db.push_query_result(unique_rows);

        let runner = MigrationRunner::new();
        let infos = runner
            .existing_column_details(&db, "users")
            .await
            .unwrap()
            .expect("should return Some");

        assert_eq!(infos.len(), 2);

        let id_col = &infos[0];
        assert_eq!(id_col.name, "id");
        assert_eq!(id_col.data_type, "bigint");
        assert!(!id_col.is_nullable);
        assert!(id_col.column_default.is_none());
        assert!(!id_col.is_unique);

        let email_col = &infos[1];
        assert_eq!(email_col.name, "email");
        assert_eq!(email_col.data_type, "varchar"); // normalised
        assert!(email_col.is_nullable);
        assert_eq!(email_col.column_default.as_deref(), Some("''"));
        assert!(email_col.is_unique);
    }

    // ── diff() tests ─────────────────────────────────────────────────

    /// Build a single-column DbColumnInfo row for MockDb.
    fn make_col_row(name: &str, data_type: &str, nullable: &str) -> Row {
        Row::new(
            vec![
                "column_name".into(),
                "data_type".into(),
                "is_nullable".into(),
                "column_default".into(),
            ],
            vec![
                Value::String(name.into()),
                Value::String(data_type.into()),
                Value::String(nullable.into()),
                Value::Null,
            ],
        )
    }

    #[tokio::test]
    async fn diff_new_table_reports_all_columns_added() {
        let db = MockDb::new();
        // existing_column_details: col query → empty (table absent)
        db.push_query_result(vec![]);

        let runner = MigrationRunner::new().add_table::<Users>();
        let diff = runner.diff(&db).await.unwrap();

        assert_eq!(diff.tables.len(), 1);
        let td = &diff.tables[0];
        assert_eq!(td.table_name, "users");
        assert!(td.is_new_table);
        // All three columns (id, email, role) reported as Added
        assert_eq!(td.column_diffs.len(), 3);
        assert!(
            td.column_diffs
                .iter()
                .all(|d| matches!(d, ColumnDiff::Added { .. }))
        );
    }

    #[tokio::test]
    async fn diff_no_changes_returns_empty() {
        let db = MockDb::new();
        // Column rows matching Users exactly
        db.push_query_result(vec![
            make_col_row("id", "bigint", "NO"),
            make_col_row("email", "text", "NO"),
            make_col_row("role", "text", "NO"),
        ]);
        db.push_query_result(vec![]); // no unique constraints

        let runner = MigrationRunner::new().add_table::<Users>();
        let diff = runner.diff(&db).await.unwrap();

        assert!(diff.is_empty(), "expected no diff, got: {:#?}", diff.tables);
    }

    #[tokio::test]
    async fn diff_added_column_detected() {
        let db = MockDb::new();
        // DB only has id + email; struct also has role
        db.push_query_result(vec![
            make_col_row("id", "bigint", "NO"),
            make_col_row("email", "text", "NO"),
        ]);
        db.push_query_result(vec![]); // no unique constraints

        let runner = MigrationRunner::new().add_table::<Users>();
        let diff = runner.diff(&db).await.unwrap();

        assert_eq!(diff.tables.len(), 1);
        let added: Vec<_> = diff.tables[0]
            .column_diffs
            .iter()
            .filter(|d| matches!(d, ColumnDiff::Added { column } if column == "role"))
            .collect();
        assert_eq!(added.len(), 1);
    }

    #[tokio::test]
    async fn diff_removed_column_detected() {
        let db = MockDb::new();
        // DB has an extra column "legacy" not in the struct
        db.push_query_result(vec![
            make_col_row("id", "bigint", "NO"),
            make_col_row("email", "text", "NO"),
            make_col_row("role", "text", "NO"),
            make_col_row("legacy", "text", "YES"),
        ]);
        db.push_query_result(vec![]); // no unique constraints

        let runner = MigrationRunner::new().add_table::<Users>();
        let diff = runner.diff(&db).await.unwrap();

        assert_eq!(diff.tables.len(), 1);
        let removed: Vec<_> = diff.tables[0]
            .column_diffs
            .iter()
            .filter(|d| matches!(d, ColumnDiff::Removed { column } if column == "legacy"))
            .collect();
        assert_eq!(removed.len(), 1);
    }

    #[tokio::test]
    async fn diff_type_change_detected() {
        let db = MockDb::new();
        // DB has id as "integer" but struct infers "bigint"
        db.push_query_result(vec![
            make_col_row("id", "integer", "NO"),
            make_col_row("email", "text", "NO"),
            make_col_row("role", "text", "NO"),
        ]);
        db.push_query_result(vec![]);

        let runner = MigrationRunner::new().add_table::<Users>();
        let diff = runner.diff(&db).await.unwrap();

        assert_eq!(diff.tables.len(), 1);
        let type_changes: Vec<_> = diff.tables[0]
            .column_diffs
            .iter()
            .filter(|d| matches!(d, ColumnDiff::TypeChanged { column, .. } if column == "id"))
            .collect();
        assert_eq!(type_changes.len(), 1);
        if let ColumnDiff::TypeChanged { from, to, .. } = type_changes[0] {
            assert_eq!(from, "integer");
            assert_eq!(to, "bigint");
        }
    }

    #[tokio::test]
    async fn diff_nullable_change_detected() {
        let db = MockDb::new();
        // DB has email as nullable; struct has it as NOT NULL (nullable: false)
        db.push_query_result(vec![
            make_col_row("id", "bigint", "NO"),
            make_col_row("email", "text", "YES"), // nullable in DB
            make_col_row("role", "text", "NO"),
        ]);
        db.push_query_result(vec![]);

        let runner = MigrationRunner::new().add_table::<Users>();
        let diff = runner.diff(&db).await.unwrap();

        assert_eq!(diff.tables.len(), 1);
        let nullable_changes: Vec<_> = diff.tables[0]
            .column_diffs
            .iter()
            .filter(
                |d| matches!(d, ColumnDiff::NullableChanged { column, .. } if column == "email"),
            )
            .collect();
        assert_eq!(nullable_changes.len(), 1);
        if let ColumnDiff::NullableChanged { from, to, .. } = nullable_changes[0] {
            assert!(*from, "DB was nullable");
            assert!(!*to, "struct is not nullable");
        }
    }

    #[tokio::test]
    async fn diff_display_output_contains_symbols() {
        let diff = SchemaDiff {
            tables: vec![TableDiff {
                table_name: "orders".into(),
                is_new_table: false,
                column_diffs: vec![
                    ColumnDiff::Added {
                        column: "total".into(),
                    },
                    ColumnDiff::Removed {
                        column: "old_col".into(),
                    },
                    ColumnDiff::TypeChanged {
                        column: "amount".into(),
                        from: "integer".into(),
                        to: "numeric".into(),
                    },
                ],
            }],
        };
        let out = diff.display();
        assert!(
            out.contains("⇄ table `orders`"),
            "missing table header: {out}"
        );
        assert!(out.contains("✚ `total`"), "missing added symbol: {out}");
        assert!(out.contains("✖ `old_col`"), "missing removed symbol: {out}");
        assert!(out.contains("⇄ `amount`"), "missing changed symbol: {out}");
        assert!(out.contains("Schema diff:"), "missing header: {out}");
    }

    #[test]
    fn create_table_sql_contains_all_columns() {
        let defs: Vec<crate::schema::ColumnDef> = vec![
            crate::schema::ColumnDef {
                name: "id",
                sql_type: crate::schema::SqlType::BigSerial,
                primary_key: true,
                auto_increment: true,
                unique: false,
                index: false,
                nullable: false,
                default: None,
                computed: None,
                timestamp_kind: None,
                timestamp_source: crate::schema::TimestampSource::Vm,
                check: None,
                foreign_key: None,
            },
            crate::schema::ColumnDef {
                name: "email",
                sql_type: crate::schema::SqlType::Text,
                primary_key: false,
                auto_increment: false,
                unique: true,
                index: false,
                nullable: false,
                default: None,
                computed: None,
                timestamp_kind: None,
                timestamp_source: crate::schema::TimestampSource::Vm,
                check: None,
                foreign_key: None,
            },
        ];
        let sql = create_table_sql::<Users>(&defs, crate::query::Dialect::Postgres);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"users\""));
        assert!(sql.contains("id"));
        assert!(sql.contains("email"));
        assert!(sql.contains("BIGSERIAL"));
        assert!(sql.contains("PRIMARY KEY"));
    }

    #[test]
    fn create_table_sql_emits_foreign_key_constraint() {
        use crate::schema::{ForeignKeyAction, ForeignKeyDef};

        struct Posts;
        impl Table for Posts {
            fn table_name() -> &'static str {
                "posts"
            }
            fn column_names() -> &'static [&'static str] {
                &["id", "user_id"]
            }
            fn into_values(&self) -> Vec<Value> {
                vec![]
            }
        }

        let defs: Vec<crate::schema::ColumnDef> = vec![
            crate::schema::ColumnDef {
                name: "id",
                sql_type: crate::schema::SqlType::BigSerial,
                primary_key: true,
                auto_increment: true,
                unique: false,
                index: false,
                nullable: false,
                default: None,
                computed: None,
                timestamp_kind: None,
                timestamp_source: crate::schema::TimestampSource::Vm,
                check: None,
                foreign_key: None,
            },
            crate::schema::ColumnDef {
                name: "user_id",
                sql_type: crate::schema::SqlType::BigInt,
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
                foreign_key: Some(ForeignKeyDef {
                    references_table: "users".to_string(),
                    references_column: "id".to_string(),
                    on_delete: ForeignKeyAction::Cascade,
                    on_update: ForeignKeyAction::NoAction,
                }),
            },
        ];
        let sql = create_table_sql::<Posts>(&defs, crate::query::Dialect::Postgres);
        assert!(sql.contains("FOREIGN KEY"), "missing FOREIGN KEY: {sql}");
        assert!(
            sql.contains("REFERENCES \"users\" (\"id\")"),
            "missing REFERENCES clause: {sql}"
        );
        assert!(
            sql.contains("ON DELETE CASCADE"),
            "missing ON DELETE CASCADE: {sql}"
        );
        // ON UPDATE NO ACTION should be omitted (default)
        assert!(
            !sql.contains("ON UPDATE"),
            "unexpected ON UPDATE clause: {sql}"
        );
    }

    // ── View migration tests ────────────────────────────────────────

    #[test]
    fn migration_context_create_view() {
        let mut ctx = MigrationContext::new();
        ctx.create_view(
            "active_users",
            "SELECT id, email FROM users WHERE deleted_at IS NULL",
        );
        assert_eq!(ctx.statements().len(), 1);
        assert!(ctx.statements()[0].contains("CREATE OR REPLACE VIEW \"active_users\""));
        assert!(ctx.statements()[0].contains("SELECT id, email FROM users"));
    }

    #[test]
    fn migration_context_drop_view() {
        let mut ctx = MigrationContext::new();
        ctx.drop_view("active_users");
        assert_eq!(ctx.statements().len(), 1);
        assert!(ctx.statements()[0].contains("DROP VIEW IF EXISTS \"active_users\""));
    }

    // Minimal View impl for tests
    struct TestView;
    impl Table for TestView {
        fn table_name() -> &'static str {
            "active_users"
        }
        fn column_names() -> &'static [&'static str] {
            &["id", "email"]
        }
        fn into_values(&self) -> Vec<Value> {
            vec![]
        }
    }
    impl crate::view::View for TestView {
        fn view_name() -> &'static str {
            "active_users"
        }
        fn view_query() -> crate::view::ViewQuery {
            crate::view::ViewQuery::Raw(
                "SELECT id, email FROM users WHERE deleted_at IS NULL".into(),
            )
        }
    }

    #[tokio::test]
    async fn dry_run_view_emits_create_view() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        let runner = MigrationRunner::new().add_view::<TestView>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].version, "auto_view__active_users");
        assert!(plans[0].statements[0].contains("CREATE OR REPLACE VIEW \"active_users\""));
    }

    #[tokio::test]
    async fn dry_run_view_skips_already_applied() {
        let db = MockDb::new();
        let applied_row = Row::new(
            vec!["version".into()],
            vec![Value::String("auto_view__active_users".into())],
        );
        db.push_query_result(vec![applied_row]);

        let runner = MigrationRunner::new().add_view::<TestView>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert!(plans.is_empty());
    }

    #[tokio::test]
    async fn run_view_executes_create_view() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        let runner = MigrationRunner::new().add_view::<TestView>();
        runner.run(&db).await.unwrap();

        let sql = db.executed_sql();
        let has_create_view = sql
            .iter()
            .any(|s| s.contains("CREATE OR REPLACE VIEW \"active_users\""));
        assert!(has_create_view, "expected CREATE VIEW in: {sql:?}");
    }

    #[test]
    fn generate_view_migration_file_produces_valid_template() {
        let content = generate_view_migration_file("active_users", "20240320_000001_active_users");
        assert!(content.contains("struct ActiveUsers"));
        assert!(content.contains("impl Migration for ActiveUsers"));
        assert!(content.contains("ctx.create_view(\"active_users\""));
        assert!(content.contains("ctx.drop_view(\"active_users\""));
    }

    // ── Materialized view migration tests ──────────────────────────────

    #[test]
    fn migration_context_create_materialized_view() {
        let mut ctx = MigrationContext::new();
        ctx.create_materialized_view(
            "sales_summary",
            "SELECT seller_no, invoice_date, sum(invoice_amt) FROM invoice GROUP BY 1, 2",
        );
        assert_eq!(ctx.statements().len(), 1);
        assert!(
            ctx.statements()[0]
                .contains("CREATE MATERIALIZED VIEW IF NOT EXISTS \"sales_summary\"")
        );
        assert!(ctx.statements()[0].contains("WITH DATA"));
    }

    #[test]
    fn migration_context_create_materialized_view_no_data() {
        let mut ctx = MigrationContext::new();
        ctx.create_materialized_view_no_data("sales_summary", "SELECT 1");
        assert_eq!(ctx.statements().len(), 1);
        assert!(ctx.statements()[0].contains("WITH NO DATA"));
    }

    #[test]
    fn migration_context_drop_materialized_view() {
        let mut ctx = MigrationContext::new();
        ctx.drop_materialized_view("sales_summary");
        assert_eq!(ctx.statements().len(), 1);
        assert!(ctx.statements()[0].contains("DROP MATERIALIZED VIEW IF EXISTS \"sales_summary\""));
    }

    #[test]
    fn migration_context_refresh_materialized_view_blocking() {
        let mut ctx = MigrationContext::new();
        ctx.refresh_materialized_view("sales_summary", false);
        assert_eq!(ctx.statements().len(), 1);
        assert_eq!(
            ctx.statements()[0],
            "REFRESH MATERIALIZED VIEW \"sales_summary\";"
        );
    }

    #[test]
    fn migration_context_refresh_materialized_view_concurrently() {
        let mut ctx = MigrationContext::new();
        ctx.refresh_materialized_view("sales_summary", true);
        assert_eq!(ctx.statements().len(), 1);
        assert_eq!(
            ctx.statements()[0],
            "REFRESH MATERIALIZED VIEW CONCURRENTLY \"sales_summary\";"
        );
    }

    // Minimal View impl for materialized view tests
    struct TestMatView;
    impl Table for TestMatView {
        fn table_name() -> &'static str {
            "sales_summary"
        }
        fn column_names() -> &'static [&'static str] {
            &["seller_no", "sales_amt"]
        }
        fn into_values(&self) -> Vec<Value> {
            vec![]
        }
    }
    impl crate::view::View for TestMatView {
        fn view_name() -> &'static str {
            "sales_summary"
        }
        fn view_query() -> crate::view::ViewQuery {
            crate::view::ViewQuery::Raw(
                "SELECT seller_no, sum(invoice_amt) FROM invoice GROUP BY seller_no".into(),
            )
        }
    }

    #[tokio::test]
    async fn dry_run_materialized_view_emits_create_materialized_view() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        let runner = MigrationRunner::new().add_materialized_view::<TestMatView>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].version, "auto_matview__sales_summary");
        assert!(
            plans[0].statements[0]
                .contains("CREATE MATERIALIZED VIEW IF NOT EXISTS \"sales_summary\"")
        );
        assert!(plans[0].statements[0].contains("WITH DATA"));
    }

    #[tokio::test]
    async fn dry_run_materialized_view_skips_already_applied() {
        let db = MockDb::new();
        let applied_row = Row::new(
            vec!["version".into()],
            vec![Value::String("auto_matview__sales_summary".into())],
        );
        db.push_query_result(vec![applied_row]);

        let runner = MigrationRunner::new().add_materialized_view::<TestMatView>();
        let plans = runner.dry_run(&db).await.unwrap();

        assert!(plans.is_empty());
    }

    #[tokio::test]
    async fn run_materialized_view_executes_create_materialized_view() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty

        let runner = MigrationRunner::new().add_materialized_view::<TestMatView>();
        runner.run(&db).await.unwrap();

        let sql = db.executed_sql();
        let has_create = sql
            .iter()
            .any(|s| s.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS \"sales_summary\""));
        assert!(has_create, "expected CREATE MATERIALIZED VIEW in: {sql:?}");
    }

    #[test]
    fn generate_materialized_view_migration_file_produces_valid_template() {
        let content = generate_materialized_view_migration_file(
            "sales_summary",
            "20240320_000001_sales_summary",
        );
        assert!(content.contains("struct SalesSummary"));
        assert!(content.contains("impl Migration for SalesSummary"));
        assert!(content.contains("ctx.create_materialized_view(\"sales_summary\""));
        assert!(content.contains("ctx.drop_materialized_view(\"sales_summary\""));
    }

    // ── Hook tests ───────────────────────────────────────────────────

    #[tokio::test]
    async fn hooks_before_each_called() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns users → absent

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let runner = MigrationRunner::new()
            .add_table::<Users>()
            .on_before_each(move |_plan| {
                let c = counter_clone.clone();
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            });

        runner.run(&db).await.unwrap();
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "before_each should be called once"
        );
    }

    #[tokio::test]
    async fn hooks_after_each_called() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns users → absent

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let runner = MigrationRunner::new()
            .add_table::<Users>()
            .on_after_each(move |_plan| {
                let c = counter_clone.clone();
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            });

        runner.run(&db).await.unwrap();
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "after_each should be called once after success"
        );
    }

    #[tokio::test]
    async fn hooks_before_each_can_abort() {
        let db = MockDb::new();
        db.push_query_result(vec![]); // applied_versions → empty
        db.push_query_result(vec![]); // existing_columns users → absent

        let runner = MigrationRunner::new()
            .add_table::<Users>()
            .on_before_each(|_plan| {
                Box::pin(async move { Err(MigrationError::Other("aborted by hook".into())) })
            });

        let result = runner.run(&db).await;
        assert!(
            matches!(result, Err(MigrationError::Other(ref msg)) if msg.contains("aborted by hook")),
            "run() should propagate the hook error: {result:?}"
        );
        // The user table DDL should NOT have been executed (tracking table setup runs before hooks).
        let sql = db.executed_sql();
        assert!(
            !sql.iter()
                .any(|s| s.contains("CREATE TABLE IF NOT EXISTS \"users\"")),
            "CREATE TABLE users should not run when before_each aborts: {sql:?}"
        );
    }
}
