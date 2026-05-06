//! Schema diff and DbColumnInfo tests, extracted from
//! `migration/mod.rs` to keep that file under 1000 LOC.

#![cfg(test)]

use super::test_support::*;
use super::*;
use crate::db::Row;
use crate::table::Table;
use crate::value::Value;

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
        .filter(|d| matches!(d, ColumnDiff::NullableChanged { column, .. } if column == "email"))
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
            soft_delete: false,
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
            soft_delete: false,
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
        fn as_values(&self) -> Vec<Value> {
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
            soft_delete: false,
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
            soft_delete: false,
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
