//! Tests for batch insert and upsert (ON CONFLICT) SQL generation.

use reify::{Dialect, Table};

// ── Shared test model ────────────────────────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
    pub name: String,
}

fn alice() -> User {
    User {
        id: 1,
        email: "alice@example.com".into(),
        name: "Alice".into(),
    }
}

fn bob() -> User {
    User {
        id: 2,
        email: "bob@example.com".into(),
        name: "Bob".into(),
    }
}

fn carol() -> User {
    User {
        id: 3,
        email: "carol@example.com".into(),
        name: "Carol".into(),
    }
}

// ── InsertManyBuilder — basic SQL generation ─────────────────────────

#[test]
fn insert_many_single_row() {
    let (sql, params) = User::insert_many(&[alice()]).build();
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"name\") VALUES (?, ?, ?)"
    );
    assert_eq!(params.len(), 3);
}

#[test]
fn insert_many_two_rows() {
    let (sql, params) = User::insert_many(&[alice(), bob()]).build();
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"name\") VALUES (?, ?, ?), (?, ?, ?)"
    );
    assert_eq!(params.len(), 6);
}

#[test]
fn insert_many_three_rows_param_order() {
    use reify::Value;
    let (sql, params) = User::insert_many(&[alice(), bob(), carol()]).build();
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"name\") VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)"
    );
    assert_eq!(params.len(), 9);
    // Params are row-major: alice's values, then bob's, then carol's.
    assert_eq!(params[0], Value::I64(1));
    assert_eq!(params[3], Value::I64(2));
    assert_eq!(params[6], Value::I64(3));
}

#[test]
#[should_panic(expected = "insert_many requires at least one row")]
fn insert_many_empty_panics() {
    User::insert_many(&[]).build();
}

#[test]
fn insert_many_try_new_returns_error_on_empty() {
    let result = reify::InsertManyBuilder::<User>::try_new(&[]);
    match result {
        Err(err) => {
            assert_eq!(err, reify::BuildError::EmptyInsert);
            assert!(err.to_string().contains("at least one row"));
        }
        Ok(_) => panic!("expected EmptyInsert error"),
    }
}

#[test]
fn insert_many_try_new_ok_with_rows() {
    let result = reify::InsertManyBuilder::<User>::try_new(&[alice()]);
    assert!(result.is_ok());
}

// ── InsertBuilder — upsert (single row) ─────────────────────────────

#[test]
fn insert_on_conflict_do_nothing_postgres() {
    let (sql, _) = User::insert(&alice())
        .on_conflict_do_nothing()
        .build_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"name\") VALUES (?, ?, ?) ON CONFLICT DO NOTHING"
    );
}

#[test]
fn insert_on_conflict_do_nothing_mysql() {
    let (sql, _) = User::insert(&alice())
        .on_conflict_do_nothing()
        .build_with_dialect(Dialect::Mysql);
    assert_eq!(
        sql,
        "INSERT IGNORE INTO \"users\" (\"id\", \"email\", \"name\") VALUES (?, ?, ?)"
    );
}

#[test]
fn insert_on_conflict_do_update_postgres() {
    let (sql, _) = User::insert(&alice())
        .on_conflict_do_update(&["email"], &["name"])
        .build_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"name\") VALUES (?, ?, ?) \
         ON CONFLICT (\"email\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""
    );
}

#[test]
fn insert_on_conflict_do_update_multiple_targets_postgres() {
    let (sql, _) = User::insert(&alice())
        .on_conflict_do_update(&["id", "email"], &["name", "email"])
        .build_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"name\") VALUES (?, ?, ?) \
         ON CONFLICT (\"id\", \"email\") DO UPDATE SET \"name\" = EXCLUDED.\"name\", \"email\" = EXCLUDED.\"email\""
    );
}

#[test]
fn insert_on_conflict_do_update_mysql() {
    let (sql, _) = User::insert(&alice())
        .on_conflict_do_update(&["email"], &["name"])
        .build_with_dialect(Dialect::Mysql);
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"name\") VALUES (?, ?, ?) \
         ON DUPLICATE KEY UPDATE \"name\" = VALUES(\"name\")"
    );
}

#[test]
fn insert_no_conflict_clause_generic() {
    // Default build() emits no conflict clause regardless of dialect.
    let (sql, _) = User::insert(&alice()).build();
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"name\") VALUES (?, ?, ?)"
    );
}

// ── InsertManyBuilder — upsert ───────────────────────────────────────

#[test]
fn insert_many_on_conflict_do_nothing_postgres() {
    let (sql, params) = User::insert_many(&[alice(), bob()])
        .on_conflict_do_nothing()
        .build_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"name\") VALUES (?, ?, ?), (?, ?, ?) ON CONFLICT DO NOTHING"
    );
    assert_eq!(params.len(), 6);
}

#[test]
fn insert_many_on_conflict_do_nothing_mysql() {
    let (sql, params) = User::insert_many(&[alice(), bob()])
        .on_conflict_do_nothing()
        .build_with_dialect(Dialect::Mysql);
    assert_eq!(
        sql,
        "INSERT IGNORE INTO \"users\" (\"id\", \"email\", \"name\") VALUES (?, ?, ?), (?, ?, ?)"
    );
    assert_eq!(params.len(), 6);
}

#[test]
fn insert_many_on_conflict_do_update_postgres() {
    let (sql, params) = User::insert_many(&[alice(), bob()])
        .on_conflict_do_update(&["email"], &["name"])
        .build_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"name\") VALUES (?, ?, ?), (?, ?, ?) \
         ON CONFLICT (\"email\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""
    );
    assert_eq!(params.len(), 6);
}

#[test]
fn insert_many_on_conflict_do_update_mysql() {
    let (sql, params) = User::insert_many(&[alice(), bob()])
        .on_conflict_do_update(&["email"], &["name", "email"])
        .build_with_dialect(Dialect::Mysql);
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"name\") VALUES (?, ?, ?), (?, ?, ?) \
         ON DUPLICATE KEY UPDATE \"name\" = VALUES(\"name\"), \"email\" = VALUES(\"email\")"
    );
    assert_eq!(params.len(), 6);
}

// ── RETURNING (PostgreSQL feature-gated) ────────────────────────────

#[cfg(feature = "postgres")]
#[test]
fn insert_many_returning_postgres() {
    let (sql, params) = User::insert_many(&[alice(), bob()])
        .returning(&["id", "email"])
        .build_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"name\") VALUES (?, ?, ?), (?, ?, ?) RETURNING \"id\", \"email\""
    );
    assert_eq!(params.len(), 6);
}

#[cfg(feature = "postgres")]
#[test]
fn insert_many_upsert_returning_postgres() {
    let (sql, _) = User::insert_many(&[alice()])
        .on_conflict_do_update(&["email"], &["name"])
        .returning(&["id"])
        .build_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"name\") VALUES (?, ?, ?) \
         ON CONFLICT (\"email\") DO UPDATE SET \"name\" = EXCLUDED.\"name\" RETURNING \"id\""
    );
}
