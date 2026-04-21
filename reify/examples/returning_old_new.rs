//! PostgreSQL 18+ `RETURNING old.*, new.*` example.
//!
//! Demonstrates how to capture both the previous and new row states
//! in a single UPDATE/DELETE/INSERT statement.
//!
//! Requires the `postgres18` feature.

use reify::{FromRowPositional, Table, Value};

// ── Model definition ────────────────────────────────────────────────

#[derive(Table, Debug, Clone, PartialEq)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    pub name: String,
    pub email: String,
    pub role: String,
}

// ── Manual FromRowPositional implementation ─────────────────────────
//
// In a real app, this would be derived by the `#[derive(Table)]` macro
// when the `postgres18` feature is enabled.

impl FromRowPositional for User {
    fn column_count() -> usize {
        4 // id, name, email, role
    }

    fn from_row_at(row: &reify::Row, offset: usize) -> Result<Self, reify::DbError> {
        Ok(Self {
            id: match row.get_idx(offset) {
                Some(Value::I64(v)) => *v,
                _ => return Err(reify::DbError::Conversion("id".into())),
            },
            name: match row.get_idx(offset + 1) {
                Some(Value::String(v)) => v.clone(),
                _ => return Err(reify::DbError::Conversion("name".into())),
            },
            email: match row.get_idx(offset + 2) {
                Some(Value::String(v)) => v.clone(),
                _ => return Err(reify::DbError::Conversion("email".into())),
            },
            role: match row.get_idx(offset + 3) {
                Some(Value::String(v)) => v.clone(),
                _ => return Err(reify::DbError::Conversion("role".into())),
            },
        })
    }
}

fn main() {
    println!("=== PostgreSQL 18+ RETURNING old.*, new.* Examples ===\n");

    // ── UPDATE with old/new ─────────────────────────────────────────
    //
    // Captures both the previous and new row states in a single query.

    let (sql, _params) = User::update()
        .set(User::role, "admin")
        .filter(User::id.eq(1i64))
        .returning_old_new_all()
        .build();

    println!("UPDATE (old + new):");
    println!("  {sql}");
    println!("  → Returns OldNew<User> with both states\n");

    // Usage with database (pseudo-code):
    // ```
    // let changes: Vec<OldNew<User>> = User::update()
    //     .set(User::role, "admin")
    //     .filter(User::id.eq(1))
    //     .returning_old_new_all()
    //     .fetch_old_new(&db).await?;
    //
    // for change in changes {
    //     println!("Role changed: {:?} → {:?}",
    //         change.old.map(|u| u.role),
    //         change.new.map(|u| u.role));
    // }
    // ```

    // ── UPDATE with only old ────────────────────────────────────────

    let (sql, _params) = User::update()
        .set(User::name, "Alice Smith")
        .filter(User::email.eq("alice@example.com"))
        .returning_old_all()
        .build();

    println!("UPDATE (old only):");
    println!("  {sql}");
    println!("  → Returns OldNew<User> with old=Some, new=None\n");

    // ── UPDATE with only new ────────────────────────────────────────

    let (sql, _params) = User::update()
        .set(User::email, "new@example.com")
        .filter(User::id.eq(2i64))
        .returning_new_all()
        .build();

    println!("UPDATE (new only):");
    println!("  {sql}");
    println!("  → Returns OldNew<User> with old=None, new=Some\n");

    // ── DELETE with old ─────────────────────────────────────────────
    //
    // Captures the deleted row state.

    let (sql, _params) = User::delete()
        .filter(User::id.eq(99i64))
        .returning_old_all()
        .build();

    println!("DELETE (old):");
    println!("  {sql}");
    println!("  → Returns OldNew<User> with old=Some (deleted row), new=None\n");

    // Usage with database (pseudo-code):
    // ```
    // let deleted: Vec<OldNew<User>> = User::delete()
    //     .filter(User::id.eq(99))
    //     .returning_old_all()
    //     .fetch_old(&db).await?;
    //
    // for d in deleted {
    //     println!("Deleted user: {:?}", d.old.unwrap().name);
    // }
    // ```

    // ── INSERT with new ─────────────────────────────────────────────
    //
    // Captures the inserted row state (with DB-generated values like
    // auto-increment IDs, default timestamps, etc.).

    let new_user = User {
        id: 0, // Will be replaced by DB
        name: "Bob".into(),
        email: "bob@example.com".into(),
        role: "user".into(),
    };

    let (sql, _params) = User::insert(&new_user).returning_new_all().build();

    println!("INSERT (new):");
    println!("  {sql}");
    println!("  → Returns OldNew<User> with old=None, new=Some (inserted row)\n");

    // Usage with database (pseudo-code):
    // ```
    // let inserted: Vec<OldNew<User>> = User::insert(&new_user)
    //     .returning_new_all()
    //     .fetch_new(&db).await?;
    //
    // let user = inserted[0].new.as_ref().unwrap();
    // println!("Inserted user with id: {}", user.id);
    // ```

    // ── Batch INSERT with new ───────────────────────────────────────

    let users = vec![
        User {
            id: 0,
            name: "Carol".into(),
            email: "carol@example.com".into(),
            role: "user".into(),
        },
        User {
            id: 0,
            name: "Dave".into(),
            email: "dave@example.com".into(),
            role: "user".into(),
        },
    ];

    let (sql, _params) = User::insert_many(&users).returning_new_all().build();

    println!("INSERT MANY (new):");
    println!("  {sql}");
    println!("  → Returns Vec<OldNew<User>> with all inserted rows\n");

    // ── Classic RETURNING (still works) ─────────────────────────────
    //
    // The traditional RETURNING clause is still available for
    // PostgreSQL < 18 compatibility.

    let (sql, _params) = User::update()
        .set(User::role, "moderator")
        .filter(User::id.eq(3i64))
        .returning(&["id", "role"])
        .build();

    println!("Classic RETURNING (Postgres < 18 compatible):");
    println!("  {sql}");
    println!("  → Returns only the new values (traditional behavior)");
}
