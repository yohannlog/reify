//! Shared workload model: the canonical `users` table used by every
//! framework-specific implementation.
//!
//! All frameworks target the same schema:
//!   CREATE TABLE users (
//!     id INTEGER PRIMARY KEY,
//!     name TEXT NOT NULL,
//!     email TEXT NOT NULL,
//!     score INTEGER NOT NULL,
//!     active INTEGER NOT NULL
//!   )
//!
//! Row generation is deterministic so every bench run builds an identical
//! working set.

pub const CREATE_TABLE_SQL: &str = "\
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    score INTEGER NOT NULL,
    active INTEGER NOT NULL
)";

pub const DROP_TABLE_SQL: &str = "DROP TABLE IF EXISTS users";

#[derive(Debug, Clone)]
pub struct UserRow {
    pub id: i64,
    pub name: String,
    pub email: String,
    pub score: i32,
    pub active: bool,
}

pub fn make_row(i: usize) -> UserRow {
    UserRow {
        id: i as i64,
        name: format!("user_{i}"),
        email: format!("user_{i}@example.com"),
        score: (i as i32) % 100,
        active: i.is_multiple_of(2),
    }
}

pub fn make_rows(n: usize) -> Vec<UserRow> {
    (0..n).map(make_row).collect()
}
