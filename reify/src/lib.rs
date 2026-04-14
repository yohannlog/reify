//! # Reify
//!
//! > *"Define your database in Rust. Reify makes it real."*
//!
//! Zero CLI for schema. Zero magic strings. Full autocompletion.
//!
//! ## Quick start
//!
//! ```rust
//! use reify::Table;
//!
//! #[derive(Table, Debug, Clone)]
//! #[table(name = "users")]
//! pub struct User {
//!     #[column(primary_key, auto_increment)]
//!     pub id: i64,
//!     #[column(unique)]
//!     pub email: String,
//! }
//!
//! // Typed query builder with full autocompletion
//! let (sql, params) = User::find()
//!     .filter(User::email.eq("alice@example.com"))
//!     .limit(1)
//!     .build();
//! ```

// Re-export everything the user needs
pub use reify_core::tracing;
pub use reify_core::*;
pub use reify_macros::DbEnum;
pub use reify_macros::Relations;
pub use reify_macros::Table;
pub use reify_macros::View;

// Database adapters behind feature flags
#[cfg(feature = "postgres")]
pub use reify_postgres::{self, NoTls, PostgresDb, deadpool_postgres, tokio_postgres};

#[cfg(feature = "mysql")]
pub use reify_mysql::{self, MysqlDb, mysql_async};

#[cfg(feature = "sqlite")]
pub use reify_sqlite::{self, SqliteDb};
