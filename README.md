# Reify

[![CI](https://github.com/yohannlog/reify/actions/workflows/ci.yml/badge.svg)](https://github.com/yohann-catherine/reify/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/reify.svg)](https://crates.io/crates/reify)
[![Docs.rs](https://docs.rs/reify/badge.svg)](https://docs.rs/reify)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

> Define your database in Rust. Reify makes it real.

Reify is a type-safe Rust ORM with compile-time checked column references, fluent query builder, and zero magic strings.

## Features

- **Type-safe queries**: Column references are typed constants — your IDE autocompletes everything
- **Compile-time safety**: Renaming a field breaks compilation wherever it's used
- **Fluent API**: Chain methods to build complex queries
- **Multiple backends**: PostgreSQL, MySQL/MariaDB, SQLite support
- **DTO generation**: Automatic DTO structs with optional validation
- **Pagination**: Offset-based and cursor-based pagination built-in

## Quick Start

```rust
use reify::Table;

#[derive(Table)]
#[table(name = "users")]
struct User {
    #[column(primary_key, auto_increment)]
    id: i64,
    #[column(unique)]
    email: String,
    name: String,
}

// Query with type-safe column references
let users = User::find()
.filter(User::email.eq("alice@example.com"))
.all( & db)
.await?;
```

## Installation

```toml
[dependencies]
reify = { version = "0.1", features = ["postgres"] }
```

## Features Flags

- `postgres` — PostgreSQL adapter (tokio-postgres)
- `mysql` — MySQL/MariaDB adapter (mysql_async)
- `sqlite` — SQLite adapter (rusqlite)
- `dto` — Automatic DTO generation
- `dto-validation` — DTO validation with the `validator` crate

## Safety

- UPDATE and DELETE builders **panic** if no `.filter()` is set — no accidental bare updates/deletes
- SQL injection protection via parameterized queries

## License

This project is licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))

at your option.
