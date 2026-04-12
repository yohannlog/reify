# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Reify is a type-safe Rust ORM library: "Define your database in Rust. Reify makes it real." It provides compile-time checked column references, a fluent query builder, proc-macro code generation, and database adapters — with zero magic strings.

## Commands

```bash
# Build the entire workspace
cargo build

# Run all tests (integration tests live in reify/tests/)
cargo test

# Run a single test
cargo test <test_name>

# Run a specific crate's tests
cargo test -p reify

# Run examples
cargo run --example basic
cargo run --example pagination
cargo run --example schema_builder
cargo run --example indexes

# Check without building
cargo check
```

## Workspace Structure

```
reify/
├── reify-core/      # Traits, types, query builder, schema, pagination (no DB drivers)
├── reify-macros/    # Proc-macro crate: #[derive(Table)]
├── reify-postgres/  # PostgreSQL adapter (tokio-postgres)
├── reify-mysql/     # MySQL/MariaDB adapter (mysql_async)
└── reify/           # Top-level crate: re-exports core + feature-gated adapters
```

- Edition: 2024, resolver v2
- Features on the `reify` crate: `postgres` (enables reify-postgres), `mysql` (enables reify-mysql)

## Architecture

### Core Trait System (reify-core)

- **`Table`** — implemented by `#[derive(Table)]`: provides `table_name()`, `column_names()`, `into_values()`, `indexes()`
- **`Database`** — async trait for adapters: `execute`, `query`, `query_one`, `transaction`
- **`FromRow`** — converts a `Row` into a model struct
- **`ToSql`** — generates SQL fragments with parameterized placeholders
- **`IntoValue`** — converts Rust types into the `Value` enum
- **`Schema`** — fluent schema builder for programmatic table definitions

### Column Type Safety

Every struct field generates a `Column<Model, Type>` constant. Methods on `Column` are type-gated:
- String columns: `.eq()`, `.like()`, `.contains()`, `.starts_with()`, `.ends_with()`
- Numeric columns: `.eq()`, `.gt()`, `.lt()`, `.gte()`, `.lte()`, `.between()`
- Option columns: `.is_null()`, `.is_not_null()`
- Temporal columns: `.before()`, `.after()`, `.between()`
- PostgreSQL: `.ilike()` (feature-gated)

### Query Builders (reify-core/src/query.rs)

Four builders with fluent API: `SelectBuilder`, `InsertBuilder`, `UpdateBuilder`, `DeleteBuilder`. Safety: UPDATE and DELETE **panic** if no `.filter()` is set — no accidental bare updates/deletes.

### SQL Generation & Placeholders

All SQL uses `?` placeholders internally. The PostgreSQL adapter rewrites them to `$1, $2, ...` at execution time. MySQL passes `?` directly.

### Value Enum (reify-core/src/value.rs)

Feature-gated variants: `Uuid`, `Timestamptz`, `Jsonb` require `postgres` feature. `Timestamp`, `Date`, `Time` require `postgres` or `mysql`.

### Proc Macro — #[derive(Table)] (reify-macros)

Generates from a struct:
1. `Table` trait impl
2. `Column<M, T>` constants for each field (as associated consts)
3. Query builder factory methods: `find()`, `insert()`, `update()`, `delete()`
4. Index definitions from `#[column(index)]` and `#[table(index(...))]` attributes

Attribute syntax:
- `#[table(name = "table_name")]` — SQL table name
- `#[table(index(columns("col1", "col2"), unique))]` — composite indexes
- `#[column(primary_key, auto_increment)]` — PK config
- `#[column(unique, index, nullable)]` — column constraints

### Database Adapters

Both adapters implement the `Database` trait with async execution, row conversion, and transaction support.

- **reify-postgres**: wraps `tokio-postgres`, converts `Value` to `PgValue` wrapper, rewrites placeholders
- **reify-mysql**: wraps `mysql_async`, converts `Value` via `value_to_mysql`, uses connection pooling

### Pagination (reify-core/src/paginate.rs)

Two modes:
- **Offset-based**: `.paginate(page, per_page)` → returns `Paginated<M>` with `PageInfo` (has_next, has_prev, total_pages)
- **Cursor-based**: `.after(column, value, limit)` / `.before()` → returns `CursorPaginated<M>` with `has_more`

## Design Principles

1. **IDE-first**: all column references are typed constants — rust-analyzer autocompletes everything
2. **Code is source of truth**: structs define tables, no external schema files
3. **Compile-time safety**: renaming a field breaks compilation wherever it's used
4. **Safe by default**: UPDATE/DELETE require filters; no accidental data loss
