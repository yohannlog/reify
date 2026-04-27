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

# Run the unit-test sweep used by CI
bash scripts/run-unit-tests.sh

# Run integration tests with live PG + MySQL (uses docker-compose.yml)
docker compose up -d postgres mysql
PG_URL=postgres://reify:reify@localhost:5432/reify_test \
MYSQL_URL=mysql://reify:reify@localhost:3306/reify_test \
  cargo test -p reify --features integration-tests --test integration -- --test-threads=1
```

## Workspace Structure

```
reify/
├── reify-core/      # Traits, types, query builder, schema, pagination (no DB drivers)
├── reify-macros/    # Proc-macro crate: #[derive(Table)]
├── reify-postgres/  # PostgreSQL adapter (tokio-postgres)
├── reify-mysql/     # MySQL / MariaDB adapter (mysql_async)
├── reify-sqlite/    # SQLite adapter (rusqlite, blocking-task pool)
├── reify-cli/       # CLI tool (migration scaffolding, schema dump)
├── reify-bench/     # Criterion benchmarks
└── reify/           # Top-level crate: re-exports core + feature-gated adapters
```

- Edition: 2024, resolver v2
- Features on the `reify` crate:
  - DB adapters: `postgres`, `postgres18`, `mysql`, `sqlite` (additive — multiple may be enabled together)
  - DTO: `dto`, `dto-validation` (the latter implies `dto`)
  - Observability: `metrics`
  - Per-DB integration test sub-features: `pg-integration-tests`, `mysql-integration-tests`, `sqlite-integration-tests`, plus the aggregate `integration-tests`

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

### Query Builders (reify-core/src/query/)

Five builders with a fluent API: `SelectBuilder`, `InsertBuilder`, `InsertManyBuilder`, `UpdateBuilder`, `DeleteBuilder` (plus `WithBuilder` for eager-loading via `.with(relation)`). Safety:
- UPDATE and DELETE **panic** if neither `.filter()` nor `.unfiltered()` is set — no accidental bare updates/deletes. Use `.try_build()` for a non-panicking `Result<(String, Vec<Value>), BuildError>`.
- All builder types and `build*()` methods carry `#[must_use]`, so `User::update().filter(...);` (with stray semicolon, no execute) is a compiler warning, not a silent no-op.
- DELETE auto-promotes to a soft-delete UPDATE when the model has a `#[column(soft_delete)]` column; call `.force()` on `DeleteBuilder` for a hard DELETE.

### SQL Generation & Placeholders

All SQL uses `?` placeholders internally. The PostgreSQL adapter rewrites them to `$1, $2, …` at execution time via `rewrite_placeholders_pg`, which is **literal-aware** (a `?` inside a single-quoted string literal is preserved verbatim). MySQL/SQLite pass `?` directly.

The `Condition::raw(RawFragment::new(sql, params))` API forces `sql: &'static str`, so user input cannot reach the SQL string at the type level — only the bound `params` carry runtime data. This is the structural defense against SQL injection on raw fragments.

### Value Enum (reify-core/src/value.rs)

Feature-gated variants:
- `Uuid`, `Timestamptz`, `Jsonb`, `Cidr`, `Inet`, `Interval`, `MacAddr`, range types, array types — require `postgres`
- `Timestamp`, `Date`, `Time` — require `postgres` or `mysql`

### Proc Macro — #[derive(Table)] (reify-macros)

Generates from a struct:
1. `Table` trait impl
2. `Column<M, T>` constants for each field (as associated consts)
3. Query builder factory methods: `find()`, `insert()`, `update()`, `delete()`
4. Index definitions from `#[column(index)]` and `#[table(index(...))]` attributes

Attribute syntax:
- `#[table(name = "table_name")]` — SQL table name (required)
- `#[table(index(columns("col1", "col2"), unique, name = "...", predicate = "..."))]` — composite indexes
- `#[table(audit)]` — emit audit-trail boilerplate for the table
- `#[table(immutable)]` — refuse to generate `update()` / `delete()` factories (Hibernate-style `@Immutable`)
- `#[table(sql_delete | sql_update | sql_insert = "...")]` — Hibernate-style custom SQL overrides
- `#[table(dto(skip = "f1,f2", derives(Serialize, Deserialize, ...)))]` — DTO field exclusion + extra derives
- `#[column(primary_key, auto_increment)]` — PK config (each option is rejected if duplicated; a nullable PK is rejected at compile time)
- `#[column(unique, index, name = "db_col", default = "...", check = "...")]` — column constraints
- `#[column(creation_timestamp, update_timestamp, source = "db" | "vm")]` — timestamp automation
- `#[column(soft_delete)]` — mark this `Option<DateTime>` column as the soft-delete sentinel
- `#[column(references = "users(id)", on_delete = "...", on_update = "...")]` — FK config
- `#[column(validate(email | length(...) | range(...) | regex(...) | required | ...))]` — `dto-validation` rules

### Database Adapters

All three adapters implement the `Database` trait with async execution, row conversion, and transaction support (with savepoints for nested `transaction()`).

- **reify-postgres**: wraps `tokio-postgres`, converts `Value` to a `PgValue` wrapper, rewrites placeholders, supports COPY, range types, jsonb, UUID v7 (PG 18+).
- **reify-mysql**: wraps `mysql_async`, converts `Value` via `value_to_mysql`, uses pooled connections. Temporal column conversion is gated on the column's declared type — a VARCHAR containing `"2024-01-15"` is preserved as `Value::String`, not silently coerced to `Value::Date`.
- **reify-sqlite**: wraps `rusqlite` via `tokio::task::spawn_blocking`. WAL + pragma config exposed at connect time. Note: do not call methods on the outer `SqliteDb` from inside a `transaction()` closure — use the `tx: &dyn DynDatabase` argument; the constraint is documented but not yet type-enforced.

### Pagination (reify-core/src/paginate.rs)

Two modes:
- **Offset-based**: `.paginate(page, per_page)` → returns `Paginated<M>` with `PageInfo` (has_next, has_prev, total_pages)
- **Cursor-based**: `.after(column, value, limit)` / `.before()` → returns `CursorPaginated<M>` with `has_more`

### DTO Generation (feature: `dto`, `dto-validation`)

Two opt-in features on the `reify` crate:

- **`dto`** — `#[derive(Table)]` automatically generates a `{Name}Dto` struct that excludes auto-increment primary keys and timestamp fields. Use `#[table(dto(skip = "field1,field2"))]` to skip additional fields.
- **`dto-validation`** — Extends DTOs with `#[derive(validator::Validate)]`. Add validation rules via `#[column(validate(email))]`, `#[column(validate(length(min = 1, max = 255)))]`, etc. The `validator` crate is re-exported automatically.

Generated DTO provides:
- `{Name}Dto::column_names()` — column names for the DTO fields
- `dto.into_values()` — convert to `Vec<Value>` for query building

## Migrations (reify-core/src/migration/)

`MigrationRunner` runs both auto-diffed migrations (CREATE TABLE / ADD COLUMN derived from `Schema`) and hand-written `Migration` impls in version order, recorded in a `_reify_migrations` table. See `migration_tests.rs` and `pg_migrations.rs` / `mysql_migrations.rs` / `sqlite_migrations.rs` integration tests for usage patterns.

## Audit / RLS (reify-core/src/{audit,rls}.rs)

- `audit.rs` — HMAC-chained audit trails (`audited_insert`/`update`/`delete`). The `ZeroOnDrop` wrapper zeroizes secrets on drop with `write_volatile + compiler_fence`.
- `rls.rs` — `Scoped<T>` and `RlsContext` for row-level-security style scoping. `scoped_fetch`, `scoped_update`, `scoped_delete` enforce a policy decision per query.

## Design Principles

1. **IDE-first**: all column references are typed constants — rust-analyzer autocompletes everything
2. **Code is source of truth**: structs define tables, no external schema files
3. **Compile-time safety**: renaming a field breaks compilation wherever it's used; the macro rejects nullable PKs, duplicate `#[column(...)]` options, and unknown attributes at compile time
4. **Safe by default**: UPDATE/DELETE require filters; builders are `#[must_use]`; SQL injection is structurally defended via `&'static str` raw fragments

## CI

`.github/workflows/ci.yml` runs: `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo doc -D warnings`, multi-OS build matrix, the unit-test sweep (`scripts/run-unit-tests.sh`), trybuild compile-fail tests (`builder_type_safety`), tarpaulin coverage, and integration tests against live PostgreSQL 16 + MySQL 8 services. SQLite integration tests run as part of the same `integration-tests` aggregate (no service required). Supply-chain rules live in `deny.toml` (run `cargo deny check` locally — not yet wired into CI).

<!-- code-review-graph MCP tools -->
## MCP Tools: code-review-graph

**IMPORTANT: This project has a knowledge graph. ALWAYS use the
code-review-graph MCP tools BEFORE using Grep/Glob/Read to explore
the codebase.** The graph is faster, cheaper (fewer tokens), and gives
you structural context (callers, dependents, test coverage) that file
scanning cannot.

### When to use graph tools FIRST

- **Exploring code**: `semantic_search_nodes` or `query_graph` instead of Grep
- **Understanding impact**: `get_impact_radius` instead of manually tracing imports
- **Code review**: `detect_changes` + `get_review_context` instead of reading entire files
- **Finding relationships**: `query_graph` with callers_of/callees_of/imports_of/tests_for
- **Architecture questions**: `get_architecture_overview` + `list_communities`

Fall back to Grep/Glob/Read **only** when the graph doesn't cover what you need.

### Key Tools

| Tool | Use when |
|------|----------|
| `detect_changes` | Reviewing code changes — gives risk-scored analysis |
| `get_review_context` | Need source snippets for review — token-efficient |
| `get_impact_radius` | Understanding blast radius of a change |
| `get_affected_flows` | Finding which execution paths are impacted |
| `query_graph` | Tracing callers, callees, imports, tests, dependencies |
| `semantic_search_nodes` | Finding functions/classes by name or keyword |
| `get_architecture_overview` | Understanding high-level codebase structure |
| `refactor_tool` | Planning renames, finding dead code |

### Workflow

1. The graph auto-updates on file changes (via hooks).
2. Use `detect_changes` for code review.
3. Use `get_affected_flows` to understand impact.
4. Use `query_graph` pattern="tests_for" to check coverage.
