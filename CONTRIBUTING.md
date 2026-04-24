# Contributing to Reify

Thank you for your interest in contributing to Reify! This document provides guidelines for contributing to the project.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/reify.git`
3. Create a branch for your changes: `git checkout -b feature/your-feature-name`

## Development Setup

```bash
# Build the workspace
cargo build

# Run tests
cargo test

# Run formatting and linting
./scripts/fmt.sh
```

## Code Style

- Follow the existing code style
- Run `cargo fmt --all` before committing
- Ensure `cargo clippy --all-targets --all-features` passes with no warnings
- Use meaningful variable names
- Add comments for complex logic

## Testing

- Add tests for new functionality
- Ensure all existing tests pass: `cargo test`
- Use `trybuild` for compile-time error tests when adding new macro features

### Unit vs integration tests

- **Unit tests** (SQL-generation, `MockDb`) live directly under
  `reify/tests/*.rs`. They are picked up automatically by the
  `unit-tests` / `coverage` CI jobs via `scripts/run-unit-tests.sh`.
  Adding a new file is enough — no workflow edit required.
- **Integration tests** (real database round-trips) live under
  `reify/tests/integration/` and are mounted by
  `reify/tests/integration.rs`. Each file is gated behind a
  per-adapter feature flag.

### Running integration tests

The integration suite is split into three sub-features so contributors
with only one database installed don't need to compile the others:

| Feature | Adapter | Needs Docker? |
|---|---|---|
| `sqlite-integration-tests` | SQLite (in-memory) | No |
| `pg-integration-tests` | PostgreSQL (`PG_URL`) | Yes |
| `mysql-integration-tests` | MySQL (`MYSQL_URL`) | Yes |
| `integration-tests` | All three | Yes (PG + MySQL) |

```bash
# SQLite only — fastest, no Docker:
cargo test -p reify --features sqlite-integration-tests --test integration

# PostgreSQL only (requires PG_URL):
PG_URL="postgres://reify:reify@localhost:5432/reify_test" \
  cargo test -p reify --features pg-integration-tests --test integration

# Full matrix (CI equivalent):
docker compose up -d
PG_URL="..." MYSQL_URL="..." \
  cargo test -p reify --features integration-tests --test integration -- --test-threads=1
```

`--test-threads=1` is used in CI as a belt-and-braces safety net
against PostgreSQL catalogue-lock contention during heavy parallel
DDL. Tables are also **prefixed per file** (`pg_basic_*`,
`pg_mig_*`, `mysql_basic_*`, …) so each file is isolated; the flag
can be omitted locally if you accept a marginal flake risk.

If `PG_URL` / `MYSQL_URL` are not set, the corresponding tests
**skip visibly** with a `SKIP: …` log line on stderr rather than
silently passing. The `integration-tests` CI job additionally
fails fast if either variable is empty.

## Pull Request Process

1. Update your fork with the latest changes from `main`
2. Run the full test suite: `cargo test`
3. Run formatting: `./scripts/fmt.sh`
4. Commit your changes with clear, descriptive messages
5. Push to your fork and open a pull request

### Commit Message Format

Use imperative mood and prefix with a category:

- `Add` - New features
- `Fix` - Bug fixes
- `Refactor` - Code restructuring
- `Docs` - Documentation changes
- `Test` - Test additions or changes

Example: `Add foreign key support for PostgreSQL`

## Code Review

- All PRs require review before merging
- Address review comments promptly
- Be respectful and constructive in discussions

## Reporting Issues

When reporting bugs, please include:
- Rust version (`rustc --version`)
- Database and version being used
- Steps to reproduce
- Expected vs actual behavior
- Error messages and stack traces (if applicable)

## Questions?

Feel free to open an issue for questions or join discussions in existing issues.

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.
