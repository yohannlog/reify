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
- For database-specific features, add integration tests in `reify/tests/` (all
  integration tests live at the top level — there is no `integration/`
  subdirectory).
- Use `trybuild` for compile-time error tests when adding new macro features

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
