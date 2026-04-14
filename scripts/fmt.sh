#!/usr/bin/env bash
# fmt.sh — Format and lint the workspace before pushing.
# Usage: ./scripts/fmt.sh
# Run this before every `git push` to ensure CI passes.

set -euo pipefail

echo "==> cargo fmt"
cargo fmt --all

echo "==> cargo clippy"
cargo clippy --all-targets --all-features

echo ""
echo "All checks passed. Ready to push."
