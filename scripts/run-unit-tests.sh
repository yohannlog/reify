#!/usr/bin/env bash
#
# Run all unit-test binaries for the top-level `reify` crate.
#
# The CI `unit-tests` / `coverage` jobs were previously hardcoding
# each `--test <name>` argument, which meant that adding a new file
# under `reify/tests/` required manually editing the workflow — and
# silently skipped any file the contributor forgot. This script is
# the single source of truth the workflow now invokes.
#
# Exclusions:
#   - `builder_type_safety` runs in the dedicated `compile-fail`
#     job via `trybuild`; it is slow and needs a fresh target dir.
#   - `integration` requires a live PostgreSQL + MySQL and runs in
#     the `integration-tests` job under the `integration-tests` feature.
#
# Extra arguments are forwarded to `cargo test` (e.g. `--no-run`,
# `-- --nocapture`, or a test filter).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEST_DIR="${PROJECT_ROOT}/reify/tests"

# Discover every `*.rs` file directly under `reify/tests/` (but not
# sub-directories like `integration/` or `compile_fail/`). The
# matching test name is the file stem (cargo convention).
tests=()
for f in "${TEST_DIR}"/*.rs; do
    name="$(basename "${f}" .rs)"
    case "${name}" in
        builder_type_safety|integration|timestamps)
            # - builder_type_safety: compile-fail job
            # - integration: integration-tests job
            # - timestamps: excluded in the previous CI (unstable
            #   features referenced in the file); keep behaviour.
            continue
            ;;
        *)
            tests+=("--test" "${name}")
            ;;
    esac
done

if [[ "${#tests[@]}" -eq 0 ]]; then
    echo "No tests discovered under ${TEST_DIR}" >&2
    exit 1
fi

echo "Running unit tests: ${tests[*]}"
cd "${PROJECT_ROOT}"
exec cargo test -p reify "${tests[@]}" "$@"
