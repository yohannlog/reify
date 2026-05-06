#!/usr/bin/env bash
#
# Seed GitHub issues from ROADMAP.md.
#
# Iterates over the open (⚪) roadmap entries and creates one issue per
# entry via `gh issue create`. Re-running the script is safe: an entry
# whose title already matches an *open* issue is skipped.
#
# Usage:
#   bash scripts/seed-issues.sh              # create issues
#   bash scripts/seed-issues.sh --dry-run    # list what would be created
#
# Requirements:
#   - gh CLI authenticated against the repo (`gh auth status`).
#   - Run from the repo root (or anywhere — the script `cd`s itself).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=1
fi

if (( DRY_RUN == 0 )); then
    if ! command -v gh >/dev/null 2>&1; then
        echo "::error:: gh CLI not found. Install via https://cli.github.com or run with --dry-run." >&2
        exit 1
    fi
    if ! gh auth status >/dev/null 2>&1; then
        echo "::error:: gh CLI not authenticated. Run \`gh auth login\`." >&2
        exit 1
    fi
fi

# ── Issue specs ──────────────────────────────────────────────────────
#
# One row per open (⚪) ROADMAP entry. Format:
#
#   id|labels|title|body
#
# `body` is a single line; insert literal `\n` for line breaks (the
# script expands them via printf %b). Labels are comma-separated.
#
# IDs match the ROADMAP table (S-4, P-1, …) so a contributor reading
# the roadmap can map an issue back to its row in one step.

ISSUES=(
"S-4|security,P0|[S-4] Trait \`AuditSink\` + backends append-only externes|Implement the \`AuditSink\` trait outlined in TODO_SECURITY.md §4 with at least these backends gated behind features:\n\n- \`audit-file\` (local append-only file)\n- \`audit-http\` (webhook)\n- \`audit-s3\` (S3 + Object Lock / WORM)\n- \`audit-kafka\` (Kafka / SQS)\n\nRequired for SOC2 / PCI-DSS compliance. The sink write must be atomic with the audit transaction (rollback on sink failure)."

"S-5|security,testing,P2|[S-5] Tests d'intégration live PG + MySQL pour la chaîne audit|Add an integration test that:\n\n1. Inserts/updates/deletes through \`audited_*\` so a chain of N audit rows is built.\n2. Runs \`verify_audit_chain\` and asserts every row returns \`AuditChainCheck::Ok\`.\n3. Manually deletes one audit row and re-runs \`verify_audit_chain\`; asserts \`BrokenChain\` on the row that followed the deleted one.\n\nGated behind \`pg-integration-tests\` and \`mysql-integration-tests\`."

"P-1|performance,P1|[P-1] \`qi()\` retourne \`Cow<'_, str>\`|Replace \`fn qi(&str) -> String\` with \`fn qi(&str) -> Cow<'_, str>\` in \`reify-core/src/ident.rs\`. Borrow when the identifier is already a safe lowercase snake_case non-keyword; allocate only when quoting/escaping is required.\n\nReference impl + keyword list in TODO_FINAL1.md §7.1. Expected impact: −30/40 % allocations on large INSERTs."

"P-2|performance,P0|[P-2] Cache de \`Statement\` préparés (PostgreSQL)|Add a \`StatementCache\` (DashMap hot path + LRU cold path) to \`reify-postgres\`. Hash on the SQL template (\`?\`/\`\$N\` normalized).\n\nTarget API: \`PostgresDb::execute_cached\`, \`query_cached\`. Reference design in TODO_FINAL1.md §4.1.A and §7.2. Expected impact: −20/50 % latency on repeated queries."

"P-3|performance,feature,P1|[P-3] Protocole \`COPY FROM STDIN BINARY\` pour bulk insert|Implement \`PostgresDb::copy_in::<T: Table>(models: &[T])\` using \`tokio_postgres::binary_copy::BinaryCopyInWriter\`.\n\nWire it as the default backend for \`InsertManyBuilder::execute\` once row count crosses a threshold (e.g. 1000). Reference design in TODO_FINAL1.md §4.2.D. Expected impact: 10-100× on bulk insert ≥ 10k rows."

"P-4|performance,P2|[P-4] Mode pipeline / batch tokio-postgres|Expose \`PostgresDb::execute_pipeline(queries: &[BuiltQuery])\` for migrations and bulk-op throughput. Reference: TODO_FINAL1.md §4.2.C."

"P-5|performance,P2|[P-5] Optimiser \`Row::get()\`|Replace the lazy \`HashMap<String,usize>\` per row in \`reify-core/src/db.rs\` with a compact \`Vec<(u64, usize)>\` index (FNV-hashed column name → position).\n\nOption B — document and prefer \`get_idx\` for hot paths. Reference: TODO_FINAL1.md §3.2."

"P-6|performance,P3|[P-6] Éviter la double réécriture de placeholders sur \`build_chunked_pg\`|In \`InsertManyBuilder::build_chunked_pg\`, rewrite \`?\`→\`\$N\` once on the chunk template and reuse with offset for subsequent chunks instead of scanning every chunk's full SQL. Reference: TODO_FINAL1.md §3.4."

"P-7|performance,P3|[P-7] \`bytecount\` SIMD pour \`bytecount_question_marks\`|Replace the iterator-filter implementation with the \`bytecount\` crate's SIMD-accelerated \`count(bytes, b'?')\`. Optional dep (\`bytecount = { version = \"0.6\", optional = true }\`). Reference: TODO_FINAL1.md §4.3.E."

"P-8|performance,P3|[P-8] Vrai async stream pour SELECT|Wire \`PostgresDb::query_stream\` to \`tokio_postgres::Client::query_raw\` instead of buffering through a Vec. Reference: TODO_FINAL1.md §5.4. Expected impact: -50 % memory on massive result sets."

"V-1|security,testing,P2|[V-1] Durcir le rewriter \`?\`→\`\$N\` contre les dollar-quoted strings|Confirm via tests that \`?\` characters inside PostgreSQL dollar-quoted strings (\`\$tag\$ ... \$tag\$\`) are preserved verbatim. Add tests next to \`pg_rewrite_*\` in \`reify-core/src/query/mod.rs\`. Reference: TODO_FINAL1.md §6."

"V-2|security,testing,P2|[V-2] Durcir le rewriter \`?\`→\`\$N\` contre les commentaires SQL|Confirm via tests that \`?\` characters inside line comments (\`-- ...\`) and block comments (\`/* ... */\`) are preserved verbatim. Reference: TODO_FINAL1.md §6."

"Q-3|refactor,P1|[Q-3] Factoriser duplication PG ↔ MySQL|\`get_conn\`, \`acquire_timeout\` wrapper, \`query_stream_idle\` and \`with_idle_timeout\` are verbatim duplicated between \`reify-postgres\` and \`reify-mysql\` (~100 LOC × 2). Extract a shared \`reify-adapter-common\` (or a helper module in \`reify-core\`) and have both adapters depend on it. Reference: revue projet §4."

"Q-4|robustness,P2|[Q-4] Remplacer panics résiduels par \`Result\`|Convert \`panic!\` to a typed \`Result\`/\`Err\` in:\n\n- \`reify-core/src/view.rs:89-90\`\n- \`reify-core/src/paginate.rs:93\`\n- \`reify-core/src/query/insert.rs:350\` (\`build_with_dialect\`)\n- \`reify-core/src/migration/ddl.rs:250\` (\`add_column_sql\`)\n\nReference: revue projet §5."

"Q-5|api,P2|[Q-5] \`FromValue::Err\` typé|Replace the \`Result<T, String>\` returned by \`FromValue\` with \`Result<T, FromValueError>\` where \`FromValueError\` is an enum (\`Missing { column }\` / \`TypeMismatch { expected, found }\` / \`Conversion(String)\`). Update all impls in \`reify-core/src/value.rs\` and downstream adapters. Reference: revue projet §6."

"Q-6|refactor,P3|[Q-6] Décomposer \`migration/mod.rs\`|\`reify-core/src/migration/mod.rs\` is 1946 LOC. Split into cohesive sub-modules (e.g. \`schema_diff\`, \`tracking\`, \`codegen\`). Reference: revue projet §7."

"Q-7|quality,P3|[Q-7] \`[workspace.lints]\` global + doctests audit/migration/column|Define a workspace-level \`[lints]\` table to enforce \`unsafe_code = forbid\` (where applicable) and other correctness lints. Add module-level doctests to \`audit.rs\`, \`migration/mod.rs\`, \`column.rs\`. Reference: revue projet §9."

"Q-8|api,P3|[Q-8] Type-safe \`RETURNING\`|Add \`returning_cols(&[Column<M, T>])\` on \`InsertBuilder\`/\`UpdateBuilder\`/\`DeleteBuilder\` mirroring the existing string-based \`returning(&[\"col\"])\`. Reference: TODO_FINAL1.md §5.1."

"Q-9|api,P3|[Q-9] \`EXPLAIN [ANALYZE]\` ergonomique sur les builders|Add \`SelectBuilder::explain(&db, format)\` and \`explain_analyze(&db)\` returning a parsed plan / execution-time struct. Reference: TODO_FINAL1.md §5.2."

"Q-10|api,P3|[Q-10] Options publiques de pool tuning|Expose \`pool_max_size\`, \`pool_timeout\`, \`pool_recycle_interval\`, \`pool_max_lifetime\` on \`PostgresConfig\`/\`MysqlConfig\`. Reference: TODO_FINAL1.md §5.3."

"D-1|docs,P3|[D-1] Fusionner GEMINI.md ↔ CLAUDE.md|\`GEMINI.md\` is a near-duplicate of \`CLAUDE.md\`. Pick one, add a one-line redirect in the other. Reference: revue projet §10."

"D-2|docs,P3|[D-2] Exemple bulk insert / COPY|Add \`reify/examples/bulk_insert.rs\` demonstrating \`InsertManyBuilder\` chunking and (once P-3 lands) the COPY backend. Reference: revue projet §10."
)

# ── Helpers ──────────────────────────────────────────────────────────

# Cache the list of open issue titles once to avoid one `gh issue list`
# per spec (rate-limit friendly on large roadmaps).
EXISTING_TITLES=""
if (( DRY_RUN == 0 )); then
    EXISTING_TITLES="$(gh issue list --state open --limit 500 --json title --jq '.[].title' || true)"
fi

# ── Main loop ────────────────────────────────────────────────────────

created=0
skipped=0
for spec in "${ISSUES[@]}"; do
    IFS='|' read -r id labels title body_raw <<< "${spec}"
    body="$(printf '%b' "${body_raw}")"

    if [[ "${EXISTING_TITLES}" == *"${title}"* ]]; then
        echo "skip   ${id}: open issue with same title already exists"
        skipped=$((skipped + 1))
        continue
    fi

    if (( DRY_RUN == 1 )); then
        printf 'would create  %s  [%s]  %s\n' "${id}" "${labels}" "${title}"
        created=$((created + 1))
        continue
    fi

    # `gh` will create labels on the fly if the user has perms; if
    # labels are missing and the user lacks admin, the call falls back
    # gracefully via --label.
    if gh issue create \
        --title "${title}" \
        --body "${body}" \
        --label "${labels}" >/dev/null; then
        echo "create ${id}: ok"
        created=$((created + 1))
    else
        echo "::error:: failed to create issue ${id}: ${title}" >&2
    fi
done

echo
if (( DRY_RUN == 1 )); then
    echo "dry-run: would create ${created}, skipped ${skipped}"
else
    echo "done: created ${created}, skipped ${skipped}"
fi
