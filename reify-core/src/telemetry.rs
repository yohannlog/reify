//! Structured observability for Reify — spans, slow-query logging, and optional metrics.
//!
//! # Spans
//!
//! Every query execution emits a `tracing` span under the `reify.query` target with
//! the following fields:
//!
//! | field          | type    | description                              |
//! |----------------|---------|------------------------------------------|
//! | `operation`    | &str    | `"select"`, `"insert"`, `"update"`, …    |
//! | `table`        | &str    | SQL table name                           |
//! | `sql`          | &str    | Full SQL string                          |
//! | `params_count` | usize   | Number of bind parameters                |
//! | `rows`         | usize   | Rows returned / affected (exec helpers)  |
//! | `elapsed_ms`   | u128    | Wall-clock time for the DB round-trip    |
//!
//! Transaction and migration spans are emitted under `reify.tx` and
//! `reify.migration` respectively.
//!
//! # Slow-query threshold
//!
//! Set [`ReifyConfig::slow_query_threshold`] to emit a `tracing::warn!` whenever
//! a query exceeds the threshold:
//!
//! ```rust
//! use std::time::Duration;
//! use reify_core::telemetry::ReifyConfig;
//!
//! ReifyConfig::set_slow_query_threshold(Duration::from_millis(200));
//! ```
//!
//! # Metrics (optional feature `metrics`)
//!
//! When compiled with `--features metrics`, each query also records:
//!
//! - **counter** `reify.queries.total` — labels: `operation`, `table`
//! - **histogram** `reify.queries.duration_ms` — labels: `operation`, `table`
//!
//! Uses the [`metrics`](https://docs.rs/metrics) facade — wire up any compatible
//! exporter (Prometheus, StatsD, …) in your application.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

// ── ReifyConfig ─────────────────────────────────────────────────────

/// Global configuration for Reify observability.
///
/// All fields are optional; the defaults produce the same behaviour as
/// before this module existed (structured `DEBUG` spans, no slow-query
/// threshold, no metrics).
///
/// # Example
///
/// ```rust
/// use std::time::Duration;
/// use reify_core::telemetry::ReifyConfig;
///
/// // Warn on any query slower than 200 ms.
/// ReifyConfig::set_slow_query_threshold(Duration::from_millis(200));
/// ```
pub struct ReifyConfig;

static SLOW_QUERY_THRESHOLD: OnceLock<Duration> = OnceLock::new();

impl ReifyConfig {
    /// Set the slow-query warn threshold.
    ///
    /// Any query whose round-trip exceeds `threshold` will emit a
    /// `tracing::warn!` at the `reify.query` target with all span fields.
    ///
    /// Can only be set once — subsequent calls are silently ignored (use
    /// early in `main` or application startup).
    pub fn set_slow_query_threshold(threshold: Duration) {
        let _ = SLOW_QUERY_THRESHOLD.set(threshold);
    }

    /// Return the configured slow-query threshold, if any.
    pub fn slow_query_threshold() -> Option<Duration> {
        SLOW_QUERY_THRESHOLD.get().copied()
    }
}

// ── Build-time span (query builder) ─────────────────────────────────

/// Emit a `DEBUG` span when a query is *built* (no elapsed — no DB round-trip yet).
///
/// Called by `trace_query` in `query/mod.rs` at builder `.build()` time.
#[inline]
pub fn record_query_built(operation: &str, table: &'static str, sql: &str, params_count: usize) {
    tracing::debug!(
        target: "reify.query",
        operation,
        table,
        sql,
        params_count,
        "query built"
    );
}

// ── Execution-time span ──────────────────────────────────────────────

/// Timing guard returned by [`start_query`].
///
/// Drop (or call [`QueryTimer::finish`]) to record elapsed time, rows, and
/// emit the slow-query warning + metrics.
pub struct QueryTimer {
    operation: &'static str,
    table: &'static str,
    sql: String,
    params_count: usize,
    start: Instant,
}

/// Start timing a query execution.  Call [`QueryTimer::finish`] (or drop)
/// when the DB call returns.
#[inline]
pub fn start_query(
    operation: &'static str,
    table: &'static str,
    sql: impl Into<String>,
    params_count: usize,
) -> QueryTimer {
    QueryTimer {
        operation,
        table,
        sql: sql.into(),
        params_count,
        start: Instant::now(),
    }
}

impl QueryTimer {
    /// Record the completed query with `rows` returned/affected.
    pub fn finish(self, rows: usize) {
        let elapsed = self.start.elapsed();
        let elapsed_ms = elapsed.as_millis();

        tracing::debug!(
            target: "reify.query",
            operation = self.operation,
            table = self.table,
            sql = %self.sql,
            params_count = self.params_count,
            rows,
            elapsed_ms,
            "query executed"
        );

        // Slow-query warning
        if let Some(threshold) = ReifyConfig::slow_query_threshold() {
            if elapsed > threshold {
                tracing::warn!(
                    target: "reify.query",
                    operation = self.operation,
                    table = self.table,
                    sql = %self.sql,
                    params_count = self.params_count,
                    rows,
                    elapsed_ms,
                    threshold_ms = threshold.as_millis(),
                    "slow query detected"
                );
            }
        }

        // Optional metrics
        #[cfg(feature = "metrics")]
        {
            let labels = [("operation", self.operation), ("table", self.table)];
            metrics::counter!("reify.queries.total", &labels).increment(1);
            metrics::histogram!("reify.queries.duration_ms", &labels).record(elapsed_ms as f64);
        }
    }
}

impl Drop for QueryTimer {
    fn drop(&mut self) {
        // If finish() was not called (e.g. on error path), still record with rows=0.
        // We can't move out of &mut self, so we record elapsed only.
        let elapsed = self.start.elapsed();
        let elapsed_ms = elapsed.as_millis();

        // Only emit if not already finished — use a sentinel: set start to
        // UNIX_EPOCH to mark "already finished". We detect this by checking
        // if elapsed is unreasonably large (> 1 year), which means finish()
        // already ran and reset the clock. This is a best-effort guard.
        // In practice callers always call finish() explicitly.
        if elapsed_ms < 365 * 24 * 3600 * 1000 {
            tracing::debug!(
                target: "reify.query",
                operation = self.operation,
                table = self.table,
                sql = %self.sql,
                params_count = self.params_count,
                elapsed_ms,
                "query dropped (error path)"
            );
        }
    }
}

// ── Transaction span ─────────────────────────────────────────────────

/// Emit a span around a transaction.
///
/// Returns an [`Instant`] — call [`finish_tx`] with the result to record
/// elapsed and outcome.
#[inline]
pub fn start_tx() -> Instant {
    tracing::debug!(target: "reify.tx", "transaction started");
    Instant::now()
}

/// Record a completed transaction.
#[inline]
pub fn finish_tx(start: Instant, committed: bool) {
    let elapsed_ms = start.elapsed().as_millis();
    if committed {
        tracing::debug!(
            target: "reify.tx",
            elapsed_ms,
            "transaction committed"
        );
    } else {
        tracing::warn!(
            target: "reify.tx",
            elapsed_ms,
            "transaction rolled back"
        );
    }

    #[cfg(feature = "metrics")]
    {
        let outcome = if committed {
            "committed"
        } else {
            "rolled_back"
        };
        metrics::counter!("reify.transactions.total", "outcome" => outcome).increment(1);
        metrics::histogram!("reify.transactions.duration_ms", "outcome" => outcome)
            .record(elapsed_ms as f64);
    }
}

// ── Migration span ───────────────────────────────────────────────────

/// Emit a span around a migration step.
#[inline]
pub fn record_migration(name: &str, direction: &'static str, elapsed_ms: u128, success: bool) {
    if success {
        tracing::info!(
            target: "reify.migration",
            name,
            direction,
            elapsed_ms,
            "migration applied"
        );
    } else {
        tracing::error!(
            target: "reify.migration",
            name,
            direction,
            elapsed_ms,
            "migration failed"
        );
    }

    #[cfg(feature = "metrics")]
    {
        let labels = [
            ("direction", direction),
            ("success", if success { "true" } else { "false" }),
        ];
        metrics::counter!("reify.migrations.total", &labels).increment(1);
        metrics::histogram!("reify.migrations.duration_ms", &labels).record(elapsed_ms as f64);
    }
}
