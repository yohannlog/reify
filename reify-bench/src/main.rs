//! reify-bench — comparative ORM benchmarks (Reify vs Diesel, SeaORM, sqlx).
//!
//! Run via:
//!   cargo run -p reify-bench --release -- --help
//!
//! Or from the CLI:
//!   reify bench               # run all benchmarks
//!   reify bench --rows 10000  # larger workload
//!   reify bench --only reify  # a single framework
//!   reify bench --scenario insert
//!
//! Comparative frameworks (Diesel, SeaORM, sqlx) are gated behind the
//! `comparative` cargo feature — the baseline (Reify + raw rusqlite) is
//! always available so that `reify bench` can be invoked in any environment.

use std::time::{Duration, Instant};

mod runner;
mod scenarios;

pub use runner::{BenchConfig, BenchResult, Framework, Scenario, run_all};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match runner::BenchConfig::parse(&args) {
        Ok(cfg) => {
            if cfg.help {
                print_help();
                return;
            }
            let results = runner::run_all(&cfg).await;
            runner::print_table(&cfg, &results);
        }
        Err(msg) => {
            eprintln!("error: {msg}");
            eprintln!();
            print_help();
            std::process::exit(2);
        }
    }
}

fn print_help() {
    println!(
        "reify-bench — comparative ORM benchmarks

USAGE:
    reify-bench [OPTIONS]

OPTIONS:
    --rows <N>          Number of rows per scenario (default: 1000)
    --iters <N>         Number of iterations per measurement (default: 5)
    --only <frameworks> Comma-separated: reify,rusqlite,diesel,seaorm,sqlx
    --scenario <name>   Run only this scenario (insert|insert_batch|select_all|select_by_pk|update|delete)
    --json              Emit JSON instead of a table
    -h, --help          Show this help
"
    );
}

/// Small helper: measure the wall time of an async block across `iters` runs
/// and return the median.
pub async fn time_iters<F, Fut>(iters: usize, mut f: F) -> Duration
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut samples: Vec<Duration> = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        f().await;
        samples.push(t0.elapsed());
    }
    samples.sort();
    samples[samples.len() / 2]
}
