//! One submodule per framework. Each exports:
//!   async fn run(scenario: Scenario, rows: usize, iters: usize) -> Duration
//!
//! All scenarios share the same schema and workload shape so numbers are
//! directly comparable.

pub mod model;
pub mod reify;
pub mod rusqlite_raw;

#[cfg(feature = "comparative")]
pub mod diesel;
#[cfg(feature = "comparative")]
pub mod seaorm;
#[cfg(feature = "comparative")]
pub mod sqlx_raw;
