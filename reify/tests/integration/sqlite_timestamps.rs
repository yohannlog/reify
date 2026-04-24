//! Placeholder: VM-source `#[column(creation_timestamp)]` /
//! `#[column(update_timestamp)]` cannot be covered against SQLite.
//!
//! The `Table` derive hard-codes `chrono::Utc::now()` (which yields
//! `Value::Timestamptz`) for VM-source timestamp columns. The
//! SQLite adapter explicitly rejects `Value::Timestamptz` in
//! `reify-sqlite/src/lib.rs::value_to_sqlite` because SQLite has no
//! native `TIMESTAMPTZ` type. The PG / MySQL adapters do accept
//! `Timestamptz`, so this feature is effectively PG/MySQL-only.
//!
//! The equivalent coverage lives in:
//! - `pg_dto.rs::case_temporal_round_trip` (Timestamptz round-trip)
//! - `mysql_dto.rs::case_temporal_round_trip` (Timestamp round-trip)
//!
//! A proper SQLite-friendly variant of the feature (emit
//! `NaiveDateTime` from `Utc::now().naive_utc()` when the field type
//! is naive) is tracked as a library improvement, not a test gap.
//!
//! This file is kept so `tests/integration.rs` keeps compiling the
//! module list without a gap, but it registers zero tests.

#![cfg(feature = "sqlite-integration-tests")]
