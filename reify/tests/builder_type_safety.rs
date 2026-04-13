//! Compile-time type-safety tests for `ColumnBuilder`.
//!
//! These tests verify that invalid builder configurations are rejected at
//! compile time by the type-state pattern on `ColumnBuilder<T, S>`.

#[test]
#[cfg(any(feature = "postgres", feature = "mysql"))]
fn creation_timestamp_on_non_temporal_type_fails_to_compile() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/creation_timestamp_on_string.rs");
}

#[test]
fn source_db_without_timestamp_fails_to_compile() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/source_db_without_timestamp.rs");
}
