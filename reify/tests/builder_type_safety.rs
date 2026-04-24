//! Compile-time type-safety tests for `ColumnBuilder` and derive macros.
//!
//! These tests verify that invalid builder configurations and unknown macro
//! attributes are rejected at compile time.

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

#[test]
fn column_unknown_attribute_fails_to_compile() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/column_unknown_attr.rs");
}

#[test]
fn db_enum_unknown_attribute_fails_to_compile() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/db_enum_unknown_attr.rs");
}

#[test]
fn soft_delete_non_option_fails_to_compile() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/soft_delete_non_option.rs");
}

#[test]
fn soft_delete_non_datetime_fails_to_compile() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/soft_delete_non_datetime.rs");
}
