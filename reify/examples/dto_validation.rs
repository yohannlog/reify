//! # DTO Validation Example
//!
//! Demonstrates the `dto-validation` feature: `#[derive(Table)]` generates a
//! `{Name}Dto` struct that automatically derives `validator::Validate`, with
//! per-field rules declared via `#[column(validate(...))]`.
//!
//! Run with:
//! ```sh
//! cargo run --example dto_validation --features dto-validation
//! ```

use reify::Table;
use validator::Validate;

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    /// Auto-increment primary key — excluded from UserDto automatically.
    #[column(primary_key, auto_increment)]
    pub id: i64,

    /// Must be a valid email address.
    #[column(unique, validate(email))]
    pub email: String,

    /// 1–100 characters.
    #[column(validate(length(min = 1, max = 100)))]
    pub name: String,

    /// Optional bio — no validation rule applied.
    #[column(nullable)]
    pub bio: Option<String>,
}

fn main() {
    // ── Valid DTO ────────────────────────────────────────────────────
    let valid = UserDto {
        email: "alice@example.com".into(),
        name: "Alice".into(),
        bio: Some("Loves Rust.".into()),
    };

    match valid.validate() {
        Ok(()) => println!("✓ Valid DTO passed validation"),
        Err(e) => println!("✗ Unexpected error: {e}"),
    }

    // ── Invalid email ────────────────────────────────────────────────
    let bad_email = UserDto {
        email: "not-an-email".into(),
        name: "Bob".into(),
        bio: None,
    };

    match bad_email.validate() {
        Ok(()) => println!("✗ Should have failed"),
        Err(e) => println!("✓ Invalid email caught: {e}"),
    }

    // ── Empty name (violates length min = 1) ─────────────────────────
    let empty_name = UserDto {
        email: "carol@example.com".into(),
        name: String::new(),
        bio: None,
    };

    match empty_name.validate() {
        Ok(()) => println!("✗ Should have failed"),
        Err(e) => println!("✓ Empty name caught: {e}"),
    }

    // ── Multiple errors ──────────────────────────────────────────────
    let both_bad = UserDto {
        email: "bad".into(),
        name: String::new(),
        bio: None,
    };

    match both_bad.validate() {
        Ok(()) => println!("✗ Should have failed"),
        Err(e) => {
            let fields: Vec<_> = e.field_errors().keys().cloned().collect();
            println!("✓ Multiple errors on fields: {fields:?}");
        }
    }

    // ── From<User> for UserDto ───────────────────────────────────────
    let user = User {
        id: 42,
        email: "dave@example.com".into(),
        name: "Dave".into(),
        bio: None,
    };
    let dto = UserDto::from(&user);
    println!("\nConverted User → UserDto: {dto:?}");
    println!("DTO columns: {:?}", UserDto::column_names());
    println!("DTO values:  {:?}", dto.into_values());
}
