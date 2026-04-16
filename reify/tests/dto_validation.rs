#![cfg(feature = "dto-validation")]

use reify::Table;
use validator::Validate;

// ── Fixtures ────────────────────────────────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(unique, validate(email))]
    pub email: String,
    #[column(validate(length(min = 1, max = 100)))]
    pub name: String,
    #[column(nullable)]
    pub bio: Option<String>,
}

#[derive(Table, Debug, Clone)]
#[table(name = "products")]
pub struct Product {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(validate(length(min = 1, max = 255)))]
    pub title: String,
    #[column(validate(range(min = 0.0)))]
    pub price: f64,
}

// ── validator::Validate is derived on the DTO ────────────────────────

#[test]
fn dto_derives_validate() {
    // Compile-time proof: UserDto implements validator::Validate
    fn assert_validate<T: Validate>() {}
    assert_validate::<UserDto>();
    assert_validate::<ProductDto>();
}

// ── Valid values pass validation ─────────────────────────────────────

#[test]
fn valid_email_passes() {
    let dto = UserDto {
        email: "alice@example.com".into(),
        name: "Alice".into(),
        bio: None,
    };
    assert!(dto.validate().is_ok(), "valid DTO should pass validation");
}

#[test]
fn valid_product_passes() {
    let dto = ProductDto {
        title: "Widget".into(),
        price: 9.99,
    };
    assert!(dto.validate().is_ok());
}

// ── Invalid values produce ValidationErrors ──────────────────────────

#[test]
fn invalid_email_fails() {
    let dto = UserDto {
        email: "not-an-email".into(),
        name: "Bob".into(),
        bio: None,
    };
    let err = dto.validate();
    assert!(err.is_err(), "invalid email should fail validation");
    let errors = err.unwrap_err();
    assert!(
        errors.field_errors().contains_key("email"),
        "error should be on the `email` field"
    );
}

#[test]
fn empty_name_fails_length_check() {
    let dto = UserDto {
        email: "carol@example.com".into(),
        name: String::new(), // violates length(min = 1)
        bio: None,
    };
    let err = dto.validate();
    assert!(err.is_err());
    assert!(err.unwrap_err().field_errors().contains_key("name"));
}

#[test]
fn name_too_long_fails() {
    let dto = UserDto {
        email: "dave@example.com".into(),
        name: "x".repeat(101), // violates length(max = 100)
        bio: None,
    };
    let err = dto.validate();
    assert!(err.is_err());
    assert!(err.unwrap_err().field_errors().contains_key("name"));
}

#[test]
fn negative_price_fails_range_check() {
    let dto = ProductDto {
        title: "Widget".into(),
        price: -1.0, // violates range(min = 0.0)
    };
    let err = dto.validate();
    assert!(err.is_err());
    assert!(err.unwrap_err().field_errors().contains_key("price"));
}

// ── Option<T> fields without validate pass through ───────────────────

#[test]
fn optional_field_without_validate_is_ignored() {
    // `bio` has no validate rule — None and Some both pass
    let dto_none = UserDto {
        email: "eve@example.com".into(),
        name: "Eve".into(),
        bio: None,
    };
    let dto_some = UserDto {
        email: "eve@example.com".into(),
        name: "Eve".into(),
        bio: Some("A short bio.".into()),
    };
    assert!(dto_none.validate().is_ok());
    assert!(dto_some.validate().is_ok());
}

// ── Multiple errors are reported together ────────────────────────────

#[test]
fn multiple_invalid_fields_reported() {
    let dto = UserDto {
        email: "bad-email".into(),
        name: String::new(),
        bio: None,
    };
    let errors = dto.validate().unwrap_err();
    let field_errors = errors.field_errors();
    assert!(field_errors.contains_key("email"));
    assert!(field_errors.contains_key("name"));
}
