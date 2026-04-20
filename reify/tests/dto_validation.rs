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

// ── validated_insert / validated_insert_many ────────────────────────

#[test]
fn validated_insert_rejects_invalid_dto() {
    let dto = UserDto {
        email: "not-an-email".into(),
        name: "Alice".into(),
        bio: None,
    };
    let errors = UserDto::validated_insert(&dto)
        .err()
        .expect("invalid DTO must not produce a builder");
    assert!(errors.field_errors().contains_key("email"));
}

#[test]
fn validated_insert_accepts_valid_dto() {
    let dto = UserDto {
        email: "alice@example.com".into(),
        name: "Alice".into(),
        bio: None,
    };
    let builder = UserDto::validated_insert(&dto).expect("valid DTO");
    let (sql, params) = builder.build();
    assert!(sql.to_ascii_uppercase().contains("INSERT INTO"));
    assert!(!params.is_empty());
}

#[test]
fn validated_insert_many_rejects_any_invalid_row() {
    let dtos = vec![
        UserDto {
            email: "ok@example.com".into(),
            name: "Ok".into(),
            bio: None,
        },
        UserDto {
            email: "bad".into(), // invalid
            name: "Bad".into(),
            bio: None,
        },
    ];
    assert!(
        UserDto::validated_insert_many(&dtos).is_err(),
        "one invalid row must fail the whole batch"
    );
}

#[test]
fn validated_insert_many_accepts_all_valid() {
    let dtos = vec![
        UserDto {
            email: "a@example.com".into(),
            name: "A".into(),
            bio: None,
        },
        UserDto {
            email: "b@example.com".into(),
            name: "B".into(),
            bio: None,
        },
    ];
    let builder = UserDto::validated_insert_many(&dtos).expect("all valid");
    let (sql, _params) = builder.build();
    assert!(sql.to_ascii_uppercase().contains("INSERT INTO"));
}

// ── 3.5 — unknown rule names rejected at macro-expansion ─────────────
//
// The negative case lives in `tests/compile_fail/`. This test is the
// positive counterpart: every rule listed in the macro's allow-list
// parses without error.

#[derive(Table, Debug, Clone)]
#[table(name = "all_rules")]
pub struct CoversAllRules {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(validate(email, length(max = 254)))]
    pub email: String,
    #[column(validate(url))]
    pub site: String,
    #[column(validate(range(min = 0, max = 100)))]
    pub score: i32,
    // `required` lifts the Option-skip footgun for value rules.
    #[column(validate(required, length(min = 1)))]
    pub bio: Option<String>,
}

#[test]
fn covers_all_rules_compiles_and_validates() {
    let dto = CoversAllRulesDto {
        email: "a@example.com".into(),
        site: "https://example.com".into(),
        score: 50,
        bio: Some("ok".into()),
    };
    assert!(dto.validate().is_ok());

    // None on a `required`d Option field must fail.
    let bad = CoversAllRulesDto {
        bio: None,
        ..dto.clone()
    };
    assert!(bad.validate().is_err());
}
