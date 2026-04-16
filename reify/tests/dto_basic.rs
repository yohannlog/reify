#![cfg(feature = "dto")]

use reify::{Table, Value};

// ── Fixtures ────────────────────────────────────────────────────────

#[derive(Table, Debug, Clone, Default)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
    pub name: String,
    #[column(nullable)]
    pub age: Option<i32>,
}

#[derive(Table, Debug, Clone)]
#[table(name = "posts", dto(skip = "slug"))]
pub struct Post {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub title: String,
    pub slug: String,
    pub body: String,
}

#[derive(Table, Debug, Clone, Default)]
#[table(name = "events")]
pub struct Event {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub label: String,
    // source = "db" avoids the macro injecting chrono::Utc::now() in
    // into_values(), while still marking the columns as timestamps so
    // the DTO exclusion logic applies.
    #[column(creation_timestamp, source = "db")]
    pub created_at: String,
    #[column(update_timestamp, source = "db")]
    pub updated_at: String,
}

// ── Basic exclusion ──────────────────────────────────────────────────

#[test]
fn dto_excludes_auto_pk() {
    let dto = UserDto {
        email: "bob@test.com".into(),
        name: "Bob".into(),
        age: Some(25),
    };
    assert_eq!(UserDto::column_names(), &["email", "name", "age"]);
    let values = dto.into_values();
    assert_eq!(values.len(), 3);
    assert_eq!(values[0], Value::String("bob@test.com".into()));
    assert_eq!(values[1], Value::String("Bob".into()));
    assert_eq!(values[2], Value::I32(25));
}

#[test]
fn dto_with_nullable() {
    let dto = UserDto {
        email: "none@test.com".into(),
        name: "None".into(),
        age: None,
    };
    let values = dto.into_values();
    assert_eq!(values[2], Value::Null);
}

#[test]
fn dto_respects_skip() {
    let dto = PostDto {
        title: "Hello".into(),
        body: "World".into(),
    };
    assert_eq!(PostDto::column_names(), &["title", "body"]);
    assert_eq!(dto.into_values().len(), 2);
}

// L3: timestamp columns are excluded from the DTO
#[test]
fn dto_excludes_timestamps() {
    // EventDto should only have `label` — created_at and updated_at are excluded
    let dto = EventDto {
        label: "launch".into(),
    };
    assert_eq!(EventDto::column_names(), &["label"]);
    assert_eq!(dto.into_values().len(), 1);
}

// ── impl Table on DTO (H2) ───────────────────────────────────────────

#[test]
fn dto_implements_table_trait() {
    // Table::column_names() and Table::into_values() work via the trait
    use reify::Table as _;
    let dto = UserDto {
        email: "trait@test.com".into(),
        name: "Trait".into(),
        age: Some(1),
    };
    assert_eq!(
        <UserDto as reify::Table>::column_names(),
        &["email", "name", "age"]
    );
    assert_eq!(<UserDto as reify::Table>::table_name(), "users");
    assert_eq!(dto.into_values().len(), 3);
}

// ── From conversions (H3) ────────────────────────────────────────────

#[test]
fn from_model_to_dto() {
    let user = User {
        id: 42,
        email: "alice@example.com".into(),
        name: "Alice".into(),
        age: Some(30),
    };
    let dto = UserDto::from(&user);
    assert_eq!(dto.email, "alice@example.com");
    assert_eq!(dto.name, "Alice");
    assert_eq!(dto.age, Some(30));
}

#[test]
fn from_dto_to_model_defaults_excluded_fields() {
    let dto = UserDto {
        email: "bob@example.com".into(),
        name: "Bob".into(),
        age: None,
    };
    let user = User::from(&dto);
    // id was not in the DTO — must be Default (0 for i64)
    assert_eq!(user.id, 0);
    assert_eq!(user.email, "bob@example.com");
    assert_eq!(user.age, None);
}

#[test]
fn from_owned_model_to_dto() {
    let user = User {
        id: 1,
        email: "owned@example.com".into(),
        name: "Owned".into(),
        age: None,
    };
    let dto: UserDto = user.into();
    assert_eq!(dto.email, "owned@example.com");
}

// ── M2: dto_skip with invalid field name is a compile error ──────────
// (verified via trybuild — see tests/compile_fail/ if present)
