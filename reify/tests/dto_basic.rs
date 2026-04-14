#![cfg(feature = "dto")]

use reify::{Table, Value};

#[derive(Table, Debug, Clone)]
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

#[test]
fn dto_excludes_auto_pk() {
    // UserDto should not have `id` field
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

#[derive(Table, Debug, Clone)]
#[table(name = "posts", dto(skip = "slug"))]
pub struct Post {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub title: String,
    pub slug: String,
    pub body: String,
}

#[test]
fn dto_respects_skip() {
    // PostDto should not have `id` (auto PK) nor `slug` (dto skip)
    let dto = PostDto {
        title: "Hello".into(),
        body: "World".into(),
    };
    assert_eq!(PostDto::column_names(), &["title", "body"]);
    let values = dto.into_values();
    assert_eq!(values.len(), 2);
}
