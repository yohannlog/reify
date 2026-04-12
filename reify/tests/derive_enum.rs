use reify::{DbEnum, Table, Value, enum_from_value};

// ── Basic enum ─────────────────────────────────────────────────────

#[derive(DbEnum, Debug, Clone, PartialEq)]
pub enum Role {
    Admin,
    Member,
    Guest,
}

#[test]
fn variants() {
    assert_eq!(Role::variants(), &["admin", "member", "guest"]);
}

#[test]
fn as_str() {
    assert_eq!(Role::Admin.as_str(), "admin");
    assert_eq!(Role::Member.as_str(), "member");
    assert_eq!(Role::Guest.as_str(), "guest");
}

#[test]
fn from_str() {
    assert_eq!(Role::from_str("admin"), Some(Role::Admin));
    assert_eq!(Role::from_str("member"), Some(Role::Member));
    assert_eq!(Role::from_str("unknown"), None);
}

#[test]
fn into_value() {
    use reify::value::IntoValue;
    assert_eq!(Role::Admin.into_value(), Value::String("admin".into()));
}

#[test]
fn enum_from_value_ok() {
    let val = Value::String("guest".into());
    let role: Role = enum_from_value(&val).unwrap();
    assert_eq!(role, Role::Guest);
}

#[test]
fn enum_from_value_unknown() {
    let val = Value::String("superadmin".into());
    let result: Result<Role, _> = enum_from_value(&val);
    assert!(result.is_err());
}

#[test]
fn enum_from_value_null() {
    let result: Result<Role, _> = enum_from_value(&Value::Null);
    assert!(result.is_err());
}

// ── Enum with rename ───────────────────────────────────────────────

#[derive(DbEnum, Debug, Clone, PartialEq)]
pub enum Status {
    Active,
    #[db_enum(rename = "on_hold")]
    OnHold,
    Archived,
}

#[test]
fn rename_variants() {
    assert_eq!(Status::variants(), &["active", "on_hold", "archived"]);
}

#[test]
fn rename_as_str() {
    assert_eq!(Status::OnHold.as_str(), "on_hold");
}

#[test]
fn rename_from_str() {
    assert_eq!(Status::from_str("on_hold"), Some(Status::OnHold));
    assert_eq!(Status::from_str("on_hold"), Some(Status::OnHold));
    // The default snake_case would be "on_hold" anyway, but let's check
    // that the explicit rename takes precedence
    assert_eq!(Status::from_str("active"), Some(Status::Active));
}

// ── Enum used as a Table column ────────────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "posts")]
pub struct Post {
    #[column(primary_key)]
    pub id: i64,
    pub title: String,
    pub status: Status,
}

#[test]
fn table_with_enum_column_insert() {
    let post = Post {
        id: 1,
        title: "Hello".into(),
        status: Status::Active,
    };
    let (sql, params) = Post::insert(&post).build();
    assert_eq!(sql, "INSERT INTO posts (id, title, status) VALUES (?, ?, ?)");
    assert_eq!(
        params,
        vec![
            Value::I64(1),
            Value::String("Hello".into()),
            Value::String("active".into()),
        ]
    );
}

#[test]
fn table_with_enum_column_filter() {
    let (sql, params) = Post::find()
        .filter(Post::status.eq(Status::OnHold))
        .build();
    assert_eq!(sql, "SELECT * FROM posts WHERE status = ?");
    assert_eq!(params, vec![Value::String("on_hold".into())]);
}

#[test]
fn table_with_enum_column_in_list() {
    let (sql, params) = Post::find()
        .filter(Post::status.in_list(vec![Status::Active, Status::Archived]))
        .build();
    assert_eq!(sql, "SELECT * FROM posts WHERE status IN (?, ?)");
    assert_eq!(
        params,
        vec![
            Value::String("active".into()),
            Value::String("archived".into()),
        ]
    );
}

#[test]
fn table_with_enum_column_update() {
    let (sql, params) = Post::update()
        .set(Post::status, Status::Archived)
        .filter(Post::id.eq(1i64))
        .build();
    assert_eq!(sql, "UPDATE posts SET status = ? WHERE id = ?");
    assert_eq!(
        params,
        vec![Value::String("archived".into()), Value::I64(1)]
    );
}
