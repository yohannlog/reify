//! Tests for typed relations, JOIN SQL generation, and eager-loading pattern.

use reify::{Relation, RelationType, Relations, Table};

// ── Test models ──────────────────────────────────────────────────────

#[derive(Table, Relations, Debug, Clone)]
#[table(name = "users")]
#[relations(
    has_many(posts:   Post,    foreign_key = "user_id"),
    has_one( profile: Profile, foreign_key = "user_id"),
    belongs_to(team: Team,    foreign_key = "team_id"),
)]
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub team_id: i64,
    pub name: String,
}

#[derive(Table, Debug, Clone)]
#[table(name = "posts")]
pub struct Post {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub user_id: i64,
    pub title: String,
}

#[derive(Table, Debug, Clone)]
#[table(name = "profiles")]
pub struct Profile {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub user_id: i64,
    pub bio: String,
}

#[derive(Table, Debug, Clone)]
#[table(name = "teams")]
pub struct Team {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub name: String,
}

// ── Relation descriptor tests ────────────────────────────────────────

#[test]
fn has_many_relation_type() {
    let rel = User::posts();
    assert_eq!(rel.rel_type, RelationType::HasMany);
    assert_eq!(rel.name, "posts");
}

#[test]
fn has_many_columns() {
    let rel = User::posts();
    assert_eq!(rel.from_col, "id");
    assert_eq!(rel.to_col, "user_id");
}

#[test]
fn has_one_relation_type() {
    let rel = User::profile();
    assert_eq!(rel.rel_type, RelationType::HasOne);
    assert_eq!(rel.from_col, "id");
    assert_eq!(rel.to_col, "user_id");
}

#[test]
fn belongs_to_relation_type() {
    let rel = User::team();
    assert_eq!(rel.rel_type, RelationType::BelongsTo);
    // belongs_to: from_col = foreign_key on self, to_col = "id" on parent
    assert_eq!(rel.from_col, "team_id");
    assert_eq!(rel.to_col, "id");
}

#[test]
fn join_condition_string() {
    let rel = User::posts();
    assert_eq!(
        rel.join_condition(),
        "\"users\".\"id\" = \"posts\".\"user_id\""
    );
}

#[test]
fn belongs_to_join_condition() {
    let rel = User::team();
    assert_eq!(
        rel.join_condition(),
        "\"users\".\"team_id\" = \"teams\".\"id\""
    );
}

// ── Related trait manual impl ────────────────────────────────────────

impl reify::Related<Post> for User {
    fn relation() -> Relation<Self, Post> {
        User::posts()
    }
}

#[test]
fn related_trait_returns_correct_relation() {
    let rel = <User as reify::Related<Post>>::relation();
    assert_eq!(rel.rel_type, RelationType::HasMany);
    assert_eq!(rel.from_col, "id");
    assert_eq!(rel.to_col, "user_id");
}

// ── JOIN SQL generation ──────────────────────────────────────────────

#[test]
fn inner_join_sql() {
    let (sql, params) = User::find().join(User::posts()).build();
    assert_eq!(
        sql,
        "SELECT \"users\".*, \"posts\".* FROM \"users\" INNER JOIN \"posts\" ON \"users\".\"id\" = \"posts\".\"user_id\""
    );
    assert!(params.is_empty());
}

#[test]
fn left_join_sql() {
    let (sql, params) = User::find().left_join(User::profile()).build();
    assert_eq!(
        sql,
        "SELECT \"users\".*, \"profiles\".* FROM \"users\" LEFT JOIN \"profiles\" ON \"users\".\"id\" = \"profiles\".\"user_id\""
    );
    assert!(params.is_empty());
}

#[test]
fn right_join_sql() {
    let (sql, params) = User::find().right_join(User::team()).build();
    assert_eq!(
        sql,
        "SELECT \"users\".*, \"teams\".* FROM \"users\" RIGHT JOIN \"teams\" ON \"users\".\"team_id\" = \"teams\".\"id\""
    );
    assert!(params.is_empty());
}

#[test]
fn chained_joins_sql() {
    let (sql, _) = User::find()
        .join(User::posts())
        .left_join(User::profile())
        .build();
    assert_eq!(
        sql,
        "SELECT \"users\".*, \"posts\".*, \"profiles\".* FROM \"users\" \
         INNER JOIN \"posts\" ON \"users\".\"id\" = \"posts\".\"user_id\" \
         LEFT JOIN \"profiles\" ON \"users\".\"id\" = \"profiles\".\"user_id\""
    );
}

#[test]
fn join_with_filter_sql() {
    let (sql, params) = User::find()
        .join(User::posts())
        .filter(User::name.eq("alice"))
        .build();
    assert_eq!(
        sql,
        "SELECT \"users\".*, \"posts\".* FROM \"users\" \
         INNER JOIN \"posts\" ON \"users\".\"id\" = \"posts\".\"user_id\" \
         WHERE \"name\" = ?"
    );
    assert_eq!(params.len(), 1);
}

#[test]
fn join_with_limit_offset_sql() {
    let (sql, _) = User::find()
        .join(User::posts())
        .limit(10)
        .offset(20)
        .build();
    assert!(sql.ends_with("LIMIT 10 OFFSET 20"));
}

// ── Eager loading — WithBuilder ──────────────────────────────────────

#[test]
fn with_builder_parent_sql() {
    let wb = User::find()
        .filter(User::name.eq("alice"))
        .with(User::posts());
    let ((parent_sql, parent_params), _child_tpl) = wb.build_queries();
    assert_eq!(parent_sql, "SELECT * FROM \"users\" WHERE \"name\" = ?");
    assert_eq!(parent_params.len(), 1);
}

#[test]
fn with_builder_child_template() {
    let wb = User::find().with(User::posts());
    let (_, child_tpl) = wb.build_queries();
    // Child query selects from the target table filtering by the FK column.
    assert_eq!(
        child_tpl,
        "SELECT * FROM \"posts\" WHERE \"user_id\" IN (?)"
    );
}

#[test]
fn with_builder_exposes_relation() {
    let wb = User::find().with(User::posts());
    let rel = wb.relation();
    assert_eq!(rel.rel_type, RelationType::HasMany);
    assert_eq!(rel.to_col, "user_id");
}

#[test]
fn with_builder_belongs_to_child_template() {
    let wb = User::find().with(User::team());
    let (_, child_tpl) = wb.build_queries();
    // belongs_to: to_col on Team is "id"
    assert_eq!(child_tpl, "SELECT * FROM \"teams\" WHERE \"id\" IN (?)");
}
