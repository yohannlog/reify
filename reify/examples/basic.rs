use reify::{Table, query::Order};

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub email: String,
    pub role: Option<String>,
}

#[derive(Table, Debug, Clone)]
#[table(name = "posts")]
pub struct Post {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub user_id: i64,
    pub title: String,
    pub body: Option<String>,
}

fn main() {
    // ── SELECT ──────────────────────────────────────────────────
    let (sql, params) = User::find()
        .filter(User::email.ends_with("@corp.io"))
        .filter(User::role.is_not_null())
        .order_by(Order::Desc("id"))
        .limit(10)
        .build();
    println!("SELECT:\n  {sql}\n  params: {params:?}\n");

    // ── INSERT ──────────────────────────────────────────────────
    let alice = User {
        id: 0,
        email: "alice@corp.io".into(),
        role: Some("admin".into()),
    };
    let (sql, params) = User::insert(&alice).build();
    println!("INSERT:\n  {sql}\n  params: {params:?}\n");

    // ── UPDATE (WHERE obligatoire) ──────────────────────────────
    let (sql, params) = User::update()
        .set(User::role, "superadmin")
        .filter(User::id.eq(1i64))
        .build();
    println!("UPDATE:\n  {sql}\n  params: {params:?}\n");

    // ── DELETE (WHERE obligatoire) ──────────────────────────────
    let (sql, params) = User::delete().filter(User::id.eq(42i64)).build();
    println!("DELETE:\n  {sql}\n  params: {params:?}\n");

    // ── Logical OR ──────────────────────────────────────────────
    let cond = User::role.is_null().or(User::email.starts_with("admin"));
    let (sql, params) = User::find().filter(cond).build();
    println!("OR condition:\n  {sql}\n  params: {params:?}\n");

    // ── Multi-table ─────────────────────────────────────────────
    let (sql, params) = Post::find()
        .filter(Post::title.contains("Rust"))
        .filter(Post::body.is_not_null())
        .limit(5)
        .build();
    println!("Posts:\n  {sql}\n  params: {params:?}");
}
