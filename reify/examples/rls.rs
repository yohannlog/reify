//! Row-Level Security (RLS) example.
//!
//! Demonstrates how to define policies that automatically restrict
//! which rows a user can see, update, or delete — without any
//! database-specific RLS support. Works on PostgreSQL, MariaDB, etc.

use reify::{Policy, PolicyDecision, RlsContext, RlsError, Table};

// ── Models ─────────────────────────────────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "posts")]
pub struct Post {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub tenant_id: i64,
    pub user_id: i64,
    pub title: String,
}

// ── Policy: tenant isolation ───────────────────────────────────────
//
// Every query on `posts` is automatically scoped to the current tenant.
// A user can only see/modify rows where `tenant_id` matches their own.

impl Policy for Post {
    fn policy(ctx: &RlsContext) -> Result<PolicyDecision, RlsError> {
        let tenant_id = ctx.require::<i64>("tenant_id")?;
        Ok(PolicyDecision::Restrict(Post::tenant_id.eq(*tenant_id)))
    }
}

fn main() {
    // Simulate a request context: tenant 42, user 7
    let ctx = RlsContext::new()
        .set("tenant_id", 42i64)
        .set("user_id", 7i64);

    // ── SELECT with RLS ────────────────────────────────────────────
    // Without scoped: SELECT * FROM posts WHERE title LIKE ?
    // With scoped:    SELECT * FROM posts WHERE title LIKE ? AND tenant_id = ?
    let builder = Post::find().filter(Post::title.contains("Rust"));

    // Manually apply the policy to show the SQL output
    let builder = match Post::policy(&ctx).unwrap() {
        PolicyDecision::Restrict(cond) => builder.filter(cond),
        PolicyDecision::Unrestricted => builder,
    };
    let (sql, params) = builder.build();
    println!("SELECT with RLS:\n  {sql}\n  params: {params:?}\n");

    // ── UPDATE with RLS ────────────────────────────────────────────
    // The policy condition is added to the WHERE clause, so a user
    // cannot update rows belonging to another tenant.
    let builder = Post::update()
        .set(Post::title, "Updated title")
        .filter(Post::id.eq(1i64));

    let builder = match Post::policy(&ctx).unwrap() {
        PolicyDecision::Restrict(cond) => builder.filter(cond),
        PolicyDecision::Unrestricted => builder,
    };
    let (sql, params) = builder.build();
    println!("UPDATE with RLS:\n  {sql}\n  params: {params:?}\n");

    // ── DELETE with RLS ────────────────────────────────────────────
    let builder = Post::delete().filter(Post::id.eq(99i64));

    let builder = match Post::policy(&ctx).unwrap() {
        PolicyDecision::Restrict(cond) => builder.filter(cond),
        PolicyDecision::Unrestricted => builder,
    };
    let (sql, params) = builder.build();
    println!("DELETE with RLS:\n  {sql}\n  params: {params:?}\n");

    // ── Admin bypass ───────────────────────────────────────────────
    // Return PolicyDecision::Unrestricted *explicitly* to allow full
    // access — there is no implicit default, a missing context key is
    // an error, not a bypass.
    //
    //   fn policy(ctx: &RlsContext) -> Result<PolicyDecision, RlsError> {
    //       if ctx.get::<String>("role").map(|r| r == "admin") == Some(true) {
    //           return Ok(PolicyDecision::Unrestricted); // admin sees everything
    //       }
    //       let tenant_id = ctx.require::<i64>("tenant_id")?;
    //       Ok(PolicyDecision::Restrict(Post::tenant_id.eq(*tenant_id)))
    //   }

    println!("── With scoped_* helpers (async) ──────────────────────");
    println!("  let scoped = Scoped::new(&db, ctx);");
    println!("  let posts = scoped_fetch(&scoped, Post::find()).await?;");
    println!("  // → automatically adds WHERE tenant_id = 42");
}
