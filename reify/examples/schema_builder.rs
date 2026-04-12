use reify::{Schema, Table, TableSchema};

// ── Define structs with #[derive(Table)] for column constants ───────
// The derive gives us User::id, User::email, etc. as typed constants.
// The Schema trait lets us describe column attributes with full autocompletion.

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    pub id: i64,
    pub email: String,
    pub role: Option<String>,
}

impl Schema for User {
    fn schema() -> TableSchema<Self> {
        reify::table::<Self>("users")
            .column(User::id, |c| c.primary_key().auto_increment())
            .column(User::email, |c| c.unique().index())
            .column(User::role, |c| c.nullable().default("member"))
    }
}

#[derive(Table, Debug, Clone)]
#[table(name = "posts")]
pub struct Post {
    pub id: i64,
    pub user_id: i64,
    pub title: String,
    pub body: Option<String>,
}

impl Schema for Post {
    fn schema() -> TableSchema<Self> {
        reify::table::<Self>("posts")
            .column(Post::id, |c| c.primary_key().auto_increment())
            .column(Post::user_id, |c| c.index())
            .column(Post::title, |c| c)
            .column(Post::body, |c| c.nullable())
    }
}

fn main() {
    // ── Inspect schema metadata ─────────────────────────────────
    println!("=== User schema ===\n");

    let schema = User::schema();
    println!("Table: {}", schema.name);
    for col in &schema.columns {
        let mut attrs = Vec::new();
        if col.primary_key {
            attrs.push("PRIMARY KEY");
        }
        if col.auto_increment {
            attrs.push("AUTOINCREMENT");
        }
        if col.unique {
            attrs.push("UNIQUE");
        }
        if col.index {
            attrs.push("INDEX");
        }
        if col.nullable {
            attrs.push("NULLABLE");
        }
        if let Some(ref default) = col.default {
            attrs.push(default);
        }
        println!(
            "  {} {}",
            col.name,
            if attrs.is_empty() {
                String::new()
            } else {
                format!("[{}]", attrs.join(", "))
            }
        );
    }

    println!("\n=== Post schema ===\n");

    let schema = Post::schema();
    println!("Table: {}", schema.name);
    for col in &schema.columns {
        let mut attrs = Vec::new();
        if col.primary_key {
            attrs.push("PRIMARY KEY");
        }
        if col.auto_increment {
            attrs.push("AUTOINCREMENT");
        }
        if col.index {
            attrs.push("INDEX");
        }
        if col.nullable {
            attrs.push("NULLABLE");
        }
        println!(
            "  {} {}",
            col.name,
            if attrs.is_empty() {
                String::new()
            } else {
                format!("[{}]", attrs.join(", "))
            }
        );
    }

    // ── The schema + query builder work together ────────────────
    println!("\n=== Queries use the same typed columns ===\n");

    let (sql, params) = User::find()
        .filter(User::role.is_null())
        .filter(User::email.contains("@corp"))
        .build();
    println!("  {sql}");
    println!("  params: {params:?}");

    let (sql, params) = User::update()
        .set(User::role, "admin")
        .filter(User::id.eq(1i64))
        .build();
    println!("  {sql}");
    println!("  params: {params:?}");
}
