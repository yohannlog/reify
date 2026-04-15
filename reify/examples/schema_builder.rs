use reify::{Schema, Table};

// ── Define structs with #[derive(Table)] for column constants ───────
// #[derive(Table)] provides typed column constants and query builders.
// Schema::schema() is the single source of truth for DDL (types, constraints, indexes).

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

// ── Parameterized types: Varchar, Char, Decimal ─────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "products")]
pub struct Product {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub name: String,
    pub price: f64,
    pub currency_code: String,
}

fn print_col_attrs(col: &reify::ColumnDef) {
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

fn main() {
    // ── Inspect schema metadata ─────────────────────────────────
    println!("=== User schema ===\n");
    let schema = User::schema();
    println!("Table: {}", schema.name);
    for col in &schema.columns {
        print_col_attrs(col);
    }
    if !schema.indexes.is_empty() {
        println!("  Indexes: {}", schema.indexes.len());
    }

    println!("\n=== Post schema ===\n");
    let schema = Post::schema();
    println!("Table: {}", schema.name);
    for col in &schema.columns {
        print_col_attrs(col);
    }
    if !schema.indexes.is_empty() {
        println!("  Indexes: {}", schema.indexes.len());
    }

    // ── Product schema (parameterized types) ────────────────────
    println!("\n=== Product schema (parameterized types) ===\n");
    let schema = Product::schema();
    println!("Table: {}", schema.name);
    for col in &schema.columns {
        let check_info = match &col.check {
            Some(expr) => format!(" CHECK ({expr})"),
            None => String::new(),
        };
        println!("  {} → {:?}{}", col.name, col.sql_type, check_info);
    }
    if !schema.checks.is_empty() {
        println!("  Table-level checks:");
        for check in &schema.checks {
            println!("    CHECK ({check})");
        }
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
