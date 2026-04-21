//! Example: String functions (trim, upper, lower, length)
//!
//! Demonstrates dialect-aware string manipulation functions.
//!
//! Run with: cargo run --example string_functions

use reify::{Expr, Table, TrimWhere, func, query::Dialect};

#[derive(Table, Debug, Clone)]
#[table(name = "products")]
pub struct Product {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub name: String,
    pub sku: String,
    pub description: Option<String>,
}

fn main() {
    println!("=== String Functions Example ===\n");

    // ── Basic TRIM (remove whitespace from both ends) ────────────────
    println!("── TRIM (whitespace) ──");
    let expr = func::trim(Product::name);

    println!(
        "  PostgreSQL: {}",
        expr.to_sql_fragment_dialect(Dialect::Postgres)
    );
    println!(
        "  MySQL:      {}",
        expr.to_sql_fragment_dialect(Dialect::Mysql)
    );
    println!(
        "  SQLite:     {}",
        expr.to_sql_fragment_dialect(Dialect::Generic)
    );
    println!();

    // ── TRIM with specific characters ────────────────────────────────
    println!("── TRIM (specific characters 'x') ──");
    let expr = func::trim_chars(Product::sku, "x");

    println!(
        "  PostgreSQL: {}",
        expr.to_sql_fragment_dialect(Dialect::Postgres)
    );
    println!(
        "  MySQL:      {}",
        expr.to_sql_fragment_dialect(Dialect::Mysql)
    );
    println!(
        "  SQLite:     {}",
        expr.to_sql_fragment_dialect(Dialect::Generic)
    );
    println!();

    // ── LTRIM (leading only) ─────────────────────────────────────────
    println!("── LTRIM (leading zeros) ──");
    let expr = func::ltrim_chars(Product::sku, "0");

    println!(
        "  PostgreSQL: {}",
        expr.to_sql_fragment_dialect(Dialect::Postgres)
    );
    println!(
        "  MySQL:      {}",
        expr.to_sql_fragment_dialect(Dialect::Mysql)
    );
    println!(
        "  SQLite:     {}",
        expr.to_sql_fragment_dialect(Dialect::Generic)
    );
    println!();

    // ── RTRIM (trailing only) ────────────────────────────────────────
    println!("── RTRIM (trailing spaces) ──");
    let expr = func::rtrim(Product::name);

    println!(
        "  PostgreSQL: {}",
        expr.to_sql_fragment_dialect(Dialect::Postgres)
    );
    println!(
        "  MySQL:      {}",
        expr.to_sql_fragment_dialect(Dialect::Mysql)
    );
    println!(
        "  SQLite:     {}",
        expr.to_sql_fragment_dialect(Dialect::Generic)
    );
    println!();

    // ── Using Expr::Trim directly ────────────────────────────────────
    println!("── Direct Expr::Trim construction ──");
    let expr = Expr::Trim("code", Some("ABC".to_string()), TrimWhere::Both);

    println!(
        "  PostgreSQL: {}",
        expr.to_sql_fragment_dialect(Dialect::Postgres)
    );
    println!(
        "  MySQL:      {}",
        expr.to_sql_fragment_dialect(Dialect::Mysql)
    );
    println!(
        "  SQLite:     {}",
        expr.to_sql_fragment_dialect(Dialect::Generic)
    );
    println!();

    // ── In a SELECT query ────────────────────────────────────────────
    println!("── SELECT with TRIM ──");
    let (sql, params) = Product::find()
        .select_expr(&[
            Expr::Col("id"),
            func::trim(Product::name),
            func::upper(Product::sku),
        ])
        .build();
    println!("  SQL:    {sql}");
    println!("  Params: {params:?}");
    println!();

    // ── Other string functions ───────────────────────────────────────
    println!("── Other string functions ──");
    println!(
        "  UPPER:  {}",
        func::upper(Product::name).to_sql_fragment_dialect(Dialect::Postgres)
    );
    println!(
        "  LOWER:  {}",
        func::lower(Product::name).to_sql_fragment_dialect(Dialect::Postgres)
    );
    println!(
        "  LENGTH: {}",
        func::length(Product::name).to_sql_fragment_dialect(Dialect::Postgres)
    );
}
