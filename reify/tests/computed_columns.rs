use reify::{ComputedColumn, Dialect, Schema, Table, Value};

// ── DB-generated computed column (GENERATED ALWAYS AS … STORED) ─────

#[derive(Table, Debug, Clone)]
#[table(name = "products")]
pub struct Product {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub price: f64,
    pub quantity: i32,
    /// DB-computed: price * quantity
    #[column(computed = "price * quantity")]
    pub total: f64,
}

#[test]
fn computed_stored_excluded_from_column_defs_writable() {
    let writable = Product::writable_column_names();
    // id is auto_increment → excluded; total is computed → excluded
    assert_eq!(writable, vec!["price", "quantity"]);
    assert!(
        !writable.contains(&"total"),
        "computed column should not be writable"
    );
}

#[test]
fn computed_stored_present_in_column_names() {
    // column_names() returns ALL columns including computed
    let all = Product::column_names();
    assert!(
        all.contains(&"total"),
        "computed column should be in column_names()"
    );
    assert_eq!(all, &["id", "price", "quantity", "total"]);
}

#[test]
fn computed_stored_present_in_db_column_names() {
    // DB-generated columns exist in the database
    let db_cols = Product::db_column_names();
    assert!(
        db_cols.contains(&"total"),
        "stored computed column should be in db_column_names()"
    );
}

#[test]
fn computed_stored_excluded_from_insert() {
    let product = Product {
        id: 0,
        price: 9.99,
        quantity: 3,
        total: 0.0, // ignored in INSERT
    };
    let (sql, params) = Product::insert(&product).build();
    assert!(
        !sql.contains("total"),
        "INSERT should not include computed column: {sql}"
    );
    assert_eq!(
        params.len(),
        2,
        "should have 2 params (price, quantity) — id is auto_increment"
    );
    assert!(
        sql.contains("\"price\", \"quantity\""),
        "should list writable columns: {sql}"
    );
}

#[test]
fn computed_stored_excluded_from_insert_many() {
    let products = vec![
        Product {
            id: 1,
            price: 10.0,
            quantity: 2,
            total: 0.0,
        },
        Product {
            id: 2,
            price: 20.0,
            quantity: 1,
            total: 0.0,
        },
    ];
    let (sql, params) = Product::insert_many(&products).build();
    assert!(
        !sql.contains("total"),
        "INSERT MANY should not include computed column: {sql}"
    );
    assert_eq!(
        params.len(),
        4,
        "should have 4 params (2 per row × 2 rows) — id excluded"
    );
}

#[test]
fn computed_stored_writable_values_excludes_computed() {
    let product = Product {
        id: 1,
        price: 9.99,
        quantity: 3,
        total: 29.97,
    };
    let writable = product.writable_values();
    // id (auto_increment) excluded, total (computed) excluded
    assert_eq!(writable.len(), 2);
    assert_eq!(writable[0], Value::F64(9.99));
    assert_eq!(writable[1], Value::I32(3));
}

#[test]
fn computed_stored_column_def_metadata() {
    let defs = Product::column_defs();
    let total_def = defs.iter().find(|d| d.name == "total").unwrap();
    assert_eq!(
        total_def.computed,
        Some(ComputedColumn::Stored("price * quantity".to_string()))
    );
}

#[test]
fn computed_stored_create_table_sql_postgres() {
    use reify::migration::create_table_sql;

    let defs = Product::column_defs();
    let sql = create_table_sql("products", &defs, Dialect::Postgres);
    assert!(
        sql.contains("GENERATED ALWAYS AS (price * quantity) STORED"),
        "CREATE TABLE should include GENERATED ALWAYS AS clause: {sql}"
    );
    // The computed column should NOT have NOT NULL or DEFAULT
    assert!(
        !sql.contains("total DOUBLE PRECISION NOT NULL"),
        "computed column should not have NOT NULL: {sql}"
    );
}

// ── Rust-side virtual column (computed_rust) ────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub first_name: String,
    pub last_name: String,
    /// Computed in Rust after fetch — not in the DB
    #[column(computed_rust)]
    pub display_name: String,
}

#[test]
fn virtual_excluded_from_writable() {
    let writable = User::writable_column_names();
    // id is auto_increment → excluded; display_name is computed_rust → excluded
    assert_eq!(writable, vec!["first_name", "last_name"]);
    assert!(!writable.contains(&"display_name"));
}

#[test]
fn virtual_excluded_from_db_columns() {
    let db_cols = User::db_column_names();
    assert!(
        !db_cols.contains(&"display_name"),
        "virtual column should not be in db_column_names()"
    );
    assert_eq!(db_cols, vec!["id", "first_name", "last_name"]);
}

#[test]
fn virtual_present_in_column_names() {
    // column_names() returns ALL columns (struct fields)
    let all = User::column_names();
    assert!(all.contains(&"display_name"));
}

#[test]
fn virtual_excluded_from_insert() {
    let user = User {
        id: 0,
        first_name: "Alice".into(),
        last_name: "Smith".into(),
        display_name: String::new(),
    };
    let (sql, params) = User::insert(&user).build();
    assert!(
        !sql.contains("display_name"),
        "INSERT should not include virtual column: {sql}"
    );
    // id (auto_increment) and display_name (computed_rust) excluded
    assert_eq!(params.len(), 2);
}

#[test]
fn virtual_excluded_from_create_table() {
    use reify::migration::create_table_sql;

    let defs = User::column_defs();
    let sql = create_table_sql("users", &defs, Dialect::Postgres);
    assert!(
        !sql.contains("display_name"),
        "CREATE TABLE should not include virtual column: {sql}"
    );
    assert!(
        sql.contains("\"first_name\""),
        "should include real columns: {sql}"
    );
    assert!(
        sql.contains("\"last_name\""),
        "should include real columns: {sql}"
    );
}

#[test]
fn virtual_column_def_metadata() {
    let defs = User::column_defs();
    let dn_def = defs.iter().find(|d| d.name == "display_name").unwrap();
    assert_eq!(dn_def.computed, Some(ComputedColumn::Virtual));
}

// ── Non-computed columns are unaffected ─────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "simple")]
pub struct Simple {
    #[column(primary_key)]
    pub id: i64,
    pub name: String,
}

#[test]
fn no_computed_columns_writable_equals_all() {
    assert_eq!(
        Simple::writable_column_names(),
        Simple::column_names().to_vec()
    );
    assert_eq!(Simple::db_column_names(), Simple::column_names().to_vec());
}

#[test]
fn no_computed_columns_insert_includes_all() {
    let s = Simple {
        id: 1,
        name: "test".into(),
    };
    let (sql, _) = Simple::insert(&s).build();
    assert!(
        sql.contains("\"id\", \"name\""),
        "should include all columns: {sql}"
    );
}

// ── Schema builder API ──────────────────────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "items")]
pub struct Item {
    #[column(primary_key)]
    pub id: i64,
    pub price: f64,
}

#[test]
fn schema_builder_computed_stored() {
    use reify::{table, SqlType, TableSchema};

    let schema: TableSchema<Item> = table::<Item>("items")
        .column(Item::id, |c| c.primary_key())
        .column(Item::price, |c| {
            c.sql_type(SqlType::Double)
                .computed_stored("base_price * tax_rate")
        });

    let price_def = schema.columns.iter().find(|d| d.name == "price").unwrap();
    assert_eq!(
        price_def.computed,
        Some(ComputedColumn::Stored("base_price * tax_rate".to_string()))
    );
}

#[test]
fn schema_builder_computed_virtual() {
    use reify::{table, TableSchema};

    let schema: TableSchema<Item> = table::<Item>("items")
        .column(Item::id, |c| c.primary_key())
        .column(Item::price, |c| c.computed_virtual());

    let price_def = schema.columns.iter().find(|d| d.name == "price").unwrap();
    assert_eq!(price_def.computed, Some(ComputedColumn::Virtual));
}
