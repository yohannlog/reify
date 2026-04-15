use reify::{IndexKind, Schema, SortDirection, Table};

// ═══════════════════════════════════════════════════════════════════
//  Indexes via Schema::schema() — single source of truth for DDL
// ═══════════════════════════════════════════════════════════════════

#[derive(Table, Debug, Clone)]
#[table(name = "orders")]
pub struct Order {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub user_id: i64,
    pub product_id: i64,
    pub status: String,
    pub region: String,
    pub created_at: i64,
    pub total: f64,
}

// ═══════════════════════════════════════════════════════════════════
//  Approach 2 — Builder-based indexes (full autocompletion)
// ═══════════════════════════════════════════════════════════════════

#[derive(Table, Debug, Clone)]
#[table(name = "events")]
pub struct Event {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub tenant_id: i64,
    pub user_id: i64,
    pub action: String,
    pub payload: String,
    pub created_at: i64,
}

// ═══════════════════════════════════════════════════════════════════

fn main() {
    println!("=== Order indexes ===\n");
    print_indexes::<Order>();

    println!("\n=== Event indexes ===\n");
    print_indexes::<Event>();
}

fn print_indexes<T: Schema>() {
    println!("Table: {}\n", T::table_name());
    for idx in T::indexes() {
        print_index(&idx);
    }
}

fn print_index(idx: &reify::IndexDef) {
    let kind = match idx.kind {
        IndexKind::BTree => "BTREE",
        IndexKind::Hash => "HASH",
        IndexKind::Gin => "GIN",
        IndexKind::Gist => "GiST",
    };
    let unique = if idx.unique { " UNIQUE" } else { "" };
    let name = idx.name.as_deref().unwrap_or("<auto>");
    let pred = idx
        .predicate
        .as_deref()
        .map(|p| format!(" WHERE {p}"))
        .unwrap_or_default();
    let cols: Vec<String> = idx
        .columns
        .iter()
        .map(|c| match c.direction {
            SortDirection::Asc => c.name.to_string(),
            SortDirection::Desc => format!("{} DESC", c.name),
        })
        .collect();
    println!("  {name}:{unique} {kind} ({}){pred}", cols.join(", "));
}
