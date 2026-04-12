use reify::{IndexKind, Schema, Table, TableSchema};

// ═══════════════════════════════════════════════════════════════════
//  Approach 1 — Macro-based indexes
// ═══════════════════════════════════════════════════════════════════

// Single-column indexes via #[column(index)]
// Composite indexes via #[table(index(...))]
// Both can coexist on the same table.

#[derive(Table, Debug, Clone)]
#[table(
    name = "orders",
    // Composite index on (user_id, created_at) — for queries like
    //   SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC
    index(columns("user_id", "created_at")),
    // Unique composite index with explicit name
    index(columns("user_id", "product_id"), unique, name = "idx_one_product_per_user"),
    // Three-column composite
    index(columns("status", "region", "created_at")),
    // Partial index: unique product only for non-cancelled orders
    index(columns("user_id", "product_id"), unique, predicate = "status != 'cancelled'", name = "idx_orders_live_product"),
)]
pub struct Order {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(index)] // ← single-column index, auto-named "idx_orders_user_id"
    pub user_id: i64,
    #[column(index)] // ← another single-column index
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

impl Schema for Event {
    fn schema() -> TableSchema<Self> {
        reify::table::<Self>("events")
            .column(Event::id, |c| c.primary_key().auto_increment())
            .column(Event::tenant_id, |c| c)
            .column(Event::user_id, |c| c)
            .column(Event::action, |c| c)
            .column(Event::payload, |c| c)
            .column(Event::created_at, |c| c)
            // Single-column index
            .index(|idx| idx.column(Event::tenant_id))
            // Composite: tenant scoped queries by time
            .index(|idx| {
                idx.column(Event::tenant_id)
                    .column(Event::created_at)
                    .name("idx_events_tenant_timeline")
            })
            // Composite unique: one action per user per timestamp
            .index(|idx| {
                idx.column(Event::user_id)
                    .column(Event::action)
                    .column(Event::created_at)
                    .unique()
                    .name("idx_events_user_action_unique")
            })
            // Hash index for exact-match lookups (PostgreSQL)
            .index(|idx| idx.column(Event::action).hash())
            // GIN index for full-text search on payload (PostgreSQL)
            .index(|idx| {
                idx.column(Event::payload)
                    .gin()
                    .name("idx_events_payload_fts")
            })
            // Partial index: only index active events (PostgreSQL)
            .index(|idx| {
                idx.column(Event::tenant_id)
                    .column(Event::created_at)
                    .predicate("action != 'deleted'")
                    .name("idx_events_active_timeline")
            })
    }
}

// ═══════════════════════════════════════════════════════════════════

fn main() {
    println!("=== Order indexes (macro) ===\n");
    print_indexes::<Order>();

    println!("\n=== Event indexes (builder) ===\n");
    let schema = Event::schema();
    println!("Table: {}\n", schema.name);
    for idx in &schema.indexes {
        print_index(idx);
    }
}

fn print_indexes<T: Table>() {
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
    println!(
        "  {name}:{unique} {kind} ({}){pred}",
        idx.columns.join(", ")
    );
}
