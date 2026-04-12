use std::io;
use std::sync::{Arc, Mutex};

use reify::{IndexKind, Schema, Table, TableSchema, Value};
use tracing_subscriber::fmt::MakeWriter;

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
    #[column(nullable)]
    pub role: Option<String>,
}

#[derive(Clone, Default)]
struct SharedWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

struct GuardedWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl<'a> MakeWriter<'a> for SharedWriter {
    type Writer = GuardedWriter;

    fn make_writer(&'a self) -> Self::Writer {
        GuardedWriter {
            buffer: Arc::clone(&self.buffer),
        }
    }
}

impl io::Write for GuardedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ── Table trait ─────────────────────────────────────────────────────

#[test]
fn table_name() {
    assert_eq!(User::table_name(), "users");
}

#[test]
fn column_names() {
    assert_eq!(User::column_names(), &["id", "email", "role"]);
}

#[test]
fn into_values() {
    let user = User {
        id: 1,
        email: "alice@example.com".into(),
        role: Some("admin".into()),
    };
    let vals = user.into_values();
    assert_eq!(vals.len(), 3);
    assert_eq!(vals[0], Value::I64(1));
    assert_eq!(vals[1], Value::String("alice@example.com".into()));
    assert_eq!(vals[2], Value::String("admin".into()));
}

#[test]
fn into_values_null() {
    let user = User {
        id: 2,
        email: "bob@example.com".into(),
        role: None,
    };
    let vals = user.into_values();
    assert_eq!(vals[2], Value::Null);
}

// ── Column constants ────────────────────────────────────────────────

#[test]
fn column_constants_exist() {
    assert_eq!(User::id.name, "id");
    assert_eq!(User::email.name, "email");
    assert_eq!(User::role.name, "role");
}

// ── SELECT builder ──────────────────────────────────────────────────

#[test]
fn select_all() {
    let (sql, params) = User::find().build();
    assert_eq!(sql, "SELECT * FROM users");
    assert!(params.is_empty());
}

#[test]
fn select_with_filter() {
    let (sql, params) = User::find()
        .filter(User::email.eq("alice@example.com"))
        .build();
    assert_eq!(sql, "SELECT * FROM users WHERE email = ?");
    assert_eq!(params, vec![Value::String("alice@example.com".into())]);
}

#[test]
fn select_with_multiple_filters() {
    let (sql, params) = User::find()
        .filter(User::id.gt(10i64))
        .filter(User::role.is_not_null())
        .build();
    assert_eq!(sql, "SELECT * FROM users WHERE id > ? AND role IS NOT NULL");
    assert_eq!(params, vec![Value::I64(10)]);
}

#[test]
fn select_with_order_limit_offset() {
    let (sql, _) = User::find()
        .order_by(reify::query::Order::Desc("id"))
        .limit(10)
        .offset(20)
        .build();
    assert_eq!(
        sql,
        "SELECT * FROM users ORDER BY id DESC LIMIT 10 OFFSET 20"
    );
}

#[test]
fn select_build_emits_trace() {
    let writer = SharedWriter::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(reify::tracing::Level::DEBUG)
        .with_writer(writer.clone())
        .without_time()
        .with_target(false)
        .finish();

    reify::tracing::subscriber::with_default(subscriber, || {
        let _ = User::find()
            .filter(User::email.eq("alice@example.com"))
            .build();
    });

    let output = String::from_utf8(writer.buffer.lock().unwrap().clone()).unwrap();
    assert!(output.contains("Built SQL query"));
    assert!(output.contains("operation=\"select\""));
    assert!(output.contains("table=\"users\""));
    assert!(output.contains("sql=SELECT * FROM users WHERE email = ?"));
}

#[test]
fn select_string_operators() {
    let (sql, params) = User::find()
        .filter(User::email.ends_with("@corp.io"))
        .build();
    assert_eq!(sql, "SELECT * FROM users WHERE email LIKE ?");
    assert_eq!(params, vec![Value::String("%@corp.io".into())]);
}

#[test]
fn select_nullable_operators() {
    let (sql, params) = User::find().filter(User::role.is_null()).build();
    assert_eq!(sql, "SELECT * FROM users WHERE role IS NULL");
    assert!(params.is_empty());
}

// ── INSERT builder ──────────────────────────────────────────────────

#[test]
fn insert_build() {
    let user = User {
        id: 0,
        email: "alice@example.com".into(),
        role: Some("member".into()),
    };
    let (sql, params) = User::insert(&user).build();
    assert_eq!(sql, "INSERT INTO users (id, email, role) VALUES (?, ?, ?)");
    assert_eq!(params.len(), 3);
}

// ── UPDATE builder ──────────────────────────────────────────────────

#[test]
fn update_build() {
    let (sql, params) = User::update()
        .set(User::role, "admin")
        .filter(User::id.eq(42i64))
        .build();
    assert_eq!(sql, "UPDATE users SET role = ? WHERE id = ?");
    assert_eq!(params, vec![Value::String("admin".into()), Value::I64(42)]);
}

#[test]
#[should_panic(expected = "UPDATE without WHERE is forbidden")]
fn update_without_where_panics() {
    User::update().set(User::role, "admin").build();
}

// ── DELETE builder ──────────────────────────────────────────────────

#[test]
fn delete_build() {
    let (sql, params) = User::delete().filter(User::id.eq(42i64)).build();
    assert_eq!(sql, "DELETE FROM users WHERE id = ?");
    assert_eq!(params, vec![Value::I64(42)]);
}

#[test]
#[should_panic(expected = "DELETE without WHERE is forbidden")]
fn delete_without_where_panics() {
    User::delete().build();
}

// ── Logical operators ───────────────────────────────────────────────

#[test]
fn or_condition() {
    let cond = User::email.eq("a@b.com").or(User::email.eq("c@d.com"));
    let (sql, params) = User::find().filter(cond).build();
    assert_eq!(sql, "SELECT * FROM users WHERE (email = ? OR email = ?)");
    assert_eq!(params.len(), 2);
}

// ── Schema builder API ──────────────────────────────────────────────

impl Schema for User {
    fn schema() -> TableSchema<Self> {
        reify::table::<Self>("users")
            .column(User::id, |c| c.primary_key().auto_increment())
            .column(User::email, |c| c.unique())
            .column(User::role, |c| c.nullable())
    }
}

#[test]
fn schema_table_name() {
    let schema = User::schema();
    assert_eq!(schema.name, "users");
}

#[test]
fn schema_column_count() {
    let schema = User::schema();
    assert_eq!(schema.columns.len(), 3);
}

#[test]
fn schema_primary_key() {
    let schema = User::schema();
    let id_col = &schema.columns[0];
    assert_eq!(id_col.name, "id");
    assert!(id_col.primary_key);
    assert!(id_col.auto_increment);
    assert!(!id_col.unique);
    assert!(!id_col.nullable);
}

#[test]
fn schema_unique() {
    let schema = User::schema();
    let email_col = &schema.columns[1];
    assert_eq!(email_col.name, "email");
    assert!(email_col.unique);
    assert!(!email_col.primary_key);
}

#[test]
fn schema_nullable() {
    let schema = User::schema();
    let role_col = &schema.columns[2];
    assert_eq!(role_col.name, "role");
    assert!(role_col.nullable);
    assert!(!role_col.unique);
}

#[test]
fn schema_default_value() {
    // Inline schema with a default
    let schema =
        reify::table::<User>("users").column(User::role, |c| c.nullable().default("member"));
    let role_col = &schema.columns[0];
    assert_eq!(role_col.default, Some("member".to_string()));
}

// ── Offset-based pagination ─────────────────────────────────────────

#[test]
fn paginate_page_1() {
    let paginated = User::find()
        .filter(User::role.is_not_null())
        .paginate(1, 25);
    let (data_sql, count_sql, params) = paginated.build();
    assert_eq!(
        data_sql,
        "SELECT * FROM users WHERE role IS NOT NULL LIMIT 25 OFFSET 0"
    );
    assert_eq!(
        count_sql,
        "SELECT COUNT(*) FROM users WHERE role IS NOT NULL"
    );
    assert!(params.is_empty());
}

#[test]
fn paginate_page_3() {
    let paginated = User::find().paginate(3, 10);
    let (data_sql, count_sql, _) = paginated.build();
    assert_eq!(data_sql, "SELECT * FROM users LIMIT 10 OFFSET 20");
    assert_eq!(count_sql, "SELECT COUNT(*) FROM users");
}

#[test]
fn paginate_page_info() {
    let paginated = User::find().paginate(2, 25);
    let page = paginated.page_info(100);
    assert_eq!(page.page, 2);
    assert_eq!(page.per_page, 25);
    assert_eq!(page.total_items, 100);
    assert_eq!(page.total_pages, 4);
    assert!(page.has_next);
    assert!(page.has_prev);
}

#[test]
fn paginate_last_page() {
    let paginated = User::find().paginate(4, 25);
    let page = paginated.page_info(100);
    assert!(!page.has_next);
    assert!(page.has_prev);
}

#[test]
fn paginate_single_page() {
    let paginated = User::find().paginate(1, 50);
    let page = paginated.page_info(10);
    assert_eq!(page.total_pages, 1);
    assert!(!page.has_next);
    assert!(!page.has_prev);
}

#[test]
#[should_panic(expected = "Page number must be >= 1")]
fn paginate_page_zero_panics() {
    User::find().paginate(0, 25);
}

// ── Cursor-based pagination ─────────────────────────────────────────

#[test]
fn cursor_after() {
    let page = User::find()
        .filter(User::role.is_not_null())
        .after(User::id, 150i64, 25);
    let (sql, params) = page.build();
    assert_eq!(
        sql,
        "SELECT * FROM users WHERE role IS NOT NULL AND id > ? ORDER BY id ASC LIMIT 26"
    );
    assert_eq!(params, vec![Value::I64(150)]);
}

#[test]
fn cursor_before() {
    let page = User::find().before(User::id, 100i64, 25);
    let (sql, params) = page.build();
    assert_eq!(
        sql,
        "SELECT * FROM users WHERE id < ? ORDER BY id DESC LIMIT 26"
    );
    assert_eq!(params, vec![Value::I64(100)]);
}

#[test]
fn cursor_first_page() {
    // No cursor value → first page
    let page = User::find().after(User::id, 0i64, 10);
    let (sql, _) = page.build();
    assert_eq!(
        sql,
        "SELECT * FROM users WHERE id > ? ORDER BY id ASC LIMIT 11"
    );
}

#[test]
fn cursor_has_more() {
    let page = User::find().after(User::id, 0i64, 25);
    assert!(page.has_more(26)); // got limit+1 rows → more exist
    assert!(!page.has_more(25)); // got exactly limit → no more
    assert!(!page.has_more(10)); // got less → no more
}

// ── Index support (macro) ───────────────────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "products")]
pub struct Product {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(index)]
    pub sku: String,
    #[column(index)]
    pub category_id: i64,
    pub name: String,
}

#[test]
fn macro_single_column_indexes() {
    let indexes = Product::indexes();
    assert_eq!(indexes.len(), 2);

    assert_eq!(indexes[0].name, Some("idx_products_sku".to_string()));
    assert_eq!(indexes[0].columns, vec!["sku"]);
    assert!(!indexes[0].unique);

    assert_eq!(
        indexes[1].name,
        Some("idx_products_category_id".to_string())
    );
    assert_eq!(indexes[1].columns, vec!["category_id"]);
}

#[derive(Table, Debug, Clone)]
#[table(
    name = "orders",
    index(columns("user_id", "created_at")),
    index(columns("email", "status"), unique, name = "idx_orders_email_status")
)]
pub struct Order {
    #[column(primary_key)]
    pub id: i64,
    pub user_id: i64,
    pub email: String,
    pub status: String,
    pub created_at: i64,
}

#[test]
fn macro_composite_indexes() {
    let indexes = Order::indexes();
    assert_eq!(indexes.len(), 2);

    // Auto-named composite index
    assert_eq!(
        indexes[0].name,
        Some("idx_orders_user_id_created_at".to_string())
    );
    assert_eq!(indexes[0].columns, vec!["user_id", "created_at"]);
    assert!(!indexes[0].unique);

    // Explicitly named unique composite index
    assert_eq!(indexes[1].name, Some("idx_orders_email_status".to_string()));
    assert_eq!(indexes[1].columns, vec!["email", "status"]);
    assert!(indexes[1].unique);
}

#[derive(Table, Debug, Clone)]
#[table(name = "events", index(columns("tenant_id", "created_at")))]
pub struct Event {
    #[column(primary_key)]
    pub id: i64,
    #[column(index)]
    pub tenant_id: i64,
    pub created_at: i64,
}

#[test]
fn macro_mixed_single_and_composite_indexes() {
    let indexes = Event::indexes();
    assert_eq!(indexes.len(), 2);
    // Single-column from #[column(index)] comes first
    assert_eq!(indexes[0].columns, vec!["tenant_id"]);
    // Composite from #[table(index(...))] comes second
    assert_eq!(indexes[1].columns, vec!["tenant_id", "created_at"]);
}

#[test]
fn macro_no_indexes() {
    // User has no #[column(index)] and no #[table(index(...))]
    let indexes = User::indexes();
    assert!(indexes.is_empty());
}

// ── Partial indexes (macro) ────────────────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(
    name = "invoices",
    index(columns("customer_id"), predicate = "status = 'active'"),
    index(
        columns("email"),
        unique,
        predicate = "deleted_at IS NULL",
        name = "idx_invoices_live_email"
    )
)]
pub struct Invoice {
    #[column(primary_key)]
    pub id: i64,
    pub customer_id: i64,
    pub email: String,
    pub status: String,
    pub deleted_at: Option<String>,
}

#[test]
fn macro_partial_index() {
    let indexes = Invoice::indexes();
    assert_eq!(indexes.len(), 2);

    // Partial index with predicate
    assert_eq!(indexes[0].columns, vec!["customer_id"]);
    assert_eq!(indexes[0].predicate, Some("status = 'active'".to_string()));
    assert!(!indexes[0].unique);
}

#[test]
fn macro_unique_partial_index() {
    let indexes = Invoice::indexes();

    // Unique partial index
    assert_eq!(indexes[1].columns, vec!["email"]);
    assert!(indexes[1].unique);
    assert_eq!(indexes[1].predicate, Some("deleted_at IS NULL".to_string()));
    assert_eq!(indexes[1].name, Some("idx_invoices_live_email".to_string()));
}

#[test]
fn macro_no_predicate() {
    // Order indexes have no predicate
    let indexes = Order::indexes();
    for idx in &indexes {
        assert!(idx.predicate.is_none());
    }
}

// ── Partial indexes (builder) ──────────────────────────────────────

#[test]
fn builder_partial_index() {
    let schema = reify::table::<Event>("events").index(|idx| {
        idx.column(Event::tenant_id)
            .column(Event::created_at)
            .predicate("tenant_id IS NOT NULL")
    });

    assert_eq!(schema.indexes.len(), 1);
    assert_eq!(
        schema.indexes[0].predicate,
        Some("tenant_id IS NOT NULL".to_string())
    );
    assert_eq!(schema.indexes[0].columns, vec!["tenant_id", "created_at"]);
}

#[test]
fn builder_unique_partial_index() {
    let schema = reify::table::<Product>("products").index(|idx| {
        idx.column(Product::sku)
            .unique()
            .predicate("deleted_at IS NULL")
            .name("idx_products_live_sku")
    });

    let idx = &schema.indexes[0];
    assert!(idx.unique);
    assert_eq!(idx.predicate, Some("deleted_at IS NULL".to_string()));
    assert_eq!(idx.name, Some("idx_products_live_sku".to_string()));
}

// ── Index support (builder) ─────────────────────────────────────────

#[test]
fn builder_single_index() {
    let schema = reify::table::<Product>("products")
        .column(Product::id, |c| c.primary_key().auto_increment())
        .column(Product::sku, |c| c)
        .index(|idx| idx.column(Product::sku));

    assert_eq!(schema.indexes.len(), 1);
    assert_eq!(schema.indexes[0].columns, vec!["sku"]);
    assert!(!schema.indexes[0].unique);
    assert_eq!(schema.indexes[0].kind, IndexKind::BTree);
}

#[test]
fn builder_composite_unique_index() {
    let schema = reify::table::<Order>("orders")
        .column(Order::id, |c| c.primary_key())
        .index(|idx| {
            idx.column(Order::email)
                .column(Order::status)
                .unique()
                .name("idx_orders_email_status")
        });

    assert_eq!(schema.indexes.len(), 1);
    let idx = &schema.indexes[0];
    assert_eq!(idx.columns, vec!["email", "status"]);
    assert!(idx.unique);
    assert_eq!(idx.name, Some("idx_orders_email_status".to_string()));
}

#[test]
fn builder_index_kinds() {
    let schema = reify::table::<Product>("products")
        .index(|idx| idx.column(Product::name).hash())
        .index(|idx| idx.column(Product::name).gin())
        .index(|idx| idx.column(Product::name).gist());

    assert_eq!(schema.indexes[0].kind, IndexKind::Hash);
    assert_eq!(schema.indexes[1].kind, IndexKind::Gin);
    assert_eq!(schema.indexes[2].kind, IndexKind::Gist);
}

#[test]
fn builder_multiple_indexes() {
    let schema = reify::table::<Event>("events")
        .column(Event::id, |c| c.primary_key())
        .index(|idx| idx.column(Event::tenant_id))
        .index(|idx| idx.column(Event::tenant_id).column(Event::created_at));

    assert_eq!(schema.indexes.len(), 2);
    assert_eq!(schema.indexes[0].columns, vec!["tenant_id"]);
    assert_eq!(schema.indexes[1].columns, vec!["tenant_id", "created_at"]);
}
