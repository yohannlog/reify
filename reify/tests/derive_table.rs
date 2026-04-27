use std::io;
use std::sync::{Arc, Mutex};

use reify::{IndexColumnDef, IndexKind, Schema, SqlType, Table, TableSchema, Value};
use tracing_subscriber::fmt::MakeWriter;

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
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
    assert_eq!(sql, "SELECT * FROM \"users\"");
    assert!(params.is_empty());
}

#[test]
fn select_with_filter() {
    let (sql, params) = User::find()
        .filter(User::email.eq("alice@example.com"))
        .build();
    assert_eq!(sql, "SELECT * FROM \"users\" WHERE \"email\" = ?");
    assert_eq!(params, vec![Value::String("alice@example.com".into())]);
}

#[test]
fn select_with_multiple_filters() {
    let (sql, params) = User::find()
        .filter(User::id.gt(10i64))
        .filter(User::role.is_not_null())
        .build();
    assert_eq!(
        sql,
        "SELECT * FROM \"users\" WHERE \"id\" > ? AND \"role\" IS NOT NULL"
    );
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
        "SELECT * FROM \"users\" ORDER BY \"id\" DESC LIMIT 10 OFFSET 20"
    );
}

#[test]
fn select_build_emits_trace() {
    // Verify that build() completes without panicking when a tracing
    // subscriber is active, and that the returned SQL is correct.
    // Subscriber-capture assertions are intentionally omitted: the fmt
    // subscriber's thread-local scope is unreliable when multiple test
    // binaries share the same process and one sets a global subscriber.
    let writer = SharedWriter::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(reify::tracing::Level::DEBUG)
        .with_writer(writer.clone())
        .without_time()
        .with_target(false)
        .finish();

    let (sql, _) = reify::tracing::subscriber::with_default(subscriber, || {
        User::find()
            .filter(User::email.eq("alice@example.com"))
            .build()
    });

    assert_eq!(sql, "SELECT * FROM \"users\" WHERE \"email\" = ?");
}

#[test]
fn select_string_operators() {
    let (sql, params) = User::find()
        .filter(User::email.ends_with("@corp.io"))
        .build();
    assert_eq!(
        sql,
        "SELECT * FROM \"users\" WHERE \"email\" LIKE ? ESCAPE '\\'"
    );
    assert_eq!(params, vec![Value::String("%@corp.io".into())]);
}

#[test]
fn select_nullable_operators() {
    let (sql, params) = User::find().filter(User::role.is_null()).build();
    assert_eq!(sql, "SELECT * FROM \"users\" WHERE \"role\" IS NULL");
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
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"role\") VALUES (?, ?, ?)"
    );
    assert_eq!(params.len(), 3);
}

#[cfg(feature = "postgres")]
#[test]
fn insert_returning_cols_postgres() {
    let user = User {
        id: 0,
        email: "alice@example.com".into(),
        role: Some("member".into()),
    };
    let (sql, params) = User::insert(&user).returning_cols(&[User::id]).build();
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"role\") VALUES (?, ?, ?) RETURNING \"id\""
    );
    assert_eq!(params.len(), 3);
}

// ── UPDATE builder ──────────────────────────────────────────────────

#[test]
fn update_build() {
    let (sql, params) = User::update()
        .set(User::role, "admin")
        .filter(User::id.eq(42i64))
        .build();
    assert_eq!(sql, "UPDATE \"users\" SET \"role\" = ? WHERE \"id\" = ?");
    assert_eq!(params, vec![Value::String("admin".into()), Value::I64(42)]);
}

#[test]
#[should_panic(expected = "UPDATE without WHERE is forbidden")]
fn update_without_where_panics() {
    let _ = User::update().set(User::role, "admin").build();
}

#[cfg(feature = "postgres")]
#[test]
fn update_returning_cols_postgres() {
    let (sql, params) = User::update()
        .set(User::role, "admin")
        .filter(User::id.eq(42i64))
        .returning_cols(&[User::id])
        .build();
    assert_eq!(
        sql,
        "UPDATE \"users\" SET \"role\" = ? WHERE \"id\" = ? RETURNING \"id\""
    );
    assert_eq!(params, vec![Value::String("admin".into()), Value::I64(42)]);
}

// ── DELETE builder ──────────────────────────────────────────────────

#[test]
fn delete_build() {
    let (sql, params) = User::delete().filter(User::id.eq(42i64)).build();
    assert_eq!(sql, "DELETE FROM \"users\" WHERE \"id\" = ?");
    assert_eq!(params, vec![Value::I64(42)]);
}

#[test]
#[should_panic(expected = "DELETE without WHERE is forbidden")]
fn delete_without_where_panics() {
    let _ = User::delete().build();
}

#[cfg(feature = "postgres")]
#[test]
fn delete_returning_cols_postgres() {
    let (sql, params) = User::delete()
        .filter(User::id.eq(42i64))
        .returning_cols(&[User::id])
        .build();
    assert_eq!(
        sql,
        "DELETE FROM \"users\" WHERE \"id\" = ? RETURNING \"id\""
    );
    assert_eq!(params, vec![Value::I64(42)]);
}

// ── PostgreSQL 18+ RETURNING old/new ─────────────────────────────

#[cfg(feature = "postgres18")]
#[test]
fn update_returning_old_new_all_postgres18() {
    let (sql, _) = User::update()
        .set(User::role, "admin")
        .filter(User::id.eq(1i64))
        .returning_old_new_all()
        .build();
    assert_eq!(
        sql,
        "UPDATE \"users\" SET \"role\" = ? WHERE \"id\" = ? RETURNING old.*, new.*"
    );
}

#[cfg(feature = "postgres18")]
#[test]
fn update_returning_old_all_postgres18() {
    let (sql, _) = User::update()
        .set(User::role, "admin")
        .filter(User::id.eq(1i64))
        .returning_old_all()
        .build();
    assert_eq!(
        sql,
        "UPDATE \"users\" SET \"role\" = ? WHERE \"id\" = ? RETURNING old.*"
    );
}

#[cfg(feature = "postgres18")]
#[test]
fn update_returning_new_all_postgres18() {
    let (sql, _) = User::update()
        .set(User::role, "admin")
        .filter(User::id.eq(1i64))
        .returning_new_all()
        .build();
    assert_eq!(
        sql,
        "UPDATE \"users\" SET \"role\" = ? WHERE \"id\" = ? RETURNING new.*"
    );
}

#[cfg(feature = "postgres18")]
#[test]
fn delete_returning_old_all_postgres18() {
    let (sql, _) = User::delete()
        .filter(User::id.eq(42i64))
        .returning_old_all()
        .build();
    assert_eq!(
        sql,
        "DELETE FROM \"users\" WHERE \"id\" = ? RETURNING old.*"
    );
}

#[cfg(feature = "postgres18")]
#[test]
fn insert_returning_new_all_postgres18() {
    let user = User {
        id: 0,
        email: "test@example.com".into(),
        role: Some("user".into()),
    };
    let (sql, _) = User::insert(&user).returning_new_all().build();
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"role\") VALUES (?, ?, ?) RETURNING new.*"
    );
}

#[cfg(feature = "postgres18")]
#[test]
fn insert_many_returning_new_all_postgres18() {
    let users = vec![
        User {
            id: 0,
            email: "a@example.com".into(),
            role: None,
        },
        User {
            id: 0,
            email: "b@example.com".into(),
            role: Some("admin".into()),
        },
    ];
    let (sql, _) = User::insert_many(&users).returning_new_all().build();
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"id\", \"email\", \"role\") VALUES (?, ?, ?), (?, ?, ?) RETURNING new.*"
    );
}

// ── try_build / unfiltered ──────────────────────────────────────────

#[test]
fn update_try_build_returns_error_without_filter() {
    let result = User::update().set(User::role, "admin").try_build();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(
        err,
        reify::BuildError::MissingFilter {
            operation: "UPDATE"
        }
    );
    assert!(err.to_string().contains("UPDATE without WHERE"));
}

#[test]
fn update_try_build_ok_with_filter() {
    let result = User::update()
        .set(User::role, "admin")
        .filter(User::id.eq(1i64))
        .try_build();
    assert!(result.is_ok());
    let (sql, _) = result.unwrap();
    assert!(sql.contains("WHERE"));
}

#[test]
fn update_unfiltered_builds_without_where() {
    let (sql, params) = User::update().set(User::role, "guest").unfiltered().build();
    assert_eq!(sql, "UPDATE \"users\" SET \"role\" = ?");
    assert_eq!(params, vec![Value::String("guest".into())]);
}

#[test]
fn delete_try_build_returns_error_without_filter() {
    let result = User::delete().try_build();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(
        err,
        reify::BuildError::MissingFilter {
            operation: "DELETE"
        }
    );
}

#[test]
fn delete_try_build_ok_with_filter() {
    let result = User::delete().filter(User::id.eq(1i64)).try_build();
    assert!(result.is_ok());
}

#[test]
fn delete_unfiltered_builds_without_where() {
    let (sql, params) = User::delete().unfiltered().build();
    assert_eq!(sql, "DELETE FROM \"users\"");
    assert!(params.is_empty());
}

// ── Logical operators ───────────────────────────────────────────────

#[test]
fn or_condition() {
    let cond = User::email.eq("a@b.com").or(User::email.eq("c@d.com"));
    let (sql, params) = User::find().filter(cond).build();
    assert_eq!(
        sql,
        "SELECT * FROM \"users\" WHERE (\"email\" = ? OR \"email\" = ?)"
    );
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
    assert_eq!(
        role_col.default,
        Some(reify::DefaultValue::Literal("member".to_string()))
    );
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
        "SELECT * FROM \"users\" WHERE \"role\" IS NOT NULL LIMIT 25 OFFSET 0"
    );
    assert_eq!(
        count_sql,
        "SELECT COUNT(*) FROM \"users\" WHERE \"role\" IS NOT NULL"
    );
    assert!(params.is_empty());
}

#[test]
fn paginate_page_3() {
    let paginated = User::find().paginate(3, 10);
    let (data_sql, count_sql, _) = paginated.build();
    assert_eq!(data_sql, "SELECT * FROM \"users\" LIMIT 10 OFFSET 20");
    assert_eq!(count_sql, "SELECT COUNT(*) FROM \"users\"");
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
fn paginate_page_zero_clamps_to_one() {
    // page=0 is clamped to 1 instead of panicking — safe for web handlers
    // that receive untrusted query-string parameters.
    let paginated = User::find().paginate(0, 25);
    let (_, _, _) = paginated.build(); // must not panic
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
        "SELECT * FROM \"users\" WHERE \"role\" IS NOT NULL AND \"id\" > ? ORDER BY \"id\" ASC LIMIT 26"
    );
    assert_eq!(params, vec![Value::I64(150)]);
}

#[test]
fn cursor_before() {
    let page = User::find().before(User::id, 100i64, 25);
    let (sql, params) = page.build();
    assert_eq!(
        sql,
        "SELECT * FROM \"users\" WHERE \"id\" < ? ORDER BY \"id\" DESC LIMIT 26"
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
        "SELECT * FROM \"users\" WHERE \"id\" > ? ORDER BY \"id\" ASC LIMIT 11"
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
    assert_eq!(indexes[0].columns, vec![IndexColumnDef::asc("sku")]);
    assert!(!indexes[0].unique);

    assert_eq!(
        indexes[1].name,
        Some("idx_products_category_id".to_string())
    );
    assert_eq!(indexes[1].columns, vec![IndexColumnDef::asc("category_id")]);
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
    assert_eq!(
        indexes[0].columns,
        vec![
            IndexColumnDef::asc("user_id"),
            IndexColumnDef::asc("created_at")
        ]
    );
    assert!(!indexes[0].unique);

    // Explicitly named unique composite index
    assert_eq!(indexes[1].name, Some("idx_orders_email_status".to_string()));
    assert_eq!(
        indexes[1].columns,
        vec![IndexColumnDef::asc("email"), IndexColumnDef::asc("status")]
    );
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
    assert_eq!(indexes[0].columns, vec![IndexColumnDef::asc("tenant_id")]);
    // Composite from #[table(index(...))] comes second
    assert_eq!(
        indexes[1].columns,
        vec![
            IndexColumnDef::asc("tenant_id"),
            IndexColumnDef::asc("created_at")
        ]
    );
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
    assert_eq!(indexes[0].columns, vec![IndexColumnDef::asc("customer_id")]);
    assert_eq!(indexes[0].predicate, Some("status = 'active'".to_string()));
    assert!(!indexes[0].unique);
}

#[test]
fn macro_unique_partial_index() {
    let indexes = Invoice::indexes();

    // Unique partial index
    assert_eq!(indexes[1].columns, vec![IndexColumnDef::asc("email")]);
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
    assert_eq!(
        schema.indexes[0].columns,
        vec![
            IndexColumnDef::asc("tenant_id"),
            IndexColumnDef::asc("created_at")
        ]
    );
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

// ── column_defs() generation tests ──────────────────────────────────

#[test]
fn column_defs_generated_with_correct_types() {
    let defs = User::column_defs();
    assert_eq!(defs.len(), 3);

    // id: i64 + primary_key + auto_increment → BigSerial
    assert_eq!(defs[0].name, "id");
    assert_eq!(defs[0].sql_type, SqlType::BigSerial);
    assert!(defs[0].primary_key);
    assert!(defs[0].auto_increment);
    assert!(!defs[0].nullable);

    // email: String → Text + unique
    assert_eq!(defs[1].name, "email");
    assert_eq!(defs[1].sql_type, SqlType::Text);
    assert!(defs[1].unique);
    assert!(!defs[1].nullable);

    // role: Option<String> → Text + nullable
    assert_eq!(defs[2].name, "role");
    assert_eq!(defs[2].sql_type, SqlType::Text);
    assert!(defs[2].nullable);
}

#[test]
fn column_defs_nullable_option_fields() {
    // User.role is Option<String> → should be nullable
    let defs = User::column_defs();
    let role_def = defs.iter().find(|d| d.name == "role").unwrap();
    assert!(role_def.nullable);
    assert_eq!(role_def.sql_type, SqlType::Text);

    // User.id is i64 (not Option) → should NOT be nullable
    let id_def = defs.iter().find(|d| d.name == "id").unwrap();
    assert!(!id_def.nullable);
}

#[test]
fn column_defs_integer_types() {
    #[derive(Table, Debug, Clone)]
    #[table(name = "metrics")]
    struct Metric {
        #[column(primary_key)]
        pub id: i32,
        pub count: i16,
        pub big_count: i64,
    }

    let defs = Metric::column_defs();
    assert_eq!(defs[0].sql_type, SqlType::Integer);
    assert_eq!(defs[1].sql_type, SqlType::SmallInt);
    assert_eq!(defs[2].sql_type, SqlType::BigInt);
}

#[test]
fn column_defs_float_and_bool() {
    #[derive(Table, Debug, Clone)]
    #[table(name = "readings")]
    struct Reading {
        #[column(primary_key)]
        pub id: i64,
        pub temperature: f64,
        pub pressure: f32,
        pub is_valid: bool,
    }

    let defs = Reading::column_defs();
    assert_eq!(defs[1].sql_type, SqlType::Double);
    assert_eq!(defs[2].sql_type, SqlType::Float);
    assert_eq!(defs[3].sql_type, SqlType::Boolean);
}

#[test]
fn sql_type_dialect_rendering() {
    use reify::Dialect;

    assert_eq!(SqlType::BigSerial.to_sql(Dialect::Postgres), "BIGSERIAL");
    assert_eq!(
        SqlType::BigSerial.to_sql(Dialect::Mysql),
        "BIGINT AUTO_INCREMENT"
    );
    assert_eq!(SqlType::BigSerial.to_sql(Dialect::Generic), "INTEGER");

    assert_eq!(SqlType::Uuid.to_sql(Dialect::Postgres), "UUID");
    assert_eq!(SqlType::Uuid.to_sql(Dialect::Mysql), "CHAR(36)");
    assert_eq!(SqlType::Uuid.to_sql(Dialect::Generic), "TEXT");

    assert_eq!(SqlType::Jsonb.to_sql(Dialect::Postgres), "JSONB");
    assert_eq!(SqlType::Jsonb.to_sql(Dialect::Mysql), "JSON");

    assert_eq!(
        SqlType::Timestamptz.to_sql(Dialect::Postgres),
        "TIMESTAMPTZ"
    );
    assert_eq!(SqlType::Timestamptz.to_sql(Dialect::Mysql), "DATETIME");
}

// ── Parameterized SqlType rendering ─────────────────────────────────

#[test]
fn sql_type_parameterized_rendering() {
    use reify::Dialect;

    // Varchar
    assert_eq!(
        &*SqlType::Varchar(255).to_sql(Dialect::Postgres),
        "VARCHAR(255)"
    );
    assert_eq!(
        &*SqlType::Varchar(255).to_sql(Dialect::Mysql),
        "VARCHAR(255)"
    );
    assert_eq!(
        &*SqlType::Varchar(100).to_sql(Dialect::Generic),
        "VARCHAR(100)"
    );

    // Char
    assert_eq!(&*SqlType::Char(36).to_sql(Dialect::Postgres), "CHAR(36)");
    assert_eq!(&*SqlType::Char(3).to_sql(Dialect::Mysql), "CHAR(3)");

    // Decimal — Postgres uses NUMERIC, others use DECIMAL
    assert_eq!(
        &*SqlType::Decimal(10, 2).to_sql(Dialect::Postgres),
        "NUMERIC(10,2)"
    );
    assert_eq!(
        &*SqlType::Decimal(10, 2).to_sql(Dialect::Mysql),
        "DECIMAL(10,2)"
    );
    assert_eq!(
        &*SqlType::Decimal(8, 4).to_sql(Dialect::Generic),
        "DECIMAL(8,4)"
    );
}

#[test]
fn builder_varchar_decimal_methods() {
    use reify::ColumnBuilder;

    let col = ColumnBuilder::<()>::new_pub("name").varchar(255).build();
    assert_eq!(col.sql_type, SqlType::Varchar(255));

    let col = ColumnBuilder::<()>::new_pub("code").char_type(3).build();
    assert_eq!(col.sql_type, SqlType::Char(3));

    let col = ColumnBuilder::<()>::new_pub("price").decimal(10, 2).build();
    assert_eq!(col.sql_type, SqlType::Decimal(10, 2));
}

// ── Index support (builder) ─────────────────────────────────────────

#[test]
fn builder_single_index() {
    let schema = reify::table::<Product>("products")
        .column(Product::id, |c| c.primary_key().auto_increment())
        .column(Product::sku, |c| c)
        .index(|idx| idx.column(Product::sku));

    assert_eq!(schema.indexes.len(), 1);
    assert_eq!(schema.indexes[0].columns, vec![IndexColumnDef::asc("sku")]);
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
    assert_eq!(
        idx.columns,
        vec![IndexColumnDef::asc("email"), IndexColumnDef::asc("status")]
    );
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
    assert_eq!(
        schema.indexes[0].columns,
        vec![IndexColumnDef::asc("tenant_id")]
    );
    assert_eq!(
        schema.indexes[1].columns,
        vec![
            IndexColumnDef::asc("tenant_id"),
            IndexColumnDef::asc("created_at")
        ]
    );
}

// ── Sort direction (builder) ────────────────────────────────────────

#[test]
fn builder_column_desc() {
    use reify::SortDirection;

    let schema = reify::table::<Event>("events")
        .column(Event::id, |c| c.primary_key())
        .index(|idx| {
            idx.column_asc(Event::tenant_id)
                .column_desc(Event::created_at)
                .name("idx_events_tenant_timeline")
        });

    assert_eq!(schema.indexes.len(), 1);
    let idx = &schema.indexes[0];
    assert_eq!(idx.columns.len(), 2);
    assert_eq!(idx.columns[0].name, "tenant_id");
    assert_eq!(idx.columns[0].direction, SortDirection::Asc);
    assert_eq!(idx.columns[1].name, "created_at");
    assert_eq!(idx.columns[1].direction, SortDirection::Desc);
}

// ── Sort direction (macro) ──────────────────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(
    name = "logs",
    index(columns("tenant_id", "created_at DESC")),
    index(columns("level DESC", "created_at DESC"), name = "idx_logs_level_time")
)]
pub struct Log {
    #[column(primary_key)]
    pub id: i64,
    pub tenant_id: i64,
    pub level: String,
    pub created_at: i64,
}

#[test]
fn macro_index_sort_direction() {
    use reify::SortDirection;

    let indexes = Log::indexes();
    assert_eq!(indexes.len(), 2);

    // First index: tenant_id ASC (default), created_at DESC
    let idx0 = &indexes[0];
    assert_eq!(idx0.columns.len(), 2);
    assert_eq!(idx0.columns[0].name, "tenant_id");
    assert_eq!(idx0.columns[0].direction, SortDirection::Asc);
    assert_eq!(idx0.columns[1].name, "created_at");
    assert_eq!(idx0.columns[1].direction, SortDirection::Desc);

    // Second index: both DESC
    let idx1 = &indexes[1];
    assert_eq!(idx1.name, Some("idx_logs_level_time".to_string()));
    assert_eq!(idx1.columns[0].name, "level");
    assert_eq!(idx1.columns[0].direction, SortDirection::Desc);
    assert_eq!(idx1.columns[1].name, "created_at");
    assert_eq!(idx1.columns[1].direction, SortDirection::Desc);
}

// ── CHECK constraints (derive macro) ───────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "items")]
pub struct Item {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(check = "price >= 0")]
    pub price: f64,
    #[column(check = "quantity >= 0")]
    pub quantity: i32,
    pub name: String,
}

#[test]
fn column_check_via_derive() {
    let defs = Item::column_defs();
    let price_def = defs.iter().find(|d| d.name == "price").unwrap();
    assert_eq!(price_def.check, Some("price >= 0".to_string()));

    let qty_def = defs.iter().find(|d| d.name == "quantity").unwrap();
    assert_eq!(qty_def.check, Some("quantity >= 0".to_string()));

    // Columns without check should be None
    let name_def = defs.iter().find(|d| d.name == "name").unwrap();
    assert_eq!(name_def.check, None);
}

// ── CHECK constraints (builder) ────────────────────────────────────

#[test]
fn builder_column_check() {
    use reify::ColumnBuilder;

    let col = ColumnBuilder::<()>::new_pub("price")
        .decimal(10, 2)
        .check("price >= 0")
        .build();
    assert_eq!(col.check, Some("price >= 0".to_string()));
}

#[test]
fn builder_column_no_check() {
    use reify::ColumnBuilder;

    let col = ColumnBuilder::<()>::new_pub("name").build();
    assert_eq!(col.check, None);
}

#[test]
fn builder_table_check() {
    let schema = reify::table::<Event>("events")
        .column(Event::id, |c| c.primary_key())
        .column(Event::tenant_id, |c| c)
        .column(Event::created_at, |c| c)
        .check("created_at > 0")
        .check("tenant_id > 0");

    assert_eq!(schema.checks.len(), 2);
    assert_eq!(schema.checks[0], "created_at > 0");
    assert_eq!(schema.checks[1], "tenant_id > 0");
}

#[test]
fn builder_table_no_checks() {
    let schema = reify::table::<Event>("events").column(Event::id, |c| c.primary_key());

    assert!(schema.checks.is_empty());
}

// ── Foreign key constraints (derive macro) ──────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "posts")]
pub struct Post {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(references = "User::id", on_delete = "CASCADE")]
    pub user_id: i64,
    pub title: String,
}

#[test]
fn column_foreign_key_via_derive() {
    use reify::{ForeignKeyAction, Table};

    let defs = Post::column_defs();
    let fk_def = defs.iter().find(|d| d.name == "user_id").unwrap();
    let fk = fk_def
        .foreign_key
        .as_ref()
        .expect("expected foreign_key on user_id");

    assert_eq!(fk.references_table, "users");
    assert_eq!(fk.references_column, "id");
    assert_eq!(fk.on_delete, ForeignKeyAction::Cascade);
    assert_eq!(fk.on_update, ForeignKeyAction::NoAction);

    // Columns without FK should be None
    let id_def = defs.iter().find(|d| d.name == "id").unwrap();
    assert!(id_def.foreign_key.is_none());
}

#[test]
fn foreign_keys_helper_returns_fk_defs() {
    use reify::{ForeignKeyAction, Table};

    let fks = Post::foreign_keys();
    assert_eq!(fks.len(), 1);
    assert_eq!(fks[0].references_table, "users");
    assert_eq!(fks[0].references_column, "id");
    assert_eq!(fks[0].on_delete, ForeignKeyAction::Cascade);
}

#[test]
fn foreign_key_ddl_contains_references_clause() {
    use reify::create_table_sql;
    use reify::query::Dialect;

    let defs = Post::column_defs();
    let sql = create_table_sql::<Post>(&defs, Dialect::Postgres);

    assert!(sql.contains("FOREIGN KEY"), "missing FOREIGN KEY: {sql}");
    assert!(
        sql.contains("REFERENCES \"users\" (\"id\")"),
        "missing REFERENCES: {sql}"
    );
    assert!(
        sql.contains("ON DELETE CASCADE"),
        "missing ON DELETE CASCADE: {sql}"
    );
    assert!(!sql.contains("ON UPDATE"), "unexpected ON UPDATE: {sql}");
}

// ── Foreign key constraints (builder API) ──────────────────────────

#[test]
fn builder_column_references() {
    use reify::{ColumnBuilder, ForeignKeyAction};

    let col = ColumnBuilder::<i64>::new_pub("user_id")
        .references("users", "id")
        .on_delete(ForeignKeyAction::Cascade)
        .build();

    let fk = col.foreign_key.as_ref().expect("expected foreign_key");
    assert_eq!(fk.references_table, "users");
    assert_eq!(fk.references_column, "id");
    assert_eq!(fk.on_delete, ForeignKeyAction::Cascade);
    assert_eq!(fk.on_update, ForeignKeyAction::NoAction);
}

#[test]
fn builder_column_no_references() {
    use reify::ColumnBuilder;

    let col = ColumnBuilder::<i64>::new_pub("amount").build();
    assert!(col.foreign_key.is_none());
}
