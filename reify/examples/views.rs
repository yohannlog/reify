use reify::{Migration, MigrationContext, SqlType, Table, View, ViewSchemaDef, query::Order};

// ═══════════════════════════════════════════════════════════════════
//  Source tables
// ═══════════════════════════════════════════════════════════════════

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
    pub role: Option<String>,
    pub deleted_at: Option<String>,
}

#[derive(Table, Debug, Clone)]
#[table(name = "orders")]
pub struct Order_ {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub user_id: i64,
    pub total: f64,
    pub status: String,
}

// ═══════════════════════════════════════════════════════════════════
//  1. MACRO APPROACH — #[derive(View)] with raw SQL query
// ═══════════════════════════════════════════════════════════════════
//
//  Best for: simple views where the SQL is known at compile time.
//  The query is embedded in the attribute — zero boilerplate.

#[derive(View, Debug, Clone)]
#[view(
    name = "active_users",
    query = "SELECT id, email, role FROM users WHERE deleted_at IS NULL"
)]
pub struct ActiveUser {
    pub id: i64,
    pub email: String,
    pub role: Option<String>,
}

// Another macro view — a cross-table summary using raw SQL
#[derive(View, Debug, Clone)]
#[view(
    name = "user_order_stats",
    query = "SELECT u.id, u.email, COUNT(o.id) AS order_count, COALESCE(SUM(o.total), 0) AS total_spent FROM users u LEFT JOIN orders o ON o.user_id = u.id WHERE u.deleted_at IS NULL GROUP BY u.id, u.email"
)]
pub struct UserOrderStats {
    pub id: i64,
    pub email: String,
    pub order_count: i64,
    pub total_spent: f64,
}

// ═══════════════════════════════════════════════════════════════════
//  2. BUILDER APPROACH — ViewSchema with typed SelectBuilder
// ═══════════════════════════════════════════════════════════════════
//
//  Best for: views whose query references existing Table columns.
//  The SelectBuilder is compile-time checked — rename a column and
//  the view definition breaks at compilation, not at runtime.

#[derive(View, Debug, Clone)]
#[view(name = "admin_users")]
pub struct AdminUser {
    pub id: i64,
    pub email: String,
}

// Implement ViewSchemaDef to define the view via the builder API.
// The query is built from User::find() — typed columns, checked at compile time.
impl ViewSchemaDef for AdminUser {
    fn view_schema() -> reify::ViewSchema<Self> {
        reify::view_schema::<Self>("admin_users")
            .column(AdminUser::id, |c| c.sql_type(SqlType::BigInt))
            .column(AdminUser::email, |c| c.sql_type(SqlType::Text))
            .query(
                User::find()
                    .select(&["id", "email"])
                    .filter(User::role.eq("admin"))
                    .filter(User::deleted_at.is_null()),
            )
    }
}

// Builder with raw_query — for complex SQL that SelectBuilder can't express,
// but you still want column metadata via the builder.
#[derive(View, Debug, Clone)]
#[view(name = "high_value_customers")]
pub struct HighValueCustomer {
    pub id: i64,
    pub email: String,
    pub total_spent: f64,
}

impl ViewSchemaDef for HighValueCustomer {
    fn view_schema() -> reify::ViewSchema<Self> {
        reify::view_schema::<Self>("high_value_customers")
            .column(HighValueCustomer::id, |c| c.sql_type(SqlType::BigInt))
            .column(HighValueCustomer::email, |c| c.sql_type(SqlType::Text))
            .column(HighValueCustomer::total_spent, |c| {
                c.sql_type(SqlType::Double)
            })
            .raw_query(
                "SELECT u.id, u.email, SUM(o.total) AS total_spent \
                 FROM users u JOIN orders o ON o.user_id = u.id \
                 GROUP BY u.id, u.email \
                 HAVING SUM(o.total) > 1000",
            )
    }
}

// ═══════════════════════════════════════════════════════════════════
//  3. MANUAL MIGRATION — create_view / drop_view in MigrationContext
// ═══════════════════════════════════════════════════════════════════

pub struct CreateDashboardView;

impl Migration for CreateDashboardView {
    fn version(&self) -> &'static str {
        "20240601_000001_create_dashboard_view"
    }
    fn description(&self) -> &'static str {
        "Create dashboard_summary view"
    }
    fn up(&self, ctx: &mut MigrationContext) {
        ctx.create_view(
            "dashboard_summary",
            "SELECT u.id, u.email, \
                    COUNT(o.id) AS order_count, \
                    COALESCE(SUM(o.total), 0) AS revenue \
             FROM users u \
             LEFT JOIN orders o ON o.user_id = u.id \
             WHERE u.deleted_at IS NULL \
             GROUP BY u.id, u.email",
        );
    }
    fn down(&self, ctx: &mut MigrationContext) {
        ctx.drop_view("dashboard_summary");
    }
}

// ═══════════════════════════════════════════════════════════════════
//  main — run all examples
// ═══════════════════════════════════════════════════════════════════

fn main() {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║  Reify Views — Macro, Builder & Migration examples      ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    // ── 1. Macro views: query with typed columns ───────────────────
    println!("━━━ 1. MACRO VIEWS ━━━\n");

    let (sql, params) = ActiveUser::find()
        .filter(ActiveUser::email.ends_with("@corp.io"))
        .filter(ActiveUser::role.is_not_null())
        .order_by(Order::Asc("email"))
        .limit(20)
        .build();
    println!("ActiveUser::find() with filters:");
    println!("  SQL:    {sql}");
    println!("  Params: {params:?}\n");

    let (sql, params) = UserOrderStats::find()
        .filter(UserOrderStats::total_spent.gt(100.0))
        .order_by(Order::Desc("total_spent"))
        .limit(10)
        .build();
    println!("UserOrderStats::find() — top spenders:");
    println!("  SQL:    {sql}");
    println!("  Params: {params:?}\n");

    // ── 2. Builder views: typed query from SelectBuilder ───────────
    println!("━━━ 2. BUILDER VIEWS (ViewSchemaDef) ━━━\n");

    let schema = AdminUser::view_schema();
    println!("AdminUser view schema:");
    println!("  Name:    {}", schema.name);
    println!(
        "  Columns: {}",
        schema
            .columns
            .iter()
            .map(|c| c.name)
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("  Query:   {}\n", schema.query_sql().unwrap());

    // The view still has typed columns for querying
    let (sql, params) = AdminUser::find()
        .filter(AdminUser::email.contains("@corp"))
        .build();
    println!("AdminUser::find() with filter:");
    println!("  SQL:    {sql}");
    println!("  Params: {params:?}\n");

    let schema = HighValueCustomer::view_schema();
    println!("HighValueCustomer view schema (raw_query):");
    println!("  Name:    {}", schema.name);
    println!(
        "  Columns: {}",
        schema
            .columns
            .iter()
            .map(|c| c.name)
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("  Query:   {}\n", schema.query_sql().unwrap());

    let (sql, params) = HighValueCustomer::find()
        .filter(HighValueCustomer::total_spent.gte(5000.0))
        .order_by(Order::Desc("total_spent"))
        .build();
    println!("HighValueCustomer::find():");
    println!("  SQL:    {sql}");
    println!("  Params: {params:?}\n");

    // ── 3. DDL generation ──────────────────────────────────────────
    println!("━━━ 3. DDL GENERATION ━━━\n");

    println!(
        "CREATE: {}",
        reify::create_view_sql(
            "active_users",
            "SELECT id, email, role FROM users WHERE deleted_at IS NULL"
        )
    );
    println!("DROP:   {}\n", reify::drop_view_sql("active_users"));

    // ── 4. Migration ───────────────────────────────────────────────
    println!("━━━ 4. MIGRATION ━━━\n");

    // Manual migration — preview the SQL
    let mut ctx = MigrationContext::new();
    let migration = CreateDashboardView;
    migration.up(&mut ctx);
    println!("CreateDashboardView.up() generates:");
    for stmt in ctx.statements() {
        println!("  {stmt}");
    }

    let mut ctx_down = MigrationContext::new();
    migration.down(&mut ctx_down);
    println!("\nCreateDashboardView.down() generates:");
    for stmt in ctx_down.statements() {
        println!("  {stmt}");
    }

    // MigrationRunner usage (compile-time checked, not executed here)
    println!("\nMigrationRunner integration:");
    println!("  MigrationRunner::new()");
    println!("      .add_table::<User>()");
    println!("      .add_table::<Order_>()");
    println!("      .add_view::<ActiveUser>()          // macro view");
    println!("      .add_view::<AdminUser>()           // builder view");
    println!("      .add_view::<HighValueCustomer>()   // builder + raw_query");
    println!("      .add(CreateDashboardView)          // manual migration");
    println!("      .run(&db).await?;");
}
