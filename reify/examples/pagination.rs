use reify::Table;

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

fn main() {
    // ── Offset-based pagination ─────────────────────────────────
    // Classic page/per_page — generates both a data query and a count query.

    println!("=== Offset-based pagination ===\n");

    let paginated = User::find()
        .filter(User::role.is_not_null())
        .paginate(1, 25); // page 1, 25 items per page

    let (data_sql, count_sql, params) = paginated.build();
    println!("Page 1:");
    println!("  data:  {data_sql}");
    println!("  count: {count_sql}");
    println!("  params: {params:?}\n");

    // Simulate: the COUNT query returned 237 total rows
    let page = paginated.page_info(237);
    println!("  page:        {}", page.page);
    println!("  per_page:    {}", page.per_page);
    println!("  total_items: {}", page.total_items);
    println!("  total_pages: {}", page.total_pages);
    println!("  has_next:    {}", page.has_next);
    println!("  has_prev:    {}\n", page.has_prev);

    // Page 5
    let paginated = User::find()
        .filter(User::role.is_not_null())
        .paginate(5, 25);

    let (data_sql, _, _) = paginated.build();
    println!("Page 5:");
    println!("  data: {data_sql}");
    let page = paginated.page_info(237);
    println!(
        "  has_next: {}, has_prev: {}\n",
        page.has_next, page.has_prev
    );

    // ── Cursor-based pagination ─────────────────────────────────
    // Keyset pagination — much faster on large tables (no OFFSET scan).

    println!("=== Cursor-based pagination ===\n");

    // First page: after id 0
    let cursor_page = User::find()
        .filter(User::email.ends_with("@corp.io"))
        .after(User::id, 0i64, 25);

    let (sql, params) = cursor_page.build();
    println!("First page (after id=0):");
    println!("  sql:    {sql}");
    println!("  params: {params:?}");
    // Note: fetches 26 rows (limit+1) to detect if more exist
    println!("  has_more (got 26 rows): {}", cursor_page.has_more(26));
    println!("  has_more (got 20 rows): {}\n", cursor_page.has_more(20));

    // Next page: after the last id we received (e.g. 150)
    let cursor_page = User::find()
        .filter(User::email.ends_with("@corp.io"))
        .after(User::id, 150i64, 25);

    let (sql, params) = cursor_page.build();
    println!("Next page (after id=150):");
    println!("  sql:    {sql}");
    println!("  params: {params:?}\n");

    // Previous page: before a known cursor
    let cursor_page = User::find()
        .filter(User::email.ends_with("@corp.io"))
        .before(User::id, 100i64, 25);

    let (sql, params) = cursor_page.build();
    println!("Previous page (before id=100):");
    println!("  sql:    {sql}");
    println!("  params: {params:?}");
}
