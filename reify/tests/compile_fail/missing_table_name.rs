// compile_fail: `#[table(name = "...")]` is required.
// The error now points at the struct identifier (or at the existing
// `#[table(...)]` attribute) instead of `Span::call_site()`.

use reify::Table;

#[derive(Table)]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
}

fn main() {}
