// compile_fail: `dto(skip = "...")` must reference an actual field name.
// The error span is anchored on the offending string literal (not on
// `Span::call_site()`) so the user sees the typo immediately.

use reify::Table;

#[derive(Table)]
#[table(name = "users", dto(skip = "id,nonexistent_field"))]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    pub name: String,
}

fn main() {}
