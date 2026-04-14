// compile_fail: unknown attribute key in #[column(...)] must produce a compile error.
// A typo like `priamry_key` instead of `primary_key` should not be silently ignored.

use reify::Table;

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    #[column(priamry_key)]
    pub email: String,
}

fn main() {}
