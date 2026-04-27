// compile_fail: each `#[column(...)]` option may only appear once.
// Pre-fix, copy-paste duplicates like `#[column(primary_key, primary_key)]`
// were silently accepted (last assignment won), hiding user typos.

use reify::Table;

#[derive(Table)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key, primary_key)] // ERROR: duplicate `primary_key`
    pub id: i64,
    pub name: String,
}

fn main() {}
