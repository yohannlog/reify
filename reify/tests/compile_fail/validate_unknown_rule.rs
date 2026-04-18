// compile_fail: unknown validator rule names are rejected at macro
// expansion (3.5) instead of being silently forwarded and only failing
// later in the user's build.
//
// Run with: `cargo test -p reify --features dto-validation validate_unknown_rule`

use reify::Table;

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(validate(nonsense(foo)))]
    pub email: String,
}

fn main() {}
