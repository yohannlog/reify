// compile_fail: `#[column(validate(...))]` with only the `dto` feature
// (without `dto-validation`) must not silently drop the rule. The macro
// refuses to compile so the developer is forced to either enable the
// validation feature or remove the attribute.
//
// Run with: `cargo test -p reify --features dto validate_without_dto_validation`

use reify::Table;

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(validate(email))]
    pub email: String,
}

fn main() {}
