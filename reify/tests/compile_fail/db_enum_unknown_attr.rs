// compile_fail: unknown attribute key in #[db_enum(...)] must produce a compile error.
// A typo like `Rename` instead of `rename` should not be silently ignored.

use reify::DbEnum;

#[derive(DbEnum, Debug, Clone, PartialEq)]
pub enum Role {
    Admin,
    #[db_enum(Rename = "moderator")]
    Moderator,
    User,
}

fn main() {}
