// compile_fail: creation_timestamp() must not be callable on a String column
// because String does not implement Temporal.

use reify_core::Column;
use reify_core::schema::table;

struct User;

const EMAIL: Column<User, String> = Column::new("email");

fn main() {
    table::<User>("users")
        .column(EMAIL, |c| c.creation_timestamp());
}
