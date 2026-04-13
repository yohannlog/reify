// compile_fail: source_db() must not be callable without a prior
// creation_timestamp() or update_timestamp() call.

use reify_core::Column;
use reify_core::schema::table;

struct User;

const NAME: Column<User, String> = Column::new("name");

fn main() {
    table::<User>("users")
        .column(NAME, |c| c.source_db());
}
