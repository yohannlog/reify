// compile_fail: a primary key column cannot be `Option<T>` (NULL).
// Every supported SGBD rejects nullable primary keys at schema creation
// time; the macro now catches the mistake at compile time.

use reify::Table;

#[derive(Table)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key)]
    pub id: Option<i64>, // ERROR: primary keys must be NOT NULL
    pub name: String,
}

fn main() {}
