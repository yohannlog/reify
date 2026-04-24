// compile_fail: soft_delete requires Option<DateTime<Utc>> or Option<NaiveDateTime>
// A String column cannot be a soft-delete marker.

use reify::Table;

#[derive(Table)]
#[table(name = "articles")]
pub struct Article {
    #[column(primary_key)]
    pub id: i64,
    #[column(soft_delete)]
    pub deleted_at: Option<String>, // ERROR: must be Option<DateTime<Utc>> or Option<NaiveDateTime>
}

fn main() {}
