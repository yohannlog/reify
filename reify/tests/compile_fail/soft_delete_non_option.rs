// compile_fail: soft_delete requires Option<DateTime> type
// A non-nullable column cannot be a soft-delete marker.

use reify::Table;
use chrono::{DateTime, Utc};

#[derive(Table)]
#[table(name = "articles")]
pub struct Article {
    #[column(primary_key)]
    pub id: i64,
    #[column(soft_delete)]
    pub deleted_at: DateTime<Utc>, // ERROR: must be Option<DateTime<Utc>>
}

fn main() {}
