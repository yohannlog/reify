//! PostgreSQL range types example.
//!
//! Run with: cargo run --example range_types --features postgres

use reify::range::{Bound, Range};
use reify::{DbEnum, Table};

// ── Enum for event kind ────────────────────────────────────────────

#[derive(DbEnum, Debug, Clone, PartialEq)]
pub enum EventKind {
    Conference,
    Workshop,
    #[db_enum(rename = "team_meeting")]
    TeamMeeting,
}

// ── Model with range columns ───────────────────────────────────────

#[derive(Table, Debug, Clone)]
#[table(name = "events")]
pub struct Event {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub name: String,
    pub kind: EventKind,
    /// Room numbers this event spans (int4range).
    pub rooms: Range<i32>,
    /// Floor range (int8range).
    pub floors: Range<i64>,
}

fn main() {
    println!("=== Range construction ===\n");

    // [10, 20) — canonical PostgreSQL form (closed-open)
    let r = Range::closed_open(10i32, 20);
    println!("closed_open(10, 20) → {r}");

    // [1, 100]
    let r = Range::closed(1i32, 100);
    println!("closed(1, 100)      → {r}");

    // (5, 15]
    let r = Range::new(Bound::Exclusive(5i32), Bound::Inclusive(15));
    println!("(5, 15]             → {r}");

    // [42, ∞)
    let r = Range::from(42i64);
    println!("from(42)            → {r}");

    // (∞, 99]
    let r = Range::to(99i32);
    println!("to(99)              → {r}");

    // empty
    let r = Range::<i32>::empty();
    println!("empty               → {r}");

    // ── INSERT ─────────────────────────────────────────────────────

    println!("\n=== INSERT with range columns ===\n");

    let event = Event {
        id: 0,
        name: "RustConf".into(),
        kind: EventKind::Conference,
        rooms: Range::closed_open(100, 110),
        floors: Range::closed(1, 3),
    };
    let (sql, params) = Event::insert(&event).build();
    println!("SQL:    {sql}");
    println!("params: {params:?}");

    // ── SELECT with range operators ────────────────────────────────

    println!("\n=== SELECT with range operators ===\n");

    // @> — range contains element
    let (sql, params) = Event::find()
        .filter(Event::rooms.contains_element(105i32))
        .build();
    println!("contains_element(105):");
    println!("  {sql}");
    println!("  params: {params:?}\n");

    // @> — range contains range
    let (sql, params) = Event::find()
        .filter(Event::rooms.contains_range(Range::closed(102, 108)))
        .build();
    println!("contains_range([102,108]):");
    println!("  {sql}");
    println!("  params: {params:?}\n");

    // <@ — contained by
    let (sql, params) = Event::find()
        .filter(Event::rooms.contained_by(Range::closed_open(0, 1000)))
        .build();
    println!("contained_by([0,1000)):");
    println!("  {sql}");
    println!("  params: {params:?}\n");

    // && — overlaps
    let (sql, params) = Event::find()
        .filter(Event::rooms.overlaps(Range::closed(105, 200)))
        .build();
    println!("overlaps([105,200]):");
    println!("  {sql}");
    println!("  params: {params:?}\n");

    // << — strictly left of
    let (sql, params) = Event::find()
        .filter(Event::rooms.left_of(Range::closed(200, 300)))
        .build();
    println!("left_of([200,300]):");
    println!("  {sql}");
    println!("  params: {params:?}\n");

    // >> — strictly right of
    let (sql, params) = Event::find()
        .filter(Event::rooms.right_of(Range::closed(0, 50)))
        .build();
    println!("right_of([0,50]):");
    println!("  {sql}");
    println!("  params: {params:?}\n");

    // -|- — adjacent
    let (sql, params) = Event::find()
        .filter(Event::rooms.adjacent(Range::closed_open(110, 120)))
        .build();
    println!("adjacent([110,120)):");
    println!("  {sql}");
    println!("  params: {params:?}\n");

    // isempty()
    let (sql, params) = Event::find()
        .filter(Event::rooms.is_empty_range())
        .build();
    println!("is_empty_range:");
    println!("  {sql}");
    println!("  params: {params:?}\n");

    // ── Combined filters ───────────────────────────────────────────

    println!("=== Combined filters ===\n");

    let (sql, params) = Event::find()
        .filter(Event::kind.eq(EventKind::Conference))
        .filter(Event::rooms.overlaps(Range::closed(100, 110)))
        .filter(Event::floors.contains_element(2i64))
        .build();
    println!("Conferences overlapping rooms [100,110] on floor 2:");
    println!("  {sql}");
    println!("  params: {params:?}\n");

    // ── UPDATE ─────────────────────────────────────────────────────

    println!("=== UPDATE range column ===\n");

    let (sql, params) = Event::update()
        .set(Event::rooms, Range::closed_open(200, 210))
        .filter(Event::id.eq(1i64))
        .build();
    println!("SQL:    {sql}");
    println!("params: {params:?}");
}
