use reify::range::{Bound, Range};
#[cfg(feature = "postgres")]
use reify::Table;
use reify::Value;

// ── Range construction ─────────────────────────────────────────────

#[test]
fn range_closed_open() {
    let r = Range::closed_open(10i32, 20i32);
    assert_eq!(r.to_string(), "[10,20)");
}

#[test]
fn range_closed() {
    let r = Range::closed(1i64, 100i64);
    assert_eq!(r.to_string(), "[1,100]");
}

#[test]
fn range_open() {
    let r = Range::open(0i32, 50i32);
    assert_eq!(r.to_string(), "(0,50)");
}

#[test]
fn range_from() {
    let r = Range::from(42i64);
    assert_eq!(r.to_string(), "[42,)");
}

#[test]
fn range_to() {
    let r = Range::to(99i32);
    assert_eq!(r.to_string(), "(,99]");
}

#[test]
fn range_empty() {
    let r = Range::<i32>::empty();
    assert_eq!(r.to_string(), "empty");
    assert!(r.is_empty());
}

#[test]
fn range_custom_bounds() {
    let r = Range::new(Bound::Exclusive(5i32), Bound::Inclusive(15i32));
    assert_eq!(r.to_string(), "(5,15]");
}

#[test]
fn range_unbounded_both() {
    let r: Range<i32> = Range::new(Bound::Unbounded, Bound::Unbounded);
    assert_eq!(r.to_string(), "(,)");
}

#[test]
fn range_accessors() {
    let r = Range::closed_open(10i32, 20i32);
    assert!(!r.is_empty());
    assert_eq!(r.lower(), Some(&Bound::Inclusive(10)));
    assert_eq!(r.upper(), Some(&Bound::Exclusive(20)));

    let e = Range::<i32>::empty();
    assert_eq!(e.lower(), None);
    assert_eq!(e.upper(), None);
}

// ── IntoValue ──────────────────────────────────────────────────────

#[test]
fn range_into_value() {
    use reify::value::IntoValue;
    let r = Range::closed_open(1i32, 10i32);
    #[cfg(not(feature = "postgres"))]
    assert_eq!(r.into_value(), Value::String("[1,10)".into()));
    #[cfg(feature = "postgres")]
    assert_eq!(r.into_value(), Value::Int4Range(Range::closed_open(1, 10)));
}

// ── Table with range column + operators ────────────────────────────

#[cfg(feature = "postgres")]
mod postgres_tests {
    use super::*;

    #[derive(Table, Debug, Clone)]
    #[table(name = "events")]
    pub struct Event {
        #[column(primary_key)]
        pub id: i64,
        pub name: String,
        pub duration: Range<i32>,
    }

    #[test]
    fn insert_with_range() {
        let event = Event {
            id: 1,
            name: "Conference".into(),
            duration: Range::closed_open(0, 120),
        };
        let (sql, params) = Event::insert(&event).build();
        assert_eq!(
            sql,
            "INSERT INTO events (id, name, duration) VALUES (?, ?, ?)"
        );
        assert_eq!(
            params,
            vec![
                Value::I64(1),
                Value::String("Conference".into()),
                Value::Int4Range(Range::closed_open(0, 120)),
            ]
        );
    }

    #[test]
    fn filter_contains_element() {
        let (sql, params) = Event::find()
            .filter(Event::duration.contains_element(50i32))
            .build();
        assert_eq!(sql, "SELECT * FROM events WHERE duration @> ?");
        assert_eq!(params, vec![Value::I32(50)]);
    }

    #[test]
    fn filter_contains_range() {
        let (sql, params) = Event::find()
            .filter(Event::duration.contains_range(Range::closed(10, 20)))
            .build();
        assert_eq!(sql, "SELECT * FROM events WHERE duration @> ?");
        assert_eq!(params, vec![Value::Int4Range(Range::closed(10, 20))]);
    }

    #[test]
    fn filter_contained_by() {
        let (sql, params) = Event::find()
            .filter(Event::duration.contained_by(Range::closed_open(0, 1000)))
            .build();
        assert_eq!(sql, "SELECT * FROM events WHERE duration <@ ?");
        assert_eq!(params, vec![Value::Int4Range(Range::closed_open(0, 1000))]);
    }

    #[test]
    fn filter_overlaps() {
        let (sql, params) = Event::find()
            .filter(Event::duration.overlaps(Range::closed(50, 150)))
            .build();
        assert_eq!(sql, "SELECT * FROM events WHERE duration && ?");
        assert_eq!(params, vec![Value::Int4Range(Range::closed(50, 150))]);
    }

    #[test]
    fn filter_left_of() {
        let (sql, params) = Event::find()
            .filter(Event::duration.left_of(Range::closed(200, 300)))
            .build();
        assert_eq!(sql, "SELECT * FROM events WHERE duration << ?");
        assert_eq!(params, vec![Value::Int4Range(Range::closed(200, 300))]);
    }

    #[test]
    fn filter_right_of() {
        let (sql, params) = Event::find()
            .filter(Event::duration.right_of(Range::closed(0, 5)))
            .build();
        assert_eq!(sql, "SELECT * FROM events WHERE duration >> ?");
        assert_eq!(params, vec![Value::Int4Range(Range::closed(0, 5))]);
    }

    #[test]
    fn filter_adjacent() {
        let (sql, params) = Event::find()
            .filter(Event::duration.adjacent(Range::closed_open(120, 240)))
            .build();
        assert_eq!(sql, "SELECT * FROM events WHERE duration -|- ?");
        assert_eq!(params, vec![Value::Int4Range(Range::closed_open(120, 240))]);
    }

    #[test]
    fn filter_is_empty_range() {
        let (sql, params) = Event::find()
            .filter(Event::duration.is_empty_range())
            .build();
        assert_eq!(sql, "SELECT * FROM events WHERE isempty(duration)");
        assert!(params.is_empty());
    }

    #[test]
    fn update_range_column() {
        let (sql, params) = Event::update()
            .set(Event::duration, Range::closed(0, 60))
            .filter(Event::id.eq(1i64))
            .build();
        assert_eq!(sql, "UPDATE events SET duration = ? WHERE id = ?");
        assert_eq!(
            params,
            vec![Value::Int4Range(Range::closed(0, 60)), Value::I64(1)]
        );
    }

    #[test]
    fn combined_range_filters() {
        let (sql, params) = Event::find()
            .filter(Event::duration.overlaps(Range::closed(10, 50)))
            .filter(Event::name.contains("meet"))
            .build();
        assert_eq!(
            sql,
            "SELECT * FROM events WHERE duration && ? AND name LIKE ?"
        );
        assert_eq!(
            params,
            vec![
                Value::Int4Range(Range::closed(10, 50)),
                Value::String("%meet%".into()),
            ]
        );
    }
}
