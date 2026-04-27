//! Integration tests for complex PostgreSQL/MySQL types.

use reify::types::{Cidr, Inet, Interval, MacAddr, Point};

// ── Point tests ─────────────────────────────────────────────────────

#[test]
fn test_point_construction() {
    let p = Point::new(3.0, 4.0);
    assert_eq!(p.x(), 3.0);
    assert_eq!(p.y(), 4.0);

    let origin = Point::origin();
    assert_eq!(origin.x(), 0.0);
    assert_eq!(origin.y(), 0.0);
}

#[test]
fn test_point_distance() {
    let a = Point::origin();
    let b = Point::new(3.0, 4.0);
    assert!((a.distance_to(&b) - 5.0).abs() < 1e-10);
}

#[test]
fn test_point_translate() {
    let p = Point::new(1.0, 2.0);
    let q = p.translate(3.0, 4.0);
    assert_eq!(q.x(), 4.0);
    assert_eq!(q.y(), 6.0);
}

#[test]
fn test_point_display_parse() {
    let p = Point::new(1.5, 2.5);
    assert_eq!(p.to_string(), "(1.5,2.5)");

    let parsed: Point = "(1.5,2.5)".parse().unwrap();
    assert_eq!(parsed, p);

    // Also accepts without parens
    let parsed2: Point = "1.5,2.5".parse().unwrap();
    assert_eq!(parsed2, p);
}

#[test]
fn test_point_from_tuple() {
    let p: Point = (1.0, 2.0).into();
    assert_eq!(p, Point::new(1.0, 2.0));

    let t: (f64, f64) = p.into();
    assert_eq!(t, (1.0, 2.0));
}

// ── Inet tests ──────────────────────────────────────────────────────

#[test]
fn test_inet_ipv4() {
    let ip: Inet = "192.168.1.1".parse().unwrap();
    assert!(ip.is_ipv4());
    assert!(!ip.is_ipv6());
    assert_eq!(ip.to_string(), "192.168.1.1");
}

#[test]
fn test_inet_ipv6() {
    let ip: Inet = "::1".parse().unwrap();
    assert!(ip.is_ipv6());
    assert!(!ip.is_ipv4());
    assert!(ip.is_loopback());
}

#[test]
fn test_inet_from_std() {
    use std::net::{IpAddr, Ipv4Addr};
    let v4 = Ipv4Addr::new(127, 0, 0, 1);
    let inet: Inet = v4.into();
    assert!(inet.is_loopback());

    let addr = IpAddr::V4(v4);
    let inet2: Inet = addr.into();
    assert_eq!(inet, inet2);
}

// ── Cidr tests ──────────────────────────────────────────────────────

#[test]
fn test_cidr_parse() {
    let cidr: Cidr = "10.0.0.0/8".parse().unwrap();
    assert_eq!(cidr.prefix(), 8);
    assert!(cidr.is_ipv4());
}

#[test]
fn test_cidr_contains() {
    let cidr: Cidr = "192.168.1.0/24".parse().unwrap();

    assert!(cidr.contains("192.168.1.1".parse().unwrap()));
    assert!(cidr.contains("192.168.1.255".parse().unwrap()));
    assert!(!cidr.contains("192.168.2.1".parse().unwrap()));
    assert!(!cidr.contains("10.0.0.1".parse().unwrap()));
}

#[test]
fn test_cidr_contains_ipv6() {
    let cidr: Cidr = "2001:db8::/32".parse().unwrap();

    assert!(cidr.contains("2001:db8::1".parse().unwrap()));
    assert!(cidr.contains("2001:db8:ffff::1".parse().unwrap()));
    assert!(!cidr.contains("2001:db9::1".parse().unwrap()));
}

#[test]
fn test_cidr_display() {
    let cidr: Cidr = "10.0.0.0/8".parse().unwrap();
    assert_eq!(cidr.to_string(), "10.0.0.0/8");
}

#[test]
fn test_cidr_invalid_prefix() {
    // IPv4 max prefix is 32
    assert!("10.0.0.0/33".parse::<Cidr>().is_err());
}

// ── MacAddr tests ───────────────────────────────────────────────────

#[test]
fn test_macaddr_parse_colon() {
    let mac: MacAddr = "08:00:2b:01:02:03".parse().unwrap();
    assert_eq!(mac.octets(), [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]);
}

#[test]
fn test_macaddr_parse_hyphen() {
    let mac: MacAddr = "08-00-2b-01-02-03".parse().unwrap();
    assert_eq!(mac.octets(), [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]);
}

#[test]
fn test_macaddr_display() {
    let mac = MacAddr::new([0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]);
    assert_eq!(mac.to_string(), "08:00:2b:01:02:03");
}

#[test]
fn test_macaddr_broadcast() {
    let broadcast: MacAddr = "ff:ff:ff:ff:ff:ff".parse().unwrap();
    assert!(broadcast.is_broadcast());
    assert!(broadcast.is_multicast());

    let unicast: MacAddr = "08:00:2b:01:02:03".parse().unwrap();
    assert!(!unicast.is_broadcast());
    assert!(unicast.is_unicast());
}

#[test]
fn test_macaddr_local_universal() {
    // Locally administered (bit 1 of first octet set)
    let local: MacAddr = "02:00:00:00:00:01".parse().unwrap();
    assert!(local.is_local());
    assert!(!local.is_universal());

    // Universally administered
    let universal: MacAddr = "00:00:00:00:00:01".parse().unwrap();
    assert!(universal.is_universal());
    assert!(!universal.is_local());
}

// ── Interval tests ──────────────────────────────────────────────────

#[test]
fn test_interval_constructors() {
    assert_eq!(Interval::years(2).months(), 24);
    assert_eq!(Interval::from_months(6).months(), 6);
    assert_eq!(Interval::from_days(30).days(), 30);
    assert_eq!(Interval::hours(2).microseconds(), 2 * 3_600_000_000);
    assert_eq!(Interval::minutes(30).microseconds(), 30 * 60_000_000);
    assert_eq!(Interval::seconds(45).microseconds(), 45_000_000);
}

#[test]
fn test_interval_zero() {
    let zero = Interval::zero();
    assert!(zero.is_zero());
    assert_eq!(zero.months(), 0);
    assert_eq!(zero.days(), 0);
    assert_eq!(zero.microseconds(), 0);
}

#[test]
fn test_interval_arithmetic() {
    let a = Interval::from_days(10) + Interval::hours(5);
    assert_eq!(a.days(), 10);
    assert_eq!(a.microseconds(), 5 * 3_600_000_000);

    let b = Interval::from_months(3) + Interval::from_days(15);
    assert_eq!(b.months(), 3);
    assert_eq!(b.days(), 15);

    let c = Interval::from_days(30) - Interval::from_days(10);
    assert_eq!(c.days(), 20);

    let d = -Interval::from_days(10);
    assert_eq!(d.days(), -10);
}

#[test]
fn test_interval_display() {
    assert_eq!(Interval::zero().to_string(), "00:00:00");
    assert_eq!(Interval::from_days(5).to_string(), "5 days");
    assert_eq!(Interval::hours(2).to_string(), "02:00:00");
    assert_eq!(Interval::years(1).to_string(), "1 year");
}

// ── Value conversion tests (requires postgres feature) ──────────────

#[cfg(feature = "postgres")]
mod value_tests {
    use super::*;
    use reify::value::{FromValue, IntoValue, Value};

    #[test]
    fn test_point_into_value() {
        let p = Point::new(1.5, 2.5);
        let v = p.into_value();
        assert!(matches!(v, Value::Point(_)));

        let p2 = Point::from_value(v).unwrap();
        assert_eq!(p2, p);
    }

    #[test]
    fn test_inet_into_value() {
        let inet: Inet = "192.168.1.1".parse().unwrap();
        let v = inet.into_value();
        assert!(matches!(v, Value::Inet(_)));

        let inet2 = Inet::from_value(v).unwrap();
        assert_eq!(inet2, inet);
    }

    #[test]
    fn test_cidr_into_value() {
        let cidr: Cidr = "10.0.0.0/8".parse().unwrap();
        let v = cidr.into_value();
        assert!(matches!(v, Value::Cidr(_)));

        let cidr2 = Cidr::from_value(v).unwrap();
        assert_eq!(cidr2, cidr);
    }

    #[test]
    fn test_macaddr_into_value() {
        let mac: MacAddr = "08:00:2b:01:02:03".parse().unwrap();
        let v = mac.into_value();
        assert!(matches!(v, Value::MacAddr(_)));

        let mac2 = MacAddr::from_value(v).unwrap();
        assert_eq!(mac2, mac);
    }

    #[test]
    fn test_interval_into_value() {
        // `Interval::days` / `Interval::hours` are accessors (return
        // the field). The constructors are `from_days` / `from_hours`.
        let interval = Interval::from_days(30) + Interval::hours(12);
        let v = interval.into_value();
        assert!(matches!(v, Value::Interval(_)));

        let interval2 = Interval::from_value(v).unwrap();
        assert_eq!(interval2, interval);
    }
}

// ── SqlType mapping tests ───────────────────────────────────────────

#[cfg(feature = "postgres")]
mod sql_type_tests {
    use reify::Dialect;
    use reify::schema::SqlType;

    #[test]
    fn test_point_sql_type() {
        assert_eq!(SqlType::Point.to_sql(Dialect::Postgres).as_ref(), "POINT");
        assert_eq!(SqlType::Point.to_sql(Dialect::Mysql).as_ref(), "POINT");
        assert_eq!(SqlType::Point.to_sql(Dialect::Sqlite).as_ref(), "TEXT");
    }

    #[test]
    fn test_inet_sql_type() {
        assert_eq!(SqlType::Inet.to_sql(Dialect::Postgres).as_ref(), "INET");
        assert_eq!(SqlType::Inet.to_sql(Dialect::Mysql).as_ref(), "VARCHAR(45)");
        assert_eq!(SqlType::Inet.to_sql(Dialect::Sqlite).as_ref(), "TEXT");
    }

    #[test]
    fn test_cidr_sql_type() {
        assert_eq!(SqlType::Cidr.to_sql(Dialect::Postgres).as_ref(), "CIDR");
        assert_eq!(SqlType::Cidr.to_sql(Dialect::Mysql).as_ref(), "VARCHAR(49)");
        assert_eq!(SqlType::Cidr.to_sql(Dialect::Sqlite).as_ref(), "TEXT");
    }

    #[test]
    fn test_macaddr_sql_type() {
        assert_eq!(
            SqlType::MacAddr.to_sql(Dialect::Postgres).as_ref(),
            "MACADDR"
        );
        assert_eq!(SqlType::MacAddr.to_sql(Dialect::Mysql).as_ref(), "CHAR(17)");
        assert_eq!(SqlType::MacAddr.to_sql(Dialect::Sqlite).as_ref(), "TEXT");
    }

    #[test]
    fn test_interval_sql_type() {
        assert_eq!(
            SqlType::Interval.to_sql(Dialect::Postgres).as_ref(),
            "INTERVAL"
        );
        // MySQL has no `INTERVAL` column type but `TIME` is a signed
        // interval (-838:59:59 to +838:59:59), which round-trips a
        // `chrono::Duration` natively. Pre-fix this rendered as `TEXT`,
        // making `Value::Duration` round-trip lossy on MySQL.
        assert_eq!(SqlType::Interval.to_sql(Dialect::Mysql).as_ref(), "TIME");
        assert_eq!(SqlType::Interval.to_sql(Dialect::Sqlite).as_ref(), "TEXT");
    }
}

// ── Derive Table with complex types (compile test) ──────────────────

#[cfg(feature = "postgres")]
mod derive_tests {
    use super::*;
    use reify::Table;

    #[derive(Table)]
    #[table(name = "locations")]
    struct Location {
        #[column(primary_key)]
        id: i64,
        coords: Point,
        server_ip: Inet,
        network: Cidr,
        device_mac: MacAddr,
        duration: Interval,
    }

    #[test]
    fn test_location_table_compiles() {
        // Just verify the derive macro works with complex types
        assert_eq!(Location::table_name(), "locations");
    }

    #[test]
    fn test_location_columns() {
        // Verify column accessors exist
        let _ = Location::id;
        let _ = Location::coords;
        let _ = Location::server_ip;
        let _ = Location::network;
        let _ = Location::device_mac;
        let _ = Location::duration;
    }
}
