//! Complex SQL types for PostgreSQL and MariaDB.
//!
//! This module provides Rust wrappers for database-specific types that don't
//! have direct Rust equivalents:
//!
//! - [`Point`] — 2D geometric point `(x, y)`
//! - [`Inet`] — IP address (IPv4 or IPv6)
//! - [`Cidr`] — Network address with prefix length
//! - [`MacAddr`] — MAC address (6 bytes)
//! - [`Interval`] — Time interval (months, days, microseconds)
//!
//! # Usage
//!
//! ```ignore
//! use reify::types::{Point, Inet, Cidr, MacAddr, Interval};
//!
//! #[derive(Table)]
//! struct Location {
//!     id: i64,
//!     coords: Point,
//!     server_ip: Inet,
//!     network: Cidr,
//!     device_mac: MacAddr,
//!     duration: Interval,
//! }
//! ```

mod cidr;
mod inet;
mod interval;
mod macaddr;
mod point;

pub use cidr::Cidr;
pub use inet::Inet;
pub use interval::Interval;
pub use macaddr::MacAddr;
pub use point::Point;
