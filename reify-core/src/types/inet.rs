//! IP address type (IPv4 or IPv6).

use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;

/// An IP address (IPv4 or IPv6).
///
/// Stores a single IP address without network prefix (use
/// [`Cidr`](super::Cidr) for network addresses).
///
/// # Adapter support
///
/// - **PostgreSQL** — native `INET` type, round-trips losslessly.
/// - **MySQL** — bound as `VARCHAR(45)` (enough for IPv6 in canonical
///   form). Comparisons are textual.
/// - **SQLite** — bound as `TEXT`.
///
/// The `Value::Inet` variant is gated on the `postgres` feature; on
/// other adapters store as `String` and parse to `Inet` in your model
/// layer if needed.
///
/// # Examples
///
/// ```
/// use reify_core::types::Inet;
/// use std::net::IpAddr;
///
/// let ip: Inet = "192.168.1.1".parse().unwrap();
/// assert!(ip.is_ipv4());
///
/// let ip6: Inet = "::1".parse().unwrap();
/// assert!(ip6.is_ipv6());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Inet(IpAddr);

impl Inet {
    /// Create a new Inet from an IP address.
    #[inline]
    pub const fn new(addr: IpAddr) -> Self {
        Self(addr)
    }

    /// Get the underlying IP address.
    #[inline]
    pub const fn addr(&self) -> IpAddr {
        self.0
    }

    /// Returns `true` if this is an IPv4 address.
    #[inline]
    pub const fn is_ipv4(&self) -> bool {
        self.0.is_ipv4()
    }

    /// Returns `true` if this is an IPv6 address.
    #[inline]
    pub const fn is_ipv6(&self) -> bool {
        self.0.is_ipv6()
    }

    /// Returns `true` if this is a loopback address.
    #[inline]
    pub fn is_loopback(&self) -> bool {
        self.0.is_loopback()
    }

    /// Returns `true` if this is an unspecified address (0.0.0.0 or ::).
    #[inline]
    pub fn is_unspecified(&self) -> bool {
        self.0.is_unspecified()
    }
}

impl fmt::Display for Inet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Inet {
    type Err = std::net::AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<IpAddr>().map(Inet)
    }
}

impl From<IpAddr> for Inet {
    fn from(addr: IpAddr) -> Self {
        Self(addr)
    }
}

impl From<std::net::Ipv4Addr> for Inet {
    fn from(addr: std::net::Ipv4Addr) -> Self {
        Self(IpAddr::V4(addr))
    }
}

impl From<std::net::Ipv6Addr> for Inet {
    fn from(addr: std::net::Ipv6Addr) -> Self {
        Self(IpAddr::V6(addr))
    }
}

impl From<Inet> for IpAddr {
    fn from(inet: Inet) -> Self {
        inet.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

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
        let v4 = Ipv4Addr::new(127, 0, 0, 1);
        let inet: Inet = v4.into();
        assert!(inet.is_loopback());

        let v6 = Ipv6Addr::LOCALHOST;
        let inet: Inet = v6.into();
        assert!(inet.is_loopback());
    }

    #[test]
    fn test_inet_display() {
        let ip = Inet::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(ip.to_string(), "10.0.0.1");
    }
}
