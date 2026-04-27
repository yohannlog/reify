//! Network address type with prefix length.

use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;

/// A network address with prefix length (CIDR notation).
///
/// Represents a network block like `192.168.1.0/24` or `2001:db8::/32`.
///
/// # Adapter support
///
/// - **PostgreSQL** — native `CIDR` type, round-trips losslessly.
/// - **MySQL** — bound as `VARCHAR(49)` (text representation). The
///   network-containment helpers (`contains` / `contains_or_equals`) only
///   run client-side, not in SQL `WHERE` clauses.
/// - **SQLite** — bound as `TEXT`. Same caveat as MySQL.
///
/// The `Value::Cidr` variant itself is gated on the `postgres` feature
/// because non-PG adapters fall back to text and lose the structural
/// type. To use this type with MySQL/SQLite, store as `String` and parse
/// to `Cidr` in your model layer.
///
/// # Examples
///
/// ```
/// use reify_core::types::Cidr;
///
/// let net: Cidr = "192.168.1.0/24".parse().unwrap();
/// assert_eq!(net.prefix(), 24);
/// assert!(net.contains("192.168.1.100".parse().unwrap()));
/// assert!(!net.contains("192.168.2.1".parse().unwrap()));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Cidr {
    addr: IpAddr,
    prefix: u8,
}

/// Parse error for CIDR notation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseCidrError(String);

impl fmt::Display for ParseCidrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid CIDR: {}", self.0)
    }
}

impl std::error::Error for ParseCidrError {}

impl Cidr {
    /// Create a new CIDR from an address and prefix length.
    ///
    /// Returns `None` if the prefix is invalid (> 32 for IPv4, > 128 for IPv6).
    pub fn new(addr: IpAddr, prefix: u8) -> Option<Self> {
        let max_prefix = match addr {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix > max_prefix {
            return None;
        }
        Some(Self { addr, prefix })
    }

    /// Get the network address.
    #[inline]
    pub const fn addr(&self) -> IpAddr {
        self.addr
    }

    /// Get the prefix length.
    #[inline]
    pub const fn prefix(&self) -> u8 {
        self.prefix
    }

    /// Returns `true` if this is an IPv4 network.
    #[inline]
    pub const fn is_ipv4(&self) -> bool {
        self.addr.is_ipv4()
    }

    /// Returns `true` if this is an IPv6 network.
    #[inline]
    pub const fn is_ipv6(&self) -> bool {
        self.addr.is_ipv6()
    }

    /// Check if an IP address is contained within this network.
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, ip) {
            (IpAddr::V4(net), IpAddr::V4(addr)) => {
                if self.prefix == 0 {
                    return true;
                }
                let mask = !0u32 << (32 - self.prefix);
                (u32::from(net) & mask) == (u32::from(addr) & mask)
            }
            (IpAddr::V6(net), IpAddr::V6(addr)) => {
                if self.prefix == 0 {
                    return true;
                }
                let mask = !0u128 << (128 - self.prefix);
                (u128::from(net) & mask) == (u128::from(addr) & mask)
            }
            _ => false, // IPv4/IPv6 mismatch
        }
    }
}

impl fmt::Display for Cidr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.addr, self.prefix)
    }
}

impl FromStr for Cidr {
    type Err = ParseCidrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 2 {
            return Err(ParseCidrError("expected format: addr/prefix".into()));
        }

        let addr: IpAddr = parts[0]
            .parse()
            .map_err(|e| ParseCidrError(format!("invalid address: {e}")))?;

        let prefix: u8 = parts[1]
            .parse()
            .map_err(|e| ParseCidrError(format!("invalid prefix: {e}")))?;

        Cidr::new(addr, prefix).ok_or_else(|| {
            let max = if addr.is_ipv4() { 32 } else { 128 };
            ParseCidrError(format!("prefix {prefix} exceeds maximum {max}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_cidr_new() {
        let cidr = Cidr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 0)), 24).unwrap();
        assert_eq!(cidr.prefix(), 24);
        assert!(cidr.is_ipv4());
    }

    #[test]
    fn test_cidr_invalid_prefix() {
        assert!(Cidr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 33).is_none());
    }

    #[test]
    fn test_cidr_parse() {
        let cidr: Cidr = "10.0.0.0/8".parse().unwrap();
        assert_eq!(cidr.addr(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)));
        assert_eq!(cidr.prefix(), 8);
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
}
