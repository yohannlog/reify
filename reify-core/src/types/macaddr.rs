//! MAC address type.

use std::fmt;
use std::str::FromStr;

/// A 48-bit MAC (Media Access Control) address.
///
/// Stored as 6 bytes.
///
/// # Adapter support
///
/// - **PostgreSQL** — native `MACADDR` type (6-byte binary), round-trips
///   losslessly.
/// - **MySQL** — bound as `CHAR(17)` in canonical
///   `XX:XX:XX:XX:XX:XX` form.
/// - **SQLite** — bound as `TEXT`.
///
/// The `Value::MacAddr` variant is gated on the `postgres` feature; on
/// other adapters store as `String` and parse to `MacAddr` in your model
/// layer if needed.
///
/// # Examples
///
/// ```
/// use reify_core::types::MacAddr;
///
/// let mac: MacAddr = "08:00:2b:01:02:03".parse().unwrap();
/// assert_eq!(mac.octets(), [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]);
///
/// // Also accepts hyphen-separated format
/// let mac2: MacAddr = "08-00-2b-01-02-03".parse().unwrap();
/// assert_eq!(mac, mac2);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct MacAddr([u8; 6]);

/// Parse error for MAC addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseMacAddrError(String);

impl fmt::Display for ParseMacAddrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid MAC address: {}", self.0)
    }
}

impl std::error::Error for ParseMacAddrError {}

impl MacAddr {
    /// Create a MAC address from 6 octets.
    #[inline]
    pub const fn new(octets: [u8; 6]) -> Self {
        Self(octets)
    }

    /// Create a MAC address from individual octets.
    #[inline]
    pub const fn from_octets(a: u8, b: u8, c: u8, d: u8, e: u8, f: u8) -> Self {
        Self([a, b, c, d, e, f])
    }

    /// Get the 6 octets of the MAC address.
    #[inline]
    pub const fn octets(&self) -> [u8; 6] {
        self.0
    }

    /// Returns `true` if this is a broadcast address (ff:ff:ff:ff:ff:ff).
    #[inline]
    pub const fn is_broadcast(&self) -> bool {
        self.0[0] == 0xff
            && self.0[1] == 0xff
            && self.0[2] == 0xff
            && self.0[3] == 0xff
            && self.0[4] == 0xff
            && self.0[5] == 0xff
    }

    /// Returns `true` if this is a unicast address (LSB of first octet is 0).
    #[inline]
    pub const fn is_unicast(&self) -> bool {
        self.0[0] & 0x01 == 0
    }

    /// Returns `true` if this is a multicast address (LSB of first octet is 1).
    #[inline]
    pub const fn is_multicast(&self) -> bool {
        self.0[0] & 0x01 == 1
    }

    /// Returns `true` if this is a locally administered address.
    #[inline]
    pub const fn is_local(&self) -> bool {
        self.0[0] & 0x02 != 0
    }

    /// Returns `true` if this is a universally administered address.
    #[inline]
    pub const fn is_universal(&self) -> bool {
        self.0[0] & 0x02 == 0
    }
}

impl fmt::Display for MacAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5]
        )
    }
}

impl FromStr for MacAddr {
    type Err = ParseMacAddrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Accept both colon and hyphen separators
        let parts: Vec<&str> = if s.contains(':') {
            s.split(':').collect()
        } else if s.contains('-') {
            s.split('-').collect()
        } else {
            return Err(ParseMacAddrError("expected ':' or '-' separator".into()));
        };

        if parts.len() != 6 {
            return Err(ParseMacAddrError(format!(
                "expected 6 octets, got {}",
                parts.len()
            )));
        }

        let mut octets = [0u8; 6];
        for (i, part) in parts.iter().enumerate() {
            octets[i] = u8::from_str_radix(part, 16)
                .map_err(|e| ParseMacAddrError(format!("invalid octet '{}': {}", part, e)))?;
        }

        Ok(MacAddr(octets))
    }
}

impl From<[u8; 6]> for MacAddr {
    fn from(octets: [u8; 6]) -> Self {
        Self(octets)
    }
}

impl From<MacAddr> for [u8; 6] {
    fn from(mac: MacAddr) -> Self {
        mac.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_macaddr_new() {
        let mac = MacAddr::new([0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]);
        assert_eq!(mac.octets(), [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_macaddr_from_octets() {
        let mac = MacAddr::from_octets(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03);
        assert_eq!(mac.octets(), [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]);
    }

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
}
