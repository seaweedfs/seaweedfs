//! Time-to-live encoding for needles.
//!
//! TTL is stored as 2 bytes: Count(1) + Unit(1).
//! Supported units: minute(m), hour(h), day(d), week(w), month(M), year(y).

use std::fmt;

/// TTL unit constants (matching Go).
pub const TTL_UNIT_EMPTY: u8 = 0;
pub const TTL_UNIT_MINUTE: u8 = 1;
pub const TTL_UNIT_HOUR: u8 = 2;
pub const TTL_UNIT_DAY: u8 = 3;
pub const TTL_UNIT_WEEK: u8 = 4;
pub const TTL_UNIT_MONTH: u8 = 5;
pub const TTL_UNIT_YEAR: u8 = 6;

pub const TTL_BYTES_LENGTH: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TTL {
    pub count: u8,
    pub unit: u8,
}

impl TTL {
    pub const EMPTY: TTL = TTL { count: 0, unit: 0 };

    pub fn is_empty(&self) -> bool {
        self.count == 0 && self.unit == 0
    }

    /// Load from 2 bytes.
    pub fn from_bytes(input: &[u8]) -> Self {
        if input.len() < 2 {
            return TTL::EMPTY;
        }
        TTL {
            count: input[0],
            unit: input[1],
        }
    }

    /// Serialize to 2 bytes.
    pub fn to_bytes(&self, output: &mut [u8]) {
        assert!(output.len() >= 2);
        output[0] = self.count;
        output[1] = self.unit;
    }

    /// Encode as u32: (count << 8) | unit.
    pub fn to_u32(&self) -> u32 {
        if self.count == 0 {
            return 0;
        }
        ((self.count as u32) << 8) + (self.unit as u32)
    }

    /// Decode from u32.
    pub fn from_u32(v: u32) -> Self {
        if v == 0 {
            return TTL::EMPTY;
        }
        TTL {
            count: (v >> 8) as u8,
            unit: (v & 0xFF) as u8,
        }
    }

    /// Convert to total seconds.
    pub fn to_seconds(&self) -> u64 {
        unit_to_seconds(self.count as u64, self.unit)
    }

    /// Parse from string like "3m", "4h", "5d", "6w", "7M", "8y".
    /// If the string is all digits (no unit suffix), defaults to minutes.
    pub fn read(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(TTL::EMPTY);
        }
        let last_byte = s.as_bytes()[s.len() - 1];
        let (num_str, unit_byte) = if last_byte >= b'0' && last_byte <= b'9' {
            // All digits — default to minutes (matching Go)
            (s, b'm')
        } else {
            (&s[..s.len() - 1], last_byte)
        };
        let count: u32 = num_str
            .parse()
            .map_err(|e| format!("invalid TTL count: {}", e))?;
        let unit = match unit_byte {
            b'm' => TTL_UNIT_MINUTE,
            b'h' => TTL_UNIT_HOUR,
            b'd' => TTL_UNIT_DAY,
            b'w' => TTL_UNIT_WEEK,
            b'M' => TTL_UNIT_MONTH,
            b'y' => TTL_UNIT_YEAR,
            _ => return Err(format!("unknown TTL unit: {}", unit_byte as char)),
        };
        Ok(fit_ttl_count(count, unit))
    }

    /// Minutes representation.
    pub fn minutes(&self) -> u32 {
        (self.to_seconds() / 60) as u32
    }
}

fn unit_to_seconds(count: u64, unit: u8) -> u64 {
    match unit {
        TTL_UNIT_EMPTY => 0,
        TTL_UNIT_MINUTE => count * 60,
        TTL_UNIT_HOUR => count * 60 * 60,
        TTL_UNIT_DAY => count * 60 * 60 * 24,
        TTL_UNIT_WEEK => count * 60 * 60 * 24 * 7,
        TTL_UNIT_MONTH => count * 60 * 60 * 24 * 30,
        TTL_UNIT_YEAR => count * 60 * 60 * 24 * 365,
        _ => 0,
    }
}

/// Fit a count+unit into a TTL that fits in a single byte count.
/// Matches Go's readTTL: if count already fits in u8, keep original unit.
/// Only rescale to a larger unit when count > 255.
fn fit_ttl_count(count: u32, unit: u8) -> TTL {
    if count == 0 || unit == TTL_UNIT_EMPTY {
        return TTL::EMPTY;
    }

    // If count fits in a byte, preserve the caller's unit (matches Go).
    if count <= 255 {
        return TTL { count: count as u8, unit };
    }

    // Count overflows a byte — rescale to a coarser unit.
    let seconds = unit_to_seconds(count as u64, unit);

    const YEAR_SECS: u64 = 3600 * 24 * 365;
    const MONTH_SECS: u64 = 3600 * 24 * 30;
    const WEEK_SECS: u64 = 3600 * 24 * 7;
    const DAY_SECS: u64 = 3600 * 24;
    const HOUR_SECS: u64 = 3600;
    const MINUTE_SECS: u64 = 60;

    // First pass: try exact fits from largest to smallest
    if seconds % YEAR_SECS == 0 && seconds / YEAR_SECS < 256 {
        return TTL { count: (seconds / YEAR_SECS) as u8, unit: TTL_UNIT_YEAR };
    }
    if seconds % MONTH_SECS == 0 && seconds / MONTH_SECS < 256 {
        return TTL { count: (seconds / MONTH_SECS) as u8, unit: TTL_UNIT_MONTH };
    }
    if seconds % WEEK_SECS == 0 && seconds / WEEK_SECS < 256 {
        return TTL { count: (seconds / WEEK_SECS) as u8, unit: TTL_UNIT_WEEK };
    }
    if seconds % DAY_SECS == 0 && seconds / DAY_SECS < 256 {
        return TTL { count: (seconds / DAY_SECS) as u8, unit: TTL_UNIT_DAY };
    }
    if seconds % HOUR_SECS == 0 && seconds / HOUR_SECS < 256 {
        return TTL { count: (seconds / HOUR_SECS) as u8, unit: TTL_UNIT_HOUR };
    }
    // Minutes: truncating division
    if seconds / MINUTE_SECS < 256 {
        return TTL { count: (seconds / MINUTE_SECS) as u8, unit: TTL_UNIT_MINUTE };
    }
    // Second pass: truncating division from smallest to largest
    if seconds / HOUR_SECS < 256 {
        return TTL { count: (seconds / HOUR_SECS) as u8, unit: TTL_UNIT_HOUR };
    }
    if seconds / DAY_SECS < 256 {
        return TTL { count: (seconds / DAY_SECS) as u8, unit: TTL_UNIT_DAY };
    }
    if seconds / WEEK_SECS < 256 {
        return TTL { count: (seconds / WEEK_SECS) as u8, unit: TTL_UNIT_WEEK };
    }
    if seconds / MONTH_SECS < 256 {
        return TTL { count: (seconds / MONTH_SECS) as u8, unit: TTL_UNIT_MONTH };
    }
    if seconds / YEAR_SECS < 256 {
        return TTL { count: (seconds / YEAR_SECS) as u8, unit: TTL_UNIT_YEAR };
    }
    TTL::EMPTY
}

fn unit_to_char(unit: u8) -> char {
    match unit {
        TTL_UNIT_MINUTE => 'm',
        TTL_UNIT_HOUR => 'h',
        TTL_UNIT_DAY => 'd',
        TTL_UNIT_WEEK => 'w',
        TTL_UNIT_MONTH => 'M',
        TTL_UNIT_YEAR => 'y',
        _ => ' ',
    }
}

impl fmt::Display for TTL {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.count == 0 || self.unit == TTL_UNIT_EMPTY {
            return write!(f, "");
        }
        write!(f, "{}{}", self.count, unit_to_char(self.unit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ttl_parse() {
        let ttl = TTL::read("3m").unwrap();
        assert_eq!(
            ttl,
            TTL {
                count: 3,
                unit: TTL_UNIT_MINUTE
            }
        );
        assert_eq!(ttl.to_seconds(), 180);
    }

    #[test]
    fn test_ttl_parse_hours() {
        let ttl = TTL::read("24h").unwrap();
        assert_eq!(ttl.to_seconds(), 86400);
    }

    #[test]
    fn test_ttl_display() {
        let ttl = TTL {
            count: 5,
            unit: TTL_UNIT_DAY,
        };
        assert_eq!(ttl.to_string(), "5d");
    }

    #[test]
    fn test_ttl_bytes_round_trip() {
        let ttl = TTL {
            count: 10,
            unit: TTL_UNIT_WEEK,
        };
        let mut buf = [0u8; 2];
        ttl.to_bytes(&mut buf);
        let ttl2 = TTL::from_bytes(&buf);
        assert_eq!(ttl, ttl2);
    }

    #[test]
    fn test_ttl_u32_round_trip() {
        let ttl = TTL {
            count: 42,
            unit: TTL_UNIT_HOUR,
        };
        let v = ttl.to_u32();
        let ttl2 = TTL::from_u32(v);
        assert_eq!(ttl, ttl2);
    }

    #[test]
    fn test_ttl_empty() {
        assert!(TTL::EMPTY.is_empty());
        assert_eq!(TTL::EMPTY.to_seconds(), 0);
        assert_eq!(TTL::EMPTY.to_u32(), 0);
    }

    #[test]
    fn test_ttl_overflow_fit() {
        // 300 minutes should fit into 5 hours
        let ttl = TTL::read("300m").unwrap();
        assert_eq!(
            ttl,
            TTL {
                count: 5,
                unit: TTL_UNIT_HOUR
            }
        );
    }
}
