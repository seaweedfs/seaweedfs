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
    pub fn read(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(TTL::EMPTY);
        }
        let (num_str, unit_char) = s.split_at(s.len() - 1);
        let count: u32 = num_str
            .parse()
            .map_err(|e| format!("invalid TTL count: {}", e))?;
        let unit = match unit_char {
            "m" => TTL_UNIT_MINUTE,
            "h" => TTL_UNIT_HOUR,
            "d" => TTL_UNIT_DAY,
            "w" => TTL_UNIT_WEEK,
            "M" => TTL_UNIT_MONTH,
            "y" => TTL_UNIT_YEAR,
            _ => return Err(format!("unknown TTL unit: {}", unit_char)),
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

/// Fit a count into a single byte, converting to larger unit if needed.
fn fit_ttl_count(count: u32, unit: u8) -> TTL {
    if count <= 255 {
        return TTL {
            count: count as u8,
            unit,
        };
    }
    // Try next larger unit
    match unit {
        TTL_UNIT_MINUTE => {
            if count / 60 <= 255 {
                return TTL {
                    count: (count / 60) as u8,
                    unit: TTL_UNIT_HOUR,
                };
            }
            if count / (60 * 24) <= 255 {
                return TTL {
                    count: (count / (60 * 24)) as u8,
                    unit: TTL_UNIT_DAY,
                };
            }
            TTL {
                count: 255,
                unit: TTL_UNIT_DAY,
            }
        }
        TTL_UNIT_HOUR => {
            if count / 24 <= 255 {
                return TTL {
                    count: (count / 24) as u8,
                    unit: TTL_UNIT_DAY,
                };
            }
            TTL {
                count: 255,
                unit: TTL_UNIT_DAY,
            }
        }
        TTL_UNIT_DAY => {
            if count / 7 <= 255 {
                return TTL {
                    count: (count / 7) as u8,
                    unit: TTL_UNIT_WEEK,
                };
            }
            if count / 30 <= 255 {
                return TTL {
                    count: (count / 30) as u8,
                    unit: TTL_UNIT_MONTH,
                };
            }
            if count / 365 <= 255 {
                return TTL {
                    count: (count / 365) as u8,
                    unit: TTL_UNIT_YEAR,
                };
            }
            TTL {
                count: 255,
                unit: TTL_UNIT_YEAR,
            }
        }
        TTL_UNIT_WEEK => {
            if count * 7 / 30 <= 255 {
                return TTL {
                    count: (count * 7 / 30) as u8,
                    unit: TTL_UNIT_MONTH,
                };
            }
            if count * 7 / 365 <= 255 {
                return TTL {
                    count: (count * 7 / 365) as u8,
                    unit: TTL_UNIT_YEAR,
                };
            }
            TTL {
                count: 255,
                unit: TTL_UNIT_YEAR,
            }
        }
        TTL_UNIT_MONTH => {
            if count / 12 <= 255 {
                return TTL {
                    count: (count / 12) as u8,
                    unit: TTL_UNIT_YEAR,
                };
            }
            TTL {
                count: 255,
                unit: TTL_UNIT_YEAR,
            }
        }
        _ => TTL { count: 255, unit },
    }
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
        if self.is_empty() {
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
