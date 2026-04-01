//! CRC32-Castagnoli checksum for needle data integrity.
//!
//! Matches Go's `crc32.MakeTable(crc32.Castagnoli)` exactly.
//! The CRC is stored as raw u32 (not the `.Value()` legacy transform).

/// CRC32-Castagnoli checksum wrapper.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CRC(pub u32);

impl CRC {
    /// Compute CRC from a byte slice (starting from 0).
    pub fn new(data: &[u8]) -> Self {
        CRC(0).update(data)
    }

    /// Update the CRC with additional bytes.
    pub fn update(self, data: &[u8]) -> Self {
        CRC(crc32c::crc32c_append(self.0, data))
    }

    /// Legacy `.Value()` function — deprecated in Go but needed for backward compat check.
    /// Formula: (crc >> 15 | crc << 17) + 0xa282ead8
    pub fn legacy_value(&self) -> u32 {
        (self.0 >> 15 | self.0 << 17).wrapping_add(0xa282ead8)
    }
}

impl From<u32> for CRC {
    fn from(v: u32) -> Self {
        CRC(v)
    }
}

impl From<CRC> for u32 {
    fn from(c: CRC) -> Self {
        c.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc_empty() {
        let crc = CRC::new(&[]);
        assert_eq!(crc.0, 0);
    }

    #[test]
    fn test_crc_known_value() {
        // CRC32-C of "hello" — verify it produces a non-zero deterministic value
        let crc = CRC::new(b"hello");
        assert_ne!(crc.0, 0);
        // Same input produces same output
        assert_eq!(crc, CRC::new(b"hello"));
    }

    #[test]
    fn test_crc_incremental() {
        let crc1 = CRC::new(b"hello world");
        let crc2 = CRC::new(b"hello").update(b" world");
        assert_eq!(crc1, crc2);
    }

    #[test]
    fn test_crc_legacy_value() {
        let crc = CRC(0x12345678);
        let v = crc.legacy_value();
        let expected = (0x12345678u32 >> 15 | 0x12345678u32 << 17).wrapping_add(0xa282ead8);
        assert_eq!(v, expected);
    }
}
