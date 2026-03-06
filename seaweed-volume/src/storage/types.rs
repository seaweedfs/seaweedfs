//! Core storage types: NeedleId, Offset, Size, Cookie, DiskType.
//!
//! These types define the binary-compatible on-disk format matching the Go implementation.
//! CRITICAL: Byte layout must match exactly for cross-compatibility.

use std::fmt;

// ============================================================================
// Constants
// ============================================================================

pub const NEEDLE_ID_SIZE: usize = 8;
pub const NEEDLE_ID_EMPTY: u64 = 0;
pub const COOKIE_SIZE: usize = 4;
pub const OFFSET_SIZE: usize = 5; // 5-byte offset (8TB max volume)
pub const SIZE_SIZE: usize = 4;
pub const NEEDLE_HEADER_SIZE: usize = COOKIE_SIZE + NEEDLE_ID_SIZE + SIZE_SIZE; // 16
pub const DATA_SIZE_SIZE: usize = 4;
pub const NEEDLE_MAP_ENTRY_SIZE: usize = NEEDLE_ID_SIZE + OFFSET_SIZE + SIZE_SIZE; // 17
pub const TIMESTAMP_SIZE: usize = 8;
pub const NEEDLE_PADDING_SIZE: usize = 8;
pub const NEEDLE_CHECKSUM_SIZE: usize = 4;

/// Maximum possible volume size with 5-byte offset: 8TB
/// Formula: 4 * 1024 * 1024 * 1024 * 8 * 256
pub const MAX_POSSIBLE_VOLUME_SIZE: u64 = 4 * 1024 * 1024 * 1024 * 8 * 256;

// ============================================================================
// NeedleId
// ============================================================================

/// 64-bit unique identifier for a needle within a volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct NeedleId(pub u64);

impl NeedleId {
    pub fn to_bytes(&self, bytes: &mut [u8]) {
        assert!(bytes.len() >= NEEDLE_ID_SIZE);
        bytes[0..8].copy_from_slice(&self.0.to_be_bytes());
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= NEEDLE_ID_SIZE);
        NeedleId(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    /// Parse a hex string into a NeedleId.
    pub fn parse(s: &str) -> Result<Self, std::num::ParseIntError> {
        u64::from_str_radix(s, 16).map(NeedleId)
    }
}

impl fmt::Display for NeedleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}", self.0)
    }
}

impl From<u64> for NeedleId {
    fn from(v: u64) -> Self { NeedleId(v) }
}

impl From<NeedleId> for u64 {
    fn from(v: NeedleId) -> Self { v.0 }
}

// ============================================================================
// Cookie
// ============================================================================

/// Random 32-bit value to mitigate brute-force lookups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Cookie(pub u32);

impl Cookie {
    pub fn to_bytes(&self, bytes: &mut [u8]) {
        assert!(bytes.len() >= COOKIE_SIZE);
        bytes[0..4].copy_from_slice(&self.0.to_be_bytes());
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= COOKIE_SIZE);
        Cookie(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// Parse a hex string into a Cookie.
    pub fn parse(s: &str) -> Result<Self, std::num::ParseIntError> {
        u32::from_str_radix(s, 16).map(Cookie)
    }
}

impl fmt::Display for Cookie {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}", self.0)
    }
}

impl From<u32> for Cookie {
    fn from(v: u32) -> Self { Cookie(v) }
}

// ============================================================================
// Size
// ============================================================================

/// Needle size as stored in the index. Negative = deleted.
///
/// - Positive: valid needle with that many bytes of body content
/// - TombstoneFileSize (-1): tombstone marker
/// - Other negative: deleted, absolute value was the original size
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Size(pub i32);

/// Special marker for a tombstone (deletion marker) entry.
pub const TOMBSTONE_FILE_SIZE: Size = Size(-1);

impl Size {
    pub fn is_tombstone(&self) -> bool {
        self.0 == TOMBSTONE_FILE_SIZE.0
    }

    pub fn is_deleted(&self) -> bool {
        self.0 < 0 || self.0 == TOMBSTONE_FILE_SIZE.0
    }

    pub fn is_valid(&self) -> bool {
        self.0 > 0 && !self.is_tombstone()
    }

    /// Raw storage size. For tombstones returns 0; for negative returns abs value.
    pub fn raw(&self) -> u32 {
        if self.is_tombstone() {
            return 0;
        }
        if self.0 < 0 {
            return (self.0 * -1) as u32;
        }
        self.0 as u32
    }

    pub fn to_bytes(&self, bytes: &mut [u8]) {
        assert!(bytes.len() >= SIZE_SIZE);
        bytes[0..4].copy_from_slice(&(self.0 as u32).to_be_bytes());
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= SIZE_SIZE);
        let v = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        Size(v as i32)
    }
}

impl From<i32> for Size {
    fn from(v: i32) -> Self { Size(v) }
}

impl From<Size> for i32 {
    fn from(v: Size) -> Self { v.0 }
}

// ============================================================================
// Offset (5 bytes)
// ============================================================================

/// 5-byte offset encoding for needle positions in .dat files.
///
/// The offset is stored divided by NEEDLE_PADDING_SIZE (8), so 5 bytes can
/// address up to 8TB. The on-disk byte layout in .idx files is:
///   [b3][b2][b1][b0][b4]  (big-endian lower 4 bytes, then highest byte)
///
/// actual_offset = stored_value * 8
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Offset {
    /// Lower 4 bytes stored as b3(MSB)..b0(LSB)
    pub b0: u8,
    pub b1: u8,
    pub b2: u8,
    pub b3: u8,
    /// Highest byte (5th byte)
    pub b4: u8,
}

impl Offset {
    /// Convert to the actual byte offset in the .dat file.
    pub fn to_actual_offset(&self) -> i64 {
        let stored = self.b0 as i64
            + (self.b1 as i64) * 256
            + (self.b2 as i64) * 65536
            + (self.b3 as i64) * 16777216
            + (self.b4 as i64) * 4294967296;
        stored * NEEDLE_PADDING_SIZE as i64
    }

    /// Create an Offset from an actual byte offset.
    pub fn from_actual_offset(offset: i64) -> Self {
        let smaller = offset / NEEDLE_PADDING_SIZE as i64;
        Offset {
            b0: smaller as u8,
            b1: (smaller >> 8) as u8,
            b2: (smaller >> 16) as u8,
            b3: (smaller >> 24) as u8,
            b4: (smaller >> 32) as u8,
        }
    }

    /// Serialize to 5 bytes in the .idx file format.
    /// Layout: [b3][b2][b1][b0][b4]
    pub fn to_bytes(&self, bytes: &mut [u8]) {
        assert!(bytes.len() >= OFFSET_SIZE);
        bytes[0] = self.b3;
        bytes[1] = self.b2;
        bytes[2] = self.b1;
        bytes[3] = self.b0;
        bytes[4] = self.b4;
    }

    /// Deserialize from 5 bytes in the .idx file format.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= OFFSET_SIZE);
        Offset {
            b3: bytes[0],
            b2: bytes[1],
            b1: bytes[2],
            b0: bytes[3],
            b4: bytes[4],
        }
    }

    pub fn is_zero(&self) -> bool {
        self.b0 == 0 && self.b1 == 0 && self.b2 == 0 && self.b3 == 0 && self.b4 == 0
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_actual_offset())
    }
}

// ============================================================================
// DiskType
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DiskType {
    HardDrive,
    Hdd,
    Ssd,
    Custom(String),
}

impl DiskType {
    pub fn from_string(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "" => DiskType::HardDrive,
            "hdd" => DiskType::Hdd,
            "ssd" => DiskType::Ssd,
            other => DiskType::Custom(other.to_string()),
        }
    }

    pub fn readable_string(&self) -> &str {
        match self {
            DiskType::HardDrive => "hdd",
            DiskType::Hdd => "hdd",
            DiskType::Ssd => "ssd",
            DiskType::Custom(s) => s,
        }
    }
}

impl fmt::Display for DiskType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DiskType::HardDrive => write!(f, ""),
            DiskType::Hdd => write!(f, "hdd"),
            DiskType::Ssd => write!(f, "ssd"),
            DiskType::Custom(s) => write!(f, "{}", s),
        }
    }
}

impl Default for DiskType {
    fn default() -> Self { DiskType::HardDrive }
}

// ============================================================================
// VolumeId
// ============================================================================

/// Volume identifier, stored as u32.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct VolumeId(pub u32);

impl VolumeId {
    pub fn parse(s: &str) -> Result<Self, std::num::ParseIntError> {
        s.parse::<u32>().map(VolumeId)
    }

    pub fn next(&self) -> VolumeId {
        VolumeId(self.0 + 1)
    }
}

impl fmt::Display for VolumeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for VolumeId {
    fn from(v: u32) -> Self { VolumeId(v) }
}

// ============================================================================
// Version
// ============================================================================

/// Needle storage format version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version(pub u8);

pub const VERSION_1: Version = Version(1);
pub const VERSION_2: Version = Version(2);
pub const VERSION_3: Version = Version(3);

impl Version {
    pub fn current() -> Self { VERSION_3 }

    pub fn is_supported(&self) -> bool {
        self.0 >= 1 && self.0 <= 3
    }
}

impl Default for Version {
    fn default() -> Self { VERSION_3 }
}

impl From<u8> for Version {
    fn from(v: u8) -> Self { Version(v) }
}

// ============================================================================
// NeedleMapEntry helpers (for .idx file)
// ============================================================================

/// Parse a single .idx file entry (17 bytes) into (NeedleId, Offset, Size).
pub fn idx_entry_from_bytes(bytes: &[u8]) -> (NeedleId, Offset, Size) {
    assert!(bytes.len() >= NEEDLE_MAP_ENTRY_SIZE);
    let key = NeedleId::from_bytes(&bytes[..NEEDLE_ID_SIZE]);
    let offset = Offset::from_bytes(&bytes[NEEDLE_ID_SIZE..NEEDLE_ID_SIZE + OFFSET_SIZE]);
    let size = Size::from_bytes(&bytes[NEEDLE_ID_SIZE + OFFSET_SIZE..NEEDLE_ID_SIZE + OFFSET_SIZE + SIZE_SIZE]);
    (key, offset, size)
}

/// Write a single .idx file entry (17 bytes).
pub fn idx_entry_to_bytes(bytes: &mut [u8], key: NeedleId, offset: Offset, size: Size) {
    assert!(bytes.len() >= NEEDLE_MAP_ENTRY_SIZE);
    key.to_bytes(&mut bytes[..NEEDLE_ID_SIZE]);
    offset.to_bytes(&mut bytes[NEEDLE_ID_SIZE..NEEDLE_ID_SIZE + OFFSET_SIZE]);
    size.to_bytes(&mut bytes[NEEDLE_ID_SIZE + OFFSET_SIZE..NEEDLE_ID_SIZE + OFFSET_SIZE + SIZE_SIZE]);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_needle_id_round_trip() {
        let id = NeedleId(0x123456789abcdef0);
        let mut buf = [0u8; 8];
        id.to_bytes(&mut buf);
        let id2 = NeedleId::from_bytes(&buf);
        assert_eq!(id, id2);
    }

    #[test]
    fn test_needle_id_display() {
        let id = NeedleId(255);
        assert_eq!(id.to_string(), "ff");
    }

    #[test]
    fn test_needle_id_parse() {
        let id = NeedleId::parse("ff").unwrap();
        assert_eq!(id, NeedleId(255));
    }

    #[test]
    fn test_cookie_round_trip() {
        let cookie = Cookie(0xdeadbeef);
        let mut buf = [0u8; 4];
        cookie.to_bytes(&mut buf);
        let cookie2 = Cookie::from_bytes(&buf);
        assert_eq!(cookie, cookie2);
    }

    #[test]
    fn test_size_semantics() {
        assert!(Size(100).is_valid());
        assert!(!Size(100).is_deleted());
        assert!(!Size(100).is_tombstone());
        assert_eq!(Size(100).raw(), 100);

        assert!(Size(-50).is_deleted());
        assert!(!Size(-50).is_tombstone());
        assert_eq!(Size(-50).raw(), 50);

        assert!(TOMBSTONE_FILE_SIZE.is_deleted());
        assert!(TOMBSTONE_FILE_SIZE.is_tombstone());
        assert_eq!(TOMBSTONE_FILE_SIZE.raw(), 0);

        assert!(!Size(0).is_valid());
        assert!(!Size(0).is_deleted());
    }

    #[test]
    fn test_size_round_trip() {
        let size = Size(12345);
        let mut buf = [0u8; 4];
        size.to_bytes(&mut buf);
        let size2 = Size::from_bytes(&buf);
        assert_eq!(size, size2);
    }

    #[test]
    fn test_size_negative_round_trip() {
        // Negative sizes round-trip through u32 bit pattern
        let size = Size(-50);
        let mut buf = [0u8; 4];
        size.to_bytes(&mut buf);
        let size2 = Size::from_bytes(&buf);
        assert_eq!(size, size2);
    }

    #[test]
    fn test_offset_round_trip() {
        // Test with a known actual offset
        let actual_offset: i64 = 8 * 1000000; // must be multiple of 8
        let offset = Offset::from_actual_offset(actual_offset);
        assert_eq!(offset.to_actual_offset(), actual_offset);

        // Test byte serialization
        let mut buf = [0u8; 5];
        offset.to_bytes(&mut buf);
        let offset2 = Offset::from_bytes(&buf);
        assert_eq!(offset.to_actual_offset(), offset2.to_actual_offset());
    }

    #[test]
    fn test_offset_zero() {
        let offset = Offset::default();
        assert!(offset.is_zero());
        assert_eq!(offset.to_actual_offset(), 0);
    }

    #[test]
    fn test_offset_max() {
        // Max 5-byte stored value = 2^40 - 1
        let max_stored: i64 = (1i64 << 40) - 1;
        let max_actual = max_stored * NEEDLE_PADDING_SIZE as i64;
        let offset = Offset::from_actual_offset(max_actual);
        assert_eq!(offset.to_actual_offset(), max_actual);
    }

    #[test]
    fn test_idx_entry_round_trip() {
        let key = NeedleId(0xdeadbeef12345678);
        let offset = Offset::from_actual_offset(8 * 999);
        let size = Size(4096);

        let mut buf = [0u8; NEEDLE_MAP_ENTRY_SIZE];
        idx_entry_to_bytes(&mut buf, key, offset, size);

        let (key2, offset2, size2) = idx_entry_from_bytes(&buf);
        assert_eq!(key, key2);
        assert_eq!(offset.to_actual_offset(), offset2.to_actual_offset());
        assert_eq!(size, size2);
    }

    #[test]
    fn test_volume_id() {
        let vid = VolumeId::parse("42").unwrap();
        assert_eq!(vid, VolumeId(42));
        assert_eq!(vid.to_string(), "42");
        assert_eq!(vid.next(), VolumeId(43));
    }

    #[test]
    fn test_version() {
        assert!(VERSION_1.is_supported());
        assert!(VERSION_2.is_supported());
        assert!(VERSION_3.is_supported());
        assert!(!Version(0).is_supported());
        assert!(!Version(4).is_supported());
        assert_eq!(Version::current(), VERSION_3);
    }

    #[test]
    fn test_disk_type() {
        assert_eq!(DiskType::from_string(""), DiskType::HardDrive);
        assert_eq!(DiskType::from_string("hdd"), DiskType::Hdd);
        assert_eq!(DiskType::from_string("SSD"), DiskType::Ssd);
        assert_eq!(DiskType::from_string("nvme"), DiskType::Custom("nvme".to_string()));
        assert_eq!(DiskType::HardDrive.readable_string(), "hdd");
        assert_eq!(DiskType::Ssd.readable_string(), "ssd");
    }
}
