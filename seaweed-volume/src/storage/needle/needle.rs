//! Needle: the individual file object stored in a volume.
//!
//! Binary format (Version 2/3):
//!   Header (16 bytes): Cookie(4) + NeedleId(8) + Size(4)
//!   Body (Size bytes):
//!     DataSize(4) + Data(DataSize) + Flags(1)
//!     [if HasName]:    NameSize(1) + Name(NameSize)
//!     [if HasMime]:    MimeSize(1) + Mime(MimeSize)
//!     [if HasLastMod]: LastModified(5)
//!     [if HasTtl]:     TTL(2)
//!     [if HasPairs]:   PairsSize(2) + Pairs(PairsSize)
//!   Tail:
//!     Checksum(4) + [if V3: AppendAtNs(8)] + Padding(0-7)

use super::crc::CRC;
use super::ttl::TTL;
use crate::storage::types::*;

// Flag bits (matching Go constants)
pub const FLAG_IS_COMPRESSED: u8 = 0x01;
pub const FLAG_HAS_NAME: u8 = 0x02;
pub const FLAG_HAS_MIME: u8 = 0x04;
pub const FLAG_HAS_LAST_MODIFIED_DATE: u8 = 0x08;
pub const FLAG_HAS_TTL: u8 = 0x10;
pub const FLAG_HAS_PAIRS: u8 = 0x20;
pub const FLAG_IS_CHUNK_MANIFEST: u8 = 0x80;

pub const LAST_MODIFIED_BYTES_LENGTH: usize = 5;
pub const TTL_BYTES_LENGTH: usize = 2;

#[derive(Debug, Clone, Default)]
pub struct Needle {
    pub cookie: Cookie,
    pub id: NeedleId,
    pub size: Size, // sum of body content fields

    // Version 2+ fields
    pub data_size: u32,
    pub data: Vec<u8>,
    pub flags: u8,
    pub name_size: u8,
    pub name: Vec<u8>,   // max 255 bytes
    pub mime_size: u8,
    pub mime: Vec<u8>,   // max 255 bytes
    pub pairs_size: u16,
    pub pairs: Vec<u8>,  // max 64KB, JSON
    pub last_modified: u64, // stored as 5 bytes on disk
    pub ttl: Option<TTL>,

    // Tail fields
    pub checksum: CRC,
    pub append_at_ns: u64, // Version 3 only
    pub padding: Vec<u8>,
}

impl Needle {
    // ---- Flag accessors (matching Go) ----

    pub fn is_compressed(&self) -> bool { self.flags & FLAG_IS_COMPRESSED != 0 }
    pub fn set_is_compressed(&mut self) { self.flags |= FLAG_IS_COMPRESSED; }

    pub fn has_name(&self) -> bool { self.flags & FLAG_HAS_NAME != 0 }
    pub fn set_has_name(&mut self) { self.flags |= FLAG_HAS_NAME; }

    pub fn has_mime(&self) -> bool { self.flags & FLAG_HAS_MIME != 0 }
    pub fn set_has_mime(&mut self) { self.flags |= FLAG_HAS_MIME; }

    pub fn has_last_modified_date(&self) -> bool { self.flags & FLAG_HAS_LAST_MODIFIED_DATE != 0 }
    pub fn set_has_last_modified_date(&mut self) { self.flags |= FLAG_HAS_LAST_MODIFIED_DATE; }

    pub fn has_ttl(&self) -> bool { self.flags & FLAG_HAS_TTL != 0 }
    pub fn set_has_ttl(&mut self) { self.flags |= FLAG_HAS_TTL; }

    pub fn has_pairs(&self) -> bool { self.flags & FLAG_HAS_PAIRS != 0 }
    pub fn set_has_pairs(&mut self) { self.flags |= FLAG_HAS_PAIRS; }

    pub fn is_chunk_manifest(&self) -> bool { self.flags & FLAG_IS_CHUNK_MANIFEST != 0 }
    pub fn set_is_chunk_manifest(&mut self) { self.flags |= FLAG_IS_CHUNK_MANIFEST; }

    // ---- Header parsing ----

    /// Parse the 16-byte needle header.
    pub fn parse_header(bytes: &[u8]) -> (Cookie, NeedleId, Size) {
        assert!(bytes.len() >= NEEDLE_HEADER_SIZE);
        let cookie = Cookie::from_bytes(&bytes[0..COOKIE_SIZE]);
        let id = NeedleId::from_bytes(&bytes[COOKIE_SIZE..COOKIE_SIZE + NEEDLE_ID_SIZE]);
        let size = Size::from_bytes(&bytes[COOKIE_SIZE + NEEDLE_ID_SIZE..NEEDLE_HEADER_SIZE]);
        (cookie, id, size)
    }

    /// Parse needle header into self.
    pub fn read_header(&mut self, bytes: &[u8]) {
        let (cookie, id, size) = Self::parse_header(bytes);
        self.cookie = cookie;
        self.id = id;
        self.size = size;
    }

    // ---- Body reading (Version 2/3) ----

    /// Read the version 2/3 body data from bytes (size bytes starting after header).
    pub fn read_body_v2(&mut self, bytes: &[u8]) -> Result<(), NeedleError> {
        let len_bytes = bytes.len();
        let mut index = 0;

        // DataSize (4 bytes)
        if index + 4 > len_bytes {
            return Err(NeedleError::IndexOutOfRange(1));
        }
        self.data_size = u32::from_be_bytes([bytes[index], bytes[index + 1], bytes[index + 2], bytes[index + 3]]);
        index += 4;

        // Data
        if index + self.data_size as usize > len_bytes {
            return Err(NeedleError::IndexOutOfRange(1));
        }
        self.data = bytes[index..index + self.data_size as usize].to_vec();
        index += self.data_size as usize;

        // Read non-data metadata
        self.read_body_v2_non_data(&bytes[index..])?;
        Ok(())
    }

    /// Read version 2/3 metadata fields (everything after Data).
    fn read_body_v2_non_data(&mut self, bytes: &[u8]) -> Result<usize, NeedleError> {
        let len_bytes = bytes.len();
        let mut index = 0;

        // Flags (1 byte)
        if index < len_bytes {
            self.flags = bytes[index];
            index += 1;
        }

        // Name
        if index < len_bytes && self.has_name() {
            self.name_size = bytes[index];
            index += 1;
            if index + self.name_size as usize > len_bytes {
                return Err(NeedleError::IndexOutOfRange(2));
            }
            self.name = bytes[index..index + self.name_size as usize].to_vec();
            index += self.name_size as usize;
        }

        // Mime
        if index < len_bytes && self.has_mime() {
            self.mime_size = bytes[index];
            index += 1;
            if index + self.mime_size as usize > len_bytes {
                return Err(NeedleError::IndexOutOfRange(3));
            }
            self.mime = bytes[index..index + self.mime_size as usize].to_vec();
            index += self.mime_size as usize;
        }

        // LastModified (5 bytes)
        if index < len_bytes && self.has_last_modified_date() {
            if index + LAST_MODIFIED_BYTES_LENGTH > len_bytes {
                return Err(NeedleError::IndexOutOfRange(4));
            }
            self.last_modified = bytes_to_u64_5(&bytes[index..index + LAST_MODIFIED_BYTES_LENGTH]);
            index += LAST_MODIFIED_BYTES_LENGTH;
        }

        // TTL (2 bytes)
        if index < len_bytes && self.has_ttl() {
            if index + TTL_BYTES_LENGTH > len_bytes {
                return Err(NeedleError::IndexOutOfRange(5));
            }
            self.ttl = Some(TTL::from_bytes(&bytes[index..index + TTL_BYTES_LENGTH]));
            index += TTL_BYTES_LENGTH;
        }

        // Pairs
        if index < len_bytes && self.has_pairs() {
            if index + 2 > len_bytes {
                return Err(NeedleError::IndexOutOfRange(6));
            }
            self.pairs_size = u16::from_be_bytes([bytes[index], bytes[index + 1]]);
            index += 2;
            if index + self.pairs_size as usize > len_bytes {
                return Err(NeedleError::IndexOutOfRange(7));
            }
            self.pairs = bytes[index..index + self.pairs_size as usize].to_vec();
            index += self.pairs_size as usize;
        }

        Ok(index)
    }

    // ---- Tail reading ----

    /// Read the needle tail (checksum + optional timestamp + padding).
    pub fn read_tail(&mut self, tail_bytes: &[u8], version: Version) -> Result<(), NeedleError> {
        if tail_bytes.len() < NEEDLE_CHECKSUM_SIZE {
            return Err(NeedleError::TailTooShort);
        }

        let expected_checksum = CRC(u32::from_be_bytes([
            tail_bytes[0], tail_bytes[1], tail_bytes[2], tail_bytes[3],
        ]));

        if !self.data.is_empty() {
            let data_checksum = CRC::new(&self.data);
            if expected_checksum != data_checksum {
                return Err(NeedleError::CrcMismatch {
                    needle_id: self.id,
                    got: data_checksum.0,
                    want: expected_checksum.0,
                });
            }
            self.checksum = data_checksum;
        } else {
            self.checksum = expected_checksum;
        }

        if version == VERSION_3 {
            let ts_offset = NEEDLE_CHECKSUM_SIZE;
            if tail_bytes.len() < ts_offset + TIMESTAMP_SIZE {
                return Err(NeedleError::TailTooShort);
            }
            self.append_at_ns = u64::from_be_bytes([
                tail_bytes[ts_offset],
                tail_bytes[ts_offset + 1],
                tail_bytes[ts_offset + 2],
                tail_bytes[ts_offset + 3],
                tail_bytes[ts_offset + 4],
                tail_bytes[ts_offset + 5],
                tail_bytes[ts_offset + 6],
                tail_bytes[ts_offset + 7],
            ]);
        }

        Ok(())
    }

    // ---- Full read from bytes ----

    /// Read a complete needle from its raw bytes (header + body + tail).
    pub fn read_bytes(&mut self, bytes: &[u8], offset: i64, expected_size: Size, version: Version) -> Result<(), NeedleError> {
        self.read_header(bytes);

        if self.size != expected_size {
            return Err(NeedleError::SizeMismatch {
                offset,
                id: self.id,
                found: self.size,
                expected: expected_size,
            });
        }

        let body_start = NEEDLE_HEADER_SIZE;
        let body_end = body_start + self.size.0 as usize;

        if version == VERSION_1 {
            self.data = bytes[body_start..body_end].to_vec();
        } else {
            self.read_body_v2(&bytes[body_start..body_end])?;
        }

        self.read_tail(&bytes[body_end..], version)?;
        Ok(())
    }

    // ---- Write (serialize) ----

    /// Serialize the needle to bytes for writing to a .dat file (Version 2/3).
    pub fn write_bytes(&mut self, version: Version) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);

        // Compute sizes and flags
        if self.name_size >= 255 { self.name_size = 255; }
        if self.name.len() < self.name_size as usize { self.name_size = self.name.len() as u8; }
        self.data_size = self.data.len() as u32;
        self.mime_size = self.mime.len() as u8;

        // Compute n.Size (body size, excluding header)
        if self.data_size > 0 {
            let mut s: i32 = 4 + self.data_size as i32 + 1; // DataSize + Data + Flags
            if self.has_name() { s += 1 + self.name_size as i32; }
            if self.has_mime() { s += 1 + self.mime_size as i32; }
            if self.has_last_modified_date() { s += LAST_MODIFIED_BYTES_LENGTH as i32; }
            if self.has_ttl() { s += TTL_BYTES_LENGTH as i32; }
            if self.has_pairs() { s += 2 + self.pairs_size as i32; }
            self.size = Size(s);
        } else {
            self.size = Size(0);
        }

        // Header: Cookie(4) + NeedleId(8) + Size(4) = 16 bytes
        let mut header = [0u8; NEEDLE_HEADER_SIZE];
        self.cookie.to_bytes(&mut header[0..COOKIE_SIZE]);
        self.id.to_bytes(&mut header[COOKIE_SIZE..COOKIE_SIZE + NEEDLE_ID_SIZE]);
        self.size.to_bytes(&mut header[COOKIE_SIZE + NEEDLE_ID_SIZE..NEEDLE_HEADER_SIZE]);
        buf.extend_from_slice(&header);

        // Body
        if self.data_size > 0 {
            buf.extend_from_slice(&self.data_size.to_be_bytes());
            buf.extend_from_slice(&self.data);
            buf.push(self.flags);
            if self.has_name() {
                buf.push(self.name_size);
                buf.extend_from_slice(&self.name[..self.name_size as usize]);
            }
            if self.has_mime() {
                buf.push(self.mime_size);
                buf.extend_from_slice(&self.mime);
            }
            if self.has_last_modified_date() {
                // Write 5 bytes of last_modified (lower 5 bytes of u64 big-endian)
                let lm_bytes = self.last_modified.to_be_bytes();
                buf.extend_from_slice(&lm_bytes[8 - LAST_MODIFIED_BYTES_LENGTH..8]);
            }
            if self.has_ttl() {
                if let Some(ref ttl) = self.ttl {
                    let mut ttl_buf = [0u8; 2];
                    ttl.to_bytes(&mut ttl_buf);
                    buf.extend_from_slice(&ttl_buf);
                } else {
                    buf.extend_from_slice(&[0u8; 2]);
                }
            }
            if self.has_pairs() {
                buf.extend_from_slice(&self.pairs_size.to_be_bytes());
                buf.extend_from_slice(&self.pairs);
            }
        }

        // Compute checksum
        self.checksum = CRC::new(&self.data);

        // Tail: Checksum + [V3: AppendAtNs] + Padding
        buf.extend_from_slice(&self.checksum.0.to_be_bytes());
        if version == VERSION_3 {
            buf.extend_from_slice(&self.append_at_ns.to_be_bytes());
        }

        // Padding to 8-byte alignment
        let padding = padding_length(self.size, version).0 as usize;
        buf.extend(std::iter::repeat(0u8).take(padding));

        buf
    }

    /// Total disk size of this needle including header, body, checksum, timestamp, and padding.
    pub fn disk_size(&self, version: Version) -> i64 {
        get_actual_size(self.size, version)
    }

    /// Compute ETag string from checksum (matching Go).
    pub fn etag(&self) -> String {
        etag_from_checksum(self.checksum.0)
    }
}

// ============================================================================
// Helper functions (matching Go)
// ============================================================================

/// Compute padding to align needle to NEEDLE_PADDING_SIZE (8 bytes).
pub fn padding_length(needle_size: Size, version: Version) -> Size {
    if version == VERSION_3 {
        Size((NEEDLE_PADDING_SIZE as i32 - ((NEEDLE_HEADER_SIZE as i32 + needle_size.0 + NEEDLE_CHECKSUM_SIZE as i32 + TIMESTAMP_SIZE as i32) % NEEDLE_PADDING_SIZE as i32)) % NEEDLE_PADDING_SIZE as i32)
    } else {
        Size((NEEDLE_PADDING_SIZE as i32 - ((NEEDLE_HEADER_SIZE as i32 + needle_size.0 + NEEDLE_CHECKSUM_SIZE as i32) % NEEDLE_PADDING_SIZE as i32)) % NEEDLE_PADDING_SIZE as i32)
    }
}

/// Body length = Size + Checksum + [Timestamp] + Padding.
pub fn needle_body_length(needle_size: Size, version: Version) -> i64 {
    if version == VERSION_3 {
        needle_size.0 as i64 + NEEDLE_CHECKSUM_SIZE as i64 + TIMESTAMP_SIZE as i64 + padding_length(needle_size, version).0 as i64
    } else {
        needle_size.0 as i64 + NEEDLE_CHECKSUM_SIZE as i64 + padding_length(needle_size, version).0 as i64
    }
}

/// Total actual size on disk: Header + Body.
pub fn get_actual_size(size: Size, version: Version) -> i64 {
    NEEDLE_HEADER_SIZE as i64 + needle_body_length(size, version)
}

/// Read 5 bytes as a u64 (big-endian, zero-padded high bytes).
fn bytes_to_u64_5(bytes: &[u8]) -> u64 {
    assert!(bytes.len() >= 5);
    // The 5 bytes are the LOWER 5 bytes of a big-endian u64.
    // In Go: util.BytesToUint64(bytes[index : index+5]) reads into a uint64
    // Go's BytesToUint64 copies into the LAST 5 bytes of an 8-byte array (big-endian).
    let mut buf = [0u8; 8];
    buf[3..8].copy_from_slice(&bytes[..5]);
    u64::from_be_bytes(buf)
}

/// ETag formatted as Go: hex of big-endian u32 bytes.
pub fn etag_from_checksum(checksum: u32) -> String {
    let bits = checksum.to_be_bytes();
    format!("{:02x}{:02x}{:02x}{:02x}", bits[0], bits[1], bits[2], bits[3])
}

// ============================================================================
// FileId
// ============================================================================

/// FileId = VolumeId + NeedleId + Cookie.
/// String format: "<volume_id>,<needle_id_hex><cookie_hex>"
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileId {
    pub volume_id: VolumeId,
    pub key: NeedleId,
    pub cookie: Cookie,
}

impl FileId {
    pub fn new(volume_id: VolumeId, key: NeedleId, cookie: Cookie) -> Self {
        FileId { volume_id, key, cookie }
    }

    /// Parse "volume_id,needle_id_cookie" or "volume_id/needle_id_cookie".
    pub fn parse(s: &str) -> Result<Self, String> {
        let (vid_str, rest) = if let Some(pos) = s.find(',') {
            (&s[..pos], &s[pos + 1..])
        } else if let Some(pos) = s.find('/') {
            (&s[..pos], &s[pos + 1..])
        } else {
            return Err(format!("invalid file id: {}", s));
        };

        let volume_id = VolumeId::parse(vid_str).map_err(|e| format!("invalid volume id: {}", e))?;
        let (key, cookie) = parse_needle_id_cookie(rest)?;
        Ok(FileId { volume_id, key, cookie })
    }

    /// Format the needle_id + cookie part as a hex string (stripping leading zeros).
    pub fn needle_id_cookie_string(&self) -> String {
        format_needle_id_cookie(self.key, self.cookie)
    }
}

impl std::fmt::Display for FileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{},{}", self.volume_id, self.needle_id_cookie_string())
    }
}

/// Format NeedleId + Cookie as hex, stripping leading zero bytes.
fn format_needle_id_cookie(key: NeedleId, cookie: Cookie) -> String {
    // Encode 12 bytes: 8 for NeedleId + 4 for Cookie
    let mut bytes = [0u8; 12];
    key.to_bytes(&mut bytes[0..8]);
    cookie.to_bytes(&mut bytes[8..12]);

    // Strip leading zero bytes
    let first_nonzero = bytes.iter().position(|&b| b != 0).unwrap_or(bytes.len() - 1);
    hex::encode(&bytes[first_nonzero..])
}

/// Parse "needle_id_cookie_hex" into (NeedleId, Cookie).
pub fn parse_needle_id_cookie(s: &str) -> Result<(NeedleId, Cookie), String> {
    let decoded = hex::decode(s).map_err(|e| format!("invalid hex: {}", e))?;

    // Pad to 12 bytes (8 NeedleId + 4 Cookie)
    let mut bytes = [0u8; 12];
    if decoded.len() > 12 {
        return Err(format!("hex too long: {}", s));
    }
    let start = 12 - decoded.len();
    bytes[start..].copy_from_slice(&decoded);

    let key = NeedleId::from_bytes(&bytes[0..8]);
    let cookie = Cookie::from_bytes(&bytes[8..12]);
    Ok((key, cookie))
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum NeedleError {
    #[error("size mismatch at offset {offset}: found id={id} size={found:?}, expected size={expected:?}")]
    SizeMismatch { offset: i64, id: NeedleId, found: Size, expected: Size },

    #[error("CRC mismatch for needle {needle_id}: got {got:08x}, want {want:08x}")]
    CrcMismatch { needle_id: NeedleId, got: u32, want: u32 },

    #[error("index out of range ({0})")]
    IndexOutOfRange(u32),

    #[error("needle tail too short")]
    TailTooShort,

    #[error("unsupported version: {0}")]
    UnsupportedVersion(u8),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_header() {
        let mut buf = [0u8; NEEDLE_HEADER_SIZE];
        let cookie = Cookie(0xdeadbeef);
        let id = NeedleId(0x123456789abcdef0);
        let size = Size(1024);
        cookie.to_bytes(&mut buf[0..4]);
        id.to_bytes(&mut buf[4..12]);
        size.to_bytes(&mut buf[12..16]);

        let (c, i, s) = Needle::parse_header(&buf);
        assert_eq!(c, cookie);
        assert_eq!(i, id);
        assert_eq!(s, size);
    }

    #[test]
    fn test_needle_write_read_round_trip_v3() {
        let mut n = Needle::default();
        n.cookie = Cookie(42);
        n.id = NeedleId(100);
        n.data = b"hello world".to_vec();
        n.flags = 0;
        n.set_has_name();
        n.name = b"test.txt".to_vec();
        n.name_size = 8;
        n.set_has_mime();
        n.mime = b"text/plain".to_vec();
        n.mime_size = 10;
        n.set_has_last_modified_date();
        n.last_modified = 1234567890;
        n.set_has_ttl();
        n.ttl = Some(TTL { count: 5, unit: super::super::ttl::TTL_UNIT_DAY });
        n.append_at_ns = 999999999;

        let bytes = n.write_bytes(VERSION_3);

        // Verify total size matches expected
        let expected_size = get_actual_size(n.size, VERSION_3);
        assert_eq!(bytes.len() as i64, expected_size);

        // Read it back
        let mut n2 = Needle::default();
        n2.read_bytes(&bytes, 0, n.size, VERSION_3).unwrap();

        assert_eq!(n2.cookie, n.cookie);
        assert_eq!(n2.id, n.id);
        assert_eq!(n2.data, n.data);
        assert_eq!(n2.name, n.name);
        assert_eq!(n2.mime, n.mime);
        assert_eq!(n2.last_modified, n.last_modified);
        assert_eq!(n2.ttl, n.ttl);
        assert_eq!(n2.checksum, n.checksum);
        assert_eq!(n2.append_at_ns, n.append_at_ns);
    }

    #[test]
    fn test_needle_write_read_round_trip_v2() {
        let mut n = Needle::default();
        n.cookie = Cookie(77);
        n.id = NeedleId(200);
        n.data = b"data v2".to_vec();
        n.flags = 0;

        let bytes = n.write_bytes(VERSION_2);
        let expected_size = get_actual_size(n.size, VERSION_2);
        assert_eq!(bytes.len() as i64, expected_size);

        let mut n2 = Needle::default();
        n2.read_bytes(&bytes, 0, n.size, VERSION_2).unwrap();

        assert_eq!(n2.data, n.data);
        assert_eq!(n2.checksum, n.checksum);
    }

    #[test]
    fn test_padding_alignment() {
        // All actual sizes should be multiples of 8
        for size_val in 0..50 {
            let s = Size(size_val);
            let actual_v2 = get_actual_size(s, VERSION_2);
            let actual_v3 = get_actual_size(s, VERSION_3);
            assert_eq!(actual_v2 % 8, 0, "V2 size {} not aligned", size_val);
            assert_eq!(actual_v3 % 8, 0, "V3 size {} not aligned", size_val);
        }
    }

    #[test]
    fn test_file_id_parse() {
        let fid = FileId::parse("3,01637037d6").unwrap();
        assert_eq!(fid.volume_id, VolumeId(3));
        // The hex "01637037d6" is 5 bytes = 0x0163..., padded to 12 bytes
        assert!(!fid.key.is_empty() || !fid.cookie.0 == 0);
    }

    #[test]
    fn test_file_id_round_trip() {
        let fid = FileId::new(VolumeId(5), NeedleId(0x123456), Cookie(0xabcd));
        let s = fid.to_string();
        let fid2 = FileId::parse(&s).unwrap();
        assert_eq!(fid, fid2);
    }

    #[test]
    fn test_needle_id_cookie_format() {
        let s = format_needle_id_cookie(NeedleId(1), Cookie(0x12345678));
        let (key, cookie) = parse_needle_id_cookie(&s).unwrap();
        assert_eq!(key, NeedleId(1));
        assert_eq!(cookie, Cookie(0x12345678));
    }
}
