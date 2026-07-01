//! EC bitrot detection — checksum sidecar.
//!
//! A per-volume sidecar file stores a CRC32C (Castagnoli) checksum for every
//! fixed-size block of every EC shard, so a scrub (and the reconstruction path)
//! can detect silent disk corruption in any shard — including cold parity shards
//! that are never read during normal serving.
//!
//! The sidecar is OPTIONAL: an absent or generation-mismatched sidecar simply
//! means "feature off" for that generation, so old binaries, JSON-only nodes,
//! and rollback deployments ignore it and degrade gracefully.
//!
//! On-disk layout of `<base>.ecsum` (legacy/generation 0) and `<base>.ecsum.v<N>`:
//!
//! ```text
//! [ magic(4) | format_version(2) | payload_len(4) | payload_crc32c(4) ] [ proto payload ]
//! ```
//!
//! All header fields are BIG-ENDIAN. The header's `payload_crc32c` lets a loader
//! detect corruption of the sidecar itself BEFORE trusting any contents, so a
//! rotted sidecar can never be mistaken for shard corruption. This format is
//! byte-identical to the Go implementation in
//! `weed/storage/erasure_coding/ec_bitrot.go`.

use std::fs::File;
use std::io::{self, Read, Write};

use prost::Message;

use crate::pb::volume_server_pb::{
    ChecksumAlgorithm, EcBitrotProtection, EcShardChecksums, EcShardConfig,
};
use crate::storage::erasure_coding::ec_shard::MAX_SHARD_COUNT;
use crate::storage::needle::crc::CRC;

/// Canonical extension for the checksum sidecar. Generation 0 (legacy/fresh
/// encode) uses `<base>.ecsum`; vacuum generation N uses `<base>.ecsum.v<N>`,
/// mirroring the `.vif`/`.ecx` versioned convention.
pub const BITROT_SIDECAR_EXT: &str = ".ecsum";

/// Default checksum granularity (16 MiB). It is a power-of-two multiple of
/// `ERASURE_CODING_SMALL_BLOCK_SIZE` (1 MiB) and keeps the sidecar tiny
/// (~11 KB for a 30 GB volume) while localizing corruption to a 16 MiB region.
pub const DEFAULT_BITROT_BLOCK_SIZE: usize = 16 * 1024 * 1024;

/// Caps the block granularity so a loaded sidecar cannot force a huge
/// scrub/verify scratch buffer. Power-of-two multiple of 1 MiB.
pub const MAX_BITROT_BLOCK_SIZE: u32 = 64 * 1024 * 1024;

/// Magic "ECSU".
pub const BITROT_MAGIC: u32 = 0x4543_5355;
/// On-disk format version.
pub const BITROT_FORMAT_VERSION: u16 = 1;
/// Header size: magic(4) + version(2) + payload_len(4) + payload_crc32c(4).
pub const BITROT_HEADER_SIZE: usize = 14;

/// Resolved protection state of an EC volume's active generation after loading
/// and validating its sidecar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitrotStatus {
    /// No sidecar, or a sidecar that does not describe the active generation.
    /// The generation is unprotected; this is NOT corruption.
    Off,
    /// A complete, well-formed, generation-matching sidecar is loaded.
    On,
    /// A generation-matching sidecar that is malformed, incomplete, or
    /// self-integrity-failed. The generation is unprotected pending repair of
    /// the sidecar, and an integrity alarm should fire. The rebuild path treats
    /// this as fail-closed.
    Invalid,
}

impl std::fmt::Display for BitrotStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BitrotStatus::On => "on",
            BitrotStatus::Invalid => "invalid",
            BitrotStatus::Off => "off",
        };
        f.write_str(s)
    }
}

/// Error returned by [`load_bitrot_sidecar`]. A self-integrity failure is a
/// sidecar-integrity problem (the caller maps it to [`BitrotStatus::Invalid`]),
/// never a shard-corruption signal.
#[derive(Debug)]
pub enum BitrotLoadError {
    /// File missing or other underlying I/O error.
    Io(io::Error),
    /// File shorter than the fixed header.
    TooShort(usize),
    /// Header magic did not match.
    BadMagic(u32),
    /// Header format version is unsupported.
    UnsupportedVersion(u16),
    /// Header payload_len disagrees with the actual payload length.
    LengthMismatch { header: u32, actual: usize },
    /// Header payload_crc32c disagrees with the computed CRC32C of the payload.
    CrcMismatch { header: u32, computed: u32 },
    /// Protobuf payload failed to decode.
    Decode(prost::DecodeError),
}

impl std::fmt::Display for BitrotLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BitrotLoadError::Io(e) => write!(f, "bitrot sidecar io error: {}", e),
            BitrotLoadError::TooShort(n) => {
                write!(f, "bitrot sidecar too short ({} bytes)", n)
            }
            BitrotLoadError::BadMagic(m) => write!(f, "bitrot sidecar bad magic {:#x}", m),
            BitrotLoadError::UnsupportedVersion(v) => {
                write!(f, "bitrot sidecar unsupported format version {}", v)
            }
            BitrotLoadError::LengthMismatch { header, actual } => write!(
                f,
                "bitrot sidecar length mismatch: header {}, actual {}",
                header, actual
            ),
            BitrotLoadError::CrcMismatch { header, computed } => write!(
                f,
                "bitrot sidecar self-integrity CRC mismatch: header {:#x}, computed {:#x}",
                header, computed
            ),
            BitrotLoadError::Decode(e) => write!(f, "unmarshal bitrot sidecar: {}", e),
        }
    }
}

impl std::error::Error for BitrotLoadError {}

impl From<io::Error> for BitrotLoadError {
    fn from(e: io::Error) -> Self {
        BitrotLoadError::Io(e)
    }
}

/// Returns the sidecar path for a base file name and EC generation. Generation
/// 0 is the un-suffixed legacy path; generation N>0 is the versioned path,
/// consistent with how `.vif`/`.ecx` are versioned by the 2PC switch.
pub fn bitrot_sidecar_path(base: &str, generation: u32) -> String {
    if generation == 0 {
        format!("{}{}", base, BITROT_SIDECAR_EXT)
    } else {
        format!("{}{}.v{}", base, BITROT_SIDECAR_EXT, generation)
    }
}

/// Returns a fresh random per-encode identity used to detect a stale sidecar
/// left behind by an in-place re-encode.
pub fn new_encode_uuid() -> Vec<u8> {
    use rand::RngCore;
    let mut b = vec![0u8; 16];
    rand::thread_rng().fill_bytes(&mut b);
    b
}

/// Reports whether `block_size` is a power of two in [1 MiB, MAX_BITROT_BLOCK_SIZE].
pub fn is_pow2_multiple_of_1mib(block_size: u32) -> bool {
    block_size >= (1 << 20) && block_size <= MAX_BITROT_BLOCK_SIZE && block_size.count_ones() == 1
}

/// Returns ceil(covered_size / block_size).
fn expected_block_count(covered_size: i64, block_size: i64) -> usize {
    if block_size <= 0 {
        return 0;
    }
    ((covered_size + block_size - 1) / block_size) as usize
}

/// Packs a slice of u32 into little-endian bytes.
fn pack_u32_le(vals: &[u32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(vals.len() * 4);
    for v in vals {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

/// Unpacks little-endian bytes into a Vec<u32>. Trailing bytes (len % 4) are
/// ignored, matching the Go implementation's `len(b)/4` truncation.
fn unpack_u32_le(b: &[u8]) -> Vec<u32> {
    let n = b.len() / 4;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(u32::from_le_bytes([
            b[i * 4],
            b[i * 4 + 1],
            b[i * 4 + 2],
            b[i * 4 + 3],
        ]));
    }
    out
}

/// Accumulates the per-block CRC32C of a single shard's byte stream as it is
/// written. Tolerates arbitrary chunk sizes that cross block boundaries, so it
/// works for both small encode buffers and the larger rebuild buffers.
pub struct ShardChecksumBuilder {
    block_size: i64,
    cur: CRC,
    cur_len: i64,
    total: i64,
    blocks: Vec<u32>,
}

impl ShardChecksumBuilder {
    /// Creates a builder over a given block size in bytes.
    pub fn new(block_size: i64) -> Self {
        ShardChecksumBuilder {
            block_size,
            cur: CRC(0),
            cur_len: 0,
            total: 0,
            blocks: Vec::new(),
        }
    }

    /// Feeds a chunk of arbitrary size, splitting it across block boundaries.
    pub fn write(&mut self, mut p: &[u8]) {
        while !p.is_empty() {
            let room = self.block_size - self.cur_len;
            let n = (p.len() as i64).min(room) as usize;
            self.cur = self.cur.update(&p[..n]);
            self.cur_len += n as i64;
            self.total += n as i64;
            p = &p[n..];
            if self.cur_len == self.block_size {
                self.blocks.push(self.cur.0);
                self.cur = CRC(0);
                self.cur_len = 0;
            }
        }
    }

    /// Flushes any partial last block and returns the covered size and the
    /// packed little-endian u32 CRC array.
    pub fn finalize(mut self) -> (i64, Vec<u8>) {
        if self.cur_len > 0 {
            self.blocks.push(self.cur.0);
            self.cur = CRC(0);
            self.cur_len = 0;
        }
        (self.total, pack_u32_le(&self.blocks))
    }
}

/// Atomically writes `prot` to `path`, wrapped in the on-disk header with a
/// CRC32C over the serialized payload (temp file + rename).
pub fn save_bitrot_sidecar(path: &str, prot: &EcBitrotProtection) -> io::Result<()> {
    let payload = prot.encode_to_vec();
    // The header records payload_len as a uint32 and the allocation below adds it
    // to a constant. Bound the payload well under any overflow (a real manifest is
    // a few KB) so neither the length field nor the buffer can wrap. Mirrors Go's
    // SaveBitrotSidecar maxBitrotPayloadSize check.
    const MAX_BITROT_PAYLOAD_SIZE: usize = 1 << 30; // 1 GiB, vastly above any real sidecar
    if payload.len() > MAX_BITROT_PAYLOAD_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bitrot sidecar payload too large: {} bytes", payload.len()),
        ));
    }
    let mut buf = Vec::with_capacity(BITROT_HEADER_SIZE + payload.len());
    buf.extend_from_slice(&BITROT_MAGIC.to_be_bytes());
    buf.extend_from_slice(&BITROT_FORMAT_VERSION.to_be_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&CRC::new(&payload).0.to_be_bytes());
    buf.extend_from_slice(&payload);

    let tmp = format!("{}.tmp", path);
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?;
    }
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(())
}

/// Reads and self-integrity-checks a sidecar file. Returns the parsed message,
/// or an error if the file is missing, truncated, has a bad magic/version, or
/// fails the payload CRC. A self-integrity failure is a sidecar-integrity
/// problem (caller maps it to [`BitrotStatus::Invalid`]), never a shard
/// corruption signal.
pub fn load_bitrot_sidecar(path: &str) -> Result<EcBitrotProtection, BitrotLoadError> {
    let mut data = Vec::new();
    File::open(path)?.read_to_end(&mut data)?;
    if data.len() < BITROT_HEADER_SIZE {
        return Err(BitrotLoadError::TooShort(data.len()));
    }
    let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    if magic != BITROT_MAGIC {
        return Err(BitrotLoadError::BadMagic(magic));
    }
    let ver = u16::from_be_bytes([data[4], data[5]]);
    if ver != BITROT_FORMAT_VERSION {
        return Err(BitrotLoadError::UnsupportedVersion(ver));
    }
    let payload_len = u32::from_be_bytes([data[6], data[7], data[8], data[9]]);
    let want_crc = u32::from_be_bytes([data[10], data[11], data[12], data[13]]);
    let payload = &data[BITROT_HEADER_SIZE..];
    if payload_len as usize != payload.len() {
        return Err(BitrotLoadError::LengthMismatch {
            header: payload_len,
            actual: payload.len(),
        });
    }
    let got = CRC::new(payload).0;
    if got != want_crc {
        return Err(BitrotLoadError::CrcMismatch {
            header: want_crc,
            computed: got,
        });
    }
    EcBitrotProtection::decode(payload).map_err(BitrotLoadError::Decode)
}

/// Performs the disk-free manifest/syntax checks that every loader runs:
/// supported algorithm, valid block size, exactly one entry per shard id in the
/// active layout (no duplicates, no out-of-range ids), positive covered_size,
/// and a packed-CRC count consistent with covered_size.
///
/// It does NOT compare covered_size against on-disk shard lengths — that is a
/// per-node physical check done only for locally-held shards.
pub fn validate_manifest(
    prot: &EcBitrotProtection,
    data_shards: usize,
    parity_shards: usize,
) -> Result<(), String> {
    if prot.algorithm != ChecksumAlgorithm::ChecksumCrc32c as i32 {
        return Err(format!("unsupported checksum algorithm {}", prot.algorithm));
    }
    if !is_pow2_multiple_of_1mib(prot.block_size) {
        return Err(format!(
            "invalid block_size {} (must be a power-of-two multiple of 1 MiB, at most {})",
            prot.block_size, MAX_BITROT_BLOCK_SIZE
        ));
    }
    let bs = prot.block_size as i64;
    let total = data_shards + parity_shards;
    if total == 0 || total > MAX_SHARD_COUNT {
        return Err(format!(
            "invalid active layout: data={} parity={}",
            data_shards, parity_shards
        ));
    }
    if prot.shards.len() != total {
        return Err(format!(
            "incomplete manifest: {} shard entries, expected {}",
            prot.shards.len(),
            total
        ));
    }
    let mut seen = vec![false; MAX_SHARD_COUNT];
    for s in &prot.shards {
        if s.shard_id >= total as u32 {
            return Err(format!(
                "shard id {} out of range [0,{})",
                s.shard_id, total
            ));
        }
        if seen[s.shard_id as usize] {
            return Err(format!("duplicate shard id {}", s.shard_id));
        }
        seen[s.shard_id as usize] = true;
        if s.covered_size <= 0 {
            return Err(format!(
                "shard {} has non-positive covered_size {}",
                s.shard_id, s.covered_size
            ));
        }
        let want_count = expected_block_count(s.covered_size, bs);
        if s.block_crc32c.len() != want_count * 4 {
            return Err(format!(
                "shard {} crc count mismatch: {} bytes, expected {} (covered_size={} block_size={})",
                s.shard_id,
                s.block_crc32c.len(),
                want_count * 4,
                s.covered_size,
                prot.block_size
            ));
        }
    }
    Ok(())
}

/// Resolves the protection status of a loaded-or-missing sidecar against the
/// active generation and layout. `loaded` is the result of attempting to load
/// the sidecar at the active generation's path.
///
/// - missing sidecar (NotFound) => [`BitrotStatus::Off`]
/// - load/self-integrity failure => [`BitrotStatus::Invalid`]
/// - generation mismatch => [`BitrotStatus::Off`]
/// - manifest validation failure => [`BitrotStatus::Invalid`]
/// - otherwise => [`BitrotStatus::On`]
pub fn resolve_status(
    loaded: &Result<EcBitrotProtection, BitrotLoadError>,
    active_generation: u32,
    data_shards: usize,
    parity_shards: usize,
) -> BitrotStatus {
    match loaded {
        Err(BitrotLoadError::Io(e)) if e.kind() == io::ErrorKind::NotFound => BitrotStatus::Off,
        Err(_) => BitrotStatus::Invalid,
        Ok(prot) => {
            if prot.generation != active_generation {
                return BitrotStatus::Off;
            }
            if validate_manifest(prot, data_shards, parity_shards).is_err() {
                return BitrotStatus::Invalid;
            }
            BitrotStatus::On
        }
    }
}

/// Returns the [`EcShardChecksums`] entry for a shard id, or `None`.
pub fn shard_checksums(prot: &EcBitrotProtection, shard_id: u32) -> Option<&EcShardChecksums> {
    prot.shards.iter().find(|s| s.shard_id == shard_id)
}

/// Reads a shard file at `path` in `block_size` chunks and compares each block's
/// CRC32C against the manifest entry. Returns the list of mismatching block
/// indices (empty == clean), or a fatal `io::Error` for genuine I/O problems.
///
/// A length mismatch (truncation or unexpected trailing bytes) is itself shard
/// corruption: every block index is reported as mismatched so the caller treats
/// the shard as bad. It does not interpret the result — the caller (scrub /
/// rebuild) arbitrates shard-vs-sidecar via Reed-Solomon before acting.
pub fn verify_shard_file_blocks(
    path: &str,
    entry: &EcShardChecksums,
    block_size: i64,
) -> io::Result<Vec<usize>> {
    let f = File::open(path)?;
    let file_size = f.metadata()?.len() as i64;
    let want = unpack_u32_le(&entry.block_crc32c);

    if file_size != entry.covered_size {
        // Length drift is shard corruption: report every block as mismatched.
        return Ok((0..want.len()).collect());
    }

    let mut mismatched = Vec::new();
    let mut buf = vec![0u8; block_size.max(1) as usize];
    let mut offset: i64 = 0;
    for (i, want_crc) in want.iter().enumerate() {
        let to_read = (entry.covered_size - offset).min(block_size);
        if to_read <= 0 {
            break;
        }
        let to_read = to_read as usize;
        read_full_at(&f, &mut buf[..to_read], offset as u64)?;
        if CRC::new(&buf[..to_read]).0 != *want_crc {
            mismatched.push(i);
        }
        offset += to_read as i64;
    }
    Ok(mismatched)
}

/// Reads exactly `buf.len()` bytes from `f` at `offset`, erroring on early EOF.
fn read_full_at(f: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    let mut total = 0usize;
    while total < buf.len() {
        #[cfg(unix)]
        let n = {
            use std::os::unix::fs::FileExt;
            f.read_at(&mut buf[total..], offset + total as u64)?
        };
        #[cfg(not(unix))]
        let n = {
            use std::io::{Read, Seek, SeekFrom};
            let mut fc = f.try_clone()?;
            fc.seek(SeekFrom::Start(offset + total as u64))?;
            fc.read(&mut buf[total..])?
        };
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short read on shard block",
            ));
        }
        total += n;
    }
    Ok(())
}

/// Builds the `EcShardConfig` proto for the given layout. The bitrot sidecar
/// carries its own top-level encode_uuid, so the nested config leaves it empty.
pub fn ec_shard_config(data_shards: u32, parity_shards: u32) -> EcShardConfig {
    EcShardConfig {
        data_shards,
        parity_shards,
        encode_ts_ns: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cross-binary byte-exact proof: CANONICAL_HEX equals `canonicalInteropHex`
    /// in weed/storage/erasure_coding/ec_bitrot_interop_test.go (the Go reference
    /// asserts the same constant). Both binaries port the same format, so a
    /// sidecar written by either must be byte-identical; this pins the Rust side
    /// of that guarantee. If you change the format, regenerate and update BOTH.
    #[test]
    fn test_byte_exact_go_interop() {
        const CANONICAL_HEX: &str = "45435355000100000039cc1b826a080110808080082204080a10042a0a108080401a04040302012a0c0801108080401a04080706053210000102030405060708090a0b0c0d0e0f";
        let prot = EcBitrotProtection {
            algorithm: ChecksumAlgorithm::ChecksumCrc32c as i32,
            block_size: DEFAULT_BITROT_BLOCK_SIZE as u32,
            generation: 0,
            ec_shard_config: Some(EcShardConfig {
                data_shards: 10,
                parity_shards: 4,
                encode_ts_ns: 0,
            }),
            shards: vec![
                EcShardChecksums {
                    shard_id: 0,
                    covered_size: 1024 * 1024,
                    block_crc32c: pack_u32_le(&[0x0102_0304]),
                },
                EcShardChecksums {
                    shard_id: 1,
                    covered_size: 1024 * 1024,
                    block_crc32c: pack_u32_le(&[0x0506_0708]),
                },
            ],
            encode_uuid: (0..16u8).collect(),
        };
        let dir = std::env::temp_dir().join(format!("ecsum_interop_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("v1.ecsum");
        let path = path.to_str().unwrap();
        save_bitrot_sidecar(path, &prot).unwrap();
        let bytes = std::fs::read(path).unwrap();
        let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
        assert_eq!(hex, CANONICAL_HEX, "Rust .ecsum bytes drifted from the Go canonical form");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_sidecar_path_generations() {
        assert_eq!(bitrot_sidecar_path("/d/1", 0), "/d/1.ecsum");
        assert_eq!(bitrot_sidecar_path("/d/1", 3), "/d/1.ecsum.v3");
    }

    #[test]
    fn test_is_pow2_multiple_of_1mib() {
        assert!(is_pow2_multiple_of_1mib(1 << 20)); // 1 MiB
        assert!(is_pow2_multiple_of_1mib(16 * 1024 * 1024)); // 16 MiB default
        assert!(is_pow2_multiple_of_1mib(1 << 25));
        assert!(is_pow2_multiple_of_1mib(MAX_BITROT_BLOCK_SIZE)); // 64 MiB boundary
        assert!(!is_pow2_multiple_of_1mib(0));
        assert!(!is_pow2_multiple_of_1mib(1 << 19)); // 512 KiB, too small
        assert!(!is_pow2_multiple_of_1mib(3 << 20)); // 3 MiB, not pow2
        assert!(!is_pow2_multiple_of_1mib(128 * 1024 * 1024)); // pow2 but > MAX_BITROT_BLOCK_SIZE
        assert!(!is_pow2_multiple_of_1mib(DEFAULT_BITROT_BLOCK_SIZE as u32 + 1));
    }

    #[test]
    fn test_expected_block_count() {
        assert_eq!(expected_block_count(0, 16), 0);
        assert_eq!(expected_block_count(1, 16), 1);
        assert_eq!(expected_block_count(16, 16), 1);
        assert_eq!(expected_block_count(17, 16), 2);
        assert_eq!(expected_block_count(32, 16), 2);
        assert_eq!(expected_block_count(100, 0), 0);
    }

    #[test]
    fn test_pack_unpack_roundtrip() {
        let vals = vec![0x0102_0304u32, 0xdead_beef, 0, u32::MAX];
        let packed = pack_u32_le(&vals);
        assert_eq!(packed.len(), 16);
        // Verify little-endian byte order of the first entry.
        assert_eq!(&packed[0..4], &[0x04, 0x03, 0x02, 0x01]);
        assert_eq!(unpack_u32_le(&packed), vals);
    }

    #[test]
    fn test_builder_block_boundaries() {
        // block_size = 4; feed 10 bytes in chunks that cross boundaries.
        let mut b = ShardChecksumBuilder::new(4);
        let data = b"0123456789";
        b.write(&data[0..3]); // partial block 0
        b.write(&data[3..7]); // completes block 0 (idx 3), fills block 1
        b.write(&data[7..10]); // partial block 2
        let (covered, packed) = b.finalize();
        assert_eq!(covered, 10);
        let crcs = unpack_u32_le(&packed);
        // ceil(10/4) = 3 blocks
        assert_eq!(crcs.len(), 3);
        // Compare against direct per-block CRCs.
        assert_eq!(crcs[0], CRC::new(&data[0..4]).0);
        assert_eq!(crcs[1], CRC::new(&data[4..8]).0);
        assert_eq!(crcs[2], CRC::new(&data[8..10]).0);
    }

    #[test]
    fn test_builder_exact_block_multiple() {
        let mut b = ShardChecksumBuilder::new(4);
        b.write(b"01234567"); // exactly 2 blocks, no partial
        let (covered, packed) = b.finalize();
        assert_eq!(covered, 8);
        assert_eq!(unpack_u32_le(&packed).len(), 2);
    }

    #[test]
    fn test_save_load_roundtrip() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp
            .path()
            .join("vol.ecsum")
            .to_str()
            .unwrap()
            .to_string();

        let mut builder = ShardChecksumBuilder::new(DEFAULT_BITROT_BLOCK_SIZE as i64);
        builder.write(b"hello world");
        let (covered, packed) = builder.finalize();

        let prot = EcBitrotProtection {
            algorithm: ChecksumAlgorithm::ChecksumCrc32c as i32,
            block_size: DEFAULT_BITROT_BLOCK_SIZE as u32,
            generation: 0,
            ec_shard_config: Some(ec_shard_config(10, 4)),
            shards: vec![EcShardChecksums {
                shard_id: 0,
                covered_size: covered,
                block_crc32c: packed,
            }],
            encode_uuid: new_encode_uuid(),
        };

        save_bitrot_sidecar(&path, &prot).unwrap();
        let loaded = load_bitrot_sidecar(&path).unwrap();
        assert_eq!(loaded, prot);
    }

    #[test]
    fn test_load_rejects_bad_magic() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("bad.ecsum").to_str().unwrap().to_string();
        std::fs::write(&path, vec![0u8; BITROT_HEADER_SIZE + 4]).unwrap();
        match load_bitrot_sidecar(&path) {
            Err(BitrotLoadError::BadMagic(_)) => {}
            other => panic!("expected BadMagic, got {:?}", other),
        }
    }

    #[test]
    fn test_load_rejects_corrupted_payload() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp
            .path()
            .join("corrupt.ecsum")
            .to_str()
            .unwrap()
            .to_string();
        let prot = EcBitrotProtection {
            algorithm: ChecksumAlgorithm::ChecksumCrc32c as i32,
            block_size: DEFAULT_BITROT_BLOCK_SIZE as u32,
            generation: 0,
            ec_shard_config: Some(ec_shard_config(10, 4)),
            shards: vec![EcShardChecksums {
                shard_id: 0,
                covered_size: 5,
                block_crc32c: pack_u32_le(&[CRC::new(b"hello").0]),
            }],
            encode_uuid: vec![0u8; 16],
        };
        save_bitrot_sidecar(&path, &prot).unwrap();

        // Flip a byte in the payload (after the 14-byte header).
        let mut data = std::fs::read(&path).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xff;
        std::fs::write(&path, &data).unwrap();

        match load_bitrot_sidecar(&path) {
            Err(BitrotLoadError::CrcMismatch { .. }) => {}
            other => panic!("expected CrcMismatch, got {:?}", other),
        }
    }

    #[test]
    fn test_load_missing_is_notfound() {
        let res = load_bitrot_sidecar("/nonexistent/path/x.ecsum");
        match res {
            Err(BitrotLoadError::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::NotFound),
            other => panic!("expected Io(NotFound), got {:?}", other),
        }
    }

    fn good_manifest() -> EcBitrotProtection {
        let mut shards = Vec::new();
        for id in 0..14u32 {
            shards.push(EcShardChecksums {
                shard_id: id,
                covered_size: 5,
                block_crc32c: pack_u32_le(&[CRC::new(b"hello").0]),
            });
        }
        EcBitrotProtection {
            algorithm: ChecksumAlgorithm::ChecksumCrc32c as i32,
            block_size: DEFAULT_BITROT_BLOCK_SIZE as u32,
            generation: 0,
            ec_shard_config: Some(ec_shard_config(10, 4)),
            shards,
            encode_uuid: vec![0u8; 16],
        }
    }

    #[test]
    fn test_validate_manifest_ok() {
        assert!(validate_manifest(&good_manifest(), 10, 4).is_ok());
    }

    #[test]
    fn test_validate_rejects_wrong_algorithm() {
        let mut m = good_manifest();
        m.algorithm = ChecksumAlgorithm::ChecksumNone as i32;
        assert!(validate_manifest(&m, 10, 4).is_err());
    }

    #[test]
    fn test_validate_rejects_bad_block_size() {
        let mut m = good_manifest();
        m.block_size = 3 << 20;
        assert!(validate_manifest(&m, 10, 4).is_err());
    }

    #[test]
    fn test_validate_rejects_incomplete() {
        let mut m = good_manifest();
        m.shards.pop();
        assert!(validate_manifest(&m, 10, 4).is_err());
    }

    #[test]
    fn test_validate_rejects_duplicate_shard_id() {
        let mut m = good_manifest();
        m.shards[1].shard_id = 0;
        assert!(validate_manifest(&m, 10, 4).is_err());
    }

    #[test]
    fn test_validate_rejects_out_of_range_id() {
        let mut m = good_manifest();
        m.shards[13].shard_id = 14;
        assert!(validate_manifest(&m, 10, 4).is_err());
    }

    #[test]
    fn test_validate_rejects_nonpositive_covered_size() {
        let mut m = good_manifest();
        m.shards[0].covered_size = 0;
        assert!(validate_manifest(&m, 10, 4).is_err());
    }

    #[test]
    fn test_validate_rejects_crc_count_mismatch() {
        let mut m = good_manifest();
        m.shards[0].block_crc32c = vec![0u8; 8]; // 2 entries but covered_size=5 => want 1
        assert!(validate_manifest(&m, 10, 4).is_err());
    }

    #[test]
    fn test_resolve_status() {
        // Missing => Off.
        let notfound: Result<EcBitrotProtection, BitrotLoadError> = Err(BitrotLoadError::Io(
            io::Error::new(io::ErrorKind::NotFound, "x"),
        ));
        assert_eq!(resolve_status(&notfound, 0, 10, 4), BitrotStatus::Off);

        // Integrity failure => Invalid.
        let bad: Result<EcBitrotProtection, BitrotLoadError> =
            Err(BitrotLoadError::BadMagic(0));
        assert_eq!(resolve_status(&bad, 0, 10, 4), BitrotStatus::Invalid);

        // Generation mismatch => Off.
        let mut m = good_manifest();
        m.generation = 7;
        let ok: Result<EcBitrotProtection, BitrotLoadError> = Ok(m);
        assert_eq!(resolve_status(&ok, 0, 10, 4), BitrotStatus::Off);

        // Matching + valid => On.
        let ok2: Result<EcBitrotProtection, BitrotLoadError> = Ok(good_manifest());
        assert_eq!(resolve_status(&ok2, 0, 10, 4), BitrotStatus::On);

        // Matching generation but invalid manifest => Invalid.
        let mut bad_m = good_manifest();
        bad_m.shards.pop();
        let ok3: Result<EcBitrotProtection, BitrotLoadError> = Ok(bad_m);
        assert_eq!(resolve_status(&ok3, 0, 10, 4), BitrotStatus::Invalid);
    }

    #[test]
    fn test_verify_shard_file_blocks() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("s.ec00").to_str().unwrap().to_string();
        let block_size: i64 = 4;
        let data = b"0123456789"; // 10 bytes, 3 blocks
        std::fs::write(&path, data).unwrap();

        let mut b = ShardChecksumBuilder::new(block_size);
        b.write(data);
        let (covered, packed) = b.finalize();
        let entry = EcShardChecksums {
            shard_id: 0,
            covered_size: covered,
            block_crc32c: packed,
        };

        // Clean file => no mismatches.
        let mm = verify_shard_file_blocks(&path, &entry, block_size).unwrap();
        assert!(mm.is_empty());

        // Corrupt block index 1 (bytes 4..8).
        let mut corrupt = data.to_vec();
        corrupt[5] ^= 0xff;
        std::fs::write(&path, &corrupt).unwrap();
        let mm = verify_shard_file_blocks(&path, &entry, block_size).unwrap();
        assert_eq!(mm, vec![1]);

        // Truncation => all blocks mismatched.
        std::fs::write(&path, b"012").unwrap();
        let mm = verify_shard_file_blocks(&path, &entry, block_size).unwrap();
        assert_eq!(mm, vec![0, 1, 2]);
    }
}
