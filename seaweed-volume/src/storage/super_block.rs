//! SuperBlock: the 8-byte (+ optional extra) header at the start of every .dat file.
//!
//! Byte layout:
//!   [0]     Version
//!   [1]     ReplicaPlacement byte
//!   [2..4]  TTL (2 bytes)
//!   [4..6]  CompactionRevision (u16 big-endian)
//!   [6..8]  ExtraSize (u16 big-endian)
//!   [8..]   Extra data (protobuf, ExtraSize bytes) — only for Version 2/3

use crate::storage::needle::ttl::TTL;
use crate::storage::types::Version;

pub const SUPER_BLOCK_SIZE: usize = 8;

/// SuperBlock metadata at the start of a volume .dat file.
#[derive(Debug, Clone)]
pub struct SuperBlock {
    pub version: Version,
    pub replica_placement: ReplicaPlacement,
    pub ttl: TTL,
    pub compaction_revision: u16,
    pub extra_size: u16,
    pub extra_data: Vec<u8>, // raw protobuf bytes (SuperBlockExtra)
}

impl SuperBlock {
    /// Total block size on disk (base 8 + extra).
    pub fn block_size(&self) -> usize {
        match self.version.0 {
            2 | 3 => SUPER_BLOCK_SIZE + self.extra_size as usize,
            _ => SUPER_BLOCK_SIZE,
        }
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut header = vec![0u8; SUPER_BLOCK_SIZE];
        header[0] = self.version.0;
        header[1] = self.replica_placement.to_byte();
        self.ttl.to_bytes(&mut header[2..4]);
        header[4..6].copy_from_slice(&self.compaction_revision.to_be_bytes());

        if !self.extra_data.is_empty() {
            let extra_size = self.extra_data.len() as u16;
            header[6..8].copy_from_slice(&extra_size.to_be_bytes());
            header.extend_from_slice(&self.extra_data);
        }

        header
    }

    /// Parse from bytes (must be at least SUPER_BLOCK_SIZE bytes).
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SuperBlockError> {
        if bytes.len() < SUPER_BLOCK_SIZE {
            return Err(SuperBlockError::TooShort(bytes.len()));
        }

        let version = Version(bytes[0]);
        let replica_placement = ReplicaPlacement::from_byte(bytes[1])?;
        let ttl = TTL::from_bytes(&bytes[2..4]);
        let compaction_revision = u16::from_be_bytes([bytes[4], bytes[5]]);
        let extra_size = u16::from_be_bytes([bytes[6], bytes[7]]);

        let extra_data = if extra_size > 0 && bytes.len() >= SUPER_BLOCK_SIZE + extra_size as usize
        {
            bytes[SUPER_BLOCK_SIZE..SUPER_BLOCK_SIZE + extra_size as usize].to_vec()
        } else {
            vec![]
        };

        Ok(SuperBlock {
            version,
            replica_placement,
            ttl,
            compaction_revision,
            extra_size,
            extra_data,
        })
    }

    pub fn initialized(&self) -> bool {
        true // ReplicaPlacement and TTL are always valid after construction
    }
}

impl Default for SuperBlock {
    fn default() -> Self {
        SuperBlock {
            version: Version::current(),
            replica_placement: ReplicaPlacement::default(),
            ttl: TTL::EMPTY,
            compaction_revision: 0,
            extra_size: 0,
            extra_data: vec![],
        }
    }
}

// ============================================================================
// ReplicaPlacement
// ============================================================================

/// Replication strategy encoded as a single byte.
///
/// Byte value = DiffDataCenterCount * 100 + DiffRackCount * 10 + SameRackCount
///
/// Examples:
///   "000" → no replication (1 copy total)
///   "010" → 1 copy in different rack (2 copies total)
///   "100" → 1 copy in different datacenter
///   "200" → 2 copies in different datacenters
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReplicaPlacement {
    pub same_rack_count: u8,
    pub diff_rack_count: u8,
    pub diff_data_center_count: u8,
}

impl ReplicaPlacement {
    /// Parse from a string like "000", "010", "100".
    /// Accepts 0-3 character strings, padding with leading zeros to match Go behavior.
    /// E.g. "" -> "000", "1" -> "001", "01" -> "001", "010" -> "010"
    pub fn from_string(s: &str) -> Result<Self, SuperBlockError> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(ReplicaPlacement::default());
        }
        // Pad with leading zeros to 3 chars, matching Go's NewReplicaPlacementFromString
        let padded = match s.len() {
            1 => format!("00{}", s),
            2 => format!("0{}", s),
            3 => s.to_string(),
            _ => return Err(SuperBlockError::InvalidReplicaPlacement(s.to_string())),
        };
        let chars: Vec<char> = padded.chars().collect();
        let dc = chars[0]
            .to_digit(10)
            .ok_or_else(|| SuperBlockError::InvalidReplicaPlacement(s.to_string()))?
            as u8;
        let rack = chars[1]
            .to_digit(10)
            .ok_or_else(|| SuperBlockError::InvalidReplicaPlacement(s.to_string()))?
            as u8;
        let same = chars[2]
            .to_digit(10)
            .ok_or_else(|| SuperBlockError::InvalidReplicaPlacement(s.to_string()))?
            as u8;
        // Go validates: value = dc*100 + rack*10 + same must fit in a byte
        let value = dc as u16 * 100 + rack as u16 * 10 + same as u16;
        if value > 255 {
            return Err(SuperBlockError::InvalidReplicaPlacement(s.to_string()));
        }
        Ok(ReplicaPlacement {
            diff_data_center_count: dc,
            diff_rack_count: rack,
            same_rack_count: same,
        })
    }

    /// Parse from a single byte.
    pub fn from_byte(b: u8) -> Result<Self, SuperBlockError> {
        Ok(ReplicaPlacement {
            diff_data_center_count: b / 100,
            diff_rack_count: (b % 100) / 10,
            same_rack_count: b % 10,
        })
    }

    /// Encode as a single byte.
    pub fn to_byte(&self) -> u8 {
        self.diff_data_center_count * 100 + self.diff_rack_count * 10 + self.same_rack_count
    }

    /// Total number of copies (including the original).
    pub fn get_copy_count(&self) -> u8 {
        self.diff_data_center_count + self.diff_rack_count + self.same_rack_count + 1
    }

    /// Whether this placement requires replication (more than 1 copy).
    pub fn has_replication(&self) -> bool {
        self.get_copy_count() > 1
    }
}

impl std::fmt::Display for ReplicaPlacement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}{}",
            self.diff_data_center_count, self.diff_rack_count, self.same_rack_count
        )
    }
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum SuperBlockError {
    #[error("super block too short: {0} bytes")]
    TooShort(usize),

    #[error("invalid replica placement: {0}")]
    InvalidReplicaPlacement(String),
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::types::*;

    #[test]
    fn test_super_block_round_trip() {
        let sb = SuperBlock {
            version: VERSION_3,
            replica_placement: ReplicaPlacement::from_string("010").unwrap(),
            ttl: TTL { count: 5, unit: 3 },
            compaction_revision: 42,
            extra_size: 0,
            extra_data: vec![],
        };

        let bytes = sb.to_bytes();
        assert_eq!(bytes.len(), SUPER_BLOCK_SIZE);

        let sb2 = SuperBlock::from_bytes(&bytes).unwrap();
        assert_eq!(sb2.version, sb.version);
        assert_eq!(sb2.replica_placement, sb.replica_placement);
        assert_eq!(sb2.ttl, sb.ttl);
        assert_eq!(sb2.compaction_revision, sb.compaction_revision);
    }

    #[test]
    fn test_super_block_with_extra() {
        let sb = SuperBlock {
            version: VERSION_3,
            replica_placement: ReplicaPlacement::default(),
            ttl: TTL::EMPTY,
            compaction_revision: 0,
            extra_size: 3,
            extra_data: vec![1, 2, 3],
        };

        let bytes = sb.to_bytes();
        assert_eq!(bytes.len(), SUPER_BLOCK_SIZE + 3);

        let sb2 = SuperBlock::from_bytes(&bytes).unwrap();
        assert_eq!(sb2.extra_data, vec![1, 2, 3]);
    }

    #[test]
    fn test_replica_placement_byte_round_trip() {
        let rp = ReplicaPlacement::from_string("123").unwrap();
        assert_eq!(rp.diff_data_center_count, 1);
        assert_eq!(rp.diff_rack_count, 2);
        assert_eq!(rp.same_rack_count, 3);
        assert_eq!(rp.to_byte(), 123);
        assert_eq!(rp.get_copy_count(), 7); // 1+2+3+1

        let rp2 = ReplicaPlacement::from_byte(123).unwrap();
        assert_eq!(rp, rp2);
    }

    #[test]
    fn test_replica_placement_no_replication() {
        let rp = ReplicaPlacement::from_string("000").unwrap();
        assert!(!rp.has_replication());
        assert_eq!(rp.get_copy_count(), 1);
    }

    #[test]
    fn test_replica_placement_display() {
        let rp = ReplicaPlacement::from_string("010").unwrap();
        assert_eq!(rp.to_string(), "010");
        assert!(rp.has_replication());
    }
}
