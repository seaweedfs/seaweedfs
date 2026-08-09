//! Mirror of `weed/storage/volume_report_hash.go`.
//!
//! The master compares the digest a volume server reports against one it
//! computes itself, so this has to agree with the Go implementation
//! byte-for-byte. `report_hash_vectors` pins that against values produced by
//! the Go side; do not change the layout without regenerating them there.

use xxhash_rust::xxh64::xxh64;

use crate::pb::master_pb;

/// Digests everything a volume server reports about a volume.
///
/// It must cover every field of `VolumeInformationMessage`: a change the hash
/// misses is a change the master would never be told about.
pub fn report_hash(m: &master_pb::VolumeInformationMessage) -> u64 {
    let mut buf = [0u8; 57];
    buf[0..4].copy_from_slice(&m.id.to_le_bytes());
    buf[4..12].copy_from_slice(&m.size.to_le_bytes());
    buf[12..20].copy_from_slice(&m.file_count.to_le_bytes());
    buf[20..28].copy_from_slice(&m.delete_count.to_le_bytes());
    buf[28..36].copy_from_slice(&m.deleted_byte_count.to_le_bytes());
    // The master stores these narrowed, so hash what it will hold, not what the
    // wire type could carry.
    buf[36..40].copy_from_slice(&((m.replica_placement as u8) as u32).to_le_bytes());
    buf[40..44].copy_from_slice(&((m.version as u8) as u32).to_le_bytes());
    buf[44..48].copy_from_slice(&normalize_ttl(m.ttl).to_le_bytes());
    buf[48..52].copy_from_slice(&m.compact_revision.to_le_bytes());
    buf[52..56].copy_from_slice(&m.disk_id.to_le_bytes());
    if m.read_only {
        buf[56] = 1;
    }
    let mut h = xxh64(&buf, 0);

    h = fold(h, xxh64(&(m.modified_at_second as u64).to_le_bytes(), 0));
    h = fold(h, xxh64(m.collection.as_bytes(), 0));
    h = fold(h, xxh64(m.disk_type.as_bytes(), 0));
    // Not the remote storage key: the master does not keep it, so a change to
    // it alters nothing its copy holds.
    h = fold(h, xxh64(m.remote_storage_name.as_bytes(), 0));
    h
}

/// A ttl whose count is zero encodes as zero however the unit is set, matching
/// what the master stores after decoding it.
fn normalize_ttl(ttl: u32) -> u32 {
    let count = (ttl >> 8) & 0xff;
    if count == 0 {
        return 0;
    }
    (count << 8) | (ttl & 0xff)
}

/// Combines two hashes order-dependently, so swapping two string fields is not
/// invisible.
fn fold(h: u64, x: u64) -> u64 {
    let h = (h ^ x).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^ (h >> 29)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Produced by the Go implementation. If these drift, every volume server
    // running this build reports a digest the master can never match, and falls
    // back to sending its whole volume list forever.
    #[test]
    fn report_hash_vectors() {
        let empty = master_pb::VolumeInformationMessage::default();
        assert_eq!(report_hash(&empty), 10988706248825469653);

        let mut one = master_pb::VolumeInformationMessage::default();
        one.id = 1;
        assert_eq!(report_hash(&one), 2035849960016744285);

        let full = master_pb::VolumeInformationMessage {
            id: 42,
            size: 1 << 30,
            collection: "c".to_string(),
            file_count: 7,
            delete_count: 2,
            deleted_byte_count: 99,
            read_only: true,
            replica_placement: 10,
            version: 3,
            ttl: 3 << 8,
            compact_revision: 5,
            modified_at_second: 1700000000,
            remote_storage_name: "s3".to_string(),
            remote_storage_key: "k/1.dat".to_string(),
            disk_type: "ssd".to_string(),
            disk_id: 2,
        };
        assert_eq!(report_hash(&full), 2748844479819636032);
    }

    #[test]
    fn ttl_with_no_count_is_dropped() {
        assert_eq!(normalize_ttl(0), 0);
        assert_eq!(normalize_ttl(3), 0);
        assert_eq!(normalize_ttl(3 << 8), 3 << 8);
        assert_eq!(normalize_ttl((3 << 8) | 4), (3 << 8) | 4);
    }
}
