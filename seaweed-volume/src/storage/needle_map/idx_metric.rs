//! Rebuild a needle map's counters from the whole `.idx` history.
//!
//! The volume's garbage ratio is `deleted_size / content_size`, and both are
//! additive over the life of the volume: an overwritten needle keeps its
//! bytes in `content_size` and adds them to `deleted_size`. A backend whose
//! table only holds the final value per key (redb) cannot recover that from
//! the table, so on load the counters come from the `.idx` file instead.
//!
//! This mirrors Go's `needleMapMetricFromIndexFile`: walk the index newest
//! entry first with a bloom filter of the keys already seen, so the memory
//! cost is a few bits per entry instead of a map of every key. The counting
//! rule reproduces what the live `on_put`/`on_delete` path accumulates:
//!
//! - every live entry is one put: `file_count`, `file_byte_count`;
//! - a live entry with a newer entry for the same key was overwritten or
//!   deleted later, so it is also one deletion: `deletion_count`,
//!   `deletion_byte_count`;
//! - a tombstone only marks its key as seen.
//!
//! A bloom false positive (0.1%) can only add a spurious deletion, which
//! over-reports garbage slightly; it never hides any.

use std::io::{self, Read, Seek, SeekFrom};
use std::sync::atomic::Ordering;

use xxhash_rust::xxh64::xxh64;

use super::NeedleMapMetric;
use crate::storage::types::*;

/// Entries read per batch while walking backwards (64 KiB of index).
const BATCH_ENTRIES: usize = 4096;
/// Same target false-positive rate as the Go server's filter.
const FALSE_POSITIVE_RATE: f64 = 0.001;

/// Minimal bloom filter over needle ids, double hashing with xxh64.
struct SeenKeys {
    bits: Vec<u64>,
    bit_count: u64,
    hashes: u64,
}

impl SeenKeys {
    fn new(expected: u64, false_positive_rate: f64) -> Self {
        let n = expected.max(1) as f64;
        let ln2 = std::f64::consts::LN_2;
        let bit_count = (-(n * false_positive_rate.ln()) / (ln2 * ln2))
            .ceil()
            .max(64.0) as u64;
        let hashes = ((bit_count as f64 / n) * ln2).round().clamp(1.0, 16.0) as u64;
        SeenKeys {
            bits: vec![0u64; bit_count.div_ceil(64) as usize],
            bit_count,
            hashes,
        }
    }

    /// Whether `key` was (probably) seen before; marks it seen either way.
    fn test_and_add(&mut self, key: u64) -> bool {
        let bytes = key.to_le_bytes();
        let h1 = xxh64(&bytes, 0);
        let h2 = xxh64(&bytes, 0x9E37_79B9_7F4A_7C15) | 1;
        let mut seen = true;
        for i in 0..self.hashes {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % self.bit_count;
            let word = (bit / 64) as usize;
            let mask = 1u64 << (bit % 64);
            if self.bits[word] & mask == 0 {
                seen = false;
                self.bits[word] |= mask;
            }
        }
        seen
    }
}

/// Walk `reader` (an `.idx` file) newest entry first and return the counters
/// a live volume would hold after applying the same history. A torn partial
/// entry at the tail is ignored, as `walk_index_file` does. The reader is
/// left positioned at the start of the file.
pub(super) fn metrics_from_idx<R: Read + Seek>(
    reader: &mut R,
    version: Version,
) -> io::Result<NeedleMapMetric> {
    let metric = NeedleMapMetric::default();
    let file_size = reader.seek(SeekFrom::End(0))?;
    let entry_count = file_size / NEEDLE_MAP_ENTRY_SIZE as u64;
    let mut seen = SeenKeys::new(entry_count, FALSE_POSITIVE_RATE);
    let mut buf = vec![0u8; NEEDLE_MAP_ENTRY_SIZE * BATCH_ENTRIES];

    let mut remaining = entry_count;
    while remaining > 0 {
        let batch = remaining.min(BATCH_ENTRIES as u64) as usize;
        let first_entry = remaining - batch as u64;
        let len = batch * NEEDLE_MAP_ENTRY_SIZE;
        reader.seek(SeekFrom::Start(first_entry * NEEDLE_MAP_ENTRY_SIZE as u64))?;
        reader.read_exact(&mut buf[..len])?;
        for i in (0..batch).rev() {
            let entry = &buf[i * NEEDLE_MAP_ENTRY_SIZE..(i + 1) * NEEDLE_MAP_ENTRY_SIZE];
            let (key, offset, size) = idx_entry_from_bytes(entry);
            metric.maybe_set_max_file_key(key);
            metric.maybe_set_max_needle_end(offset, size, version);
            let superseded = seen.test_and_add(key.into());
            if offset.is_zero() || size.is_deleted() {
                // Tombstone: reserves no bytes, only marks the key as seen.
                continue;
            }
            metric.file_count.fetch_add(1, Ordering::Relaxed);
            metric
                .file_byte_count
                .fetch_add(size.0 as u64, Ordering::Relaxed);
            if superseded && size.0 > 0 {
                metric.deletion_count.fetch_add(1, Ordering::Relaxed);
                metric
                    .deletion_byte_count
                    .fetch_add(size.0 as u64, Ordering::Relaxed);
            }
        }
        remaining = first_entry;
    }

    reader.seek(SeekFrom::Start(0))?;
    Ok(metric)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seen_keys_reports_repeats_and_not_fresh_keys() {
        let mut seen = SeenKeys::new(10_000, FALSE_POSITIVE_RATE);
        // Fresh keys may occasionally collide (that is the false-positive
        // rate), but only rarely.
        let fresh_reported_seen = (0..10_000u64)
            .filter(|&key| seen.test_and_add(key))
            .count();
        assert!(
            fresh_reported_seen < 50,
            "fresh keys reported seen: {fresh_reported_seen}"
        );
        // A repeated key is never reported fresh: no false negatives.
        for key in 0..10_000u64 {
            assert!(seen.test_and_add(key), "repeated key {key} reported fresh");
        }
        // Over 100k never-inserted keys the false-positive rate stays near
        // the 0.1% target; allow a generous margin.
        let false_positives = (1_000_000..1_100_000u64)
            .filter(|k| {
                let bytes = k.to_le_bytes();
                let h1 = xxh64(&bytes, 0);
                let h2 = xxh64(&bytes, 0x9E37_79B9_7F4A_7C15) | 1;
                (0..seen.hashes).all(|i| {
                    let bit = h1.wrapping_add(i.wrapping_mul(h2)) % seen.bit_count;
                    seen.bits[(bit / 64) as usize] & (1u64 << (bit % 64)) != 0
                })
            })
            .count();
        assert!(false_positives < 500, "false positives: {false_positives}");
    }
}
