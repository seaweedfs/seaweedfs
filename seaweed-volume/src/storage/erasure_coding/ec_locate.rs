//! EC data location: maps needle offset/size to shard intervals.
//!
//! Determines which shard(s) contain data for a given needle and at what
//! offsets within those shards. Handles both large (1GB) and small (1MB)
//! block sections.

use crate::storage::erasure_coding::ec_shard::*;
use crate::storage::types::*;

/// An interval to read from EC shards.
#[derive(Debug, Clone)]
pub struct Interval {
    pub block_index: usize,
    pub inner_block_offset: i64,
    pub size: i64,
    pub is_large_block: bool,
    pub large_block_rows_count: usize,
}

impl Interval {
    pub fn to_shard_id_and_offset(&self, data_shards: u32) -> (ShardId, i64) {
        let data_shards_usize = data_shards as usize;
        let shard_id = (self.block_index % data_shards_usize) as ShardId;
        let row_index = self.block_index / data_shards_usize;

        let block_size = if self.is_large_block {
            ERASURE_CODING_LARGE_BLOCK_SIZE as i64
        } else {
            ERASURE_CODING_SMALL_BLOCK_SIZE as i64
        };

        let mut offset = row_index as i64 * block_size + self.inner_block_offset;
        if !self.is_large_block {
            // Small blocks come after large blocks in the shard file
            offset += self.large_block_rows_count as i64 * ERASURE_CODING_LARGE_BLOCK_SIZE as i64;
        }

        (shard_id, offset)
    }
}

/// Locate the EC shard intervals needed to read data at the given offset and size.
///
/// `shard_size` is the size of a single shard file.
pub fn locate_data(offset: i64, size: Size, shard_size: i64, data_shards: u32) -> Vec<Interval> {
    let mut intervals = Vec::new();
    let data_size = size.0 as i64;

    if data_size <= 0 || shard_size <= 0 {
        return intervals;
    }

    let large_block_size = ERASURE_CODING_LARGE_BLOCK_SIZE as i64;
    let small_block_size = ERASURE_CODING_SMALL_BLOCK_SIZE as i64;
    let large_row_size = large_block_size * data_shards as i64;
    let small_row_size = small_block_size * data_shards as i64;

    // Number of large block rows
    let n_large_block_rows = if shard_size > 0 {
        ((shard_size - 1) / large_block_size) as usize
    } else {
        0
    };
    let large_section_size = n_large_block_rows as i64 * large_row_size;

    let mut remaining_offset = offset;
    let mut remaining_size = data_size;

    // In large block section?
    if remaining_offset < large_section_size {
        let available_in_large = large_section_size - remaining_offset;
        let to_read = remaining_size.min(available_in_large);

        add_intervals(
            &mut intervals,
            remaining_offset,
            to_read,
            large_block_size,
            large_row_size,
            true,
            n_large_block_rows,
        );

        remaining_offset += to_read;
        remaining_size -= to_read;
    }

    // In small block section?
    if remaining_size > 0 {
        let small_offset = remaining_offset - large_section_size;
        add_intervals(
            &mut intervals,
            small_offset,
            remaining_size,
            small_block_size,
            small_row_size,
            false,
            n_large_block_rows,
        );
    }

    intervals
}

fn add_intervals(
    intervals: &mut Vec<Interval>,
    offset: i64,
    size: i64,
    block_size: i64,
    _row_size: i64,
    is_large_block: bool,
    large_block_rows_count: usize,
) {
    let mut pos = offset;
    let end = offset + size;

    while pos < end {
        let block_index = (pos / block_size) as usize;
        let inner_offset = pos % block_size;
        let remaining_in_block = block_size - inner_offset;
        let interval_size = remaining_in_block.min(end - pos);

        intervals.push(Interval {
            block_index,
            inner_block_offset: inner_offset,
            size: interval_size,
            is_large_block,
            large_block_rows_count,
        });

        pos += interval_size;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_to_shard_id() {
        let data_shards = 10;
        let large_block_size = ERASURE_CODING_LARGE_BLOCK_SIZE as i64;
        let _shard_size = 1024 * 1024; // Example shard size

        // Block index 0 → shard 0
        let interval = Interval {
            block_index: 0,
            inner_block_offset: 100,
            size: 50,
            is_large_block: true,
            large_block_rows_count: 1,
        };
        let (shard_id, offset) = interval.to_shard_id_and_offset(data_shards);
        assert_eq!(shard_id, 0);
        assert_eq!(offset, 100);

        // Block index 5 → shard 5
        let interval = Interval {
            block_index: 5,
            inner_block_offset: 0,
            size: 1024,
            is_large_block: true,
            large_block_rows_count: 1,
        };
        let (shard_id, _offset) = interval.to_shard_id_and_offset(data_shards);
        assert_eq!(shard_id, 5);

        // Block index 12 (data_shards=10) → row_index 1, shard_id 2
        let interval = Interval {
            block_index: 12,
            inner_block_offset: 200,
            size: 50,
            is_large_block: true,
            large_block_rows_count: 5,
        };
        let (shard_id, offset) = interval.to_shard_id_and_offset(data_shards);
        assert_eq!(shard_id, 2); // 12 % 10 = 2
        assert_eq!(offset, large_block_size + 200); // row 1 offset + inner_block_offset

        // Block index 10 → shard 0 (second row)
        let interval = Interval {
            block_index: 10,
            inner_block_offset: 0,
            size: 100,
            is_large_block: true,
            large_block_rows_count: 2,
        };
        let (shard_id, offset) = interval.to_shard_id_and_offset(data_shards);
        assert_eq!(shard_id, 0);
        assert_eq!(offset, ERASURE_CODING_LARGE_BLOCK_SIZE as i64); // row 1 offset
    }

    #[test]
    fn test_locate_data_small_file() {
        // Small file: 100 bytes at offset 50, shard size = 1MB
        let intervals = locate_data(50, Size(100), 1024 * 1024, 10);
        assert!(!intervals.is_empty());

        // Should be a single small block interval (no large block rows for 1MB shard)
        assert_eq!(intervals.len(), 1);
        assert!(!intervals[0].is_large_block);
    }

    #[test]
    fn test_locate_data_empty() {
        let intervals = locate_data(0, Size(0), 1024 * 1024, 10);
        assert!(intervals.is_empty());
    }

    #[test]
    fn test_small_block_after_large() {
        let interval = Interval {
            block_index: 0,
            inner_block_offset: 0,
            size: 100,
            is_large_block: false,
            large_block_rows_count: 2,
        };
        let (_shard_id, offset) = interval.to_shard_id_and_offset(10);
        // Should be after 2 large block rows
        assert_eq!(offset, 2 * ERASURE_CODING_LARGE_BLOCK_SIZE as i64);
    }
}
