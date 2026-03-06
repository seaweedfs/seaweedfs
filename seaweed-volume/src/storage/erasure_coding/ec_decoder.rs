//! EC decoding: reconstruct a .dat file from EC shards.
//!
//! Rebuilds the original .dat + .idx files from data shards (.ec00-.ec09)
//! and the sorted index (.ecx) + deletion journal (.ecj).

use std::fs::File;
use std::io::{self, Read, Write};

use crate::storage::erasure_coding::ec_shard::*;
use crate::storage::idx;
use crate::storage::types::*;
use crate::storage::volume::volume_file_name;

/// Reconstruct a .dat file from EC data shards.
///
/// Reads from .ec00-.ec09 and writes a new .dat file.
pub fn write_dat_file_from_shards(
    dir: &str,
    collection: &str,
    volume_id: VolumeId,
    dat_file_size: i64,
) -> io::Result<()> {
    let base = volume_file_name(dir, collection, volume_id);
    let dat_path = format!("{}.dat", base);

    // Open data shards
    let mut shards: Vec<EcVolumeShard> = (0..DATA_SHARDS_COUNT as u8)
        .map(|i| EcVolumeShard::new(dir, collection, volume_id, i))
        .collect();

    for shard in &mut shards {
        shard.open()?;
    }

    let mut dat_file = File::create(&dat_path)?;
    let mut remaining = dat_file_size;
    let large_block_size = ERASURE_CODING_LARGE_BLOCK_SIZE;
    let small_block_size = ERASURE_CODING_SMALL_BLOCK_SIZE;
    let large_row_size = (large_block_size * DATA_SHARDS_COUNT) as i64;

    let mut shard_offset: u64 = 0;

    // Read large blocks
    while remaining >= large_row_size {
        for i in 0..DATA_SHARDS_COUNT {
            let mut buf = vec![0u8; large_block_size];
            shards[i].read_at(&mut buf, shard_offset)?;
            let to_write = large_block_size.min(remaining as usize);
            dat_file.write_all(&buf[..to_write])?;
            remaining -= to_write as i64;
            if remaining <= 0 {
                break;
            }
        }
        shard_offset += large_block_size as u64;
    }

    // Read small blocks
    while remaining > 0 {
        for i in 0..DATA_SHARDS_COUNT {
            let mut buf = vec![0u8; small_block_size];
            shards[i].read_at(&mut buf, shard_offset)?;
            let to_write = small_block_size.min(remaining as usize);
            dat_file.write_all(&buf[..to_write])?;
            remaining -= to_write as i64;
            if remaining <= 0 {
                break;
            }
        }
        shard_offset += small_block_size as u64;
    }

    for shard in &mut shards {
        shard.close();
    }

    dat_file.sync_all()?;
    Ok(())
}

/// Write .idx file from .ecx index + .ecj deletion journal.
///
/// Copies sorted .ecx entries to .idx, then appends tombstones for
/// deleted needles from .ecj.
pub fn write_idx_file_from_ec_index(
    dir: &str,
    collection: &str,
    volume_id: VolumeId,
) -> io::Result<()> {
    let base = volume_file_name(dir, collection, volume_id);
    let ecx_path = format!("{}.ecx", base);
    let ecj_path = format!("{}.ecj", base);
    let idx_path = format!("{}.idx", base);

    // Copy .ecx to .idx
    std::fs::copy(&ecx_path, &idx_path)?;

    // Append deletions from .ecj as tombstones
    if std::path::Path::new(&ecj_path).exists() {
        let ecj_data = std::fs::read(&ecj_path)?;
        if !ecj_data.is_empty() {
            let mut idx_file = std::fs::OpenOptions::new()
                .write(true)
                .append(true)
                .open(&idx_path)?;

            let count = ecj_data.len() / NEEDLE_ID_SIZE;
            for i in 0..count {
                let start = i * NEEDLE_ID_SIZE;
                let needle_id = NeedleId::from_bytes(&ecj_data[start..start + NEEDLE_ID_SIZE]);
                idx::write_index_entry(
                    &mut idx_file,
                    needle_id,
                    Offset::default(),
                    TOMBSTONE_FILE_SIZE,
                )?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::erasure_coding::ec_encoder;
    use crate::storage::needle::needle::Needle;
    use crate::storage::needle_map::NeedleMapKind;
    use crate::storage::volume::Volume;
    use tempfile::TempDir;

    #[test]
    fn test_ec_full_round_trip() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        // Create volume with data
        let mut v = Volume::new(
            dir, dir, "", VolumeId(1),
            NeedleMapKind::InMemory, None, None, 0, Version::current(),
        ).unwrap();

        let test_data: Vec<(NeedleId, Vec<u8>)> = (1..=3).map(|i| {
            let data = format!("EC round trip data for needle {}", i);
            (NeedleId(i), data.into_bytes())
        }).collect();

        for (id, data) in &test_data {
            let mut n = Needle {
                id: *id,
                cookie: Cookie(id.0 as u32),
                data: data.clone(),
                data_size: data.len() as u32,
                ..Needle::default()
            };
            v.write_needle(&mut n, true).unwrap();
        }
        v.sync_to_disk().unwrap();
        let original_dat_size = v.dat_file_size().unwrap();
        v.close();

        // Read original .dat for comparison
        let original_dat = std::fs::read(format!("{}/1.dat", dir)).unwrap();

        // Encode to EC
        ec_encoder::write_ec_files(dir, "", VolumeId(1)).unwrap();

        // Delete original .dat and .idx
        std::fs::remove_file(format!("{}/1.dat", dir)).unwrap();
        std::fs::remove_file(format!("{}/1.idx", dir)).unwrap();

        // Reconstruct from EC shards
        write_dat_file_from_shards(dir, "", VolumeId(1), original_dat_size as i64).unwrap();
        write_idx_file_from_ec_index(dir, "", VolumeId(1)).unwrap();

        // Verify reconstructed .dat matches original
        let reconstructed_dat = std::fs::read(format!("{}/1.dat", dir)).unwrap();
        assert_eq!(
            original_dat[..original_dat_size as usize],
            reconstructed_dat[..original_dat_size as usize],
            "reconstructed .dat should match original"
        );

        // Verify we can load and read from reconstructed volume
        let v2 = Volume::new(
            dir, dir, "", VolumeId(1),
            NeedleMapKind::InMemory, None, None, 0, Version::current(),
        ).unwrap();

        for (id, expected_data) in &test_data {
            let mut n = Needle { id: *id, ..Needle::default() };
            v2.read_needle(&mut n).unwrap();
            assert_eq!(&n.data, expected_data, "needle {} data should match", id);
        }
    }
}
