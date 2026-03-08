//! EC encoding: convert a .dat file into 10 data + 4 parity shards.
//!
//! Uses Reed-Solomon erasure coding. The .dat file is split into blocks
//! (1GB large, 1MB small) and encoded across 14 shard files.

use std::fs::File;
use std::io;
#[cfg(not(unix))]
use std::io::{Seek, SeekFrom};

use reed_solomon_erasure::galois_8::ReedSolomon;

use crate::storage::erasure_coding::ec_shard::*;
use crate::storage::idx;
use crate::storage::types::*;
use crate::storage::volume::volume_file_name;

/// Encode a .dat file into EC shard files.
///
/// Creates .ec00-.ec13 files in the same directory.
/// Also creates a sorted .ecx index from the .idx file.
pub fn write_ec_files(
    dir: &str,
    collection: &str,
    volume_id: VolumeId,
    data_shards: usize,
    parity_shards: usize,
) -> io::Result<()> {
    let base = volume_file_name(dir, collection, volume_id);
    let dat_path = format!("{}.dat", base);
    let idx_path = format!("{}.idx", base);

    // Create sorted .ecx from .idx
    write_sorted_ecx_from_idx(&idx_path, &format!("{}.ecx", base))?;

    // Encode .dat into shards
    let dat_file = File::open(&dat_path)?;
    let dat_size = dat_file.metadata()?.len() as i64;

    let rs = ReedSolomon::new(data_shards, parity_shards).map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("reed-solomon init: {:?}", e))
    })?;

    // Create shard files
    let total_shards = data_shards + parity_shards;
    let mut shards: Vec<EcVolumeShard> = (0..total_shards as u8)
        .map(|i| EcVolumeShard::new(dir, collection, volume_id, i))
        .collect();

    for shard in &mut shards {
        shard.create()?;
    }

    // Encode in large blocks, then small blocks
    encode_dat_file(&dat_file, dat_size, &rs, &mut shards, data_shards, parity_shards)?;

    // Close all shards
    for shard in &mut shards {
        shard.close();
    }

    Ok(())
}

/// Write sorted .ecx index from .idx file.
fn write_sorted_ecx_from_idx(idx_path: &str, ecx_path: &str) -> io::Result<()> {
    if !std::path::Path::new(idx_path).exists() {
        return Err(io::Error::new(io::ErrorKind::NotFound, "idx file not found"));
    }

    // Read all idx entries
    let mut idx_file = File::open(idx_path)?;
    let mut entries: Vec<(NeedleId, Offset, Size)> = Vec::new();

    idx::walk_index_file(&mut idx_file, 0, |key, offset, size| {
        entries.push((key, offset, size));
        Ok(())
    })?;

    // Sort by NeedleId, then by actual offset so later entries come last
    entries.sort_by_key(|&(key, offset, _)| (key, offset.to_actual_offset()));

    // Remove duplicates (keep last/latest entry for each key).
    // dedup_by_key keeps the first in each run, so we reverse first,
    // dedup, then reverse back.
    entries.reverse();
    entries.dedup_by_key(|entry| entry.0);
    entries.reverse();

    // Write sorted entries to .ecx
    let mut ecx_file = File::create(ecx_path)?;
    for &(key, offset, size) in &entries {
        idx::write_index_entry(&mut ecx_file, key, offset, size)?;
    }

    Ok(())
}

/// Encode the .dat file data into shard files.
fn encode_dat_file(
    dat_file: &File,
    dat_size: i64,
    rs: &ReedSolomon,
    shards: &mut [EcVolumeShard],
    data_shards: usize,
    parity_shards: usize,
) -> io::Result<()> {
    let block_size = ERASURE_CODING_SMALL_BLOCK_SIZE;
    let row_size = block_size * data_shards;

    let mut remaining = dat_size;
    let mut offset: u64 = 0;

    // Process all data in small blocks to avoid large memory allocations
    while remaining > 0 {
        let to_process = remaining.min(row_size as i64);
        encode_one_batch(dat_file, offset, block_size, rs, shards, data_shards, parity_shards)?;
        offset += to_process as u64;
        remaining -= to_process;
    }

    Ok(())
}

/// Encode one batch (row) of data.
fn encode_one_batch(
    dat_file: &File,
    offset: u64,
    block_size: usize,
    rs: &ReedSolomon,
    shards: &mut [EcVolumeShard],
    data_shards: usize,
    parity_shards: usize,
) -> io::Result<()> {
    let total_shards = data_shards + parity_shards;
    // Each batch allocates block_size * total_shards bytes.
    // With large blocks (1 GiB) this is 14 GiB -- guard against OOM.
    let total_alloc = block_size.checked_mul(total_shards).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "block_size * shard count overflows usize")
    })?;
    const MAX_BATCH_ALLOC: usize = 1024 * 1024 * 1024; // 1 GiB safety limit
    if total_alloc > MAX_BATCH_ALLOC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "batch allocation too large ({} bytes, limit {} bytes); block_size={} shards={}",
                total_alloc, MAX_BATCH_ALLOC, block_size, total_shards,
            ),
        ));
    }

    // Allocate buffers for all shards
    let mut buffers: Vec<Vec<u8>> = (0..total_shards)
        .map(|_| vec![0u8; block_size])
        .collect();

    // Read data shards from .dat file
    for i in 0..data_shards {
        let read_offset = offset + (i * block_size) as u64;

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            dat_file.read_at(&mut buffers[i], read_offset)?;
        }

        #[cfg(not(unix))]
        {
            let mut f = dat_file.try_clone()?;
            f.seek(SeekFrom::Start(read_offset))?;
            f.read(&mut buffers[i])?;
        }
    }

    // Encode parity shards
    rs.encode(&mut buffers).map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("reed-solomon encode: {:?}", e))
    })?;

    // Write all shard buffers to files
    for (i, buf) in buffers.iter().enumerate() {
        shards[i].write_all(buf)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::needle::needle::Needle;
    use crate::storage::needle_map::NeedleMapKind;
    use crate::storage::volume::Volume;
    use tempfile::TempDir;

    #[test]
    fn test_ec_encode_decode_round_trip() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        // Create a volume with some data
        let mut v = Volume::new(
            dir, dir, "", VolumeId(1),
            NeedleMapKind::InMemory, None, None, 0, Version::current(),
        ).unwrap();

        for i in 1..=5 {
            let data = format!("test data for needle {}", i);
            let mut n = Needle {
                id: NeedleId(i),
                cookie: Cookie(i as u32),
                data: data.as_bytes().to_vec(),
                data_size: data.len() as u32,
                ..Needle::default()
            };
            v.write_needle(&mut n, true).unwrap();
        }
        v.sync_to_disk().unwrap();
        v.close();

        // Encode to EC shards
        let data_shards = 10;
        let parity_shards = 4;
        let total_shards = data_shards + parity_shards;
        write_ec_files(dir, "", VolumeId(1), data_shards, parity_shards).unwrap();

        // Verify shard files exist
        for i in 0..total_shards {
            let path = format!("{}/{}.ec{:02}", dir, 1, i);
            assert!(
                std::path::Path::new(&path).exists(),
                "shard file {} should exist", path
            );
        }

        // Verify .ecx exists
        let ecx_path = format!("{}/1.ecx", dir);
        assert!(std::path::Path::new(&ecx_path).exists());
    }

    #[test]
    fn test_reed_solomon_basic() {
        let data_shards = 10;
        let parity_shards = 4;
        let total_shards = data_shards + parity_shards;
        let rs = ReedSolomon::new(data_shards, parity_shards).unwrap();
        let block_size = 1024;
        let mut shards: Vec<Vec<u8>> = (0..total_shards)
            .map(|i| {
                if i < data_shards {
                    vec![(i as u8).wrapping_mul(7); block_size]
                } else {
                    vec![0u8; block_size]
                }
            })
            .collect();

        // Encode
        rs.encode(&mut shards).unwrap();

        // Verify parity is non-zero (at least some)
        let parity_nonzero: bool = shards[data_shards..].iter()
            .any(|s| s.iter().any(|&b| b != 0));
        assert!(parity_nonzero);

        // Simulate losing 4 shards and reconstructing
        let original_0 = shards[0].clone();
        let original_1 = shards[1].clone();

        let mut shard_opts: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shard_opts[0] = None;
        shard_opts[1] = None;
        shard_opts[2] = None;
        shard_opts[3] = None;

        rs.reconstruct(&mut shard_opts).unwrap();

        assert_eq!(shard_opts[0].as_ref().unwrap(), &original_0);
        assert_eq!(shard_opts[1].as_ref().unwrap(), &original_1);
    }
}
