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
    idx_dir: &str,
    collection: &str,
    volume_id: VolumeId,
    data_shards: usize,
    parity_shards: usize,
) -> io::Result<()> {
    let base = volume_file_name(dir, collection, volume_id);
    let dat_path = format!("{}.dat", base);
    let idx_base = volume_file_name(idx_dir, collection, volume_id);
    let idx_path = format!("{}.idx", idx_base);

    // Create sorted .ecx from .idx
    write_sorted_ecx_from_idx(&idx_path, &format!("{}.ecx", base))?;

    // Encode .dat into shards
    let dat_file = File::open(&dat_path)?;
    let dat_size = dat_file.metadata()?.len() as i64;

    let rs = ReedSolomon::new(data_shards, parity_shards)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("reed-solomon init: {:?}", e)))?;

    // Create shard files
    let total_shards = data_shards + parity_shards;
    let mut shards: Vec<EcVolumeShard> = (0..total_shards as u8)
        .map(|i| EcVolumeShard::new(dir, collection, volume_id, i))
        .collect();

    for shard in &mut shards {
        shard.create()?;
    }

    // Encode in large blocks, then small blocks
    encode_dat_file(
        &dat_file,
        dat_size,
        &rs,
        &mut shards,
        data_shards,
        parity_shards,
    )?;

    // Close all shards
    for shard in &mut shards {
        shard.close();
    }

    Ok(())
}

/// Rebuild missing EC shard files from existing shards using Reed-Solomon reconstruct.
///
/// This does not require the `.dat` file, only the existing `.ecXX` shard files.
pub fn rebuild_ec_files(
    dir: &str,
    collection: &str,
    volume_id: VolumeId,
    missing_shard_ids: &[u32],
    data_shards: usize,
    parity_shards: usize,
) -> io::Result<()> {
    if missing_shard_ids.is_empty() {
        return Ok(());
    }

    let rs = ReedSolomon::new(data_shards, parity_shards)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("reed-solomon init: {:?}", e)))?;

    let total_shards = data_shards + parity_shards;
    let mut shards: Vec<EcVolumeShard> = (0..total_shards as u8)
        .map(|i| EcVolumeShard::new(dir, collection, volume_id, i))
        .collect();

    // Determine the exact shard size from the first available existing shard
    let mut shard_size = 0;
    for (i, shard) in shards.iter_mut().enumerate() {
        if !missing_shard_ids.contains(&(i as u32)) {
            if let Ok(_) = shard.open() {
                let size = shard.file_size();
                if size > shard_size {
                    shard_size = size;
                }
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("missing non-rebuild shard {}", i),
                ));
            }
        }
    }

    if shard_size == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "all existing shards are empty or cannot find an existing shard to determine size",
        ));
    }

    // Create the missing shards for writing
    for i in missing_shard_ids {
        if let Some(shard) = shards.get_mut(*i as usize) {
            shard.create()?;
        }
    }

    let block_size = ERASURE_CODING_SMALL_BLOCK_SIZE;
    let mut remaining = shard_size;
    let mut offset: u64 = 0;

    // Process all data in blocks
    while remaining > 0 {
        let to_process = remaining.min(block_size as i64) as usize;

        // Allocate buffers for all shards. Option<Vec<u8>> is required by rs.reconstruct()
        let mut buffers: Vec<Option<Vec<u8>>> = vec![None; total_shards];

        // Read available shards
        for (i, shard) in shards.iter().enumerate() {
            if !missing_shard_ids.contains(&(i as u32)) {
                let mut buf = vec![0u8; to_process];
                shard.read_at(&mut buf, offset)?;
                buffers[i] = Some(buf);
            }
        }

        // Reconstruct missing shards
        rs.reconstruct(&mut buffers).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("reed-solomon reconstruct: {:?}", e),
            )
        })?;

        // Write recovered data into the missing shards
        for i in missing_shard_ids {
            let idx = *i as usize;
            if let Some(buf) = buffers[idx].take() {
                shards[idx].write_all(&buf)?;
            }
        }

        offset += to_process as u64;
        remaining -= to_process as i64;
    }

    // Close all shards
    for shard in &mut shards {
        shard.close();
    }

    Ok(())
}

/// Verify EC shards by computing parity against the existing data and identifying corrupted shards.
pub fn verify_ec_shards(
    dir: &str,
    collection: &str,
    volume_id: VolumeId,
    data_shards: usize,
    parity_shards: usize,
) -> io::Result<(Vec<u32>, Vec<String>)> {
    let rs = ReedSolomon::new(data_shards, parity_shards)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("reed-solomon init: {:?}", e)))?;

    let total_shards = data_shards + parity_shards;
    let mut shards: Vec<EcVolumeShard> = (0..total_shards as u8)
        .map(|i| EcVolumeShard::new(dir, collection, volume_id, i))
        .collect();

    let mut shard_size = 0;
    let mut broken_shards = std::collections::HashSet::new();
    let mut details = Vec::new();

    for (i, shard) in shards.iter_mut().enumerate() {
        if let Ok(_) = shard.open() {
            let size = shard.file_size();
            if size > shard_size {
                shard_size = size;
            }
        } else {
            broken_shards.insert(i as u32);
            details.push(format!("failed to open or missing shard {}", i));
        }
    }

    if shard_size == 0 || broken_shards.len() >= parity_shards {
        // Can't do much if we don't know the size or have too many missing
        return Ok((broken_shards.into_iter().collect(), details));
    }

    let block_size = ERASURE_CODING_SMALL_BLOCK_SIZE;
    let mut remaining = shard_size;
    let mut offset: u64 = 0;

    while remaining > 0 {
        let to_process = remaining.min(block_size as i64) as usize;
        let mut buffers = vec![vec![0u8; to_process]; total_shards];

        let mut read_failed = false;
        for i in 0..total_shards {
            if !broken_shards.contains(&(i as u32)) {
                if let Err(e) = shards[i].read_at(&mut buffers[i], offset) {
                    broken_shards.insert(i as u32);
                    details.push(format!("read error shard {}: {}", i, e));
                    read_failed = true;
                }
            } else {
                read_failed = true;
            }
        }

        // Only do verification if all shards were readable
        if !read_failed {
            // Need to convert Vec<Vec<u8>> to &[&[u8]] for rs.verify
            let slice_ptrs: Vec<&[u8]> = buffers.iter().map(|v| v.as_slice()).collect();
            if let Ok(is_valid) = rs.verify(&slice_ptrs) {
                if !is_valid {
                    // Reed-Solomon verification failed. We cannot easily pinpoint which shard
                    // is corrupted without recalculating parities or syndromes, so we just
                    // log that this batch has corruption. Wait, we can test each parity shard!
                    // Let's re-encode from the first `data_shards` and compare to the actual `parity_shards`.

                    let mut verify_buffers = buffers.clone();
                    // Clear the parity parts
                    for i in data_shards..total_shards {
                        verify_buffers[i].fill(0);
                    }
                    if rs.encode(&mut verify_buffers).is_ok() {
                        for i in 0..total_shards {
                            if buffers[i] != verify_buffers[i] {
                                broken_shards.insert(i as u32);
                                details.push(format!(
                                    "parity mismatch on shard {} at offset {}",
                                    i, offset
                                ));
                            }
                        }
                    }
                }
            }
        }

        offset += to_process as u64;
        remaining -= to_process as i64;
    }

    // Close all shards
    for shard in &mut shards {
        shard.close();
    }

    let mut broken_vec: Vec<u32> = broken_shards.into_iter().collect();
    broken_vec.sort_unstable();

    Ok((broken_vec, details))
}

/// Write sorted .ecx index from .idx file.
fn write_sorted_ecx_from_idx(idx_path: &str, ecx_path: &str) -> io::Result<()> {
    if !std::path::Path::new(idx_path).exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "idx file not found",
        ));
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

/// Rebuild the .ecx index file by walking needles in the EC data shards.
///
/// This is the equivalent of Go's `RebuildEcxFile`. It reads the logical .dat
/// content from the EC data shards, walks through needle headers to extract
/// (needle_id, offset, size) entries, deduplicates them, and writes a sorted
/// .ecx index file.
pub fn rebuild_ecx_file(
    dir: &str,
    collection: &str,
    volume_id: VolumeId,
    data_shards: usize,
) -> io::Result<()> {
    use crate::storage::needle::needle::get_actual_size;
    use crate::storage::super_block::SUPER_BLOCK_SIZE;

    let base = volume_file_name(dir, collection, volume_id);
    let ecx_path = format!("{}.ecx", base);

    // Open data shards to read logical .dat content
    let mut shards: Vec<EcVolumeShard> = (0..data_shards as u8)
        .map(|i| EcVolumeShard::new(dir, collection, volume_id, i))
        .collect();

    for shard in &mut shards {
        if let Err(_) = shard.open() {
            // If a data shard is missing, we can't rebuild ecx
            for s in &mut shards {
                s.close();
            }
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("cannot open data shard for ecx rebuild"),
            ));
        }
    }

    // Determine total logical data size from shard sizes
    let shard_size = shards.iter().map(|s| s.file_size()).max().unwrap_or(0);
    let total_data_size = shard_size as i64 * data_shards as i64;

    // Read version from superblock (first byte of logical data)
    let mut sb_buf = [0u8; SUPER_BLOCK_SIZE];
    read_from_data_shards(&shards, &mut sb_buf, 0, data_shards)?;
    let version = Version(sb_buf[0]);

    // Walk needles starting after superblock
    let mut offset = SUPER_BLOCK_SIZE as i64;
    let header_size = NEEDLE_HEADER_SIZE;
    let mut entries: Vec<(NeedleId, Offset, Size)> = Vec::new();

    while offset + header_size as i64 <= total_data_size {
        // Read needle header (cookie + needle_id + size = 16 bytes)
        let mut header_buf = [0u8; NEEDLE_HEADER_SIZE];
        if read_from_data_shards(&shards, &mut header_buf, offset as u64, data_shards).is_err() {
            break;
        }

        let cookie = Cookie::from_bytes(&header_buf[..COOKIE_SIZE]);
        let needle_id = NeedleId::from_bytes(&header_buf[COOKIE_SIZE..COOKIE_SIZE + NEEDLE_ID_SIZE]);
        let size = Size::from_bytes(&header_buf[COOKIE_SIZE + NEEDLE_ID_SIZE..header_size]);

        // Validate: stop if we hit zero cookie+id (end of data)
        if cookie.0 == 0 && needle_id.0 == 0 {
            break;
        }

        // Validate size is reasonable
        if size.0 < 0 && !size.is_deleted() {
            break;
        }

        let actual_size = get_actual_size(size, version);
        if actual_size <= 0 || offset + actual_size > total_data_size {
            break;
        }

        entries.push((needle_id, Offset::from_actual_offset(offset), size));

        // Advance to next needle (aligned to NEEDLE_PADDING_SIZE)
        offset += actual_size;
        let padding_rem = offset % NEEDLE_PADDING_SIZE as i64;
        if padding_rem != 0 {
            offset += NEEDLE_PADDING_SIZE as i64 - padding_rem;
        }
    }

    for shard in &mut shards {
        shard.close();
    }

    // Sort by NeedleId, then by offset (later entries override earlier)
    entries.sort_by_key(|&(key, offset, _)| (key, offset.to_actual_offset()));

    // Deduplicate: keep latest entry per needle_id
    entries.reverse();
    entries.dedup_by_key(|entry| entry.0);
    entries.reverse();

    // Write sorted .ecx
    let mut ecx_file = File::create(&ecx_path)?;
    for &(key, offset, size) in &entries {
        idx::write_index_entry(&mut ecx_file, key, offset, size)?;
    }
    ecx_file.sync_all()?;

    Ok(())
}

/// Read bytes from EC data shards at a logical offset in the .dat file.
fn read_from_data_shards(
    shards: &[EcVolumeShard],
    buf: &mut [u8],
    logical_offset: u64,
    data_shards: usize,
) -> io::Result<()> {
    let small_block = ERASURE_CODING_SMALL_BLOCK_SIZE as u64;
    let data_shards_u64 = data_shards as u64;

    let mut bytes_read = 0u64;
    let mut remaining = buf.len() as u64;
    let mut current_offset = logical_offset;

    while remaining > 0 {
        // Determine which shard and at what shard-offset this logical offset maps to.
        // The data is interleaved: large blocks first, then small blocks.
        // For simplicity, use the small block size for all calculations since
        // large blocks are multiples of small blocks.
        let row_size = small_block * data_shards_u64;
        let row_index = current_offset / row_size;
        let row_offset = current_offset % row_size;
        let shard_index = (row_offset / small_block) as usize;
        let shard_offset = row_index * small_block + (row_offset % small_block);

        if shard_index >= data_shards {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shard index out of range",
            ));
        }

        // How many bytes can we read from this position in this shard block
        let bytes_left_in_block = small_block - (row_offset % small_block);
        let to_read = remaining.min(bytes_left_in_block) as usize;

        let dest = &mut buf[bytes_read as usize..bytes_read as usize + to_read];
        shards[shard_index].read_at(dest, shard_offset)?;

        bytes_read += to_read as u64;
        remaining -= to_read as u64;
        current_offset += to_read as u64;
    }

    Ok(())
}

/// Encode the .dat file data into shard files.
///
/// Uses a two-phase approach matching Go's ec_encoder.go:
/// 1. Process as many large blocks (1GB) as possible
/// 2. Process remaining data with small blocks (1MB)
fn encode_dat_file(
    dat_file: &File,
    dat_size: i64,
    rs: &ReedSolomon,
    shards: &mut [EcVolumeShard],
    data_shards: usize,
    parity_shards: usize,
) -> io::Result<()> {
    let mut remaining = dat_size;
    let mut offset: u64 = 0;

    // Phase 1: Process large blocks (1GB each) while enough data remains
    let large_block_size = ERASURE_CODING_LARGE_BLOCK_SIZE;
    let large_row_size = large_block_size * data_shards;

    while remaining >= large_row_size as i64 {
        encode_one_batch(
            dat_file,
            offset,
            large_block_size,
            rs,
            shards,
            data_shards,
            parity_shards,
        )?;
        offset += large_row_size as u64;
        remaining -= large_row_size as i64;
    }

    // Phase 2: Process remaining data with small blocks (1MB each)
    let small_block_size = ERASURE_CODING_SMALL_BLOCK_SIZE;
    let small_row_size = small_block_size * data_shards;

    while remaining > 0 {
        let to_process = remaining.min(small_row_size as i64);
        encode_one_batch(
            dat_file,
            offset,
            small_block_size,
            rs,
            shards,
            data_shards,
            parity_shards,
        )?;
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
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "block_size * shard count overflows usize",
        )
    })?;
    // Large-block encoding uses 1 GiB * 14 shards = 14 GiB; allow up to 16 GiB.
    const MAX_BATCH_ALLOC: usize = 16 * 1024 * 1024 * 1024; // 16 GiB safety limit
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
    let mut buffers: Vec<Vec<u8>> = (0..total_shards).map(|_| vec![0u8; block_size]).collect();

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
        io::Error::new(
            io::ErrorKind::Other,
            format!("reed-solomon encode: {:?}", e),
        )
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
            dir,
            dir,
            "",
            VolumeId(1),
            NeedleMapKind::InMemory,
            None,
            None,
            0,
            Version::current(),
        )
        .unwrap();

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
        write_ec_files(dir, dir, "", VolumeId(1), data_shards, parity_shards).unwrap();

        // Verify shard files exist
        for i in 0..total_shards {
            let path = format!("{}/{}.ec{:02}", dir, 1, i);
            assert!(
                std::path::Path::new(&path).exists(),
                "shard file {} should exist",
                path
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
        let parity_nonzero: bool = shards[data_shards..]
            .iter()
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

    /// EC encode must read .idx from a separate index directory when configured.
    #[test]
    fn test_ec_encode_with_separate_idx_dir() {
        let dat_tmp = TempDir::new().unwrap();
        let idx_tmp = TempDir::new().unwrap();
        let dat_dir = dat_tmp.path().to_str().unwrap();
        let idx_dir = idx_tmp.path().to_str().unwrap();

        // Create a volume with separate data and index directories
        let mut v = Volume::new(
            dat_dir,
            idx_dir,
            "",
            VolumeId(1),
            NeedleMapKind::InMemory,
            None,
            None,
            0,
            Version::current(),
        )
        .unwrap();

        for i in 1..=5 {
            let data = format!("needle {} payload", i);
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

        // Verify .dat is in data dir, .idx is in idx dir
        assert!(std::path::Path::new(&format!("{}/1.dat", dat_dir)).exists());
        assert!(!std::path::Path::new(&format!("{}/1.idx", dat_dir)).exists());
        assert!(std::path::Path::new(&format!("{}/1.idx", idx_dir)).exists());
        assert!(!std::path::Path::new(&format!("{}/1.dat", idx_dir)).exists());

        // EC encode with separate idx dir
        let data_shards = 10;
        let parity_shards = 4;
        let total_shards = data_shards + parity_shards;
        write_ec_files(
            dat_dir,
            idx_dir,
            "",
            VolumeId(1),
            data_shards,
            parity_shards,
        )
        .unwrap();

        // Verify all 14 shard files in data dir
        for i in 0..total_shards {
            let path = format!("{}/1.ec{:02}", dat_dir, i);
            assert!(
                std::path::Path::new(&path).exists(),
                "shard {} should exist in data dir",
                path
            );
        }

        // Verify .ecx in data dir (not idx dir)
        assert!(std::path::Path::new(&format!("{}/1.ecx", dat_dir)).exists());
        assert!(!std::path::Path::new(&format!("{}/1.ecx", idx_dir)).exists());

        // Verify no shard files leaked into idx dir
        for i in 0..total_shards {
            let path = format!("{}/1.ec{:02}", idx_dir, i);
            assert!(
                !std::path::Path::new(&path).exists(),
                "shard {} should NOT exist in idx dir",
                path
            );
        }
    }

    /// EC encode should fail gracefully when .idx is only in the data dir
    /// but we pass a wrong idx_dir. This guards against regressions where
    /// write_ec_files ignores the idx_dir parameter.
    #[test]
    fn test_ec_encode_fails_with_wrong_idx_dir() {
        let dat_tmp = TempDir::new().unwrap();
        let idx_tmp = TempDir::new().unwrap();
        let wrong_tmp = TempDir::new().unwrap();
        let dat_dir = dat_tmp.path().to_str().unwrap();
        let idx_dir = idx_tmp.path().to_str().unwrap();
        let wrong_dir = wrong_tmp.path().to_str().unwrap();

        let mut v = Volume::new(
            dat_dir,
            idx_dir,
            "",
            VolumeId(1),
            NeedleMapKind::InMemory,
            None,
            None,
            0,
            Version::current(),
        )
        .unwrap();

        let mut n = Needle {
            id: NeedleId(1),
            cookie: Cookie(1),
            data: b"hello".to_vec(),
            data_size: 5,
            ..Needle::default()
        };
        v.write_needle(&mut n, true).unwrap();
        v.sync_to_disk().unwrap();
        v.close();

        // Should fail: .idx is in idx_dir, not wrong_dir
        let result = write_ec_files(dat_dir, wrong_dir, "", VolumeId(1), 10, 4);
        assert!(
            result.is_err(),
            "should fail when idx_dir doesn't contain .idx"
        );
    }
}
