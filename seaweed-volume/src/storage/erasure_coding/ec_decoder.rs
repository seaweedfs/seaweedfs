//! EC decoding: reconstruct a .dat file from EC shards.
//!
//! Rebuilds the original .dat + .idx files from data shards (.ec00-.ec09)
//! and the sorted index (.ecx) + deletion journal (.ecj).

use std::fs::File;
use std::io::{self, Read, Write};

use crate::storage::erasure_coding::ec_shard::*;
use crate::storage::idx;
use crate::storage::needle::needle::get_actual_size;
use crate::storage::super_block::SUPER_BLOCK_SIZE;
use crate::storage::types::*;
use crate::storage::volume::volume_file_name;

/// Calculate .dat file size from the max offset entry in .ecx.
/// Reads the volume version from the first EC shard (.ec00) superblock,
/// then scans .ecx entries to find the largest (offset + needle_actual_size).
///
/// `dir` is used both for reading `.ec00` and `.ecx`. For split-disk
/// reconciled volumes call [`find_dat_file_size_with_dirs`] instead.
pub fn find_dat_file_size(dir: &str, collection: &str, volume_id: VolumeId) -> io::Result<i64> {
    find_dat_file_size_with_dirs(dir, dir, collection, volume_id)
}

/// Like [`find_dat_file_size`] but lets the caller pass separate dirs
/// for `.ec00` (the data shard) and `.ecx` (the sealed index). This
/// is the form needed when shards are split across data dirs and the
/// `.ecx` lives on a sibling disk's idx dir (#9252).
pub fn find_dat_file_size_with_dirs(
    ec00_dir: &str,
    ecx_dir: &str,
    collection: &str,
    volume_id: VolumeId,
) -> io::Result<i64> {
    let ec00_base = volume_file_name(ec00_dir, collection, volume_id);
    let ecx_base = volume_file_name(ecx_dir, collection, volume_id);

    // Read volume version from .ec00 superblock
    let ec00_path = format!("{}.ec00", ec00_base);
    let mut ec00 = File::open(&ec00_path)?;
    let mut sb_buf = [0u8; SUPER_BLOCK_SIZE];
    ec00.read_exact(&mut sb_buf)?;
    let version = Version(sb_buf[0]);

    // Start with at least the superblock size
    let mut dat_size: i64 = SUPER_BLOCK_SIZE as i64;

    // Scan .ecx entries
    let ecx_path = format!("{}.ecx", ecx_base);
    let ecx_data = std::fs::read(&ecx_path)?;
    let entry_count = ecx_data.len() / NEEDLE_MAP_ENTRY_SIZE;

    for i in 0..entry_count {
        let start = i * NEEDLE_MAP_ENTRY_SIZE;
        let (_, offset, size) =
            idx_entry_from_bytes(&ecx_data[start..start + NEEDLE_MAP_ENTRY_SIZE]);
        if size.is_deleted() {
            continue;
        }
        let entry_stop = offset.to_actual_offset() + get_actual_size(size, version);
        if entry_stop > dat_size {
            dat_size = entry_stop;
        }
    }

    Ok(dat_size)
}

/// Reconstruct a .dat file from EC data shards.
///
/// Reads from .ec00-.ec09 and writes a new .dat file. All data shards
/// must live in `dir`. For the cross-disk reconciled layout where
/// shards are split across multiple data dirs of the same node, use
/// [`write_dat_file_from_shards_with_dirs`] instead.
pub fn write_dat_file_from_shards(
    dir: &str,
    collection: &str,
    volume_id: VolumeId,
    dat_file_size: i64,
    data_shards: usize,
) -> io::Result<()> {
    let dirs: Vec<String> = (0..data_shards).map(|_| dir.to_string()).collect();
    write_dat_file_from_shards_with_dirs(
        dir,
        collection,
        volume_id,
        dat_file_size,
        data_shards,
        &dirs,
    )
}

/// Reconstruct a .dat file from EC data shards, taking the source
/// directory for each shard separately.
///
/// `dat_dir` is where the produced `.dat` is written. `shard_dirs[i]`
/// is the directory holding shard `i`. For the simple "all shards in
/// one dir" case both can be the same value.
///
/// Mirrors Go's `WriteDatFile(baseFileName, datFileSize,
/// shardFileNames)` shape — Go passes per-shard paths so a
/// reconciled volume with shards split across disks of the same
/// volume server can still be decoded back to a regular .dat
/// (seaweedfs/seaweedfs#9252).
pub fn write_dat_file_from_shards_with_dirs(
    dat_dir: &str,
    collection: &str,
    volume_id: VolumeId,
    dat_file_size: i64,
    data_shards: usize,
    shard_dirs: &[String],
) -> io::Result<()> {
    if shard_dirs.len() < data_shards {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "shard_dirs len {} < data_shards {}",
                shard_dirs.len(),
                data_shards
            ),
        ));
    }
    let base = volume_file_name(dat_dir, collection, volume_id);
    let dat_path = format!("{}.dat", base);

    // Open data shards from their individual home dirs.
    let mut shards: Vec<EcVolumeShard> = (0..data_shards as u8)
        .map(|i| EcVolumeShard::new(&shard_dirs[i as usize], collection, volume_id, i))
        .collect();

    for shard in &mut shards {
        shard.open()?;
    }

    let mut dat_file = File::create(&dat_path)?;
    let mut remaining = dat_file_size;
    let large_block_size = ERASURE_CODING_LARGE_BLOCK_SIZE;
    let small_block_size = ERASURE_CODING_SMALL_BLOCK_SIZE;
    let large_row_size = (large_block_size * data_shards) as i64;

    let mut shard_offset: u64 = 0;

    // Read large blocks
    while remaining >= large_row_size {
        for i in 0..data_shards {
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
        for i in 0..data_shards {
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

        let test_data: Vec<(NeedleId, Vec<u8>)> = (1..=3)
            .map(|i| {
                let data = format!("EC round trip data for needle {}", i);
                (NeedleId(i), data.into_bytes())
            })
            .collect();

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
        let data_shards = 10;
        let parity_shards = 4;
        ec_encoder::write_ec_files(dir, dir, "", VolumeId(1), data_shards, parity_shards).unwrap();

        // Delete original .dat and .idx
        std::fs::remove_file(format!("{}/1.dat", dir)).unwrap();
        std::fs::remove_file(format!("{}/1.idx", dir)).unwrap();

        // Reconstruct from EC shards
        write_dat_file_from_shards(dir, "", VolumeId(1), original_dat_size as i64, data_shards)
            .unwrap();
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

        for (id, expected_data) in &test_data {
            let mut n = Needle {
                id: *id,
                ..Needle::default()
            };
            v2.read_needle(&mut n).unwrap();
            assert_eq!(&n.data, expected_data, "needle {} data should match", id);
        }
    }
}
