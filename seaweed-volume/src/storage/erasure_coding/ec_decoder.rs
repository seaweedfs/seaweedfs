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
use crate::storage::volume::{fsync_dir, volume_file_name};

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
    encoded_dat_file_size: i64,
    data_shards: usize,
) -> io::Result<()> {
    let dirs: Vec<String> = (0..data_shards).map(|_| dir.to_string()).collect();
    write_dat_file_from_shards_with_dirs(
        dir,
        collection,
        volume_id,
        dat_file_size,
        encoded_dat_file_size,
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
/// encodedDatFileSize, shardFileNames)` shape — Go passes per-shard
/// paths so a reconciled volume with shards split across disks of the
/// same volume server can still be decoded back to a regular .dat
/// (seaweedfs/seaweedfs#9252).
///
/// `dat_file_size` is the number of bytes to write, i.e. the live data
/// extent from [`find_dat_file_size`]. `encoded_dat_file_size` is the
/// .dat size at encode time, which fixed the shard block layout:
/// deletions can move the live extent below the large-block row
/// boundary, and deriving the layout from the shrunk extent would read
/// the shards in the wrong block order.
pub fn write_dat_file_from_shards_with_dirs(
    dat_dir: &str,
    collection: &str,
    volume_id: VolumeId,
    dat_file_size: i64,
    encoded_dat_file_size: i64,
    data_shards: usize,
    shard_dirs: &[String],
) -> io::Result<()> {
    write_dat_file(
        dat_dir,
        collection,
        volume_id,
        dat_file_size,
        encoded_dat_file_size,
        data_shards,
        shard_dirs,
        ERASURE_CODING_LARGE_BLOCK_SIZE,
        ERASURE_CODING_SMALL_BLOCK_SIZE,
    )
}

#[allow(clippy::too_many_arguments)]
fn write_dat_file(
    dat_dir: &str,
    collection: &str,
    volume_id: VolumeId,
    dat_file_size: i64,
    encoded_dat_file_size: i64,
    data_shards: usize,
    shard_dirs: &[String],
    large_block_size: usize,
    small_block_size: usize,
) -> io::Result<()> {
    if dat_file_size > encoded_dat_file_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "dat file size {} exceeds encoded dat file size {}",
                dat_file_size, encoded_dat_file_size
            ),
        ));
    }
    if data_shards == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no data shards",
        ));
    }
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
    // Write to a temp file and atomically rename into place, so a crash
    // mid-write never leaves a partial .dat at the final name beside the
    // source shards.
    let tmp_path = format!("{}.tmp", dat_path);

    let write_result = (|| -> io::Result<()> {
        // Open data shards from their individual home dirs.
        let mut shards: Vec<EcVolumeShard> = (0..data_shards as u8)
            .map(|i| EcVolumeShard::new(&shard_dirs[i as usize], collection, volume_id, i))
            .collect();

        for shard in &mut shards {
            shard.open()?;
        }

        let mut dat_file = File::create(&tmp_path)?;
        let mut remaining = dat_file_size;
        let mut encoded_remaining = encoded_dat_file_size;
        let large_row_size = (large_block_size * data_shards) as i64;

        let mut shard_offset: u64 = 0;

        // Read large blocks
        while encoded_remaining >= large_row_size && remaining > 0 {
            for i in 0..data_shards {
                let to_write = large_block_size.min(remaining as usize);
                let mut buf = vec![0u8; to_write];
                let n = shards[i].read_at(&mut buf, shard_offset)?;
                if n != to_write {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("short read of large block on shard {}", i),
                    ));
                }
                dat_file.write_all(&buf)?;
                remaining -= to_write as i64;
                if remaining <= 0 {
                    break;
                }
            }
            encoded_remaining -= large_row_size;
            shard_offset += large_block_size as u64;
        }

        // Read small blocks
        while remaining > 0 {
            for i in 0..data_shards {
                let to_write = small_block_size.min(remaining as usize);
                let mut buf = vec![0u8; to_write];
                let n = shards[i].read_at(&mut buf, shard_offset)?;
                if n != to_write {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("short read of small block on shard {}", i),
                    ));
                }
                dat_file.write_all(&buf)?;
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

        // fsync, rename, then fsync the dir so the decoded .dat is durable and
        // atomically published before the caller deletes the source shards.
        dat_file.sync_all()?;
        drop(dat_file);
        // Windows rename does not replace an existing file on every version;
        // remove the destination first, matching the compaction commit path.
        #[cfg(windows)]
        {
            let _ = std::fs::remove_file(&dat_path);
        }
        std::fs::rename(&tmp_path, &dat_path)?;
        fsync_dir(&dat_path)?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }
    write_result
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
    // Write to a temp file and atomically rename into place, so a crash
    // mid-write never leaves a partial .idx at the final name beside the
    // source shards.
    let tmp_path = format!("{}.tmp", idx_path);

    let write_result = (|| -> io::Result<()> {
        // Copy .ecx to the temp .idx
        std::fs::copy(&ecx_path, &tmp_path)?;

        // Append deletions from .ecj as tombstones. Read the journal directly
        // and treat only NotFound as "no journal": Path::exists would also
        // swallow a permission/IO error and silently skip deletions, which
        // would resurrect deleted needles as live.
        let mut idx_file = std::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&tmp_path)?;
        match std::fs::read(&ecj_path) {
            Ok(ecj_data) => {
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
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }

        // fsync, rename, then fsync the dir so the decoded .idx is durable and
        // atomically published before the caller deletes the source shards.
        idx_file.sync_all()?;
        drop(idx_file);
        // Windows rename does not replace an existing file on every version;
        // remove the destination first, matching the compaction commit path.
        #[cfg(windows)]
        {
            let _ = std::fs::remove_file(&idx_path);
        }
        std::fs::rename(&tmp_path, &idx_path)?;
        fsync_dir(&idx_path)?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }
    write_result
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
        write_dat_file_from_shards(
            dir,
            "",
            VolumeId(1),
            original_dat_size as i64,
            original_dat_size as i64,
            data_shards,
        )
        .unwrap();
        write_idx_file_from_ec_index(dir, "", VolumeId(1)).unwrap();

        // Atomic publish must rename the temp files away, never leaving them behind.
        assert!(!std::path::Path::new(&format!("{}/1.dat.tmp", dir)).exists());
        assert!(!std::path::Path::new(&format!("{}/1.idx.tmp", dir)).exists());

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

    #[test]
    fn test_decode_missing_shard_leaves_no_dat() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        // No shard files exist, so de-striping must fail and publish nothing:
        // neither the final .dat nor a partial .dat.tmp may remain.
        let res = write_dat_file_from_shards(dir, "", VolumeId(7), 100, 100, 10);
        assert!(res.is_err());
        assert!(!std::path::Path::new(&format!("{}/7.dat", dir)).exists());
        assert!(!std::path::Path::new(&format!("{}/7.dat.tmp", dir)).exists());
    }

    // Decoding after deletions moved the live extent below the large-block row
    // boundary: the shard block layout is fixed by the encode-time .dat size,
    // so de-striping must not derive it from the shrunk live extent.
    #[test]
    fn test_write_dat_file_after_tail_deletion() {
        use crate::storage::erasure_coding::ec_bitrot::{
            ShardChecksumBuilder, DEFAULT_BITROT_BLOCK_SIZE,
        };
        use reed_solomon_erasure::galois_8::ReedSolomon;

        const LARGE: usize = 10000;
        const SMALL: usize = 100;
        let data_shards = 10usize;
        let parity_shards = 4usize;
        let large_row_size = (LARGE * data_shards) as i64;
        let small_row_size = (SMALL * data_shards) as i64;

        // one full large-block row plus a small-block tail
        let dat_size = large_row_size + 2 * small_row_size + 530;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let original: Vec<u8> = (0..dat_size as usize)
            .map(|i| ((i as u64).wrapping_mul(2654435761) >> 8) as u8)
            .collect();
        std::fs::write(format!("{}/1.dat", dir), &original).unwrap();

        let dat_file = File::open(format!("{}/1.dat", dir)).unwrap();
        let rs = ReedSolomon::new(data_shards, parity_shards).unwrap();
        let total = data_shards + parity_shards;
        let mut shards: Vec<EcVolumeShard> = (0..total as u8)
            .map(|i| EcVolumeShard::new(dir, "", VolumeId(1), i))
            .collect();
        for shard in &mut shards {
            shard.create().unwrap();
        }
        let mut builders: Vec<ShardChecksumBuilder> = (0..total)
            .map(|_| ShardChecksumBuilder::new(DEFAULT_BITROT_BLOCK_SIZE as i64))
            .collect();
        ec_encoder::encode_dat_file(
            &dat_file,
            dat_size,
            &rs,
            &mut shards,
            &mut builders,
            data_shards,
            parity_shards,
            LARGE,
            SMALL,
        )
        .unwrap();
        for shard in &mut shards {
            shard.close();
        }

        let shard_size = std::fs::metadata(format!("{}/1.ec00", dir)).unwrap().len() as i64;
        let padded_size = data_shards as i64 * shard_size;
        let shard_dirs: Vec<String> = (0..data_shards).map(|_| dir.to_string()).collect();

        // decode into a separate dir so the output does not collide with the source .dat
        let out_dir = tmp.path().join("out");
        std::fs::create_dir(&out_dir).unwrap();
        let out = out_dir.to_str().unwrap();
        let decode = |live_size: i64, encoded_size: i64| -> Vec<u8> {
            write_dat_file(
                out,
                "",
                VolumeId(1),
                live_size,
                encoded_size,
                data_shards,
                &shard_dirs,
                LARGE,
                SMALL,
            )
            .unwrap();
            let path = format!("{}/1.dat", out);
            let decoded = std::fs::read(&path).unwrap();
            std::fs::remove_file(&path).unwrap();
            decoded
        };

        for live_size in [
            LARGE as i64 - 1,                  // within the first large block
            LARGE as i64 + 42,                 // partial second large block
            large_row_size / 2,                // mid large row
            large_row_size,                    // exactly the large row
            large_row_size + 5 * SMALL as i64, // into the small-block tail
            dat_size,                          // nothing deleted
        ] {
            assert_eq!(
                &original[..live_size as usize],
                &decode(live_size, dat_size)[..],
                "live size {} with encode-time layout",
                live_size
            );
            assert_eq!(
                &original[..live_size as usize],
                &decode(live_size, padded_size)[..],
                "live size {} with padded layout",
                live_size
            );
        }

        // deriving the layout from the shrunk live extent reorders the data
        let control = decode(large_row_size / 2, large_row_size / 2);
        assert_ne!(&original[..(large_row_size / 2) as usize], &control[..]);

        // the live extent can never exceed the encode-time size
        assert!(write_dat_file(
            out,
            "",
            VolumeId(1),
            dat_size + 1,
            dat_size,
            data_shards,
            &shard_dirs,
            LARGE,
            SMALL,
        )
        .is_err());
    }
}
