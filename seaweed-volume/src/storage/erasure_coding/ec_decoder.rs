//! EC decoding: reconstruct a .dat file from EC shards.
//!
//! Rebuilds the original .dat + .idx files from data shards (.ec00-.ec09)
//! and the sorted index (.ecx) + deletion journal (.ecj).

use std::collections::HashSet;
use std::fs::File;
use std::io::{self, Read, Write};

use crate::storage::erasure_coding::ec_shard::*;
use crate::storage::erasure_coding::ec_volume::read_ecj_ids;
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
    let deleted = read_ecj_deletions(&[dir], collection, volume_id)?;
    find_dat_file_size_with_dirs(dir, dir, collection, volume_id, &deleted)
}

/// Like [`find_dat_file_size`] but lets the caller pass separate dirs
/// for `.ec00` (the data shard) and `.ecx` (the sealed index). This
/// is the form needed when shards are split across data dirs and the
/// `.ecx` lives on a sibling disk's idx dir (#9252). Needles in `deleted`
/// count as deleted.
pub fn find_dat_file_size_with_dirs(
    ec00_dir: &str,
    ecx_dir: &str,
    collection: &str,
    volume_id: VolumeId,
    deleted: &HashSet<NeedleId>,
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
        let (key, offset, size) =
            idx_entry_from_bytes(&ecx_data[start..start + NEEDLE_MAP_ENTRY_SIZE]);
        if size.is_deleted() || deleted.contains(&key) {
            continue;
        }
        let entry_stop = offset.to_actual_offset() + get_actual_size(size, version);
        if entry_stop > dat_size {
            dat_size = entry_stop;
        }
    }

    Ok(dat_size)
}

/// Whether the `.ecx` in `ecx_dir` indexes a needle deleted neither there nor
/// in `deleted`.
pub fn has_live_needles(
    ecx_dir: &str,
    collection: &str,
    volume_id: VolumeId,
    deleted: &HashSet<NeedleId>,
) -> io::Result<bool> {
    let ecx_base = volume_file_name(ecx_dir, collection, volume_id);
    let ecx_data = std::fs::read(format!("{}.ecx", ecx_base))?;
    let (entries, _) = ecx_data.as_chunks::<NEEDLE_MAP_ENTRY_SIZE>();
    Ok(entries.iter().any(|entry| {
        let (key, _, size) = idx_entry_from_bytes(entry);
        !size.is_deleted() && !deleted.contains(&key)
    }))
}

/// Distinct needle ids journaled in the `.ecj` of any of `dirs`. Go folds the
/// journal into the `.ecx` (RebuildEcxFile) before a decode; reading it leaves
/// the sealed index untouched. Only NotFound means "no journal".
pub fn read_ecj_deletions(
    dirs: &[&str],
    collection: &str,
    volume_id: VolumeId,
) -> io::Result<HashSet<NeedleId>> {
    let mut ids = HashSet::new();
    for (i, dir) in dirs.iter().enumerate() {
        if dirs[..i].contains(dir) {
            continue;
        }
        let path = format!("{}.ecj", volume_file_name(dir, collection, volume_id));
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        let len = file.metadata()?.len();
        read_ecj_ids(&file, len, &mut ids)?;
    }
    Ok(ids)
}

/// What it takes to rebuild a volume's .dat from its EC data shards.
///
/// Mirrors Go's `WriteDatFile(baseFileName, datFileSize,
/// encodedDatFileSize, shardFileNames)` shape — Go passes per-shard
/// paths so a reconciled volume with shards split across disks of the
/// same volume server can still be decoded back to a regular .dat
/// (seaweedfs/seaweedfs#9252).
#[derive(Clone, Copy, Debug)]
pub struct DatRebuild<'a> {
    /// Where the produced `.dat` is written.
    pub dat_dir: &'a str,
    pub collection: &'a str,
    pub volume_id: VolumeId,
    /// The number of bytes to write, i.e. the live data extent from
    /// [`find_dat_file_size`].
    pub dat_file_size: i64,
    /// The .dat size at encode time, which fixed the shard block layout:
    /// deletions can move the live extent below the large-block row
    /// boundary, and deriving the layout from the shrunk extent would read
    /// the shards in the wrong block order. Zero when the .vif does not
    /// record the encode-time size; the layout is then inferred from the
    /// shard size.
    pub encoded_dat_file_size: i64,
    pub data_shards: usize,
    /// `shard_dirs[i]` is the directory holding shard `i`. `None` means every
    /// data shard sits in `dat_dir`.
    pub shard_dirs: Option<&'a [String]>,
    /// The volume's shard block layout, e.g. `EcVolume::large_block_size()`
    /// / `small_block_size()` from its .vif EC config.
    pub large_block_size: usize,
    pub small_block_size: usize,
}

/// Reconstruct a .dat file from EC data shards.
///
/// Reads from .ec00-.ec09 and writes a new .dat file, from one directory or
/// from the per-shard directories of a cross-disk reconciled volume.
pub fn write_dat_file_from_shards(spec: &DatRebuild<'_>) -> io::Result<()> {
    let DatRebuild {
        dat_dir,
        collection,
        volume_id,
        dat_file_size,
        encoded_dat_file_size,
        data_shards,
        shard_dirs,
        large_block_size,
        small_block_size,
    } = *spec;
    let same_dir: Vec<String>;
    let shard_dirs: &[String] = match shard_dirs {
        Some(dirs) => dirs,
        None => {
            same_dir = vec![dat_dir.to_string(); data_shards];
            &same_dir
        }
    };
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

        let mut encoded_dat_file_size = encoded_dat_file_size;
        if encoded_dat_file_size <= 0 {
            // .vif without the encode-time size: infer the padded layout from
            // the physical shard size, which reads the shards in the same
            // block order.
            let shard_size = std::fs::metadata(shards[0].file_name())?.len() as i64;
            // A shard size that is an exact multiple of the large block size
            // is ambiguous: N large rows, or N-1 large rows plus a full
            // small-block region. The two layouts only agree below the last
            // large row.
            let large = large_block_size as i64;
            if shard_size % large == 0
                && dat_file_size > (shard_size / large - 1) * large * data_shards as i64
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "shard size {} does not identify the block layout; re-encode to record the dat size in .vif",
                        shard_size
                    ),
                ));
            }
            encoded_dat_file_size = data_shards as i64 * shard_size;
        }
        if dat_file_size > encoded_dat_file_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "dat file size {} exceeds encoded dat file size {}",
                    dat_file_size, encoded_dat_file_size
                ),
            ));
        }

        let mut dat_file = File::create(&tmp_path)?;
        let mut remaining = dat_file_size;
        let mut encoded_remaining = encoded_dat_file_size;
        let large_row_size = (large_block_size * data_shards) as i64;

        let mut shard_offset: u64 = 0;

        // Read large blocks
        while encoded_remaining >= large_row_size && remaining > 0 {
            for (i, shard) in shards[..data_shards].iter().enumerate() {
                let to_write = large_block_size.min(remaining as usize);
                let mut buf = vec![0u8; to_write];
                let n = shard.read_at(&mut buf, shard_offset)?;
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
            for (i, shard) in shards[..data_shards].iter().enumerate() {
                let to_write = small_block_size.min(remaining as usize);
                let mut buf = vec![0u8; to_write];
                let n = shard.read_at(&mut buf, shard_offset)?;
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
/// See [`write_idx_file_from_ec_index_with_dirs`]; everything lives in `dir`.
pub fn write_idx_file_from_ec_index(
    dir: &str,
    collection: &str,
    volume_id: VolumeId,
) -> io::Result<()> {
    let deleted = read_ecj_deletions(&[dir], collection, volume_id)?;
    let dat_file_size = find_dat_file_size_with_dirs(dir, dir, collection, volume_id, &deleted)?;
    write_idx_file_from_ec_index_with_dirs(dir, dir, collection, volume_id, &deleted, dat_file_size)
}

/// Write the `.idx` for a `.dat` decoded to `dat_file_size` bytes, from the
/// `.ecx` in `ecx_dir`, into `idx_dir`.
///
/// Copies the `.ecx` rows, then appends one tombstone per row whose needle is
/// in `deleted`. A deleted needle at or past `dat_file_size` was cut from the
/// `.dat`, so its row is dropped: a row pointing past the end of the `.dat`
/// makes the volume load read-only.
pub fn write_idx_file_from_ec_index_with_dirs(
    ecx_dir: &str,
    idx_dir: &str,
    collection: &str,
    volume_id: VolumeId,
    deleted: &HashSet<NeedleId>,
    dat_file_size: i64,
) -> io::Result<()> {
    let ecx_path = format!("{}.ecx", volume_file_name(ecx_dir, collection, volume_id));
    let idx_path = format!("{}.idx", volume_file_name(idx_dir, collection, volume_id));
    // Write to a temp file and atomically rename into place, so a crash
    // mid-write never leaves a partial .idx at the final name beside the
    // source shards.
    let tmp_path = format!("{}.tmp", idx_path);

    let write_result = (|| -> io::Result<()> {
        let mut ecx_file = File::open(&ecx_path)?;
        let mut idx_file = io::BufWriter::new(File::create(&tmp_path)?);
        let mut tombstoned = Vec::new();
        idx::walk_index_file(&mut ecx_file, 0, |key, offset, size| {
            let is_deleted = size.is_deleted() || deleted.contains(&key);
            if is_deleted && offset.to_actual_offset() >= dat_file_size {
                return Ok(());
            }
            idx::write_index_entry(&mut idx_file, key, offset, size)?;
            if !size.is_deleted() && deleted.contains(&key) {
                tombstoned.push(key);
            }
            Ok(())
        })?;
        for key in tombstoned {
            idx::write_index_entry(&mut idx_file, key, Offset::default(), TOMBSTONE_FILE_SIZE)?;
        }

        // fsync, rename, then fsync the dir so the decoded .idx is durable and
        // atomically published before the caller deletes the source shards.
        let idx_file = idx_file.into_inner().map_err(|e| e.into_error())?;
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
    use crate::storage::volume::{Volume, VolumeSpec};
    use tempfile::TempDir;

    #[test]
    fn test_ec_full_round_trip() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        // Create volume with data
        let mut v = Volume::new(
            dir,
            dir,
            VolumeId(1),
            NeedleMapKind::InMemory,
            &VolumeSpec::default(),
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
            v.write_needle(&mut n, true, false).unwrap();
        }
        v.sync_to_disk().unwrap();
        let original_dat_size = v.dat_file_size().unwrap();
        v.close();

        // Read original .dat for comparison
        let original_dat = std::fs::read(format!("{}/1.dat", dir)).unwrap();

        // Encode to EC
        let data_shards = 10;
        let parity_shards = 4;
        let block_size =
            ec_encoder::write_ec_files(dir, dir, "", VolumeId(1), data_shards, parity_shards)
                .unwrap();

        // Delete original .dat and .idx
        std::fs::remove_file(format!("{}/1.dat", dir)).unwrap();
        std::fs::remove_file(format!("{}/1.idx", dir)).unwrap();

        // Reconstruct from EC shards
        write_dat_file_from_shards(&DatRebuild {
            dat_dir: dir,
            collection: "",
            volume_id: VolumeId(1),
            dat_file_size: original_dat_size as i64,
            encoded_dat_file_size: original_dat_size as i64,
            data_shards,
            shard_dirs: None,
            large_block_size: block_size as usize,
            small_block_size: block_size as usize,
        })
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
            VolumeId(1),
            NeedleMapKind::InMemory,
            &VolumeSpec::default(),
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
        let res = write_dat_file_from_shards(&DatRebuild {
            dat_dir: dir,
            collection: "",
            volume_id: VolumeId(7),
            dat_file_size: 100,
            encoded_dat_file_size: 100,
            data_shards: 10,
            shard_dirs: None,
            large_block_size: ERASURE_CODING_LARGE_BLOCK_SIZE,
            small_block_size: ERASURE_CODING_SMALL_BLOCK_SIZE,
        });
        assert!(res.is_err());
        assert!(!std::path::Path::new(&format!("{}/7.dat", dir)).exists());
        assert!(!std::path::Path::new(&format!("{}/7.dat.tmp", dir)).exists());
    }

    // Decoding when .vif does not record the encode-time size: the layout is
    // inferred from the shard size, except when that is an exact large-block
    // multiple and the live extent reaches the ambiguous region.
    #[test]
    fn test_write_dat_file_fallback_layout() {
        use crate::storage::erasure_coding::ec_bitrot::{
            DEFAULT_BITROT_BLOCK_SIZE, ShardChecksumBuilder,
        };
        use reed_solomon_erasure::galois_8::ReedSolomon;

        const LARGE: usize = 10000;
        const SMALL: usize = 100;
        let data_shards = 10usize;
        let parity_shards = 4usize;
        let large_row_size = (LARGE * data_shards) as i64;

        let tmp = TempDir::new().unwrap();

        let encode = |name: &str, dat_size: i64| -> (String, Vec<String>, Vec<u8>) {
            let dir = tmp.path().join(name);
            std::fs::create_dir(&dir).unwrap();
            let dir = dir.to_str().unwrap().to_string();
            let original: Vec<u8> = (0..dat_size as usize)
                .map(|i| ((i as u64).wrapping_mul(2654435761) >> 8) as u8)
                .collect();
            std::fs::write(format!("{}/1.dat", dir), &original).unwrap();
            let dat_file = File::open(format!("{}/1.dat", dir)).unwrap();
            let rs = ReedSolomon::new(data_shards, parity_shards).unwrap();
            let total = data_shards + parity_shards;
            let mut shards: Vec<EcVolumeShard> = (0..total as u8)
                .map(|i| EcVolumeShard::new(&dir, "", VolumeId(1), i))
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
                ec_encoder::EcEncodeLayout {
                    data_shards,
                    parity_shards,
                    buffer_size: SMALL,
                    large_block_size: LARGE,
                    small_block_size: SMALL,
                },
            )
            .unwrap();
            for shard in &mut shards {
                shard.close();
            }
            let shard_dirs: Vec<String> = (0..data_shards).map(|_| dir.clone()).collect();
            (dir, shard_dirs, original)
        };

        let decode_to = |dir: &str,
                         sub: &str,
                         live: i64,
                         encoded: i64,
                         shard_dirs: &[String]|
         -> io::Result<Vec<u8>> {
            let out = format!("{}/{}", dir, sub);
            std::fs::create_dir_all(&out).unwrap();
            write_dat_file_from_shards(&DatRebuild {
                dat_dir: &out,
                collection: "",
                volume_id: VolumeId(1),
                dat_file_size: live,
                encoded_dat_file_size: encoded,
                data_shards: 10,
                shard_dirs: Some(shard_dirs),
                large_block_size: LARGE,
                small_block_size: SMALL,
            })?;
            Ok(std::fs::read(format!("{}/1.dat", out)).unwrap())
        };

        // a small-block tail that is not a large-block multiple is unambiguous
        let (dir, shard_dirs, original) = encode("plain", large_row_size + 2530);
        let live = large_row_size / 2;
        let decoded = decode_to(&dir, "out", live, 0, &shard_dirs).unwrap();
        assert_eq!(&original[..live as usize], &decoded[..]);

        // datSize just under one large row: the full small-block region makes
        // each shard exactly one large block, indistinguishable from one large row
        let (dir, shard_dirs, _) = encode("ambig1", large_row_size - 1);
        let err = decode_to(&dir, "out", large_row_size / 2, 0, &shard_dirs).unwrap_err();
        assert!(
            err.to_string()
                .contains("does not identify the block layout")
        );

        // two-row equivalent: decoding within the agreed prefix still works
        let (dir, shard_dirs, original) = encode("ambig2", 2 * large_row_size - 1);
        let decoded = decode_to(&dir, "outa", large_row_size, 0, &shard_dirs).unwrap();
        assert_eq!(&original[..large_row_size as usize], &decoded[..]);
        let err = decode_to(&dir, "outb", large_row_size + 1, 0, &shard_dirs).unwrap_err();
        assert!(
            err.to_string()
                .contains("does not identify the block layout")
        );
    }

    // Decoding after deletions moved the live extent below the large-block row
    // boundary: the shard block layout is fixed by the encode-time .dat size,
    // so de-striping must not derive it from the shrunk live extent.
    #[test]
    fn test_write_dat_file_after_tail_deletion() {
        use crate::storage::erasure_coding::ec_bitrot::{
            DEFAULT_BITROT_BLOCK_SIZE, ShardChecksumBuilder,
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
            ec_encoder::EcEncodeLayout {
                data_shards,
                parity_shards,
                buffer_size: SMALL,
                large_block_size: LARGE,
                small_block_size: SMALL,
            },
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
            write_dat_file_from_shards(&DatRebuild {
                dat_dir: out,
                collection: "",
                volume_id: VolumeId(1),
                dat_file_size: live_size,
                encoded_dat_file_size: encoded_size,
                data_shards,
                shard_dirs: Some(&shard_dirs),
                large_block_size: LARGE,
                small_block_size: SMALL,
            })
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
        assert!(
            write_dat_file_from_shards(&DatRebuild {
                dat_dir: out,
                collection: "",
                volume_id: VolumeId(1),
                dat_file_size: dat_size + 1,
                encoded_dat_file_size: dat_size,
                data_shards,
                shard_dirs: Some(&shard_dirs),
                large_block_size: LARGE,
                small_block_size: SMALL,
            })
            .is_err()
        );
    }

    /// A journal many chunks long that repeats a few ids reads back as those
    /// ids, from each dir once, whatever its length.
    #[test]
    fn test_read_ecj_deletions_collects_distinct_ids_across_dirs() {
        let tmp = TempDir::new().unwrap();
        let data = tmp.path().join("data");
        let idx = tmp.path().join("idx");
        let missing = tmp.path().join("missing");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::create_dir_all(&idx).unwrap();
        let (data, idx, missing) = (
            data.to_str().unwrap(),
            idx.to_str().unwrap(),
            missing.to_str().unwrap(),
        );

        let entry = |id: u64| {
            let mut buf = [0u8; NEEDLE_ID_SIZE];
            NeedleId(id).to_bytes(&mut buf);
            buf
        };
        // Past two load chunks of three repeating ids, then an id only in the
        // last chunk and a torn trailing record.
        let mut ecj = Vec::new();
        while ecj.len() <= 2 * (1 << 20) {
            for id in [1, 2, 3] {
                ecj.extend_from_slice(&entry(id));
            }
        }
        ecj.extend_from_slice(&entry(7));
        ecj.extend_from_slice(&entry(8)[..3]);
        std::fs::write(format!("{idx}/1.ecj"), &ecj).unwrap();
        std::fs::write(format!("{data}/1.ecj"), entry(9)).unwrap();

        let ids = read_ecj_deletions(&[data, idx, idx, missing], "", VolumeId(1)).unwrap();
        let expected: HashSet<NeedleId> = [1, 2, 3, 7, 9].into_iter().map(NeedleId).collect();
        assert_eq!(ids, expected);
    }
}
