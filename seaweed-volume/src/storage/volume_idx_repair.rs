//! Recovery for .idx rows that deletes on a tiered read-only volume
//! overwrote. Mirrors `weed/storage/volume_idx_repair.go`.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Write};
use std::path::Path;

use tracing::info;

use crate::storage::idx;
use crate::storage::needle::needle::needle_body_length;
use crate::storage::needle::Needle;
use crate::storage::types::*;
use crate::storage::volume::{fsync_dir, Volume, VolumeError};

/// Needles found in the head of .dat, keyed by id, plus the ids in .dat order.
type DatHeadNeedles = (HashMap<NeedleId, (Offset, Size)>, Vec<NeedleId>);

impl Volume {
    /// Restore the .idx rows that deletes on a tiered read-only volume used to
    /// overwrite.
    ///
    /// The sorted-file needle map opened .idx read-write but never seeded its
    /// write position, so a delete wrote its (key, offset 0, tombstone) row at
    /// .idx offset 0 and advanced one row at a time instead of appending. Every
    /// delete therefore replaced one more row at the front of .idx, and the rows
    /// it replaced -- the Put rows indexing the first needles in .dat -- were
    /// lost. Reads for those needles return not-found even though .dat still
    /// holds them intact.
    ///
    /// The damage leaves a fingerprint: .idx begins with a run of offset-0
    /// tombstones. A healthy .idx never does. Its first row is the Put for the
    /// first needle in .dat, and an offset-0 tombstone -- a delete against a
    /// tiered volume, which appends no .dat record and so has no extent to point
    /// at -- can only ever land at the tail.
    ///
    /// .idx and .dat grow in lockstep, so the clobbered rows indexed exactly the
    /// first N records of .dat. Re-deriving them is a header-only walk over the
    /// head of .dat, which stays cheap even when .dat is served from a remote
    /// tier.
    ///
    /// The recovered rows go back in front, where the rows they replace used to
    /// sit, and every existing row keeps its relative order behind them. That
    /// restores .idx to .dat append order, which several readers lean on: the
    /// fingerprint is gone so a later load stops after one row, and the last row
    /// is the .dat-tail needle again.
    pub(crate) fn repair_idx_head_tombstones(&self) -> Result<usize, VolumeError> {
        if !self.has_data_backend() {
            return Ok(0);
        }
        let version = self.version();
        if !version.is_supported() {
            return Ok(0);
        }
        let first_needle_offset = self.super_block.block_size() as i64;
        let idx_path = self.file_name(".idx");

        let clobbered = idx_head_tombstone_count(&idx_path)?;
        if clobbered == 0 {
            return Ok(0);
        }

        info!(
            volume_id = self.id.0,
            idx = %idx_path,
            clobbered,
            "idx starts with offset-0 tombstones, recovering the rows they overwrote from .dat"
        );

        let (mut lost, order) = self.scan_dat_head(version, first_needle_offset, clobbered)?;
        drop_indexed_keys(&idx_path, &mut lost)?;
        if lost.is_empty() {
            return Ok(0);
        }

        let mut rows = Vec::with_capacity(lost.len() * NEEDLE_MAP_ENTRY_SIZE);
        let mut restored = 0;
        for key in order {
            let Some((offset, size)) = lost.get(&key).copied() else {
                continue;
            };
            idx::write_index_entry(&mut rows, key, offset, size)?;
            restored += 1;
        }
        prepend_idx_rows(&idx_path, &rows)?;
        Ok(restored)
    }

    /// Read the headers of the first `limit` records of .dat and return the
    /// needles they hold, in .dat order. A record with an invalid size is a
    /// delete marker, so the key it names drops out of the result rather than
    /// being resurrected.
    fn scan_dat_head(
        &self,
        version: Version,
        first_needle_offset: i64,
        limit: usize,
    ) -> Result<DatHeadNeedles, VolumeError> {
        let mut found: HashMap<NeedleId, (Offset, Size)> = HashMap::new();
        let mut order: Vec<NeedleId> = Vec::new();
        let mut offset = first_needle_offset;

        for _ in 0..limit {
            let mut header = [0u8; NEEDLE_HEADER_SIZE];
            match self.read_exact_at_backend(&mut header, offset as u64) {
                Ok(()) => {}
                Err(VolumeError::Io(ref e)) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let (_cookie, id, size) = Needle::parse_header(&header);
            if size.0 == 0 && id.is_empty() {
                break;
            }
            if size.is_valid() {
                if found
                    .insert(id, (Offset::from_actual_offset(offset), size))
                    .is_none()
                {
                    order.push(id);
                }
            } else {
                found.remove(&id);
            }
            offset += NEEDLE_HEADER_SIZE as i64 + needle_body_length(size, version);
        }

        Ok((found, order))
    }
}

/// Report how many rows at the front of .idx are offset-0 tombstones. A healthy
/// .idx answers 0 on its first row.
fn idx_head_tombstone_count(idx_path: &str) -> Result<usize, VolumeError> {
    if !Path::new(idx_path).exists() {
        return Ok(0);
    }
    let mut reader = BufReader::new(File::open(idx_path)?);

    let mut clobbered = 0usize;
    let walk = idx::walk_index_file(&mut reader, 0, |_key, offset, size| {
        if !offset.is_zero() || !size.is_tombstone() {
            return Err(stop_walk());
        }
        clobbered += 1;
        Ok(())
    });
    finish_walk(walk)?;
    Ok(clobbered)
}

/// Remove every candidate the .idx already names, whether by a Put row or a
/// tombstone. What is left is only what the clobbered rows held.
fn drop_indexed_keys(
    idx_path: &str,
    candidates: &mut HashMap<NeedleId, (Offset, Size)>,
) -> Result<(), VolumeError> {
    if candidates.is_empty() {
        return Ok(());
    }
    let mut reader = BufReader::new(File::open(idx_path)?);
    let walk = idx::walk_index_file(&mut reader, 0, |key, _offset, _size| {
        candidates.remove(&key);
        if candidates.is_empty() {
            return Err(stop_walk());
        }
        Ok(())
    });
    finish_walk(walk)
}

/// Rewrite .idx as `rows` followed by its current contents, through a temp file
/// and a rename so a crash mid-write never leaves a partial index at the live
/// name.
fn prepend_idx_rows(idx_path: &str, rows: &[u8]) -> Result<(), VolumeError> {
    let tmp_path = format!("{}.tmp", idx_path);
    let mut src = File::open(idx_path)?;
    let commit = (|| -> io::Result<()> {
        let mut dst = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        dst.write_all(rows)?;
        io::copy(&mut src, &mut dst)?;
        dst.sync_all()?;
        drop(dst);
        fs::rename(&tmp_path, idx_path)?;
        fsync_dir(idx_path)
    })();
    if commit.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    Ok(commit?)
}

/// Sentinel that ends an `idx::walk_index_file` early once the answer is known.
fn stop_walk() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "stop idx walk")
}

fn finish_walk(walk: io::Result<()>) -> Result<(), VolumeError> {
    match walk {
        Err(ref e) if e.kind() == io::ErrorKind::Interrupted => Ok(()),
        other => Ok(other?),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::needle::crc::CRC;
    use crate::storage::needle_map::NeedleMapKind;
    use std::os::unix::fs::FileExt;
    use tempfile::TempDir;

    fn open_volume(dir: &str) -> Volume {
        Volume::new(
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
        .unwrap()
    }

    fn write_test_volume(dir: &str, needle_count: u64) -> Vec<Vec<u8>> {
        let mut v = open_volume(dir);
        let mut written = Vec::new();
        for i in 1..=needle_count {
            let data = format!("needle-{}-payload", i).into_bytes();
            let mut n = Needle {
                id: NeedleId(i),
                cookie: Cookie(0x1234_0000 + i as u32),
                data_size: data.len() as u32,
                checksum: CRC::new(&data),
                data: data.clone(),
                ..Needle::default()
            };
            v.write_needle(&mut n, true).unwrap();
            written.push(data);
        }
        written
    }

    /// Reproduce the damage a delete on a tiered read-only volume used to do: it
    /// writes (key, offset 0, tombstone) rows over the front of .idx instead of
    /// appending them.
    fn clobber_idx_head(idx_path: &str, keys: &[u64]) {
        let file = OpenOptions::new().write(true).open(idx_path).unwrap();
        for (i, key) in keys.iter().enumerate() {
            let mut row = Vec::new();
            idx::write_index_entry(
                &mut row,
                NeedleId(*key),
                Offset::from_actual_offset(0),
                TOMBSTONE_FILE_SIZE,
            )
            .unwrap();
            file.write_at(&row, (i * NEEDLE_MAP_ENTRY_SIZE) as u64)
                .unwrap();
        }
    }

    fn idx_size(idx_path: &str) -> u64 {
        std::fs::metadata(idx_path).unwrap().len()
    }

    fn idx_rows(idx_path: &str) -> Vec<(NeedleId, i64, Size)> {
        let mut reader = BufReader::new(File::open(idx_path).unwrap());
        let mut rows = Vec::new();
        idx::walk_index_file(&mut reader, 0, |key, offset, size| {
            rows.push((key, offset.to_actual_offset(), size));
            Ok(())
        })
        .unwrap();
        rows
    }

    fn read_needle_data(v: &Volume, id: u64) -> Result<Vec<u8>, VolumeError> {
        let mut n = Needle {
            id: NeedleId(id),
            ..Needle::default()
        };
        v.read_needle(&mut n)?;
        Ok(n.data)
    }

    #[test]
    fn test_repair_idx_head_tombstones_restores_clobbered_rows() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let idx_path = format!("{}/1.idx", dir);

        let written = write_test_volume(dir, 12);
        let size_before = idx_size(&idx_path);

        // Deletes against needles 9..12 land on the front of .idx and take the
        // rows indexing needles 1..4 with them.
        clobber_idx_head(&idx_path, &[9, 10, 11, 12]);

        let v = open_volume(dir);
        for i in 1..=8u64 {
            assert_eq!(
                read_needle_data(&v, i).unwrap(),
                written[(i - 1) as usize],
                "needle {} not recovered",
                i
            );
        }

        let want = size_before + 4 * NEEDLE_MAP_ENTRY_SIZE as u64;
        assert_eq!(idx_size(&idx_path), want, "idx size after recovery");

        // The recovered rows go back in front, so .idx is in .dat append order
        // again: the fingerprint is gone and the last row is still the .dat tail.
        let rows = idx_rows(&idx_path);
        assert_eq!(
            rows[0].1,
            v.super_block.block_size() as i64,
            "first row does not index the first needle in .dat"
        );
        for i in 1..4 {
            assert!(
                rows[i - 1].1 < rows[i].1,
                "recovered rows are not in .dat order: {:?} then {:?}",
                rows[i - 1],
                rows[i]
            );
        }

        // The recovery is idempotent: a second load finds nothing left to restore.
        drop(v);
        let _v = open_volume(dir);
        assert_eq!(idx_size(&idx_path), want, "idx grew on second load");
    }

    #[test]
    fn test_repair_idx_head_tombstones_leaves_healthy_idx_alone() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let idx_path = format!("{}/1.idx", dir);

        write_test_volume(dir, 6);
        let mut v = open_volume(dir);
        v.delete_needle(&mut Needle {
            id: NeedleId(3),
            ..Needle::default()
        })
        .unwrap();
        drop(v);

        let size_before = idx_size(&idx_path);
        let v = open_volume(dir);

        assert_eq!(
            idx_size(&idx_path),
            size_before,
            "healthy idx was rewritten"
        );
        assert!(
            matches!(
                read_needle_data(&v, 3),
                Err(VolumeError::Deleted) | Err(VolumeError::NotFound)
            ),
            "needle 3 should stay deleted"
        );
    }

    /// A needle deleted before the damage must not be resurrected: its tombstone
    /// row survives at the tail of .idx, so the key is still indexed and stays
    /// out of the recovery.
    #[test]
    fn test_repair_idx_head_tombstones_keeps_deleted_needles_deleted() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let idx_path = format!("{}/1.idx", dir);

        write_test_volume(dir, 8);
        let mut v = open_volume(dir);
        v.delete_needle(&mut Needle {
            id: NeedleId(2),
            ..Needle::default()
        })
        .unwrap();
        drop(v);

        clobber_idx_head(&idx_path, &[7, 8]);

        let v = open_volume(dir);
        assert!(
            matches!(
                read_needle_data(&v, 2),
                Err(VolumeError::Deleted) | Err(VolumeError::NotFound)
            ),
            "needle 2 was resurrected"
        );
        assert!(
            read_needle_data(&v, 1).is_ok(),
            "needle 1 should have been recovered"
        );
    }
}
