//! Rebuild a missing .idx from the .dat it indexes. Mirrors
//! `weed/storage/volume_idx_rebuild.go`.

use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::storage::idx;
use crate::storage::needle::Needle;
use crate::storage::super_block::SuperBlock;
use crate::storage::types::*;
use crate::storage::volume::{
    fsync_dir, needle_disk_end, scan_volume_file, Volume, VolumeError, VolumeFileVisitor,
};

/// Writes one .idx row per .dat record, in .dat append order, which is the
/// shape the volume server's own writes leave behind.
struct VolumeFileScanner4RebuildIdx<W: Write> {
    writer: W,
    dat_size: i64,
    version: Version,
    stopped: bool,
}

impl<W: Write> VolumeFileVisitor for VolumeFileScanner4RebuildIdx<W> {
    fn visit_super_block(&mut self, _sb: &SuperBlock) -> Result<(), VolumeError> {
        Ok(())
    }

    fn read_needle_body(&self) -> bool {
        false
    }

    fn visit_needle(&mut self, n: &Needle, offset: i64) -> Result<(), VolumeError> {
        // A record reaching past the end of .dat is a torn append or a corrupt
        // header: nothing beyond it is indexable, and a row pointing past EOF
        // would fail every read of that needle. The all-zero header case ends
        // the walk upstream.
        if self.stopped {
            return Ok(());
        }
        if needle_disk_end(Offset::from_actual_offset(offset), n.size, self.version) > self.dat_size
        {
            self.stopped = true;
            return Ok(());
        }
        let size = if n.size.is_valid() {
            n.size
        } else {
            TOMBSTONE_FILE_SIZE
        };
        idx::write_index_entry(
            &mut self.writer,
            n.id,
            Offset::from_actual_offset(offset),
            size,
        )?;
        Ok(())
    }
}

impl Volume {
    /// Regenerate the volume's .idx from its .dat. The whole index is derivable
    /// from the data file, so a volume whose index directory has no .idx -- a
    /// --dir.idx pointed at an empty directory, or a lost index -- comes back on
    /// its own instead of mounting with every needle invisible. The rows go to a
    /// temp file that is renamed in, so an interrupted rebuild leaves no partial
    /// index behind.
    pub(crate) fn rebuild_idx_file(&self) -> Result<(), VolumeError> {
        let idx_path = self.file_name(".idx");
        let dat_path = self.file_name(".dat");
        let tmp_path = format!("{idx_path}.tmp");

        let rebuild = || -> Result<(), VolumeError> {
            if let Some(parent) = Path::new(&idx_path).parent() {
                fs::create_dir_all(parent)?;
            }
            let tmp_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            let mut scanner = VolumeFileScanner4RebuildIdx {
                writer: BufWriter::new(&tmp_file),
                dat_size: fs::metadata(&dat_path)?.len() as i64,
                version: self.version(),
                stopped: false,
            };
            scan_volume_file(&dat_path, &mut scanner)?;
            scanner.writer.flush()?;
            drop(scanner);
            tmp_file.sync_all()?;
            fs::rename(&tmp_path, &idx_path)?;
            fsync_dir(&idx_path)?;
            Ok(())
        };

        let result = rebuild();
        if result.is_err() {
            let _ = fs::remove_file(&tmp_path);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::needle::crc::CRC;
    use crate::storage::needle::Needle;
    use crate::storage::needle_map::NeedleMapKind;
    use crate::storage::types::*;
    use crate::storage::volume::Volume;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    fn needle(id: u64) -> Needle {
        let data = format!("payload-{id}").into_bytes();
        Needle {
            id: NeedleId(id),
            cookie: Cookie(0x55),
            data_size: data.len() as u32,
            checksum: CRC::new(&data),
            data,
            ..Needle::default()
        }
    }

    // Pointing --dir.idx at a directory with no .idx used to mount the volume on
    // an empty index; the index is derivable from the .dat, so it must be
    // rebuilt in place instead.
    #[test]
    fn test_load_moved_idx_directory_rebuilds_idx() {
        let root = TempDir::new().unwrap();
        let data_dir = root.path().join("data");
        let old_idx_dir = root.path().join("idxA");
        let new_idx_dir = root.path().join("idxB");
        for dir in [&data_dir, &old_idx_dir, &new_idx_dir] {
            fs::create_dir_all(dir).unwrap();
        }
        let data = data_dir.to_str().unwrap();
        let old_idx = old_idx_dir.to_str().unwrap();
        let new_idx = new_idx_dir.to_str().unwrap();

        let mut v = Volume::new(
            data,
            old_idx,
            "",
            VolumeId(1),
            NeedleMapKind::InMemory,
            None,
            None,
            0,
            Version::current(),
        )
        .unwrap();
        for id in 1..=3 {
            v.write_needle(&mut needle(id), true, false).unwrap();
        }
        v.delete_needle(&mut needle(2)).unwrap();
        v.sync_to_disk().unwrap();
        let (want_count, want_deleted) = (v.file_count(), v.deleted_count());
        drop(v);

        let seeded = fs::read(format!("{old_idx}/1.idx")).unwrap();

        let reopened = Volume::new(
            data,
            new_idx,
            "",
            VolumeId(1),
            NeedleMapKind::InMemory,
            None,
            None,
            0,
            Version::current(),
        )
        .unwrap();

        assert!(
            Path::new(&format!("{new_idx}/1.idx")).exists(),
            "idx not rebuilt in the new idx dir"
        );
        assert_eq!(reopened.file_count(), want_count);
        assert_eq!(reopened.deleted_count(), want_deleted);
        assert_eq!(
            fs::read(format!("{new_idx}/1.idx")).unwrap(),
            seeded,
            "rebuilt idx differs from the one the server wrote"
        );

        for id in [1, 3] {
            let mut got = needle(id);
            got.data.clear();
            reopened.read_needle(&mut got).unwrap();
            assert_eq!(got.data, format!("payload-{id}").into_bytes());
        }
    }

    // A .dat padded with zeros must not be indexed as needle 0 rows: the walk
    // stops where the records do.
    #[test]
    fn test_rebuild_idx_stops_at_zero_padded_dat_tail() {
        let root = TempDir::new().unwrap();
        let dir = root.path().to_str().unwrap();

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
        v.write_needle(&mut needle(1), true, false).unwrap();
        v.sync_to_disk().unwrap();
        drop(v);

        let seeded = fs::read(format!("{dir}/1.idx")).unwrap();
        let dat = fs::OpenOptions::new()
            .write(true)
            .open(format!("{dir}/1.dat"))
            .unwrap();
        let dat_size = dat.metadata().unwrap().len();
        dat.set_len(dat_size + 4096).unwrap();
        drop(dat);
        fs::remove_file(format!("{dir}/1.idx")).unwrap();

        let reopened = Volume::new(
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
        drop(reopened);

        assert_eq!(
            fs::read(format!("{dir}/1.idx")).unwrap(),
            seeded,
            "the zero-padded tail leaked into the rebuilt idx"
        );
    }

    // A .dat whose last append was torn mid-body must not gain an index row
    // that points past the end of the file.
    #[test]
    fn test_rebuild_idx_skips_truncated_dat_tail() {
        let root = TempDir::new().unwrap();
        let dir = root.path().to_str().unwrap();

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
        v.write_needle(&mut needle(1), true, false).unwrap();
        v.sync_to_disk().unwrap();
        let kept = fs::read(format!("{dir}/1.idx")).unwrap();
        v.write_needle(&mut needle(2), true, false).unwrap();
        v.sync_to_disk().unwrap();
        drop(v);

        // Chop the second needle's body, leaving its header intact.
        let dat = fs::OpenOptions::new()
            .write(true)
            .open(format!("{dir}/1.dat"))
            .unwrap();
        let torn_size = dat.metadata().unwrap().len() - 8;
        dat.set_len(torn_size).unwrap();
        drop(dat);
        fs::remove_file(format!("{dir}/1.idx")).unwrap();

        let reopened = Volume::new(
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
        drop(reopened);

        assert_eq!(
            fs::read(format!("{dir}/1.idx")).unwrap(),
            kept,
            "the torn record leaked into the rebuilt idx"
        );
    }

    // A corrupt header carrying a negative size advances the .dat walk
    // backwards, which cycles forever between it and the record before it.
    #[test]
    fn test_rebuild_idx_stops_at_negative_size_header() {
        let root = TempDir::new().unwrap();
        let dir = root.path().to_str().unwrap();

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
        v.write_needle(&mut needle(1), true, false).unwrap();
        v.sync_to_disk().unwrap();
        let kept = fs::read(format!("{dir}/1.idx")).unwrap();
        v.write_needle(&mut needle(2), true, false).unwrap();
        v.sync_to_disk().unwrap();
        drop(v);

        let rows = fs::read(format!("{dir}/1.idx")).unwrap();
        let (_, offset, _) = idx_entry_from_bytes(&rows[NEEDLE_MAP_ENTRY_SIZE..]);
        let corrupt_at = offset.to_actual_offset();

        // Overwrite the second needle's size field with a negative i32.
        use std::io::{Seek, SeekFrom, Write};
        let mut dat = fs::OpenOptions::new()
            .write(true)
            .open(format!("{dir}/1.dat"))
            .unwrap();
        dat.seek(SeekFrom::Start(
            corrupt_at as u64 + COOKIE_SIZE as u64 + NEEDLE_ID_SIZE as u64,
        ))
        .unwrap();
        dat.write_all(&[0xff, 0xff, 0xf0, 0x00]).unwrap();
        drop(dat);
        fs::remove_file(format!("{dir}/1.idx")).unwrap();

        // Pre-fix the rebuild walked backwards from here and never terminated.
        let reopened = Volume::new(
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
        drop(reopened);

        assert_eq!(
            fs::read(format!("{dir}/1.idx")).unwrap(),
            kept,
            "the corrupt record leaked into the rebuilt idx"
        );
    }
}
