//! Repair of the .dat tail a crash between the needle append and the .idx
//! append leaves. Mirrors `repairUnindexedDatTail` and its helpers in
//! weed/storage/volume_checking.go (#11864).
//!
//! A write appends the needle to .dat, then its entry to .idx, then answers
//! the client. A process killed between the first two steps leaves a needle,
//! or part of one, past the end of the needle at the highest indexed offset.
//! After a power loss a complete needle can survive whose .idx entry did
//! not, although the client was answered. So a complete needle is never
//! dropped: every complete needle in the tail gets the .idx entry the volume
//! server would have written, in .dat order, the .dat synced first so the
//! rows never become durable ahead of the records they name, and only an
//! incomplete last record is cut. A size 0 record is indexed at size 0, the
//! same live entry
//! the write path publishes for an empty write: a delete appends the
//! identical record, so the two cannot be told apart, and resurrecting an
//! empty placeholder beats a wrong tombstone, which drops an acknowledged
//! write's object entirely.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use tracing::warn;

use crate::storage::needle::needle::{Needle, NeedleError, get_actual_size};
use crate::storage::super_block::SUPER_BLOCK_SIZE;
use crate::storage::types::*;
use crate::storage::volume::{Volume, VolumeError, VolumeIo, needle_disk_end};

/// What stopped the unindexed-.dat-tail repair. Mirrors the sentinels Go
/// distinguishes: a `NotTorn` tail is left untouched for the integrity check
/// to quarantine read-only, a `Corrupt` record is damaged bytes (Go errors
/// wrapping needle.ErrorCorrupted) that `walk_unindexed_dat_tail` resolves
/// into a cut or a `NotTorn`, an `OpenIndex` failure is the .idx refusing a
/// read-write open — which in Go fails the whole load before the check ever
/// runs — and `Failed` is any other repair failure, which fails the check so
/// the volume loads read-only.
#[derive(Debug)]
pub(crate) enum TailRepairError {
    NotTorn(String),
    Corrupt(String),
    OpenIndex(VolumeError),
    Failed(VolumeError),
}

impl fmt::Display for TailRepairError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TailRepairError::NotTorn(why) | TailRepairError::Corrupt(why) => {
                write!(f, "{why}: not a torn tail")
            }
            TailRepairError::OpenIndex(e) | TailRepairError::Failed(e) => write!(f, "{e}"),
        }
    }
}

impl TailRepairError {
    /// Mirrors Go's `errors.Is(err, errTailNotTorn)`: a tail that is not the
    /// result of an interrupted append keeps the volume loading read-only
    /// through the integrity check; a `Corrupt` record is resolved inside
    /// the walk and everything else is a real repair failure.
    pub(crate) fn is_not_torn(&self) -> bool {
        matches!(self, TailRepairError::NotTorn(_))
    }
}

impl From<io::Error> for TailRepairError {
    fn from(e: io::Error) -> Self {
        TailRepairError::Failed(VolumeError::Io(e))
    }
}

impl From<VolumeError> for TailRepairError {
    fn from(e: VolumeError) -> Self {
        TailRepairError::Failed(e)
    }
}

impl Volume {
    /// Re-index the complete needles a crash left past the last indexed
    /// needle of a local version 3 volume, and cut an incomplete remainder.
    /// Mirrors Go's repairUnindexedDatTail.
    ///
    /// Runs ahead of the index load so the re-indexed rows land in the
    /// needle map and inside its append offset. Only a volume about to load
    /// writable is repaired; the caller checks the no-write flags. A crash
    /// after the very first append leaves the needle past an index with no
    /// entry at all: the walk then starts at the first record past the
    /// superblock with no anchor needle and no append timestamp to compare
    /// against, and a tail it cannot explain fails the load like any other
    /// repair failure, since every later check skips an empty index. Nothing
    /// is changed when the tail is anything but what an interrupted append
    /// leaves, and the integrity check that follows then marks the volume
    /// read-only exactly as before.
    pub(crate) fn repair_unindexed_dat_tail(&self, io: &VolumeIo) -> Result<(), TailRepairError> {
        let version = self.version();
        if version != VERSION_3 || !self.has_data_backend_io(io) {
            return Ok(());
        }
        let idx_path = self.file_name(".idx");
        if !Path::new(&idx_path).exists() {
            return Ok(());
        }
        let idx_size = fs::metadata(&idx_path).map(|m| m.len()).unwrap_or(0) as i64;
        if idx_size % NEEDLE_MAP_ENTRY_SIZE as i64 != 0 {
            // a partial .idx entry is left alone, as today
            return Ok(());
        }
        let file_size = self.current_dat_file_size_io(io)? as i64;
        if file_size <= SUPER_BLOCK_SIZE as i64 || file_size > MAX_POSSIBLE_VOLUME_SIZE as i64 {
            // past MaxPossibleVolumeSize the .idx offsets wrap: an entry's
            // offset does not name its needle's position on its own
            return Ok(());
        }

        // Go opens the index O_RDWR ahead of the check when the volume is
        // writable, and a failure there fails the load, not just the repair.
        let mut idx_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&idx_path)
            .map_err(|e| TailRepairError::OpenIndex(VolumeError::Io(e)))?;

        let mut last_append_at_ns = 0u64;
        let start = if idx_size == 0 {
            // The index holds no row at all, so no entry can anchor the
            // walk: it starts at the first record past the superblock.
            // Mirrors Go's tailEntryPos < 0.
            self.super_block().block_size() as i64
        } else {
            let tail_entry_pos =
                self.find_dat_tail_entry_offset(io, &mut idx_file, idx_size, version)?;
            if tail_entry_pos < 0 {
                // pre-allocated / all-zero index, no anchor to repair past
                return Ok(());
            }
            let mut buf = [0u8; NEEDLE_MAP_ENTRY_SIZE];
            idx_file.seek(SeekFrom::Start(tail_entry_pos as u64))?;
            idx_file.read_exact(&mut buf)?;
            let (key, offset, size) = idx_entry_from_bytes(&buf);
            if offset.is_zero() {
                return Ok(());
            }
            let start = needle_disk_end(offset, size, version);
            if start >= file_size {
                // the .dat ends at the last needle (clean), or the index
                // references bytes the .dat does not have: not a torn tail
                return Ok(());
            }

            // Anchor: the needle at the highest indexed offset must be the one
            // its entry describes, whole and with a valid CRC.
            let on_disk_size = if size.is_deleted() { Size(0) } else { size };
            let anchor = self
                .read_tail_needle(io, offset.to_actual_offset(), on_disk_size, version)
                .map_err(|e| {
                    TailRepairError::NotTorn(format!(
                        "last indexed needle {key} at {}: {e}",
                        offset.to_actual_offset()
                    ))
                })?;
            if anchor.id != key {
                return Err(TailRepairError::NotTorn(format!(
                    "needle at {} is id {:?}, its index entry says id {:?}",
                    offset.to_actual_offset(),
                    anchor.id,
                    key
                )));
            }
            last_append_at_ns = anchor.append_at_ns;
            start
        };
        if start >= file_size {
            // no record past the superblock: nothing to index
            return Ok(());
        }

        let (entries, cut_at) =
            self.walk_unindexed_dat_tail(io, version, start, file_size, last_append_at_ns)?;
        let reindexed = entries.len() / NEEDLE_MAP_ENTRY_SIZE;
        if reindexed > 0 {
            // The new .idx rows must not become durable before the .dat
            // records they name: a power loss between the two would leave
            // the .idx pointing past the end of the .dat.
            self.flush_dat(io).map_err(|e| {
                TailRepairError::Failed(VolumeError::Io(io::Error::new(
                    e.kind(),
                    format!("sync {} before re-indexing: {e}", self.file_name(".dat")),
                )))
            })?;
            Self::append_index_entries(&mut idx_file, &entries, idx_size)?;
        }
        let mut cut = 0i64;
        if cut_at < file_size {
            self.truncate_dat(io, cut_at as u64).map_err(|e| {
                TailRepairError::Failed(VolumeError::Io(io::Error::new(
                    e.kind(),
                    format!("truncate {} to {cut_at}: {e}", self.file_name(".dat")),
                )))
            })?;
            self.flush_dat(io).map_err(|e| {
                TailRepairError::Failed(VolumeError::Io(io::Error::new(
                    e.kind(),
                    format!(
                        "sync {} after truncating to {cut_at}: {e}",
                        self.file_name(".dat")
                    ),
                )))
            })?;
            cut = file_size - cut_at;
        }
        warn!(
            "volume {}: repaired the unindexed tail of {}: re-indexed {} needle(s), cut {} bytes ({} -> {} bytes)",
            self.id.0,
            self.file_name(".dat"),
            reindexed,
            cut,
            file_size,
            file_size - cut
        );
        Ok(())
    }

    /// Read and verify the whole record of a needle of the given on-disk
    /// size at offset: header, body, CRC and timestamp. Mirrors Go's
    /// readTailNeedle.
    fn read_tail_needle(
        &self,
        io: &VolumeIo,
        at: i64,
        size: Size,
        version: Version,
    ) -> Result<Needle, TailRepairError> {
        let blob = self
            .read_needle_blob_unlocked(io, at, size)
            .map_err(TailRepairError::Failed)?;
        let mut n = Needle::default();
        n.read_bytes(&blob, at, size, version)
            .map_err(|e| match e {
                NeedleError::CrcMismatch { .. }
                | NeedleError::IndexOutOfRange(_)
                | NeedleError::TailTooShort => TailRepairError::Corrupt(e.to_string()),
                other => TailRepairError::Failed(VolumeError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    other.to_string(),
                ))),
            })?;
        if size.0 > 0 && n.data.is_empty() {
            // A needle with no data is written with size 0, so a record with
            // a body whose data size reads 0 is damaged. The CRC covers only
            // the data, and read_bytes does not check it for empty data.
            return Err(TailRepairError::Corrupt(format!(
                "needle {:?} at {at} has size {} but no data",
                n.id, size.0
            )));
        }
        Ok(n)
    }

    /// Walk the needles from start to file_size and return the .idx entries
    /// of the complete ones, and the offset from which the rest is an
    /// incomplete last record (file_size when there is none). Fails with
    /// NotTorn when the tail is damaged in a way an interrupted append does
    /// not explain. Mirrors Go's walkUnindexedDatTail.
    fn walk_unindexed_dat_tail(
        &self,
        io: &VolumeIo,
        version: Version,
        start: i64,
        file_size: i64,
        last_append_at_ns: u64,
    ) -> Result<(Vec<u8>, i64), TailRepairError> {
        let mut entries = Vec::new();
        let mut last_append_at_ns = last_append_at_ns;
        let mut at = start;
        while at < file_size {
            if file_size - at < NEEDLE_HEADER_SIZE as i64 {
                return Ok((entries, at)); // a partial header
            }
            let mut header = [0u8; NEEDLE_HEADER_SIZE];
            self.read_exact_at_backend(io, &mut header, at as u64)
                .map_err(|e| {
                    TailRepairError::Failed(VolumeError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("read needle header at {at}: {e}"),
                    )))
                })?;
            let (_, id, size) = Needle::parse_header(&header);
            if id.is_empty() || size.0 < 0 {
                return Err(TailRepairError::NotTorn(format!(
                    "record at {at} has id {id:?} size {}",
                    size.0
                )));
            }
            let end = at + get_actual_size(size, version);
            if end > file_size {
                return Ok((entries, at)); // the last record runs past the end of the file
            }
            let n = match self.read_tail_needle(io, at, size, version) {
                Ok(n) => n,
                Err(e @ TailRepairError::Corrupt(_)) => {
                    if end == file_size {
                        return Ok((entries, at)); // the last record is damaged: it never completed
                    }
                    return Err(TailRepairError::NotTorn(format!(
                        "record {id:?} at {at} is damaged and {} bytes follow it: {e}",
                        file_size - end
                    )));
                }
                Err(e) => return Err(e), // a read error says nothing about the bytes: leave them
            };
            if n.append_at_ns <= last_append_at_ns {
                return Err(TailRepairError::NotTorn(format!(
                    "record {:?} at {at} was appended at {}, not after {last_append_at_ns}",
                    n.id, n.append_at_ns
                )));
            }
            last_append_at_ns = n.append_at_ns;
            // A size 0 record is a delete or an empty write; the two append
            // the identical record, so it is indexed at size 0 the way the
            // write path indexes an empty write.
            let mut entry = [0u8; NEEDLE_MAP_ENTRY_SIZE];
            idx_entry_to_bytes(&mut entry, n.id, Offset::from_actual_offset(at), size);
            entries.extend_from_slice(&entry);
            at = end;
        }
        Ok((entries, file_size))
    }

    /// Write entries at the end (at) of the .idx and sync it. A failed write
    /// can leave part of the entries behind, which would make the next load
    /// reject the .idx as a partial entry; they are removed again. Mirrors
    /// Go's appendIndexEntries.
    fn append_index_entries(
        idx_file: &mut File,
        entries: &[u8],
        at: i64,
    ) -> Result<(), TailRepairError> {
        let write = (|| -> io::Result<()> {
            idx_file.seek(SeekFrom::Start(at as u64))?;
            idx_file.write_all(entries)
        })();
        if let Err(e) = write {
            return Err(match idx_file.set_len(at as u64) {
                Ok(()) => TailRepairError::Failed(VolumeError::Io(io::Error::new(
                    e.kind(),
                    format!("append {} bytes to index: {e}", entries.len()),
                ))),
                Err(te) => TailRepairError::Failed(VolumeError::Io(io::Error::new(
                    e.kind(),
                    format!(
                        "append {} bytes to index: {e}; truncate back to {at}: {te}",
                        entries.len()
                    ),
                ))),
            });
        }
        idx_file.sync_all().map_err(|e| {
            TailRepairError::Failed(VolumeError::Io(io::Error::new(
                e.kind(),
                format!("sync index: {e}"),
            )))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::needle_map::NeedleMapKind;
    use std::collections::HashMap;
    use crate::storage::volume::VolumeSpec;
    use tempfile::TempDir;

    // A torn tail is what a crash between the two appends of a write leaves:
    // the needle is in the .dat, its entry never reached the .idx. These
    // tests write needles (and deletes) normally, drop the last .idx
    // entries, cut or damage the .dat as a crash would, and reload the
    // volume through the real load path — the same fixture Go's
    // volume_checking_tail_repair_test.go builds.

    enum Op {
        Write(u64),
        Del(u64),
        Empty(u64), // a write with no data appends a size 0 record like a delete
    }

    struct TornFixture {
        _tmp: TempDir,
        dir: String,
        starts: Vec<i64>, // .dat offset of each op's record
        end: i64,         // .dat size after every op
        written: HashMap<u64, Vec<u8>>,
    }

    impl TornFixture {
        fn dat_path(&self) -> String {
            format!("{}/1.dat", self.dir)
        }
        fn idx_path(&self) -> String {
            format!("{}/1.idx", self.dir)
        }
        fn dat_size(&self) -> i64 {
            fs::metadata(self.dat_path()).unwrap().len() as i64
        }
        fn idx_size(&self) -> i64 {
            fs::metadata(self.idx_path()).unwrap().len() as i64
        }
        /// An offset inside the body of op i's record: past its header,
        /// short of its end.
        fn mid_record(&self, i: usize) -> i64 {
            let end = if i + 1 < self.starts.len() {
                self.starts[i + 1]
            } else {
                self.end
            };
            self.starts[i]
                + NEEDLE_HEADER_SIZE as i64
                + (end - self.starts[i] - NEEDLE_HEADER_SIZE as i64) / 2
        }
        fn truncate_dat(&self, size: i64) {
            OpenOptions::new()
                .write(true)
                .open(self.dat_path())
                .unwrap()
                .set_len(size as u64)
                .unwrap();
        }
        /// Damage the data of the record at the given offset so its CRC no
        /// longer matches.
        fn flip_data_byte(&self, at: i64) {
            // Positional writes without Unix-only FileExt so the helper also
            // builds on Windows; single-threaded, no reader can move the
            // offset between seek and write.
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(self.dat_path())
                .unwrap();
            let pos = (at + NEEDLE_HEADER_SIZE as i64 + 4) as u64; // past the 4-byte DataSize field
            let mut b = [0u8; 1];
            file.seek(SeekFrom::Start(pos)).unwrap();
            file.read_exact(&mut b).unwrap();
            b[0] ^= 0xff;
            file.seek(SeekFrom::Start(pos)).unwrap();
            file.write_all(&b).unwrap();
        }
        /// Zero the DataSize field and the first data byte (read as the
        /// flags once DataSize is 0): the header still claims a body, the
        /// body parses as an empty needle.
        fn zero_data_size(&self, at: i64) {
            let mut file = OpenOptions::new()
                .write(true)
                .open(self.dat_path())
                .unwrap();
            file.seek(SeekFrom::Start(at as u64 + NEEDLE_HEADER_SIZE as u64))
                .unwrap();
            file.write_all(&[0u8; 5]).unwrap();
        }
    }

    fn open_volume(dir: &str) -> Volume {
        Volume::new(
            dir,
            dir,
            VolumeId(1),
            NeedleMapKind::InMemory,
            &VolumeSpec::default(),
        )
        .unwrap()
    }

    fn new_needle(id: u64) -> Needle {
        let data = format!("needle-{id}-payload").into_bytes();
        Needle {
            id: NeedleId(id),
            cookie: Cookie(0x1234_0000 + id as u32),
            data,
            ..Needle::default()
        }
    }

    /// Run ops against a fresh volume (each op appends one .dat record and
    /// one .idx entry), drop it, and remove the last `unindexed` entries.
    fn build_fixture(ops: &[Op], unindexed: usize) -> TornFixture {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();
        let mut f = TornFixture {
            _tmp: tmp,
            dir,
            starts: Vec::new(),
            end: 0,
            written: HashMap::new(),
        };
        {
            let v = open_volume(&f.dir);
            for op in ops {
                f.starts.push(v.current_dat_file_size().unwrap() as i64);
                match op {
                    Op::Write(id) => {
                        let mut n = new_needle(*id);
                        v.write_needle(&mut n, true, false).unwrap();
                        f.written.insert(*id, n.data.clone());
                    }
                    Op::Del(id) => {
                        v.delete_needle(&mut Needle {
                            id: NeedleId(*id),
                            ..Needle::default()
                        })
                        .unwrap();
                    }
                    Op::Empty(id) => {
                        let mut n = Needle {
                            id: NeedleId(*id),
                            cookie: Cookie(0x1234_0000 + *id as u32),
                            ..Needle::default()
                        };
                        v.write_needle(&mut n, true, false).unwrap();
                        f.written.insert(*id, n.data.clone());
                    }
                }
            }
            f.end = v.current_dat_file_size().unwrap() as i64;
        }
        if unindexed > 0 {
            let idx_file = OpenOptions::new().write(true).open(f.idx_path()).unwrap();
            let idx_size = idx_file.metadata().unwrap().len();
            idx_file
                .set_len(idx_size - unindexed as u64 * NEEDLE_MAP_ENTRY_SIZE as u64)
                .unwrap();
        }
        f
    }

    fn assert_readable(v: &Volume, id: u64, want: &[u8]) {
        let mut n = Needle {
            id: NeedleId(id),
            ..Needle::default()
        };
        v.read_needle(&mut n).unwrap();
        assert_eq!(n.data, want, "needle {id} reads back different data");
    }

    fn assert_not_readable(v: &Volume, id: u64) {
        let mut n = Needle {
            id: NeedleId(id),
            ..Needle::default()
        };
        assert!(
            v.read_needle(&mut n).is_err(),
            "needle {id} reads back; it should be deleted or absent"
        );
    }

    /// A recovered size 0 record indexes as a live empty needle: a delete
    /// and a write with no data append the identical record, so the repair
    /// cannot tell them apart and indexes the entry the write path would
    /// have written.
    fn assert_empty(v: &Volume, id: u64) {
        let mut n = Needle {
            id: NeedleId(id),
            ..Needle::default()
        };
        v.read_needle(&mut n)
            .unwrap_or_else(|e| panic!("read needle {id}: {e}"));
        assert!(
            n.data.is_empty(),
            "needle {id} reads back {} bytes, want an empty needle",
            n.data.len()
        );
    }

    fn writes(ids: &[u64]) -> Vec<Op> {
        ids.iter().map(|id| Op::Write(*id)).collect()
    }

    struct RepairCase {
        name: &'static str,
        ops: Vec<Op>,
        unindexed: usize,
        damage: Option<fn(&TornFixture)>,
        want_dat_size: fn(&TornFixture) -> i64,
        reindexed: usize,
        readable: &'static [u64],
        empty: &'static [u64], // readable as an empty live needle
        absent: &'static [u64],
    }

    struct ReadOnlyCase {
        name: &'static str,
        ops: Vec<Op>,
        unindexed: usize,
        damage: fn(&TornFixture),
    }

    #[test]
    fn volume_load_repairs_unindexed_dat_tail() {
        let cases = vec![
            RepairCase {
                name: "one complete unindexed needle is re-indexed",
                ops: writes(&[1, 2, 3, 4]),
                unindexed: 1,
                damage: None,
                want_dat_size: |f| f.end,
                reindexed: 1,
                readable: &[1, 2, 3, 4],
                empty: &[],
                absent: &[],
            },
            RepairCase {
                name: "two complete unindexed needles are re-indexed",
                ops: writes(&[1, 2, 3, 4, 5]),
                unindexed: 2,
                damage: None,
                want_dat_size: |f| f.end,
                reindexed: 2,
                readable: &[1, 2, 3, 4, 5],
                empty: &[],
                absent: &[],
            },
            RepairCase {
                name: "an unindexed delete is re-indexed as an empty live needle",
                ops: {
                    let mut ops = writes(&[1, 2, 3]);
                    ops.push(Op::Del(2));
                    ops
                },
                unindexed: 1,
                damage: None,
                want_dat_size: |f| f.end,
                reindexed: 1,
                readable: &[1, 3],
                empty: &[2],
                absent: &[],
            },
            RepairCase {
                name: "an unindexed empty write of a live id stays an empty needle",
                ops: {
                    let mut ops = writes(&[1, 2, 3]);
                    ops.push(Op::Empty(2));
                    ops
                },
                unindexed: 1,
                damage: None,
                want_dat_size: |f| f.end,
                reindexed: 1,
                readable: &[1, 3],
                empty: &[2],
                absent: &[],
            },
            RepairCase {
                name: "a partial header is cut",
                ops: writes(&[1, 2, 3, 4]),
                unindexed: 1,
                damage: Some(|f| f.truncate_dat(f.starts[3] + 7)),
                want_dat_size: |f| f.starts[3],
                reindexed: 0,
                readable: &[1, 2, 3],
                empty: &[],
                absent: &[4],
            },
            RepairCase {
                name: "a partial body is cut",
                ops: writes(&[1, 2, 3, 4]),
                unindexed: 1,
                damage: Some(|f| f.truncate_dat(f.mid_record(3))),
                want_dat_size: |f| f.starts[3],
                reindexed: 0,
                readable: &[1, 2, 3],
                empty: &[],
                absent: &[4],
            },
            RepairCase {
                name: "a complete needle is re-indexed and the partial one after it cut",
                ops: writes(&[1, 2, 3, 4, 5]),
                unindexed: 2,
                damage: Some(|f| f.truncate_dat(f.mid_record(4))),
                want_dat_size: |f| f.starts[4],
                reindexed: 1,
                readable: &[1, 2, 3, 4],
                empty: &[],
                absent: &[5],
            },
            RepairCase {
                name: "a last needle with a bad CRC is cut",
                ops: writes(&[1, 2, 3, 4]),
                unindexed: 1,
                damage: Some(|f| f.flip_data_byte(f.starts[3])),
                want_dat_size: |f| f.starts[3],
                reindexed: 0,
                readable: &[1, 2, 3],
                empty: &[],
                absent: &[4],
            },
            RepairCase {
                name: "a last needle whose data size field reads 0 is cut",
                ops: writes(&[1, 2, 3, 4]),
                unindexed: 1,
                damage: Some(|f| f.zero_data_size(f.starts[3])),
                want_dat_size: |f| f.starts[3],
                reindexed: 0,
                readable: &[1, 2, 3],
                empty: &[],
                absent: &[4],
            },
            RepairCase {
                name: "an unindexed empty write of a new id stays an empty needle",
                ops: {
                    let mut ops = writes(&[1, 2]);
                    ops.push(Op::Empty(3));
                    ops
                },
                unindexed: 1,
                damage: None,
                want_dat_size: |f| f.end,
                reindexed: 1,
                readable: &[1, 2, 3],
                empty: &[],
                absent: &[],
            },
            RepairCase {
                name: "an unindexed empty write after a delete of the same id stays an empty needle",
                ops: {
                    let mut ops = writes(&[1, 2]);
                    ops.push(Op::Del(2));
                    ops.push(Op::Empty(2));
                    ops
                },
                unindexed: 1,
                damage: None,
                want_dat_size: |f| f.end,
                reindexed: 1,
                readable: &[1, 2],
                empty: &[],
                absent: &[],
            },
            RepairCase {
                name: "an unindexed write and delete of the same id are re-indexed in order",
                ops: {
                    let mut ops = writes(&[1, 2, 3]);
                    ops.push(Op::Del(3));
                    ops
                },
                unindexed: 2,
                damage: None,
                want_dat_size: |f| f.end,
                reindexed: 2,
                readable: &[1, 2],
                empty: &[3],
                absent: &[],
            },
            RepairCase {
                name: "an unindexed empty write and its delete are re-indexed in order",
                ops: {
                    let mut ops = writes(&[1, 2]);
                    ops.push(Op::Empty(3));
                    ops.push(Op::Del(3));
                    ops
                },
                unindexed: 2,
                damage: None,
                want_dat_size: |f| f.end,
                reindexed: 2,
                readable: &[1, 2],
                empty: &[3],
                absent: &[],
            },
            RepairCase {
                name: "a partial needle after an indexed tombstone is cut",
                ops: {
                    let mut ops = writes(&[1, 2, 3]);
                    ops.push(Op::Del(2));
                    ops.push(Op::Write(4));
                    ops
                },
                unindexed: 1,
                damage: Some(|f| f.truncate_dat(f.mid_record(4))),
                want_dat_size: |f| f.starts[4],
                reindexed: 0,
                readable: &[1, 3],
                empty: &[],
                absent: &[2, 4],
            },
        ];
        for tc in cases {
            let f = build_fixture(&tc.ops, tc.unindexed);
            if let Some(damage) = tc.damage {
                damage(&f);
            }
            let idx_before = f.idx_size();

            {
                let v = open_volume(&f.dir);
                assert!(
                    !v.is_read_only(),
                    "{}: volume with an unindexed .dat tail loaded read-only",
                    tc.name
                );
                assert_eq!(
                    f.dat_size(),
                    (tc.want_dat_size)(&f),
                    "{}: .dat size after load",
                    tc.name
                );
                assert_eq!(
                    f.idx_size(),
                    idx_before + tc.reindexed as i64 * NEEDLE_MAP_ENTRY_SIZE as i64,
                    "{}: .idx size after load ({} entries re-indexed)",
                    tc.name,
                    tc.reindexed
                );
                for &id in tc.readable {
                    assert_readable(&v, id, &f.written[&id]);
                }
                for &id in tc.empty {
                    assert_empty(&v, id);
                }
                for &id in tc.absent {
                    assert_not_readable(&v, id);
                }
                let mut extra = new_needle(100);
                v.write_needle(&mut extra, true, false)
                    .expect("write after repair");
                assert_readable(&v, 100, &extra.data);
            }

            // the repaired volume reloads clean, and keeps what it indexed
            let v = open_volume(&f.dir);
            assert!(
                !v.is_read_only(),
                "{}: repaired volume loaded read-only on the next load",
                tc.name
            );
            for &id in tc.readable {
                assert_readable(&v, id, &f.written[&id]);
            }
            assert_readable(&v, 100, b"needle-100-payload");
        }
    }

    /// An empty index can hide writes a crash stranded in the .dat: the
    /// writable index loader creates the .idx before the first needle
    /// append, so a crash after that append leaves a volume whose data
    /// nothing indexes. The same repair runs from the first record past
    /// the superblock.
    #[test]
    fn volume_load_repairs_tail_past_empty_index() {
        let cases = vec![
            RepairCase {
                name: "the only write is re-indexed",
                ops: writes(&[7]),
                unindexed: 1,
                damage: None,
                want_dat_size: |f| f.end,
                reindexed: 1,
                readable: &[7],
                empty: &[],
                absent: &[],
            },
            RepairCase {
                name: "every write is re-indexed",
                ops: writes(&[1, 2, 3]),
                unindexed: 3,
                damage: None,
                want_dat_size: |f| f.end,
                reindexed: 3,
                readable: &[1, 2, 3],
                empty: &[],
                absent: &[],
            },
            RepairCase {
                name: "an unindexed delete is re-indexed as an empty live needle",
                ops: {
                    let mut ops = writes(&[1, 2, 3]);
                    ops.push(Op::Del(2));
                    ops
                },
                unindexed: 4,
                damage: None,
                want_dat_size: |f| f.end,
                reindexed: 4,
                readable: &[1, 3],
                empty: &[2],
                absent: &[],
            },
            RepairCase {
                name: "a complete needle is re-indexed and the partial one after it cut",
                ops: writes(&[1, 2]),
                unindexed: 2,
                damage: Some(|f| f.truncate_dat(f.mid_record(1))),
                want_dat_size: |f| f.starts[1],
                reindexed: 1,
                readable: &[1],
                empty: &[],
                absent: &[2],
            },
            RepairCase {
                name: "a last needle with a bad CRC is cut",
                ops: writes(&[1, 2]),
                unindexed: 2,
                damage: Some(|f| f.flip_data_byte(f.starts[1])),
                want_dat_size: |f| f.starts[1],
                reindexed: 1,
                readable: &[1],
                empty: &[],
                absent: &[2],
            },
        ];
        for tc in cases {
            let f = build_fixture(&tc.ops, tc.unindexed);
            if let Some(damage) = tc.damage {
                damage(&f);
            }

            {
                let v = open_volume(&f.dir);
                assert!(
                    !v.is_read_only(),
                    "{}: volume with an unindexed .dat tail loaded read-only",
                    tc.name
                );
                assert_eq!(
                    f.dat_size(),
                    (tc.want_dat_size)(&f),
                    "{}: .dat size after load",
                    tc.name
                );
                assert_eq!(
                    f.idx_size(),
                    tc.reindexed as i64 * NEEDLE_MAP_ENTRY_SIZE as i64,
                    "{}: .idx size after load ({} entries re-indexed)",
                    tc.name,
                    tc.reindexed
                );
                for &id in tc.readable {
                    assert_readable(&v, id, &f.written[&id]);
                }
                for &id in tc.empty {
                    assert_empty(&v, id);
                }
                for &id in tc.absent {
                    assert_not_readable(&v, id);
                }
            }

            // the repaired volume reloads clean, and keeps what it indexed
            let v = open_volume(&f.dir);
            assert!(
                !v.is_read_only(),
                "{}: repaired volume loaded read-only on the next load",
                tc.name
            );
            for &id in tc.readable {
                assert_readable(&v, id, &f.written[&id]);
            }
            for &id in tc.empty {
                assert_empty(&v, id);
            }
        }
    }

    /// A .dat an interrupted append does not explain, over an index with no
    /// rows at all, loads the volume read-only: the checks that would
    /// quarantine it otherwise all skip an empty index.
    #[test]
    fn volume_load_keeps_unexplained_tail_past_empty_index_read_only() {
        let cases = vec![
            ReadOnlyCase {
                name: "zero bytes where the next record would be (id 0)",
                ops: writes(&[1, 2]),
                unindexed: 2,
                damage: |f| {
                    let mut file = OpenOptions::new().append(true).open(f.dat_path()).unwrap();
                    file.write_all(&[0u8; 64]).unwrap();
                },
            },
            ReadOnlyCase {
                name: "a damaged record more data follows",
                ops: writes(&[1, 2]),
                unindexed: 2,
                damage: |f| f.flip_data_byte(f.starts[0]),
            },
        ];
        for tc in cases {
            let f = build_fixture(&tc.ops, tc.unindexed);
            (tc.damage)(&f);
            let (dat_before, idx_before) = (f.dat_size(), f.idx_size());
            assert_eq!(idx_before, 0, "fixture did not leave an empty .idx");

            let v = open_volume(&f.dir);
            assert!(
                v.is_read_only(),
                "{}: volume with an unexplained tail past an empty index loaded writable",
                tc.name
            );
            assert_eq!(
                f.dat_size(),
                dat_before,
                "{}: .dat changed on load",
                tc.name
            );
            assert_eq!(
                f.idx_size(),
                idx_before,
                "{}: .idx changed on load",
                tc.name
            );
        }
    }

    /// Every tail an interrupted append does not explain is left exactly as
    /// it is, and the volume loads read-only as before.
    #[test]
    fn volume_load_keeps_unexplained_tail_read_only() {
        let cases = vec![
            ReadOnlyCase {
                name: "the last indexed needle's id differs from its entry",
                ops: writes(&[1, 2, 3, 4]),
                unindexed: 1,
                damage: |f| {
                    let mut file = OpenOptions::new().write(true).open(f.dat_path()).unwrap();
                    file.seek(SeekFrom::Start(
                        (f.starts[2] + COOKIE_SIZE as i64 + NEEDLE_ID_SIZE as i64 - 1) as u64,
                    ))
                    .unwrap();
                    file.write_all(&[0xff]).unwrap();
                },
            },
            ReadOnlyCase {
                name: "the last indexed needle fails its CRC",
                ops: writes(&[1, 2, 3, 4]),
                unindexed: 1,
                damage: |f| f.flip_data_byte(f.starts[2]),
            },
            ReadOnlyCase {
                name: "a zeroed data size field followed by more data",
                ops: writes(&[1, 2, 3, 4, 5]),
                unindexed: 2,
                damage: |f| f.zero_data_size(f.starts[3]),
            },
            ReadOnlyCase {
                name: "a bad CRC followed by more data",
                ops: writes(&[1, 2, 3, 4, 5]),
                unindexed: 2,
                damage: |f| f.flip_data_byte(f.starts[3]),
            },
            ReadOnlyCase {
                name: "zero bytes past the last needle (id 0)",
                ops: writes(&[1, 2, 3]),
                unindexed: 0,
                damage: |f| {
                    let mut file = OpenOptions::new().append(true).open(f.dat_path()).unwrap();
                    file.write_all(&[0u8; 64]).unwrap();
                },
            },
            ReadOnlyCase {
                name: "the index ends past the data",
                ops: writes(&[1, 2, 3]),
                unindexed: 0,
                damage: |f| f.truncate_dat(f.end - 10),
            },
        ];
        for tc in cases {
            let f = build_fixture(&tc.ops, tc.unindexed);
            (tc.damage)(&f);
            let (dat_before, idx_before) = (f.dat_size(), f.idx_size());

            let v = open_volume(&f.dir);
            assert!(
                v.is_read_only(),
                "{}: volume with an unexplained tail loaded writable",
                tc.name
            );
            assert_eq!(
                f.dat_size(),
                dat_before,
                "{}: .dat changed on load",
                tc.name
            );
            assert_eq!(
                f.idx_size(),
                idx_before,
                "{}: .idx changed on load",
                tc.name
            );
        }
    }

    #[test]
    fn volume_load_leaves_clean_volume_untouched() {
        let f = build_fixture(
            &{
                let mut ops = writes(&[1, 2, 3]);
                ops.push(Op::Del(2));
                ops
            },
            0,
        );
        let (dat_before, idx_before) = (f.dat_size(), f.idx_size());

        let v = open_volume(&f.dir);
        assert!(!v.is_read_only(), "clean volume loaded read-only");
        assert_eq!(f.dat_size(), dat_before, "clean .dat changed");
        assert_eq!(f.idx_size(), idx_before, "clean .idx changed");
        assert_readable(&v, 1, &f.written[&1]);
        assert_readable(&v, 3, &f.written[&3]);
        assert_not_readable(&v, 2);
    }

    /// A volume persisted read-only is not repaired: the tail stays for
    /// whoever marked it.
    #[test]
    fn volume_load_leaves_read_only_volume_tail_alone() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();
        {
            let v = open_volume(&dir);
            for id in [1u64, 2, 99] {
                let mut n = new_needle(id);
                v.write_needle(&mut n, true, false).unwrap();
            }
            v.set_read_only_persist(false, true).unwrap();
        }
        let idx_path = format!("{dir}/1.idx");
        let idx_len = fs::metadata(&idx_path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&idx_path)
            .unwrap()
            .set_len(idx_len - NEEDLE_MAP_ENTRY_SIZE as u64)
            .unwrap();
        let dat_path = format!("{dir}/1.dat");
        let (dat_before, idx_before) = (
            fs::metadata(&dat_path).unwrap().len(),
            fs::metadata(&idx_path).unwrap().len(),
        );

        let v = open_volume(&dir);
        assert!(
            v.is_read_only(),
            "volume persisted read-only loaded writable"
        );
        assert_eq!(fs::metadata(&dat_path).unwrap().len(), dat_before);
        assert_eq!(fs::metadata(&idx_path).unwrap().len(), idx_before);
    }

    /// A canDelete volume still appends a tombstone record on every delete,
    /// so it can tear a tail the same way: it is repaired and keeps taking
    /// deletes. Mirrors Go's TestVolumeLoadRepairsTailOnCanDeleteVolume.
    #[test]
    fn volume_load_repairs_tail_on_can_delete_volume() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();
        let mut last_data = Vec::new();
        {
            let v = open_volume(&dir);
            for id in [1u64, 2, 99] {
                let mut n = new_needle(id);
                v.write_needle(&mut n, true, false).unwrap();
                last_data = n.data.clone();
            }
            v.set_read_only_persist(true, true).unwrap();
        }
        let idx_path = format!("{dir}/1.idx");
        let idx_len = fs::metadata(&idx_path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&idx_path)
            .unwrap()
            .set_len(idx_len - NEEDLE_MAP_ENTRY_SIZE as u64)
            .unwrap();

        let v = open_volume(&dir);
        assert!(
            !v.is_no_write_or_delete() && v.is_no_write_can_delete(),
            "canDelete volume with a torn tail did not load canDelete after the repair"
        );
        assert_readable(&v, 99, &last_data);
    }

    /// A torn record whose header advertises a positive size smaller than
    /// the body's own 4-byte data-size field is damaged, not decodable: the
    /// decoder's range check rejects it, and as the last record it is cut.
    /// Mirrors Go's TestVolumeLoadCutsDamagedSmallSizeTailOnCanDeleteVolume.
    #[test]
    fn volume_load_cuts_damaged_small_size_tail() {
        let f = build_fixture(&writes(&[1, 2, 3]), 1);
        let mut record = vec![0u8; get_actual_size(Size(2), VERSION_3) as usize];
        Cookie(0xabcd).to_bytes(&mut record[0..COOKIE_SIZE]);
        NeedleId(7).to_bytes(&mut record[COOKIE_SIZE..COOKIE_SIZE + NEEDLE_ID_SIZE]);
        Size(2).to_bytes(
            &mut record[COOKIE_SIZE + NEEDLE_ID_SIZE..NEEDLE_HEADER_SIZE],
        );
        OpenOptions::new()
            .append(true)
            .open(f.dat_path())
            .unwrap()
            .write_all(&record)
            .unwrap();

        let v = open_volume(&f.dir);
        assert!(
            !v.is_read_only(),
            "damaged tail record loaded the volume read-only"
        );
        assert_not_readable(&v, 7);
        assert_eq!(
            f.dat_size(),
            f.end,
            "damaged tail was not cut"
        );
    }
}
