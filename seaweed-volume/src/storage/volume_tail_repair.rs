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
//! server would have written, in .dat order, and only an incomplete last
//! record is cut.

use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use tracing::warn;

use crate::storage::idx;
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
    /// writable is repaired; the caller checks the no-write flags. Nothing
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
        if idx_size == 0 || idx_size % NEEDLE_MAP_ENTRY_SIZE as i64 != 0 {
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

        let (mut entries, cut_at) =
            self.walk_unindexed_dat_tail(io, version, start, file_size, anchor.append_at_ns)?;
        let reindexed = entries.len() / NEEDLE_MAP_ENTRY_SIZE;
        if reindexed > 0 {
            Self::resolve_empty_tail_records(&idx_file, &mut entries)?;
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
            // a size 0 record is a delete or an empty write:
            // resolve_empty_tail_records tells them apart
            let mut entry = [0u8; NEEDLE_MAP_ENTRY_SIZE];
            idx_entry_to_bytes(&mut entry, n.id, Offset::from_actual_offset(at), size);
            entries.extend_from_slice(&entry);
            at = end;
        }
        Ok((entries, file_size))
    }

    /// Turn the size 0 entries among the re-indexed ones into what the
    /// volume server indexed them as: a delete is a tombstone, an empty
    /// write keeps size 0. A delete is only appended for a needle that is
    /// live, so a size 0 record of a needle that is not live (absent or
    /// deleted in the .idx and in the tail before it) is an empty write.
    /// Mirrors Go's resolveEmptyTailRecords.
    fn resolve_empty_tail_records(
        idx_file: &File,
        entries: &mut [u8],
    ) -> Result<(), TailRepairError> {
        let mut live: HashMap<NeedleId, bool> = HashMap::new();
        for chunk in entries.as_chunks::<NEEDLE_MAP_ENTRY_SIZE>().0.iter() {
            let (key, _, size) = idx_entry_from_bytes(chunk);
            if size.0 == 0 {
                live.insert(key, false);
            }
        }
        if live.is_empty() {
            return Ok(());
        }
        let mut reader = io::BufReader::new(idx_file);
        idx::walk_index_file(&mut reader, 0, |key, offset, size| {
            if let Some(was_live) = live.get_mut(&key) {
                *was_live = !offset.is_zero() && !size.is_deleted();
            }
            Ok(())
        })
        .map_err(|e| {
            TailRepairError::Failed(VolumeError::Io(io::Error::new(
                e.kind(),
                format!("scan index: {e}"),
            )))
        })?;
        for chunk in entries
            .as_chunks_mut::<NEEDLE_MAP_ENTRY_SIZE>()
            .0
            .iter_mut()
        {
            let (key, offset, size) = idx_entry_from_bytes(chunk);
            let Some(was_live) = live.get(&key).copied() else {
                continue;
            };
            if size.0 == 0 && was_live {
                idx_entry_to_bytes(chunk, key, offset, TOMBSTONE_FILE_SIZE);
                live.insert(key, false);
                continue;
            }
            live.insert(key, true);
        }
        Ok(())
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

