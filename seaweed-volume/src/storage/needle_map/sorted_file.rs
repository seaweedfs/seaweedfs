//! Disk-backed needle map for read-only volumes, mirroring Go's
//! `weed/storage/needle_map_sorted_file.go`.
//!
//! A read-only or cloud-tiered volume does not need its index in RAM: `.sdx` is
//! the `.idx` rewritten as a sorted array of live entries, so a lookup is a
//! binary search on disk. Deletes append a tombstone to the tail of `.idx` and
//! mark the `.sdx` record in place.
//!
//! The map holds no descriptor of its own — both files are borrowed per
//! operation from [`file_pool`](super::file_pool) — so a volume nobody is
//! reading costs zero fds and zero index bytes of RAM.

use std::fs::{File, OpenOptions};
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Mutex;

use super::file_pool::pooled_index_files;
use crate::storage::idx;
use crate::storage::needle_map::{CompactNeedleMap, NeedleMapMetric, NeedleValue};
use crate::storage::types::*;

/// Entries per positional read when scanning `.sdx` end to end.
const ROWS_TO_READ: u64 = 1024;

/// Byte offset of the `Size` field inside a 17-byte index entry.
const ENTRY_SIZE_OFFSET: u64 = (NEEDLE_ID_SIZE + OFFSET_SIZE) as u64;

/// Appends made since the last sync, plus the tail offset the next tombstone
/// goes to. Seeded from the `.idx` size at open so a delete appends instead of
/// overwriting the front of the file.
struct IndexTail {
    offset: u64,
    needs_sync: bool,
}

pub struct SortedFileNeedleMap {
    metric: NeedleMapMetric,
    index_file_name: String,
    db_file_name: String,
    db_file_size: i64,
    tail: Mutex<IndexTail>,
}

impl SortedFileNeedleMap {
    /// Open the sorted map for the volume whose index files share
    /// `index_base_file_name` (the path with no extension), regenerating `.sdx`
    /// when it is older than `.idx` or torn.
    pub fn open(index_base_file_name: &str, version: Version) -> io::Result<Self> {
        let index_file_name = format!("{index_base_file_name}.idx");
        let db_file_name = format!("{index_base_file_name}.sdx");

        if !is_sorted_file_fresh(&db_file_name, &index_file_name)
            || !holds_whole_entries(&db_file_name)
        {
            tracing::info!(sdx = %db_file_name, idx = %index_file_name, "generating sorted index");
            write_sorted_file_from_idx(&index_file_name, &db_file_name, version)?;
        }

        let db_file_size = std::fs::metadata(&db_file_name)?.len();
        if db_file_size % NEEDLE_MAP_ENTRY_SIZE as u64 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{db_file_name} is {db_file_size} bytes, not whole {NEEDLE_MAP_ENTRY_SIZE}-byte entries"
                ),
            ));
        }
        let db_file_size = db_file_size as i64;
        let index_file_size = std::fs::metadata(&index_file_name)?.len();
        // Start appends at the last whole entry. A torn tail is overwritten by
        // the next tombstone rather than leaving every later row misaligned; the
        // metric walk below ignores it for the same reason.
        let index_file_size = index_file_size - index_file_size % NEEDLE_MAP_ENTRY_SIZE as u64;

        let metric = index_metric(&index_file_name, &db_file_name, version)?;

        Ok(SortedFileNeedleMap {
            metric,
            index_file_name,
            db_file_name,
            db_file_size,
            tail: Mutex::new(IndexTail {
                offset: index_file_size,
                needs_sync: false,
            }),
        })
    }

    /// Path of the `.sdx` this map searches.
    pub fn db_file_name(&self) -> &str {
        &self.db_file_name
    }

    /// Path of the `.idx` this map appends tombstones to.
    pub fn index_file_name(&self) -> &str {
        &self.index_file_name
    }

    pub fn get(&self, key: NeedleId) -> Option<NeedleValue> {
        let file = match pooled_index_files().borrow(&self.db_file_name, false) {
            Ok(file) => file,
            Err(e) => {
                tracing::warn!(sdx = %self.db_file_name, error = %e, "open sorted index");
                return None;
            }
        };
        match search_sorted_index(&file, self.db_file_size, key) {
            Ok(Some((_, offset, size))) => Some(NeedleValue { offset, size }),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(sdx = %self.db_file_name, error = %e, "search sorted index");
                None
            }
        }
    }

    /// Always an error: a volume backed by `.sdx` is read-only. Mirrors Go's
    /// `SortedFileNeedleMap.Put`.
    pub fn put(&mut self, _key: NeedleId, _offset: Offset, _size: Size) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("needle map {} is read only", self.db_file_name),
        ))
    }

    /// Append a tombstone for `key` to `.idx` and mark its `.sdx` record
    /// deleted. A key that is absent or already deleted is a no-op, matching
    /// Go.
    pub fn delete(&self, key: NeedleId, offset: Offset) -> io::Result<Option<Size>> {
        let file = pooled_index_files().borrow(&self.db_file_name, true)?;
        let Some((entry_index, _, size)) = search_sorted_index(&file, self.db_file_size, key)?
        else {
            return Ok(None);
        };
        if size.is_deleted() {
            return Ok(None);
        }

        // Write to the index file first: .idx is the source of truth a reload
        // rebuilds .sdx from. If the in-place mark below then fails, the delete
        // is still durable and neither shape of failure corrupts the index: a
        // write that lands nothing leaves .sdx older than the .idx we just
        // appended to, so the next load rebuilds it, and a partial write leaves
        // the size field's leading 0xff behind, which reads back negative and so
        // already counts as deleted.
        self.append_to_index_file(key, offset, TOMBSTONE_FILE_SIZE)?;

        let mut buf = [0u8; SIZE_SIZE];
        TOMBSTONE_FILE_SIZE.to_bytes(&mut buf);
        write_at(
            &file,
            &buf,
            entry_index * NEEDLE_MAP_ENTRY_SIZE as u64 + ENTRY_SIZE_OFFSET,
        )?;

        // Move the counters to where a reload would put them, so the heartbeat
        // does not report this volume as garbage-free until it restarts. The
        // tombstone is one more .idx row, and under the rule index_metric
        // applies both it and the row it supersedes count as deletions; the
        // bytes leave the live set without leaving the .idx.
        self.metric.file_count.fetch_add(1, Ordering::Relaxed);
        self.metric.deletion_count.fetch_add(2, Ordering::Relaxed);
        self.metric
            .deletion_byte_count
            .fetch_add(size.0.max(0) as u64, Ordering::Relaxed);
        Ok(Some(size))
    }

    fn append_to_index_file(&self, key: NeedleId, offset: Offset, size: Size) -> io::Result<()> {
        let file = pooled_index_files().borrow(&self.index_file_name, true)?;
        let mut buf = [0u8; NEEDLE_MAP_ENTRY_SIZE];
        idx_entry_to_bytes(&mut buf, key, offset, size);

        let mut tail = self.tail.lock().unwrap();
        write_at(&file, &buf, tail.offset)?;
        tail.offset += NEEDLE_MAP_ENTRY_SIZE as u64;
        tail.needs_sync = true;
        Ok(())
    }

    /// Answers from the offset the appends maintain rather than a stat: the
    /// heartbeat asks every volume for this on every beat, and a read-only
    /// volume's `.idx` only ever grows through `append_to_index_file`.
    pub fn index_file_size(&self) -> u64 {
        self.tail.lock().unwrap().offset
    }

    /// Flushes tombstones appended by [`delete`](Self::delete). A read-only
    /// volume that has never been deleted from — the overwhelming majority —
    /// opens nothing here, so shutting down a server holding hundreds of
    /// thousands of them costs no fsyncs.
    pub fn sync(&self) -> io::Result<()> {
        let mut tail = self.tail.lock().unwrap();
        if !tail.needs_sync {
            return Ok(());
        }
        let file = pooled_index_files().borrow(&self.index_file_name, true)?;
        file.sync_all()?;
        tail.needs_sync = false;
        Ok(())
    }

    /// Drops the pooled handles: the caller may be about to rename or remove
    /// these paths, and a descriptor left behind would keep answering reads
    /// from the old inode.
    pub fn close(&mut self) {
        let _ = self.sync();
        pooled_index_files().discard(&self.index_file_name);
        pooled_index_files().discard(&self.db_file_name);
    }

    /// Visit the live entries in ascending needle-id order, straight off `.sdx`.
    fn visit_live_entries<F>(&self, mut f: F) -> io::Result<()>
    where
        F: FnMut(NeedleId, &NeedleValue) -> io::Result<()>,
    {
        let file = pooled_index_files().borrow(&self.db_file_name, false)?;
        let entry_count = self.db_file_size.max(0) as u64 / NEEDLE_MAP_ENTRY_SIZE as u64;
        // A batch per 1024 entries rather than a syscall per entry, matching
        // idx::walk_index_file. Positional reads, not a cursor: the handle is
        // shared with any other borrower, so a file position would race.
        let rows_per_read = ROWS_TO_READ.min(entry_count.max(1));
        let mut block = vec![0u8; rows_per_read as usize * NEEDLE_MAP_ENTRY_SIZE];
        let mut done: u64 = 0;
        while done < entry_count {
            let rows = rows_per_read.min(entry_count - done) as usize;
            let bytes = &mut block[..rows * NEEDLE_MAP_ENTRY_SIZE];
            read_exact_at(&file, bytes, done * NEEDLE_MAP_ENTRY_SIZE as u64)?;
            for entry in bytes.chunks_exact(NEEDLE_MAP_ENTRY_SIZE) {
                let (key, offset, size) = idx_entry_from_bytes(entry);
                if !size.is_valid() {
                    continue; // deleted in place by a runtime delete
                }
                f(key, &NeedleValue { offset, size })?;
            }
            done += rows as u64;
        }
        Ok(())
    }

    pub fn ascending_visit<F>(&self, mut f: F) -> Result<(), String>
    where
        F: FnMut(NeedleId, &NeedleValue) -> Result<(), String>,
    {
        let mut visit_error = None;
        self.visit_live_entries(|id, nv| {
            if let Err(e) = f(id, nv) {
                visit_error = Some(e);
                return Err(io::Error::other("visit aborted"));
            }
            Ok(())
        })
        .map_err(|e| visit_error.take().unwrap_or_else(|| e.to_string()))
    }

    pub fn iter_entries(&self) -> io::Result<Vec<(NeedleId, NeedleValue)>> {
        let mut entries = Vec::new();
        self.visit_live_entries(|id, nv| {
            entries.push((id, *nv));
            Ok(())
        })?;
        Ok(entries)
    }

    pub fn save_to_idx(&self, path: &str) -> io::Result<()> {
        let mut out = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        self.visit_live_entries(|id, nv| idx::write_index_entry(&mut out, id, nv.offset, nv.size))?;
        out.sync_all()
    }

    // ---- Metrics accessors ----

    pub fn content_size(&self) -> u64 {
        self.metric.file_byte_count.load(Ordering::Relaxed)
    }

    pub fn deleted_size(&self) -> u64 {
        self.metric.deletion_byte_count.load(Ordering::Relaxed)
    }

    pub fn file_count(&self) -> i64 {
        self.metric.file_count.load(Ordering::Relaxed)
    }

    pub fn deleted_count(&self) -> i64 {
        self.metric.deletion_count.load(Ordering::Relaxed)
    }

    pub fn max_file_key(&self) -> NeedleId {
        NeedleId(self.metric.max_file_key.load(Ordering::Relaxed))
    }

    pub fn max_needle_end(&self) -> i64 {
        self.metric.max_needle_end.load(Ordering::Relaxed)
    }
}

impl Drop for SortedFileNeedleMap {
    fn drop(&mut self) {
        self.close();
    }
}

/// Whether a sorted index is a whole number of entries. A torn one is treated
/// like a stale one and rebuilt: truncation bumps the mtime, so the staleness
/// check alone would trust it, and flooring the entry count would silently hide
/// the last needle — which a later compaction would then drop for good. Go
/// writes `.sdx` in place, so a crash mid-generation can leave one behind.
fn holds_whole_entries(db_file_name: &str) -> bool {
    std::fs::metadata(db_file_name)
        .map(|m| m.len() % NEEDLE_MAP_ENTRY_SIZE as u64 == 0)
        .unwrap_or(false)
}

/// `.sdx` is stale when it is not newer than `.idx`; writes always land in
/// `.idx` first. Mirrors Go's `isSortedFileFresh`.
fn is_sorted_file_fresh(db_file_name: &str, index_file_name: &str) -> bool {
    let (Ok(db), Ok(index)) = (
        std::fs::metadata(db_file_name),
        std::fs::metadata(index_file_name),
    ) else {
        return false;
    };
    match (db.modified(), index.modified()) {
        (Ok(db_time), Ok(index_time)) => db_time > index_time,
        _ => false,
    }
}

/// Rewrite `.idx` as a sorted array of the entries that are still live, the
/// same file Go's `WriteSortedFileFromIdx` produces: last write wins per key,
/// and a tombstoned key is dropped entirely.
fn write_sorted_file_from_idx(
    index_file_name: &str,
    db_file_name: &str,
    version: Version,
) -> io::Result<()> {
    let mut index_file = File::open(index_file_name)?;
    let nm = CompactNeedleMap::load_from_idx(&mut index_file, version)?;

    let tmp_name = format!("{db_file_name}.tmp");
    let mut out = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp_name)?;
    let write_result = nm
        .ascending_visit(|id, nv| {
            idx::write_index_entry(&mut out, id, nv.offset, nv.size).map_err(|e| e.to_string())
        })
        .map_err(io::Error::other)
        .and_then(|()| out.sync_all());
    if let Err(e) = write_result {
        let _ = std::fs::remove_file(&tmp_name);
        return Err(e);
    }
    drop(out);
    // Rename in: a crash mid-generation must not leave a short .sdx that looks
    // fresh and silently hides needles.
    std::fs::rename(&tmp_name, db_file_name)
}

/// Counters equivalent to Go's `needleMapMetricFromIndexFile`, derived without
/// its bloom filter — and so without its false positives or its per-volume
/// allocation.
///
/// Go counts every `.idx` row in `FileCounter`, and counts a row in
/// `DeletionCounter` when it is not the last, still-valid row for its key. The
/// rows that *are* the last valid row for their key are exactly the entries
/// `.sdx` holds, so `DeletionCounter = rows - live` and
/// `DeletionByteCounter = valid bytes in .idx - live bytes in .sdx`, each
/// computable in one sequential pass with no per-key state.
fn index_metric(
    index_file_name: &str,
    db_file_name: &str,
    version: Version,
) -> io::Result<NeedleMapMetric> {
    let metric = NeedleMapMetric::default();

    let mut rows: i64 = 0;
    let mut valid_bytes: u64 = 0;
    let mut index_file = File::open(index_file_name)?;
    idx::walk_index_file(&mut index_file, 0, |key, offset, size| {
        rows += 1;
        metric.maybe_set_max_file_key(key);
        metric.maybe_set_max_needle_end(offset, size, version);
        if size.is_valid() {
            valid_bytes += size.0 as u64;
        }
        Ok(())
    })?;

    let mut live: i64 = 0;
    let mut live_bytes: u64 = 0;
    let mut db_file = File::open(db_file_name)?;
    idx::walk_index_file(&mut db_file, 0, |_, _, size| {
        if size.is_valid() {
            live += 1;
            live_bytes += size.0 as u64;
        }
        Ok(())
    })?;

    metric.file_count.store(rows, Ordering::Relaxed);
    metric.file_byte_count.store(valid_bytes, Ordering::Relaxed);
    metric
        .deletion_count
        .store((rows - live).max(0), Ordering::Relaxed);
    metric
        .deletion_byte_count
        .store(valid_bytes.saturating_sub(live_bytes), Ordering::Relaxed);
    Ok(metric)
}

/// Binary search the sorted index for `key`, returning its entry index along
/// with the record. Mirrors Go's `SearchNeedleFromSortedIndex`.
fn search_sorted_index(
    file: &File,
    file_size: i64,
    key: NeedleId,
) -> io::Result<Option<(u64, Offset, Size)>> {
    let mut lo: u64 = 0;
    let mut hi: u64 = file_size.max(0) as u64 / NEEDLE_MAP_ENTRY_SIZE as u64;
    let mut buf = [0u8; NEEDLE_MAP_ENTRY_SIZE];
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        read_exact_at(file, &mut buf, mid * NEEDLE_MAP_ENTRY_SIZE as u64)?;
        let (entry_key, offset, size) = idx_entry_from_bytes(&buf);
        if entry_key == key {
            return Ok(Some((mid, offset, size)));
        }
        if entry_key < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(None)
}

fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut filled = 0;
        let mut at = offset;
        while filled < buf.len() {
            let n = file.seek_read(&mut buf[filled..], at)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected EOF in seek_read",
                ));
            }
            filled += n;
            at += n as u64;
        }
        Ok(())
    }
}

fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut written = 0;
        let mut at = offset;
        while written < buf.len() {
            let n = file.seek_write(&buf[written..], at)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "seek_write wrote nothing",
                ));
            }
            written += n;
            at += n as u64;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    fn version() -> Version {
        VERSION_3
    }

    /// Write an .idx with the given rows, in order.
    fn write_idx(path: &str, rows: &[(u64, u32, i32)]) {
        let mut f = File::create(path).unwrap();
        for &(key, offset, size) in rows {
            let mut buf = [0u8; NEEDLE_MAP_ENTRY_SIZE];
            idx_entry_to_bytes(
                &mut buf,
                NeedleId(key),
                Offset::from_actual_offset(offset as i64),
                Size(size),
            );
            f.write_all(&buf).unwrap();
        }
        f.sync_all().unwrap();
    }

    fn base(dir: &tempfile::TempDir) -> String {
        dir.path().join("7").to_str().unwrap().to_string()
    }

    #[test]
    fn get_finds_live_needles_and_skips_deleted_ones() {
        let dir = tempfile::tempdir().unwrap();
        let base = base(&dir);
        write_idx(
            &format!("{base}.idx"),
            &[
                (1, 8, 100),
                (2, 16, 200),
                (3, 24, 300),
                (2, 0, -1), // tombstone
            ],
        );

        let m = SortedFileNeedleMap::open(&base, version()).unwrap();
        assert_eq!(m.get(NeedleId(1)).unwrap().size, Size(100));
        assert_eq!(m.get(NeedleId(3)).unwrap().size, Size(300));
        // A key tombstoned before the .sdx was written is not in it at all.
        assert!(m.get(NeedleId(2)).is_none());
        assert!(m.get(NeedleId(99)).is_none());
    }

    #[test]
    fn metrics_match_the_go_counters() {
        let dir = tempfile::tempdir().unwrap();
        let base = base(&dir);
        // key 1 rewritten once, key 2 deleted, key 3 live.
        write_idx(
            &format!("{base}.idx"),
            &[
                (1, 8, 100),
                (2, 16, 200),
                (1, 32, 150),
                (3, 24, 300),
                (2, 0, -1),
            ],
        );

        let m = SortedFileNeedleMap::open(&base, version()).unwrap();
        // Every row counts, matching Go's FileCounter.
        assert_eq!(m.file_count(), 5);
        assert_eq!(m.content_size(), 100 + 200 + 150 + 300);
        // Superseded row for key 1, plus both rows for the deleted key 2.
        assert_eq!(m.deleted_count(), 3);
        assert_eq!(m.deleted_size(), 100 + 200);
        assert_eq!(m.max_file_key(), NeedleId(3));
    }

    #[test]
    fn put_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let base = base(&dir);
        write_idx(&format!("{base}.idx"), &[(1, 8, 100)]);

        let mut m = SortedFileNeedleMap::open(&base, version()).unwrap();
        let err = m
            .put(NeedleId(2), Offset::from_actual_offset(16), Size(10))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn delete_appends_a_tombstone_to_the_idx_tail() {
        let dir = tempfile::tempdir().unwrap();
        let base = base(&dir);
        let idx_path = format!("{base}.idx");
        write_idx(&idx_path, &[(1, 8, 100), (2, 16, 200), (3, 24, 300)]);
        let before = std::fs::metadata(&idx_path).unwrap().len();

        let m = SortedFileNeedleMap::open(&base, version()).unwrap();
        assert_eq!(
            m.delete(NeedleId(2), Offset::from_actual_offset(16))
                .unwrap(),
            Some(Size(200))
        );

        // Appended, not overwritten from the front.
        let after = std::fs::metadata(&idx_path).unwrap().len();
        assert_eq!(after, before + NEEDLE_MAP_ENTRY_SIZE as u64);
        assert_eq!(m.index_file_size(), after);

        let mut rows = Vec::new();
        let mut f = File::open(&idx_path).unwrap();
        idx::walk_index_file(&mut f, 0, |key, _, size| {
            rows.push((key, size));
            Ok(())
        })
        .unwrap();
        assert_eq!(rows[0], (NeedleId(1), Size(100)));
        assert_eq!(rows[1], (NeedleId(2), Size(200)));
        assert_eq!(rows[2], (NeedleId(3), Size(300)));
        assert_eq!(rows[3], (NeedleId(2), TOMBSTONE_FILE_SIZE));

        // .sdx is marked in place, so the needle reads back as a tombstone
        // without a reload — the same contract Go's Get has, where callers
        // check size.is_deleted().
        assert!(m.get(NeedleId(2)).unwrap().size.is_deleted());
        assert!(m
            .delete(NeedleId(2), Offset::from_actual_offset(16))
            .unwrap()
            .is_none());
        assert!(!m.get(NeedleId(1)).unwrap().size.is_deleted());
    }

    #[test]
    fn delete_moves_the_counters_where_a_reload_would() {
        let dir = tempfile::tempdir().unwrap();
        let base = base(&dir);
        write_idx(
            &format!("{base}.idx"),
            &[(1, 8, 100), (2, 16, 200), (3, 24, 300)],
        );

        let m = SortedFileNeedleMap::open(&base, version()).unwrap();
        assert_eq!(
            (m.file_count(), m.deleted_count(), m.deleted_size()),
            (3, 0, 0)
        );

        m.delete(NeedleId(2), Offset::from_actual_offset(16))
            .unwrap();
        let live = m.file_count() - m.deleted_count();
        assert_eq!(live, 2, "one of the three needles is gone");
        assert_eq!(m.deleted_size(), 200);
        assert_eq!(m.content_size(), 600, "a delete does not shrink .idx bytes");

        // A reload recomputes from .idx and .sdx; the runtime numbers must
        // already agree with it or the heartbeat jumps at every restart.
        drop(m);
        let reloaded = SortedFileNeedleMap::open(&base, version()).unwrap();
        assert_eq!(reloaded.file_count(), 4);
        assert_eq!(reloaded.deleted_count(), 2);
        assert_eq!(reloaded.deleted_size(), 200);
        assert_eq!(reloaded.content_size(), 600);

        // Deleting again is a no-op and must not move them further.
        reloaded
            .delete(NeedleId(2), Offset::from_actual_offset(16))
            .unwrap();
        assert_eq!((reloaded.file_count(), reloaded.deleted_count()), (4, 2));
    }

    #[test]
    fn a_torn_idx_tail_does_not_misalign_appends() {
        let dir = tempfile::tempdir().unwrap();
        let base = base(&dir);
        let idx_path = format!("{base}.idx");
        write_idx(&idx_path, &[(1, 8, 100), (2, 16, 200)]);
        // A short write left half a record behind. Seeding the append offset
        // from the raw file length would put every later row off by five bytes.
        {
            use std::io::Write as _;
            let mut f = OpenOptions::new().append(true).open(&idx_path).unwrap();
            f.write_all(&[0xab; 5]).unwrap();
        }

        let m = SortedFileNeedleMap::open(&base, version()).unwrap();
        assert_eq!(m.index_file_size(), 2 * NEEDLE_MAP_ENTRY_SIZE as u64);
        m.delete(NeedleId(1), Offset::from_actual_offset(8))
            .unwrap();

        let mut rows = Vec::new();
        let mut f = File::open(&idx_path).unwrap();
        idx::walk_index_file(&mut f, 0, |key, _, size| {
            rows.push((key, size));
            Ok(())
        })
        .unwrap();
        assert_eq!(
            std::fs::metadata(&idx_path).unwrap().len(),
            3 * NEEDLE_MAP_ENTRY_SIZE as u64,
            "the tombstone should have overwritten the torn tail"
        );
        assert_eq!(rows[0], (NeedleId(1), Size(100)));
        assert_eq!(rows[1], (NeedleId(2), Size(200)));
        assert_eq!(rows[2], (NeedleId(1), TOMBSTONE_FILE_SIZE));
    }

    #[test]
    fn torn_sdx_is_regenerated() {
        let dir = tempfile::tempdir().unwrap();
        let base = base(&dir);
        write_idx(
            &format!("{base}.idx"),
            &[(1, 8, 100), (2, 16, 200), (3, 24, 300)],
        );

        // Build a good .sdx, then lop off part of its last record. Truncation
        // bumps the mtime, so the staleness check sees a *fresher* file than the
        // .idx — without the whole-entry check the map would trust it and needle
        // 3 would disappear.
        drop(SortedFileNeedleMap::open(&base, version()).unwrap());
        let good = std::fs::metadata(format!("{base}.sdx")).unwrap().len();
        assert_eq!(good, 3 * NEEDLE_MAP_ENTRY_SIZE as u64);
        let sdx = OpenOptions::new()
            .write(true)
            .open(format!("{base}.sdx"))
            .unwrap();
        sdx.set_len(good - 5).unwrap();
        drop(sdx);
        pooled_index_files().discard(&format!("{base}.sdx"));
        assert!(
            std::fs::metadata(format!("{base}.sdx"))
                .unwrap()
                .modified()
                .unwrap()
                > std::fs::metadata(format!("{base}.idx"))
                    .unwrap()
                    .modified()
                    .unwrap(),
            "the torn file must look fresh, or the test proves nothing"
        );

        let m = SortedFileNeedleMap::open(&base, version()).unwrap();
        assert_eq!(
            std::fs::metadata(format!("{base}.sdx")).unwrap().len(),
            good
        );
        assert_eq!(m.get(NeedleId(3)).unwrap().size, Size(300));
        assert_eq!(m.iter_entries().unwrap().len(), 3);
    }

    #[test]
    fn stale_sdx_is_regenerated() {
        let dir = tempfile::tempdir().unwrap();
        let base = base(&dir);
        write_idx(&format!("{base}.idx"), &[(1, 8, 100)]);
        // A short, stale .sdx from an older .idx must not be trusted.
        std::fs::write(format!("{base}.sdx"), b"").unwrap();
        filetime_backdate(&format!("{base}.sdx"));

        let m = SortedFileNeedleMap::open(&base, version()).unwrap();
        assert_eq!(m.get(NeedleId(1)).unwrap().size, Size(100));
    }

    #[test]
    fn scan_spans_read_batches() {
        let dir = tempfile::tempdir().unwrap();
        let base = base(&dir);
        // Straddles ROWS_TO_READ so the batched scan has to stitch a full
        // batch, a second full batch and a short tail together.
        let total = ROWS_TO_READ as u32 * 2 + 7;
        let rows: Vec<(u64, u32, i32)> = (1..=total)
            .map(|i| (i as u64, i * 8, (i * 10) as i32))
            .collect();
        write_idx(&format!("{base}.idx"), &rows);

        let m = SortedFileNeedleMap::open(&base, version()).unwrap();
        let entries = m.iter_entries().unwrap();
        assert_eq!(entries.len(), total as usize);
        assert_eq!(entries[0].0, NeedleId(1));
        assert_eq!(entries[entries.len() - 1].0, NeedleId(total as u64));
        assert_eq!(entries[entries.len() - 1].1.size, Size((total * 10) as i32));
        // Every id is still reachable through the binary search.
        assert_eq!(
            m.get(NeedleId(total as u64)).unwrap().size,
            Size((total * 10) as i32)
        );
    }

    #[test]
    fn iter_entries_reports_a_truncated_sdx() {
        let dir = tempfile::tempdir().unwrap();
        let base = base(&dir);
        write_idx(
            &format!("{base}.idx"),
            &[(1, 8, 100), (2, 16, 200), (3, 24, 300), (4, 32, 400)],
        );

        let m = SortedFileNeedleMap::open(&base, version()).unwrap();
        assert_eq!(m.iter_entries().unwrap().len(), 4);

        // Losing the tail of .sdx must surface as an error, not as a shorter
        // list: compaction takes this vector for the complete live set and would
        // otherwise commit a volume missing the needles it could not read.
        let sdx = OpenOptions::new()
            .write(true)
            .open(format!("{base}.sdx"))
            .unwrap();
        sdx.set_len(2 * NEEDLE_MAP_ENTRY_SIZE as u64).unwrap();
        drop(sdx);
        pooled_index_files().discard(m.db_file_name());

        let err = m.iter_entries().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert!(m.save_to_idx(&format!("{base}.check")).is_err());
    }

    #[test]
    fn holds_no_descriptors_when_idle() {
        let dir = tempfile::tempdir().unwrap();
        let base = base(&dir);
        write_idx(&format!("{base}.idx"), &[(1, 8, 100), (2, 16, 200)]);

        let m = SortedFileNeedleMap::open(&base, version()).unwrap();
        let Some(_) = super::super::file_pool::open_index_fds(dir.path()) else {
            return; // cannot enumerate descriptors here
        };
        drop_pooled(&m);
        assert_eq!(
            super::super::file_pool::open_index_fds(dir.path()),
            Some(0),
            "an idle sorted map must hold no .idx/.sdx descriptor"
        );

        assert!(m.get(NeedleId(1)).is_some());
        drop_pooled(&m);
        assert_eq!(
            super::super::file_pool::open_index_fds(dir.path()),
            Some(0),
            "a lookup must give its borrowed handle back"
        );
    }

    fn drop_pooled(m: &SortedFileNeedleMap) {
        pooled_index_files().discard(m.index_file_name());
        pooled_index_files().discard(m.db_file_name());
    }

    fn filetime_backdate(path: &str) {
        // Push the mtime into the past so the freshness check sees it as stale
        // even when both files were written within the same clock tick.
        let past = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1);
        let f = OpenOptions::new().write(true).open(path).unwrap();
        let _ = f.set_times(std::fs::FileTimes::new().set_modified(past));
    }
}

// Only meaningful with 5-byte offsets, which is what production Go builds use.
#[cfg(all(test, feature = "5bytes"))]
mod go_parity_tests {
    use super::*;

    /// The `.sdx` bytes Go's `WriteSortedFileFromIdx` produces for the fixture
    /// below, captured from `weed/storage/erasure_coding` built with
    /// `-tags 5BytesOffset`. A volume moved between a Go and a Rust server reads
    /// whichever `.sdx` is already on disk, so the two generators must agree
    /// byte for byte: sorted by needle id, last write wins, tombstoned keys
    /// dropped entirely.
    const GO_SDX_HEX: &str = "0000000000000001000000200000000096000000000000000300000018000000012c00000000000000040000003000000001900000000000000005000000080000\
0001f4";

    #[test]
    fn sdx_matches_the_bytes_go_writes() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("1").to_str().unwrap().to_string();

        // Rewrites, tombstones and out-of-order keys, matching the Go fixture.
        let rows: &[(u64, u32, i32)] = &[
            (5, 8, 500),
            (1, 16, 100),
            (3, 24, 300),
            (1, 32, 150),
            (2, 40, 200),
            (2, 0, -1),
            (4, 48, 400),
            (9, 56, 900),
            (9, 0, -1),
        ];
        let mut f = std::fs::File::create(format!("{base}.idx")).unwrap();
        for &(key, offset, size) in rows {
            let mut buf = [0u8; NEEDLE_MAP_ENTRY_SIZE];
            // The Go fixture stores raw offsets; scale by the padding so both
            // sides put the same bytes in the entry.
            idx_entry_to_bytes(
                &mut buf,
                NeedleId(key),
                Offset::from_actual_offset(offset as i64 * NEEDLE_PADDING_SIZE as i64),
                Size(size),
            );
            use std::io::Write as _;
            f.write_all(&buf).unwrap();
        }
        drop(f);

        write_sorted_file_from_idx(&format!("{base}.idx"), &format!("{base}.sdx"), VERSION_3)
            .unwrap();
        let sdx = std::fs::read(format!("{base}.sdx")).unwrap();
        let hex: String = sdx.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, GO_SDX_HEX.replace('\n', ""));
    }
}
