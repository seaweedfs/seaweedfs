//! NeedleMapper: index mapping NeedleId -> (Offset, Size).
//!
//! Two implementations:
//!   - `CompactNeedleMap`: in-memory segmented sorted arrays (~10 bytes/entry)
//!   - `RedbNeedleMap`: disk-backed via redb (low RAM, slightly slower)
//!
//! The `NeedleMap` enum wraps both and provides a uniform interface.
//! Loaded from .idx file on volume mount. Supports Get, Put, Delete with
//! metrics tracking (file count, byte count, deleted count, deleted bytes).

use std::collections::HashMap;
use std::io::{self, Read, Seek, Write};
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

mod compact_map;
pub mod file_pool;
mod idx_metric;
pub mod sorted_file;
use compact_map::CompactMap;
use idx_metric::metrics_from_idx;
use sorted_file::SortedFileNeedleMap;

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};

use crate::storage::idx;
use crate::storage::needle::needle::get_actual_size;
use crate::storage::types::*;

// ============================================================================
// NeedleValue
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NeedleValue {
    pub offset: Offset,
    pub size: Size,
}

/// Packed size of a NeedleValue in redb storage: OFFSET_SIZE + SIZE_SIZE.
const PACKED_NEEDLE_VALUE_SIZE: usize = OFFSET_SIZE + SIZE_SIZE;

/// Pack an (Offset, Size) pair into bytes for redb storage.
/// Layout: [offset OFFSET_SIZE bytes] [size 4 bytes big-endian]
fn pack_needle_value(nv: &NeedleValue) -> [u8; PACKED_NEEDLE_VALUE_SIZE] {
    let mut buf = [0u8; PACKED_NEEDLE_VALUE_SIZE];
    nv.offset.to_bytes(&mut buf[..OFFSET_SIZE]);
    nv.size.to_bytes(&mut buf[OFFSET_SIZE..]);
    buf
}

/// Unpack bytes from redb storage into (Offset, Size).
fn unpack_needle_value(bytes: &[u8; PACKED_NEEDLE_VALUE_SIZE]) -> NeedleValue {
    NeedleValue {
        offset: Offset::from_bytes(&bytes[..OFFSET_SIZE]),
        size: Size::from_bytes(&bytes[OFFSET_SIZE..]),
    }
}

// ============================================================================
// NeedleMapMetric
// ============================================================================

/// Metrics tracking for needle map operations.
#[derive(Debug, Default)]
pub struct NeedleMapMetric {
    pub file_count: AtomicI64,
    pub file_byte_count: AtomicU64,
    pub deletion_count: AtomicI64,
    pub deletion_byte_count: AtomicU64,
    pub max_file_key: AtomicU64,
    /// Largest (offset.to_actual_offset() + get_actual_size(size, version))
    /// observed during the load walk. Used at volume load to verify that no
    /// .idx entry references bytes past the end of .dat (issue #8928)
    /// without paying for a second linear scan.
    pub max_needle_end: AtomicI64,
}

impl NeedleMapMetric {
    /// Update metrics based on a Put operation (additive-only, matching Go's logPut).
    fn on_put(&self, key: NeedleId, old: Option<&NeedleValue>, new_size: Size) {
        self.maybe_set_max_file_key(key);
        // Go: always LogFileCounter(newSize) which does FileCounter++ and FileByteCounter += newSize
        self.file_count.fetch_add(1, Ordering::Relaxed);
        self.file_byte_count
            .fetch_add(new_size.0 as u64, Ordering::Relaxed);
        // Go: if oldSize > 0 && oldSize.IsValid() { LogDeletionCounter(oldSize) }
        if let Some(old_val) = old {
            if old_val.size.0 > 0 && old_val.size.is_valid() {
                self.deletion_count.fetch_add(1, Ordering::Relaxed);
                self.deletion_byte_count
                    .fetch_add(old_val.size.0 as u64, Ordering::Relaxed);
            }
        }
    }

    /// Update metrics based on a Delete operation (additive-only, matching Go's logDelete).
    fn on_delete(&self, old: &NeedleValue) {
        if old.size.0 > 0 {
            self.deletion_count.fetch_add(1, Ordering::Relaxed);
            self.deletion_byte_count
                .fetch_add(old.size.0 as u64, Ordering::Relaxed);
        }
    }

    fn maybe_set_max_file_key(&self, key: NeedleId) {
        let key_val: u64 = key.into();
        loop {
            let current = self.max_file_key.load(Ordering::Relaxed);
            if key_val <= current {
                break;
            }
            if self
                .max_file_key
                .compare_exchange(current, key_val, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Update `max_needle_end` if this entry's (offset + actual size) exceeds
    /// the running maximum. Skips deleted/zero-offset entries because they
    /// don't reserve space in .dat.
    fn maybe_set_max_needle_end(&self, offset: Offset, size: Size, version: Version) {
        if offset.is_zero() || !size.is_valid() {
            return;
        }
        let end = offset.to_actual_offset() + get_actual_size(size, version);
        loop {
            let current = self.max_needle_end.load(Ordering::Relaxed);
            if end <= current {
                break;
            }
            if self
                .max_needle_end
                .compare_exchange(current, end, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }
}

// ============================================================================
// NeedleMapKind
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NeedleMapKind {
    InMemory,
    Redb,
    RedbMedium,
    RedbLarge,
}

impl NeedleMapKind {
    /// Bytes of redb page cache to give ONE volume's index database.
    ///
    /// A volume server opens a separate redb database per volume, so the
    /// process-wide ceiling is roughly (open volumes x this budget). redb's
    /// own default is 1 GiB per database, which with hundreds of volumes
    /// grows without practical bound as traffic touches them (#11179).
    /// These tiers mirror the Go server's LevelDB sizing (block cache +
    /// write buffer of 3/6/12 MiB), rounded to powers of two.
    pub fn redb_cache_bytes(self) -> usize {
        const MIB: usize = 1024 * 1024;
        match self {
            // InMemory never opens redb; return the smallest tier so a
            // caller that does not branch on kind still gets a sane bound.
            NeedleMapKind::InMemory | NeedleMapKind::Redb => 4 * MIB,
            NeedleMapKind::RedbMedium => 8 * MIB,
            NeedleMapKind::RedbLarge => 16 * MIB,
        }
    }
}

// ============================================================================
// IdxFileWriter trait
// ============================================================================

/// Trait for appending to an index file.
pub trait IdxFileWriter: Write + Send + Sync {
    fn sync_all(&self) -> io::Result<()>;
}

impl IdxFileWriter for std::fs::File {
    fn sync_all(&self) -> io::Result<()> {
        std::fs::File::sync_all(self)
    }
}

// ============================================================================
// CompactNeedleMap (in-memory)
// ============================================================================

/// In-memory needle map backed by a CompactMap (segmented sorted arrays).
/// Uses ~10 bytes per entry instead of ~40-48 bytes with HashMap.
/// The .idx file is kept open for append-only writes.
pub struct CompactNeedleMap {
    map: CompactMap,
    metric: NeedleMapMetric,
    idx_file: Option<Box<dyn IdxFileWriter>>,
    idx_file_offset: u64,
}

impl CompactNeedleMap {
    /// Create a new empty in-memory map.
    pub fn new() -> Self {
        CompactNeedleMap {
            map: CompactMap::new(),
            metric: NeedleMapMetric::default(),
            idx_file: None,
            idx_file_offset: 0,
        }
    }

    /// Load from an .idx file, building the in-memory map.
    pub fn load_from_idx<R: Read + Seek>(reader: &mut R, version: Version) -> io::Result<Self> {
        let mut nm = CompactNeedleMap::new();
        idx::walk_index_file(reader, 0, |key, offset, size| {
            nm.metric.maybe_set_max_needle_end(offset, size, version);
            if offset.is_zero() || size.is_deleted() {
                nm.delete_from_map(key);
            } else {
                nm.set_internal(key, NeedleValue { offset, size });
            }
            Ok(())
        })?;
        Ok(nm)
    }

    /// Set the index file for append-only writes.
    pub fn set_idx_file(&mut self, file: Box<dyn IdxFileWriter>, offset: u64) {
        self.idx_file = Some(file);
        self.idx_file_offset = offset;
    }

    /// True when an .idx file writer is attached. A read-only load leaves
    /// this `false` — set_writable() must reattach a writer or subsequent
    /// puts silently skip the disk append.
    pub fn has_idx_writer(&self) -> bool {
        self.idx_file.is_some()
    }

    // ---- Map operations ----

    /// Insert or update an entry. Appends to .idx file if present.
    pub fn put(&mut self, key: NeedleId, offset: Offset, size: Size) -> io::Result<()> {
        // Persist to idx file BEFORE mutating in-memory state for crash consistency
        if let Some(ref mut idx_file) = self.idx_file {
            idx::write_index_entry(idx_file, key, offset, size)?;
            self.idx_file_offset += NEEDLE_MAP_ENTRY_SIZE as u64;
        }

        let old = self.map.get(key);
        self.metric.on_put(key, old.as_ref(), size);
        self.map.set(key, offset, size);
        Ok(())
    }

    /// Look up a needle.
    pub fn get(&self, key: NeedleId) -> Option<NeedleValue> {
        self.map.get(key)
    }

    /// Mark a needle as deleted. Appends tombstone to .idx file.
    /// Matches Go's NeedleMap.Delete: ALWAYS writes tombstone to idx and
    /// increments deletion counter, even if needle doesn't exist or is
    /// already deleted (important for replication).
    pub fn delete(&mut self, key: NeedleId, offset: Offset) -> io::Result<Option<Size>> {
        // Go unconditionally calls nm.m.Delete(), nm.logDelete(), nm.appendToIndexFile()
        let deleted_bytes = self.map.delete(key).unwrap_or(Size(0));

        // Match Go's logDelete -> LogDeletionCounter: only increment when oldSize > 0.
        // Go does NOT decrement FileCounter/FileByteCounter in Delete;
        // live counts are computed as FileCounter - DeletionCounter.
        if deleted_bytes.0 > 0 {
            self.metric.deletion_count.fetch_add(1, Ordering::Relaxed);
            self.metric
                .deletion_byte_count
                .fetch_add(deleted_bytes.0 as u64, Ordering::Relaxed);
        }

        // Always write tombstone to idx file (matching Go)
        if let Some(ref mut idx_file) = self.idx_file {
            idx::write_index_entry(idx_file, key, offset, TOMBSTONE_FILE_SIZE)?;
            self.idx_file_offset += NEEDLE_MAP_ENTRY_SIZE as u64;
        }

        if deleted_bytes.0 > 0 {
            Ok(Some(deleted_bytes))
        } else {
            Ok(None)
        }
    }

    // ---- Internal helpers ----

    /// Insert into map during loading (no idx file write).
    fn set_internal(&mut self, key: NeedleId, nv: NeedleValue) {
        let old = self.map.get(key);
        self.metric.on_put(key, old.as_ref(), nv.size);
        self.map.set(key, nv.offset, nv.size);
    }

    /// Remove from map during loading (handle deletions in idx walk).
    /// Matches Go's doLoading else branch: always increments DeletionCounter,
    /// and adds old size bytes to DeletionByteCounter.
    fn delete_from_map(&mut self, key: NeedleId) {
        self.metric.maybe_set_max_file_key(key);
        // Go's CompactMap.Delete returns old size (0 if not found or already deleted).
        // Go's doLoading always does DeletionCounter++ and DeletionByteCounter += uint64(oldSize).
        let old_size = self.map.get(key).map(|nv| nv.size).unwrap_or(Size(0));
        // Go unconditionally increments DeletionCounter
        self.metric.deletion_count.fetch_add(1, Ordering::Relaxed);
        // Go adds uint64(oldSize) which for valid sizes adds the value, for 0/negative adds 0
        if old_size.0 > 0 {
            self.metric
                .deletion_byte_count
                .fetch_add(old_size.0 as u64, Ordering::Relaxed);
        }
        self.map.remove(key);
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

    /// Largest (offset + actual size) seen during the load walk; 0 if the
    /// map is empty. See `NeedleMapMetric::maybe_set_max_needle_end`.
    pub fn max_needle_end(&self) -> i64 {
        self.metric.max_needle_end.load(Ordering::Relaxed)
    }

    pub fn index_file_size(&self) -> u64 {
        self.idx_file_offset
    }

    /// Sync index file to disk.
    pub fn sync(&self) -> io::Result<()> {
        if let Some(ref idx_file) = self.idx_file {
            idx_file.sync_all()?;
        }
        Ok(())
    }

    /// Close index file.
    pub fn close(&mut self) {
        let _ = self.sync();
        self.idx_file = None;
    }

    /// Save the in-memory map to an index file, sorted by needle ID ascending.
    pub fn save_to_idx(&self, path: &str) -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        self.map.ascending_visit(|id, nv| {
            if nv.size.is_valid() {
                idx::write_index_entry(&mut file, id, nv.offset, nv.size)
            } else {
                Ok(())
            }
        })?;
        file.sync_all()?;
        Ok(())
    }

    /// Visit all entries in ascending order by needle ID.
    pub fn ascending_visit<F>(&self, f: F) -> Result<(), String>
    where
        F: FnMut(NeedleId, &NeedleValue) -> Result<(), String>,
    {
        self.map.ascending_visit(f)
    }
}

// ============================================================================
// RedbNeedleMap (disk-backed via redb)
// ============================================================================

/// redb table: NeedleId (u64) -> packed [offset(4) + size(4)]
const NEEDLE_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("needles");

/// Metadata table: stores the .idx file size that was used to build this redb.
/// Key "idx_size" -> u64 byte offset. Used to detect whether the .rdb can be
/// reused on restart or needs a full/incremental rebuild.
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("meta");
const META_IDX_SIZE: &str = "idx_size";

/// Writes between two durable redb checkpoints. Every non-durable commit
/// leaves an entry in redb's transaction tracker and pins the pages that
/// were on disk at the last durable commit; only a durable commit clears
/// both. Without a cadence they grow for the life of the process (#11179).
/// The map only counts; the volume takes the checkpoint (see
/// `RedbNeedleMap::checkpoint_due`) because the .dat must be flushed first.
const REDB_CHECKPOINT_INTERVAL: u32 = 1000;

/// Disk-backed needle map using redb.
/// Low memory usage — data lives on disk behind a small, bounded redb page
/// cache sized by `NeedleMapKind::redb_cache_bytes`.
pub struct RedbNeedleMap {
    db: Database,
    metric: NeedleMapMetric,
    idx_file: Option<Box<dyn IdxFileWriter>>,
    idx_file_offset: u64,
    /// Puts/deletes since the last durable checkpoint.
    writes_since_checkpoint: u32,
}

impl RedbNeedleMap {
    /// Begin a write transaction with `Durability::None` (no fsync).
    /// The .idx file is the source of truth for crash recovery: a crash
    /// loses at most the writes since the last checkpoint from redb, and
    /// the next load replays them from .idx.
    fn begin_write_no_fsync(db: &Database) -> io::Result<redb::WriteTransaction> {
        let mut txn = db.begin_write().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("redb begin_write: {}", e))
        })?;
        let _ = txn.set_durability(Durability::None);
        Ok(txn)
    }

    /// True once `REDB_CHECKPOINT_INTERVAL` puts/deletes have been committed
    /// non-durably. The caller (the volume) must then flush the .dat and
    /// call [`checkpoint`](Self::checkpoint): a checkpoint makes the index
    /// durable, so the bytes it points at have to be on disk before it.
    pub fn checkpoint_due(&self) -> bool {
        self.writes_since_checkpoint >= REDB_CHECKPOINT_INTERVAL
    }

    /// Make the table durable and record how much of the .idx it reflects.
    /// Precondition: the .dat the index points into has been flushed.
    pub fn checkpoint(&mut self) -> io::Result<()> {
        let txn = self.begin_checkpoint()?;
        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb commit: {}", e)))?;
        self.writes_since_checkpoint = 0;
        Ok(())
    }

    /// Begin a durable transaction (fsync on commit) that also records how
    /// much of the .idx the table reflects, so a reload replays only the
    /// tail appended after it. The .idx is synced first: the recorded size
    /// must never exceed what is on disk, or the reload would have to
    /// rebuild from scratch.
    fn begin_checkpoint(&self) -> io::Result<redb::WriteTransaction> {
        self.sync()?;
        let txn = self.db.begin_write().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("redb begin_write: {}", e))
        })?;
        if self.idx_file.is_some() {
            let mut meta = txn.open_table(META_TABLE).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("redb open meta: {}", e))
            })?;
            meta.insert(META_IDX_SIZE, self.idx_file_offset).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("redb insert meta: {}", e))
            })?;
        }
        Ok(txn)
    }

    /// Create a new redb-backed needle map at the given path.
    /// The database file will be created if it does not exist.
    /// `cache_bytes` bounds redb's page cache for this one database.
    pub fn new(db_path: &str, cache_bytes: usize) -> io::Result<Self> {
        let db = Database::builder()
            .set_cache_size(cache_bytes)
            .create(db_path)
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("redb create error: {}", e))
            })?;

        // Ensure tables exist
        let txn = Self::begin_write_no_fsync(&db)?;
        {
            let _table = txn.open_table(NEEDLE_TABLE).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("redb open_table: {}", e))
            })?;
            let _meta = txn.open_table(META_TABLE).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("redb open_table meta: {}", e))
            })?;
        }
        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb commit: {}", e)))?;

        Ok(RedbNeedleMap {
            db,
            metric: NeedleMapMetric::default(),
            idx_file: None,
            idx_file_offset: 0,
            writes_since_checkpoint: 0,
        })
    }

    /// Save the .idx file size into redb metadata so we can detect whether
    /// the .rdb is up-to-date on the next startup.
    fn save_idx_size_meta(&self, idx_size: u64) -> io::Result<()> {
        let txn = Self::begin_write_no_fsync(&self.db)?;
        {
            let mut meta = txn.open_table(META_TABLE).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("redb open meta: {}", e))
            })?;
            meta.insert(META_IDX_SIZE, idx_size).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("redb insert meta: {}", e))
            })?;
        }
        txn.commit().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("redb commit meta: {}", e))
        })?;
        Ok(())
    }

    /// Read the stored .idx file size from redb metadata.
    fn read_idx_size_meta(&self) -> io::Result<Option<u64>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb begin_read: {}", e)))?;
        let meta = txn
            .open_table(META_TABLE)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb open meta: {}", e)))?;
        match meta.get(META_IDX_SIZE) {
            Ok(Some(guard)) => Ok(Some(guard.value())),
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("redb get meta: {}", e),
            )),
        }
    }

    /// Load from an .idx file, reusing an existing .rdb if it is consistent.
    ///
    /// Strategy:
    /// 1. Try to open existing .rdb and read its stored .idx size
    /// 2. If .idx size matches → reuse .rdb, rebuild metrics from scan
    /// 3. If .idx is larger → replay new entries incrementally
    /// 4. Otherwise (missing, corrupted, .idx smaller) → full rebuild
    pub fn load_from_idx<R: Read + Seek>(
        db_path: &str,
        reader: &mut R,
        version: Version,
        cache_bytes: usize,
    ) -> io::Result<Self> {
        let idx_size = reader.seek(io::SeekFrom::End(0))?;
        reader.seek(io::SeekFrom::Start(0))?;

        // Try to reuse existing .rdb
        if Path::new(db_path).exists() {
            if let Ok(nm) = Self::try_reuse_rdb(db_path, reader, idx_size, version, cache_bytes) {
                return Ok(nm);
            }
            // Reuse failed — fall through to full rebuild
            reader.seek(io::SeekFrom::Start(0))?;
        }

        Self::full_rebuild(db_path, reader, idx_size, version, cache_bytes)
    }

    /// Try to reuse an existing .rdb file. Returns Ok if successful,
    /// Err if a full rebuild is needed.
    fn try_reuse_rdb<R: Read + Seek>(
        db_path: &str,
        reader: &mut R,
        idx_size: u64,
        version: Version,
        cache_bytes: usize,
    ) -> io::Result<Self> {
        let db = Database::builder()
            .set_cache_size(cache_bytes)
            .open(db_path)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb open: {}", e)))?;

        let mut nm = RedbNeedleMap {
            db,
            metric: NeedleMapMetric::default(),
            idx_file: None,
            idx_file_offset: 0,
            writes_since_checkpoint: 0,
        };

        let stored_idx_size = nm
            .read_idx_size_meta()?
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no idx_size in redb meta"))?;

        if stored_idx_size > idx_size {
            // .idx shrank — corrupted or truncated, need full rebuild
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "idx file smaller than stored size",
            ));
        }

        // Counters come from the whole .idx history, never from the table,
        // so the replay below is free to re-apply rows the table already
        // holds (redb flushes on drop even without a checkpoint).
        nm.metric = metrics_from_idx(reader, version)?;

        if stored_idx_size < idx_size {
            // .idx grew — replay new entries incrementally
            let start_entry = stored_idx_size / NEEDLE_MAP_ENTRY_SIZE as u64;
            let txn = Self::begin_write_no_fsync(&nm.db)?;
            {
                let mut table = txn.open_table(NEEDLE_TABLE).map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("redb open_table: {}", e))
                })?;
                idx::walk_index_file(reader, start_entry, |key, offset, size| {
                    let key_u64: u64 = key.into();
                    if offset.is_zero() || size.is_deleted() {
                        // Delete: store a tombstone (negative size, original
                        // offset) over a live value; already deleted is a no-op.
                        if let Ok(Some(old)) = nm.get_via_table(&table, key_u64) {
                            if old.size.is_valid() {
                                let deleted_nv = NeedleValue {
                                    offset: old.offset,
                                    size: Size(-(old.size.0)),
                                };
                                let packed = pack_needle_value(&deleted_nv);
                                table.insert(key_u64, packed.as_slice()).map_err(|e| {
                                    io::Error::new(
                                        io::ErrorKind::Other,
                                        format!("redb insert: {}", e),
                                    )
                                })?;
                            }
                        }
                    } else {
                        let packed = pack_needle_value(&NeedleValue { offset, size });
                        table.insert(key_u64, packed.as_slice()).map_err(|e| {
                            io::Error::new(io::ErrorKind::Other, format!("redb insert: {}", e))
                        })?;
                    }
                    Ok(())
                })?;
            }
            txn.commit()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb commit: {}", e)))?;

            nm.save_idx_size_meta(idx_size)?;
        }

        Ok(nm)
    }

    /// Look up a needle value using an already-open table reference.
    /// Used during incremental replay to avoid opening separate read transactions.
    fn get_via_table(
        &self,
        table: &redb::Table<u64, &[u8]>,
        key_u64: u64,
    ) -> io::Result<Option<NeedleValue>> {
        match table.get(key_u64) {
            Ok(Some(guard)) => {
                let bytes: &[u8] = guard.value();
                if bytes.len() == PACKED_NEEDLE_VALUE_SIZE {
                    let mut arr = [0u8; PACKED_NEEDLE_VALUE_SIZE];
                    arr.copy_from_slice(bytes);
                    Ok(Some(unpack_needle_value(&arr)))
                } else {
                    Ok(None)
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("redb get: {}", e),
            )),
        }
    }

    /// Full rebuild: delete existing .rdb and rebuild from entire .idx file.
    fn full_rebuild<R: Read + Seek>(
        db_path: &str,
        reader: &mut R,
        idx_size: u64,
        version: Version,
        cache_bytes: usize,
    ) -> io::Result<Self> {
        let _ = std::fs::remove_file(db_path);
        let mut nm = RedbNeedleMap::new(db_path, cache_bytes)?;

        // Collect entries from idx file, resolving duplicates/deletions
        let mut entries: HashMap<NeedleId, Option<NeedleValue>> = HashMap::new();
        idx::walk_index_file(reader, 0, |key, offset, size| {
            if offset.is_zero() || size.is_deleted() {
                entries.insert(key, None);
            } else {
                entries.insert(key, Some(NeedleValue { offset, size }));
            }
            Ok(())
        })?;

        // Write all live entries to redb in a single transaction
        let txn = Self::begin_write_no_fsync(&nm.db)?;
        {
            let mut table = txn.open_table(NEEDLE_TABLE).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("redb open_table: {}", e))
            })?;

            for (key, maybe_nv) in &entries {
                let key_u64: u64 = (*key).into();
                if let Some(nv) = maybe_nv {
                    let packed = pack_needle_value(nv);
                    table.insert(key_u64, packed.as_slice()).map_err(|e| {
                        io::Error::new(io::ErrorKind::Other, format!("redb insert: {}", e))
                    })?;
                } else {
                    // Entry was deleted — remove from redb if present
                    table.remove(key_u64).map_err(|e| {
                        io::Error::new(io::ErrorKind::Other, format!("redb remove: {}", e))
                    })?;
                }
            }
        }
        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb commit: {}", e)))?;

        nm.save_idx_size_meta(idx_size)?;
        nm.metric = metrics_from_idx(reader, version)?;

        Ok(nm)
    }

    /// Set the index file for append-only writes.
    pub fn set_idx_file(&mut self, file: Box<dyn IdxFileWriter>, offset: u64) {
        self.idx_file = Some(file);
        self.idx_file_offset = offset;
    }

    /// True when an .idx file writer is attached. See CompactNeedleMap.
    pub fn has_idx_writer(&self) -> bool {
        self.idx_file.is_some()
    }

    // ---- Map operations ----

    /// Insert or update an entry. Writes to idx file first, then redb.
    pub fn put(&mut self, key: NeedleId, offset: Offset, size: Size) -> io::Result<()> {
        // Persist to idx file BEFORE mutating redb state for crash consistency
        if let Some(ref mut idx_file) = self.idx_file {
            idx::write_index_entry(idx_file, key, offset, size)?;
            self.idx_file_offset += NEEDLE_MAP_ENTRY_SIZE as u64;
        }

        let key_u64: u64 = key.into();
        let nv = NeedleValue { offset, size };
        let packed = pack_needle_value(&nv);

        // Read old value for metric update
        let old = self.get_internal(key_u64)?;

        let txn = Self::begin_write_no_fsync(&self.db)?;
        {
            let mut table = txn.open_table(NEEDLE_TABLE).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("redb open_table: {}", e))
            })?;
            table
                .insert(key_u64, packed.as_slice())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb insert: {}", e)))?;
        }
        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb commit: {}", e)))?;
        self.writes_since_checkpoint += 1;

        self.metric.on_put(key, old.as_ref(), size);
        Ok(())
    }

    /// Look up a needle. A redb failure is an ERROR, not an absent needle:
    /// answering "not found" would turn a database problem into a read miss
    /// and let a delete report success without recording a tombstone.
    pub fn get(&self, key: NeedleId) -> io::Result<Option<NeedleValue>> {
        let key_u64: u64 = key.into();
        self.get_internal(key_u64)
    }

    /// Internal get that returns io::Result for error propagation.
    fn get_internal(&self, key_u64: u64) -> io::Result<Option<NeedleValue>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb begin_read: {}", e)))?;
        let table = txn
            .open_table(NEEDLE_TABLE)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb open_table: {}", e)))?;
        match table.get(key_u64) {
            Ok(Some(guard)) => {
                let bytes: &[u8] = guard.value();
                if bytes.len() == PACKED_NEEDLE_VALUE_SIZE {
                    let mut arr = [0u8; PACKED_NEEDLE_VALUE_SIZE];
                    arr.copy_from_slice(bytes);
                    Ok(Some(unpack_needle_value(&arr)))
                } else {
                    Ok(None)
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("redb get: {}", e),
            )),
        }
    }

    /// Mark a needle as deleted. Appends tombstone to .idx file, negates size in redb.
    pub fn delete(&mut self, key: NeedleId, offset: Offset) -> io::Result<Option<Size>> {
        let key_u64: u64 = key.into();

        if let Some(old) = self.get_internal(key_u64)? {
            if old.size.is_valid() {
                // Persist tombstone to idx file BEFORE mutating redb
                if let Some(ref mut idx_file) = self.idx_file {
                    idx::write_index_entry(idx_file, key, offset, TOMBSTONE_FILE_SIZE)?;
                    self.idx_file_offset += NEEDLE_MAP_ENTRY_SIZE as u64;
                }

                let deleted_size = Size(-(old.size.0));
                // Keep original offset so readDeleted can find original data (matching Go behavior)
                let deleted_nv = NeedleValue {
                    offset: old.offset,
                    size: deleted_size,
                };
                let packed = pack_needle_value(&deleted_nv);

                let txn = Self::begin_write_no_fsync(&self.db)?;
                {
                    let mut table = txn.open_table(NEEDLE_TABLE).map_err(|e| {
                        io::Error::new(io::ErrorKind::Other, format!("redb open_table: {}", e))
                    })?;
                    table.insert(key_u64, packed.as_slice()).map_err(|e| {
                        io::Error::new(io::ErrorKind::Other, format!("redb insert: {}", e))
                    })?;
                }
                txn.commit().map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("redb commit: {}", e))
                })?;
                self.writes_since_checkpoint += 1;

                // Only now is the tombstone in the table the metrics describe.
                self.metric.on_delete(&old);
                return Ok(Some(old.size));
            }
        }
        Ok(None)
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

    /// Largest (offset + actual size) seen during the load walk; 0 if the
    /// map is empty. See `NeedleMapMetric::maybe_set_max_needle_end`.
    pub fn max_needle_end(&self) -> i64 {
        self.metric.max_needle_end.load(Ordering::Relaxed)
    }

    pub fn index_file_size(&self) -> u64 {
        self.idx_file_offset
    }

    /// Sync index file to disk.
    pub fn sync(&self) -> io::Result<()> {
        if let Some(ref idx_file) = self.idx_file {
            idx_file.sync_all()?;
        }
        Ok(())
    }

    /// Close the index file, checkpointing first so a reload starts from
    /// the recorded .idx size instead of replaying entries the table
    /// already holds.
    pub fn close(&mut self) {
        if let Err(e) = self.checkpoint() {
            tracing::warn!("redb checkpoint on close failed: {}", e);
        }
        self.idx_file = None;
    }

    /// Save the redb contents to an index file, sorted by needle ID ascending.
    pub fn save_to_idx(&self, path: &str) -> io::Result<()> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb begin_read: {}", e)))?;
        let table = txn
            .open_table(NEEDLE_TABLE)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb open_table: {}", e)))?;

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        // redb iterates in key order (u64 ascending)
        let iter = table
            .iter()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb iter: {}", e)))?;

        for entry in iter {
            let (key_guard, val_guard) = entry.map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("redb iter next: {}", e))
            })?;
            let key_u64: u64 = key_guard.value();
            let bytes: &[u8] = val_guard.value();
            if bytes.len() == PACKED_NEEDLE_VALUE_SIZE {
                let mut arr = [0u8; PACKED_NEEDLE_VALUE_SIZE];
                arr.copy_from_slice(bytes);
                let nv = unpack_needle_value(&arr);
                if nv.size.is_valid() {
                    idx::write_index_entry(&mut file, NeedleId(key_u64), nv.offset, nv.size)?;
                }
            }
        }
        file.sync_all()?;
        Ok(())
    }

    /// Visit all entries in ascending order by needle ID.
    pub fn ascending_visit<F>(&self, mut f: F) -> Result<(), String>
    where
        F: FnMut(NeedleId, &NeedleValue) -> Result<(), String>,
    {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| format!("redb begin_read: {}", e))?;
        let table = txn
            .open_table(NEEDLE_TABLE)
            .map_err(|e| format!("redb open_table: {}", e))?;
        let iter = table.iter().map_err(|e| format!("redb iter: {}", e))?;

        for entry in iter {
            let (key_guard, val_guard) = entry.map_err(|e| format!("redb iter next: {}", e))?;
            let key_u64: u64 = key_guard.value();
            let bytes: &[u8] = val_guard.value();
            if bytes.len() == PACKED_NEEDLE_VALUE_SIZE {
                let mut arr = [0u8; PACKED_NEEDLE_VALUE_SIZE];
                arr.copy_from_slice(bytes);
                let nv = unpack_needle_value(&arr);
                f(NeedleId(key_u64), &nv)?;
            }
        }
        Ok(())
    }

    /// Collect all entries as a Vec for iteration (used by volume.rs iter patterns).
    pub fn collect_entries(&self) -> io::Result<Vec<(NeedleId, NeedleValue)>> {
        let mut result = Vec::new();
        let txn: redb::ReadTransaction = self
            .db
            .begin_read()
            .map_err(|e| io::Error::other(format!("redb begin_read: {e}")))?;
        let table = txn
            .open_table(NEEDLE_TABLE)
            .map_err(|e| io::Error::other(format!("redb open_table: {e}")))?;
        let iter = table
            .iter()
            .map_err(|e| io::Error::other(format!("redb iter: {e}")))?;
        for entry in iter {
            let (key_guard, val_guard) =
                entry.map_err(|e| io::Error::other(format!("redb entry: {e}")))?;
            let key_u64: u64 = key_guard.value();
            let bytes: &[u8] = val_guard.value();
            if bytes.len() == PACKED_NEEDLE_VALUE_SIZE {
                let mut arr = [0u8; PACKED_NEEDLE_VALUE_SIZE];
                arr.copy_from_slice(bytes);
                let nv = unpack_needle_value(&arr);
                result.push((NeedleId(key_u64), nv));
            }
        }
        Ok(result)
    }
}

// ============================================================================
// NeedleMap enum — unified interface over both implementations
// ============================================================================

/// Unified needle map wrapping either in-memory or redb-backed storage.
pub enum NeedleMap {
    InMemory(CompactNeedleMap),
    Redb(RedbNeedleMap),
    /// Read-only volumes — including every cloud-tiered one — search the sorted
    /// `.sdx` on disk instead of holding an index in RAM. Mirrors Go's
    /// `SortedFileNeedleMap`.
    SortedFile(SortedFileNeedleMap),
}

impl NeedleMap {
    /// Insert or update an entry.
    pub fn put(&mut self, key: NeedleId, offset: Offset, size: Size) -> io::Result<()> {
        match self {
            NeedleMap::InMemory(nm) => nm.put(key, offset, size),
            NeedleMap::Redb(nm) => nm.put(key, offset, size),
            NeedleMap::SortedFile(nm) => nm.put(key, offset, size),
        }
    }

    /// Look up a needle. Disk- and database-backed maps report their own
    /// failures rather than folding them into "not found" — see the notes on
    /// `RedbNeedleMap::get` and `SortedFileNeedleMap::get`.
    pub fn get(&self, key: NeedleId) -> io::Result<Option<NeedleValue>> {
        match self {
            NeedleMap::InMemory(nm) => Ok(nm.get(key)),
            NeedleMap::Redb(nm) => nm.get(key),
            NeedleMap::SortedFile(nm) => nm.get(key),
        }
    }

    /// Mark a needle as deleted.
    pub fn delete(&mut self, key: NeedleId, offset: Offset) -> io::Result<Option<Size>> {
        match self {
            NeedleMap::InMemory(nm) => nm.delete(key, offset),
            NeedleMap::Redb(nm) => nm.delete(key, offset),
            NeedleMap::SortedFile(nm) => nm.delete(key, offset),
        }
    }

    /// Set the index file for append-only writes.
    pub fn set_idx_file(&mut self, file: Box<dyn IdxFileWriter>, offset: u64) {
        match self {
            NeedleMap::InMemory(nm) => nm.set_idx_file(file, offset),
            NeedleMap::Redb(nm) => nm.set_idx_file(file, offset),
            // The sorted map borrows its .idx per append, so there is no
            // long-lived writer to install.
            NeedleMap::SortedFile(_) => {}
        }
    }

    /// True when an .idx file writer is attached.
    pub fn has_idx_writer(&self) -> bool {
        match self {
            NeedleMap::InMemory(nm) => nm.has_idx_writer(),
            NeedleMap::Redb(nm) => nm.has_idx_writer(),
            // Appends open the .idx on demand, so one is always available.
            NeedleMap::SortedFile(_) => true,
        }
    }

    /// Whether the backend wants a durable checkpoint. Only the redb map
    /// commits non-durably between checkpoints; see
    /// `RedbNeedleMap::checkpoint_due`.
    pub fn checkpoint_due(&self) -> bool {
        match self {
            NeedleMap::Redb(nm) => nm.checkpoint_due(),
            NeedleMap::InMemory(_) | NeedleMap::SortedFile(_) => false,
        }
    }

    /// Take the checkpoint `checkpoint_due` asked for. The caller must have
    /// flushed the .dat first.
    pub fn checkpoint(&mut self) -> io::Result<()> {
        match self {
            NeedleMap::Redb(nm) => nm.checkpoint(),
            NeedleMap::InMemory(_) | NeedleMap::SortedFile(_) => Ok(()),
        }
    }

    /// Content byte count.
    pub fn content_size(&self) -> u64 {
        match self {
            NeedleMap::InMemory(nm) => nm.content_size(),
            NeedleMap::Redb(nm) => nm.content_size(),
            NeedleMap::SortedFile(nm) => nm.content_size(),
        }
    }

    /// Deleted byte count.
    pub fn deleted_size(&self) -> u64 {
        match self {
            NeedleMap::InMemory(nm) => nm.deleted_size(),
            NeedleMap::Redb(nm) => nm.deleted_size(),
            NeedleMap::SortedFile(nm) => nm.deleted_size(),
        }
    }

    /// Live file count.
    pub fn file_count(&self) -> i64 {
        match self {
            NeedleMap::InMemory(nm) => nm.file_count(),
            NeedleMap::Redb(nm) => nm.file_count(),
            NeedleMap::SortedFile(nm) => nm.file_count(),
        }
    }

    /// Deleted file count.
    pub fn deleted_count(&self) -> i64 {
        match self {
            NeedleMap::InMemory(nm) => nm.deleted_count(),
            NeedleMap::Redb(nm) => nm.deleted_count(),
            NeedleMap::SortedFile(nm) => nm.deleted_count(),
        }
    }

    /// Maximum needle ID seen.
    pub fn max_file_key(&self) -> NeedleId {
        match self {
            NeedleMap::InMemory(nm) => nm.max_file_key(),
            NeedleMap::Redb(nm) => nm.max_file_key(),
            NeedleMap::SortedFile(nm) => nm.max_file_key(),
        }
    }

    /// Largest (offset + actual size) seen during the load walk; 0 if the
    /// map is empty. Used at volume load to detect .idx entries that
    /// reference past the end of .dat (issue #8928) without a second scan.
    pub fn max_needle_end(&self) -> i64 {
        match self {
            NeedleMap::InMemory(nm) => nm.max_needle_end(),
            NeedleMap::Redb(nm) => nm.max_needle_end(),
            NeedleMap::SortedFile(nm) => nm.max_needle_end(),
        }
    }

    /// Index file size in bytes.
    pub fn index_file_size(&self) -> u64 {
        match self {
            NeedleMap::InMemory(nm) => nm.index_file_size(),
            NeedleMap::Redb(nm) => nm.index_file_size(),
            NeedleMap::SortedFile(nm) => nm.index_file_size(),
        }
    }

    /// Sync index file to disk.
    pub fn sync(&self) -> io::Result<()> {
        match self {
            NeedleMap::InMemory(nm) => nm.sync(),
            NeedleMap::Redb(nm) => nm.sync(),
            NeedleMap::SortedFile(nm) => nm.sync(),
        }
    }

    /// Close index file.
    pub fn close(&mut self) {
        match self {
            NeedleMap::InMemory(nm) => nm.close(),
            NeedleMap::Redb(nm) => nm.close(),
            NeedleMap::SortedFile(nm) => nm.close(),
        }
    }

    /// Save to an index file.
    pub fn save_to_idx(&self, path: &str) -> io::Result<()> {
        match self {
            NeedleMap::InMemory(nm) => nm.save_to_idx(path),
            NeedleMap::Redb(nm) => nm.save_to_idx(path),
            NeedleMap::SortedFile(nm) => nm.save_to_idx(path),
        }
    }

    /// Visit all entries in ascending order by needle ID.
    pub fn ascending_visit<F>(&self, f: F) -> Result<(), String>
    where
        F: FnMut(NeedleId, &NeedleValue) -> Result<(), String>,
    {
        match self {
            NeedleMap::InMemory(nm) => nm.ascending_visit(f),
            NeedleMap::Redb(nm) => nm.ascending_visit(f),
            NeedleMap::SortedFile(nm) => nm.ascending_visit(f),
        }
    }

    /// Iterate all entries. Returns a Vec of (NeedleId, NeedleValue) pairs.
    /// For InMemory this collects via ascending visit; the disk-backed maps read
    /// it back off disk, so a truncated .sdx or a redb read fault surfaces here
    /// as an error. Compaction treats the result as the complete live set, so a
    /// partial scan must never be mistaken for an empty tail.
    pub fn iter_entries(&self) -> io::Result<Vec<(NeedleId, NeedleValue)>> {
        match self {
            NeedleMap::InMemory(nm) => {
                let mut entries = Vec::new();
                // The visitor never fails, so neither can this.
                let _ = nm.ascending_visit(|id, nv| {
                    entries.push((id, *nv));
                    Ok(())
                });
                Ok(entries)
            }
            NeedleMap::Redb(nm) => nm.collect_entries(),
            NeedleMap::SortedFile(nm) => nm.iter_entries(),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// The `.idx` size recorded in the durable state of the `.rdb` at
    /// `rdb_path`, read from a copy taken while the map may still be open:
    /// exactly what a crash would leave behind. `None` when nothing durable
    /// has been recorded yet.
    pub(crate) fn durable_idx_size(rdb_path: &Path) -> Option<u64> {
        let copy = rdb_path.with_extension("crash-copy.rdb");
        std::fs::copy(rdb_path, &copy).unwrap();
        let db = Database::open(&copy).unwrap();
        let txn = db.begin_read().unwrap();
        let meta = txn.open_table(META_TABLE).ok()?;
        let size = meta.get(META_IDX_SIZE).unwrap().map(|g| g.value());
        drop(meta);
        drop(txn);
        drop(db);
        let _ = std::fs::remove_file(&copy);
        size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_needle_map_put_get() {
        let mut nm = CompactNeedleMap::new();
        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100))
            .unwrap();
        nm.put(NeedleId(2), Offset::from_actual_offset(128), Size(200))
            .unwrap();

        let v1 = nm.get(NeedleId(1)).unwrap();
        assert_eq!(v1.size, Size(100));

        let v2 = nm.get(NeedleId(2)).unwrap();
        assert_eq!(v2.size, Size(200));

        assert!(nm.get(NeedleId(99)).is_none());
    }

    #[test]
    fn test_needle_map_delete() {
        let mut nm = CompactNeedleMap::new();
        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100))
            .unwrap();

        assert_eq!(nm.file_count(), 1);
        assert_eq!(nm.content_size(), 100);

        let deleted = nm
            .delete(NeedleId(1), Offset::from_actual_offset(0))
            .unwrap();
        assert_eq!(deleted, Some(Size(100)));

        // Additive-only: file_count stays at 1 after delete
        assert_eq!(nm.file_count(), 1);
        assert_eq!(nm.deleted_count(), 1);
        assert_eq!(nm.deleted_size(), 100);
    }

    #[test]
    fn test_needle_map_metrics() {
        let mut nm = CompactNeedleMap::new();
        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100))
            .unwrap();
        nm.put(NeedleId(2), Offset::from_actual_offset(128), Size(200))
            .unwrap();
        nm.put(NeedleId(3), Offset::from_actual_offset(384), Size(300))
            .unwrap();

        assert_eq!(nm.file_count(), 3);
        assert_eq!(nm.content_size(), 600);
        assert_eq!(nm.max_file_key(), NeedleId(3));

        // Update existing — additive-only: file_count increments, content_size adds
        nm.put(NeedleId(2), Offset::from_actual_offset(700), Size(250))
            .unwrap();
        assert_eq!(nm.file_count(), 4); // 3 + 1 (always increments)
        assert_eq!(nm.content_size(), 850); // 600 + 250 (always adds)

        // Delete — additive-only: file_count unchanged
        nm.delete(NeedleId(1), Offset::from_actual_offset(0))
            .unwrap();
        assert_eq!(nm.file_count(), 4); // unchanged
        assert_eq!(nm.deleted_count(), 2); // 1 from overwrite + 1 from delete
    }

    #[test]
    fn test_needle_map_load_from_idx() {
        // Build an idx file in memory
        // Note: offset 0 is reserved for the SuperBlock, so real needles start at offset >= 8
        let mut idx_data = Vec::new();
        idx::write_index_entry(
            &mut idx_data,
            NeedleId(1),
            Offset::from_actual_offset(8),
            Size(100),
        )
        .unwrap();
        idx::write_index_entry(
            &mut idx_data,
            NeedleId(2),
            Offset::from_actual_offset(128),
            Size(200),
        )
        .unwrap();
        idx::write_index_entry(
            &mut idx_data,
            NeedleId(3),
            Offset::from_actual_offset(384),
            Size(300),
        )
        .unwrap();
        // Delete needle 2
        idx::write_index_entry(
            &mut idx_data,
            NeedleId(2),
            Offset::default(),
            TOMBSTONE_FILE_SIZE,
        )
        .unwrap();

        let mut cursor = Cursor::new(idx_data);
        let nm = CompactNeedleMap::load_from_idx(&mut cursor, Version::current()).unwrap();

        assert!(nm.get(NeedleId(1)).is_some());
        assert!(nm.get(NeedleId(2)).is_none()); // deleted
        assert!(nm.get(NeedleId(3)).is_some());
        // Additive-only: put(1)+put(2)+put(3) = 3, delete doesn't decrement
        assert_eq!(nm.file_count(), 3);
    }

    #[test]
    fn test_needle_map_double_delete() {
        let mut nm = CompactNeedleMap::new();
        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100))
            .unwrap();

        let r1 = nm
            .delete(NeedleId(1), Offset::from_actual_offset(0))
            .unwrap();
        assert_eq!(r1, Some(Size(100)));

        // Second delete should return None (already deleted)
        let r2 = nm
            .delete(NeedleId(1), Offset::from_actual_offset(0))
            .unwrap();
        assert_eq!(r2, None);
        assert_eq!(nm.deleted_count(), 1); // not double counted
    }

    // ---- RedbNeedleMap tests ----

    /// The cache budget production gives the smallest redb tier.
    fn redb_test_cache() -> usize {
        NeedleMapKind::Redb.redb_cache_bytes()
    }

    /// Open a redb map on an empty .idx with an append writer attached, the
    /// way `Volume::load_index_redb` opens a writable volume.
    fn open_writable_redb(
        dir: &std::path::Path,
    ) -> (RedbNeedleMap, std::path::PathBuf, std::path::PathBuf) {
        let db_path = dir.join("v.rdb");
        let idx_path = dir.join("v.idx");
        let idx_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&idx_path)
            .unwrap();
        let idx_size = idx_file.metadata().unwrap().len();
        let mut reader = std::io::BufReader::new(&idx_file);
        let mut nm = RedbNeedleMap::load_from_idx(
            db_path.to_str().unwrap(),
            &mut reader,
            Version::current(),
            redb_test_cache(),
        )
        .unwrap();
        let writer = std::fs::OpenOptions::new()
            .append(true)
            .open(&idx_path)
            .unwrap();
        nm.set_idx_file(Box::new(writer), idx_size);
        (nm, db_path, idx_path)
    }

    #[test]
    fn test_redb_needle_map_put_get() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.rdb");
        let mut nm = RedbNeedleMap::new(db_path.to_str().unwrap(), redb_test_cache()).unwrap();

        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100))
            .unwrap();
        nm.put(NeedleId(2), Offset::from_actual_offset(128), Size(200))
            .unwrap();

        let v1 = nm.get(NeedleId(1)).unwrap().unwrap();
        assert_eq!(v1.size, Size(100));

        let v2 = nm.get(NeedleId(2)).unwrap().unwrap();
        assert_eq!(v2.size, Size(200));

        assert!(nm.get(NeedleId(99)).unwrap().is_none());
    }

    #[test]
    fn test_redb_needle_map_delete() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.rdb");
        let mut nm = RedbNeedleMap::new(db_path.to_str().unwrap(), redb_test_cache()).unwrap();

        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100))
            .unwrap();
        assert_eq!(nm.file_count(), 1);
        assert_eq!(nm.content_size(), 100);

        let deleted = nm
            .delete(NeedleId(1), Offset::from_actual_offset(0))
            .unwrap();
        assert_eq!(deleted, Some(Size(100)));

        // Additive-only: file_count stays at 1 after delete
        assert_eq!(nm.file_count(), 1);
        assert_eq!(nm.deleted_count(), 1);
        assert_eq!(nm.deleted_size(), 100);

        // Deleted entry should have negated size
        let nv = nm.get(NeedleId(1)).unwrap().unwrap();
        assert_eq!(nv.size, Size(-100));
    }

    #[test]
    fn test_redb_needle_map_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.rdb");
        let mut nm = RedbNeedleMap::new(db_path.to_str().unwrap(), redb_test_cache()).unwrap();

        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100))
            .unwrap();
        nm.put(NeedleId(2), Offset::from_actual_offset(128), Size(200))
            .unwrap();
        nm.put(NeedleId(3), Offset::from_actual_offset(384), Size(300))
            .unwrap();

        assert_eq!(nm.file_count(), 3);
        assert_eq!(nm.content_size(), 600);
        assert_eq!(nm.max_file_key(), NeedleId(3));

        // Update existing — additive-only: file_count increments, content_size adds
        nm.put(NeedleId(2), Offset::from_actual_offset(700), Size(250))
            .unwrap();
        assert_eq!(nm.file_count(), 4); // 3 + 1 (always increments)
        assert_eq!(nm.content_size(), 850); // 600 + 250 (always adds)

        // Delete — additive-only: file_count unchanged
        nm.delete(NeedleId(1), Offset::from_actual_offset(0))
            .unwrap();
        assert_eq!(nm.file_count(), 4); // unchanged
        assert_eq!(nm.deleted_count(), 2); // 1 from overwrite + 1 from delete
    }

    #[test]
    fn test_redb_needle_map_load_from_idx() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.rdb");

        let mut idx_data = Vec::new();
        idx::write_index_entry(
            &mut idx_data,
            NeedleId(1),
            Offset::from_actual_offset(8),
            Size(100),
        )
        .unwrap();
        idx::write_index_entry(
            &mut idx_data,
            NeedleId(2),
            Offset::from_actual_offset(128),
            Size(200),
        )
        .unwrap();
        idx::write_index_entry(
            &mut idx_data,
            NeedleId(3),
            Offset::from_actual_offset(384),
            Size(300),
        )
        .unwrap();
        // Delete needle 2
        idx::write_index_entry(
            &mut idx_data,
            NeedleId(2),
            Offset::default(),
            TOMBSTONE_FILE_SIZE,
        )
        .unwrap();

        let mut cursor = Cursor::new(idx_data);
        let nm = RedbNeedleMap::load_from_idx(
            db_path.to_str().unwrap(),
            &mut cursor,
            Version::current(),
            redb_test_cache(),
        )
        .unwrap();

        assert!(nm.get(NeedleId(1)).unwrap().is_some());
        assert!(nm.get(NeedleId(2)).unwrap().is_none()); // deleted and removed
        assert!(nm.get(NeedleId(3)).unwrap().is_some());
        // Same history as test_needle_map_load_from_idx: the counters must
        // match what the in-memory map (and a live volume) accumulates.
        assert_eq!(nm.file_count(), 3);
        assert_eq!(nm.deleted_count(), 1);
        assert_eq!(nm.deleted_size(), 200);
    }

    #[test]
    fn test_redb_needle_map_double_delete() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.rdb");
        let mut nm = RedbNeedleMap::new(db_path.to_str().unwrap(), redb_test_cache()).unwrap();

        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100))
            .unwrap();

        let r1 = nm
            .delete(NeedleId(1), Offset::from_actual_offset(0))
            .unwrap();
        assert_eq!(r1, Some(Size(100)));

        // Second delete should return None (already deleted)
        let r2 = nm
            .delete(NeedleId(1), Offset::from_actual_offset(0))
            .unwrap();
        assert_eq!(r2, None);
        assert_eq!(nm.deleted_count(), 1); // not double counted
    }

    #[test]
    fn test_redb_needle_map_ascending_visit() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.rdb");
        let mut nm = RedbNeedleMap::new(db_path.to_str().unwrap(), redb_test_cache()).unwrap();

        nm.put(NeedleId(3), Offset::from_actual_offset(384), Size(300))
            .unwrap();
        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100))
            .unwrap();
        nm.put(NeedleId(2), Offset::from_actual_offset(128), Size(200))
            .unwrap();

        let mut visited = Vec::new();
        nm.ascending_visit(|id, nv| {
            visited.push((id, nv.size));
            Ok(())
        })
        .unwrap();

        assert_eq!(visited.len(), 3);
        assert_eq!(visited[0], (NeedleId(1), Size(100)));
        assert_eq!(visited[1], (NeedleId(2), Size(200)));
        assert_eq!(visited[2], (NeedleId(3), Size(300)));
    }

    #[test]
    fn test_redb_needle_map_save_to_idx() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.rdb");
        let idx_path = dir.path().join("test.idx");

        let mut nm = RedbNeedleMap::new(db_path.to_str().unwrap(), redb_test_cache()).unwrap();
        nm.put(NeedleId(1), Offset::from_actual_offset(8), Size(100))
            .unwrap();
        nm.put(NeedleId(2), Offset::from_actual_offset(128), Size(200))
            .unwrap();
        nm.put(NeedleId(3), Offset::from_actual_offset(384), Size(300))
            .unwrap();
        // Delete needle 2
        nm.delete(NeedleId(2), Offset::from_actual_offset(128))
            .unwrap();

        nm.save_to_idx(idx_path.to_str().unwrap()).unwrap();

        // Load back with CompactNeedleMap to verify
        let mut idx_file = std::fs::File::open(&idx_path).unwrap();
        let loaded = CompactNeedleMap::load_from_idx(&mut idx_file, Version::current()).unwrap();
        assert_eq!(loaded.file_count(), 2); // only live entries
        assert!(loaded.get(NeedleId(1)).is_some());
        assert!(loaded.get(NeedleId(2)).is_none()); // deleted, not saved
        assert!(loaded.get(NeedleId(3)).is_some());
    }

    #[test]
    fn test_pack_unpack_needle_value() {
        let nv = NeedleValue {
            offset: Offset::from_actual_offset(8 * 1000),
            size: Size(4096),
        };
        let packed = pack_needle_value(&nv);
        let unpacked = unpack_needle_value(&packed);
        assert_eq!(
            nv.offset.to_actual_offset(),
            unpacked.offset.to_actual_offset()
        );
        assert_eq!(nv.size, unpacked.size);
    }

    #[test]
    fn test_pack_unpack_negative_size() {
        let nv = NeedleValue {
            offset: Offset::from_actual_offset(8 * 500),
            size: Size(-100),
        };
        let packed = pack_needle_value(&nv);
        let unpacked = unpack_needle_value(&packed);
        assert_eq!(
            nv.offset.to_actual_offset(),
            unpacked.offset.to_actual_offset()
        );
        assert_eq!(nv.size, unpacked.size);
    }

    // ---- NeedleMap enum tests ----

    #[test]
    fn test_needle_map_enum_inmemory() {
        let mut nm = NeedleMap::InMemory(CompactNeedleMap::new());
        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100))
            .unwrap();
        assert_eq!(nm.get(NeedleId(1)).unwrap().unwrap().size, Size(100));
        assert_eq!(nm.file_count(), 1);
    }

    #[test]
    fn test_needle_map_enum_redb() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.rdb");
        let mut nm = NeedleMap::Redb(
            RedbNeedleMap::new(db_path.to_str().unwrap(), redb_test_cache()).unwrap(),
        );
        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100))
            .unwrap();
        assert_eq!(nm.get(NeedleId(1)).unwrap().unwrap().size, Size(100));
        assert_eq!(nm.file_count(), 1);
    }

    #[test]
    fn test_needle_map_kind_redb_cache_bytes_grows_by_tier() {
        // Per-volume redb cache budgets. These must stay small: a volume
        // server opens one redb database per volume, so the process-wide
        // ceiling is roughly (volumes x budget).
        assert_eq!(NeedleMapKind::Redb.redb_cache_bytes(), 4 * 1024 * 1024);
        assert_eq!(NeedleMapKind::RedbMedium.redb_cache_bytes(), 8 * 1024 * 1024);
        assert_eq!(NeedleMapKind::RedbLarge.redb_cache_bytes(), 16 * 1024 * 1024);
    }

    #[test]
    fn test_redb_needle_map_tiny_cache_still_serves_all_entries() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.rdb");
        // A cache far smaller than the data forces redb to evict pages on
        // every write and read; every entry must still round-trip.
        let mut nm = RedbNeedleMap::new(db_path.to_str().unwrap(), 64 * 1024).unwrap();
        let n = 3000u64;
        for i in 1..=n {
            nm.put(
                NeedleId(i),
                Offset::from_actual_offset((i * 8) as i64),
                Size(i as i32),
            )
            .unwrap();
        }
        for i in 1..=n {
            let v = nm.get(NeedleId(i)).unwrap().unwrap();
            assert_eq!(v.size, Size(i as i32));
            assert_eq!(v.offset, Offset::from_actual_offset((i * 8) as i64));
        }
        assert_eq!(nm.file_count(), n as i64);
    }

    #[test]
    fn test_redb_checkpoint_is_explicit_and_due_every_interval() {
        use test_support::durable_idx_size;

        // Every non-durable redb commit leaves bookkeeping behind until a
        // durable one clears it, so a writable map asks for a checkpoint on
        // a fixed cadence. The map never takes it by itself: the volume has
        // to flush the .dat first, then call checkpoint().
        const EXPECTED_INTERVAL: u64 = 1000;
        let dir = tempfile::tempdir().unwrap();
        let (mut nm, db_path, _idx_path) = open_writable_redb(dir.path());
        for i in 1..EXPECTED_INTERVAL {
            nm.put(NeedleId(i), Offset::from_actual_offset((i * 8) as i64), Size(1))
                .unwrap();
            assert!(!nm.checkpoint_due(), "due after only {i} writes");
        }
        nm.put(
            NeedleId(EXPECTED_INTERVAL),
            Offset::from_actual_offset((EXPECTED_INTERVAL * 8) as i64),
            Size(1),
        )
        .unwrap();
        assert!(nm.checkpoint_due());
        assert_eq!(durable_idx_size(&db_path), None, "put() must not commit durably");

        nm.checkpoint().unwrap();
        assert!(!nm.checkpoint_due());
        assert_eq!(
            durable_idx_size(&db_path),
            Some(EXPECTED_INTERVAL * NEEDLE_MAP_ENTRY_SIZE as u64),
            "checkpoint records how much of the .idx the table reflects"
        );

        // Snapshot the .rdb while the map is still open: what a crash leaves.
        let crash_copy = dir.path().join("crash.rdb");
        std::fs::copy(&db_path, &crash_copy).unwrap();
        drop(nm);
        let db = Database::open(&crash_copy).unwrap();
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(NEEDLE_TABLE).unwrap();
        assert!(
            table.get(EXPECTED_INTERVAL).unwrap().is_some(),
            "entries up to the checkpoint are durable"
        );
    }

    #[test]
    fn test_redb_close_records_idx_size_so_reload_does_not_double_count() {
        let dir = tempfile::tempdir().unwrap();
        let (mut nm, db_path, idx_path) = open_writable_redb(dir.path());
        for i in 1..=5u64 {
            nm.put(NeedleId(i), Offset::from_actual_offset((i * 8) as i64), Size(1))
                .unwrap();
        }
        nm.close();
        drop(nm);

        // A clean close leaves the table durable; the recorded .idx size
        // must match it, or the reload replays the same 5 entries on top.
        let mut idx = std::fs::File::open(&idx_path).unwrap();
        let reloaded = RedbNeedleMap::load_from_idx(
            db_path.to_str().unwrap(),
            &mut idx,
            Version::current(),
            redb_test_cache(),
        )
        .unwrap();
        assert_eq!(reloaded.file_count(), 5);
        assert_eq!(reloaded.deleted_count(), 0);
    }

    #[test]
    fn test_redb_reload_with_stale_idx_size_does_not_double_count() {
        let dir = tempfile::tempdir().unwrap();
        let (mut nm, db_path, idx_path) = open_writable_redb(dir.path());
        for i in 1..=5u64 {
            nm.put(NeedleId(i), Offset::from_actual_offset((i * 8) as i64), Size(1))
                .unwrap();
        }
        // Drop without close(): redb makes the table durable on drop, but the
        // recorded .idx size stays at its load-time value (0), so the reload
        // replays all 5 entries over rows the table already holds.
        drop(nm);

        let mut idx = std::fs::File::open(&idx_path).unwrap();
        let reloaded = RedbNeedleMap::load_from_idx(
            db_path.to_str().unwrap(),
            &mut idx,
            Version::current(),
            redb_test_cache(),
        )
        .unwrap();
        assert_eq!(reloaded.file_count(), 5);
        assert_eq!(reloaded.deleted_count(), 0);
        assert_eq!(
            reloaded.get(NeedleId(5)).unwrap().unwrap().offset,
            Offset::from_actual_offset(40)
        );
    }

    #[test]
    fn test_redb_reload_metrics_keep_overwrite_and_delete_history() {
        // garbage_level() is deleted_size / content_size. Every load path
        // must rebuild both from the whole .idx history, the way the live
        // counters accumulate, not from the table's final state, or the
        // bytes of overwritten and deleted needles stop counting as garbage
        // after a restart.
        for (close_first, rebuild) in [(true, false), (false, false), (true, true)] {
            let dir = tempfile::tempdir().unwrap();
            let (mut nm, db_path, idx_path) = open_writable_redb(dir.path());
            nm.put(NeedleId(1), Offset::from_actual_offset(8), Size(100))
                .unwrap();
            // Overwrite: the first 100 bytes become garbage.
            nm.put(NeedleId(1), Offset::from_actual_offset(200), Size(200))
                .unwrap();
            nm.put(NeedleId(2), Offset::from_actual_offset(500), Size(50))
                .unwrap();
            nm.delete(NeedleId(2), Offset::from_actual_offset(600))
                .unwrap();
            let live = (
                nm.file_count(),
                nm.content_size(),
                nm.deleted_count(),
                nm.deleted_size(),
            );
            assert_eq!(live, (3, 350, 2, 150));
            if close_first {
                nm.close();
            }
            drop(nm);
            if rebuild {
                std::fs::remove_file(&db_path).unwrap();
            }

            let mut idx = std::fs::File::open(&idx_path).unwrap();
            let reloaded = RedbNeedleMap::load_from_idx(
                db_path.to_str().unwrap(),
                &mut idx,
                Version::current(),
                redb_test_cache(),
            )
            .unwrap();
            let after = (
                reloaded.file_count(),
                reloaded.content_size(),
                reloaded.deleted_count(),
                reloaded.deleted_size(),
            );
            assert_eq!(
                after, live,
                "close_first={close_first} rebuild={rebuild}"
            );
            assert_eq!(reloaded.get(NeedleId(1)).unwrap().unwrap().size, Size(200));
            assert!(reloaded.get(NeedleId(2)).unwrap().map_or(true, |v| v.size.is_deleted()));
        }
    }
}
