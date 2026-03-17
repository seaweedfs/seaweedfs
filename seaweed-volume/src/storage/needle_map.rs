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
use compact_map::CompactMap;

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};

use crate::storage::idx;
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
}

// ============================================================================
// NeedleMapKind
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NeedleMapKind {
    InMemory,
    LevelDb,
    LevelDbMedium,
    LevelDbLarge,
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
    pub fn load_from_idx<R: Read + Seek>(reader: &mut R) -> io::Result<Self> {
        let mut nm = CompactNeedleMap::new();
        idx::walk_index_file(reader, 0, |key, offset, size| {
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
    /// Matches Go's CompactMap.Delete: checks !IsDeleted() (not IsValid()),
    /// so needles with size==0 can still be deleted.
    pub fn delete(&mut self, key: NeedleId, offset: Offset) -> io::Result<Option<Size>> {
        if let Some(old) = self.map.get(key) {
            if !old.size.is_deleted() {
                // Persist tombstone to idx file BEFORE mutating in-memory state for crash consistency
                if let Some(ref mut idx_file) = self.idx_file {
                    idx::write_index_entry(idx_file, key, offset, TOMBSTONE_FILE_SIZE)?;
                    self.idx_file_offset += NEEDLE_MAP_ENTRY_SIZE as u64;
                }

                self.metric.on_delete(&old);
                // Mark as deleted in compact map (negates size in-place)
                self.map.delete(key);
                return Ok(Some(old.size));
            }
        }
        Ok(None)
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

/// Disk-backed needle map using redb.
/// Low memory usage — data lives on disk with redb's page cache.
pub struct RedbNeedleMap {
    db: Database,
    metric: NeedleMapMetric,
    idx_file: Option<Box<dyn IdxFileWriter>>,
    idx_file_offset: u64,
}

impl RedbNeedleMap {
    /// Begin a write transaction with `Durability::None` (no fsync).
    /// The .idx file is the source of truth for crash recovery, so redb
    /// is always rebuilt from .idx on startup — fsync is unnecessary.
    fn begin_write_no_fsync(db: &Database) -> io::Result<redb::WriteTransaction> {
        let mut txn = db.begin_write().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("redb begin_write: {}", e))
        })?;
        let _ = txn.set_durability(Durability::None);
        Ok(txn)
    }

    /// Create a new redb-backed needle map at the given path.
    /// The database file will be created if it does not exist.
    pub fn new(db_path: &str) -> io::Result<Self> {
        let db = Database::create(db_path).map_err(|e| {
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

    /// Rebuild metrics by scanning all entries in the redb table.
    /// Called when reusing an existing .rdb without a full rebuild.
    fn rebuild_metrics_from_db(&self) -> io::Result<()> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb begin_read: {}", e)))?;
        let table = txn
            .open_table(NEEDLE_TABLE)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb open_table: {}", e)))?;
        let iter = table
            .iter()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb iter: {}", e)))?;
        for entry in iter {
            let (key_guard, val_guard) = entry.map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("redb iter next: {}", e))
            })?;
            let key = NeedleId(key_guard.value());
            let bytes: &[u8] = val_guard.value();
            if bytes.len() == PACKED_NEEDLE_VALUE_SIZE {
                let mut arr = [0u8; PACKED_NEEDLE_VALUE_SIZE];
                arr.copy_from_slice(bytes);
                let nv = unpack_needle_value(&arr);
                self.metric.maybe_set_max_file_key(key);
                if nv.size.is_valid() {
                    self.metric.file_count.fetch_add(1, Ordering::Relaxed);
                    self.metric
                        .file_byte_count
                        .fetch_add(nv.size.0 as u64, Ordering::Relaxed);
                } else {
                    // Deleted entry (negative size)
                    self.metric.deletion_count.fetch_add(1, Ordering::Relaxed);
                    self.metric
                        .deletion_byte_count
                        .fetch_add((-nv.size.0) as u64, Ordering::Relaxed);
                }
            }
        }
        Ok(())
    }

    /// Load from an .idx file, reusing an existing .rdb if it is consistent.
    ///
    /// Strategy:
    /// 1. Try to open existing .rdb and read its stored .idx size
    /// 2. If .idx size matches → reuse .rdb, rebuild metrics from scan
    /// 3. If .idx is larger → replay new entries incrementally
    /// 4. Otherwise (missing, corrupted, .idx smaller) → full rebuild
    pub fn load_from_idx<R: Read + Seek>(db_path: &str, reader: &mut R) -> io::Result<Self> {
        let idx_size = reader.seek(io::SeekFrom::End(0))?;
        reader.seek(io::SeekFrom::Start(0))?;

        // Try to reuse existing .rdb
        if Path::new(db_path).exists() {
            if let Ok(nm) = Self::try_reuse_rdb(db_path, reader, idx_size) {
                return Ok(nm);
            }
            // Reuse failed — fall through to full rebuild
            reader.seek(io::SeekFrom::Start(0))?;
        }

        Self::full_rebuild(db_path, reader, idx_size)
    }

    /// Try to reuse an existing .rdb file. Returns Ok if successful,
    /// Err if a full rebuild is needed.
    fn try_reuse_rdb<R: Read + Seek>(
        db_path: &str,
        reader: &mut R,
        idx_size: u64,
    ) -> io::Result<Self> {
        let db = Database::open(db_path)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("redb open: {}", e)))?;

        let nm = RedbNeedleMap {
            db,
            metric: NeedleMapMetric::default(),
            idx_file: None,
            idx_file_offset: 0,
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

        // Rebuild metrics from existing data
        nm.rebuild_metrics_from_db()?;

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
                        // Delete: look up old value for metric update, then
                        // store tombstone (negative size with original offset)
                        if let Ok(Some(old)) = nm.get_via_table(&table, key_u64) {
                            if old.size.is_valid() {
                                nm.metric.on_delete(&old);
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
                        // Put: look up old value for metric update
                        let old = nm.get_via_table(&table, key_u64).ok().flatten();
                        let nv = NeedleValue { offset, size };
                        let packed = pack_needle_value(&nv);
                        table.insert(key_u64, packed.as_slice()).map_err(|e| {
                            io::Error::new(io::ErrorKind::Other, format!("redb insert: {}", e))
                        })?;
                        nm.metric.on_put(key, old.as_ref(), size);
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
    ) -> io::Result<Self> {
        let _ = std::fs::remove_file(db_path);
        let nm = RedbNeedleMap::new(db_path)?;

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
                    nm.metric.on_put(*key, None, nv.size);
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

        Ok(nm)
    }

    /// Set the index file for append-only writes.
    pub fn set_idx_file(&mut self, file: Box<dyn IdxFileWriter>, offset: u64) {
        self.idx_file = Some(file);
        self.idx_file_offset = offset;
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

        self.metric.on_put(key, old.as_ref(), size);
        Ok(())
    }

    /// Look up a needle.
    pub fn get(&self, key: NeedleId) -> Option<NeedleValue> {
        let key_u64: u64 = key.into();
        self.get_internal(key_u64).ok().flatten()
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

                self.metric.on_delete(&old);
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
    pub fn collect_entries(&self) -> Vec<(NeedleId, NeedleValue)> {
        let mut result = Vec::new();
        let txn: redb::ReadTransaction = match self.db.begin_read() {
            Ok(t) => t,
            Err(_) => return result,
        };
        let table = match txn.open_table(NEEDLE_TABLE) {
            Ok(t) => t,
            Err(_) => return result,
        };
        let iter = match table.iter() {
            Ok(i) => i,
            Err(_) => return result,
        };
        for entry in iter {
            if let Ok((key_guard, val_guard)) = entry {
                let key_u64: u64 = key_guard.value();
                let bytes: &[u8] = val_guard.value();
                if bytes.len() == PACKED_NEEDLE_VALUE_SIZE {
                    let mut arr = [0u8; PACKED_NEEDLE_VALUE_SIZE];
                    arr.copy_from_slice(bytes);
                    let nv = unpack_needle_value(&arr);
                    result.push((NeedleId(key_u64), nv));
                }
            }
        }
        result
    }
}

// ============================================================================
// NeedleMap enum — unified interface over both implementations
// ============================================================================

/// Unified needle map wrapping either in-memory or redb-backed storage.
pub enum NeedleMap {
    InMemory(CompactNeedleMap),
    Redb(RedbNeedleMap),
}

impl NeedleMap {
    /// Insert or update an entry.
    pub fn put(&mut self, key: NeedleId, offset: Offset, size: Size) -> io::Result<()> {
        match self {
            NeedleMap::InMemory(nm) => nm.put(key, offset, size),
            NeedleMap::Redb(nm) => nm.put(key, offset, size),
        }
    }

    /// Look up a needle.
    pub fn get(&self, key: NeedleId) -> Option<NeedleValue> {
        match self {
            NeedleMap::InMemory(nm) => nm.get(key),
            NeedleMap::Redb(nm) => nm.get(key),
        }
    }

    /// Mark a needle as deleted.
    pub fn delete(&mut self, key: NeedleId, offset: Offset) -> io::Result<Option<Size>> {
        match self {
            NeedleMap::InMemory(nm) => nm.delete(key, offset),
            NeedleMap::Redb(nm) => nm.delete(key, offset),
        }
    }

    /// Set the index file for append-only writes.
    pub fn set_idx_file(&mut self, file: Box<dyn IdxFileWriter>, offset: u64) {
        match self {
            NeedleMap::InMemory(nm) => nm.set_idx_file(file, offset),
            NeedleMap::Redb(nm) => nm.set_idx_file(file, offset),
        }
    }

    /// Content byte count.
    pub fn content_size(&self) -> u64 {
        match self {
            NeedleMap::InMemory(nm) => nm.content_size(),
            NeedleMap::Redb(nm) => nm.content_size(),
        }
    }

    /// Deleted byte count.
    pub fn deleted_size(&self) -> u64 {
        match self {
            NeedleMap::InMemory(nm) => nm.deleted_size(),
            NeedleMap::Redb(nm) => nm.deleted_size(),
        }
    }

    /// Live file count.
    pub fn file_count(&self) -> i64 {
        match self {
            NeedleMap::InMemory(nm) => nm.file_count(),
            NeedleMap::Redb(nm) => nm.file_count(),
        }
    }

    /// Deleted file count.
    pub fn deleted_count(&self) -> i64 {
        match self {
            NeedleMap::InMemory(nm) => nm.deleted_count(),
            NeedleMap::Redb(nm) => nm.deleted_count(),
        }
    }

    /// Maximum needle ID seen.
    pub fn max_file_key(&self) -> NeedleId {
        match self {
            NeedleMap::InMemory(nm) => nm.max_file_key(),
            NeedleMap::Redb(nm) => nm.max_file_key(),
        }
    }

    /// Index file size in bytes.
    pub fn index_file_size(&self) -> u64 {
        match self {
            NeedleMap::InMemory(nm) => nm.index_file_size(),
            NeedleMap::Redb(nm) => nm.index_file_size(),
        }
    }

    /// Sync index file to disk.
    pub fn sync(&self) -> io::Result<()> {
        match self {
            NeedleMap::InMemory(nm) => nm.sync(),
            NeedleMap::Redb(nm) => nm.sync(),
        }
    }

    /// Close index file.
    pub fn close(&mut self) {
        match self {
            NeedleMap::InMemory(nm) => nm.close(),
            NeedleMap::Redb(nm) => nm.close(),
        }
    }

    /// Save to an index file.
    pub fn save_to_idx(&self, path: &str) -> io::Result<()> {
        match self {
            NeedleMap::InMemory(nm) => nm.save_to_idx(path),
            NeedleMap::Redb(nm) => nm.save_to_idx(path),
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
        }
    }

    /// Iterate all entries. Returns a Vec of (NeedleId, NeedleValue) pairs.
    /// For InMemory this collects via ascending visit; for Redb it reads from disk.
    pub fn iter_entries(&self) -> Vec<(NeedleId, NeedleValue)> {
        match self {
            NeedleMap::InMemory(nm) => {
                let mut entries = Vec::new();
                let _ = nm.ascending_visit(|id, nv| {
                    entries.push((id, *nv));
                    Ok(())
                });
                entries
            }
            NeedleMap::Redb(nm) => nm.collect_entries(),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

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
        let nm = CompactNeedleMap::load_from_idx(&mut cursor).unwrap();

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

    #[test]
    fn test_redb_needle_map_put_get() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.rdb");
        let mut nm = RedbNeedleMap::new(db_path.to_str().unwrap()).unwrap();

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
    fn test_redb_needle_map_delete() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.rdb");
        let mut nm = RedbNeedleMap::new(db_path.to_str().unwrap()).unwrap();

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
        let nv = nm.get(NeedleId(1)).unwrap();
        assert_eq!(nv.size, Size(-100));
    }

    #[test]
    fn test_redb_needle_map_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.rdb");
        let mut nm = RedbNeedleMap::new(db_path.to_str().unwrap()).unwrap();

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
        let nm = RedbNeedleMap::load_from_idx(db_path.to_str().unwrap(), &mut cursor).unwrap();

        assert!(nm.get(NeedleId(1)).is_some());
        assert!(nm.get(NeedleId(2)).is_none()); // deleted and removed
        assert!(nm.get(NeedleId(3)).is_some());
        assert_eq!(nm.file_count(), 2);
    }

    #[test]
    fn test_redb_needle_map_double_delete() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.rdb");
        let mut nm = RedbNeedleMap::new(db_path.to_str().unwrap()).unwrap();

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
        let mut nm = RedbNeedleMap::new(db_path.to_str().unwrap()).unwrap();

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

        let mut nm = RedbNeedleMap::new(db_path.to_str().unwrap()).unwrap();
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
        let loaded = CompactNeedleMap::load_from_idx(&mut idx_file).unwrap();
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
        assert_eq!(nm.get(NeedleId(1)).unwrap().size, Size(100));
        assert_eq!(nm.file_count(), 1);
    }

    #[test]
    fn test_needle_map_enum_redb() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.rdb");
        let mut nm = NeedleMap::Redb(RedbNeedleMap::new(db_path.to_str().unwrap()).unwrap());
        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100))
            .unwrap();
        assert_eq!(nm.get(NeedleId(1)).unwrap().size, Size(100));
        assert_eq!(nm.file_count(), 1);
    }
}
