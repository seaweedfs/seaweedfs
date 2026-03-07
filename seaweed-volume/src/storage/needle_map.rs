//! NeedleMapper: in-memory index mapping NeedleId → (Offset, Size).
//!
//! Loaded from .idx file on volume mount. Supports Get, Put, Delete with
//! metrics tracking (file count, byte count, deleted count, deleted bytes).

use std::collections::HashMap;
use std::io::{self, Read, Seek, Write};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

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
    /// Update metrics based on a Put operation.
    fn on_put(&self, key: NeedleId, old: Option<&NeedleValue>, new_size: Size) {
        if new_size.is_valid() {
            if old.is_none() || !old.unwrap().size.is_valid() {
                self.file_count.fetch_add(1, Ordering::Relaxed);
            }
            self.file_byte_count.fetch_add(new_size.0 as u64, Ordering::Relaxed);
            if let Some(old_val) = old {
                if old_val.size.is_valid() {
                    self.file_byte_count.fetch_sub(old_val.size.0 as u64, Ordering::Relaxed);
                    // Track overwritten bytes as garbage for compaction (garbage_level)
                    self.deletion_byte_count.fetch_add(old_val.size.0 as u64, Ordering::Relaxed);
                }
            }
        }
        let key_val: u64 = key.into();
        loop {
            let current = self.max_file_key.load(Ordering::Relaxed);
            if key_val <= current {
                break;
            }
            if self.max_file_key.compare_exchange(current, key_val, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                break;
            }
        }
    }

    /// Update metrics based on a Delete operation.
    fn on_delete(&self, old: &NeedleValue) {
        if old.size.is_valid() {
            self.deletion_count.fetch_add(1, Ordering::Relaxed);
            self.deletion_byte_count.fetch_add(old.size.0 as u64, Ordering::Relaxed);
            self.file_count.fetch_sub(1, Ordering::Relaxed);
            self.file_byte_count.fetch_sub(old.size.0 as u64, Ordering::Relaxed);
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
// CompactNeedleMap (in-memory)
// ============================================================================

/// In-memory needle map backed by a HashMap.
/// The .idx file is kept open for append-only writes.
pub struct CompactNeedleMap {
    map: HashMap<NeedleId, NeedleValue>,
    metric: NeedleMapMetric,
    idx_file: Option<Box<dyn IdxFileWriter>>,
    idx_file_offset: u64,
}

/// Trait for appending to an index file.
pub trait IdxFileWriter: Write + Send + Sync {
    fn sync_all(&self) -> io::Result<()>;
}

impl IdxFileWriter for std::fs::File {
    fn sync_all(&self) -> io::Result<()> {
        std::fs::File::sync_all(self)
    }
}

impl CompactNeedleMap {
    /// Create a new empty in-memory map.
    pub fn new() -> Self {
        CompactNeedleMap {
            map: HashMap::new(),
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
                nm.set(key, NeedleValue { offset, size });
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

        let old = self.map.get(&key).cloned();
        let nv = NeedleValue { offset, size };
        self.metric.on_put(key, old.as_ref(), size);
        self.map.insert(key, nv);
        Ok(())
    }

    /// Look up a needle.
    pub fn get(&self, key: NeedleId) -> Option<&NeedleValue> {
        self.map.get(&key)
    }

    /// Mark a needle as deleted. Appends tombstone to .idx file.
    pub fn delete(&mut self, key: NeedleId, offset: Offset) -> io::Result<Option<Size>> {
        if let Some(old) = self.map.get(&key).cloned() {
            if old.size.is_valid() {
                // Persist tombstone to idx file BEFORE mutating in-memory state for crash consistency
                if let Some(ref mut idx_file) = self.idx_file {
                    idx::write_index_entry(idx_file, key, offset, TOMBSTONE_FILE_SIZE)?;
                    self.idx_file_offset += NEEDLE_MAP_ENTRY_SIZE as u64;
                }

                self.metric.on_delete(&old);
                let deleted_size = Size(-(old.size.0));
                // Keep original offset so readDeleted can find original data (matching Go behavior)
                self.map.insert(key, NeedleValue { offset: old.offset, size: deleted_size });
                return Ok(Some(old.size));
            }
        }
        Ok(None)
    }

    // ---- Internal helpers ----

    /// Insert into map during loading (no idx file write).
    fn set(&mut self, key: NeedleId, nv: NeedleValue) {
        let old = self.map.get(&key).cloned();
        self.metric.on_put(key, old.as_ref(), nv.size);
        self.map.insert(key, nv);
    }

    /// Remove from map during loading (handle deletions in idx walk).
    fn delete_from_map(&mut self, key: NeedleId) {
        if let Some(old) = self.map.get(&key).cloned() {
            if old.size.is_valid() {
                self.metric.on_delete(&old);
            }
        }
        self.map.remove(&key);
    }

    /// Iterate over all entries in the needle map.
    pub fn iter(&self) -> impl Iterator<Item = (&NeedleId, &NeedleValue)> {
        self.map.iter()
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_needle_map_put_get() {
        let mut nm = CompactNeedleMap::new();
        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100)).unwrap();
        nm.put(NeedleId(2), Offset::from_actual_offset(128), Size(200)).unwrap();

        let v1 = nm.get(NeedleId(1)).unwrap();
        assert_eq!(v1.size, Size(100));

        let v2 = nm.get(NeedleId(2)).unwrap();
        assert_eq!(v2.size, Size(200));

        assert!(nm.get(NeedleId(99)).is_none());
    }

    #[test]
    fn test_needle_map_delete() {
        let mut nm = CompactNeedleMap::new();
        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100)).unwrap();

        assert_eq!(nm.file_count(), 1);
        assert_eq!(nm.content_size(), 100);

        let deleted = nm.delete(NeedleId(1), Offset::from_actual_offset(0)).unwrap();
        assert_eq!(deleted, Some(Size(100)));

        assert_eq!(nm.file_count(), 0);
        assert_eq!(nm.deleted_count(), 1);
        assert_eq!(nm.deleted_size(), 100);
    }

    #[test]
    fn test_needle_map_metrics() {
        let mut nm = CompactNeedleMap::new();
        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100)).unwrap();
        nm.put(NeedleId(2), Offset::from_actual_offset(128), Size(200)).unwrap();
        nm.put(NeedleId(3), Offset::from_actual_offset(384), Size(300)).unwrap();

        assert_eq!(nm.file_count(), 3);
        assert_eq!(nm.content_size(), 600);
        assert_eq!(nm.max_file_key(), NeedleId(3));

        // Update existing
        nm.put(NeedleId(2), Offset::from_actual_offset(700), Size(250)).unwrap();
        assert_eq!(nm.file_count(), 3); // still 3
        assert_eq!(nm.content_size(), 650); // 100 + 250 + 300

        // Delete
        nm.delete(NeedleId(1), Offset::from_actual_offset(0)).unwrap();
        assert_eq!(nm.file_count(), 2);
        assert_eq!(nm.deleted_count(), 1);
    }

    #[test]
    fn test_needle_map_load_from_idx() {
        // Build an idx file in memory
        // Note: offset 0 is reserved for the SuperBlock, so real needles start at offset >= 8
        let mut idx_data = Vec::new();
        idx::write_index_entry(&mut idx_data, NeedleId(1), Offset::from_actual_offset(8), Size(100)).unwrap();
        idx::write_index_entry(&mut idx_data, NeedleId(2), Offset::from_actual_offset(128), Size(200)).unwrap();
        idx::write_index_entry(&mut idx_data, NeedleId(3), Offset::from_actual_offset(384), Size(300)).unwrap();
        // Delete needle 2
        idx::write_index_entry(&mut idx_data, NeedleId(2), Offset::default(), TOMBSTONE_FILE_SIZE).unwrap();

        let mut cursor = Cursor::new(idx_data);
        let nm = CompactNeedleMap::load_from_idx(&mut cursor).unwrap();

        assert!(nm.get(NeedleId(1)).is_some());
        assert!(nm.get(NeedleId(2)).is_none()); // deleted
        assert!(nm.get(NeedleId(3)).is_some());
        assert_eq!(nm.file_count(), 2);
    }

    #[test]
    fn test_needle_map_double_delete() {
        let mut nm = CompactNeedleMap::new();
        nm.put(NeedleId(1), Offset::from_actual_offset(0), Size(100)).unwrap();

        let r1 = nm.delete(NeedleId(1), Offset::from_actual_offset(0)).unwrap();
        assert_eq!(r1, Some(Size(100)));

        // Second delete should return None (already deleted)
        let r2 = nm.delete(NeedleId(1), Offset::from_actual_offset(0)).unwrap();
        assert_eq!(r2, None);
        assert_eq!(nm.deleted_count(), 1); // not double counted
    }
}
