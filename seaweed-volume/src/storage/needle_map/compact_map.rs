//! CompactMap: memory-efficient in-memory map of NeedleId -> (Offset, Size).
//!
//! Port of Go's CompactMap from weed/storage/needle_map/compact_map.go.
//! Uses segmented sorted arrays with compressed keys (u16 instead of u64)
//! to achieve ~10 bytes per entry instead of ~40-48 bytes with HashMap.
//!
//! NeedleId is split into: chunk = id / SEGMENT_CHUNK_SIZE, compact_key = id % SEGMENT_CHUNK_SIZE.
//! Each segment stores up to SEGMENT_CHUNK_SIZE entries in a sorted Vec, searched via binary search.
//! Best case (ordered inserts): O(1). Worst case: O(log n) per segment.

use std::collections::HashMap;

use crate::storage::types::*;
use super::NeedleValue;

/// Maximum entries per segment. Must be <= u16::MAX (65535).
const SEGMENT_CHUNK_SIZE: u64 = 50_000;

/// Compact key: only the low bits of NeedleId within a segment.
type CompactKey = u16;

/// Segment chunk identifier: NeedleId / SEGMENT_CHUNK_SIZE.
type Chunk = u64;

/// Compact entry: 10 bytes (2 + 4 + 4) vs 16 bytes for full NeedleId + NeedleValue.
#[derive(Clone, Copy)]
struct CompactEntry {
    key: CompactKey,       // 2 bytes
    offset: [u8; OFFSET_SIZE], // 4 bytes
    size: Size,            // 4 bytes
}

impl CompactEntry {
    fn to_needle_value(&self) -> NeedleValue {
        NeedleValue {
            offset: Offset::from_bytes(&self.offset),
            size: self.size,
        }
    }
}

/// A sorted segment of compact entries for a given chunk.
struct Segment {
    list: Vec<CompactEntry>,
    chunk: Chunk,
    first_key: CompactKey,
    last_key: CompactKey,
}

impl Segment {
    fn new(chunk: Chunk) -> Self {
        Segment {
            list: Vec::new(),
            chunk,
            first_key: u16::MAX,
            last_key: 0,
        }
    }

    fn compact_key(&self, id: NeedleId) -> CompactKey {
        (id.0 - SEGMENT_CHUNK_SIZE * self.chunk) as CompactKey
    }

    /// Binary search for a compact key. Returns (index, found).
    /// If not found, index is the insertion point.
    fn bsearch(&self, id: NeedleId) -> (usize, bool) {
        let ck = self.compact_key(id);

        if self.list.is_empty() {
            return (0, false);
        }
        if ck == self.first_key {
            return (0, true);
        }
        if ck < self.first_key {
            return (0, false);
        }
        if ck == self.last_key {
            return (self.list.len() - 1, true);
        }
        if ck > self.last_key {
            return (self.list.len(), false);
        }

        let i = self.list.partition_point(|e| e.key < ck);
        if i < self.list.len() && self.list[i].key == ck {
            (i, true)
        } else {
            (i, false)
        }
    }

    /// Insert or update. Returns old NeedleValue if updating.
    fn set(&mut self, id: NeedleId, offset: Offset, size: Size) -> Option<NeedleValue> {
        let (i, found) = self.bsearch(id);

        if found {
            let old = self.list[i].to_needle_value();
            let mut offset_bytes = [0u8; OFFSET_SIZE];
            offset.to_bytes(&mut offset_bytes);
            self.list[i].offset = offset_bytes;
            self.list[i].size = size;
            return Some(old);
        }

        // Insert at sorted position
        let ck = self.compact_key(id);
        let mut offset_bytes = [0u8; OFFSET_SIZE];
        offset.to_bytes(&mut offset_bytes);

        let entry = CompactEntry {
            key: ck,
            offset: offset_bytes,
            size,
        };

        if self.list.len() == SEGMENT_CHUNK_SIZE as usize - 1 {
            // Pin capacity to exact size when maxing out
            let mut new_list = Vec::with_capacity(SEGMENT_CHUNK_SIZE as usize);
            new_list.extend_from_slice(&self.list[..i]);
            new_list.push(entry);
            new_list.extend_from_slice(&self.list[i..]);
            self.list = new_list;
        } else {
            self.list.insert(i, entry);
        }

        if ck < self.first_key {
            self.first_key = ck;
        }
        if ck > self.last_key {
            self.last_key = ck;
        }

        None
    }

    fn get(&self, id: NeedleId) -> Option<NeedleValue> {
        let (i, found) = self.bsearch(id);
        if found {
            Some(self.list[i].to_needle_value())
        } else {
            None
        }
    }

    /// Mark as deleted by negating size. Returns previous size if not already deleted.
    /// Matches Go behavior: checks !IsDeleted() (i.e., size >= 0).
    fn delete(&mut self, id: NeedleId) -> Option<Size> {
        let (i, found) = self.bsearch(id);
        if found && !self.list[i].size.is_deleted() {
            let old_size = self.list[i].size;
            if self.list[i].size.0 == 0 {
                self.list[i].size = TOMBSTONE_FILE_SIZE;
            } else {
                self.list[i].size = Size(-self.list[i].size.0);
            }
            Some(old_size)
        } else {
            None
        }
    }
}

/// Memory-efficient map of NeedleId -> (Offset, Size).
/// Segments NeedleIds into chunks of 50,000 and stores compact 10-byte entries
/// in sorted arrays, using only 2 bytes for the key within each segment.
pub struct CompactMap {
    segments: HashMap<Chunk, Segment>,
}

impl CompactMap {
    pub fn new() -> Self {
        CompactMap {
            segments: HashMap::new(),
        }
    }

    fn segment_for_key(&mut self, id: NeedleId) -> &mut Segment {
        let chunk = id.0 / SEGMENT_CHUNK_SIZE;
        self.segments
            .entry(chunk)
            .or_insert_with(|| Segment::new(chunk))
    }

    /// Insert or update. Returns old NeedleValue if updating.
    pub fn set(&mut self, id: NeedleId, offset: Offset, size: Size) -> Option<NeedleValue> {
        let chunk = id.0 / SEGMENT_CHUNK_SIZE;
        let segment = self.segments
            .entry(chunk)
            .or_insert_with(|| Segment::new(chunk));
        segment.set(id, offset, size)
    }

    pub fn get(&self, id: NeedleId) -> Option<NeedleValue> {
        let chunk = id.0 / SEGMENT_CHUNK_SIZE;
        self.segments.get(&chunk)?.get(id)
    }

    /// Mark as deleted. Returns previous size if was valid.
    pub fn delete(&mut self, id: NeedleId) -> Option<Size> {
        let chunk = id.0 / SEGMENT_CHUNK_SIZE;
        self.segments.get_mut(&chunk)?.delete(id)
    }

    /// Remove entry entirely (used during idx loading).
    pub fn remove(&mut self, id: NeedleId) -> Option<NeedleValue> {
        let chunk = id.0 / SEGMENT_CHUNK_SIZE;
        let segment = self.segments.get_mut(&chunk)?;
        let (i, found) = segment.bsearch(id);
        if found {
            let entry = segment.list.remove(i);
            // Update first/last keys
            if segment.list.is_empty() {
                segment.first_key = u16::MAX;
                segment.last_key = 0;
            } else {
                segment.first_key = segment.list[0].key;
                segment.last_key = segment.list[segment.list.len() - 1].key;
            }
            Some(entry.to_needle_value())
        } else {
            None
        }
    }

    /// Iterate all entries in ascending NeedleId order.
    pub fn ascending_visit<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(NeedleId, &NeedleValue) -> Result<(), E>,
    {
        let mut chunks: Vec<Chunk> = self.segments.keys().copied().collect();
        chunks.sort_unstable();

        for chunk in chunks {
            let segment = &self.segments[&chunk];
            for entry in &segment.list {
                let id = NeedleId(SEGMENT_CHUNK_SIZE * segment.chunk + entry.key as u64);
                let nv = entry.to_needle_value();
                f(id, &nv)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offset(v: u32) -> Offset {
        let bytes = v.to_be_bytes();
        Offset::from_bytes(&bytes)
    }

    #[test]
    fn test_compact_map_basic() {
        let mut m = CompactMap::new();

        // Insert
        assert!(m.set(NeedleId(1), offset(100), Size(50)).is_none());
        assert!(m.set(NeedleId(2), offset(200), Size(60)).is_none());

        // Get
        let nv = m.get(NeedleId(1)).unwrap();
        assert_eq!(nv.size, Size(50));

        // Update returns old value
        let old = m.set(NeedleId(1), offset(300), Size(70)).unwrap();
        assert_eq!(old.size, Size(50));

        // Get updated value
        let nv = m.get(NeedleId(1)).unwrap();
        assert_eq!(nv.size, Size(70));

        // Miss
        assert!(m.get(NeedleId(999)).is_none());
    }

    #[test]
    fn test_compact_map_delete() {
        let mut m = CompactMap::new();
        m.set(NeedleId(1), offset(100), Size(50));

        // Delete returns old size
        let old = m.delete(NeedleId(1)).unwrap();
        assert_eq!(old, Size(50));

        // Get returns deleted (negative size)
        let nv = m.get(NeedleId(1)).unwrap();
        assert!(nv.size.is_deleted());

        // Delete again returns None (already deleted)
        assert!(m.delete(NeedleId(1)).is_none());
    }

    #[test]
    fn test_compact_map_zero_size_delete() {
        let mut m = CompactMap::new();
        m.set(NeedleId(1), offset(100), Size(0));

        let old = m.delete(NeedleId(1)).unwrap();
        assert_eq!(old, Size(0));

        let nv = m.get(NeedleId(1)).unwrap();
        assert_eq!(nv.size, TOMBSTONE_FILE_SIZE);
    }

    #[test]
    fn test_compact_map_cross_segment() {
        let mut m = CompactMap::new();

        // Insert across multiple segments
        m.set(NeedleId(1), offset(1), Size(1));
        m.set(NeedleId(50_000), offset(2), Size(2));
        m.set(NeedleId(100_000), offset(3), Size(3));

        assert_eq!(m.get(NeedleId(1)).unwrap().size, Size(1));
        assert_eq!(m.get(NeedleId(50_000)).unwrap().size, Size(2));
        assert_eq!(m.get(NeedleId(100_000)).unwrap().size, Size(3));
    }

    #[test]
    fn test_compact_map_ascending_visit() {
        let mut m = CompactMap::new();
        m.set(NeedleId(100_005), offset(3), Size(3));
        m.set(NeedleId(5), offset(1), Size(1));
        m.set(NeedleId(50_005), offset(2), Size(2));

        let mut visited = Vec::new();
        m.ascending_visit(|id, nv| {
            visited.push((id, nv.size));
            Ok::<_, String>(())
        })
        .unwrap();

        assert_eq!(visited.len(), 3);
        assert_eq!(visited[0].0, NeedleId(5));
        assert_eq!(visited[1].0, NeedleId(50_005));
        assert_eq!(visited[2].0, NeedleId(100_005));
    }

    #[test]
    fn test_compact_map_remove() {
        let mut m = CompactMap::new();
        m.set(NeedleId(1), offset(100), Size(50));
        m.set(NeedleId(2), offset(200), Size(60));

        let removed = m.remove(NeedleId(1)).unwrap();
        assert_eq!(removed.size, Size(50));

        assert!(m.get(NeedleId(1)).is_none());
        assert_eq!(m.get(NeedleId(2)).unwrap().size, Size(60));
    }

    #[test]
    fn test_compact_map_reverse_insert_order() {
        let mut m = CompactMap::new();
        // Insert in reverse order to test sorted insert
        for i in (0..100).rev() {
            m.set(NeedleId(i), offset(i as u32), Size(i as i32));
        }
        for i in 0..100 {
            assert_eq!(m.get(NeedleId(i)).unwrap().size, Size(i as i32));
        }
    }
}
