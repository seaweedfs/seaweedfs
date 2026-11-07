//! A small fileId-keyed whole-chunk cache for read prefetch.
//!
//! SeaweedFS needles are immutable for a given fileId (`<vid>,<key><cookie>`),
//! so caching a chunk's bytes by fileId is coherence-safe — no invalidation is
//! ever needed, only capacity-bounded LRU eviction. This lets the read path
//! prefetch upcoming chunks in parallel (independent of the kernel's small,
//! single-chunk read upcalls) so sequential reads hit already-fetched bytes.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// Bounded LRU of fileId -> whole-chunk bytes. Not thread-safe on its own; wrap
/// in a `Mutex`. All ops are O(cache size) at worst (the LRU order is a deque),
/// which is fine for the few-hundred-entry caches used here.
pub struct ChunkCache {
    map: HashMap<String, Arc<Vec<u8>>>,
    order: VecDeque<String>, // front = least-recently-used
    bytes: usize,
    cap: usize,
}

impl ChunkCache {
    pub fn new(cap_bytes: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            cap: cap_bytes,
        }
    }

    /// Fetch a cached chunk, marking it most-recently-used.
    pub fn get(&mut self, file_id: &str) -> Option<Arc<Vec<u8>>> {
        let v = self.map.get(file_id)?.clone();
        if let Some(pos) = self.order.iter().position(|x| x == file_id) {
            let f = self.order.remove(pos).unwrap();
            self.order.push_back(f);
        }
        Some(v)
    }

    pub fn contains(&self, file_id: &str) -> bool {
        self.map.contains_key(file_id)
    }

    /// Insert a chunk (no-op if empty, already present or larger than the whole
    /// cap). Evicts least-recently-used entries until within capacity.
    pub fn put(&mut self, file_id: String, data: Arc<Vec<u8>>) {
        if self.map.contains_key(&file_id) {
            return;
        }
        let len = data.len();
        if len == 0 || len > self.cap {
            return; // a single chunk bigger than the whole cache: don't churn
        }
        self.map.insert(file_id.clone(), data);
        self.order.push_back(file_id);
        self.bytes += len;
        while self.bytes > self.cap {
            let Some(old) = self.order.pop_front() else {
                break;
            };
            if let Some(o) = self.map.remove(&old) {
                self.bytes -= o.len();
            }
        }
    }

    #[cfg(test)]
    pub fn len_bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(n: usize) -> Arc<Vec<u8>> {
        Arc::new(vec![0u8; n])
    }

    #[test]
    fn get_miss_then_hit() {
        let mut c = ChunkCache::new(1024);
        assert!(c.get("a").is_none());
        c.put("a".into(), chunk(100));
        assert_eq!(c.get("a").unwrap().len(), 100);
    }

    #[test]
    fn put_is_idempotent_on_bytes() {
        let mut c = ChunkCache::new(1024);
        c.put("a".into(), chunk(100));
        c.put("a".into(), chunk(100)); // duplicate ignored
        assert_eq!(c.len_bytes(), 100);
    }

    #[test]
    fn lru_evicts_oldest_first() {
        let mut c = ChunkCache::new(250);
        c.put("a".into(), chunk(100));
        c.put("b".into(), chunk(100));
        // touch a so b is now least-recently-used
        assert!(c.get("a").is_some());
        c.put("c".into(), chunk(100)); // bytes would be 300 > 250 -> evict LRU (b)
        assert!(c.contains("a"));
        assert!(!c.contains("b"));
        assert!(c.contains("c"));
        assert_eq!(c.len_bytes(), 200);
    }

    #[test]
    fn empty_chunks_are_not_cached() {
        let mut c = ChunkCache::new(0);
        for i in 0..10 {
            c.put(i.to_string(), chunk(0));
        }
        assert!(c.map.is_empty() && c.order.is_empty());
    }

    #[test]
    fn oversized_chunk_not_cached() {
        let mut c = ChunkCache::new(100);
        c.put("big".into(), chunk(200));
        assert!(!c.contains("big"));
        assert_eq!(c.len_bytes(), 0);
    }
}
