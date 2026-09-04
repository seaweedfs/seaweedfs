//! Bounded pool of open index-file descriptors.
//!
//! Read-only volumes — cloud-tiered ones above all — outnumber writable ones by
//! orders of magnitude on a large server, and a volume that pins its `.idx` and
//! `.sdx` for the life of the process costs two descriptors whether or not
//! anybody reads it. At ~600K volumes per server that alone exhausts any fd
//! limit. Neither file is needed except while a lookup is in flight, so
//! [`SortedFileNeedleMap`](super::sorted_file::SortedFileNeedleMap) borrows them
//! from this pool: an idle volume holds nothing, a busy one keeps its handles
//! hot rather than paying an `open()` per needle.
//!
//! Mirrors Go's `weed/storage/needle_map_file_pool.go`. Handles are handed out
//! as `Arc<File>`, so an eviction cannot close a descriptor a reader still
//! holds — the file closes when the last borrower drops its `Arc`.

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io;
use std::sync::{Arc, Mutex, OnceLock};

use crate::storage::volume_open::open_volume_file;

/// Descriptors the pool keeps open. Matches Go's `maxPooledIndexFiles`.
pub const MAX_POOLED_INDEX_FILES: usize = 1024;

struct Entry {
    file: Arc<File>,
    tick: u64,
}

#[derive(Default)]
struct Inner {
    entries: HashMap<String, Entry>,
    /// Recency order, oldest tick first, so eviction is a `pop_first`.
    order: BTreeMap<u64, String>,
    next_tick: u64,
}

pub struct IndexFilePool {
    capacity: usize,
    inner: Mutex<Inner>,
}

/// Writable and read-only handles for the same path are pooled separately so a
/// read never depends on the file being openable for write — a volume served
/// off a read-only mount still answers lookups.
fn pool_key(path: &str, writable: bool) -> String {
    if writable {
        format!("{path}\0rw")
    } else {
        path.to_string()
    }
}

impl IndexFilePool {
    pub fn new(capacity: usize) -> Self {
        IndexFilePool {
            capacity: capacity.max(1),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Hand out an open handle for `path`, reusing the pooled one when there is
    /// one. The descriptor lives as long as the returned `Arc`.
    pub fn borrow(&self, path: &str, writable: bool) -> io::Result<Arc<File>> {
        let key = pool_key(path, writable);
        if let Some(file) = self.touch(&key) {
            return Ok(file);
        }

        // Opened outside the lock: a cold open blocks on disk, and holding a
        // process-wide mutex across it would serialize every volume's lookups.
        let file = Arc::new(open_volume_file(
            OpenOptions::new().read(true).write(writable),
            path,
        )?);
        Ok(self.insert(key, file))
    }

    /// Forget the pooled handles for `path`, so a later rename or delete of that
    /// path cannot be served from a descriptor on the old inode.
    pub fn discard(&self, path: &str) {
        let mut inner = self.inner.lock().unwrap();
        for key in [pool_key(path, false), pool_key(path, true)] {
            if let Some(entry) = inner.entries.remove(&key) {
                inner.order.remove(&entry.tick);
            }
        }
    }

    /// Descriptors currently pooled. Test-only visibility into the bound.
    #[cfg(test)]
    pub fn pooled_count(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    fn touch(&self, key: &str) -> Option<Arc<File>> {
        let mut inner = self.inner.lock().unwrap();
        let tick = inner.next_tick;
        let entry = inner.entries.get_mut(key)?;
        let file = entry.file.clone();
        let old_tick = std::mem::replace(&mut entry.tick, tick);
        inner.order.remove(&old_tick);
        inner.order.insert(tick, key.to_string());
        inner.next_tick += 1;
        Some(file)
    }

    fn insert(&self, key: String, file: Arc<File>) -> Arc<File> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.entries.get(&key) {
            // Another borrower opened the same path first; keep one descriptor.
            return entry.file.clone();
        }
        let tick = inner.next_tick;
        inner.next_tick += 1;
        inner.order.insert(tick, key.clone());
        inner.entries.insert(
            key,
            Entry {
                file: file.clone(),
                tick,
            },
        );
        while inner.entries.len() > self.capacity {
            let Some((_, oldest)) = inner.order.pop_first() else {
                break;
            };
            inner.entries.remove(&oldest);
        }
        file
    }
}

/// Process-wide pool shared by every read-only volume on this server.
pub fn pooled_index_files() -> &'static IndexFilePool {
    static POOL: OnceLock<IndexFilePool> = OnceLock::new();
    POOL.get_or_init(|| IndexFilePool::new(MAX_POOLED_INDEX_FILES))
}

/// Descriptors this process holds on `.idx`/`.sdx` files under `dir`, read from
/// `/proc/self/fd` where it exists and from `lsof` otherwise. `None` when
/// neither is available, so a caller can skip rather than assert vacuously.
#[cfg(test)]
pub(crate) fn open_index_fds(dir: &std::path::Path) -> Option<usize> {
    let prefix = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    let is_index = |target: &std::path::Path| {
        target.starts_with(&prefix)
            && matches!(
                target.extension().and_then(|e| e.to_str()),
                Some("idx") | Some("sdx")
            )
    };

    if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
        return Some(
            entries
                .filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
                .filter(|target| is_index(target))
                .count(),
        );
    }

    let out = std::process::Command::new("lsof")
        .args(["-p", &std::process::id().to_string(), "-F", "n"])
        .output()
        .ok()?;
    Some(
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|line| line.strip_prefix('n'))
            .filter(|line| is_index(std::path::Path::new(line)))
            .count(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_file(dir: &std::path::Path, name: &str, contents: &[u8]) -> String {
        let path = dir.join(name);
        let mut f = File::create(&path).unwrap();
        f.write_all(contents).unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn evicted_handle_stays_usable_for_its_borrower() {
        let dir = tempfile::tempdir().unwrap();
        let first = write_file(dir.path(), "first", b"first");
        let second = write_file(dir.path(), "second", b"second");

        let pool = IndexFilePool::new(1);
        let borrowed = pool.borrow(&first, false).unwrap();

        // Pushes the single slot over, evicting the entry still in use.
        let _other = pool.borrow(&second, false).unwrap();
        assert_eq!(pool.pooled_count(), 1);

        let mut buf = [0u8; 5];
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            borrowed.read_exact_at(&mut buf, 0).unwrap();
        }
        assert_eq!(&buf, b"first");
    }

    #[test]
    fn borrow_reuses_the_pooled_handle() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "idx", b"x");

        let pool = IndexFilePool::new(4);
        let a = pool.borrow(&path, false).unwrap();
        let b = pool.borrow(&path, false).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(pool.pooled_count(), 1);

        // A writable handle is pooled separately from the read-only one.
        let w = pool.borrow(&path, true).unwrap();
        assert!(!Arc::ptr_eq(&a, &w));
        assert_eq!(pool.pooled_count(), 2);
    }

    #[test]
    fn discard_drops_both_handles() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "idx", b"x");

        let pool = IndexFilePool::new(4);
        let _r = pool.borrow(&path, false).unwrap();
        let _w = pool.borrow(&path, true).unwrap();
        assert_eq!(pool.pooled_count(), 2);

        pool.discard(&path);
        assert_eq!(pool.pooled_count(), 0);
    }

    #[test]
    fn pool_stays_within_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let pool = IndexFilePool::new(3);
        for i in 0..10 {
            let path = write_file(dir.path(), &format!("f{i}"), b"x");
            let _ = pool.borrow(&path, false).unwrap();
        }
        assert_eq!(pool.pooled_count(), 3);
    }
}
