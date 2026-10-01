//! Process-wide coordination of everything that touches one `.ecj` path.
//!
//! Mount-time compaction replaces a deletion journal with a new inode. That is
//! only safe while nothing else in this process can write the old one:
//!
//! - **Holders** are mounted `EcVolume`s with an append handle on the path. A
//!   store holds one `EcVolume` per disk location, and a shard mount or a
//!   cross-disk reconcile can point one disk's volume at another disk's
//!   `.ecj`, so several holders of one path are normal. A holder that keeps
//!   appending to a replaced inode acknowledges deletes that are gone at the
//!   next mount.
//! - **Writers** append to or replace the path by name without holding it
//!   open across calls: `VolumeEcShardsCopy` (`copy_ecj_file`) and EC index
//!   recovery. Bytes they write after the compactor sized the journal would be
//!   dropped by the rename.
//!
//! Compaction therefore runs only while its caller is the sole holder and no
//! writer is active, and while it runs no holder may open the path and no
//! writer may start. Both wait instead; a compaction rewrites only the distinct
//! id set, so the wait is short.
//!
//! Paths are keyed by their canonical parent directory, so two disk locations
//! that spell one directory differently still meet here.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Condvar, LazyLock, Mutex, MutexGuard};

#[derive(Default)]
struct PathState {
    holders: usize,
    writers: usize,
    compacting: bool,
}

impl PathState {
    fn idle(&self) -> bool {
        self.holders == 0 && self.writers == 0 && !self.compacting
    }
}

struct Registry {
    paths: Mutex<HashMap<PathBuf, PathState>>,
    changed: Condvar,
}

static REGISTRY: LazyLock<Registry> = LazyLock::new(|| Registry {
    paths: Mutex::new(HashMap::new()),
    changed: Condvar::new(),
});

fn lock() -> MutexGuard<'static, HashMap<PathBuf, PathState>> {
    // The critical sections only adjust counters and cannot panic midway, so
    // a poisoned lock still guards consistent state.
    REGISTRY.paths.lock().unwrap_or_else(|e| e.into_inner())
}

/// Canonical key for `path`: its resolved parent directory joined with the
/// file name. The file itself may not exist yet (a copy creates it), so only
/// the directory is resolved.
fn key_for(path: &str) -> PathBuf {
    let p = Path::new(path);
    let (Some(parent), Some(name)) = (p.parent(), p.file_name()) else {
        return std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf());
    };
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let dir = std::fs::canonicalize(parent)
        .or_else(|_| std::path::absolute(parent))
        .unwrap_or_else(|_| parent.to_path_buf());
    dir.join(name)
}

/// Block until no compaction is running on `key`, then apply `f` to its state.
fn update_when_not_compacting(key: &Path, f: impl FnOnce(&mut PathState)) {
    let mut paths = lock();
    while paths.get(key).is_some_and(|s| s.compacting) {
        paths = REGISTRY
            .changed
            .wait(paths)
            .unwrap_or_else(|e| e.into_inner());
    }
    f(paths.entry(key.to_path_buf()).or_default());
}

fn release(key: &Path, f: impl FnOnce(&mut PathState)) {
    let mut paths = lock();
    if let Some(state) = paths.get_mut(key) {
        f(state);
        if state.idle() {
            paths.remove(key);
        }
    }
    drop(paths);
    REGISTRY.changed.notify_all();
}

/// A mounted `EcVolume`'s registration as a holder of its `.ecj`. Taken before
/// the journal is opened and released when dropped.
pub(crate) struct EcjHold {
    key: PathBuf,
}

impl EcjHold {
    /// Register as a holder of `ecj_path`, first waiting out any compaction in
    /// progress so the handle opened afterwards is on the final inode.
    pub(crate) fn acquire(ecj_path: &str) -> Self {
        let key = key_for(ecj_path);
        update_when_not_compacting(&key, |s| s.holders += 1);
        EcjHold { key }
    }

    /// Reserve the path for a compaction, or `None` when another holder or an
    /// active writer could still reach the current inode.
    pub(crate) fn try_begin_compaction(&self) -> Option<EcjCompaction> {
        let mut paths = lock();
        let state = paths.get_mut(&self.key)?;
        if state.holders != 1 || state.writers != 0 || state.compacting {
            return None;
        }
        state.compacting = true;
        Some(EcjCompaction {
            key: self.key.clone(),
        })
    }
}

impl Drop for EcjHold {
    fn drop(&mut self) {
        release(&self.key, |s| s.holders = s.holders.saturating_sub(1));
    }
}

/// An exclusive reservation of a `.ecj` path for compaction. Holders and
/// writers wait until it is dropped.
pub(crate) struct EcjCompaction {
    key: PathBuf,
}

impl Drop for EcjCompaction {
    fn drop(&mut self) {
        release(&self.key, |s| s.compacting = false);
    }
}

/// An out-of-band writer (shard copy, index recovery) on a `.ecj` path.
/// Compaction does not start while one is alive.
pub(crate) struct EcjWrite {
    key: PathBuf,
}

impl Drop for EcjWrite {
    fn drop(&mut self) {
        release(&self.key, |s| s.writers = s.writers.saturating_sub(1));
    }
}

/// Register as a writer of `ecj_path`, waiting out any compaction in progress.
/// Blocks; async callers use [`begin_ecj_write_async`].
pub(crate) fn begin_ecj_write(ecj_path: &str) -> EcjWrite {
    let key = key_for(ecj_path);
    update_when_not_compacting(&key, |s| s.writers += 1);
    EcjWrite { key }
}

/// [`begin_ecj_write`] for async handlers: the wait runs on the blocking pool
/// so a compaction in progress never stalls a runtime worker.
pub(crate) async fn begin_ecj_write_async(ecj_path: &str) -> EcjWrite {
    let path = ecj_path.to_string();
    match tokio::task::spawn_blocking(move || begin_ecj_write(&path)).await {
        Ok(write) => write,
        // Only a panic inside the registry lands here, and it leaves no count
        // behind; registering inline is still correct, merely blocking.
        Err(_) => begin_ecj_write(ecj_path),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::TempDir;

    fn ecj(dir: &TempDir) -> String {
        dir.path().join("1.ecj").to_str().unwrap().to_string()
    }

    #[test]
    fn sole_holder_may_compact() {
        let dir = TempDir::new().unwrap();
        let hold = EcjHold::acquire(&ecj(&dir));
        assert!(hold.try_begin_compaction().is_some());
    }

    #[test]
    fn second_holder_blocks_compaction() {
        let dir = TempDir::new().unwrap();
        let a = EcjHold::acquire(&ecj(&dir));
        let b = EcjHold::acquire(&ecj(&dir));
        assert!(a.try_begin_compaction().is_none());
        drop(b);
        assert!(a.try_begin_compaction().is_some());
    }

    #[test]
    fn active_writer_blocks_compaction() {
        let dir = TempDir::new().unwrap();
        let hold = EcjHold::acquire(&ecj(&dir));
        let w = begin_ecj_write(&ecj(&dir));
        assert!(hold.try_begin_compaction().is_none());
        drop(w);
        assert!(hold.try_begin_compaction().is_some());
    }

    #[test]
    fn differently_spelled_paths_share_one_key() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        let plain = ecj(&dir);
        let dotted = dir
            .path()
            .join("sub")
            .join("..")
            .join("1.ecj")
            .to_str()
            .unwrap()
            .to_string();
        let a = EcjHold::acquire(&plain);
        let _b = EcjHold::acquire(&dotted);
        assert!(a.try_begin_compaction().is_none());
    }

    #[test]
    fn writer_waits_for_compaction_to_finish() {
        let dir = TempDir::new().unwrap();
        let path = ecj(&dir);
        let hold = EcjHold::acquire(&path);
        let compaction = hold.try_begin_compaction().unwrap();

        let (tx, rx) = mpsc::channel();
        let p = path.clone();
        let t = std::thread::spawn(move || {
            let _w = begin_ecj_write(&p);
            tx.send(()).unwrap();
        });
        assert!(
            rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "a writer must not start while a compaction holds the path",
        );
        drop(compaction);
        rx.recv_timeout(Duration::from_secs(5))
            .expect("writer must proceed once the compaction ends");
        t.join().unwrap();
    }
}
