//! Async batched write processing for the volume server.
//!
//! Durable (fsync) uploads are submitted here instead of each handler calling
//! `write_needle` directly — Go's `writeNeedle2` makes the same split, so the
//! queue is always constructed. Each volume gets its own FIFO worker (Go's
//! volume_write.go keeps one batching worker per volume), which drains its
//! channel in batches of up to 128 entries or 4 MiB and shares the .dat/.idx
//! flushes across durable writes (see `Volume::write_needles_grouped`).
//! Writes to different volumes proceed in parallel; a volume's ordering and
//! exclusion still come from its data-file write lease.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tracing::debug;

use crate::storage::needle::needle::Needle;
use crate::storage::types::{Size, VolumeId};
use crate::storage::volume::VolumeError;

use super::volume_server::VolumeServerState;

/// Result of a single write operation: (offset, size, is_unchanged).
pub type WriteResult = Result<(u64, Size, bool), VolumeError>;

/// The needles queued for one volume, each with the durability it asked for.
type VolumeBatch = Vec<(Needle, bool, oneshot::Sender<WriteResult>)>;

/// A request to write a needle, submitted to the write queue.
pub struct WriteRequest {
    pub volume_id: VolumeId,
    pub needle: Needle,
    pub fsync: bool,
    pub response_tx: oneshot::Sender<WriteResult>,
    /// The global queued-slot permit, owned by the request itself: a caller
    /// that cancels `submit` after the send must not free the slot while the
    /// worker still holds the needle.
    pub queue_permit: Option<OwnedSemaphorePermit>,
}

/// Maximum number of write requests to batch together.
const MAX_BATCH_SIZE: usize = 128;

/// Maximum bytes to accumulate per batch before breaking (matches Go's 4MB limit).
/// This prevents large writes from accumulating unbounded latency.
const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// Cap on volumes doing grouped I/O at once, so many writers cannot crowd
/// readers off a slow disk the way unlimited parallel fsyncs would.
const MAX_CONCURRENT_VOLUME_WRITES: usize = 8;

/// How often an idle worker re-checks that its volume is still mounted.
/// Workers live for their volume's queue lifetime (Go parity), so the only
/// retirement is the volume leaving the store.
const IDLE_WORKER_POLL: std::time::Duration = std::time::Duration::from_secs(60);

/// Tunes the per-volume durable-write worker. Once the queue drains with the
/// batch still below min_bytes and min_requests, the batch stays open up to
/// wait for stragglers instead of paying an fsync per request. Mirrors Go's
/// storage.SyncBatchOption.
#[derive(Clone, Copy)]
pub struct SyncBatchOption {
    pub min_bytes: usize,
    pub min_requests: usize,
    pub wait: std::time::Duration,
}

impl Default for SyncBatchOption {
    fn default() -> Self {
        SyncBatchOption {
            min_bytes: 1024 * 1024,
            min_requests: MAX_BATCH_SIZE,
            wait: std::time::Duration::from_micros(500),
        }
    }
}

/// Handle for submitting write requests to the per-volume workers.
#[derive(Clone)]
pub struct WriteQueue {
    state: Arc<VolumeServerState>,
    /// Per-volume channel capacity; backpressure kicks in when full.
    capacity: usize,
    io_permits: Arc<Semaphore>,
    /// Bounds queued+in-flight writes across all volumes — per-volume channels
    /// alone would multiply the old single queue's capacity by volume count.
    queue_permits: Arc<Semaphore>,
    workers: Arc<Mutex<HashMap<VolumeId, mpsc::Sender<WriteRequest>>>>,
    sync_batch: SyncBatchOption,
    /// Submissions accepted, for tests asserting which writes route here.
    #[cfg(test)]
    submits: Arc<std::sync::atomic::AtomicU64>,
    /// Times a worker armed the straggler window, for tests that must land a
    /// write inside an already-open batch.
    #[cfg(test)]
    windows_armed: Arc<std::sync::atomic::AtomicU64>,
}

impl WriteQueue {
    /// Create a new write queue. Per-volume workers are spawned lazily on the
    /// first submission for that volume.
    pub fn new(
        state: Arc<VolumeServerState>,
        capacity: usize,
        sync_batch: SyncBatchOption,
    ) -> Self {
        WriteQueue {
            state,
            capacity,
            io_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_VOLUME_WRITES)),
            queue_permits: Arc::new(Semaphore::new(capacity)),
            workers: Arc::new(Mutex::new(HashMap::new())),
            sync_batch,
            #[cfg(test)]
            submits: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(test)]
            windows_armed: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// How many writes were submitted, for tests asserting upload routing.
    #[cfg(test)]
    pub fn submits_for_test(&self) -> u64 {
        self.submits.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// How many batch windows workers have armed so far.
    #[cfg(test)]
    fn windows_armed_for_test(&self) -> u64 {
        self.windows_armed.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Sender for the volume's worker, spawning one on first use.
    fn worker_for(&self, volume_id: VolumeId) -> mpsc::Sender<WriteRequest> {
        let mut workers = self.workers.lock().unwrap();
        match workers.entry(volume_id) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let (tx, rx) = mpsc::channel(self.capacity);
                tokio::spawn(
                    VolumeWriteWorker {
                        rx,
                        vid: volume_id,
                        state: self.state.clone(),
                        io_permits: self.io_permits.clone(),
                        workers: self.workers.clone(),
                        sync_batch: self.sync_batch,
                        #[cfg(test)]
                        windows_armed: self.windows_armed.clone(),
                    }
                    .run(),
                );
                e.insert(tx.clone());
                tx
            }
        }
    }

    /// Submit a write request and wait for the result.
    ///
    /// Returns `Err` if the worker has shut down or the response channel was dropped.
    pub async fn submit(&self, volume_id: VolumeId, needle: Needle, fsync: bool) -> WriteResult {
        // Workers spawn only for mounted volumes — a bogus id never creates
        // a channel, so volume-id sprays can't grow the worker map.
        #[cfg(test)]
        self.submits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        {
            let store = self.state.store.read().unwrap();
            if store.find_volume(volume_id).is_none() {
                return Err(VolumeError::NotFound);
            }
        }
        // The permit travels inside the request, so the global queued bound
        // covers the request until the worker drops it — including after a
        // canceled submit.
        let queue_permit = self
            .queue_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| broken_pipe("write queue shut down"))?;
        let (response_tx, response_rx) = oneshot::channel();
        let mut request = WriteRequest {
            volume_id,
            needle,
            fsync,
            response_tx,
            queue_permit: Some(queue_permit),
        };

        // A send fails only against a retiring worker's closed channel; the
        // request was never accepted, so retrying it behind a fresh worker
        // keeps same-volume order. The volume recheck bounds the spin.
        loop {
            let tx = self.worker_for(volume_id);
            match tx.send(request).await {
                Ok(()) => break,
                Err(mpsc::error::SendError(returned)) => {
                    request = returned;
                    {
                        let store = self.state.store.read().unwrap();
                        if store.find_volume(volume_id).is_none() {
                            return Err(VolumeError::NotFound);
                        }
                    }
                    tokio::task::yield_now().await;
                }
            }
        }

        // Wait for the worker to process our request.
        match response_rx.await {
            Ok(result) => result,
            Err(_) => Err(broken_pipe("write queue worker dropped response channel")),
        }
    }
}

fn broken_pipe(msg: &'static str) -> VolumeError {
    VolumeError::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, msg))
}

/// FIFO worker draining one volume's write channel in batches. Like Go's
/// per-volume async writer it runs for the volume's queue lifetime — exiting
/// only when the channel closes or its volume leaves the store — so no
/// handoff can ever split one volume's write order across two workers.
struct VolumeWriteWorker {
    rx: mpsc::Receiver<WriteRequest>,
    vid: VolumeId,
    state: Arc<VolumeServerState>,
    io_permits: Arc<Semaphore>,
    workers: Arc<Mutex<HashMap<VolumeId, mpsc::Sender<WriteRequest>>>>,
    sync_batch: SyncBatchOption,
    #[cfg(test)]
    windows_armed: Arc<std::sync::atomic::AtomicU64>,
}

impl VolumeWriteWorker {
    async fn run(mut self) {
        loop {
            let first = match tokio::time::timeout(IDLE_WORKER_POLL, self.rx.recv()).await {
                Ok(Some(req)) => req,
                // recv() yields None only once every sender is gone — the map
                // holds a sender per volume, so that is exactly queue shutdown.
                Ok(None) => return,
                Err(_) if self.volume_gone() => {
                    self.retire().await;
                    return;
                }
                Err(_) => continue,
            };
            self.process_from(first).await;
        }
    }

    /// The volume's handle left the store, so the worker can go too; an idle
    /// worker for a mounted volume stays (Go keeps the writer for the mount).
    fn volume_gone(&self) -> bool {
        let store = self.state.store.read().unwrap();
        store.find_volume(self.vid).is_none()
    }

    /// Retire in a fixed order that keeps same-volume FIFO: close the channel
    /// so racing senders fail fast (they retry and wait for a fresh worker),
    /// drain what was already accepted, and only then give up the map slot —
    /// a replacement worker can never start while this one still holds queued
    /// writes.
    async fn retire(&mut self) {
        self.rx.close();
        while let Some(first) = self.rx.recv().await {
            self.process_from(first).await;
        }
        self.workers.lock().unwrap().remove(&self.vid);
    }

    /// Collect a batch around `first` and process it under the I/O bound.
    async fn process_from(&mut self, first: WriteRequest) {
        // Drain as many additional requests as available, up to MAX_BATCH_SIZE
        // or MAX_BATCH_BYTES (matches Go: 128 requests or 4MB, whichever comes first).
        let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
        let mut batch_bytes: usize = first.needle.data.len();
        batch.push(first);
        // Armed once the queue first drains below the minimum batch; holds the
        // batch open for stragglers so the wait bound is per batch, not per
        // lull (Go's batchWait).
        let mut window_deadline = None;
        while batch.len() < MAX_BATCH_SIZE && batch_bytes < MAX_BATCH_BYTES {
            if window_deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
                break;
            }
            let req = match self.rx.try_recv() {
                Ok(req) => req,
                Err(mpsc::error::TryRecvError::Empty) => {
                    let sync = self.sync_batch;
                    if sync.wait.is_zero()
                        || batch_bytes >= sync.min_bytes
                        || batch.len() >= sync.min_requests
                    {
                        // a drained queue flushes right away once it holds enough
                        break;
                    }
                    let deadline = *window_deadline.get_or_insert_with(|| {
                        #[cfg(test)]
                        self.windows_armed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tokio::time::Instant::now() + sync.wait
                    });
                    match tokio::time::timeout_at(deadline, self.rx.recv()).await {
                        Ok(Some(req)) => req,
                        Ok(None) | Err(_) => break,
                    }
                }
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            };
            batch_bytes += req.needle.data.len();
            batch.push(req);
        }
        debug!("processing write batch of {} requests", batch.len());

        // Bound the number of volumes doing grouped I/O at once, then run
        // the batch in spawn_blocking since write_needle does file I/O.
        let Ok(_permit) = self.io_permits.clone().acquire_owned().await else {
            return;
        };
        let state = self.state.clone();
        let _ = tokio::task::spawn_blocking(move || {
            process_batch(state, batch);
        })
        .await;
    }
}

/// Process a batch of write requests, grouped by volume ID.
///
/// Groups writes by volume to minimize the number of store lock acquisitions,
/// then sends results back via each request's oneshot channel.
fn process_batch(state: Arc<VolumeServerState>, batch: Vec<WriteRequest>) {
    // Group requests by volume ID for efficient processing.
    // We use a Vec of (VolumeId, Vec<(Needle, Sender)>) to preserve order
    // and avoid requiring Hash on VolumeId.
    let mut groups: Vec<(VolumeId, VolumeBatch)> = Vec::new();

    for req in batch {
        let vid = req.volume_id;
        if let Some(group) = groups.iter_mut().find(|(v, _)| *v == vid) {
            group.1.push((req.needle, req.fsync, req.response_tx));
        } else {
            groups.push((vid, vec![(req.needle, req.fsync, req.response_tx)]));
        }
    }

    // Resolve each group's volume handle under a brief store read, then run
    // the group's I/O with the guard released — one volume's write must not
    // block admission on another volume's.
    for (vid, entries) in groups {
        let (mut writes, senders): (Vec<_>, Vec<_>) = entries
            .into_iter()
            .map(|(needle, fsync, response_tx)| ((needle, fsync), response_tx))
            .unzip();
        let vol = {
            let store = state.store.read().unwrap();
            store.writable_volume(vid)
        };
        let results = match vol {
            Ok(vol) => vol.write_needles_grouped(&mut writes),
            Err(e) => writes
                .iter()
                .map(|_| match e {
                    VolumeError::ReadOnly(vid) => Err(VolumeError::ReadOnly(vid)),
                    _ => Err(VolumeError::NotFound),
                })
                .collect(),
        };
        for (response_tx, result) in senders.into_iter().zip(results) {
            // Send result back; ignore error if receiver dropped.
            let _ = response_tx.send(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::types::VolumeId;

    /// Helper to create a minimal VolumeServerState for testing.
    fn make_test_state() -> Arc<VolumeServerState> {
        use crate::security::{Guard, SigningKey};
        use crate::server::volume_server::RuntimeMetricsConfig;
        use crate::storage::needle_map::NeedleMapKind;
        use crate::storage::store::Store;
        use std::sync::RwLock;
        use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32};

        let store = Store::new(NeedleMapKind::InMemory);
        let guard = Guard::new(&[], SigningKey(vec![]), 0, SigningKey(vec![]), 0);

        Arc::new(VolumeServerState {
            store: RwLock::new(store),
            guard: RwLock::new(guard),
            is_stopping: RwLock::new(false),
            maintenance: AtomicBool::new(false),
            state_version: AtomicU32::new(0),
            concurrent_upload_limit: 0,
            concurrent_download_limit: 0,
            inflight_upload_data_timeout: std::time::Duration::ZERO,
            inflight_download_data_timeout: std::time::Duration::ZERO,
            inflight_upload_bytes: AtomicI64::new(0),
            inflight_download_bytes: AtomicI64::new(0),
            upload_notify: tokio::sync::Notify::new(),
            download_notify: tokio::sync::Notify::new(),
            data_center: String::new(),
            rack: String::new(),
            file_size_limit_bytes: 0,
            maintenance_byte_per_second: 0,
            vacuum_commit_slots: None,
            vacuum_commits_waiting: std::sync::atomic::AtomicU32::new(0),
            is_heartbeating: AtomicBool::new(false),
            has_master: false,
            pre_stop_seconds: 0,
            volume_state_notify: tokio::sync::Notify::new(),
            write_queue: std::sync::OnceLock::new(),
            read_mode: crate::config::ReadMode::Local,
            allow_untrusted_remote_endpoints: false,
            master_url: String::new(),
            master_urls: Vec::new(),
            seed_master_set: std::collections::HashSet::new(),
            current_master_url: tokio::sync::RwLock::new(String::new()),
            self_url: String::new(),
            http_client: reqwest::Client::new(),
            outgoing_http_scheme: "http".to_string(),
            outgoing_grpc_tls: None,
            metrics_runtime: std::sync::RwLock::new(RuntimeMetricsConfig::default()),
            metrics_notify: tokio::sync::Notify::new(),
            fix_jpg_orientation: false,
            has_slow_read: true,
            read_buffer_size_bytes: 4 * 1024 * 1024,
            security_file: String::new(),
            cli_white_list: vec![],
            state_file_path: String::new(),
            ec_decodes_in_flight: std::sync::Mutex::new(std::collections::HashSet::new()),
            volume_copies_in_flight: std::sync::Mutex::new(std::collections::HashSet::new()),
            ec_decode_tail: std::sync::Mutex::new(std::collections::HashSet::new()),
            ec_decode_tail_notify: tokio::sync::Notify::new(),
        })
    }

    #[tokio::test]
    async fn test_write_queue_submit_no_volume() {
        // Submit a write to a non-existent volume -- should return VolumeError::NotFound.
        let state = make_test_state();
        let queue = WriteQueue::new(state, MAX_BATCH_SIZE, SyncBatchOption::default());

        let needle = Needle {
            id: 1.into(),
            cookie: 0x12345678.into(),
            data: vec![1, 2, 3],
            data_size: 3,
            ..Needle::default()
        };

        let result = queue.submit(VolumeId(999), needle, false).await;
        assert!(result.is_err());
        match result {
            Err(VolumeError::NotFound) => {} // expected
            other => panic!("expected NotFound, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_write_queue_concurrent_submissions() {
        // Submit multiple concurrent writes -- all should complete (with errors since no volume).
        let state = make_test_state();
        let queue = WriteQueue::new(state, MAX_BATCH_SIZE, SyncBatchOption::default());

        let mut handles = Vec::new();
        for i in 0..10u64 {
            let q = queue.clone();
            handles.push(tokio::spawn(async move {
                let needle = Needle {
                    id: i.into(),
                    cookie: 0xABCD.into(),
                    data: vec![i as u8; 10],
                    data_size: 10,
                    ..Needle::default()
                };
                q.submit(VolumeId(1), needle, false).await
            }));
        }

        for handle in handles {
            let result = handle.await.unwrap();
            // All should fail with NotFound since there's no volume 1
            assert!(matches!(result, Err(VolumeError::NotFound)));
        }
    }

    #[tokio::test]
    async fn test_write_queue_batching() {
        // Verify that many concurrent writes get processed (testing the batching path).
        let state = make_test_state();
        let queue = WriteQueue::new(state, MAX_BATCH_SIZE, SyncBatchOption::default());

        // Submit MAX_BATCH_SIZE requests concurrently
        let mut handles = Vec::new();
        for i in 0..MAX_BATCH_SIZE as u64 {
            let q = queue.clone();
            handles.push(tokio::spawn(async move {
                let needle = Needle {
                    id: i.into(),
                    cookie: 0x1111.into(),
                    data: vec![0u8; 4],
                    data_size: 4,
                    ..Needle::default()
                };
                q.submit(VolumeId(42), needle, false).await
            }));
        }

        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }

        // All should complete (with NotFound errors since no volume exists)
        assert_eq!(results.len(), MAX_BATCH_SIZE);
        for r in results {
            assert!(matches!(r, Err(VolumeError::NotFound)));
        }
    }

    /// The queue hands a volume's batch to the grouped path, so ten durable
    /// writes cost one .dat sync and one .idx sync, not ten of each.
    #[test]
    fn test_process_batch_group_commits_fsync_writes() {
        use crate::config::MinFreeSpace;
        use crate::storage::types::{DiskType, NeedleId};
        use crate::storage::volume::VolumeSpec;

        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let state = make_test_state();
        {
            let mut store = state.store.write().unwrap();
            store
                .add_location(
                    dir,
                    dir,
                    10,
                    DiskType::HardDrive,
                    MinFreeSpace::Percent(1.0),
                    Vec::new(),
                )
                .unwrap();
            store
                .add_volume(VolumeId(1), DiskType::HardDrive, &VolumeSpec::default())
                .unwrap();
        }

        let mut receivers = Vec::new();
        let batch = (1..=10u64)
            .map(|id| {
                let (response_tx, response_rx) = oneshot::channel();
                receivers.push(response_rx);
                WriteRequest {
                    volume_id: VolumeId(1),
                    needle: Needle {
                        id: NeedleId(id),
                        cookie: 0x1111.into(),
                        data: vec![id as u8; 8],
                        data_size: 8,
                        ..Needle::default()
                    },
                    fsync: true,
                    response_tx,
                    queue_permit: None,
                }
            })
            .collect();
        process_batch(state.clone(), batch);

        for mut rx in receivers {
            assert!(matches!(rx.try_recv().unwrap(), Ok((_, _, false))));
        }
        let store = state.store.read().unwrap();
        let (_, vol) = store.find_volume(VolumeId(1)).unwrap();
        assert_eq!(vol.sync_counts_for_test(), (1, 1));
    }

    #[tokio::test]
    async fn test_write_queue_dropped_sender() {
        // When the queue is dropped, subsequent submits should fail gracefully.
        let state = make_test_state();
        let queue = WriteQueue::new(state, 1, SyncBatchOption::default());

        // Clone then drop the original -- the worker keeps running via its rx handle.
        let queue2 = queue.clone();
        drop(queue);

        // This should still work since the worker is alive.
        let needle = Needle {
            id: 1.into(),
            cookie: 0.into(),
            data: vec![],
            data_size: 0,
            ..Needle::default()
        };
        let result = queue2.submit(VolumeId(1), needle, false).await;
        assert!(result.is_err()); // NotFound is fine -- the point is it doesn't panic
    }

    /// Issue #791: a write parked inside its .dat append must not hold the
    /// store guard, so another volume still resolves and writes. The hook
    /// runs under the volume's data-file lease, so the assertion is that the
    /// store — not the per-volume serialization — stays open.
    #[test]
    fn test_process_batch_does_not_block_other_volumes() {
        use crate::config::MinFreeSpace;
        use crate::storage::types::{DiskType, NeedleId};
        use crate::storage::volume::{VolumeSpec, needle_write_hook};
        use std::sync::Barrier;

        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let state = make_test_state();
        {
            let mut store = state.store.write().unwrap();
            store
                .add_location(
                    dir,
                    dir,
                    10,
                    DiskType::HardDrive,
                    MinFreeSpace::Percent(1.0),
                    Vec::new(),
                )
                .unwrap();
            for vid in [VolumeId(1), VolumeId(2)] {
                store
                    .add_volume(vid, DiskType::HardDrive, &VolumeSpec::default())
                    .unwrap();
            }
        }

        let in_append = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let _reg = needle_write_hook::register(NeedleId(101), {
            let in_append = in_append.clone();
            let release = release.clone();
            move |_| {
                in_append.wait();
                release.wait();
            }
        });

        let (response_tx, mut response_rx) = oneshot::channel();
        let batch = vec![WriteRequest {
            volume_id: VolumeId(1),
            needle: Needle {
                id: NeedleId(101),
                cookie: 0x1111.into(),
                data: vec![7u8; 8],
                data_size: 8,
                ..Needle::default()
            },
            fsync: false,
            response_tx,
            queue_permit: None,
        }];
        let writer = {
            let state = state.clone();
            std::thread::spawn(move || process_batch(state, batch))
        };

        in_append.wait(); // volume 1's write is inside its .dat append.

        // The store still admits a lifecycle writer and a volume-2 write.
        drop(state.store.try_write().unwrap());
        let vol2 = state
            .store
            .read()
            .unwrap()
            .writable_volume(VolumeId(2))
            .unwrap();
        let mut n2 = Needle {
            id: NeedleId(9),
            cookie: 0x2222.into(),
            data: vec![9u8; 8],
            data_size: 8,
            ..Needle::default()
        };
        vol2.write_needle(&mut n2, true, false).unwrap();

        // Same-volume exclusion is intact: volume 1's data-file lease is held.
        let (_, vol1) = state.store.read().unwrap().find_volume(VolumeId(1)).unwrap();
        assert!(vol1.instance().try_write_lock().is_none());

        release.wait();
        writer.join().unwrap();
        assert!(response_rx.try_recv().unwrap().is_ok());
        assert!(vol1.instance().try_write_lock().is_some());
    }

    /// A write parked inside volume 1's .dat append must not keep later
    /// batches for other volumes queued behind it: each volume drains through
    /// its own FIFO worker, while same-volume writes still serialize.
    /// NeedleId(102) is reserved for this test — hooks are keyed by needle id
    /// process-wide and parallel tests must not share one.
    #[tokio::test]
    async fn test_write_queue_does_not_block_other_volumes() {
        use crate::config::MinFreeSpace;
        use crate::storage::types::{DiskType, NeedleId};
        use crate::storage::volume::{VolumeSpec, needle_write_hook};
        use std::sync::Barrier;

        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let state = make_test_state();
        {
            let mut store = state.store.write().unwrap();
            store
                .add_location(
                    dir,
                    dir,
                    10,
                    DiskType::HardDrive,
                    MinFreeSpace::Percent(1.0),
                    Vec::new(),
                )
                .unwrap();
            for vid in [VolumeId(1), VolumeId(2)] {
                store
                    .add_volume(vid, DiskType::HardDrive, &VolumeSpec::default())
                    .unwrap();
            }
        }
        let queue = WriteQueue::new(state, MAX_BATCH_SIZE, SyncBatchOption::default());

        let in_append = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let _reg = needle_write_hook::register(NeedleId(102), {
            let in_append = in_append.clone();
            let release = release.clone();
            move |_| {
                in_append.wait();
                release.wait();
            }
        });

        let needle_a = Needle {
            id: NeedleId(102),
            cookie: 0x1111.into(),
            data: vec![7u8; 8],
            data_size: 8,
            ..Needle::default()
        };
        let a1 = tokio::spawn({
            let queue = queue.clone();
            async move { queue.submit(VolumeId(1), needle_a, false).await }
        });
        // The hook parks on a blocking thread, so wait on the barrier off the
        // runtime worker.
        let parked = {
            let in_append = in_append.clone();
            tokio::task::spawn_blocking(move || in_append.wait())
        };
        parked.await.unwrap(); // volume 1's write is inside its .dat append.

        // Volume 2's worker is independent: its submit completes now.
        let needle_b = Needle {
            id: NeedleId(9),
            cookie: 0x2222.into(),
            data: vec![9u8; 8],
            data_size: 8,
            ..Needle::default()
        };
        queue.submit(VolumeId(2), needle_b, true).await.unwrap();

        // Same-volume FIFO is preserved: a second volume-1 write stays queued.
        let needle_a2 = Needle {
            id: NeedleId(2),
            cookie: 0x3333.into(),
            data: vec![8u8; 8],
            data_size: 8,
            ..Needle::default()
        };
        let mut a2 = tokio::spawn({
            let queue = queue.clone();
            async move { queue.submit(VolumeId(1), needle_a2, false).await }
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), &mut a2)
                .await
                .is_err(),
            "a same-volume write must queue behind the stalled one"
        );

        let released = {
            let release = release.clone();
            tokio::task::spawn_blocking(move || release.wait())
        };
        released.await.unwrap();
        assert!(a1.await.unwrap().is_ok());
        assert!(a2.await.unwrap().is_ok());
    }

    /// A durable write below the minimum batch rides out its window so a
    /// straggler joins the same sync instead of each paying an fsync alone —
    /// Go's batchWait. Paused time makes the hold observable: a lone write can
    /// only complete once the runtime auto-advances to the window's deadline.
    #[tokio::test(start_paused = true)]
    async fn test_batch_window_defers_flush_for_stragglers() {
        use crate::config::MinFreeSpace;
        use crate::storage::types::{DiskType, NeedleId};
        use crate::storage::volume::VolumeSpec;
        use std::time::Duration;

        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let state = make_test_state();
        {
            let mut store = state.store.write().unwrap();
            store
                .add_location(
                    dir,
                    dir,
                    10,
                    DiskType::HardDrive,
                    MinFreeSpace::Percent(1.0),
                    Vec::new(),
                )
                .unwrap();
            store
                .add_volume(VolumeId(1), DiskType::HardDrive, &VolumeSpec::default())
                .unwrap();
        }

        let queue = WriteQueue::new(
            state,
            MAX_BATCH_SIZE,
            SyncBatchOption {
                min_bytes: usize::MAX,
                min_requests: MAX_BATCH_SIZE,
                wait: Duration::from_secs(30),
            },
        );

        let needle = |id: u64| Needle {
            id: NeedleId(id),
            cookie: 0x1111.into(),
            data: vec![id as u8; 8],
            data_size: 8,
            ..Needle::default()
        };

        let start = tokio::time::Instant::now();
        queue.submit(VolumeId(1), needle(1), true).await.unwrap();
        assert!(start.elapsed() >= Duration::from_secs(30));

        // Wait for the worker to take the second write and open its batch
        // window, so the straggler provably lands inside an open batch
        // instead of arriving during the initial drain.
        let armed = queue.windows_armed_for_test();
        let second = tokio::spawn({
            let queue = queue.clone();
            async move { queue.submit(VolumeId(1), needle(2), true).await }
        });
        for _ in 0..10_000 {
            if queue.windows_armed_for_test() > armed {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(queue.windows_armed_for_test() > armed);
        let third = tokio::spawn({
            let queue = queue.clone();
            async move { queue.submit(VolumeId(1), needle(3), true).await }
        });
        let (second, third) = tokio::join!(second, third);
        second.unwrap().unwrap();
        third.unwrap().unwrap();
        // One shared window ends both writes ~30s after their batch armed;
        // back-to-back windows would keep the third out to ~90s.
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_secs(60) && elapsed < Duration::from_secs(90));
    }
}
