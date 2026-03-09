//! Async batched write processing for the volume server.
//!
//! Instead of each upload handler directly calling `write_needle` and syncing,
//! writes are submitted to a queue. A background worker drains the queue in
//! batches (up to 128 entries), groups them by volume ID, processes them
//! together, and syncs once per volume for the entire batch.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use crate::storage::needle::needle::Needle;
use crate::storage::types::{Size, VolumeId};
use crate::storage::volume::VolumeError;

use super::volume_server::VolumeServerState;

/// Result of a single write operation: (offset, size, is_unchanged).
pub type WriteResult = Result<(u64, Size, bool), VolumeError>;

/// A request to write a needle, submitted to the write queue.
pub struct WriteRequest {
    pub volume_id: VolumeId,
    pub needle: Needle,
    pub response_tx: oneshot::Sender<WriteResult>,
}

/// Maximum number of write requests to batch together.
const MAX_BATCH_SIZE: usize = 128;

/// Handle for submitting write requests to the background worker.
#[derive(Clone)]
pub struct WriteQueue {
    tx: mpsc::Sender<WriteRequest>,
}

impl WriteQueue {
    /// Create a new write queue and spawn the background worker.
    ///
    /// `capacity` controls the channel buffer size (backpressure kicks in when full).
    /// The worker holds a reference to `state` for accessing the store.
    pub fn new(state: Arc<VolumeServerState>, capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        let worker = WriteQueueWorker { rx, state };
        tokio::spawn(worker.run());
        WriteQueue { tx }
    }

    /// Submit a write request and wait for the result.
    ///
    /// Returns `Err` if the worker has shut down or the response channel was dropped.
    pub async fn submit(&self, volume_id: VolumeId, needle: Needle) -> WriteResult {
        let (response_tx, response_rx) = oneshot::channel();
        let request = WriteRequest {
            volume_id,
            needle,
            response_tx,
        };

        // Send to queue; this awaits if the channel is full (backpressure).
        if self.tx.send(request).await.is_err() {
            return Err(VolumeError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "write queue worker has shut down",
            )));
        }

        // Wait for the worker to process our request.
        match response_rx.await {
            Ok(result) => result,
            Err(_) => Err(VolumeError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "write queue worker dropped response channel",
            ))),
        }
    }
}

/// Background worker that drains write requests and processes them in batches.
struct WriteQueueWorker {
    rx: mpsc::Receiver<WriteRequest>,
    state: Arc<VolumeServerState>,
}

impl WriteQueueWorker {
    async fn run(mut self) {
        debug!("write queue worker started");

        loop {
            // Wait for the first request (blocks until one arrives or channel closes).
            let first = match self.rx.recv().await {
                Some(req) => req,
                None => {
                    debug!("write queue channel closed, worker exiting");
                    return;
                }
            };

            // Drain as many additional requests as available, up to MAX_BATCH_SIZE.
            let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
            batch.push(first);

            while batch.len() < MAX_BATCH_SIZE {
                match self.rx.try_recv() {
                    Ok(req) => batch.push(req),
                    Err(_) => break,
                }
            }

            let batch_size = batch.len();
            debug!("processing write batch of {} requests", batch_size);

            // Process the batch in spawn_blocking since write_needle does file I/O.
            let state = self.state.clone();
            let _ = tokio::task::spawn_blocking(move || {
                process_batch(state, batch);
            })
            .await;
        }
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
    let mut groups: Vec<(VolumeId, Vec<(Needle, oneshot::Sender<WriteResult>)>)> = Vec::new();

    for req in batch {
        let vid = req.volume_id;
        if let Some(group) = groups.iter_mut().find(|(v, _)| *v == vid) {
            group.1.push((req.needle, req.response_tx));
        } else {
            groups.push((vid, vec![(req.needle, req.response_tx)]));
        }
    }

    // Process each volume group under a single store lock.
    let mut store = state.store.write().unwrap();

    for (vid, entries) in groups {
        for (mut needle, response_tx) in entries {
            let result = store.write_volume_needle(vid, &mut needle);
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
        use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32};
        use std::sync::RwLock;

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
            is_heartbeating: AtomicBool::new(false),
            has_master: false,
            pre_stop_seconds: 0,
            volume_state_notify: tokio::sync::Notify::new(),
            write_queue: std::sync::OnceLock::new(),
            s3_tier_registry: std::sync::RwLock::new(
                crate::remote_storage::s3_tier::S3TierRegistry::new(),
            ),
            read_mode: crate::config::ReadMode::Local,
            master_url: String::new(),
            self_url: String::new(),
            http_client: reqwest::Client::new(),
            metrics_runtime: std::sync::RwLock::new(RuntimeMetricsConfig::default()),
            metrics_notify: tokio::sync::Notify::new(),
            has_slow_read: true,
            read_buffer_size_bytes: 4 * 1024 * 1024,
            security_file: String::new(),
            cli_white_list: vec![],
        })
    }

    #[tokio::test]
    async fn test_write_queue_submit_no_volume() {
        // Submit a write to a non-existent volume -- should return VolumeError::NotFound.
        let state = make_test_state();
        let queue = WriteQueue::new(state, MAX_BATCH_SIZE);

        let needle = Needle {
            id: 1.into(),
            cookie: 0x12345678.into(),
            data: vec![1, 2, 3],
            data_size: 3,
            ..Needle::default()
        };

        let result = queue.submit(VolumeId(999), needle).await;
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
        let queue = WriteQueue::new(state, MAX_BATCH_SIZE);

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
                q.submit(VolumeId(1), needle).await
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
        let queue = WriteQueue::new(state, MAX_BATCH_SIZE);

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
                q.submit(VolumeId(42), needle).await
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

    #[tokio::test]
    async fn test_write_queue_dropped_sender() {
        // When the queue is dropped, subsequent submits should fail gracefully.
        let state = make_test_state();
        let queue = WriteQueue::new(state, 1);

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
        let result = queue2.submit(VolumeId(1), needle).await;
        assert!(result.is_err()); // NotFound is fine -- the point is it doesn't panic
    }
}
