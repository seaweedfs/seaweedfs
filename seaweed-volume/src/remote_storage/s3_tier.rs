//! S3-compatible tiered storage backend for volume .dat file upload/download.
//!
//! Provides multipart upload and concurrent download with progress callbacks,
//! matching the Go SeaweedFS S3 backend behavior.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, OnceLock, RwLock};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::error::{DisplayErrorContext, SdkError};
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Semaphore;

/// Concurrency limit for multipart upload/download (matches Go's s3manager).
const CONCURRENCY: usize = 5;

/// A tier transfer failure. The variant is what callers match on; the
/// message is the operator-facing text.
#[derive(Debug, thiserror::Error)]
pub enum TierError {
    /// The remote object does not exist.
    #[error("{0}")]
    NotFound(String),
    /// An S3 request or a local file operation failed.
    #[error("{0}")]
    Io(String),
    /// The tier I/O runtime could not be built or dropped the task.
    #[error("{0}")]
    RuntimeUnavailable(String),
    /// The progress callback asked to stop.
    #[error("{0}")]
    Aborted(String),
}

// Not-found rules as in remote_storage/s3.rs: HEAD by the raw 404 status,
// GET by the NoSuchKey code only.
fn head_object_error(key: &str, e: SdkError<HeadObjectError, HttpResponse>) -> TierError {
    let message = format!("failed to head object {}: {}", key, DisplayErrorContext(&e));
    match e {
        SdkError::ServiceError(ref se) if se.raw().status().as_u16() == 404 => {
            TierError::NotFound(message)
        }
        _ => TierError::Io(message),
    }
}

fn get_object_error(
    key: &str,
    range: &str,
    e: SdkError<GetObjectError, HttpResponse>,
) -> TierError {
    let message = format!(
        "failed to get object {} range {}: {}",
        key,
        range,
        DisplayErrorContext(&e)
    );
    match e {
        SdkError::ServiceError(ref se) if se.err().is_no_such_key() => TierError::NotFound(message),
        _ => TierError::Io(message),
    }
}

/// Configuration for an S3 tier backend.
#[derive(Debug, Clone)]
pub struct S3TierConfig {
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub bucket: String,
    pub endpoint: String,
    pub storage_class: String,
    pub force_path_style: bool,
}

/// S3 tier backend for uploading/downloading volume .dat files.
pub struct S3TierBackend {
    client: Client,
    pub bucket: String,
    pub storage_class: String,
}

impl S3TierBackend {
    /// Create a new S3 tier backend from configuration.
    pub fn new(config: &S3TierConfig) -> Self {
        let region = if config.region.is_empty() {
            "us-east-1"
        } else {
            &config.region
        };

        let credentials = Credentials::new(
            &config.access_key,
            &config.secret_key,
            None,
            None,
            "seaweedfs-volume-tier",
        );

        let mut s3_config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .credentials_provider(credentials)
            .force_path_style(config.force_path_style);

        if !config.endpoint.is_empty() {
            s3_config = s3_config.endpoint_url(&config.endpoint);
        }

        let client = Client::from_conf(s3_config.build());

        S3TierBackend {
            client,
            bucket: config.bucket.clone(),
            storage_class: if config.storage_class.is_empty() {
                "STANDARD_IA".to_string()
            } else {
                config.storage_class.clone()
            },
        }
    }

    /// Upload a local file to S3 using multipart upload with concurrent parts
    /// and progress reporting.
    ///
    /// Returns (s3_key, file_size) on success.
    /// The progress callback receives (bytes_uploaded, percentage).
    /// Uses 64MB part size and 5 concurrent uploads (matches Go s3manager).
    /// `progress_fn` returning `Err` aborts the upload, mirroring Go's
    /// `fn(progressed, percentage) error` in `s3_upload.go`, where the error
    /// surfaces out of `ReadAt` and fails the transfer. It is what lets a
    /// caller that has hung up stop the work it is no longer waiting for.
    pub async fn upload_file<F>(
        &self,
        file_path: &str,
        progress_fn: F,
    ) -> Result<(String, u64), TierError>
    where
        F: FnMut(i64, f32) -> Result<(), String> + Send + Sync + 'static,
    {
        let key = uuid::Uuid::new_v4().to_string();

        let metadata = tokio::fs::metadata(file_path)
            .await
            .map_err(|e| TierError::Io(format!("failed to stat file {}: {}", file_path, e)))?;
        let file_size = metadata.len();

        // Calculate part size: start at 64MB, scale up for very large files (matches Go)
        let mut part_size: u64 = 64 * 1024 * 1024;
        while part_size * 1000 < file_size {
            part_size *= 4;
        }

        // Initiate multipart upload
        let create_resp = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .storage_class(
                self.storage_class
                    .parse()
                    .unwrap_or(aws_sdk_s3::types::StorageClass::StandardIa),
            )
            .send()
            .await
            .map_err(|e| {
                TierError::Io(format!(
                    "failed to create multipart upload: {}",
                    DisplayErrorContext(&e)
                ))
            })?;

        let upload_id = create_resp
            .upload_id()
            .ok_or_else(|| TierError::Io("no upload_id in multipart upload response".to_string()))?
            .to_string();

        // Build list of (part_number, offset, size) for all parts
        let mut parts_plan: Vec<(i32, u64, usize)> = Vec::new();
        let mut offset: u64 = 0;
        let mut part_number: i32 = 1;
        while offset < file_size {
            let remaining = file_size - offset;
            let this_part_size = std::cmp::min(part_size, remaining) as usize;
            parts_plan.push((part_number, offset, this_part_size));
            offset += this_part_size as u64;
            part_number += 1;
        }

        // Upload parts concurrently with a semaphore limiting to CONCURRENCY
        let semaphore = Arc::new(Semaphore::new(CONCURRENCY));
        let client = &self.client;
        let bucket = &self.bucket;
        let file_path_owned = file_path.to_string();
        let progress = Arc::new(std::sync::Mutex::new((0u64, progress_fn)));

        let mut handles = Vec::with_capacity(parts_plan.len());
        for (pn, off, size) in parts_plan {
            let sem = semaphore.clone();
            let client = client.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            let upload_id = upload_id.clone();
            let fp = file_path_owned.clone();
            let progress = progress.clone();

            handles.push(tokio::spawn(async move {
                let _permit = sem
                    .acquire()
                    .await
                    .map_err(|e| TierError::Io(format!("semaphore error: {}", e)))?;

                // Read this part's data from the file at the correct offset
                let mut file = tokio::fs::File::open(&fp)
                    .await
                    .map_err(|e| TierError::Io(format!("failed to open file {}: {}", fp, e)))?;
                file.seek(std::io::SeekFrom::Start(off))
                    .await
                    .map_err(|e| {
                        TierError::Io(format!("failed to seek to offset {}: {}", off, e))
                    })?;
                let mut buf = vec![0u8; size];
                file.read_exact(&mut buf).await.map_err(|e| {
                    TierError::Io(format!("failed to read file at offset {}: {}", off, e))
                })?;

                let upload_part_resp = client
                    .upload_part()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .part_number(pn)
                    .body(buf.into())
                    .send()
                    .await
                    .map_err(|e| {
                        TierError::Io(format!(
                            "failed to upload part {} at offset {}: {}",
                            pn,
                            off,
                            DisplayErrorContext(&e)
                        ))
                    })?;

                let e_tag = upload_part_resp.e_tag().unwrap_or_default().to_string();

                // Report progress. The lock is released before the result is
                // propagated so an aborting callback cannot poison the mutex
                // for the other parts still in flight.
                let progress_result = {
                    let mut guard = progress.lock().unwrap();
                    guard.0 += size as u64;
                    let uploaded = guard.0;
                    let pct = if file_size > 0 {
                        (uploaded as f32 * 100.0) / file_size as f32
                    } else {
                        100.0
                    };
                    (guard.1)(uploaded as i64, pct)
                };
                progress_result.map_err(TierError::Aborted)?;

                Ok::<_, TierError>(
                    CompletedPart::builder()
                        .e_tag(e_tag)
                        .part_number(pn)
                        .build(),
                )
            }));
        }

        let finish = async {
            // Collect results, preserving part order
            let mut completed_parts = Vec::with_capacity(handles.len());
            for handle in handles {
                let part = handle
                    .await
                    .map_err(|e| TierError::Io(format!("upload task panicked: {}", e)))??;
                completed_parts.push(part);
            }

            // Complete multipart upload
            let completed_upload = CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build();

            self.client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(&key)
                .upload_id(&upload_id)
                .multipart_upload(completed_upload)
                .send()
                .await
                .map_err(|e| {
                    TierError::Io(format!(
                        "failed to complete multipart upload: {}",
                        DisplayErrorContext(&e)
                    ))
                })?;

            Ok::<(), TierError>(())
        }
        .await;

        if let Err(e) = finish {
            // An abandoned multipart upload does not appear in an ordinary
            // object listing but still accrues storage charges until a
            // lifecycle rule reaps it. Now that a departing caller aborts the
            // transfer this is a routine path, not a rare one.
            if let Err(abort_err) = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await
            {
                tracing::warn!(
                    "failed to abort multipart upload {} for key {}: {}",
                    upload_id,
                    key,
                    abort_err
                );
            }
            return Err(e);
        }

        Ok((key, file_size))
    }

    /// Download a file from S3 to a local path with concurrent range requests
    /// and progress reporting.
    ///
    /// Returns the file size on success.
    /// Uses 64MB part size and 5 concurrent downloads (matches Go s3manager).
    /// `progress_fn` returning `Err` aborts the download, mirroring Go's
    /// `fn(progressed, percentage) error` in `s3_download.go`.
    pub async fn download_file<F>(
        &self,
        dest_path: &str,
        key: &str,
        progress_fn: F,
    ) -> Result<u64, TierError>
    where
        F: FnMut(i64, f32) -> Result<(), String> + Send + Sync + 'static,
    {
        // Get file size first
        let head_resp = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| head_object_error(key, e))?;

        let file_size = head_resp.content_length().unwrap_or(0) as u64;

        // Pre-allocate file to full size so concurrent WriteAt-style writes work
        {
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(dest_path)
                .await
                .map_err(|e| {
                    TierError::Io(format!("failed to open dest file {}: {}", dest_path, e))
                })?;
            file.set_len(file_size)
                .await
                .map_err(|e| TierError::Io(format!("failed to set file length: {}", e)))?;
        }

        let part_size: u64 = 64 * 1024 * 1024;

        // Build list of (offset, size) for all parts
        let mut parts_plan: Vec<(u64, u64)> = Vec::new();
        let mut offset: u64 = 0;
        while offset < file_size {
            let remaining = file_size - offset;
            let this_part_size = std::cmp::min(part_size, remaining);
            parts_plan.push((offset, this_part_size));
            offset += this_part_size;
        }

        // Download parts concurrently with a semaphore limiting to CONCURRENCY
        let semaphore = Arc::new(Semaphore::new(CONCURRENCY));
        let client = &self.client;
        let bucket = &self.bucket;
        let dest_path_owned = dest_path.to_string();
        let key_owned = key.to_string();
        let progress = Arc::new(std::sync::Mutex::new((0u64, progress_fn)));

        let mut handles = Vec::with_capacity(parts_plan.len());
        for (off, size) in parts_plan {
            let sem = semaphore.clone();
            let client = client.clone();
            let bucket = bucket.clone();
            let key = key_owned.clone();
            let dp = dest_path_owned.clone();
            let progress = progress.clone();

            handles.push(tokio::spawn(async move {
                let _permit = sem
                    .acquire()
                    .await
                    .map_err(|e| TierError::Io(format!("semaphore error: {}", e)))?;

                let end = off + size - 1;
                let range = format!("bytes={}-{}", off, end);

                let get_resp = client
                    .get_object()
                    .bucket(&bucket)
                    .key(&key)
                    .range(&range)
                    .send()
                    .await
                    .map_err(|e| get_object_error(&key, &range, e))?;

                let body = get_resp
                    .body
                    .collect()
                    .await
                    .map_err(|e| TierError::Io(format!("failed to read body: {}", e)))?;
                let bytes = body.into_bytes();

                // Write at the correct offset (like Go's WriteAt)
                let mut file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&dp)
                    .await
                    .map_err(|e| {
                        TierError::Io(format!("failed to open dest file {}: {}", dp, e))
                    })?;
                file.seek(std::io::SeekFrom::Start(off))
                    .await
                    .map_err(|e| {
                        TierError::Io(format!("failed to seek to offset {}: {}", off, e))
                    })?;
                file.write_all(&bytes)
                    .await
                    .map_err(|e| TierError::Io(format!("failed to write to {}: {}", dp, e)))?;

                // Report progress. The lock is released before the result is
                // propagated so an aborting callback cannot poison the mutex
                // for the other parts still in flight.
                let progress_result = {
                    let mut guard = progress.lock().unwrap();
                    guard.0 += bytes.len() as u64;
                    let downloaded = guard.0;
                    let pct = if file_size > 0 {
                        (downloaded as f32 * 100.0) / file_size as f32
                    } else {
                        100.0
                    };
                    (guard.1)(downloaded as i64, pct)
                };
                progress_result.map_err(TierError::Aborted)?;

                Ok::<_, TierError>(())
            }));
        }

        // Wait for all download tasks
        for handle in handles {
            handle
                .await
                .map_err(|e| TierError::Io(format!("download task panicked: {}", e)))??;
        }

        // fsync the file so its content is durable before the caller trims the .vif
        // and deletes the remote object (matches Go's DownloadFile f.Sync()).
        let synced = tokio::fs::OpenOptions::new()
            .write(true)
            .open(dest_path)
            .await
            .map_err(|e| TierError::Io(format!("failed to open {} for fsync: {}", dest_path, e)))?;
        synced
            .sync_all()
            .await
            .map_err(|e| TierError::Io(format!("failed to fsync {}: {}", dest_path, e)))?;

        Ok(file_size)
    }

    pub async fn read_range(
        &self,
        key: &str,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>, TierError> {
        let end = offset + (size as u64).saturating_sub(1);
        let range = format!("bytes={}-{}", offset, end);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(&range)
            .send()
            .await
            .map_err(|e| get_object_error(key, &range, e))?;

        let body = resp
            .body
            .collect()
            .await
            .map_err(|e| TierError::Io(format!("failed to read object {} body: {}", key, e)))?;
        Ok(body.into_bytes().to_vec())
    }

    /// Delete a file from S3.
    pub async fn delete_file(&self, key: &str) -> Result<(), TierError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                TierError::Io(format!(
                    "failed to delete object {}: {}",
                    key,
                    DisplayErrorContext(&e)
                ))
            })?;
        Ok(())
    }

    pub fn delete_file_blocking(&self, key: &str) -> Result<(), TierError> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = key.to_string();
        block_on_tier_future(async move {
            client
                .delete_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| {
                    TierError::Io(format!(
                        "failed to delete object {}: {}",
                        key,
                        DisplayErrorContext(&e)
                    ))
                })?;
            Ok(())
        })
    }

    pub fn read_range_blocking(
        &self,
        key: &str,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>, TierError> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = key.to_string();
        block_on_tier_future(async move {
            let end = offset + (size as u64).saturating_sub(1);
            let range = format!("bytes={}-{}", offset, end);
            let resp = client
                .get_object()
                .bucket(&bucket)
                .key(&key)
                .range(&range)
                .send()
                .await
                .map_err(|e| get_object_error(&key, &range, e))?;

            let body =
                resp.body.collect().await.map_err(|e| {
                    TierError::Io(format!("failed to read object {} body: {}", key, e))
                })?;
            Ok(body.into_bytes().to_vec())
        })
    }
}

/// Parse a backend name like "s3" or "s3.default" into (backend_type, backend_id).
/// Matches Go's `BackendNameToTypeId`.
pub fn backend_name_to_type_id(backend_name: &str) -> (String, String) {
    let parts: Vec<&str> = backend_name.split('.').collect();
    match parts.len() {
        1 => (backend_name.to_string(), "default".to_string()),
        2 => (parts[0].to_string(), parts[1].to_string()),
        _ => (String::new(), String::new()),
    }
}

/// A registry of configured S3 tier backends, keyed by backend name (e.g., "s3.default").
#[derive(Default)]
pub struct S3TierRegistry {
    backends: HashMap<String, Arc<S3TierBackend>>,
}

impl S3TierRegistry {
    pub fn new() -> Self {
        Self {
            backends: HashMap::new(),
        }
    }

    /// Register a backend with the given name.
    pub fn register(&mut self, name: String, backend: S3TierBackend) {
        self.backends.insert(name, Arc::new(backend));
    }

    /// Look up a backend by name.
    pub fn get(&self, name: &str) -> Option<Arc<S3TierBackend>> {
        self.backends.get(name).cloned()
    }

    /// List all registered backend names.
    pub fn names(&self) -> Vec<String> {
        self.backends.keys().cloned().collect()
    }

    /// Remove a backend by name.
    pub fn remove(&mut self, name: &str) {
        self.backends.remove(name);
    }

    pub fn clear(&mut self) {
        self.backends.clear();
    }
}

static GLOBAL_S3_TIER_REGISTRY: OnceLock<RwLock<S3TierRegistry>> = OnceLock::new();

pub fn global_s3_tier_registry() -> &'static RwLock<S3TierRegistry> {
    GLOBAL_S3_TIER_REGISTRY.get_or_init(|| RwLock::new(S3TierRegistry::new()))
}

/// The one process-wide runtime for tiered-S3 I/O issued from synchronous
/// storage code. A per-call runtime tore down the SDK's pooled connections
/// after every 64 KiB chunk, re-dialing TLS per read; a long-lived runtime
/// keeps the pool warm.
///
/// Built on first use. A build failure is returned, not cached or panicked:
/// callers sit inside `Volume::destroy` and needle reads, whose own error
/// paths must run, and a later call may succeed.
static TIER_RUNTIME: std::sync::Mutex<Option<tokio::runtime::Runtime>> =
    std::sync::Mutex::new(None);

fn tier_handle() -> Result<tokio::runtime::Handle, TierError> {
    let mut slot = TIER_RUNTIME
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if slot.is_none() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("tier-io")
            .enable_all()
            .build()
            .map_err(|e| {
                TierError::RuntimeUnavailable(format!(
                    "failed to build the tier I/O tokio runtime: {}",
                    e
                ))
            })?;
        *slot = Some(runtime);
    }
    Ok(slot.as_ref().expect("just initialised").handle().clone())
}

/// Run `future` on the tier runtime and block the calling thread until it
/// finishes. The caller may be a worker of *another* tokio runtime, so this
/// waits on a channel rather than `Handle::block_on`, which panics when
/// called from inside any runtime context.
fn block_on_tier_future<F, T>(future: F) -> Result<T, TierError>
where
    F: Future<Output = Result<T, TierError>> + Send + 'static,
    T: Send + 'static,
{
    let handle = tier_handle()?;
    let task = handle.spawn(future);
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    handle.spawn(async move {
        // The receiver only goes away if the caller was unwound; nothing to
        // report then.
        let _ = tx.send(task.await);
    });
    match rx.recv() {
        Ok(Ok(result)) => result,
        Ok(Err(join_error)) => Err(describe_join_error(join_error)),
        Err(_) => Err(TierError::RuntimeUnavailable(
            "tier I/O runtime dropped the task before it finished".to_string(),
        )),
    }
}

/// Turn a `JoinError` into a message that keeps the panic payload, so an
/// SDK panic surfaces as "boom" rather than a fixed "thread panicked".
fn describe_join_error(join_error: tokio::task::JoinError) -> TierError {
    if join_error.is_panic() {
        let payload = join_error.into_panic();
        let message = if let Some(s) = payload.downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.clone()
        } else {
            "non-string panic payload".to_string()
        };
        TierError::Io(format!("tier I/O task panicked: {}", message))
    } else {
        TierError::RuntimeUnavailable(format!("tier I/O task failed: {}", join_error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote_storage::s3::tests::{CannedResponse, NO_SUCH_KEY};
    use std::collections::HashSet;
    use tokio::runtime::Handle;

    fn probe() -> Result<(tokio::runtime::Id, Option<String>), TierError> {
        block_on_tier_future(async {
            Ok((
                Handle::current().id(),
                std::thread::current().name().map(str::to_string),
            ))
        })
    }

    #[test]
    fn block_on_tier_future_reuses_one_runtime() {
        let (first_runtime, first_thread) = probe().expect("first call");
        let (second_runtime, second_thread) = probe().expect("second call");
        assert_eq!(
            first_runtime, second_runtime,
            "each call must run on the same long-lived tier runtime"
        );
        assert_eq!(first_thread.as_deref(), Some("tier-io"));
        assert_eq!(second_thread.as_deref(), Some("tier-io"));

        let mut runtimes = HashSet::new();
        for _ in 0..20 {
            let (id, _) = probe().expect("probe");
            runtimes.insert(id);
        }
        assert_eq!(runtimes.len(), 1);
    }

    #[test]
    fn block_on_tier_future_returns_the_value_and_the_error() {
        assert_eq!(block_on_tier_future(async { Ok(7u32) }).unwrap(), 7);
        let err = block_on_tier_future::<_, u32>(async { Err(TierError::NotFound("nope".into())) })
            .unwrap_err();
        assert!(
            matches!(&err, TierError::NotFound(m) if m == "nope"),
            "{err:?}"
        );
    }

    #[test]
    fn block_on_tier_future_works_from_a_std_thread() {
        let (id, _) = std::thread::spawn(probe)
            .join()
            .expect("probe thread")
            .expect("probe");
        assert_eq!(id, tier_handle().expect("tier runtime").id());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn block_on_tier_future_works_from_spawn_blocking() {
        let (id, _) = tokio::task::spawn_blocking(probe)
            .await
            .expect("spawn_blocking")
            .expect("probe");
        assert_eq!(id, tier_handle().expect("tier runtime").id());
        assert_ne!(id, Handle::current().id());
    }

    // Called straight from another runtime's async context: the case that
    // would panic with `Handle::block_on` ("Cannot start a runtime from
    // within a runtime").
    #[tokio::test]
    async fn block_on_tier_future_works_from_a_current_thread_runtime() {
        let (id, _) = probe().expect("probe");
        assert_eq!(id, tier_handle().expect("tier runtime").id());
        assert_ne!(id, Handle::current().id());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn block_on_tier_future_works_from_a_multi_thread_runtime_worker() {
        let (id, _) = probe().expect("probe");
        assert_eq!(id, tier_handle().expect("tier runtime").id());
        assert_ne!(id, Handle::current().id());
    }

    #[test]
    fn block_on_tier_future_reports_the_panic_payload() {
        let err = block_on_tier_future::<_, ()>(async {
            if std::hint::black_box(true) {
                panic!("boom {}", 42);
            }
            Ok(())
        })
        .expect_err("a panicking future must be an error");
        assert!(matches!(err, TierError::Io(_)), "got: {err:?}");
        assert!(err.to_string().contains("boom 42"), "got: {err}");
        assert!(err.to_string().contains("panicked"), "got: {err}");
    }

    #[test]
    fn block_on_tier_future_reports_a_str_panic_payload() {
        let err = block_on_tier_future::<_, ()>(async {
            if std::hint::black_box(true) {
                panic!("static boom");
            }
            Ok(())
        })
        .expect_err("a panicking future must be an error");
        assert!(err.to_string().contains("static boom"), "got: {err}");
    }

    fn backend_answering(status: u16, body: &'static str) -> S3TierBackend {
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("AKIATEST", "secret", None, None, "test"))
            .endpoint_url("http://127.0.0.1:1")
            .force_path_style(true)
            .http_client(CannedResponse { status, body })
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
            .build();
        S3TierBackend {
            client: Client::from_conf(config),
            bucket: "bucket".to_string(),
            storage_class: "STANDARD".to_string(),
        }
    }

    #[tokio::test]
    async fn download_head_404_is_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("1.dat");
        let err = backend_answering(404, "")
            .download_file(dest.to_str().unwrap(), "missing", |_, _| Ok(()))
            .await
            .unwrap_err();
        assert!(matches!(err, TierError::NotFound(_)), "{err:?}");
        assert!(
            err.to_string()
                .starts_with("failed to head object missing: "),
            "{err}"
        );
    }

    #[tokio::test]
    async fn download_head_403_is_io() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("1.dat");
        let err = backend_answering(403, "")
            .download_file(dest.to_str().unwrap(), "denied", |_, _| Ok(()))
            .await
            .unwrap_err();
        assert!(matches!(err, TierError::Io(_)), "{err:?}");
    }

    #[tokio::test]
    async fn read_range_no_such_key_is_not_found() {
        let err = backend_answering(404, NO_SUCH_KEY)
            .read_range("missing", 0, 8)
            .await
            .unwrap_err();
        assert!(matches!(err, TierError::NotFound(_)), "{err:?}");
        assert!(
            err.to_string()
                .starts_with("failed to get object missing range bytes=0-7: "),
            "{err}"
        );
    }

    #[tokio::test]
    async fn read_range_bare_404_is_io() {
        // As in Go, GET is not-found by the NoSuchKey code, not the status.
        let err = backend_answering(404, "")
            .read_range("missing", 0, 8)
            .await
            .unwrap_err();
        assert!(matches!(err, TierError::Io(_)), "{err:?}");
    }

    #[test]
    fn read_range_blocking_no_such_key_is_not_found() {
        let err = backend_answering(404, NO_SUCH_KEY)
            .read_range_blocking("missing", 0, 8)
            .unwrap_err();
        assert!(matches!(err, TierError::NotFound(_)), "{err:?}");
    }

    #[test]
    fn backend_name_to_type_id_splits_on_dot() {
        assert_eq!(
            backend_name_to_type_id("s3"),
            ("s3".to_string(), "default".to_string())
        );
        assert_eq!(
            backend_name_to_type_id("s3.eu"),
            ("s3".to_string(), "eu".to_string())
        );
        assert_eq!(
            backend_name_to_type_id("s3.a.b"),
            (String::new(), String::new())
        );
    }
}
