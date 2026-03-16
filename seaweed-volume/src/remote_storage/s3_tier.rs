//! S3-compatible tiered storage backend for volume .dat file upload/download.
//!
//! Provides multipart upload and concurrent download with progress callbacks,
//! matching the Go SeaweedFS S3 backend behavior.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, OnceLock, RwLock};

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Semaphore;

/// Concurrency limit for multipart upload/download (matches Go's s3manager).
const CONCURRENCY: usize = 5;

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
    pub async fn upload_file<F>(
        &self,
        file_path: &str,
        progress_fn: F,
    ) -> Result<(String, u64), String>
    where
        F: FnMut(i64, f32) + Send + Sync + 'static,
    {
        let key = uuid::Uuid::new_v4().to_string();

        let metadata = tokio::fs::metadata(file_path)
            .await
            .map_err(|e| format!("failed to stat file {}: {}", file_path, e))?;
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
            .map_err(|e| format!("failed to create multipart upload: {}", e))?;

        let upload_id = create_resp
            .upload_id()
            .ok_or_else(|| "no upload_id in multipart upload response".to_string())?
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
                    .map_err(|e| format!("semaphore error: {}", e))?;

                // Read this part's data from the file at the correct offset
                let mut file = tokio::fs::File::open(&fp)
                    .await
                    .map_err(|e| format!("failed to open file {}: {}", fp, e))?;
                file.seek(std::io::SeekFrom::Start(off))
                    .await
                    .map_err(|e| format!("failed to seek to offset {}: {}", off, e))?;
                let mut buf = vec![0u8; size];
                file.read_exact(&mut buf)
                    .await
                    .map_err(|e| format!("failed to read file at offset {}: {}", off, e))?;

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
                        format!("failed to upload part {} at offset {}: {}", pn, off, e)
                    })?;

                let e_tag = upload_part_resp.e_tag().unwrap_or_default().to_string();

                // Report progress
                {
                    let mut guard = progress.lock().unwrap();
                    guard.0 += size as u64;
                    let uploaded = guard.0;
                    let pct = if file_size > 0 {
                        (uploaded as f32 * 100.0) / file_size as f32
                    } else {
                        100.0
                    };
                    (guard.1)(uploaded as i64, pct);
                }

                Ok::<_, String>(
                    CompletedPart::builder()
                        .e_tag(e_tag)
                        .part_number(pn)
                        .build(),
                )
            }));
        }

        // Collect results, preserving part order
        let mut completed_parts = Vec::with_capacity(handles.len());
        for handle in handles {
            let part = handle
                .await
                .map_err(|e| format!("upload task panicked: {}", e))??;
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
            .map_err(|e| format!("failed to complete multipart upload: {}", e))?;

        Ok((key, file_size))
    }

    /// Download a file from S3 to a local path with concurrent range requests
    /// and progress reporting.
    ///
    /// Returns the file size on success.
    /// Uses 64MB part size and 5 concurrent downloads (matches Go s3manager).
    pub async fn download_file<F>(
        &self,
        dest_path: &str,
        key: &str,
        progress_fn: F,
    ) -> Result<u64, String>
    where
        F: FnMut(i64, f32) + Send + Sync + 'static,
    {
        // Get file size first
        let head_resp = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| format!("failed to head object {}: {}", key, e))?;

        let file_size = head_resp.content_length().unwrap_or(0) as u64;

        // Pre-allocate file to full size so concurrent WriteAt-style writes work
        {
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(dest_path)
                .await
                .map_err(|e| format!("failed to open dest file {}: {}", dest_path, e))?;
            file.set_len(file_size)
                .await
                .map_err(|e| format!("failed to set file length: {}", e))?;
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
                    .map_err(|e| format!("semaphore error: {}", e))?;

                let end = off + size - 1;
                let range = format!("bytes={}-{}", off, end);

                let get_resp = client
                    .get_object()
                    .bucket(&bucket)
                    .key(&key)
                    .range(&range)
                    .send()
                    .await
                    .map_err(|e| format!("failed to get object {} range {}: {}", key, range, e))?;

                let body = get_resp
                    .body
                    .collect()
                    .await
                    .map_err(|e| format!("failed to read body: {}", e))?;
                let bytes = body.into_bytes();

                // Write at the correct offset (like Go's WriteAt)
                let mut file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&dp)
                    .await
                    .map_err(|e| format!("failed to open dest file {}: {}", dp, e))?;
                file.seek(std::io::SeekFrom::Start(off))
                    .await
                    .map_err(|e| format!("failed to seek to offset {}: {}", off, e))?;
                file.write_all(&bytes)
                    .await
                    .map_err(|e| format!("failed to write to {}: {}", dp, e))?;

                // Report progress
                {
                    let mut guard = progress.lock().unwrap();
                    guard.0 += bytes.len() as u64;
                    let downloaded = guard.0;
                    let pct = if file_size > 0 {
                        (downloaded as f32 * 100.0) / file_size as f32
                    } else {
                        100.0
                    };
                    (guard.1)(downloaded as i64, pct);
                }

                Ok::<_, String>(())
            }));
        }

        // Wait for all download tasks
        for handle in handles {
            handle
                .await
                .map_err(|e| format!("download task panicked: {}", e))??;
        }

        Ok(file_size)
    }

    /// Delete a file from S3.
    pub async fn delete_file(&self, key: &str) -> Result<(), String> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| format!("failed to delete object {}: {}", key, e))?;
        Ok(())
    }

    pub fn delete_file_blocking(&self, key: &str) -> Result<(), String> {
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
                .map_err(|e| format!("failed to delete object {}: {}", key, e))?;
            Ok(())
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

    pub fn clear(&mut self) {
        self.backends.clear();
    }
}

static GLOBAL_S3_TIER_REGISTRY: OnceLock<RwLock<S3TierRegistry>> = OnceLock::new();

pub fn global_s3_tier_registry() -> &'static RwLock<S3TierRegistry> {
    GLOBAL_S3_TIER_REGISTRY.get_or_init(|| RwLock::new(S3TierRegistry::new()))
}

fn block_on_tier_future<F, T>(future: F) -> Result<T, String>
where
    F: Future<Output = Result<T, String>> + Send + 'static,
    T: Send + 'static,
{
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("failed to build tokio runtime: {}", e))?;
        runtime.block_on(future)
    })
    .join()
    .map_err(|_| "tier runtime thread panicked".to_string())?
}
