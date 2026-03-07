//! S3-compatible tiered storage backend for volume .dat file upload/download.
//!
//! Provides multipart upload and concurrent download with progress callbacks,
//! matching the Go SeaweedFS S3 backend behavior.

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use tokio::io::AsyncReadExt;

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

    /// Upload a local file to S3 using multipart upload with progress reporting.
    ///
    /// Returns (s3_key, file_size) on success.
    /// The progress callback receives (bytes_uploaded, percentage).
    pub async fn upload_file<F>(
        &self,
        file_path: &str,
        mut progress_fn: F,
    ) -> Result<(String, u64), String>
    where
        F: FnMut(i64, f32) + Send + Sync + 'static,
    {
        let key = uuid::Uuid::new_v4().to_string();

        let metadata = tokio::fs::metadata(file_path)
            .await
            .map_err(|e| format!("failed to stat file {}: {}", file_path, e))?;
        let file_size = metadata.len();

        // Calculate part size: start at 64MB, scale up for very large files
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

        let mut file = tokio::fs::File::open(file_path)
            .await
            .map_err(|e| format!("failed to open file {}: {}", file_path, e))?;

        let mut completed_parts = Vec::new();
        let mut offset: u64 = 0;
        let mut part_number: i32 = 1;

        loop {
            let remaining = file_size - offset;
            if remaining == 0 {
                break;
            }
            let this_part_size = std::cmp::min(part_size, remaining) as usize;
            let mut buf = vec![0u8; this_part_size];
            file.read_exact(&mut buf)
                .await
                .map_err(|e| format!("failed to read file at offset {}: {}", offset, e))?;

            let upload_part_resp = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(&key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(buf.into())
                .send()
                .await
                .map_err(|e| {
                    format!(
                        "failed to upload part {} at offset {}: {}",
                        part_number, offset, e
                    )
                })?;

            let e_tag = upload_part_resp.e_tag().unwrap_or_default().to_string();
            completed_parts.push(
                CompletedPart::builder()
                    .e_tag(e_tag)
                    .part_number(part_number)
                    .build(),
            );

            offset += this_part_size as u64;

            // Report progress
            let pct = if file_size > 0 {
                (offset as f32 * 100.0) / file_size as f32
            } else {
                100.0
            };
            progress_fn(offset as i64, pct);

            part_number += 1;
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

    /// Download a file from S3 to a local path with progress reporting.
    ///
    /// Returns the file size on success.
    pub async fn download_file<F>(
        &self,
        dest_path: &str,
        key: &str,
        mut progress_fn: F,
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

        // Download in chunks
        let part_size: u64 = 64 * 1024 * 1024;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(dest_path)
            .await
            .map_err(|e| format!("failed to open dest file {}: {}", dest_path, e))?;

        let mut offset: u64 = 0;

        loop {
            if offset >= file_size {
                break;
            }
            let end = std::cmp::min(offset + part_size - 1, file_size - 1);
            let range = format!("bytes={}-{}", offset, end);

            let get_resp = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
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

            use tokio::io::AsyncWriteExt;
            file.write_all(&bytes)
                .await
                .map_err(|e| format!("failed to write to {}: {}", dest_path, e))?;

            offset += bytes.len() as u64;

            let pct = if file_size > 0 {
                (offset as f32 * 100.0) / file_size as f32
            } else {
                100.0
            };
            progress_fn(offset as i64, pct);
        }

        use tokio::io::AsyncWriteExt;
        file.flush()
            .await
            .map_err(|e| format!("failed to flush {}: {}", dest_path, e))?;

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
}
