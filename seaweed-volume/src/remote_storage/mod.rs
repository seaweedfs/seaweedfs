//! Remote storage backends for tiered storage support.
//!
//! Provides a trait-based abstraction over cloud storage providers (S3, GCS, Azure, etc.)
//! and a registry to create clients from protobuf RemoteConf messages.

pub mod s3;
pub mod s3_tier;

use crate::pb::remote_pb::{RemoteConf, RemoteStorageLocation};

/// Error type for remote storage operations.
#[derive(Debug, thiserror::Error)]
pub enum RemoteStorageError {
    #[error("remote storage type {0} not found")]
    TypeNotFound(String),
    #[error("remote object not found: {0}")]
    ObjectNotFound(String),
    #[error("remote storage error: {0}")]
    Other(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Metadata about a remote file entry.
#[derive(Debug, Clone)]
pub struct RemoteEntry {
    pub size: i64,
    pub last_modified_at: i64, // Unix seconds
    pub e_tag: String,
    pub storage_name: String,
}

/// Trait for remote storage clients. Matches Go's RemoteStorageClient interface.
#[async_trait::async_trait]
pub trait RemoteStorageClient: Send + Sync {
    /// Read (part of) a file from remote storage.
    async fn read_file(
        &self,
        loc: &RemoteStorageLocation,
        offset: i64,
        size: i64,
    ) -> Result<Vec<u8>, RemoteStorageError>;

    /// Write a file to remote storage.
    async fn write_file(
        &self,
        loc: &RemoteStorageLocation,
        data: &[u8],
    ) -> Result<RemoteEntry, RemoteStorageError>;

    /// Get metadata for a file in remote storage.
    async fn stat_file(
        &self,
        loc: &RemoteStorageLocation,
    ) -> Result<RemoteEntry, RemoteStorageError>;

    /// Delete a file from remote storage.
    async fn delete_file(
        &self,
        loc: &RemoteStorageLocation,
    ) -> Result<(), RemoteStorageError>;

    /// List all buckets.
    async fn list_buckets(&self) -> Result<Vec<String>, RemoteStorageError>;

    /// The RemoteConf used to create this client.
    fn remote_conf(&self) -> &RemoteConf;
}

/// Create a new remote storage client from a RemoteConf.
pub fn make_remote_storage_client(
    conf: &RemoteConf,
) -> Result<Box<dyn RemoteStorageClient>, RemoteStorageError> {
    match conf.r#type.as_str() {
        // All S3-compatible backends use the same client with different credentials
        "s3" | "wasabi" | "backblaze" | "aliyun" | "tencent" | "baidu"
        | "filebase" | "storj" | "contabo" => {
            let (access_key, secret_key, endpoint, region) = extract_s3_credentials(conf);
            Ok(Box::new(s3::S3RemoteStorageClient::new(
                conf.clone(),
                &access_key,
                &secret_key,
                &region,
                &endpoint,
                conf.s3_force_path_style,
            )))
        }
        other => Err(RemoteStorageError::TypeNotFound(other.to_string())),
    }
}

/// Extract S3-compatible credentials from a RemoteConf based on its type.
fn extract_s3_credentials(conf: &RemoteConf) -> (String, String, String, String) {
    match conf.r#type.as_str() {
        "s3" => (
            conf.s3_access_key.clone(),
            conf.s3_secret_key.clone(),
            conf.s3_endpoint.clone(),
            if conf.s3_region.is_empty() { "us-east-1".to_string() } else { conf.s3_region.clone() },
        ),
        "wasabi" => (
            conf.wasabi_access_key.clone(),
            conf.wasabi_secret_key.clone(),
            conf.wasabi_endpoint.clone(),
            conf.wasabi_region.clone(),
        ),
        "backblaze" => (
            conf.backblaze_key_id.clone(),
            conf.backblaze_application_key.clone(),
            conf.backblaze_endpoint.clone(),
            conf.backblaze_region.clone(),
        ),
        "aliyun" => (
            conf.aliyun_access_key.clone(),
            conf.aliyun_secret_key.clone(),
            conf.aliyun_endpoint.clone(),
            conf.aliyun_region.clone(),
        ),
        "tencent" => (
            conf.tencent_secret_id.clone(),
            conf.tencent_secret_key.clone(),
            conf.tencent_endpoint.clone(),
            String::new(),
        ),
        "baidu" => (
            conf.baidu_access_key.clone(),
            conf.baidu_secret_key.clone(),
            conf.baidu_endpoint.clone(),
            conf.baidu_region.clone(),
        ),
        "filebase" => (
            conf.filebase_access_key.clone(),
            conf.filebase_secret_key.clone(),
            conf.filebase_endpoint.clone(),
            String::new(),
        ),
        "storj" => (
            conf.storj_access_key.clone(),
            conf.storj_secret_key.clone(),
            conf.storj_endpoint.clone(),
            String::new(),
        ),
        "contabo" => (
            conf.contabo_access_key.clone(),
            conf.contabo_secret_key.clone(),
            conf.contabo_endpoint.clone(),
            conf.contabo_region.clone(),
        ),
        _ => (
            conf.s3_access_key.clone(),
            conf.s3_secret_key.clone(),
            conf.s3_endpoint.clone(),
            conf.s3_region.clone(),
        ),
    }
}
