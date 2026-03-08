//! S3-compatible remote storage client.
//!
//! Works with AWS S3, MinIO, SeaweedFS S3, and all S3-compatible providers.

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use super::{RemoteEntry, RemoteStorageClient, RemoteStorageError};
use crate::pb::remote_pb::{RemoteConf, RemoteStorageLocation};

/// S3-compatible remote storage client.
pub struct S3RemoteStorageClient {
    client: Client,
    conf: RemoteConf,
}

impl S3RemoteStorageClient {
    /// Create a new S3 client from credentials and endpoint configuration.
    pub fn new(
        conf: RemoteConf,
        access_key: &str,
        secret_key: &str,
        region: &str,
        endpoint: &str,
        force_path_style: bool,
    ) -> Self {
        let region = if region.is_empty() {
            "us-east-1"
        } else {
            region
        };

        let credentials = Credentials::new(
            access_key,
            secret_key,
            None, // session token
            None, // expiry
            "seaweedfs-volume",
        );

        let mut s3_config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .credentials_provider(credentials)
            .force_path_style(force_path_style);

        if !endpoint.is_empty() {
            s3_config = s3_config.endpoint_url(endpoint);
        }

        let client = Client::from_conf(s3_config.build());

        S3RemoteStorageClient { client, conf }
    }
}

#[async_trait::async_trait]
impl RemoteStorageClient for S3RemoteStorageClient {
    async fn read_file(
        &self,
        loc: &RemoteStorageLocation,
        offset: i64,
        size: i64,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let key = loc.path.trim_start_matches('/');

        let mut req = self.client.get_object().bucket(&loc.bucket).key(key);

        // Set byte range if specified
        if size > 0 {
            let end = offset + size - 1;
            req = req.range(format!("bytes={}-{}", offset, end));
        } else if offset > 0 {
            req = req.range(format!("bytes={}-", offset));
        }

        let resp = req.send().await.map_err(|e| {
            let msg = format!("{}", e);
            if msg.contains("NoSuchKey") || msg.contains("404") {
                RemoteStorageError::ObjectNotFound(format!("{}/{}", loc.bucket, key))
            } else {
                RemoteStorageError::Other(format!("s3 get object: {}", e))
            }
        })?;

        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| RemoteStorageError::Other(format!("s3 read body: {}", e)))?;

        Ok(data.into_bytes().to_vec())
    }

    async fn write_file(
        &self,
        loc: &RemoteStorageLocation,
        data: &[u8],
    ) -> Result<RemoteEntry, RemoteStorageError> {
        let key = loc.path.trim_start_matches('/');

        let resp = self
            .client
            .put_object()
            .bucket(&loc.bucket)
            .key(key)
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await
            .map_err(|e| RemoteStorageError::Other(format!("s3 put object: {}", e)))?;

        Ok(RemoteEntry {
            size: data.len() as i64,
            last_modified_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            e_tag: resp.e_tag().unwrap_or_default().to_string(),
            storage_name: loc.name.clone(),
        })
    }

    async fn stat_file(
        &self,
        loc: &RemoteStorageLocation,
    ) -> Result<RemoteEntry, RemoteStorageError> {
        let key = loc.path.trim_start_matches('/');

        let resp = self
            .client
            .head_object()
            .bucket(&loc.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                let msg = format!("{}", e);
                if msg.contains("404") || msg.contains("NotFound") {
                    RemoteStorageError::ObjectNotFound(format!("{}/{}", loc.bucket, key))
                } else {
                    RemoteStorageError::Other(format!("s3 head object: {}", e))
                }
            })?;

        Ok(RemoteEntry {
            size: resp.content_length().unwrap_or(0),
            last_modified_at: resp.last_modified().map(|t| t.secs()).unwrap_or(0),
            e_tag: resp.e_tag().unwrap_or_default().to_string(),
            storage_name: loc.name.clone(),
        })
    }

    async fn delete_file(&self, loc: &RemoteStorageLocation) -> Result<(), RemoteStorageError> {
        let key = loc.path.trim_start_matches('/');

        self.client
            .delete_object()
            .bucket(&loc.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| RemoteStorageError::Other(format!("s3 delete object: {}", e)))?;

        Ok(())
    }

    async fn list_buckets(&self) -> Result<Vec<String>, RemoteStorageError> {
        let resp = self
            .client
            .list_buckets()
            .send()
            .await
            .map_err(|e| RemoteStorageError::Other(format!("s3 list buckets: {}", e)))?;

        Ok(resp
            .buckets()
            .iter()
            .filter_map(|b| b.name().map(String::from))
            .collect())
    }

    fn remote_conf(&self) -> &RemoteConf {
        &self.conf
    }
}
