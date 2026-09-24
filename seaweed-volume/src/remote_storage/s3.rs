//! S3-compatible remote storage client.
//!
//! Works with AWS S3, MinIO, SeaweedFS S3, and all S3-compatible providers.

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::error::{DisplayErrorContext, SdkError};
use aws_sdk_s3::primitives::ByteStream;

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
        let client = Client::from_conf(
            Self::config_builder(access_key, secret_key, region, endpoint, force_path_style)
                .build(),
        );

        S3RemoteStorageClient { client, conf }
    }

    /// Build the SDK config for the given credentials and endpoint. Split out so
    /// tests can attach a canned HTTP client before building the [`Client`].
    fn config_builder(
        access_key: &str,
        secret_key: &str,
        region: &str,
        endpoint: &str,
        force_path_style: bool,
    ) -> aws_sdk_s3::config::Builder {
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

        s3_config
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

        let resp = req.send().await.map_err(|e| match e {
            // Go checks `aerr.Code() == s3.ErrCodeNoSuchKey` on GET
            // (weed/remote_storage/s3/s3_storage_client.go:436). A bare HTTP 404
            // without a NoSuchKey body is deliberately NOT treated as not-found
            // here: the Go SDK maps such a response to code "NotFound", which
            // fails Go's NoSuchKey comparison, so it stays a generic error.
            SdkError::ServiceError(ref se) if se.err().is_no_such_key() => {
                RemoteStorageError::ObjectNotFound(format!("{}/{}", loc.bucket, key))
            }
            e => RemoteStorageError::Other(format!("s3 get object: {}", DisplayErrorContext(&e))),
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
            .map_err(|e| {
                RemoteStorageError::Other(format!("s3 put object: {}", DisplayErrorContext(&e)))
            })?;

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
            .map_err(|e| match e {
                // Go checks only the raw HTTP status on HEAD
                // (weed/remote_storage/s3/s3_storage_client.go:373), because a
                // HEAD response carries no error body to name a code. The raw
                // status covers both the body-less 404 the SDK turns into
                // `NotFound` and a 404 whose body names some other code
                // (non-AWS servers do send one). A `NotFound` code on a
                // non-404 status is NOT a missing object, as in Go.
                SdkError::ServiceError(ref se) if se.raw().status().as_u16() == 404 => {
                    RemoteStorageError::ObjectNotFound(format!("{}/{}", loc.bucket, key))
                }
                e => RemoteStorageError::Other(format!(
                    "s3 head object: {}",
                    DisplayErrorContext(&e)
                )),
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
            .map_err(|e| {
                RemoteStorageError::Other(format!("s3 delete object: {}", DisplayErrorContext(&e)))
            })?;

        Ok(())
    }

    async fn list_buckets(&self) -> Result<Vec<String>, RemoteStorageError> {
        let resp = self.client.list_buckets().send().await.map_err(|e| {
            RemoteStorageError::Other(format!("s3 list buckets: {}", DisplayErrorContext(&e)))
        })?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::config::http::{HttpRequest, HttpResponse};
    use aws_sdk_s3::config::retry::RetryConfig;
    use aws_sdk_s3::config::{HttpClient, RuntimeComponents};
    use aws_sdk_s3::primitives::SdkBody;
    use aws_smithy_runtime_api::client::http::{
        HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
    };
    use aws_smithy_runtime_api::http::StatusCode;

    /// An SDK HTTP client that answers every request with one canned response,
    /// so the error-mapping paths can be exercised without a network or a
    /// running S3 server.
    #[derive(Debug, Clone)]
    struct CannedResponse {
        status: u16,
        body: &'static str,
    }

    impl HttpConnector for CannedResponse {
        fn call(&self, _request: HttpRequest) -> HttpConnectorFuture {
            let status = StatusCode::try_from(self.status).expect("valid HTTP status");
            HttpConnectorFuture::ready(Ok(HttpResponse::new(status, SdkBody::from(self.body))))
        }
    }

    impl HttpClient for CannedResponse {
        fn http_connector(
            &self,
            _settings: &HttpConnectorSettings,
            _components: &RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(self.clone())
        }
    }

    fn client_with(status: u16, body: &'static str) -> S3RemoteStorageClient {
        let config = S3RemoteStorageClient::config_builder(
            "AKIATEST",
            "secret",
            "us-east-1",
            "http://127.0.0.1:1",
            true,
        )
        .http_client(CannedResponse { status, body })
        .retry_config(RetryConfig::disabled())
        .build();
        S3RemoteStorageClient {
            client: Client::from_conf(config),
            conf: RemoteConf::default(),
        }
    }

    fn location() -> RemoteStorageLocation {
        RemoteStorageLocation {
            name: "remote".to_string(),
            bucket: "bucket".to_string(),
            path: "/dir/missing".to_string(),
            ..Default::default()
        }
    }

    const NO_SUCH_KEY: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Key>dir/missing</Key></Error>"#;

    const NOT_FOUND_BODY: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NotFound</Code><Message>Not Found</Message></Error>"#;

    const ACCESS_DENIED: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"#;

    #[tokio::test]
    async fn get_no_such_key_is_object_not_found() {
        let err = client_with(404, NO_SUCH_KEY)
            .read_file(&location(), 0, 0)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, RemoteStorageError::ObjectNotFound(path) if path == "bucket/dir/missing"),
            "expected ObjectNotFound, got {err:?}"
        );
    }

    #[tokio::test]
    async fn get_bare_404_is_not_object_not_found() {
        // Go only compares the error code against NoSuchKey on GET; a 404 with
        // no error body gets code "NotFound" in the Go SDK and stays generic.
        let err = client_with(404, "")
            .read_file(&location(), 0, 0)
            .await
            .unwrap_err();
        assert!(
            matches!(err, RemoteStorageError::Other(_)),
            "expected Other, got {err:?}"
        );
    }

    #[tokio::test]
    async fn head_404_is_object_not_found() {
        let err = client_with(404, "")
            .stat_file(&location())
            .await
            .unwrap_err();
        assert!(
            matches!(&err, RemoteStorageError::ObjectNotFound(path) if path == "bucket/dir/missing"),
            "expected ObjectNotFound, got {err:?}"
        );
    }

    #[tokio::test]
    async fn head_404_with_foreign_error_body_is_object_not_found() {
        // A 404 whose body names a code other than NotFound: the SDK does not
        // classify it, but Go's raw status check still says not-found.
        let err = client_with(404, NO_SUCH_KEY)
            .stat_file(&location())
            .await
            .unwrap_err();
        assert!(
            matches!(err, RemoteStorageError::ObjectNotFound(_)),
            "expected ObjectNotFound, got {err:?}"
        );
    }

    #[tokio::test]
    async fn head_not_found_code_on_a_non_404_status_is_not_object_not_found() {
        // The SDK classifies a `NotFound` error code whatever the status; Go
        // looks only at the status, so a 400 with such a body stays an error.
        let err = client_with(400, NOT_FOUND_BODY)
            .stat_file(&location())
            .await
            .unwrap_err();
        assert!(
            matches!(&err, RemoteStorageError::Other(msg) if msg.contains("NotFound")),
            "expected Other naming the code, got {err:?}"
        );
    }

    #[tokio::test]
    async fn get_access_denied_keeps_service_error_code() {
        let err = client_with(403, ACCESS_DENIED)
            .read_file(&location(), 0, 0)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, RemoteStorageError::Other(_)),
            "expected Other, got {err:?}"
        );
        assert!(
            msg.contains("AccessDenied"),
            "message should carry the S3 error code, got: {msg}"
        );
        assert!(
            !msg.ends_with("service error"),
            "message should not be the bare SdkError Display, got: {msg}"
        );
    }

    #[tokio::test]
    async fn head_access_denied_keeps_service_error_code() {
        let err = client_with(403, ACCESS_DENIED)
            .stat_file(&location())
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, RemoteStorageError::Other(_)),
            "expected Other, got {err:?}"
        );
        assert!(
            msg.contains("AccessDenied"),
            "message should carry the S3 error code, got: {msg}"
        );
    }
}
