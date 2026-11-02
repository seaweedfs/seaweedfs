use std::collections::HashMap;

use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

/// The delimiter the Lance namespace joins identifier parts with.
const DELIMITER: &str = "$";

/// Carries a token that proves the caller holds the cluster key, which marks
/// it as one of the cluster's own components.
const CLUSTER_TOKEN_HEADER: &str = "X-Seaweedfs-Cluster-Token";

/// A cluster token is minted for each request, so it only has to outlast the
/// clock skew between this host and the gateway.
const CLUSTER_TOKEN_SECS: u64 = 300;

/// The key cluster tokens are signed with, derived from the cluster key
/// (`jwt.filer_signing.key`) the way the gateway derives it, so a token
/// captured on the wire does not authorize admin calls. The catalog vends
/// credentials that write a vector bucket's index data only to callers that
/// send one. Not Debug, so the key cannot reach a log.
#[derive(Clone)]
pub struct SigningKey {
    key: Vec<u8>,
}

impl SigningKey {
    pub fn new(key: Option<String>) -> Option<Self> {
        key.filter(|key| !key.is_empty()).map(|key| Self {
            key: seaweed_worker_core::token::derive_key(
                key.as_bytes(),
                "seaweedfs-lance-cluster-token",
            ),
        })
    }

    fn token(&self) -> Option<String> {
        seaweed_worker_core::token::admin_token(&self.key, CLUSTER_TOKEN_SECS)
    }
}

#[derive(Debug, Deserialize)]
pub struct TableDescription {
    pub location: String,
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
    #[serde(default)]
    pub managed_versioning: bool,
}

#[derive(Debug, Deserialize)]
struct ListNamespacesResponse {
    #[serde(default)]
    namespaces: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ListTablesResponse {
    #[serde(default)]
    tables: Vec<String>,
}

/// A thin client for the operations this worker needs. It deliberately does not
/// wrap the whole spec: a maintenance worker lists, describes, and commits.
pub struct NamespaceClient {
    base_url: String,
    http: reqwest::Client,
    api_key: Option<String>,
    signing_key: Option<SigningKey>,
}

/// A namespace call that has not answered by now is not going to. Without this
/// a gateway that accepts the connection and then goes quiet holds a detection
/// slot open forever, and the sweep never finishes.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_REDIRECT_HOPS: usize = 10;

impl NamespaceClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            // reqwest forwards custom headers on a follow, so only a same-origin
            // hop is allowed: anywhere else would carry x-api-key to a listener
            // the credential was never meant for.
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                if may_follow(attempt.previous(), attempt.url()) {
                    attempt.follow()
                } else {
                    attempt.stop()
                }
            }))
            .build()
            // The builder only fails on a bad TLS backend, which would break
            // every call anyway; a client with no timeouts is worse than a panic
            // at startup, so keep the default only as a last resort.
            .unwrap_or_else(|err| {
                tracing::warn!("falling back to an untimed HTTP client: {err}");
                reqwest::Client::new()
            });
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
            api_key: None,
            signing_key: None,
        }
    }

    /// The catalog accepts an S3 credential as `x-api-key`; where the worker
    /// already holds one for storage it authenticates the namespace calls too.
    pub fn with_fallback(mut self, fallback: &crate::dataset::FallbackOptions) -> Self {
        if let (Some(access), Some(secret)) = (
            fallback.get("aws_access_key_id"),
            fallback.get("aws_secret_access_key"),
        ) {
            if !protected_transport(&self.base_url) {
                tracing::warn!(
                    "catalog url {} exposes the x-api-key credential in cleartext",
                    self.base_url
                );
            }
            self.api_key = Some(format!("{access}:{secret}"));
        }
        self
    }

    pub fn with_signing_key(mut self, signing_key: Option<&SigningKey>) -> Self {
        if signing_key.is_some() && !protected_transport(&self.base_url) {
            tracing::warn!(
                "catalog url {} exposes the cluster token in cleartext",
                self.base_url
            );
        }
        self.signing_key = signing_key.cloned();
        self
    }

    fn authed(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let request = match &self.api_key {
            Some(key) => request.header("x-api-key", key),
            None => request,
        };
        match self.signing_key.as_ref().and_then(SigningKey::token) {
            Some(token) => request.header(CLUSTER_TOKEN_HEADER, token),
            None => request,
        }
    }

    /// Every table the namespace holds, as delimiter-joined identifiers.
    pub async fn list_all_tables(&self) -> Result<Vec<String>> {
        let url = format!("{}/v1/table", self.base_url);
        let response: ListTablesResponse = self
            .authed(self.http.get(&url))
            .send()
            .await
            .context("list tables")?
            .error_for_status()?
            .json()
            .await?;
        Ok(response.tables)
    }

    /// Child namespaces of `id`; the root lists table buckets.
    pub async fn list_namespaces(&self, id: &[String]) -> Result<Vec<String>> {
        let url = format!("{}/v1/namespace/{}/list", self.base_url, encode_id(id));
        let response: ListNamespacesResponse = self
            .authed(self.http.get(&url))
            .send()
            .await
            .context("list namespaces")?
            .error_for_status()?
            .json()
            .await?;
        Ok(response.namespaces)
    }

    /// Declare a table that does not exist yet; the catalog allocates the
    /// location when none is given and returns the table description.
    pub async fn declare_table(
        &self,
        id: &[String],
        location: Option<&str>,
    ) -> Result<TableDescription> {
        let url = format!("{}/v1/table/{}/declare", self.base_url, encode_id(id));
        let mut body = serde_json::json!({ "id": id, "vend_credentials": true });
        if let Some(location) = location {
            body["location"] = location.into();
        }
        let description: TableDescription = self
            .authed(self.http.post(&url))
            .json(&body)
            .send()
            .await
            .context("declare table")?
            .error_for_status()?
            .json()
            .await?;
        Ok(description)
    }

    /// Resolve a table to a location and the credentials to reach it. The
    /// credentials expire, so a long compaction re-describes rather than
    /// carrying one set for the whole job.
    pub async fn describe_table(&self, id: &[String]) -> Result<TableDescription> {
        let url = format!("{}/v1/table/{}/describe", self.base_url, encode_id(id));
        let body = serde_json::json!({ "id": id, "vend_credentials": true });
        let description: TableDescription = self
            .authed(self.http.post(&url))
            .json(&body)
            .send()
            .await
            .context("describe table")?
            .error_for_status()?
            .json()
            .await?;
        Ok(description)
    }
}

/// Without a hop bound a catalog that redirects to itself keeps the request
/// alive until the call timeout, so the custom policy keeps the limit the
/// default one had.
fn may_follow(previous: &[reqwest::Url], next: &reqwest::Url) -> bool {
    previous.len() < MAX_REDIRECT_HOPS
        && previous.last().is_some_and(|prev| same_origin(prev, next))
}

/// A redirect stays within an origin when scheme, host, and port all match, so
/// a catalog cannot bounce the api key onto a downgraded or foreign listener.
fn same_origin(from: &reqwest::Url, to: &reqwest::Url) -> bool {
    from.scheme() == to.scheme()
        && from.host_str() == to.host_str()
        && from.port_or_known_default() == to.port_or_known_default()
}

/// A catalog url protects the api key when the hop is TLS or never leaves the
/// host; anything else sends it where the network can read it.
fn protected_transport(base_url: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(base_url) else {
        return false;
    };
    if url.scheme() == "https" {
        return true;
    }
    match url.host_str() {
        Some("localhost") => true,
        Some(host) => host
            .trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback()),
        None => false,
    }
}

fn encode_id(id: &[String]) -> String {
    if id.is_empty() {
        DELIMITER.to_string()
    } else {
        id.join(DELIMITER)
    }
}

/// Splits a delimiter-joined identifier back into parts.
pub fn parse_id(encoded: &str) -> Vec<String> {
    encoded
        .split(DELIMITER)
        .filter(|part| !part.is_empty())
        .map(|part| part.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        CLUSTER_TOKEN_SECS, MAX_REDIRECT_HOPS, NamespaceClient, SigningKey, may_follow,
        protected_transport, same_origin,
    };

    /// A catalog that answers one request with a table location and hands back
    /// the request it read.
    async fn recording_catalog() -> (String, tokio::sync::oneshot::Receiver<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 8192];
            let n = conn.read(&mut request).await.unwrap();
            let _ = sender.send(String::from_utf8_lossy(&request[..n]).into_owned());
            let body = r#"{"location":"s3://b/vector/t"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = conn.write_all(response.as_bytes()).await;
        });
        (url, receiver)
    }

    fn header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
        request.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }

    #[tokio::test]
    async fn signing_key_sends_a_cluster_token() {
        let id = ["b".to_string(), "vector".to_string(), "t".to_string()];
        let (url, request) = recording_catalog().await;
        let key = SigningKey::new(Some("cluster-key".into()));
        NamespaceClient::new(url)
            .with_signing_key(key.as_ref())
            .describe_table(&id)
            .await
            .unwrap();
        let request = request.await.unwrap();
        let token = header(&request, "x-seaweedfs-cluster-token").expect("cluster token");
        let raw =
            seaweed_worker_core::token::admin_token(b"cluster-key", CLUSTER_TOKEN_SECS).unwrap();
        assert_eq!(token.split('.').count(), 3);
        assert_ne!(
            token.rsplit('.').next(),
            raw.rsplit('.').next(),
            "signed with the cluster key itself"
        );

        let (url, request) = recording_catalog().await;
        NamespaceClient::new(url).describe_table(&id).await.unwrap();
        assert_eq!(
            header(&request.await.unwrap(), "x-seaweedfs-cluster-token"),
            None
        );
    }

    #[test]
    fn an_empty_signing_key_is_no_key() {
        assert!(SigningKey::new(None).is_none());
        assert!(SigningKey::new(Some(String::new())).is_none());
    }

    #[test]
    fn may_follow_bounds_hops_within_the_origin() {
        let url = |u: &str| reqwest::Url::parse(u).unwrap();
        let first = [url("https://c.local/ns")];
        let next = url("https://c.local/other");
        assert!(may_follow(&first, &next));
        assert!(!may_follow(&[], &next));
        assert!(!may_follow(&first, &url("http://c.local/other")));
        let full: Vec<_> = (0..MAX_REDIRECT_HOPS)
            .map(|i| url(&format!("https://c.local/{i}")))
            .collect();
        assert!(!may_follow(&full, &next));
    }

    #[test]
    fn same_origin_rejects_scheme_and_port_hops() {
        for (from, to, expected) in [
            ("https://c.local/ns", "https://c.local/other", true),
            ("http://c.local", "http://c.local:80/next", true),
            ("https://c.local", "http://c.local/next", false),
            ("https://c.local:8443", "https://c.local/next", false),
            ("https://c.local", "https://other.local/next", false),
        ] {
            let from = reqwest::Url::parse(from).unwrap();
            let to = reqwest::Url::parse(to).unwrap();
            assert_eq!(same_origin(&from, &to), expected, "{from} -> {to}");
        }
    }

    #[test]
    fn protected_transport_covers_tls_and_loopback() {
        for url in [
            "https://catalog.internal:8443",
            "http://127.0.0.1:8080",
            "http://localhost",
            "http://[::1]:8080",
        ] {
            assert!(protected_transport(url), "{url} should be protected");
        }
        for url in [
            "http://catalog.internal",
            "http://127.0.0.1.evil.test",
            "not a url",
        ] {
            assert!(!protected_transport(url), "{url} should warn");
        }
    }
}
