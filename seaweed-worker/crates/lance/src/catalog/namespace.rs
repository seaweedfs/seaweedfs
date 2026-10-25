use std::collections::HashMap;

use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

/// The delimiter the Lance namespace joins identifier parts with.
const DELIMITER: &str = "$";

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

    fn authed(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.api_key {
            Some(key) => request.header("x-api-key", key),
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
    use super::{MAX_REDIRECT_HOPS, may_follow, protected_transport, same_origin};

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
