use std::collections::HashMap;

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
}

impl NamespaceClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

    /// Every table the namespace holds, as delimiter-joined identifiers.
    pub async fn list_all_tables(&self) -> Result<Vec<String>> {
        let url = format!("{}/v1/table", self.base_url);
        let response: ListTablesResponse = self
            .http
            .get(&url)
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
            .http
            .get(&url)
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
            .http
            .post(&url)
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
