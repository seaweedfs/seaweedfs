//! Opening a Lance dataset with credentials the namespace vended.
//!
//! The worker holds no storage credentials of its own: it asks the namespace to
//! describe a table with `vend_credentials`, and the `storage_options` that come
//! back are handed to lance as-is. They expire, so a job that runs longer than
//! their lifetime re-describes rather than carrying one set throughout.

use std::collections::HashMap;

use anyhow::{Context, Result};
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::Dataset;

use crate::catalog::{NamespaceClient, TableDescription};

/// A table the worker is about to work on.
pub struct OpenTable {
    pub id: Vec<String>,
    pub location: String,
    pub dataset: Dataset,
}

/// What detection needs to decide whether a table is worth a job. Reading it
/// opens the dataset but touches no data files.
pub struct TableStats {
    pub fragments: usize,
    pub version: u64,
    pub total_versions: usize,
}

/// Storage options an operator supplies for deployments that vend none.
pub type FallbackOptions = HashMap<String, String>;

/// Resolve a table through the namespace and open it.
pub async fn open(
    client: &NamespaceClient,
    id: &[String],
    fallback: &FallbackOptions,
) -> Result<OpenTable> {
    let description = client.describe_table(id).await?;
    let dataset = open_at(&description, fallback).await?;
    Ok(OpenTable {
        id: id.to_vec(),
        location: description.location,
        dataset,
    })
}

/// The namespace vends object_store's own option names, so they pass straight
/// through. What it vends always wins: the fallback exists because a deployment
/// without STS vends no credentials at all, and then the worker has no other way
/// to reach the data.
async fn open_at(description: &TableDescription, fallback: &FallbackOptions) -> Result<Dataset> {
    let mut options: HashMap<String, String> = fallback.clone();
    options.extend(description.storage_options.clone());
    DatasetBuilder::from_uri(&description.location)
        .with_storage_options(options)
        .load()
        .await
        .with_context(|| format!("open lance dataset at {}", description.location))
}

impl OpenTable {
    pub async fn stats(&self) -> Result<TableStats> {
        let versions = self.dataset.versions().await?;
        Ok(TableStats {
            fragments: self.dataset.get_fragments().len(),
            version: self.dataset.version().version,
            total_versions: versions.len(),
        })
    }
}
