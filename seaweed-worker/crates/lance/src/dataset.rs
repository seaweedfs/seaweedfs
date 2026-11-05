//! Opening a Lance dataset with credentials the namespace vended.
//!
//! The worker holds no storage credentials of its own: it asks the namespace to
//! describe a table with `vend_credentials`, and the `storage_options` that come
//! back are handed to lance as-is. They expire, so a job that runs longer than
//! their lifetime re-describes rather than carrying one set throughout.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use lance::dataset::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::io::{
    ObjectStore, ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor,
    WrappingObjectStore,
};

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
    pub rows: usize,
    /// The Arrow schema as JSON, which is the only description of this table
    /// anything outside the format can produce.
    pub schema: Option<String>,
}

/// Storage options an operator supplies for deployments that vend none.
pub type FallbackOptions = HashMap<String, String>;

/// Resolve a table through the namespace and open it.
pub async fn open(
    client: &NamespaceClient,
    id: &[String],
    fallback: &FallbackOptions,
) -> Result<OpenTable> {
    open_with(client, id, fallback, None).await
}

/// [`open`] with the dataset's object store wrapped, e.g. by a read cache.
pub async fn open_with(
    client: &NamespaceClient,
    id: &[String],
    fallback: &FallbackOptions,
    wrapper: Option<Arc<dyn WrappingObjectStore>>,
) -> Result<OpenTable> {
    let description = client.describe_table(id).await?;
    let dataset = open_at(&description, fallback, wrapper).await?;
    Ok(OpenTable {
        id: id.to_vec(),
        location: description.location,
        dataset,
    })
}

/// The object_store options for a table. The namespace vends object_store's own
/// option names, so they pass straight through. What it vends always wins: the
/// fallback exists because a deployment without STS vends no credentials at
/// all, and then the worker has no other way to reach the data.
pub fn table_options(
    description: &TableDescription,
    fallback: &FallbackOptions,
) -> HashMap<String, String> {
    let mut options: HashMap<String, String> = fallback.clone();
    options.extend(description.storage_options.clone());
    // A store that takes unsigned requests needs no credentials, but without
    // any lance builds the AWS default chain, and that probes instance
    // metadata on every open.
    if options
        .get("aws_skip_signature")
        .is_some_and(|v| v == "true")
    {
        options.entry("aws_access_key_id".into()).or_default();
        options.entry("aws_secret_access_key".into()).or_default();
    }
    options
}

async fn open_at(
    description: &TableDescription,
    fallback: &FallbackOptions,
    wrapper: Option<Arc<dyn WrappingObjectStore>>,
) -> Result<Dataset> {
    DatasetBuilder::from_uri(&description.location)
        .with_store_params(ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                table_options(description, fallback),
            ))),
            object_store_wrapper: wrapper,
            ..Default::default()
        })
        .load()
        .await
        .with_context(|| format!("open lance dataset at {}", description.location))
}

/// Remove every object under a table's location. A table the catalog can no
/// longer describe is already dropped, so the call stays idempotent.
pub async fn drop(
    client: &NamespaceClient,
    id: &[String],
    fallback: &FallbackOptions,
) -> Result<()> {
    let description = match client.describe_table(id).await {
        Ok(description) => description,
        Err(err) if describe_not_found(&err) => return Ok(()),
        Err(err) => return Err(err),
    };
    check_drop_location(id, &description.location)?;
    let (store, path) = ObjectStore::from_uri_and_params(
        Arc::new(ObjectStoreRegistry::default()),
        &description.location,
        &ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                table_options(&description, fallback),
            ))),
            ..Default::default()
        },
    )
    .await?;
    store.remove_dir_all(path).await?;
    Ok(())
}

/// Where the catalog puts a table it was given no location for, as the
/// gateway's tableLocation derives it.
pub fn table_location(id: &[String]) -> Option<String> {
    match id {
        [bucket, ns @ .., name] if !ns.is_empty() => {
            Some(format!("s3://{bucket}/{}/{name}", ns.join(".")))
        }
        _ => None,
    }
}

/// A drop refused before anything was removed.
#[derive(Debug)]
pub struct DropRefused(String);

impl std::fmt::Display for DropRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DropRefused {}

/// A location is caller-supplied and may name the bucket, the namespace, or
/// another table, so only the table's own colocated directory is removed.
pub fn check_drop_location(id: &[String], location: &str) -> Result<(), DropRefused> {
    let Some(expected) = table_location(id) else {
        return Err(DropRefused(format!(
            "table id {id:?} needs a bucket, a namespace and a name"
        )));
    };
    if location.trim_end_matches('/') != expected {
        return Err(DropRefused(format!(
            "refusing to drop table {}: location {location} is not its colocated path {expected}",
            id.join("/")
        )));
    }
    Ok(())
}

fn describe_not_found(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<reqwest::Error>()
            .and_then(|e| e.status())
            == Some(reqwest::StatusCode::NOT_FOUND)
    })
}

impl OpenTable {
    pub async fn stats(&self) -> Result<TableStats> {
        let versions = self.dataset.versions().await?;
        Ok(TableStats {
            fragments: self.dataset.get_fragments().len(),
            version: self.dataset.version().version,
            total_versions: versions.len(),
            rows: self.dataset.count_rows(None).await.unwrap_or(0),
            schema: schema_json(&self.dataset),
        })
    }
}

/// Renders the dataset's schema as JSON. Best effort: a schema that will not
/// serialise is not a reason to fail a maintenance sweep.
fn schema_json(dataset: &Dataset) -> Option<String> {
    let arrow: arrow_schema::Schema = dataset.schema().into();
    serde_json::to_string(
        &arrow
            .fields()
            .iter()
            .map(|f| {
                serde_json::json!({
                    "name": f.name(),
                    "type": f.data_type().to_string(),
                    "nullable": f.is_nullable(),
                })
            })
            .collect::<Vec<_>>(),
    )
    .ok()
}

#[cfg(test)]
mod tests {
    use super::{FallbackOptions, check_drop_location, table_location, table_options};
    use crate::catalog::TableDescription;

    fn id(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|part| part.to_string()).collect()
    }

    fn options(pairs: &[(&str, &str)]) -> FallbackOptions {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn table_options_keep_an_unsigned_store_off_the_aws_default_chain() {
        let described = |vended: &[(&str, &str)]| TableDescription {
            location: "s3://bkt/vector/x".into(),
            storage_options: options(vended),
            managed_versioning: false,
        };
        let unsigned = described(&[("aws_skip_signature", "true")]);

        let got = table_options(&unsigned, &FallbackOptions::new());
        assert_eq!(got.get("aws_access_key_id").map(String::as_str), Some(""));
        assert_eq!(
            got.get("aws_secret_access_key").map(String::as_str),
            Some("")
        );

        let fallback = options(&[("aws_access_key_id", "k"), ("aws_secret_access_key", "s")]);
        let got = table_options(&unsigned, &fallback);
        assert_eq!(got.get("aws_access_key_id").map(String::as_str), Some("k"));

        let signed = described(&[("aws_endpoint", "http://s3:8333")]);
        let got = table_options(&signed, &FallbackOptions::new());
        assert!(!got.contains_key("aws_access_key_id"), "{got:?}");
    }

    #[test]
    fn table_location_matches_the_gateway_layout() {
        assert_eq!(
            table_location(&id(&["bkt", "vector", "x"])).as_deref(),
            Some("s3://bkt/vector/x")
        );
        assert_eq!(
            table_location(&id(&["bkt", "a", "b", "t"])).as_deref(),
            Some("s3://bkt/a.b/t")
        );
        assert_eq!(table_location(&id(&["bkt", "t"])), None);
    }

    #[test]
    fn drop_location_accepts_the_colocated_path() {
        let table = id(&["bkt", "vector", "x"]);
        for location in ["s3://bkt/vector/x", "s3://bkt/vector/x/"] {
            assert!(check_drop_location(&table, location).is_ok(), "{location}");
        }
    }

    #[test]
    fn drop_location_refuses_anything_else() {
        let table = id(&["bkt", "vector", "x"]);
        for location in [
            // ancestors: the bucket and the namespace root
            "s3://bkt",
            "s3://bkt/",
            "s3://bkt/vector",
            "s3://bkt/vector/",
            // a sibling index's directory
            "s3://bkt/vector/prod",
            "s3://bkt/vector/x/data",
            "s3://other/vector/x",
            "",
        ] {
            assert!(check_drop_location(&table, location).is_err(), "{location}");
        }
        assert!(check_drop_location(&id(&["bkt", "x"]), "s3://bkt/x").is_err());
    }
}
