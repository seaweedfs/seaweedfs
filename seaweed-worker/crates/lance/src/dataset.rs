//! Opening a Lance dataset on the cluster's storage.
//!
//! The namespace says where a table is; its files are read and written on the
//! filers and volume servers through [`DirectReads`]. The S3 store lance builds
//! for the location is unsigned and never dialed, so no process here holds S3
//! credentials. Tests on an in-process store open without storage.

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
use crate::direct_read::DirectReads;

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

/// Where tables are kept: the cluster's filers and volume servers, or, for
/// tests on an in-process store, nowhere in particular.
#[derive(Clone, Default)]
pub struct Storage(pub Option<DirectReads>);

impl Storage {
    /// The store wrapper for a table at `location`, writing as the table's
    /// bucket declares.
    pub async fn wrapper(&self, location: &str) -> Result<Option<Arc<dyn WrappingObjectStore>>> {
        let Some(direct) = &self.0 else {
            return Ok(None);
        };
        let bucket = location
            .strip_prefix("s3://")
            .and_then(|rest| rest.split('/').next())
            .with_context(|| format!("{location} names no bucket"))?;
        let sse_s3 = direct.bucket_encrypts(bucket).await?;
        Ok(Some(Arc::new(direct.encrypting(sse_s3))))
    }
}

/// What lance builds its own S3 store with: unsigned, so it looks for no
/// credentials, and never dialed.
pub fn unsigned_options() -> HashMap<String, String> {
    HashMap::from([
        ("aws_skip_signature".to_string(), "true".to_string()),
        ("aws_region".to_string(), "us-east-1".to_string()),
        ("aws_access_key_id".to_string(), String::new()),
        ("aws_secret_access_key".to_string(), String::new()),
    ])
}

/// Resolve a table through the namespace and open it.
pub async fn open(client: &NamespaceClient, id: &[String], storage: &Storage) -> Result<OpenTable> {
    let description = client.describe_table(id).await?;
    let wrapper = storage.wrapper(&description.location).await?;
    let dataset = open_at(&description, wrapper).await?;
    Ok(OpenTable {
        id: id.to_vec(),
        location: description.location,
        dataset,
    })
}

/// Open a table the caller has already resolved, with the store wrapped.
pub async fn open_at(
    description: &TableDescription,
    wrapper: Option<Arc<dyn WrappingObjectStore>>,
) -> Result<Dataset> {
    DatasetBuilder::from_uri(&description.location)
        .with_store_params(store_params(wrapper))
        .load()
        .await
        .with_context(|| format!("open lance dataset at {}", description.location))
}

/// The store parameters every open and write uses.
pub fn store_params(wrapper: Option<Arc<dyn WrappingObjectStore>>) -> ObjectStoreParams {
    ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
            unsigned_options(),
        ))),
        object_store_wrapper: wrapper,
        ..Default::default()
    }
}

/// Remove every object under a table's location. A table the catalog can no
/// longer describe is already dropped, so the call stays idempotent.
pub async fn drop(client: &NamespaceClient, id: &[String], storage: &Storage) -> Result<()> {
    let description = match client.describe_table(id).await {
        Ok(description) => description,
        Err(err) if describe_not_found(&err) => return Ok(()),
        Err(err) => return Err(err),
    };
    check_drop_location(id, &description.location)?;
    match &storage.0 {
        Some(direct) => direct.remove_table(&description.location).await,
        None => {
            let (store, path) = ObjectStore::from_uri_and_params(
                Arc::new(ObjectStoreRegistry::default()),
                &description.location,
                &store_params(None),
            )
            .await?;
            store.remove_dir_all(path).await?;
            Ok(())
        }
    }
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
    use super::{check_drop_location, table_location};

    fn id(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|part| part.to_string()).collect()
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
