//! Per-table retention for version cleanup, and purges: compaction that
//! rewrites every fragment with deletions followed by a cleanup that keeps
//! only the latest version, so the bytes of deleted rows leave storage now
//! rather than after the retention window.
//!
//! Both are set in the table's own Lance config, so any client that can write
//! the table can set them and they travel with it:
//!
//! - `seaweedfs.cleanup.retain_hours` and `seaweedfs.cleanup.min_versions`
//!   override the worker's retention for this table.
//! - `seaweedfs.purge.requested` holds a request token ([`request_purge`]
//!   writes the current time); a purge is pending while it differs from
//!   `seaweedfs.purge.completed`, which the purge sets to the token it served.
//!   `seaweedfs.purge.rebuilt` records the token whose rebuild is done.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use lance::Dataset;
use lance::dataset::cleanup::{RemovalStats, cleanup_old_versions};
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::optimize::OptimizeOptions;
use lance_index::{IndexType, is_system_index};
use lance_table::format::IndexMetadata;
use tracing::warn;

use crate::jobs::cleanup::retention_policy;

pub const RETAIN_HOURS_KEY: &str = "seaweedfs.cleanup.retain_hours";
pub const MIN_VERSIONS_KEY: &str = "seaweedfs.cleanup.min_versions";
pub const PURGE_REQUESTED_KEY: &str = "seaweedfs.purge.requested";
pub const PURGE_COMPLETED_KEY: &str = "seaweedfs.purge.completed";
pub const PURGE_REBUILT_KEY: &str = "seaweedfs.purge.rebuilt";

/// The retention cleanup applies to one table: the worker's, unless the
/// table's config overrides a value with one inside the worker's range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    pub retain_hours: i64,
    pub min_versions: usize,
}

impl Retention {
    pub fn for_table(
        config: &HashMap<String, String>,
        worker: Retention,
        max_retain_hours: i64,
        max_min_versions: i64,
    ) -> Retention {
        let read = |key: &str, min: i64, max: i64| -> Option<i64> {
            let raw = config.get(key)?;
            match raw.trim().parse::<i64>() {
                Ok(value) if (min..=max).contains(&value) => Some(value),
                _ => {
                    warn!("ignoring table config {key}={raw:?}: not an integer in {min}..={max}");
                    None
                }
            }
        };
        Retention {
            retain_hours: read(RETAIN_HOURS_KEY, 0, max_retain_hours)
                .unwrap_or(worker.retain_hours),
            min_versions: read(MIN_VERSIONS_KEY, 1, max_min_versions)
                .map(|v| v as usize)
                .unwrap_or(worker.min_versions),
        }
    }
}

/// The request token of a purge the table is waiting for, if any.
pub fn pending(config: &HashMap<String, String>) -> Option<String> {
    let requested = config.get(PURGE_REQUESTED_KEY).map(|s| s.trim())?;
    if requested.is_empty() {
        return None;
    }
    match config.get(PURGE_COMPLETED_KEY) {
        Some(completed) if completed.trim() == requested => None,
        _ => Some(requested.to_string()),
    }
}

/// Records a purge request on the table and returns its token. The next
/// cleanup detection proposes the purge.
pub async fn request_purge(dataset: &mut Dataset) -> Result<String> {
    let token = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    dataset
        .update_config([(PURGE_REQUESTED_KEY, token.as_str())])
        .await
        .context("record purge request")?;
    Ok(token)
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PurgeStats {
    pub fragments_rewritten: usize,
    pub old_versions: u64,
    pub bytes_removed: u64,
    /// False while versions from before the rebuild remain.
    pub served: bool,
}

/// Rewrites every fragment that has deleted rows and rebuilds the indices from
/// the rows that remain, then removes the versions from before that through
/// the cleanup job's retention policy with only the latest version kept, and
/// marks `token` served once none of them is left. The rebuild is needed
/// because compaction only remaps an index: an IVF_FLAT index keeps the
/// deleted vector's values in its partitions under a remapped row address. It
/// runs once per request, so a purge that has to wait for its cleanup (a
/// version pinned by a tag, a query lease or one someone named, fails it)
/// retries only the cleanup. Files no version references stay while Lance
/// cannot tell them from a writer's uncommitted ones, which is up to seven
/// days for an abandoned write.
pub async fn purge(dataset: &mut Dataset, token: &str) -> Result<PurgeStats> {
    let fragments_rewritten = rebuild(dataset, token).await?;
    let cleanup = match retention_policy(dataset, 0, 1).await? {
        Some(policy) => cleanup_old_versions(dataset, policy)
            .await
            .context("remove versions older than the purge")?,
        None => RemovalStats::default(),
    };
    let served = finish(dataset, token).await?;
    Ok(PurgeStats {
        fragments_rewritten,
        old_versions: cleanup.old_versions,
        bytes_removed: cleanup.bytes_removed,
        served,
    })
}

/// Rewrites the fragments with deletions and rebuilds the indices for
/// `token`, unless that was done already, and returns how many fragments it
/// rewrote.
pub async fn rebuild(dataset: &mut Dataset, token: &str) -> Result<usize> {
    if dataset.config().get(PURGE_REBUILT_KEY).map(|s| s.trim()) == Some(token) {
        return Ok(0);
    }
    let vector_indices = vector_index_names(dataset).await?;
    let options = CompactionOptions {
        materialize_deletions: true,
        // Zero materializes every fragment with any deletion.
        materialize_deletions_threshold: 0.0,
        ..Default::default()
    };
    let compaction = compact_files(dataset, options, None)
        .await
        .context("rewrite fragments with deletions")?;
    dataset.checkout_latest().await?;
    if !vector_indices.is_empty() {
        let mut options = OptimizeOptions::retrain();
        options.index_names = Some(vector_indices);
        dataset
            .optimize_indices(&options)
            .await
            .context("rebuild indices without the deleted rows")?;
    }
    rebuild_scalar_indices(dataset).await?;
    dataset.checkout_latest().await?;
    dataset
        .update_config([(PURGE_REBUILT_KEY, token)])
        .await
        .context("record purge rebuild")?;
    Ok(compaction.fragments_removed)
}

/// Marks `token` served when the oldest version left is one the rebuild for
/// it wrote, so no version from before the rebuild remains.
pub async fn finish(dataset: &mut Dataset, token: &str) -> Result<bool> {
    let oldest = dataset
        .versions()
        .await?
        .iter()
        .map(|v| v.version)
        .min()
        .context("the table has no versions")?;
    let oldest = dataset.checkout_version(oldest).await?;
    if oldest.config().get(PURGE_REBUILT_KEY).map(|s| s.trim()) != Some(token) {
        return Ok(false);
    }
    dataset
        .update_config([(PURGE_COMPLETED_KEY, token)])
        .await
        .context("record purge completion")?;
    Ok(true)
}

/// Names of the vector indices. A retrain builds each name over the whole
/// table, so a column whose index is sharded over several names is refused:
/// its shards would overlap.
async fn vector_index_names(dataset: &Dataset) -> Result<Vec<String>> {
    let mut by_column: BTreeMap<Vec<i32>, BTreeSet<String>> = BTreeMap::new();
    for index in dataset
        .load_indices()
        .await?
        .iter()
        .filter(|i| is_vector(i))
    {
        by_column
            .entry(index.fields.clone())
            .or_default()
            .insert(index.name.clone());
    }
    let mut names = Vec::new();
    for shards in by_column.into_values() {
        if shards.len() > 1 {
            bail!(
                "the vector index is sharded over {shards:?}; a purge would rebuild each over the whole table"
            );
        }
        names.extend(shards);
    }
    Ok(names)
}

/// Builds every scalar index again from the rows that remain. Compaction only
/// remaps them: a bitmap index keeps a deleted key, and under stable row ids
/// no scalar index is rewritten at all.
async fn rebuild_scalar_indices(dataset: &mut Dataset) -> Result<()> {
    let indices = dataset.load_indices().await?;
    let mut rebuilt = BTreeSet::new();
    for index in indices.iter() {
        if is_system_index(index) || is_vector(index) || !rebuilt.insert(index.name.clone()) {
            continue;
        }
        let [field] = index.fields[..] else {
            bail!("index {} covers {} columns", index.name, index.fields.len());
        };
        let column = dataset.schema().field_path(field)?;
        let params = dataset
            .open_scalar_index(&column, &index.uuid, &NoOpMetricsCollector)
            .await?
            .derive_index_params()?;
        dataset
            .create_index(
                &[column.as_str()],
                IndexType::Scalar,
                Some(index.name.clone()),
                &params,
                true,
            )
            .await
            .with_context(|| format!("rebuild index {}", index.name))?;
    }
    Ok(())
}

fn is_vector(index: &IndexMetadata) -> bool {
    index
        .index_details
        .as_ref()
        .is_some_and(|details| details.type_url.ends_with("VectorIndexDetails"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    const WORKER: Retention = Retention {
        retain_hours: 168,
        min_versions: 5,
    };

    #[test]
    fn table_retention_overrides_within_range() {
        assert_eq!(
            Retention::for_table(&config(&[]), WORKER, 8760, 1000),
            WORKER
        );
        assert_eq!(
            Retention::for_table(
                &config(&[(RETAIN_HOURS_KEY, "0"), (MIN_VERSIONS_KEY, "1")]),
                WORKER,
                8760,
                1000
            ),
            Retention {
                retain_hours: 0,
                min_versions: 1
            }
        );
        for bad in ["-1", "8761", "x", ""] {
            assert_eq!(
                Retention::for_table(&config(&[(RETAIN_HOURS_KEY, bad)]), WORKER, 8760, 1000)
                    .retain_hours,
                168,
                "{bad:?}"
            );
        }
        assert_eq!(
            Retention::for_table(&config(&[(MIN_VERSIONS_KEY, "0")]), WORKER, 8760, 1000)
                .min_versions,
            5
        );
    }

    #[test]
    fn purge_is_pending_until_its_token_is_served() {
        assert_eq!(pending(&config(&[])), None);
        assert_eq!(pending(&config(&[(PURGE_REQUESTED_KEY, "")])), None);
        assert_eq!(
            pending(&config(&[(PURGE_REQUESTED_KEY, "t1")])),
            Some("t1".to_string())
        );
        assert_eq!(
            pending(&config(&[
                (PURGE_REQUESTED_KEY, "t2"),
                (PURGE_COMPLETED_KEY, "t1")
            ])),
            Some("t2".to_string())
        );
        assert_eq!(
            pending(&config(&[
                (PURGE_REQUESTED_KEY, "t2"),
                (PURGE_COMPLETED_KEY, "t2")
            ])),
            None
        );
    }
}
