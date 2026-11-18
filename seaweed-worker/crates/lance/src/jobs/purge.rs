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

use std::collections::HashMap;

use anyhow::{Context, Result};
use chrono::Utc;
use lance::Dataset;
use lance::dataset::cleanup::{CleanupPolicy, cleanup_old_versions};
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance::index::DatasetIndexExt;
use lance_index::optimize::OptimizeOptions;
use tracing::warn;

pub const RETAIN_HOURS_KEY: &str = "seaweedfs.cleanup.retain_hours";
pub const MIN_VERSIONS_KEY: &str = "seaweedfs.cleanup.min_versions";
pub const PURGE_REQUESTED_KEY: &str = "seaweedfs.purge.requested";
pub const PURGE_COMPLETED_KEY: &str = "seaweedfs.purge.completed";

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
}

/// Rewrites every fragment that has deleted rows, rebuilds the indices from
/// the rows that remain, then removes every version but the latest, and marks
/// `token` served. The rebuild is needed because compaction only remaps an
/// index: an IVF_FLAT index keeps the deleted vector's values in its
/// partitions under a remapped row address. A version pinned by a tag (a
/// query lease, or one someone named) still holds the deleted rows, so the
/// purge fails without marking the request served and the next run retries.
pub async fn purge(dataset: &mut Dataset, token: &str) -> Result<PurgeStats> {
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
    if !dataset.load_indices().await?.is_empty() {
        dataset
            .optimize_indices(&OptimizeOptions::retrain())
            .await
            .context("rebuild indices without the deleted rows")?;
    }
    let latest = dataset.latest_version_id().await?;
    dataset.checkout_latest().await?;
    let policy = CleanupPolicy {
        before_timestamp: Some(Utc::now()),
        before_version: Some(latest),
        delete_unverified: false,
        error_if_tagged_old_versions: true,
        ..Default::default()
    };
    let cleanup = cleanup_old_versions(dataset, policy)
        .await
        .context("remove versions older than the purge")?;
    dataset
        .update_config([(PURGE_COMPLETED_KEY, token)])
        .await
        .context("record purge completion")?;
    Ok(PurgeStats {
        fragments_rewritten: compaction.fragments_removed,
        old_versions: cleanup.old_versions,
        bytes_removed: cleanup.bytes_removed,
    })
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
