//! Snapshot discipline under Lance maintenance: queries pin a dataset version,
//! and version tags act as the lease maintenance must respect.

use anyhow::Result;
use lance::Dataset;
use lance::dataset::refs::Ref;

const QUERY_TAG_PREFIX: &str = "weed-vector-query-";

/// Lease-pin `dataset`'s current version with a tag so cleanup skips it.
/// Returns the tag name; release with [`release`] when the query finishes.
pub async fn pin(dataset: &Dataset) -> Result<String> {
    let tag = format!(
        "{QUERY_TAG_PREFIX}{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    );
    dataset
        .tags()
        .create(&tag, Ref::Version(None, Some(dataset.version().version)))
        .await
        .map_err(anyhow::Error::msg)?;
    Ok(tag)
}

/// Drop a lease tag returned by [`pin`].
pub async fn release(dataset: &Dataset, tag: &str) -> Result<()> {
    dataset.tags().delete(tag).await.map_err(anyhow::Error::msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Query, VectorTable, query, testing};
    use lance::dataset::cleanup::{CleanupPolicy, cleanup_old_versions};
    use lance::dataset::optimize::{CompactionOptions, compact_files};
    use lance_linalg::distance::DistanceType;
    use tempfile::TempDir;

    async fn pinned_hits(dataset: &Dataset, version: u64, query_vector: &[f32]) -> Vec<u64> {
        let pinned = dataset.checkout_version(version).await.unwrap();
        let resolved = VectorTable::resolve(&pinned, None, None).unwrap();
        let q = Query {
            vector: query_vector.to_vec(),
            k: 3,
            filter: None,
            postfilter: false,
            distance: DistanceType::L2,
            nprobes: None,
        };
        let mut ids: Vec<u64> = query::flat(&pinned, &resolved, &q)
            .await
            .unwrap()
            .iter()
            .map(|h| h.row_id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Compaction commits a new version and rewrites fragments; a query pinned
    /// at the old version still reads it.
    #[tokio::test]
    async fn pinned_query_survives_compaction() {
        let dir = TempDir::new().unwrap();
        let mut dataset = testing::write_dataset(&dir, &testing::vectors(4, 4), &["a", "b", "c", "d"]).await;
        testing::append_dataset(&mut dataset, &testing::vectors(4, 4), &["e", "f", "g", "h"]).await;
        let v1 = dataset.version().version;
        let before = pinned_hits(&dataset, v1, &[0.0, 0.0, 0.0, 1.0]).await;

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 64,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        assert!(dataset.version().version > v1);
        assert_eq!(pinned_hits(&dataset, v1, &[0.0, 0.0, 0.0, 1.0]).await, before);
    }

    /// A pinned version carrying a query tag is skipped by cleanup; removing
    /// the lease first lets cleanup delete it.
    #[tokio::test]
    async fn tagged_version_survives_cleanup() {
        let dir = TempDir::new().unwrap();
        let mut dataset = testing::write_dataset(&dir, &testing::vectors(4, 4), &["a", "b", "c", "d"]).await;
        let v1 = dataset.version().version;
        let before = pinned_hits(&dataset, v1, &[0.0, 0.0, 0.0, 1.0]).await;
        let lease = pin(&dataset).await.unwrap();

        testing::append_dataset(&mut dataset, &testing::vectors(4, 4), &["e", "f", "g", "h"]).await;
        let v2 = dataset.version().version;

        // Tagged old versions refuse to clean by default.
        let policy = CleanupPolicy {
            before_version: Some(v2),
            delete_unverified: true,
            error_if_tagged_old_versions: true,
            clean_referenced_branches: false,
            delete_rate_limit: None,
            ..Default::default()
        };
        cleanup_old_versions(&dataset, policy.clone()).await.unwrap_err();

        let policy = CleanupPolicy {
            error_if_tagged_old_versions: false,
            ..policy
        };
        cleanup_old_versions(&dataset, policy).await.unwrap();
        assert_eq!(pinned_hits(&dataset, v1, &[0.0, 0.0, 0.0, 1.0]).await, before);

        // Lease released: the old version is now fair game.
        release(&dataset, &lease).await.unwrap();
    }

    /// A data file deleted under a pinned query fails the request rather than
    /// returning a partial result set.
    #[tokio::test]
    async fn missing_data_file_errors_instead_of_partial() {
        let dir = TempDir::new().unwrap();
        let dataset = testing::write_dataset(&dir, &testing::vectors(4, 4), &["a", "b", "c", "d"]).await;
        let data_dir = dir.path().join("data");
        let file = std::fs::read_dir(&data_dir)
            .unwrap()
            .flatten()
            .find(|e| e.file_name().to_string_lossy().ends_with(".lance"))
            .unwrap();
        std::fs::remove_file(file.path()).unwrap();

        let resolved = VectorTable::resolve(&dataset, None, None).unwrap();
        let q = Query {
            vector: vec![0.0; 4],
            k: 2,
            filter: None,
            postfilter: false,
            distance: DistanceType::L2,
            nprobes: None,
        };
        assert!(query::flat(&dataset, &resolved, &q).await.is_err());
    }
}
