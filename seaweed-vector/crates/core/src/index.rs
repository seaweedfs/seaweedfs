//! Sizing the vector index an S3 Vectors table searches through, and the
//! probes a query spends on it. Building lives in the worker crate.

pub use weed_lance_worker::vector_index::{
    INDEX_NAME, MAX_PARTITIONS, build, distance_metric, index_names, optimize, partitions_for,
};

pub const MIN_PROBES: usize = 8;

/// Partitions one query probes: a tenth of them, at least [`MIN_PROBES`], never
/// more than exist.
pub fn probes_for(partitions: usize) -> usize {
    MIN_PROBES
        .max(partitions.div_ceil(10))
        .min(partitions.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coverage::coverage;
    use crate::query::{self, Query};
    use crate::table::VectorTable;
    use crate::testing::{self, DIM};
    use lance::Dataset;
    use lance::dataset::optimize::{CompactionOptions, compact_files};
    use lance_linalg::distance::DistanceType;
    use tempfile::TempDir;

    #[test]
    fn probes_are_a_tenth_with_a_floor() {
        assert_eq!(probes_for(1), 1);
        assert_eq!(probes_for(8), 8);
        assert_eq!(probes_for(64), 8);
        assert_eq!(probes_for(224), 23);
        assert_eq!(probes_for(4096), 410);
    }

    fn exact(vector: &[f32], k: usize) -> Query {
        Query {
            vector: vector.to_vec(),
            k,
            filter: None,
            postfilter: false,
            distance: DistanceType::L2,
            nprobes: None,
            index_segments: None,
            fragments: None,
        }
    }

    /// The test vectors repeat, so ties are common: compare distances, not rows.
    async fn assert_same_top_k(dataset: &Dataset, vector: &[f32], k: usize) {
        let table = VectorTable::resolve(dataset, None, None).unwrap();
        let q = exact(vector, k);
        let flat = query::flat(dataset, &table, &q).await.unwrap();
        let ann = query::ann(
            dataset,
            &table,
            &Query {
                nprobes: Some(MAX_PARTITIONS),
                ..q
            },
        )
        .await
        .unwrap();
        assert_eq!(flat.len(), ann.len());
        for (f, a) in flat.iter().zip(ann.iter()) {
            assert!(
                (f.distance - a.distance).abs() < 1e-6,
                "{flat:?} vs {ann:?}"
            );
        }
    }

    #[tokio::test]
    async fn build_covers_every_fragment() {
        let dir = TempDir::new().unwrap();
        let mut dataset =
            testing::write_dataset(&dir, &testing::vectors(200, DIM), &["a"; 200]).await;
        testing::append_dataset(&mut dataset, &testing::vectors(50, DIM), &["b"; 50]).await;
        let table = VectorTable::resolve(&dataset, None, None).unwrap();
        build(
            &mut dataset,
            &table.vector_column,
            INDEX_NAME,
            partitions_for(250),
            false,
        )
        .await
        .unwrap();
        let cov = coverage(&dataset, "vector").await.unwrap();
        assert_eq!(cov.len(), 1);
        assert_eq!(cov[0].index_name, INDEX_NAME);
        assert_eq!(cov[0].segments, 1);
        assert!(cov[0].coverage_known);
        assert!(cov[0].unindexed_fragments.is_empty());
        for vector in testing::vectors(4, DIM) {
            assert_same_top_k(&dataset, &vector, 10).await;
        }
    }

    #[tokio::test]
    async fn optimize_appends_then_retrain_folds_segments() {
        let dir = TempDir::new().unwrap();
        let mut dataset =
            testing::write_dataset(&dir, &testing::vectors(100, DIM), &["a"; 100]).await;
        let table = VectorTable::resolve(&dataset, None, None).unwrap();
        build(&mut dataset, &table.vector_column, INDEX_NAME, 4, false)
            .await
            .unwrap();
        testing::append_dataset(&mut dataset, &testing::vectors(40, DIM), &["b"; 40]).await;
        optimize(&mut dataset, false).await.unwrap();
        let cov = &coverage(&dataset, "vector").await.unwrap()[0];
        assert_eq!(cov.segments, 2);
        assert!(cov.unindexed_fragments.is_empty());

        testing::append_dataset(&mut dataset, &testing::vectors(40, DIM), &["c"; 40]).await;
        optimize(&mut dataset, true).await.unwrap();
        let cov = &coverage(&dataset, "vector").await.unwrap()[0];
        assert_eq!(cov.segments, 1);
        assert!(cov.coverage_known);
        assert!(cov.unindexed_fragments.is_empty());
        for vector in testing::vectors(4, DIM) {
            assert_same_top_k(&dataset, &vector, 10).await;
        }
    }

    #[tokio::test]
    async fn compaction_keeps_the_index_covering() {
        let dir = TempDir::new().unwrap();
        let mut dataset =
            testing::write_dataset(&dir, &testing::vectors(60, DIM), &["a"; 60]).await;
        for tag in ["b", "c", "d"] {
            testing::append_dataset(&mut dataset, &testing::vectors(60, DIM), &[tag; 60]).await;
        }
        let table = VectorTable::resolve(&dataset, None, None).unwrap();
        build(&mut dataset, &table.vector_column, INDEX_NAME, 4, false)
            .await
            .unwrap();
        dataset.delete("key = 'k0'").await.unwrap();
        assert_eq!(dataset.get_fragments().len(), 4);

        let options = CompactionOptions {
            target_rows_per_fragment: 1000,
            materialize_deletions: true,
            materialize_deletions_threshold: 0.0,
            ..Default::default()
        };
        compact_files(&mut dataset, options, None).await.unwrap();
        dataset.checkout_latest().await.unwrap();
        assert_eq!(dataset.get_fragments().len(), 1);

        let cov = &coverage(&dataset, "vector").await.unwrap()[0];
        assert!(cov.coverage_known);
        assert!(cov.unindexed_fragments.is_empty(), "{cov:?}");
        for vector in testing::vectors(4, DIM) {
            assert_same_top_k(&dataset, &vector, 10).await;
        }
    }
}
