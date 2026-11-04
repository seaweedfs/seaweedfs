//! Sizing the vector index an S3 Vectors table searches through, and the
//! probes a query spends on it. Building lives in the worker crate.

use anyhow::Result;
use lance::Dataset;

pub use weed_lance_worker::vector_index::{
    INDEX_NAME, MAX_PARTITIONS, build, distance_metric, index_names, optimize, partitions,
    partitions_for,
};

pub const MIN_PROBES: usize = 8;

/// How many partitions a query searches: `min` always, and under a prefilter
/// up to `max` until it has k rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Probes {
    pub min: usize,
    pub max: usize,
}

/// Probes for an index of `partitions`: a tenth of them, at least
/// [`MIN_PROBES`], never more than exist; a filtered query may widen to four
/// times that.
pub fn probes_for(partitions: usize) -> Probes {
    let partitions = partitions.max(1);
    let min = MIN_PROBES.max(partitions.div_ceil(10)).min(partitions);
    Probes {
        min,
        max: (min * 4).min(partitions),
    }
}

/// The probes a query of `vector_column` uses when it names none: from the
/// partition count of the index that covers it, or `None` without an index.
pub async fn default_probes(dataset: &Dataset, vector_column: &str) -> Result<Option<Probes>> {
    let Some(name) = index_names(dataset, vector_column)
        .await?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    Ok(Some(probes_for(partitions(dataset, &name).await?)))
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
        assert_eq!(probes_for(1), Probes { min: 1, max: 1 });
        assert_eq!(probes_for(8), Probes { min: 8, max: 8 });
        assert_eq!(probes_for(64), Probes { min: 8, max: 32 });
        assert_eq!(probes_for(224), Probes { min: 23, max: 92 });
        assert_eq!(
            probes_for(4096),
            Probes {
                min: 410,
                max: 1640
            }
        );
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

    /// Pseudo-random rows, so partitions are not degenerate; a different seed
    /// gives vectors the table does not hold.
    fn scattered(rows: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut state: u64 = seed;
        (0..rows)
            .map(|_| {
                (0..dim)
                    .map(|_| {
                        state = state
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        ((state >> 40) as f32) / (1u64 << 24) as f32
                    })
                    .collect()
            })
            .collect()
    }

    #[tokio::test]
    async fn default_probes_follow_the_index_and_find_the_neighbours() {
        let dir = TempDir::new().unwrap();
        let rows = scattered(4000, 8, 0x9E3779B97F4A7C15);
        let tags: Vec<&str> = (0..4000)
            .map(|i| if i % 100 == 0 { "rare" } else { "common" })
            .collect();
        let mut dataset = testing::write_dataset(&dir, &rows, &tags).await;
        let table = VectorTable::resolve(&dataset, None, None).unwrap();
        assert_eq!(default_probes(&dataset, "vector").await.unwrap(), None);
        build(&mut dataset, &table.vector_column, INDEX_NAME, 64, false)
            .await
            .unwrap();
        assert_eq!(
            default_probes(&dataset, "vector").await.unwrap(),
            Some(probes_for(64))
        );

        let mut found = 0;
        let total = 20 * 10;
        for vector in &scattered(20, 8, 0xD1B54A32D192ED03) {
            let q = exact(vector, 10);
            let flat: std::collections::HashSet<u64> = query::flat(&dataset, &table, &q)
                .await
                .unwrap()
                .iter()
                .map(|h| h.row_id)
                .collect();
            let ann = query::ann(&dataset, &table, &q).await.unwrap();
            assert_eq!(ann.len(), 10);
            found += ann.iter().filter(|h| flat.contains(&h.row_id)).count();
        }
        assert!(found * 10 >= total * 9, "recall {found}/{total}");

        // Eight probes do not hold ten of the 1% rare rows; a filtered query
        // widens past the minimum until it has them.
        let filtered = query::ann(
            &dataset,
            &table,
            &Query {
                filter: Some("tag = 'rare'".to_string()),
                ..exact(&rows[0], 10)
            },
        )
        .await
        .unwrap();
        assert_eq!(filtered.len(), 10);
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
    async fn build_uses_the_tables_metric() {
        for (metric, distance) in [
            ("COSINE", DistanceType::Cosine),
            ("DOT_PRODUCT", DistanceType::Dot),
            ("L2", DistanceType::L2),
        ] {
            let dir = TempDir::new().unwrap();
            let rows = testing::vectors(300, DIM);
            let mut dataset =
                testing::write_dataset_with_metric(&dir, &rows, &["a"; 300], metric).await;
            let table = VectorTable::resolve(&dataset, None, None).unwrap();
            assert_eq!(distance_metric(&dataset, "vector"), distance);
            build(&mut dataset, &table.vector_column, INDEX_NAME, 4, false)
                .await
                .unwrap();
            for vector in testing::vectors(3, DIM) {
                let q = Query {
                    distance,
                    ..exact(&vector, 10)
                };
                let flat = query::flat(&dataset, &table, &q).await.unwrap();
                let ann = query::ann(
                    &dataset,
                    &table,
                    &Query {
                        nprobes: Some(4),
                        ..q
                    },
                )
                .await
                .unwrap();
                assert_eq!(flat.len(), ann.len(), "{metric}");
                for (f, a) in flat.iter().zip(ann.iter()) {
                    assert!(
                        (f.distance - a.distance).abs() < 1e-5,
                        "{metric}: {flat:?} vs {ann:?}"
                    );
                }
            }
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
        optimize(&mut dataset, &table.vector_column, false)
            .await
            .unwrap();
        let cov = &coverage(&dataset, "vector").await.unwrap()[0];
        assert_eq!(cov.segments, 2);
        assert!(cov.unindexed_fragments.is_empty());

        testing::append_dataset(&mut dataset, &testing::vectors(40, DIM), &["c"; 40]).await;
        optimize(&mut dataset, &table.vector_column, true)
            .await
            .unwrap();
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
