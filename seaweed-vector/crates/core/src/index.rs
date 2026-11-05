//! Sizing the vector index an S3 Vectors table searches through, and the
//! probes a query spends on it. Building lives in the worker crate.

use anyhow::Result;
use lance::Dataset;
use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};

pub use weed_lance_worker::vector_index::{
    INDEX_NAME, MAX_PARTITIONS, build, distance_metric, index_names, optimize, partitions_for,
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

/// Partitions of one index segment, read from the opened index. Lance's index
/// statistics cost several milliseconds and may rewrite an old manifest.
pub async fn segment_partitions(
    dataset: &Dataset,
    vector_column: &str,
    segment: &uuid::Uuid,
) -> Result<usize> {
    let opened = dataset
        .open_vector_index(
            vector_column,
            segment,
            &lance_index::metrics::NoOpMetricsCollector,
        )
        .await?;
    Ok(opened.total_partitions())
}

/// The probes a query of `vector_column` uses when it names none, or `None`
/// without an index. The count comes from the most partitions any index on
/// the column has, whichever of them the scan ends up searching; the scan
/// then finds the index it was read from already open.
pub async fn default_probes(dataset: &Dataset, vector_column: &str) -> Result<Option<Probes>> {
    let Some(field) = dataset.schema().field(vector_column) else {
        return Ok(None);
    };
    let mut partitions = None;
    for index in dataset.load_indices().await?.iter() {
        if index.fields != [field.id] {
            continue;
        }
        let count = segment_partitions(dataset, vector_column, &index.uuid).await?;
        partitions = Some(count.max(partitions.unwrap_or(0)));
    }
    Ok(partitions.map(probes_for))
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
    async fn default_probes_take_the_largest_index() {
        use lance::index::DatasetIndexExt;
        use lance::index::vector::VectorIndexParams;
        use lance_index::IndexType;
        let dir = TempDir::new().unwrap();
        let mut dataset =
            testing::write_dataset(&dir, &scattered(1000, 8, 0x9E3779B97F4A7C15), &["a"; 1000])
                .await;
        testing::append_dataset(
            &mut dataset,
            &scattered(1000, 8, 0xD1B54A32D192ED03),
            &["b"; 1000],
        )
        .await;
        for (name, fragment, partitions) in [("a-small", 0u32, 2), ("z-large", 1u32, 100)] {
            dataset
                .create_index_builder(
                    &["vector"],
                    IndexType::Vector,
                    &VectorIndexParams::ivf_flat(partitions, DistanceType::L2),
                )
                .name(name.to_string())
                .fragments(vec![fragment])
                .await
                .unwrap();
            dataset.checkout_latest().await.unwrap();
        }
        assert_eq!(
            default_probes(&dataset, "vector").await.unwrap(),
            Some(probes_for(100))
        );
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

    /// An index built as cosine on a table without metric metadata keeps
    /// cosine through a retrain, and gets partitions for the new row count.
    #[tokio::test]
    async fn retrain_keeps_the_indexs_own_metric() {
        use lance::index::DatasetIndexExt;
        use lance::index::vector::VectorIndexParams;
        use lance_index::IndexType;
        use weed_lance_worker::vector_index::index_settings;
        let dir = TempDir::new().unwrap();
        let mut dataset =
            testing::write_dataset(&dir, &testing::vectors(100, DIM), &["a"; 100]).await;
        let params = VectorIndexParams::ivf_flat(2, DistanceType::Cosine);
        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("idx".to_string()),
                &params,
                false,
            )
            .await
            .unwrap();
        dataset.checkout_latest().await.unwrap();
        testing::append_dataset(&mut dataset, &testing::vectors(800, DIM), &["b"; 800]).await;
        optimize(&mut dataset, "vector", true).await.unwrap();
        let settings = index_settings(&dataset, "idx").await.unwrap();
        assert_eq!(settings.metric, Some(DistanceType::Cosine));
        assert_eq!(settings.index_type, "IVF_FLAT");
        assert_eq!(settings.partitions, partitions_for(900));
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
        let mut appended = testing::vectors(40, DIM);
        appended.push(vec![9.0; DIM]);
        testing::append_dataset(&mut dataset, &appended, &["b"; 41]).await;
        optimize(&mut dataset, &table.vector_column, false)
            .await
            .unwrap();
        let cov = &coverage(&dataset, "vector").await.unwrap()[0];
        assert_eq!(cov.segments, 2);
        assert!(cov.unindexed_fragments.is_empty());
        let hits = query::ann(
            &dataset,
            &table,
            &Query {
                nprobes: Some(4),
                ..exact(&[9.0; DIM], 1)
            },
        )
        .await
        .unwrap();
        assert_eq!(hits[0].key, "k140", "{hits:?}");

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

    /// Append and optimize nine times: the deltas climb to the limit and then
    /// fold into one, and nothing is ever uncovered after an optimize.
    #[tokio::test]
    async fn deltas_stay_under_the_limit_across_cycles() {
        use weed_lance_worker::vector_index::{MAX_DELTAS, coverage as covered};
        let dir = TempDir::new().unwrap();
        let mut dataset =
            testing::write_dataset(&dir, &testing::vectors(100, DIM), &["a"; 100]).await;
        let table = VectorTable::resolve(&dataset, None, None).unwrap();
        build(&mut dataset, &table.vector_column, INDEX_NAME, 4, false)
            .await
            .unwrap();
        let mut seen = Vec::new();
        for round in 0..9 {
            testing::append_dataset(&mut dataset, &testing::vectors(20, DIM), &["b"; 20]).await;
            optimize(&mut dataset, &table.vector_column, false)
                .await
                .unwrap();
            let cover = covered(&dataset, "vector").await.unwrap().unwrap();
            assert_eq!(cover.unindexed_rows, 0, "round {round}");
            seen.push(cover.deltas[INDEX_NAME]);
        }
        assert_eq!(seen, vec![2, 3, 4, 5, 6, 7, 8, 1, 2]);
        assert!(seen.iter().all(|&d| d <= MAX_DELTAS));
    }

    /// Two names covering disjoint fragments, as a sharded build leaves them:
    /// coverage is their union, and an optimize extends one name only.
    #[tokio::test]
    async fn shards_stay_disjoint_through_an_optimize() {
        use lance::index::DatasetIndexExt;
        use lance::index::vector::VectorIndexParams;
        use lance_index::IndexType;
        use weed_lance_worker::vector_index::coverage as covered;
        let dir = TempDir::new().unwrap();
        let mut dataset =
            testing::write_dataset(&dir, &testing::vectors(60, DIM), &["a"; 60]).await;
        testing::append_dataset(&mut dataset, &testing::vectors(60, DIM), &["b"; 60]).await;
        let params = VectorIndexParams::ivf_flat(2, DistanceType::L2);
        for (name, fragment) in [("shard-0", 0u32), ("shard-1", 1u32)] {
            dataset
                .create_index_builder(&["vector"], IndexType::Vector, &params)
                .name(name.to_string())
                .fragments(vec![fragment])
                .await
                .unwrap();
            dataset.checkout_latest().await.unwrap();
        }
        let cover = covered(&dataset, "vector").await.unwrap().unwrap();
        assert_eq!((cover.indexed_rows, cover.unindexed_rows), (120, 0));
        assert_eq!(cover.deltas.len(), 2);

        testing::append_dataset(&mut dataset, &testing::vectors(30, DIM), &["c"; 30]).await;
        let table = VectorTable::resolve(&dataset, None, None).unwrap();
        optimize(&mut dataset, &table.vector_column, false)
            .await
            .unwrap();
        let cover = covered(&dataset, "vector").await.unwrap().unwrap();
        assert_eq!(cover.unindexed_rows, 0);
        assert_eq!(
            cover.deltas.values().sum::<usize>(),
            3,
            "{:?}",
            cover.deltas
        );
        let per_name = coverage(&dataset, "vector").await.unwrap();
        let untouched = per_name
            .iter()
            .filter(|c| c.segments == 1 && c.indexed_fragments.len() == 1)
            .count();
        assert_eq!(untouched, 1, "{per_name:?}");
        for vector in testing::vectors(3, DIM) {
            assert_same_top_k(&dataset, &vector, 10).await;
        }
    }

    /// A retrain is due for one IVF_FLAT name the table outgrew, never for
    /// shards sized each for their own fragments or for another index type,
    /// and an explicit retrain of shards leaves them as they are.
    #[tokio::test]
    async fn retrain_is_due_for_one_ivf_flat_name_only() {
        use lance::index::DatasetIndexExt;
        use lance::index::vector::VectorIndexParams;
        use lance_index::IndexType;
        use weed_lance_worker::vector_index::{coverage as covered, retrain_due};
        let grown = |dir: &TempDir| {
            let path = dir.path().to_str().unwrap().to_string();
            async move {
                let mut dataset =
                    testing::write_dataset_at(&path, &testing::vectors(300, DIM), &["a"; 300])
                        .await;
                testing::append_dataset(&mut dataset, &testing::vectors(300, DIM), &["b"; 300])
                    .await;
                dataset
            }
        };

        let dir = TempDir::new().unwrap();
        let mut dataset = grown(&dir).await;
        build(&mut dataset, "vector", INDEX_NAME, 2, false)
            .await
            .unwrap();
        assert!(retrain_due(&dataset, "vector").await.unwrap());

        let dir = TempDir::new().unwrap();
        let mut dataset = grown(&dir).await;
        let params = VectorIndexParams::ivf_pq(2, 8, 2, DistanceType::L2, 10);
        dataset
            .create_index(&["vector"], IndexType::Vector, None, &params, false)
            .await
            .unwrap();
        dataset.checkout_latest().await.unwrap();
        assert!(!retrain_due(&dataset, "vector").await.unwrap());

        let dir = TempDir::new().unwrap();
        let mut dataset = grown(&dir).await;
        let params = VectorIndexParams::ivf_flat(2, DistanceType::L2);
        for (name, fragment) in [("shard-0", 0u32), ("shard-1", 1u32)] {
            dataset
                .create_index_builder(&["vector"], IndexType::Vector, &params)
                .name(name.to_string())
                .fragments(vec![fragment])
                .await
                .unwrap();
            dataset.checkout_latest().await.unwrap();
        }
        assert!(!retrain_due(&dataset, "vector").await.unwrap());
        let before = covered(&dataset, "vector").await.unwrap();
        assert!(optimize(&mut dataset, "vector", true).await.is_err());
        assert_eq!(covered(&dataset, "vector").await.unwrap(), before);
        let per_name = coverage(&dataset, "vector").await.unwrap();
        assert!(
            per_name.iter().all(|c| c.indexed_fragments.len() == 1),
            "{per_name:?}"
        );
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
        let hits = query::ann(
            &dataset,
            &table,
            &Query {
                nprobes: Some(MAX_PARTITIONS),
                ..exact(&testing::vectors(1, DIM)[0], 10)
            },
        )
        .await
        .unwrap();
        let mut tied: Vec<&str> = hits
            .iter()
            .filter(|h| h.distance < 1e-6)
            .map(|h| h.key.as_str())
            .collect();
        tied.sort();
        assert_eq!(tied, ["k120", "k180", "k60"], "{hits:?}");
        for vector in testing::vectors(4, DIM) {
            assert_same_top_k(&dataset, &vector, 10).await;
        }
    }
}
