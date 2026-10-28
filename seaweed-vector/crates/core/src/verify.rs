//! The spike's exit gate: the engine's answers must equal what a bare Lance
//! `scan().nearest()` produces on the same pinned version, and no eligible
//! row — indexed or not — may be missing.

use arrow_array::{Float32Array, UInt64Array};
use futures::TryStreamExt;
use lance::Dataset;
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_linalg::distance::DistanceType;
use tempfile::TempDir;

use crate::coverage::coverage;
use crate::query::{self, Query};
use crate::table::VectorTable;
use crate::testing::{self, DIM};

async fn indexed_dataset(dir: &TempDir) -> Dataset {
    let mut dataset = testing::write_dataset(dir, &testing::vectors(64, DIM), &["a"; 64]).await;
    let params = VectorIndexParams::ivf_flat(4, DistanceType::L2);
    dataset
        .create_index(
            &["vector"],
            IndexType::Vector,
            Some("vidx".to_string()),
            &params,
            true,
        )
        .await
        .unwrap();
    dataset.checkout_latest().await.unwrap();
    dataset
}

fn query_for(vector: Vec<f32>, k: usize) -> Query {
    Query {
        vector,
        k,
        filter: None,
        postfilter: false,
        distance: DistanceType::L2,
        nprobes: Some(4),
        index_segments: None,
        fragments: None,
    }
}

/// The reference path: a bare scanner over the same dataset handle.
async fn reference(dataset: &Dataset, vector: &[f32], k: usize) -> Vec<(u64, f32)> {
    let mut scanner = dataset.scan();
    scanner
        .nearest("vector", &Float32Array::from(vector.to_vec()), k)
        .unwrap()
        .distance_metric(DistanceType::L2)
        .nprobes(4)
        .with_row_id();
    let mut stream = scanner.try_into_stream().await.unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("_rowid")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let distances = batch
            .column_by_name("_distance")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), distances.value(i)));
        }
    }
    rows
}

async fn assert_matches_reference(dataset: &Dataset, vector: &[f32], k: usize) {
    let table = VectorTable::resolve(dataset, None, None).unwrap();
    let hits = query::ann(dataset, &table, &query_for(vector.to_vec(), k))
        .await
        .unwrap();
    let expected = reference(dataset, vector, k).await;
    assert_eq!(hits.len(), expected.len());
    for (hit, (row_id, distance)) in hits.iter().zip(expected.iter()) {
        assert_eq!(hit.row_id, *row_id);
        assert!((hit.distance - distance).abs() < 1e-6, "{}", hit.key);
    }
}

#[tokio::test]
async fn ann_matches_lance_reference() {
    let dir = TempDir::new().unwrap();
    let dataset = indexed_dataset(&dir).await;
    assert_matches_reference(&dataset, &testing::vectors(1, DIM)[0], 10).await;
}

#[tokio::test]
async fn ann_matches_reference_over_unindexed_fragments() {
    let dir = TempDir::new().unwrap();
    let mut dataset = indexed_dataset(&dir).await;
    testing::append_dataset(&mut dataset, &testing::vectors(8, DIM), &["a"; 8]).await;

    let cov = &coverage(&dataset, "vector").await.unwrap()[0];
    assert_eq!(cov.unindexed_fragments.len(), 1);
    assert_eq!(cov.unindexed_rows, 8);

    for vector in testing::vectors(4, DIM) {
        assert_matches_reference(&dataset, &vector, 10).await;
    }
}

#[tokio::test]
async fn ann_matches_reference_across_segments() {
    let dir = TempDir::new().unwrap();
    let mut dataset = indexed_dataset(&dir).await;
    testing::append_dataset(&mut dataset, &testing::vectors(32, DIM), &["a"; 32]).await;
    dataset
        .optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();

    let cov = &coverage(&dataset, "vector").await.unwrap()[0];
    assert_eq!(cov.segments, 2);
    assert!(cov.unindexed_fragments.is_empty());

    for vector in testing::vectors(4, DIM) {
        assert_matches_reference(&dataset, &vector, 10).await;
    }
}

#[tokio::test]
async fn deletes_are_not_returned() {
    let dir = TempDir::new().unwrap();
    let mut dataset = indexed_dataset(&dir).await;
    dataset.delete("key = 'k0'").await.unwrap();
    assert_matches_reference(&dataset, &testing::vectors(1, DIM)[0], 10).await;
}

/// Writes a small indexed dataset with unindexed rows to
/// $WEED_VECTOR_SPIKE_DIR for exercising the `weed-vector` CLI:
///
///   WEED_VECTOR_SPIKE_DIR=/tmp/vds cargo test make_spike_dataset -- --ignored
///   weed-vector-cli describe /tmp/vds
///   weed-vector-cli coverage /tmp/vds
///   weed-vector-cli query /tmp/vds --vector 0,0,0,0 --k 5
#[tokio::test]
#[ignore]
async fn make_spike_dataset() {
    let dir = std::env::var("WEED_VECTOR_SPIKE_DIR").expect("set WEED_VECTOR_SPIKE_DIR");
    let mut dataset = testing::write_dataset_at(&dir, &testing::vectors(64, DIM), &["a"; 64]).await;
    let params = VectorIndexParams::ivf_flat(4, DistanceType::L2);
    dataset
        .create_index(
            &["vector"],
            IndexType::Vector,
            Some("vidx".to_string()),
            &params,
            true,
        )
        .await
        .unwrap();
    dataset.checkout_latest().await.unwrap();
    testing::append_dataset(&mut dataset, &testing::vectors(8, DIM), &["b"; 8]).await;
}

#[tokio::test]
async fn query_at_pinned_version_survives_maintenance() {
    let dir = TempDir::new().unwrap();
    let mut dataset = indexed_dataset(&dir).await;
    let pinned = dataset
        .checkout_version(dataset.version().version)
        .await
        .unwrap();

    testing::append_dataset(&mut dataset, &testing::vectors(8, DIM), &["b"; 8]).await;
    dataset
        .optimize_indices(&OptimizeOptions::default())
        .await
        .unwrap();

    assert_eq!(pinned.count_rows(None).await.unwrap(), 64);
    let vector = testing::vectors(1, DIM)[0].clone();
    let hits = query::ann(
        &pinned,
        &VectorTable::resolve(&pinned, None, None).unwrap(),
        &query_for(vector.clone(), 10),
    )
    .await
    .unwrap();
    let expected = reference(&pinned, &vector, 10).await;
    assert_eq!(hits.len(), expected.len());
    assert!(hits.iter().all(|h| h.row_id < 64));
    for (hit, (row_id, distance)) in hits.iter().zip(expected.iter()) {
        assert_eq!(hit.row_id, *row_id);
        assert!((hit.distance - distance).abs() < 1e-6);
    }

    assert_matches_reference(&dataset, &vector, 10).await;
}
