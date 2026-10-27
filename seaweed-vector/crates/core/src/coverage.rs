//! Which fragments the vector index covers, and which it has not reached yet.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use lance::Dataset;
use lance::index::DatasetIndexExt;
use roaring::RoaringBitmap;

/// How much of the dataset one named index covers at this version.
#[derive(Debug, serde::Serialize)]
pub struct IndexCoverage {
    pub index_name: String,
    /// Physical index segments: the IndexMetadata deltas sharing the name.
    pub segments: usize,
    /// False when any delta of this index lacks a fragment bitmap: the index
    /// exists but which fragments it covers is unknown.
    pub coverage_known: bool,
    pub indexed_fragments: Vec<u64>,
    pub unindexed_fragments: Vec<u64>,
    pub unindexed_rows: usize,
}

/// Coverage for every index built on `vector_column` at this version.
pub async fn coverage(dataset: &Dataset, vector_column: &str) -> Result<Vec<IndexCoverage>> {
    let field = dataset
        .schema()
        .field(vector_column)
        .with_context(|| format!("no such column {vector_column}"))?;
    let mut by_name: BTreeMap<String, Vec<Option<RoaringBitmap>>> = BTreeMap::new();
    for index in dataset.load_indices().await?.iter() {
        if index.fields != [field.id] {
            continue;
        }
        by_name
            .entry(index.name.clone())
            .or_default()
            .push(index.fragment_bitmap.clone());
    }
    let all: RoaringBitmap = dataset.fragments().iter().map(|f| f.id as u32).collect();
    let mut out = Vec::new();
    for (name, bitmaps) in by_name {
        let coverage_known = bitmaps.iter().all(|b| b.is_some());
        let mut indexed = RoaringBitmap::new();
        for bitmap in bitmaps.iter().flatten() {
            indexed |= bitmap;
        }
        // The bitmap records what the index covered when built; fragments
        // the manifest has since dropped are not part of this version.
        let indexed = indexed & &all;
        let unindexed = &all - &indexed;
        let mut unindexed_rows = 0;
        for fragment in dataset.get_fragments() {
            // physical_rows counts deleted rows too, and may be absent in the
            // manifest; count live rows instead.
            if unindexed.contains(fragment.id() as u32) {
                unindexed_rows += fragment.count_rows(None).await?;
            }
        }
        out.push(IndexCoverage {
            index_name: name,
            segments: bitmaps.len(),
            coverage_known,
            indexed_fragments: indexed.iter().map(|id| id as u64).collect(),
            unindexed_rows,
            unindexed_fragments: unindexed.iter().map(|id| id as u64).collect(),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{Query, ann, flat};
    use crate::table::VectorTable;
    use crate::testing::{DIM, append_dataset, vectors, write_dataset};
    use lance::index::vector::VectorIndexParams;
    use lance_index::IndexType;
    use lance_linalg::distance::DistanceType;
    use tempfile::TempDir;

    async fn indexed_dataset(dir: &TempDir, tags: &[&str]) -> Dataset {
        let mut dataset = write_dataset(dir, &vectors(tags.len(), DIM), tags).await;
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

    #[tokio::test]
    async fn coverage_reports_the_unindexed_fragment() {
        let dir = TempDir::new().unwrap();
        let mut dataset = indexed_dataset(&dir, &["a"; 64]).await;
        append_dataset(&mut dataset, &vectors(4, DIM), &["b"; 4]).await;

        let coverages = coverage(&dataset, "vector").await.unwrap();
        assert_eq!(coverages.len(), 1);
        let cov = &coverages[0];
        assert_eq!(cov.index_name, "vidx");
        assert_eq!(cov.segments, 1);
        assert_eq!(cov.indexed_fragments, vec![0]);
        assert_eq!(cov.unindexed_fragments.len(), 1);
        assert_eq!(cov.unindexed_rows, 4);
    }

    #[tokio::test]
    async fn ann_searches_unindexed_fragments() {
        let dir = TempDir::new().unwrap();
        let mut dataset = indexed_dataset(&dir, &["a"; 64]).await;
        append_dataset(&mut dataset, &[vec![9.0; DIM]], &["b"]).await;
        let table = VectorTable::resolve(&dataset, None, None).unwrap();

        let hits = ann(
            &dataset,
            &table,
            &Query {
                vector: vec![9.0; DIM],
                k: 1,
                filter: None,
                postfilter: false,
                distance: DistanceType::L2,
                nprobes: Some(4),
                index_segments: None,
                fragments: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key, "k64");
        assert!(hits[0].distance < 1e-6);
    }

    #[tokio::test]
    async fn prefiltered_ann_matches_filtered_flat() {
        let dir = TempDir::new().unwrap();
        let tags: Vec<&str> = (0..64)
            .map(|i| if i % 2 == 0 { "a" } else { "b" })
            .collect();
        let mut dataset = indexed_dataset(&dir, &tags).await;
        append_dataset(&mut dataset, &vectors(4, DIM), &["a"; 4]).await;
        let table = VectorTable::resolve(&dataset, None, None).unwrap();

        let query = Query {
            vector: vectors(1, DIM).remove(0),
            k: 10,
            filter: Some("tag = 'a'".to_string()),
            postfilter: false,
            distance: DistanceType::L2,
            nprobes: Some(4),
            index_segments: None,
            fragments: None,
        };
        let filtered = ann(&dataset, &table, &query).await.unwrap();
        let exact = flat(&dataset, &table, &query).await.unwrap();
        // equal-distance hits have no specified order between the two scans
        let mut filtered_keys = filtered.iter().map(|h| &h.key).collect::<Vec<_>>();
        let mut exact_keys = exact.iter().map(|h| &h.key).collect::<Vec<_>>();
        filtered_keys.sort();
        exact_keys.sort();
        assert_eq!(filtered_keys, exact_keys);
        assert!(filtered.len() <= 10);
        assert!(filtered.iter().all(|h| h.key != "k1"));
    }

    #[tokio::test]
    async fn pinned_version_ignores_newer_commits() {
        let dir = TempDir::new().unwrap();
        let mut dataset = write_dataset(&dir, &vectors(4, DIM), &["a"; 4]).await;
        let pinned = dataset
            .checkout_version(dataset.version().version)
            .await
            .unwrap();
        append_dataset(&mut dataset, &vectors(4, DIM), &["b"; 4]).await;

        assert_eq!(pinned.count_rows(None).await.unwrap(), 4);
        let table = VectorTable::resolve(&pinned, None, None).unwrap();
        let hits = flat(
            &pinned,
            &table,
            &Query {
                vector: vectors(1, DIM).remove(0),
                k: 10,
                filter: None,
                postfilter: false,
                distance: DistanceType::L2,
                nprobes: None,
                index_segments: None,
                fragments: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(hits.len(), 4);
        assert!(hits.iter().all(|h| h.row_id < 4));
    }
}
