//! Physical index segments and the partition a fanout planner assigns to each
//! worker. OSS runs the same restriction in-process; Enterprise ships the
//! segment ids to remote workers — the engine does not change.

use anyhow::Result;
use lance::Dataset;
use lance::index::DatasetIndexExt;
use uuid::Uuid;

/// UUIDs of every physical index segment covering `field` at this version.
pub async fn index_segments(dataset: &Dataset, field: &str) -> Result<Vec<Uuid>> {
    Ok(segment_fragments(dataset, field)
        .await?
        .iter()
        .map(|(id, _)| *id)
        .collect())
}

/// Per index segment on `field`: its uuid and the live fragments it covers.
/// Segments with unknown coverage (no bitmap) are skipped — callers needing
/// strict coverage must treat that as a degenerate plan.
pub async fn segment_fragments(
    dataset: &Dataset,
    field: &str,
) -> Result<Vec<(Uuid, roaring::RoaringBitmap)>> {
    Ok(named_segment_fragments(dataset, field)
        .await?
        .into_iter()
        .map(|(id, _, b)| (id, b))
        .collect())
}

/// Like `segment_fragments` but carries the logical index name — sharded
/// builds create one named index per shard, and a `with_index_segments`
/// scan must not mix names, so a fanout planner groups by name.
pub async fn named_segment_fragments(
    dataset: &Dataset,
    field: &str,
) -> Result<Vec<(Uuid, String, roaring::RoaringBitmap)>> {
    let field = dataset
        .schema()
        .field(field)
        .ok_or_else(|| anyhow::anyhow!("no such field {field}"))?;
    let live: roaring::RoaringBitmap = dataset.fragments().iter().map(|f| f.id as u32).collect();
    Ok(dataset
        .load_indices()
        .await?
        .iter()
        .filter(|i| i.fields == [field.id])
        .filter_map(|i| {
            i.fragment_bitmap
                .clone()
                .map(|b| (i.uuid, i.name.clone(), b & &live))
        })
        .collect())
}

/// Live fragment ids covered by `segments` on `field` — the union a scoped
/// fragment list needs so the selected segments are not filtered out of the
/// scan.
pub async fn covered_fragments(
    dataset: &Dataset,
    field: &str,
    segments: &[Uuid],
) -> Result<Vec<u64>> {
    Ok(segment_fragments(dataset, field)
        .await?
        .into_iter()
        .filter(|(id, _)| segments.contains(id))
        .flat_map(|(_, b)| b.iter().map(u64::from).collect::<Vec<_>>())
        .collect())
}

/// Round-robin `segments` across `n` workers. Index order is unstable, so
/// workers see disjoint, deterministic-in-name partitions rather than
/// physically adjacent ones. Zero workers means zero partitions — nothing is
/// silently assigned.
pub fn partition(segments: &[Uuid], n: usize) -> Vec<Vec<Uuid>> {
    let mut out = vec![Vec::new(); n];
    if n == 0 {
        return out;
    }
    let mut ids = segments.to_vec();
    ids.sort();
    for (i, id) in ids.into_iter().enumerate() {
        out[i % n].push(id);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Query, VectorTable, query, testing};
    use lance::index::vector::VectorIndexParams;
    use lance_index::IndexType;
    use lance_index::optimize::OptimizeOptions;
    use lance_linalg::distance::DistanceType;
    use tempfile::TempDir;

    /// Two workers each scan their share of the index segments; merged, the
    /// hits equal a single full-dataset search.
    #[tokio::test]
    async fn partitioned_segments_merge_to_full_result() {
        let dir = TempDir::new().unwrap();
        let mut dataset = testing::write_dataset(
            &dir,
            &testing::vectors(8, 4),
            &["a", "b", "c", "d", "e", "f", "g", "h"],
        )
        .await;
        let params = VectorIndexParams::ivf_flat(4, DistanceType::L2);
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
        // Second fragment + delta segment so the index has two parts, then a
        // third fragment left unindexed for the sweep half of the plan.
        testing::append_dataset(&mut dataset, &testing::vectors(4, 4), &["i", "j", "k", "l"]).await;
        dataset
            .optimize_indices(&OptimizeOptions::append())
            .await
            .unwrap();
        testing::append_dataset(&mut dataset, &testing::vectors(2, 4), &["m", "n"]).await;

        let resolved = VectorTable::resolve(&dataset, None, None).unwrap();
        let base = Query {
            vector: vec![0.5; 4],
            k: 5,
            filter: None,
            postfilter: false,
            distance: DistanceType::L2,
            nprobes: None,
            index_segments: None,
            fragments: None,
        };
        let want: Vec<(u64, u32)> = query::ann(&dataset, &resolved, &base)
            .await
            .unwrap()
            .iter()
            .map(|h| (h.row_id, h.distance.to_bits()))
            .collect();

        let segments_bitmap = segment_fragments(&dataset, "vector").await.unwrap();
        let segments: Vec<Uuid> = segments_bitmap.iter().map(|(id, _)| *id).collect();
        assert!(
            segments.len() >= 2,
            "expected >=2 segments, got {segments:?}"
        );
        // The unindexed fragment belongs to nobody's segment; worker 0 sweeps
        // it next to its own share — the same contract plan_search implements
        // on the enterprise side.
        let unindexed: Vec<u64> = {
            let all: std::collections::BTreeSet<u64> =
                dataset.fragments().iter().map(|f| f.id).collect();
            let mut indexed = std::collections::BTreeSet::new();
            for (_, b) in &segments_bitmap {
                indexed.extend(b.iter().map(|i| i as u64));
            }
            all.difference(&indexed).copied().collect()
        };
        assert!(!unindexed.is_empty(), "expected an unindexed fragment");
        let mut merged: Vec<(f32, u64)> = Vec::new();
        for (w, share) in partition(&segments, 2).into_iter().enumerate() {
            let mut worker_q = base.clone();
            worker_q.index_segments = Some(share);
            if w == 0 {
                worker_q.fragments = Some(unindexed.clone());
            }
            merged.extend(
                query::ann(&dataset, &resolved, &worker_q)
                    .await
                    .unwrap()
                    .iter()
                    .map(|h| (h.distance, h.row_id)),
            );
        }
        merged.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.1.cmp(&b.1))
        });
        let got: Vec<(u64, u32)> = merged
            .iter()
            .take(base.k)
            .map(|(d, id)| (*id, d.to_bits()))
            .collect();
        assert_eq!(got, want);
    }

    /// named_segment_fragments must carry the logical index name through —
    /// a fanout planner groups tasks by name.
    #[tokio::test]
    async fn named_segment_fragments_returns_index_names() {
        let dir = TempDir::new().unwrap();
        let mut dataset = testing::write_dataset(
            &dir,
            &testing::vectors(8, 4),
            &["a", "b", "c", "d", "e", "f", "g", "h"],
        )
        .await;
        let params = VectorIndexParams::ivf_flat(4, DistanceType::L2);
        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("shard-a".to_string()),
                &params,
                false,
            )
            .await
            .unwrap();
        let named = named_segment_fragments(&dataset, "vector").await.unwrap();
        assert!(!named.is_empty());
        assert!(named.iter().all(|(_, name, _)| name == "shard-a"));
    }

    /// A scope of fragment ids that no longer exist, an empty list, or an
    /// empty-segment assignment must return no hits — never fall through to
    /// an unrestricted scan.
    #[tokio::test]
    async fn empty_scope_scans_nothing() {
        let dir = TempDir::new().unwrap();
        let dataset =
            testing::write_dataset(&dir, &testing::vectors(4, 4), &["a", "b", "c", "d"]).await;
        let resolved = VectorTable::resolve(&dataset, None, None).unwrap();
        let mut q = Query {
            vector: vec![0.5; 4],
            k: 4,
            filter: None,
            postfilter: false,
            distance: DistanceType::L2,
            nprobes: None,
            index_segments: None,
            fragments: Some(vec![u64::MAX]),
        };
        assert!(
            query::ann(&dataset, &resolved, &q)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            query::flat(&dataset, &resolved, &q)
                .await
                .unwrap()
                .is_empty()
        );
        q.fragments = Some(Vec::new());
        assert!(
            query::ann(&dataset, &resolved, &q)
                .await
                .unwrap()
                .is_empty()
        );
        q.fragments = None;
        q.index_segments = Some(Vec::new());
        assert!(
            query::ann(&dataset, &resolved, &q)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(partition(&[Uuid::nil()], 0).is_empty());
    }
}
