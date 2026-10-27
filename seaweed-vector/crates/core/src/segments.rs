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
        .filter_map(|i| i.fragment_bitmap.clone().map(|b| (i.uuid, b & &live)))
        .collect())
}

/// Round-robin `segments` across `n` workers. Index order is unstable, so
/// workers see disjoint, deterministic-in-name partitions rather than
/// physically adjacent ones.
pub fn partition(segments: &[Uuid], n: usize) -> Vec<Vec<Uuid>> {
    let mut ids = segments.to_vec();
    ids.sort();
    let mut out = vec![Vec::new(); n.max(1)];
    for (i, id) in ids.into_iter().enumerate() {
        out[i % n.max(1)].push(id);
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
        // Second fragment + delta segment so the index has two parts.
        testing::append_dataset(&mut dataset, &testing::vectors(4, 4), &["i", "j", "k", "l"]).await;
        dataset
            .optimize_indices(&OptimizeOptions::append())
            .await
            .unwrap();

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
        // The unindexed fragment belongs to nobody's segment; worker 0
        // sweeps it — the same contract plan_search implements on the
        // enterprise side.
        let unindexed: Vec<u64> = {
            let all: std::collections::BTreeSet<u64> =
                dataset.fragments().iter().map(|f| f.id).collect();
            let mut indexed = std::collections::BTreeSet::new();
            for (_, b) in &segments_bitmap {
                indexed.extend(b.iter().map(|i| i as u64));
            }
            all.difference(&indexed).copied().collect()
        };
        let mut merged: Vec<(f32, u64)> = Vec::new();
        for (w, share) in partition(&segments, 2).into_iter().enumerate() {
            let mut worker_q = base.clone();
            worker_q.index_segments = Some(share);
            if w == 0 && !unindexed.is_empty() {
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
}
