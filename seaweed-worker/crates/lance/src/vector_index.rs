//! The vector index an S3 Vectors table searches through: how it is named,
//! sized and built. The query engine and the maintenance worker share this.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use lance::Dataset;
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_linalg::distance::DistanceType;
use roaring::RoaringBitmap;

/// The name every S3 Vectors index build uses, so a table has one logical index.
pub const INDEX_NAME: &str = "vector_idx";
pub const MAX_PARTITIONS: usize = 4096;
/// Delta segments one index name may pile up before they are merged into one:
/// each is a separate search per query.
pub const MAX_DELTAS: usize = 8;

/// IVF partitions for a table of `rows`: about one per sqrt(rows), at least one.
pub fn partitions_for(rows: usize) -> usize {
    ((rows as f64).sqrt().round() as usize).clamp(1, MAX_PARTITIONS)
}

/// The metric the table was created with, from the vector field's
/// `distance_metric` metadata as the S3 Vectors gateway writes it; L2 when
/// absent.
pub fn distance_metric(dataset: &Dataset, vector_column: &str) -> DistanceType {
    match dataset
        .schema()
        .field(vector_column)
        .and_then(|f| f.metadata.get("distance_metric").map(String::as_str))
    {
        Some("COSINE") => DistanceType::Cosine,
        Some("DOT_PRODUCT") => DistanceType::Dot,
        _ => DistanceType::L2,
    }
}

/// Names of the indices built on `vector_column`, each once.
pub async fn index_names(dataset: &Dataset, vector_column: &str) -> Result<Vec<String>> {
    let Some(field) = dataset.schema().field(vector_column) else {
        return Ok(Vec::new());
    };
    let mut names: Vec<String> = dataset
        .load_indices()
        .await?
        .iter()
        .filter(|i| i.fields == [field.id])
        .map(|i| i.name.clone())
        .collect();
    names.sort();
    names.dedup();
    Ok(names)
}

/// What the indices on a vector column cover, taken together. Sharded builds
/// give one name per shard and each name misses the others' fragments, so
/// coverage is the union over names, never a single name's statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Coverage {
    pub indexed_rows: u64,
    pub unindexed_rows: u64,
    /// Delta segments per index name.
    pub deltas: BTreeMap<String, usize>,
}

/// Coverage of `vector_column` by every index built on it, or `None` when it
/// has no index. An index that does not record its fragments is an error:
/// counting its rows as uncovered would have them indexed a second time.
pub async fn coverage(dataset: &Dataset, vector_column: &str) -> Result<Option<Coverage>> {
    let Some(field) = dataset.schema().field(vector_column) else {
        return Ok(None);
    };
    let mut deltas: BTreeMap<String, usize> = BTreeMap::new();
    let mut covered = RoaringBitmap::new();
    for index in dataset.load_indices().await?.iter() {
        if index.fields != [field.id] {
            continue;
        }
        let Some(bitmap) = &index.fragment_bitmap else {
            anyhow::bail!(
                "index {} does not record the fragments it covers",
                index.name
            );
        };
        *deltas.entry(index.name.clone()).or_default() += 1;
        covered |= bitmap;
    }
    if deltas.is_empty() {
        return Ok(None);
    }
    let (mut indexed_rows, mut unindexed_rows) = (0, 0);
    for fragment in dataset.get_fragments() {
        let rows = fragment.count_rows(None).await? as u64;
        if covered.contains(fragment.id() as u32) {
            indexed_rows += rows;
        } else {
            unindexed_rows += rows;
        }
    }
    Ok(Some(Coverage {
        indexed_rows,
        unindexed_rows,
        deltas,
    }))
}

/// How to extend an index name that already has `deltas` segments: another
/// delta while there is room, and once the limit is reached every delta
/// merged with the new rows into one. Lance's default options promise no
/// merge, so the count is explicit.
pub fn optimize_policy(deltas: usize) -> OptimizeOptions {
    if deltas >= MAX_DELTAS {
        OptimizeOptions::merge(deltas)
    } else {
        OptimizeOptions::append()
    }
}

/// What an index was built as, from Lance's statistics.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexSettings {
    /// The most partitions any of its deltas has.
    pub partitions: usize,
    /// Lance's name for the type, such as `IVF_FLAT`.
    pub index_type: String,
    pub metric: Option<DistanceType>,
}

pub async fn index_settings(dataset: &Dataset, name: &str) -> Result<IndexSettings> {
    let stats: serde_json::Value = serde_json::from_str(&dataset.index_statistics(name).await?)
        .with_context(|| format!("parse index statistics for {name}"))?;
    let deltas: Vec<&serde_json::Value> =
        stats["indices"].as_array().into_iter().flatten().collect();
    Ok(IndexSettings {
        partitions: deltas
            .iter()
            .filter_map(|i| i["num_partitions"].as_u64())
            .max()
            .unwrap_or(0) as usize,
        index_type: deltas
            .first()
            .and_then(|i| i["index_type"].as_str())
            .unwrap_or_default()
            .to_string(),
        metric: deltas
            .first()
            .and_then(|i| i["metric_type"].as_str())
            .and_then(|m| DistanceType::try_from(m).ok()),
    })
}

/// Whether an index built with `partitions` should be trained again for a
/// table of `rows`: once the table has grown to twice the partitions it was
/// sized for, the centroids no longer fit it.
pub fn needs_retrain(partitions: usize, rows: usize) -> bool {
    partitions_for(rows) >= 2 * partitions.max(1)
}

/// Whether the index on `vector_column` is due for [`optimize`] with
/// `retrain`: a single IVF_FLAT name the table has outgrown. A sharded build
/// sizes each name for its shard, and other index types keep the settings
/// they were built with.
pub async fn retrain_due(dataset: &Dataset, vector_column: &str) -> Result<bool> {
    let Some(cover) = coverage(dataset, vector_column).await? else {
        return Ok(false);
    };
    let [name] = Vec::from_iter(cover.deltas.keys())[..] else {
        return Ok(false);
    };
    let settings = index_settings(dataset, name).await?;
    Ok(settings.index_type == "IVF_FLAT"
        && needs_retrain(settings.partitions, dataset.count_rows(None).await?))
}

/// Build an IVF_FLAT index named `name` over `vector_column` in the metric the
/// table was created with. `replace` rebuilds an index of that name; the
/// dataset is left at the new version.
pub async fn build(
    dataset: &mut Dataset,
    vector_column: &str,
    name: &str,
    partitions: usize,
    replace: bool,
) -> Result<()> {
    let metric = distance_metric(dataset, vector_column);
    build_with(dataset, vector_column, name, partitions, replace, metric).await
}

async fn build_with(
    dataset: &mut Dataset,
    vector_column: &str,
    name: &str,
    partitions: usize,
    replace: bool,
    metric: DistanceType,
) -> Result<()> {
    let params = VectorIndexParams::ivf_flat(partitions, metric);
    dataset
        .create_index(
            &[vector_column],
            IndexType::Vector,
            Some(name.to_string()),
            &params,
            replace,
        )
        .await
        .with_context(|| format!("build index {name} with {partitions} partitions"))?;
    dataset.checkout_latest().await?;
    Ok(())
}

/// Cover the rows written since the indices on `vector_column` were built.
/// One name takes them, the one with the fewest deltas, under
/// [`optimize_policy`]; the other names keep the fragments they have, so
/// shards stay disjoint. With `retrain` the index is built again over the
/// whole table with partitions for the current row count, in the metric it
/// has; only a single IVF_FLAT name is rebuilt this way. Lance's own retrain
/// does not size the partitions, so the rebuild goes through [`build`].
/// Indices on other columns are left alone.
pub async fn optimize(dataset: &mut Dataset, vector_column: &str, retrain: bool) -> Result<()> {
    let Some(cover) = coverage(dataset, vector_column).await? else {
        return Ok(());
    };
    if retrain {
        if cover.deltas.len() > 1 {
            anyhow::bail!(
                "the vector index is sharded over {} names; each would be rebuilt over the whole table",
                cover.deltas.len()
            );
        }
        let partitions = partitions_for(dataset.count_rows(None).await?);
        for name in cover.deltas.keys() {
            let settings = index_settings(dataset, name).await?;
            if settings.index_type != "IVF_FLAT" {
                anyhow::bail!(
                    "index {name} is {}; only an IVF_FLAT index is retrained here",
                    settings.index_type
                );
            }
            let metric = settings
                .metric
                .unwrap_or_else(|| distance_metric(dataset, vector_column));
            build_with(dataset, vector_column, name, partitions, true, metric).await?;
        }
        return Ok(());
    }
    let (name, deltas) = cover
        .deltas
        .iter()
        .min_by_key(|(name, deltas)| (**deltas, (*name).clone()))
        .map(|(name, deltas)| (name.clone(), *deltas))
        .context("no index to extend")?;
    let mut options = optimize_policy(deltas);
    options.index_names = Some(vec![name]);
    dataset.optimize_indices(&options).await?;
    dataset.checkout_latest().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retrain_once_the_table_quadruples() {
        assert!(!needs_retrain(224, 50_000));
        assert!(!needs_retrain(224, 150_000));
        assert!(needs_retrain(224, 200_704));
        assert!(needs_retrain(0, 4));
        assert!(!needs_retrain(4096, 100_000_000));
    }

    #[test]
    fn deltas_append_until_the_limit_then_merge() {
        assert_eq!(optimize_policy(1).num_indices_to_merge, Some(0));
        assert_eq!(optimize_policy(7).num_indices_to_merge, Some(0));
        assert_eq!(optimize_policy(8).num_indices_to_merge, Some(8));
        assert_eq!(optimize_policy(11).num_indices_to_merge, Some(11));
    }

    #[test]
    fn partitions_follow_the_square_root() {
        assert_eq!(partitions_for(0), 1);
        assert_eq!(partitions_for(64), 8);
        assert_eq!(partitions_for(50_000), 224);
        assert_eq!(partitions_for(1_000_000), 1000);
        assert_eq!(partitions_for(100_000_000), MAX_PARTITIONS);
    }
}
