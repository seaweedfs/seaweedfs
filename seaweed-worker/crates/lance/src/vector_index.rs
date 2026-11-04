//! The vector index an S3 Vectors table searches through: how it is named,
//! sized and built. The query engine and the maintenance worker share this.

use anyhow::{Context, Result};
use lance::Dataset;
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_linalg::distance::DistanceType;

/// The name every S3 Vectors index build uses, so a table has one logical index.
pub const INDEX_NAME: &str = "vector_idx";
pub const MAX_PARTITIONS: usize = 4096;

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

/// Partitions of the index named `name`: the most any of its deltas has.
pub async fn partitions(dataset: &Dataset, name: &str) -> Result<usize> {
    let stats: serde_json::Value = serde_json::from_str(&dataset.index_statistics(name).await?)
        .with_context(|| format!("parse index statistics for {name}"))?;
    Ok(stats["indices"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|i| i["num_partitions"].as_u64())
        .max()
        .unwrap_or(0) as usize)
}

/// Whether an index built with `partitions` should be trained again for a
/// table of `rows`: once the table has grown to twice the partitions it was
/// sized for, the centroids no longer fit it.
pub fn needs_retrain(partitions: usize, rows: usize) -> bool {
    partitions_for(rows) >= 2 * partitions.max(1)
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
    let params = VectorIndexParams::ivf_flat(partitions, distance_metric(dataset, vector_column));
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

/// Extend the indices on `vector_column` over rows written since they were
/// built: a new delta segment, or with `retrain` each index built again over
/// all rows with partitions for the current row count. Lance's own retrain
/// does not size the partitions, so the rebuild goes through [`build`].
/// Indices on other columns are left alone.
pub async fn optimize(dataset: &mut Dataset, vector_column: &str, retrain: bool) -> Result<()> {
    let names = index_names(dataset, vector_column).await?;
    if retrain {
        let partitions = partitions_for(dataset.count_rows(None).await?);
        for name in &names {
            build(dataset, vector_column, name, partitions, true).await?;
        }
        return Ok(());
    }
    let mut options = OptimizeOptions::append();
    options.index_names = Some(names);
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
    fn partitions_follow_the_square_root() {
        assert_eq!(partitions_for(0), 1);
        assert_eq!(partitions_for(64), 8);
        assert_eq!(partitions_for(50_000), 224);
        assert_eq!(partitions_for(1_000_000), 1000);
        assert_eq!(partitions_for(100_000_000), MAX_PARTITIONS);
    }
}
