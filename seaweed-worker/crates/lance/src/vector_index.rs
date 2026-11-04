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

/// Extend the table's indices over rows written since they were built: a new
/// delta segment, or with `retrain` one index trained again on all rows.
pub async fn optimize(dataset: &mut Dataset, retrain: bool) -> Result<()> {
    let options = if retrain {
        OptimizeOptions::retrain()
    } else {
        OptimizeOptions::append()
    };
    dataset.optimize_indices(&options).await?;
    dataset.checkout_latest().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partitions_follow_the_square_root() {
        assert_eq!(partitions_for(0), 1);
        assert_eq!(partitions_for(64), 8);
        assert_eq!(partitions_for(50_000), 224);
        assert_eq!(partitions_for(1_000_000), 1000);
        assert_eq!(partitions_for(100_000_000), MAX_PARTITIONS);
    }
}
