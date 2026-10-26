//! Top-K queries over a [`VectorTable`]: exact flat scan, or the ANN index
//! with a flat pass over the fragments it does not cover yet.

use anyhow::{Context, Result, bail};
use arrow_array::{Array, Float32Array, RecordBatch, StringArray, UInt64Array};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::scanner::Scanner;
use lance_linalg::distance::DistanceType;

use crate::table::VectorTable;

const DISTANCE_COLUMN: &str = "_distance";
const ROW_ID_COLUMN: &str = "_rowid";

/// One vector search request.
#[derive(Debug, Clone)]
pub struct Query {
    pub vector: Vec<f32>,
    pub k: usize,
    /// A Lance SQL filter over the metadata columns.
    pub filter: Option<String>,
    /// Filter the top-K rows after ranking instead of the eligible rows
    /// before it. Lance's default post-filters and can return fewer than k
    /// results while eligible neighbours go unreported; keep it off for
    /// S3 Vectors filter semantics.
    pub postfilter: bool,
    pub distance: DistanceType,
    pub nprobes: Option<usize>,
}

/// One row of a top-K answer.
#[derive(Debug, Clone)]
pub struct Hit {
    /// The key column value, or the row id when the table has no key column.
    pub key: String,
    /// Raw Lance `_distance` in the query's metric.
    pub distance: f32,
    pub row_id: u64,
}

/// Exact top-K over every fragment, ignoring any index.
pub async fn flat(dataset: &Dataset, table: &VectorTable, query: &Query) -> Result<Vec<Hit>> {
    run(scanner(dataset, table, query, false)?, table).await
}

/// Top-K with the ANN index where it covers the fragment and a flat pass over
/// the rest. Whether the index is used is Lance's decision per fragment, so
/// rows written after the last index build are always candidates.
pub async fn ann(dataset: &Dataset, table: &VectorTable, query: &Query) -> Result<Vec<Hit>> {
    run(scanner(dataset, table, query, true)?, table).await
}

/// Fetch rows by key.
pub async fn get(
    dataset: &Dataset,
    table: &VectorTable,
    keys: &[String],
) -> Result<Vec<RecordBatch>> {
    let Some(key_column) = &table.key_column else {
        bail!("the table has no key column to look up");
    };
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let list = keys
        .iter()
        .map(|k| format!("'{}'", k.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    let mut scanner = dataset.scan();
    scanner
        .filter(&format!("`{}` in ({list})", key_column.replace('`', "``")))?
        .with_row_id();
    scanner
        .try_into_stream()
        .await?
        .try_collect()
        .await
        .context("scan by keys")
}

fn scanner(
    dataset: &Dataset,
    table: &VectorTable,
    query: &Query,
    use_index: bool,
) -> Result<Scanner> {
    if query.vector.len() != table.dimension {
        bail!(
            "query dim({}) does not match the table's dim({})",
            query.vector.len(),
            table.dimension
        );
    }
    let vector = Float32Array::from(query.vector.clone());
    let mut scanner = dataset.scan();
    scanner
        .nearest(&table.vector_column, &vector, query.k)?
        .distance_metric(query.distance)
        .use_index(use_index)
        .with_row_id();
    if let Some(filter) = &query.filter {
        scanner.filter(filter)?;
        scanner.prefilter(!query.postfilter);
    }
    if let Some(nprobes) = query.nprobes {
        scanner.nprobes(nprobes);
    }
    let mut columns = Vec::new();
    if let Some(key) = &table.key_column {
        columns.push(key.clone());
    }
    scanner.project(&columns)?;
    Ok(scanner)
}

async fn run(scanner: Scanner, table: &VectorTable) -> Result<Vec<Hit>> {
    let mut stream = scanner.try_into_stream().await?;
    let mut out = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        out.extend(hits(&batch, table)?);
    }
    Ok(out)
}

fn hits(batch: &RecordBatch, table: &VectorTable) -> Result<Vec<Hit>> {
    let distances = batch
        .column_by_name(DISTANCE_COLUMN)
        .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
        .context("the scan did not return a _distance column")?;
    let row_ids = batch
        .column_by_name(ROW_ID_COLUMN)
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .context("the scan did not return a _rowid column")?;
    let keys = table.key_column.as_ref().and_then(|name| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
    });
    if table.key_column.is_some() && keys.is_none() {
        bail!("key column did not come back as Utf8");
    }
    (0..batch.num_rows())
        .map(|i| {
            Ok(Hit {
                key: match keys {
                    Some(keys) if !keys.is_null(i) => keys.value(i).to_string(),
                    _ => row_ids.value(i).to_string(),
                },
                distance: distances.value(i),
                row_id: row_ids.value(i),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::VectorTable;
    use crate::testing::{DIM, write_dataset};
    use tempfile::TempDir;

    fn l2(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
    }

    #[tokio::test]
    async fn flat_returns_exact_top_k() {
        let dir = TempDir::new().unwrap();
        let vectors = vec![
            vec![1.0, 0.0, 0.0, 0.0],
            vec![0.9, 0.1, 0.0, 0.0],
            vec![0.0, 1.0, 0.0, 0.0],
            vec![0.0, 0.0, 1.0, 0.0],
        ];
        let dataset = write_dataset(&dir, &vectors, &["a", "b", "a", "b"]).await;
        let table = VectorTable::resolve(&dataset, None, None).unwrap();

        let query = Query {
            vector: vec![1.0, 0.0, 0.0, 0.0],
            k: 2,
            filter: None,
            postfilter: false,
            distance: DistanceType::L2,
            nprobes: None,
        };
        let hits = flat(&dataset, &table, &query).await.unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].key, "k0");
        assert!(hits[0].distance <= hits[1].distance);

        let mut brute: Vec<(String, f32)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (format!("k{i}"), l2(&query.vector, v)))
            .collect();
        brute.sort_by(|a, b| a.1.total_cmp(&b.1));
        for (hit, (key, distance)) in hits.iter().zip(brute.iter()) {
            assert_eq!(&hit.key, key);
            assert!((hit.distance - distance).abs() < 1e-5);
        }
    }

    #[tokio::test]
    async fn flat_applies_the_filter() {
        let dir = TempDir::new().unwrap();
        let vectors = vec![
            vec![1.0, 0.0, 0.0, 0.0],
            vec![0.9, 0.1, 0.0, 0.0],
            vec![0.0, 1.0, 0.0, 0.0],
        ];
        let dataset = write_dataset(&dir, &vectors, &["a", "b", "a"]).await;
        let table = VectorTable::resolve(&dataset, None, None).unwrap();

        let hits = flat(
            &dataset,
            &table,
            &Query {
                vector: vec![1.0, 0.0, 0.0, 0.0],
                k: 3,
                filter: Some("tag = 'a'".to_string()),
                postfilter: false,
                distance: DistanceType::L2,
                nprobes: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].key, "k0");
        assert_eq!(hits[1].key, "k2");
    }

    #[tokio::test]
    async fn get_fetches_rows_by_key() {
        let dir = TempDir::new().unwrap();
        let vectors = vec![vec![1.0; DIM], vec![2.0; DIM], vec![3.0; DIM]];
        let dataset = write_dataset(&dir, &vectors, &["a", "b", "c"]).await;
        let table = VectorTable::resolve(&dataset, None, None).unwrap();

        let batches = get(&dataset, &table, &["k0".to_string(), "k2".to_string()])
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
    }

    #[tokio::test]
    async fn query_rejects_a_wrong_dimension() {
        let dir = TempDir::new().unwrap();
        let vectors = vec![vec![1.0; DIM]];
        let dataset = write_dataset(&dir, &vectors, &["a"]).await;
        let table = VectorTable::resolve(&dataset, None, None).unwrap();
        let query = Query {
            vector: vec![1.0; DIM + 1],
            k: 1,
            filter: None,
            postfilter: false,
            distance: DistanceType::L2,
            nprobes: None,
        };
        assert!(flat(&dataset, &table, &query).await.is_err());
    }

    #[tokio::test]
    async fn ann_matches_flat_without_an_index() {
        let dir = TempDir::new().unwrap();
        let vectors = vec![
            vec![1.0, 0.0, 0.0, 0.0],
            vec![0.0, 1.0, 0.0, 0.0],
            vec![0.0, 0.0, 1.0, 0.0],
        ];
        let dataset = write_dataset(&dir, &vectors, &["a", "b", "c"]).await;
        let table = VectorTable::resolve(&dataset, None, None).unwrap();
        let query = Query {
            vector: vec![1.0, 0.0, 0.0, 0.0],
            k: 2,
            filter: None,
            postfilter: false,
            distance: DistanceType::L2,
            nprobes: None,
        };
        let indexed = ann(&dataset, &table, &query).await.unwrap();
        let exact = flat(&dataset, &table, &query).await.unwrap();
        assert_eq!(
            indexed.iter().map(|h| &h.key).collect::<Vec<_>>(),
            exact.iter().map(|h| &h.key).collect::<Vec<_>>()
        );
    }
}
