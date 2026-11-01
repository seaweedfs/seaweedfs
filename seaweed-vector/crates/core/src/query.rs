//! Top-K queries over a [`VectorTable`]: exact flat scan, or the ANN index
//! with a flat pass over the fragments it does not cover yet.

use anyhow::{Context, Result, bail};
use arrow_array::{Array, Float32Array, RecordBatch, StringArray, UInt64Array};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::scanner::Scanner;
use lance_linalg::distance::DistanceType;
use lance_table::format::Fragment;

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
    /// Restrict the indexed scan to these physical index segments (the
    /// Enterprise planner fans a query out across them). Without `fragments`,
    /// unindexed fragments outside the segments are not searched — the caller
    /// owns disjoint coverage of them. When `fragments` is also set the scan
    /// covers both, so a worker can take its segment share plus an
    /// unindexed-fragment sweep in one request.
    pub index_segments: Option<Vec<uuid::Uuid>>,
    /// Restrict the scan to these fragment ids — used by the planner to give
    /// a worker the unindexed-fragment sweep. Listed ids that no longer exist
    /// scan nothing.
    pub fragments: Option<Vec<u64>>,
}

/// One row of a top-K answer.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Hit {
    /// The key column value, or the row id when the table has no key column.
    pub key: String,
    /// Raw Lance `_distance` in the query's metric.
    pub distance: f32,
    pub row_id: u64,
}

/// Exact top-K over every fragment, ignoring any index.
pub async fn flat(dataset: &Dataset, table: &VectorTable, query: &Query) -> Result<Vec<Hit>> {
    run(scanner(dataset, table, query, false).await?, table).await
}

/// Top-K with the ANN index where it covers the fragment and a flat pass over
/// the rest. Whether the index is used is Lance's decision per fragment, so
/// rows written after the last index build are always candidates.
pub async fn ann(dataset: &Dataset, table: &VectorTable, query: &Query) -> Result<Vec<Hit>> {
    run(scanner(dataset, table, query, true).await?, table).await
}

/// Fetch rows by key, projected to `columns` (all of them when None).
pub async fn get(
    dataset: &Dataset,
    table: &VectorTable,
    keys: &[String],
    columns: Option<&[String]>,
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
    if let Some(columns) = columns {
        scanner.project(columns)?;
    }
    scanner
        .try_into_stream()
        .await?
        .try_collect()
        .await
        .context("scan by keys")
}

async fn scanner(
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
        crate::filter::apply(&mut scanner, filter)?;
        scanner.prefilter(!query.postfilter);
    }
    if let Some(nprobes) = query.nprobes {
        scanner.nprobes(nprobes);
    }
    let segments = query.index_segments.as_deref().unwrap_or(&[]);
    if use_index && !segments.is_empty() {
        scanner.with_index_segments(segments.to_vec())?;
    }
    // Lance intersects the fragment list with the selected segments, so a
    // scoped scan must name the segment-covered fragments itself: the universe
    // is the listed ids plus the live coverage of the selected segments. An
    // empty universe (or an empty-segment, fragment-less assignment) scopes to
    // nothing — `with_fragments` with an empty list scans nothing.
    let mut wanted: Option<Vec<u64>> = query.fragments.clone();
    if let Some(ids) = &mut wanted {
        if use_index && !segments.is_empty() {
            ids.extend(
                crate::segments::covered_fragments(dataset, &table.vector_column, segments).await?,
            );
        }
    } else if use_index && query.index_segments.as_ref().is_some_and(|s| s.is_empty()) {
        wanted = Some(Vec::new());
    }
    if let Some(ids) = wanted {
        let frags: Vec<Fragment> = dataset
            .fragments()
            .iter()
            .filter(|f| ids.contains(&f.id))
            .cloned()
            .collect();
        scanner.with_fragments(frags);
    }
    let mut columns = Vec::new();
    if let Some(key) = &table.key_column {
        columns.push(key.clone());
    }
    columns.push(DISTANCE_COLUMN.to_string());
    columns.push(ROW_ID_COLUMN.to_string());
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
            let key = match keys {
                Some(keys) if !keys.is_null(i) => keys.value(i).to_string(),
                Some(_) => bail!(
                    "key column {} has a null at row {i}",
                    table.key_column.as_deref().unwrap_or_default()
                ),
                None => row_ids.value(i).to_string(),
            };
            Ok(Hit {
                key,
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
    use arrow_array::{FixedSizeListArray, RecordBatchIterator, types::Float32Type};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
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
            index_segments: None,
            fragments: None,
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
                index_segments: None,
                fragments: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].key, "k0");
        assert_eq!(hits[1].key, "k2");
    }

    #[tokio::test]
    async fn filter_matches_json_types_exactly() {
        let dir = TempDir::new().unwrap();
        let metas = vec![
            r#"{"k": 3}"#,      // k0: number
            r#"{"k": "3"}"#,    // k1: numeric string
            r#"{"k": true}"#,   // k2: bool
            r#"{"k": "abc"}"#,  // k3: non-numeric string
            r#"{"x": 1}"#,      // k4: missing key
            r#"{"k": [1, 2]}"#, // k5: array
            r#"{"k": null}"#,   // k6: null
            r#"{"k": 1}"#,      // k7: serializes to '1', castable to boolean
            r#"{"k": 0}"#,      // k8: serializes to '0', castable to boolean
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    DIM as i32,
                ),
                false,
            ),
            Field::new("metadata", DataType::Utf8, true).with_metadata(
                [("ARROW:extension:name".to_string(), "arrow.json".to_string())]
                    .into_iter()
                    .collect(),
            ),
        ]));
        let keys: Vec<String> = (0..metas.len()).map(|i| format!("k{i}")).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        (0..metas.len())
                            .map(|i| Some(vec![Some(i as f32); DIM]))
                            .collect::<Vec<_>>(),
                        DIM as i32,
                    ),
                ),
                Arc::new(StringArray::from(
                    metas
                        .iter()
                        .map(|m| Some(m.to_string()))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        let dataset = Dataset::write(reader, dir.path().to_str().unwrap(), None)
            .await
            .unwrap();
        let table = VectorTable::resolve(&dataset, None, None).unwrap();
        async fn run(dataset: &Dataset, table: &VectorTable, filter: &str) -> Vec<Hit> {
            flat(
                dataset,
                table,
                &Query {
                    vector: vec![1.0, 0.0, 0.0, 0.0],
                    k: 10,
                    filter: Some(filter.to_string()),
                    postfilter: false,
                    distance: DistanceType::L2,
                    nprobes: None,
                    index_segments: None,
                    fragments: None,
                },
            )
            .await
            .unwrap()
        }
        let keys_of = |hits: Vec<Hit>| {
            let mut keys: Vec<String> = hits.into_iter().map(|h| h.key).collect();
            keys.sort();
            keys
        };

        // A stored string, boolean, array, or null must not satisfy a numeric
        // comparison — nor fail the query, the way the converting json_get_*
        // extractors do.
        let hits = run(
            &dataset,
            &table,
            "TRY_CAST(json_extract(`metadata`, '$[\"k\"]') AS DOUBLE) = 3",
        )
        .await;
        assert_eq!(keys_of(hits), ["k0"]);
        let hits = run(
            &dataset,
            &table,
            "TRY_CAST(json_extract(`metadata`, '$[\"k\"]') AS DOUBLE) > 1",
        )
        .await;
        assert_eq!(keys_of(hits), ["k0"]);

        // Booleans compare the serialized form as well: '1' and '0' would
        // satisfy a cast but not the text match.
        let hits = run(
            &dataset,
            &table,
            "json_extract(`metadata`, '$[\"k\"]') = 'true'",
        )
        .await;
        assert_eq!(keys_of(hits), ["k2"]);

        // The serialized extract is exact for strings too: the numeric 3 is
        // not a match for '"3"', and comparing json_extract never trips on
        // the array or null rows.
        let hits = run(
            &dataset,
            &table,
            "json_extract(`metadata`, '$[\"k\"]') = '\"3\"'",
        )
        .await;
        assert_eq!(keys_of(hits), ["k1"]);
        let hits = run(
            &dataset,
            &table,
            "NOT COALESCE(json_extract(`metadata`, '$[\"k\"]') = '\"3\"', false)",
        )
        .await;
        assert_eq!(
            keys_of(hits),
            ["k0", "k2", "k3", "k4", "k5", "k6", "k7", "k8"]
        );
    }

    #[tokio::test]
    async fn get_fetches_rows_by_key() {
        let dir = TempDir::new().unwrap();
        let vectors = vec![vec![1.0; DIM], vec![2.0; DIM], vec![3.0; DIM]];
        let dataset = write_dataset(&dir, &vectors, &["a", "b", "c"]).await;
        let table = VectorTable::resolve(&dataset, None, None).unwrap();

        let keys = ["k0".to_string(), "k2".to_string()];
        let batches = get(&dataset, &table, &keys, None).await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
        assert!(batches.iter().all(|b| b.column_by_name("vector").is_some()));

        let projected = get(&dataset, &table, &keys, Some(&["key".to_string()]))
            .await
            .unwrap();
        let mut got: Vec<String> = Vec::new();
        for batch in &projected {
            assert!(batch.column_by_name("vector").is_none());
            let column = batch.column_by_name("key").unwrap();
            let column = column.as_any().downcast_ref::<StringArray>().unwrap();
            got.extend(column.iter().flatten().map(String::from));
        }
        got.sort();
        assert_eq!(got, keys);
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
            index_segments: None,
            fragments: None,
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
            index_segments: None,
            fragments: None,
        };
        let indexed = ann(&dataset, &table, &query).await.unwrap();
        let exact = flat(&dataset, &table, &query).await.unwrap();
        assert_eq!(
            indexed.iter().map(|h| &h.key).collect::<Vec<_>>(),
            exact.iter().map(|h| &h.key).collect::<Vec<_>>()
        );
    }
}
