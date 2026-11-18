//! A purge removes a deleted vector's bytes from storage: after it, no file
//! under the table holds the vector, its key or its metadata. Runs on a local
//! table, so it needs no cluster.

use std::path::Path;
use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::dataset::cleanup::{CleanupPolicy, cleanup_old_versions};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_index::IndexType;
use lance_linalg::distance::DistanceType;
use weed_lance_worker::jobs::purge;

const DIM: i32 = 8;
const ROWS: usize = 300;
const MARKER_ROW: usize = 137;
const KEY_MARKER: &str = "purge-key-5f0c2e9a7d41b3c8";
const METADATA_MARKER: &str = "purge-metadata-a93e1d5b77c2f046";

fn marker_vector() -> Vec<f32> {
    // Values no other row has, so their bytes identify the row.
    (0..DIM).map(|i| 31_337.125 + i as f32 * 0.5).collect()
}

fn batch() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), DIM),
            false,
        ),
        Field::new("metadata", DataType::Utf8, true),
    ]));
    let keys: Vec<String> = (0..ROWS)
        .map(|i| {
            if i == MARKER_ROW {
                KEY_MARKER.to_string()
            } else {
                format!("k{i}")
            }
        })
        .collect();
    let metadata: Vec<String> = (0..ROWS)
        .map(|i| {
            if i == MARKER_ROW {
                format!("{{\"note\":\"{METADATA_MARKER}\"}}")
            } else {
                format!("{{\"n\":{i}}}")
            }
        })
        .collect();
    let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        (0..ROWS).map(|i| {
            let values: Vec<f32> = if i == MARKER_ROW {
                marker_vector()
            } else {
                (0..DIM)
                    .map(|d| ((i * 7 + d as usize) % 97) as f32)
                    .collect()
            };
            Some(values.into_iter().map(Some).collect::<Vec<_>>())
        }),
        DIM,
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(vectors),
            Arc::new(StringArray::from(metadata)),
        ],
    )
    .unwrap();
    (schema, batch)
}

fn vector_bytes() -> Vec<u8> {
    marker_vector()
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect()
}

/// Each marker found under `dir`, as "<marker> in <file>".
fn markers_in(dir: &Path) -> Vec<String> {
    let vector = vector_bytes();
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in std::fs::read_dir(&path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                stack.push(entry.path());
                continue;
            }
            let bytes = std::fs::read(entry.path()).unwrap();
            let has = |needle: &[u8]| bytes.windows(needle.len()).any(|w| w == needle);
            for (name, needle) in [
                ("vector", vector.as_slice()),
                ("key", KEY_MARKER.as_bytes()),
                ("metadata", METADATA_MARKER.as_bytes()),
            ] {
                if has(needle) {
                    let file = entry.path();
                    let file = file.strip_prefix(dir).unwrap_or(&file);
                    found.push(format!("{name} in {}", file.display()));
                }
            }
        }
    }
    found.sort_unstable();
    found
}

#[tokio::test]
async fn purge_removes_a_deleted_vectors_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("index.lance");
    let uri = uri.to_str().unwrap();
    let (schema, batch) = batch();
    let mut dataset = Dataset::write(RecordBatchIterator::new(vec![Ok(batch)], schema), uri, None)
        .await
        .unwrap();
    dataset
        .create_index(
            &["vector"],
            IndexType::Vector,
            Some("vector_idx".to_string()),
            &VectorIndexParams::ivf_flat(2, DistanceType::L2),
            false,
        )
        .await
        .unwrap();
    dataset
        .create_index(
            &["key"],
            IndexType::BTree,
            Some("key_idx".to_string()),
            &lance_index::scalar::ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();
    dataset.checkout_latest().await.unwrap();

    let before = markers_in(dir.path());
    assert!(
        before.iter().any(|m| m.starts_with("vector ")),
        "the vector's bytes should be found before deletion, found {before:?}"
    );

    dataset
        .delete(&format!("key = '{KEY_MARKER}'"))
        .await
        .unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), ROWS - 1);

    // A deletion alone, and the default retention cleanup, leave the bytes:
    // the fragment still holds the row and older versions still reference it.
    cleanup_old_versions(
        &dataset,
        CleanupPolicy {
            before_timestamp: Some(chrono::Utc::now() - chrono::Duration::hours(168)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        markers_in(dir.path())
            .iter()
            .any(|m| m.starts_with("vector ")),
        "a deleted vector is expected to stay until purged"
    );

    let token = purge::request_purge(&mut dataset).await.unwrap();
    assert_eq!(purge::pending(dataset.config()), Some(token.clone()));
    let stats = purge::purge(&mut dataset, &token).await.unwrap();
    assert!(stats.fragments_rewritten >= 1, "{stats:?}");
    assert!(stats.old_versions >= 1, "{stats:?}");

    assert_eq!(
        markers_in(dir.path()),
        Vec::<String>::new(),
        "deleted vector's bytes remain in storage after the purge"
    );
    assert_eq!(purge::pending(dataset.config()), None);

    // The table is intact: every other row, and the index, still serve.
    let reopened = Dataset::open(uri).await.unwrap();
    assert_eq!(reopened.count_rows(None).await.unwrap(), ROWS - 1);
    assert_eq!(purge::pending(reopened.config()), None);
    assert_eq!(reopened.load_indices().await.unwrap().len(), 2);
}

/// A version a query lease (a tag) pins still holds the deleted row, so the
/// purge must fail and leave the request pending rather than report success.
#[tokio::test]
async fn purge_refuses_while_a_tag_pins_old_data() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("index.lance");
    let uri = uri.to_str().unwrap();
    let (schema, batch) = batch();
    let mut dataset = Dataset::write(RecordBatchIterator::new(vec![Ok(batch)], schema), uri, None)
        .await
        .unwrap();
    let pinned = dataset.version().version;
    dataset
        .tags()
        .create(
            "weed-vector-query-lease",
            lance::dataset::refs::Ref::Version(None, Some(pinned)),
        )
        .await
        .unwrap();
    dataset
        .delete(&format!("key = '{KEY_MARKER}'"))
        .await
        .unwrap();
    let token = purge::request_purge(&mut dataset).await.unwrap();
    assert!(purge::purge(&mut dataset, &token).await.is_err());
    dataset.checkout_latest().await.unwrap();
    assert_eq!(purge::pending(dataset.config()), Some(token));
}
