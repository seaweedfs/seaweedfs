//! A purge removes a deleted vector's bytes from storage: after it, no file
//! under the table holds the vector, its key or its metadata. Runs on a local
//! table, so it needs no cluster.

use std::path::Path;
use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::dataset::cleanup::{CleanupPolicy, RemovalStats, cleanup_old_versions};
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

/// A purge whose cleanup nothing holds back, so only the latest version
/// stays. [`purge::purge`] cleans up through the cleanup job's retention
/// policy, which a deployment may have keep more.
async fn purge_to_latest(
    dataset: &mut Dataset,
    token: &str,
) -> anyhow::Result<(usize, RemovalStats, bool)> {
    let rewritten = purge::rebuild(dataset, token).await?;
    let policy = CleanupPolicy {
        before_timestamp: Some(chrono::Utc::now()),
        before_version: Some(dataset.latest_version_id().await?),
        error_if_tagged_old_versions: true,
        ..Default::default()
    };
    let removed = cleanup_old_versions(dataset, policy).await?;
    Ok((rewritten, removed, purge::finish(dataset, token).await?))
}

#[tokio::test]
async fn purge_removes_a_deleted_vectors_bytes() {
    purge_leaves_no_trace(false).await;
}

/// Under stable row ids compaction leaves scalar indices as they are, so only
/// a rebuild takes the key out of them.
#[tokio::test]
async fn purge_removes_a_deleted_vectors_bytes_under_stable_row_ids() {
    purge_leaves_no_trace(true).await;
}

async fn purge_leaves_no_trace(stable_row_ids: bool) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("index.lance");
    let uri = uri.to_str().unwrap();
    let (schema, batch) = batch();
    let params = lance::dataset::WriteParams {
        enable_stable_row_ids: stable_row_ids,
        ..Default::default()
    };
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        uri,
        Some(params),
    )
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
    // A remapped bitmap index still lists the deleted row's value.
    dataset
        .create_index(
            &["metadata"],
            IndexType::Bitmap,
            Some("metadata_idx".to_string()),
            &lance_index::scalar::ScalarIndexParams::for_builtin(
                lance_index::scalar::BuiltinIndexType::Bitmap,
            ),
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
    let (rewritten, removed, served) = purge_to_latest(&mut dataset, &token).await.unwrap();
    assert!(served);
    assert!(rewritten >= 1);
    assert!(removed.old_versions >= 1, "{removed:?}");

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
    assert_eq!(reopened.load_indices().await.unwrap().len(), 3);
}

/// A version a query lease (a tag) pins still holds the deleted row, so the
/// purge must fail and leave the request pending rather than report success.
/// Once the tag goes, the next purge only cleans up: the rebuild is not done
/// again.
#[tokio::test]
async fn purge_refuses_while_a_tag_pins_old_data() {
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
    assert!(purge_to_latest(&mut dataset, &token).await.is_err());
    dataset.checkout_latest().await.unwrap();
    assert_eq!(purge::pending(dataset.config()), Some(token.clone()));

    let index = |dataset: &Dataset| {
        let dataset = dataset.clone();
        async move { dataset.load_indices().await.unwrap()[0].uuid }
    };
    let rebuilt = index(&dataset).await;
    dataset
        .tags()
        .delete("weed-vector-query-lease")
        .await
        .unwrap();
    let (rewritten, _, served) = purge_to_latest(&mut dataset, &token).await.unwrap();
    assert!(served);
    assert_eq!(rewritten, 0);
    assert_eq!(
        index(&dataset).await,
        rebuilt,
        "the index was rebuilt again"
    );
    assert_eq!(purge::pending(dataset.config()), None);
    assert_eq!(markers_in(dir.path()), Vec::<String>::new());
}

/// The request is served only once no version from before the rebuild is
/// left, whatever kept one.
#[tokio::test]
async fn purge_is_served_once_older_versions_are_gone() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("index.lance");
    let uri = uri.to_str().unwrap();
    let (schema, batch) = batch();
    let mut dataset = Dataset::write(RecordBatchIterator::new(vec![Ok(batch)], schema), uri, None)
        .await
        .unwrap();
    dataset
        .delete(&format!("key = '{KEY_MARKER}'"))
        .await
        .unwrap();
    let token = purge::request_purge(&mut dataset).await.unwrap();
    assert_eq!(purge::rebuild(&mut dataset, &token).await.unwrap(), 1);
    assert!(!purge::finish(&mut dataset, &token).await.unwrap());
    assert_eq!(purge::pending(dataset.config()), Some(token.clone()));

    let (rewritten, removed, served) = purge_to_latest(&mut dataset, &token).await.unwrap();
    assert!(served);
    assert_eq!(rewritten, 0);
    assert!(removed.old_versions >= 1, "{removed:?}");
    assert_eq!(purge::pending(dataset.config()), None);
}

/// A retrain builds every index name over the whole table, so shards of one
/// vector index would end up overlapping: the purge refuses before touching
/// the table and leaves the request pending.
#[tokio::test]
async fn purge_refuses_a_sharded_vector_index() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("index.lance");
    let uri = uri.to_str().unwrap();
    let (schema, batch) = batch();
    let params = lance::dataset::WriteParams {
        max_rows_per_file: ROWS / 2,
        ..Default::default()
    };
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        uri,
        Some(params),
    )
    .await
    .unwrap();
    let fragments: Vec<u32> = dataset
        .get_fragments()
        .iter()
        .map(|f| f.id() as u32)
        .collect();
    assert_eq!(fragments.len(), 2);
    for (shard, fragment) in fragments.iter().enumerate() {
        dataset
            .create_index_builder(
                &["vector"],
                IndexType::Vector,
                &VectorIndexParams::ivf_flat(2, DistanceType::L2),
            )
            .name(format!("shard-{shard}"))
            .fragments(vec![*fragment])
            .await
            .unwrap();
        dataset.checkout_latest().await.unwrap();
    }
    dataset
        .delete(&format!("key = '{KEY_MARKER}'"))
        .await
        .unwrap();
    let token = purge::request_purge(&mut dataset).await.unwrap();
    let version = dataset.version().version;

    let err = purge::purge(&mut dataset, &token).await.unwrap_err();
    assert!(format!("{err:#}").contains("sharded"), "{err:#}");
    dataset.checkout_latest().await.unwrap();
    assert_eq!(dataset.version().version, version, "the table was changed");
    assert_eq!(purge::pending(dataset.config()), Some(token));
}
