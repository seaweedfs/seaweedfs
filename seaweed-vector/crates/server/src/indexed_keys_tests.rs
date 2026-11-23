//! Indexed metadata keys through the VectorService: PutVectors derives the
//! columns, and a query answers the same with or without them.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use lance::Dataset;
use lance::dataset::{BatchUDF, NewColumnTransform};
use lance::index::DatasetIndexExt;
use lance_index::IndexType;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use tonic::Request;
use weed_lance_worker::metadata_index::{self, STATE_FIELD_META, State};
use weed_vector_core::query::{self, Query};
use weed_vector_core::table::VectorTable;

use crate::pb::vector::{self as pb, vector_service_server::VectorService};
use crate::service::VectorServer;

const DIM: usize = 8;
const ROWS: usize = 3000;
const COLUMN: &str = "_mk_0";

/// How the engine answers a filter once `author` is indexed.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Path {
    /// Through the column, at most the exact-match limit of rows: exact.
    Exact,
    /// Through the column, more rows: the vector index with the column's
    /// prefilter.
    Ivf,
    /// Not through the column.
    Json,
}

/// S3 Vectors filters, the gateway's translation of each, and its path.
const FILTERS: [(&str, &str, Path); 22] = [
    (
        "{\"author\":\"alice\"}",
        "(COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"alice\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"alice\"'), false))",
        Path::Ivf,
    ),
    (
        "{\"author\":{\"$eq\":\"bob\"}}",
        "(COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"bob\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"bob\"'), false))",
        Path::Exact,
    ),
    (
        "{\"author\":\"carol\"}",
        "(COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"carol\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"carol\"'), false))",
        Path::Exact,
    ),
    (
        "{\"author\":\"x\"}",
        "(COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"x\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"x\"'), false))",
        Path::Exact,
    ),
    (
        "{\"author\":\"7\"}",
        "(COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"7\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"7\"'), false))",
        Path::Exact,
    ),
    (
        "{\"author\":7}",
        "(COALESCE(TRY_CAST(json_extract(`metadata`, '$[\"author\"]') AS DOUBLE) = 7, false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '7'), false))",
        Path::Json,
    ),
    (
        "{\"author\":true}",
        "(COALESCE(json_extract(`metadata`, '$[\"author\"]') = 'true', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', 'true'), false))",
        Path::Json,
    ),
    (
        "{\"author\":{\"$in\":[\"alice\",\"bob\",\"x\"]}}",
        "(COALESCE(json_extract(`metadata`, '$[\"author\"]') in ('\"alice\"', '\"bob\"', '\"x\"'), false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"alice\"'), false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"bob\"'), false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"x\"'), false))",
        Path::Ivf,
    ),
    (
        "{\"author\":{\"$in\":[\"alice\",7]}}",
        "(COALESCE(json_extract(`metadata`, '$[\"author\"]') in ('\"alice\"'), false) OR COALESCE(TRY_CAST(json_extract(`metadata`, '$[\"author\"]') AS DOUBLE) in (7), false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"alice\"'), false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '7'), false))",
        Path::Json,
    ),
    (
        "{\"author\":{\"$ne\":\"alice\"}}",
        "NOT (COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"alice\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"alice\"'), false))",
        Path::Json,
    ),
    (
        "{\"author\":{\"$nin\":[\"alice\",\"bob\"]}}",
        "NOT (COALESCE(json_extract(`metadata`, '$[\"author\"]') in ('\"alice\"', '\"bob\"'), false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"alice\"'), false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"bob\"'), false))",
        Path::Json,
    ),
    (
        "{\"author\":{\"$exists\":false}}",
        "NOT json_exists(`metadata`, '$[\"author\"]')",
        Path::Json,
    ),
    (
        "{\"$and\":[{\"author\":\"alice\"},{\"genre\":\"g1\"}]}",
        "((COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"alice\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"alice\"'), false)) AND (COALESCE(json_extract(`metadata`, '$[\"genre\"]') = '\"g1\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"genre\"]', '\"g1\"'), false)))",
        Path::Ivf,
    ),
    (
        "{\"$and\":[{\"author\":\"bob\"},{\"year\":{\"$gt\":2005}}]}",
        "((COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"bob\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"bob\"'), false)) AND TRY_CAST(json_extract(`metadata`, '$[\"year\"]') AS DOUBLE) > 2005)",
        Path::Exact,
    ),
    (
        "{\"$or\":[{\"author\":\"bob\"},{\"genre\":\"g2\"}]}",
        "((COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"bob\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"bob\"'), false)) OR (COALESCE(json_extract(`metadata`, '$[\"genre\"]') = '\"g2\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"genre\"]', '\"g2\"'), false)))",
        Path::Json,
    ),
    (
        "{\"author\":\"o'neil \\\"q\\\" \\\\ \\n\"}",
        "(COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"o''neil \\\"q\\\" \\\\ \\n\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"o''neil \\\"q\\\" \\\\ \\n\"'), false))",
        Path::Exact,
    ),
    (
        "{\"author\":\"nobody\"}",
        "(COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"nobody\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"nobody\"'), false))",
        Path::Exact,
    ),
    ("{\"author\":{\"$in\":[]}}", "false", Path::Json),
    (
        "{\"genre\":\"g1\"}",
        "(COALESCE(json_extract(`metadata`, '$[\"genre\"]') = '\"g1\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"genre\"]', '\"g1\"'), false))",
        Path::Json,
    ),
    (
        "{\"author\":\"u5\"}",
        "(COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"u5\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"u5\"'), false))",
        Path::Exact,
    ),
    (
        "{\"author\":\"ctl\\u0001x\"}",
        "(COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"ctl\\u{1}x\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"ctl\\u0001x\"'), false))",
        Path::Json,
    ),
    (
        "{\"author\":{\"$in\":[\"u5\",\"u6\"]}}",
        "(COALESCE(json_extract(`metadata`, '$[\"author\"]') in ('\"u5\"', '\"u6\"'), false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"u5\"'), false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"u6\"'), false))",
        Path::Exact,
    ),
];

fn rng(seed: &mut u64) -> f32 {
    *seed = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    ((*seed >> 33) as f32 / (1u64 << 31) as f32) - 0.5
}

fn vector_of(i: usize) -> Vec<f32> {
    let mut centre = (i % 16) as u64 + 1;
    let mut noise = i as u64 * 7919 + 13;
    (0..DIM)
        .map(|_| rng(&mut centre) * 4.0 + rng(&mut noise))
        .collect()
}

fn author(i: usize) -> Option<serde_json::Value> {
    use serde_json::json;
    Some(match i % 100 {
        0..=9 => json!("alice"),
        10 => json!("bob"),
        11 => json!(["carol", "x"]),
        12 => json!(7),
        13 => json!("7"),
        14 => serde_json::Value::Null,
        15 => return None,
        16 => json!(true),
        17 => json!("o'neil \"q\" \\ \n"),
        18 => json!("ctl\u{1}x"),
        19 => json!(["alice", 3]),
        _ => json!(format!("u{}", i % 37)),
    })
}

fn row(i: usize, author: Option<serde_json::Value>) -> pb::Vector {
    let mut meta = serde_json::json!({"genre": format!("g{}", i % 3), "year": 2000 + i % 10});
    if let Some(author) = author {
        meta["author"] = author;
    }
    let serde_json::Value::Object(meta) = meta else {
        unreachable!()
    };
    pb::Vector {
        key: format!("k{i}"),
        data: vector_of(i),
        metadata: Some(crate::service::json_to_struct(meta)),
    }
}

struct Fixture {
    server: VectorServer,
    exact_off: VectorServer,
    table: pb::Table,
}

impl Fixture {
    async fn new() -> Self {
        let bucket = format!(
            "mk-{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let table = pb::Table {
            id: vec![bucket.clone(), "vector".into(), "x".into()],
            location: format!("shared-memory://{bucket}/vector/x"),
            ..Default::default()
        };
        let fixture = Self {
            server: VectorServer::new().with_schemes(&["shared-memory"]),
            exact_off: VectorServer::new()
                .with_schemes(&["shared-memory"])
                .with_exact_match_limit(Some(0)),
            table,
        };
        fixture
            .server
            .create_index(Request::new(pb::CreateIndexRequest {
                table: Some(fixture.table.clone()),
                dimension: DIM as u32,
                distance_metric: pb::DistanceMetric::L2 as i32,
                ..Default::default()
            }))
            .await
            .unwrap();
        fixture
    }

    async fn put(&self, vectors: Vec<pb::Vector>) {
        self.server
            .put_vectors(Request::new(pb::PutVectorsRequest {
                table: Some(self.table.clone()),
                vectors,
            }))
            .await
            .unwrap();
    }

    async fn dataset(&self, version: u64) -> Dataset {
        let dataset = Dataset::open(&self.table.location).await.unwrap();
        if version == 0 {
            dataset
        } else {
            dataset.checkout_version(version).await.unwrap()
        }
    }

    /// Adds `author`'s column, indexes it and marks it ready, returning the
    /// version before it was ready.
    async fn index_author(&self) -> u64 {
        let mut dataset = self.dataset(0).await;
        let output = Arc::new(Schema::new(vec![metadata_index::field(
            COLUMN,
            "author",
            State::Building,
        )]));
        let schema = output.clone();
        dataset
            .add_columns(
                NewColumnTransform::BatchUDF(BatchUDF {
                    mapper: Box::new(move |batch: &RecordBatch| {
                        let column = metadata_index::column_from_metadata(
                            batch.column_by_name("metadata").unwrap().as_ref(),
                            "author",
                        )
                        .map_err(|e| lance::Error::invalid_input(e.to_string()))?;
                        Ok(RecordBatch::try_new(schema.clone(), vec![column])?)
                    }),
                    output_schema: output,
                    result_checkpoint: None,
                }),
                Some(vec!["metadata".to_string()]),
                None,
            )
            .await
            .unwrap();
        dataset
            .create_index(
                &[COLUMN],
                IndexType::LabelList,
                Some(format!("{COLUMN}_idx")),
                &ScalarIndexParams::for_builtin(BuiltinIndexType::LabelList),
                true,
            )
            .await
            .unwrap();
        let building = dataset.version().version;
        self.set_state(State::Ready).await;
        building
    }

    async fn set_state(&self, state: State) -> u64 {
        let mut dataset = self.dataset(0).await;
        dataset
            .update_field_metadata()
            .update(COLUMN, [(STATE_FIELD_META, state.as_str())])
            .unwrap()
            .await
            .unwrap();
        dataset.version().version
    }

    async fn query(
        &self,
        server: &VectorServer,
        version: u64,
        filter: &str,
        k: u32,
        v: &[f32],
    ) -> Vec<(String, f32)> {
        let mut table = self.table.clone();
        table.version = version;
        server
            .query_vectors(Request::new(pb::QueryVectorsRequest {
                table: Some(table),
                query_vector: v.to_vec(),
                top_k: k,
                filter: filter.to_string(),
                return_distance: true,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
            .vectors
            .into_iter()
            .map(|r| (r.key, r.distance))
            .collect()
    }

    /// Exact filtered top-k at `version`.
    async fn flat(&self, version: u64, filter: &str, k: u32, v: &[f32]) -> Vec<(String, f32)> {
        let dataset = self.dataset(version).await;
        let table = VectorTable::resolve(&dataset, None, Some("key")).unwrap();
        query::flat(
            &dataset,
            &table,
            &Query {
                vector: v.to_vec(),
                k: k as usize,
                filter: Some(filter.to_string()),
                postfilter: false,
                distance: lance_linalg::distance::DistanceType::L2,
                nprobes: None,
                index_segments: None,
                fragments: None,
                exact_match_limit: None,
            },
        )
        .await
        .unwrap()
        .into_iter()
        .map(|h| (h.key, h.distance))
        .collect()
    }

    /// Every filter answers through the column as through the JSON at
    /// `json` (the same rows and indexes, the column not ready), and an
    /// exact answer as the exact filtered top-k.
    async fn compare(&self, json: u64, ready: u64) {
        for q in [7, 1234] {
            let v = vector_of(q);
            for (s3, filter, path) in FILTERS {
                for k in [10, 400] {
                    let what = format!("{s3} k={k} q={q}");
                    let exact = self.flat(json, filter, k, &v).await;
                    assert_same(&self.flat(ready, filter, k, &v).await, &exact, &what);
                    let json_ann = self.query(&self.server, json, filter, k, &v).await;
                    let off = self.query(&self.exact_off, ready, filter, k, &v).await;
                    assert_same(&off, &json_ann, &format!("{what}: indexed prefilter"));
                    let got = self.query(&self.server, ready, filter, k, &v).await;
                    let want = if path == Path::Exact {
                        &exact
                    } else {
                        &json_ann
                    };
                    assert_same(&got, want, &format!("{what}: {path:?}"));
                }
            }
        }
    }
}

fn assert_same(got: &[(String, f32)], want: &[(String, f32)], what: &str) {
    assert_eq!(got.len(), want.len(), "{what}: {got:?} vs {want:?}");
    // Ties may come back in either order: the same keys, and the same
    // distance at each rank.
    fn keys(hits: &[(String, f32)]) -> Vec<&str> {
        let mut keys: Vec<&str> = hits.iter().map(|(k, _)| k.as_str()).collect();
        keys.sort();
        keys
    }
    assert_eq!(keys(got), keys(want), "{what}: {got:?} vs {want:?}");
    for (g, w) in got.iter().zip(want) {
        let same = (g.1 - w.1).abs() < 1e-4 || (g.1.is_infinite() && w.1.is_infinite());
        assert!(same, "{what}: {got:?} vs {want:?}");
        if g.0 != w.0 {
            let tie = want
                .iter()
                .any(|(k, d)| k == &g.0 && (d - w.1).abs() < 1e-4);
            assert!(
                tie,
                "{what}: {} at {} is no tie: {got:?} vs {want:?}",
                g.0, g.1
            );
        }
    }
}

/// Indexed and JSON answers agree on mixed types, missing keys, arrays,
/// null, untouched operators, and after updates, deletes and rows no
/// scalar or vector index covers yet.
#[tokio::test]
async fn indexed_answers_match_the_json_path() {
    let fixture = Fixture::new().await;
    for start in (0..ROWS).step_by(300) {
        fixture
            .put((start..start + 300).map(|i| row(i, author(i))).collect())
            .await;
    }
    let mut dataset = fixture.dataset(0).await;
    weed_lance_worker::vector_index::build(&mut dataset, "vector", "vector_idx", 16, false)
        .await
        .unwrap();
    let json = fixture.index_author().await;
    let schema: Schema = fixture.dataset(0).await.schema().into();
    assert_eq!(
        metadata_index::indexed_keys(&schema)[0].state,
        Some(State::Ready)
    );
    fixture.compare(json, 0).await;

    // Updates move rows between values, deletes drop some, and new rows land
    // in fragments neither index covers; PutVectors fills the column.
    fixture
        .put(
            (0..60)
                .map(|i| row(i, Some(serde_json::json!(["moved", "bob"]))))
                .chain((ROWS..ROWS + 200).map(|i| row(i, author(i))))
                .collect(),
        )
        .await;
    fixture
        .server
        .delete_vectors(Request::new(pb::DeleteVectorsRequest {
            table: Some(fixture.table.clone()),
            keys: (100..130).map(|i| format!("k{i}")).collect(),
        }))
        .await
        .unwrap();
    let json = fixture.set_state(State::Building).await;
    let ready = fixture.set_state(State::Ready).await;
    fixture.compare(json, ready).await;
    let moved = r#"(COALESCE(json_extract(`metadata`, '$["author"]') = '"moved"', false) OR COALESCE(json_array_contains(`metadata`, '$["author"]', '"moved"'), false))"#;
    let hits = fixture
        .query(&fixture.server, ready, moved, 100, &vector_of(0))
        .await;
    assert_eq!(hits.len(), 60);
}

/// The planner counts and scores per scope: an exact answer over a fragment
/// subset is that subset's exact top-k.
#[tokio::test]
async fn a_scoped_query_is_exact_within_its_fragments() {
    let fixture = Fixture::new().await;
    for start in (0..ROWS).step_by(300) {
        fixture
            .put((start..start + 300).map(|i| row(i, author(i))).collect())
            .await;
    }
    let mut dataset = fixture.dataset(0).await;
    weed_lance_worker::vector_index::build(&mut dataset, "vector", "vector_idx", 16, false)
        .await
        .unwrap();
    let json = fixture.index_author().await;
    let ready = fixture.dataset(0).await;
    let table = VectorTable::resolve(&ready, None, Some("key")).unwrap();
    let ids: Vec<u64> = ready.fragments().iter().map(|f| f.id).take(4).collect();
    let filter = FILTERS[1].1;
    let scoped = || Query {
        vector: vector_of(5),
        k: 10,
        filter: Some(filter.to_string()),
        postfilter: false,
        distance: lance_linalg::distance::DistanceType::L2,
        nprobes: None,
        index_segments: None,
        fragments: Some(ids.clone()),
        exact_match_limit: None,
    };
    let got = query::ann(&ready, &table, &scoped()).await.unwrap();
    let old = fixture.dataset(json).await;
    let want = query::flat(&old, &table, &scoped()).await.unwrap();
    assert_eq!(want.len(), 10);
    let pairs = |hits: Vec<weed_vector_core::Hit>| {
        hits.into_iter()
            .map(|h| (h.key, h.distance))
            .collect::<Vec<_>>()
    };
    assert_same(&pairs(got), &pairs(want), "scoped bob");
}

/// The engine counts each index's filtered queries by key.
#[tokio::test]
async fn queries_are_counted_by_filter_key() {
    let fixture = Fixture::new().await;
    fixture
        .put((0..50).map(|i| row(i, author(i))).collect())
        .await;
    for (_, filter, _) in &FILTERS[..3] {
        fixture
            .query(&fixture.server, 0, filter, 5, &vector_of(1))
            .await;
    }
    let year = FILTERS[13].1;
    fixture
        .query(&fixture.server, 0, year, 5, &vector_of(1))
        .await;
    let stats = fixture
        .server
        .filter_stats(Request::new(pb::FilterStatsRequest {
            table: Some(fixture.table.clone()),
        }))
        .await
        .unwrap()
        .into_inner();
    let author = stats.keys.iter().find(|k| k.key == "author").unwrap();
    assert_eq!(author.queries, 4);
    assert!(author.string_operand && !author.number_operand);
    let year = stats.keys.iter().find(|k| k.key == "year").unwrap();
    assert_eq!(year.queries, 1);
    assert!(year.number_operand && !year.string_operand);
}

/// A PutVectors that opened the table before its key column was added and
/// commits after: Lance rebases the write and its rows hold NULL in the new
/// column, which the write then fills from the rows' metadata.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_that_raced_the_column_add_fills_the_column() {
    let fixture = Fixture::new().await;
    fixture
        .put((0..300).map(|i| row(i, author(i))).collect())
        .await;
    let unhealed = fixture.dataset(0).await;
    let healed = fixture.dataset(0).await;
    fixture.index_author().await;
    let nulls = |dataset: Dataset| async move {
        dataset
            .count_rows(Some(format!("{COLUMN} IS NULL")))
            .await
            .unwrap()
    };
    let rows: Vec<pb::Vector> = (250..400).map(|i| row(i, author(i))).collect();

    let written = crate::service::merge_vectors(unhealed, &rows)
        .await
        .unwrap();
    assert!(written.schema().field(COLUMN).is_some());
    assert!(nulls(written).await >= 100, "the race is reproduced");

    let written = crate::service::write_vectors(healed, &rows).await.unwrap();
    assert_eq!(nulls(written.clone()).await, 0);
    let filter = FILTERS[1].1;
    let latest = written.version().version;
    let v = vector_of(5);
    assert_same(
        &fixture
            .query(&fixture.server, latest, filter, 400, &v)
            .await,
        &fixture.flat(latest, filter, 400, &v).await,
        "bob after the race",
    );
    assert_eq!(fixture.flat(latest, filter, 400, &v).await.len(), 4);
}

/// A write names no version: one at a version from before a key's column
/// add would commit rows without the column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_at_an_older_version_is_refused() {
    let fixture = Fixture::new().await;
    fixture
        .put((0..10).map(|i| row(i, author(i))).collect())
        .await;
    let before = fixture.dataset(0).await.version().version;
    fixture.index_author().await;
    let mut table = fixture.table.clone();
    table.version = before;
    let refused = fixture
        .server
        .put_vectors(Request::new(pb::PutVectorsRequest {
            table: Some(table),
            vectors: (10..20).map(|i| row(i, author(i))).collect(),
        }))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
}

/// Two writes updating the same keys raced the column add, and the first
/// one's fill commits last: each write's rows get the values of its own
/// metadata, not the ones derived before it or from the other write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_that_raced_the_column_add_derives_its_updated_rows() {
    let fixture = Fixture::new().await;
    fixture
        .put((0..300).map(|i| row(i, author(i))).collect())
        .await;
    let first = fixture.dataset(0).await;
    let second = fixture.dataset(0).await;
    let written: Schema = first.schema().into();
    fixture.index_author().await;
    let rows = |name: &str| -> Vec<pb::Vector> {
        (250..300)
            .map(|i| row(i, Some(serde_json::json!(name))))
            .collect()
    };
    let keys: Vec<String> = (250..300).map(|i| format!("k{i}")).collect();

    let first = crate::service::merge_vectors(first, &rows("alice"))
        .await
        .unwrap();
    let second = crate::service::merge_vectors(second, &rows("bob"))
        .await
        .unwrap();
    crate::service::heal_columns(second, &written, &keys)
        .await
        .unwrap();
    crate::service::heal_columns(first, &written, &keys)
        .await
        .unwrap();

    let latest = fixture.dataset(0).await;
    let count = |filter: String| {
        let latest = latest.clone();
        async move { latest.count_rows(Some(filter)).await.unwrap() }
    };
    assert_eq!(count(format!("{COLUMN} IS NULL")).await, 0);
    for (name, filter) in [("alice", FILTERS[0].1), ("bob", FILTERS[1].1)] {
        assert_eq!(
            count(format!("array_has({COLUMN}, '{name}')")).await,
            count(filter.to_string()).await,
            "{name}"
        );
    }
    let mut got = fixture
        .server
        .get_vectors(Request::new(pb::GetVectorsRequest {
            table: Some(fixture.table.clone()),
            keys: keys.clone(),
            return_data: true,
            return_metadata: true,
        }))
        .await
        .unwrap()
        .into_inner()
        .vectors;
    got.sort_by_key(|v| v.key[1..].parse::<usize>().unwrap());
    assert_eq!(got, rows("bob"));
}

/// Operators ask through the engine; the table records the request and,
/// once the worker has done its part, the key's state, coverage and size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_are_requested_and_listed_through_the_engine() {
    let fixture = Fixture::new().await;
    fixture
        .put((0..600).map(|i| row(i, author(i))).collect())
        .await;
    let update = |key: &str, remove: bool| {
        fixture
            .server
            .update_metadata_key(Request::new(pb::UpdateMetadataKeyRequest {
                table: Some(fixture.table.clone()),
                key: key.to_string(),
                remove,
            }))
    };
    let listed = update("author", false).await.unwrap().into_inner();
    assert_eq!(listed.keys.len(), 1);
    assert_eq!(listed.keys[0].state, "requested");
    assert_eq!(listed.keys[0].rows, 600);
    // A retry of a committed request answers as the first one did.
    let again = update("author", false).await.unwrap().into_inner();
    assert_eq!(again.keys.len(), 1);
    assert_eq!(again.keys[0].state, "requested");

    let mut dataset = fixture.dataset(0).await;
    let key = metadata_index::add_column(&mut dataset, "author")
        .await
        .unwrap();
    metadata_index::finish_after(&mut dataset, &key, 1_000_000, std::time::Duration::ZERO)
        .await
        .unwrap();
    let listed = fixture
        .server
        .metadata_keys(Request::new(pb::MetadataKeysRequest {
            table: Some(fixture.table.clone()),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((listed.rows, listed.vector_index.as_str()), (600, ""));
    let author = &listed.keys[0];
    assert_eq!(
        (
            author.key.as_str(),
            author.state.as_str(),
            author.column.as_str()
        ),
        ("author", "ready", "_mk_0")
    );
    assert_eq!(
        (author.indexed_rows, author.rows, author.segments),
        (600, 600, 1)
    );
    assert!(author.index_bytes > 0);

    // Off at once: the next query reads the JSON again.
    let listed = update("author", true).await.unwrap().into_inner();
    assert_eq!(listed.keys[0].state, "off");
    let dataset = fixture.dataset(0).await;
    assert_eq!(metadata_index::keys_of(&dataset)[0].state, Some(State::Off));
    // Removing a key that is not indexed needs nothing.
    let nobody = update("nobody", true).await.unwrap().into_inner();
    assert_eq!(nobody.keys.len(), 1);
    assert_eq!(
        update("author", false).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition,
        "being removed"
    );
}

/// DeleteIndex purges the table's directory and an index created again
/// under the same name reuses it: the same running engine serves nothing of
/// the old table, neither rows, nor its indexed key, nor its filter stats.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_and_recreated_index_serves_nothing_old() {
    let fixture = Fixture::new().await;
    fixture
        .put((0..600).map(|i| row(i, author(i))).collect())
        .await;
    let mut dataset = fixture.dataset(0).await;
    weed_lance_worker::vector_index::build(&mut dataset, "vector", "vector_idx", 4, false)
        .await
        .unwrap();
    fixture.index_author().await;
    let bob = FILTERS[1].1;
    let old = fixture
        .query(&fixture.server, 0, bob, 50, &vector_of(10))
        .await;
    assert_eq!(old.len(), 6);
    let server = &fixture.server;
    let stats = || {
        server.filter_stats(Request::new(pb::FilterStatsRequest {
            table: Some(fixture.table.clone()),
        }))
    };
    assert_eq!(stats().await.unwrap().into_inner().keys[0].queries, 1);

    // What the catalog's DeleteTable does, then CreateIndex again.
    let (store, path) = lance::io::ObjectStore::from_uri(&fixture.table.location)
        .await
        .unwrap();
    store.remove_dir_all(path).await.unwrap();
    fixture
        .server
        .create_index(Request::new(pb::CreateIndexRequest {
            table: Some(fixture.table.clone()),
            dimension: DIM as u32,
            distance_metric: pb::DistanceMetric::L2 as i32,
            ..Default::default()
        }))
        .await
        .unwrap();
    let keys = fixture
        .server
        .metadata_keys(Request::new(pb::MetadataKeysRequest {
            table: Some(fixture.table.clone()),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(keys.keys.is_empty());
    assert!(
        stats().await.unwrap().into_inner().keys.is_empty(),
        "the old table's filter stats"
    );

    fixture
        .put((1000..1100).map(|i| row(i, author(i))).collect())
        .await;
    let new = |hits: &[(String, f32)]| {
        hits.iter().all(|(k, _)| {
            k[1..]
                .parse::<usize>()
                .is_ok_and(|i| (1000..1100).contains(&i))
        })
    };
    let unfiltered = fixture
        .query(&fixture.server, 0, "", 200, &vector_of(10))
        .await;
    assert_eq!(unfiltered.len(), 100);
    assert!(new(&unfiltered), "{unfiltered:?}");
    let bob_now = fixture
        .query(&fixture.server, 0, bob, 50, &vector_of(10))
        .await;
    assert_eq!(bob_now.len(), 1);
    assert!(new(&bob_now), "{bob_now:?}");
    let author = stats().await.unwrap().into_inner().keys;
    assert_eq!((author.len(), author[0].queries), (1, 1));
}

#[tokio::test]
async fn indexed_queries_acknowledge_coverage_and_skip_recent_writes() {
    let fixture = Fixture::new().await;
    fixture
        .put(
            (0..64)
                .map(|i| row(i, Some(serde_json::json!("alice"))))
                .collect(),
        )
        .await;
    fixture.index_author().await;
    let mut dataset = fixture.dataset(0).await;
    weed_vector_core::index::build(
        &mut dataset,
        "vector",
        weed_vector_core::index::INDEX_NAME,
        4,
        false,
    )
    .await
    .unwrap();
    fixture
        .put(vec![row(100, Some(serde_json::json!("alice")))])
        .await;
    let request = pb::QueryVectorsRequest {
        table: Some(fixture.table.clone()),
        query_vector: vector_of(100),
        top_k: 10,
        filter: FILTERS[0].1.to_string(),
        return_distance: true,
        return_metadata: true,
        ..Default::default()
    };
    let all = fixture
        .server
        .query_vectors(Request::new(request.clone()))
        .await
        .unwrap();
    assert_eq!(all.get_ref().vectors[0].key, "k100");
    let indexed = fixture
        .server
        .query_indexed_vectors(Request::new(request.clone()))
        .await
        .unwrap();
    assert_eq!(
        indexed
            .metadata()
            .get("x-seaweedfs-vector-search-mode")
            .unwrap(),
        "indexed-only"
    );
    assert_eq!(
        indexed
            .metadata()
            .get("x-seaweedfs-vector-excluded-fragments")
            .unwrap(),
        "1"
    );
    assert_eq!(indexed.get_ref().vectors.len(), 10);
    assert!(
        indexed
            .get_ref()
            .vectors
            .iter()
            .all(|v| v.key != "k100" && v.metadata.is_some())
    );
    let mut dataset = fixture.dataset(0).await;
    dataset
        .optimize_indices(&lance_index::optimize::OptimizeOptions::append())
        .await
        .unwrap();
    let indexed = fixture
        .server
        .query_indexed_vectors(Request::new(request.clone()))
        .await
        .unwrap();
    assert_eq!(indexed.get_ref().vectors[0].key, "k100");
    assert_eq!(
        indexed
            .metadata()
            .get("x-seaweedfs-vector-excluded-fragments")
            .unwrap(),
        "0"
    );
    fixture.set_state(State::Building).await;
    let err = fixture
        .server
        .query_indexed_vectors(Request::new(request))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}
