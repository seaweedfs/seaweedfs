//! Drives the compaction handler against a live namespace.
//!
//! Skipped unless WEED_LANCE_NAMESPACE names one, the way the Go integration
//! tests skip without Docker: compaction rewrites real files, and there is
//! nothing to learn from it against a fake.

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Result;
use seaweed_worker_core::pb::{
    config_value::Kind, ConfigValue, DetectionComplete, DetectionProposals, ExecuteJobRequest,
    JobCompleted, JobProgressUpdate, JobProposal, JobSpec, RunDetectionRequest,
};
use seaweed_worker_core::{DetectionSender, ExecutionSender, JobHandler, PreviewProvider};
use weed_lance_worker::catalog::NamespaceClient;
use weed_lance_worker::jobs::cleanup::CleanupVersionsHandler;
use weed_lance_worker::jobs::compact::{CompactHandler, JOB_TYPE};
use weed_lance_worker::jobs::indices::OptimizeIndicesHandler;
use weed_lance_worker::preview::LancePreview;

#[derive(Default)]
struct Recorder {
    proposals: Mutex<Vec<JobProposal>>,
    observations: Mutex<Vec<seaweed_worker_core::pb::ObjectObservation>>,
    completed: Mutex<Vec<JobCompleted>>,
}

impl DetectionSender for Recorder {
    fn send_proposals(&self, proposals: DetectionProposals) -> Result<()> {
        self.proposals.lock().unwrap().extend(proposals.proposals);
        Ok(())
    }
    fn send_complete(&self, _complete: DetectionComplete) -> Result<()> {
        Ok(())
    }
    fn send_activity(&self, _activity: seaweed_worker_core::pb::ActivityEvent) -> Result<()> {
        Ok(())
    }
    fn send_observations(
        &self,
        observations: seaweed_worker_core::pb::WorkerObservations,
    ) -> Result<()> {
        self.observations
            .lock()
            .unwrap()
            .extend(observations.observations);
        Ok(())
    }
}

impl ExecutionSender for Recorder {
    fn send_progress(&self, _progress: JobProgressUpdate) -> Result<()> {
        Ok(())
    }
    fn send_completed(&self, completed: JobCompleted) -> Result<()> {
        self.completed.lock().unwrap().push(completed);
        Ok(())
    }
}

fn namespace_url() -> Option<String> {
    std::env::var("WEED_LANCE_NAMESPACE")
        .ok()
        .filter(|s| !s.is_empty())
}

fn int_config(name: &str, value: i64) -> HashMap<String, ConfigValue> {
    let mut values = HashMap::new();
    values.insert(
        name.to_string(),
        ConfigValue {
            kind: Some(Kind::Int64Value(value)),
        },
    );
    values
}

/// Declares a table through the namespace and writes `fragments` one-row
/// appends into it, so a test brings its own state instead of depending on
/// whatever a previous run left behind.
async fn seed_fragmented_table(url: &str, name: &str, fragments: usize) -> Result<String> {
    seed_table(url, name, fragments, 1, false).await
}

/// Writes `batches` appends of `rows_each` into a freshly declared table, and
/// optionally builds a vector index after the first batch so the later ones are
/// rows no index covers.
async fn seed_table(
    url: &str,
    name: &str,
    batches: usize,
    rows_each: usize,
    with_index: bool,
) -> Result<String> {
    use arrow_array::{
        FixedSizeListArray, Float32Array, Int64Array, RecordBatch, RecordBatchIterator,
    };
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::{Dataset, WriteMode, WriteParams};
    use lance::io::{ObjectStoreParams, StorageOptionsAccessor};
    use std::sync::Arc;

    // Declaring is the namespace's job, not the worker's, so the test asks for
    // it directly rather than widening the client the worker uses. The bucket and
    // namespace come first: a table cannot be declared under a parent that does
    // not exist, and a test that assumes one is a test that only passes twice.
    let http = reqwest::Client::new();
    for parent in ["vec", "vec$ml"] {
        http.post(format!("{url}/v1/namespace/{parent}/create"))
            .json(&serde_json::json!({"mode": "EXIST_OK"}))
            .send()
            .await?
            .error_for_status()?;
    }
    let encoded = format!("vec$ml${name}");
    http.post(format!("{url}/v1/table/{encoded}/declare"))
        .json(&serde_json::json!({}))
        .send()
        .await?
        .error_for_status()?;

    let client = NamespaceClient::new(url.to_string());
    let id = vec!["vec".to_string(), "ml".to_string(), name.to_string()];
    let description = client.describe_table(&id).await?;

    let mut options = description.storage_options.clone();
    options.extend(fallback());
    const DIM: i32 = 16;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), DIM),
            false,
        ),
    ]));

    for i in 0..batches {
        let ids: Vec<i64> = (0..rows_each).map(|r| (i * rows_each + r) as i64).collect();
        let values: Vec<f32> = ids
            .iter()
            .flat_map(|id| (0..DIM).map(move |d| (*id as f32) + d as f32))
            .collect();
        let vectors = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            DIM,
            Arc::new(Float32Array::from(values)),
            None,
        );
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(ids)), Arc::new(vectors)],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let params = WriteParams {
            mode: if i == 0 {
                WriteMode::Overwrite
            } else {
                WriteMode::Append
            },
            store_params: Some(ObjectStoreParams {
                storage_options_accessor: Some(std::sync::Arc::new(
                    StorageOptionsAccessor::with_static_options(options.clone()),
                )),
                ..Default::default()
            }),
            ..Default::default()
        };
        let dataset = Dataset::write(reader, description.location.as_str(), Some(params)).await?;

        // The index is built after the first batch, so everything appended
        // afterwards is a row it does not cover.
        if with_index && i == 0 {
            use lance::index::vector::VectorIndexParams;
            use lance::index::DatasetIndexExt;
            use lance_index::vector::{ivf::IvfBuildParams, pq::PQBuildParams};
            use lance_index::IndexType;

            let mut dataset = dataset;
            let params = VectorIndexParams::with_ivf_pq_params(
                lance_linalg::distance::MetricType::L2,
                IvfBuildParams::new(1),
                PQBuildParams::new(4, 8),
            );
            dataset
                .create_index(&["vec"], IndexType::Vector, None, &params, true)
                .await?;
        }
    }
    Ok(encoded)
}

/// A table with more fragments than the policy allows is proposed, and running
/// the proposal leaves it with fewer than it started with.
#[tokio::test]
async fn compacts_a_fragmented_table() {
    let Some(url) = namespace_url() else {
        eprintln!("WEED_LANCE_NAMESPACE is unset, skipping");
        return;
    };
    let mut fallback = weed_lance_worker::dataset::FallbackOptions::new();
    fallback.insert("aws_access_key_id".to_string(), "any".to_string());
    fallback.insert("aws_secret_access_key".to_string(), "any".to_string());
    // Seeded here rather than by a script, so the test is repeatable: a previous
    // run compacts the table it depended on.
    let encoded = seed_fragmented_table(&url, "compactme", 12)
        .await
        .expect("seed a fragmented table");
    let handler = CompactHandler::new(url).with_fallback(fallback);
    let recorder = Recorder::default();

    let request = RunDetectionRequest {
        request_id: "detect-1".to_string(),
        job_type: JOB_TYPE.to_string(),
        worker_config_values: int_config("min_fragments", 4),
        ..Default::default()
    };
    handler
        .detect(&request, &recorder)
        .await
        .expect("detection failed");

    let proposals = recorder.proposals.lock().unwrap().clone();
    assert!(
        !proposals.is_empty(),
        "expected a proposal for the fragmented table"
    );
    let proposal = proposals
        .iter()
        .find(|p| p.summary.contains(encoded.as_str()))
        .expect("no proposal for the seeded table");

    // Detection opened the dataset to decide, so it reports what it saw. This is
    // the only description of a Lance table anything outside the format can give.
    let observations = recorder.observations.lock().unwrap().clone();
    let observed = observations
        .iter()
        .find(|o| o.object_id.last().map(String::as_str) == Some("compactme"))
        .expect("detection reported no observation for the seeded table");
    assert_eq!(observed.format, "LANCE");
    for attribute in ["fragments", "rows", "versions", "schema"] {
        assert!(
            observed.attributes.contains_key(attribute),
            "observation is missing {attribute}: {:?}",
            observed.attributes.keys().collect::<Vec<_>>()
        );
    }

    let execute = ExecuteJobRequest {
        request_id: "execute-1".to_string(),
        job: Some(JobSpec {
            job_id: "job-1".to_string(),
            job_type: JOB_TYPE.to_string(),
            parameters: proposal.parameters.clone(),
            ..Default::default()
        }),
        worker_config_values: int_config("target_rows_per_fragment", 1_048_576),
        ..Default::default()
    };
    handler
        .execute(&execute, &recorder)
        .await
        .expect("execution failed");

    let completed = recorder.completed.lock().unwrap().clone();
    let result = completed.first().expect("no completion reported");
    assert!(
        result.success,
        "compaction reported failure: {}",
        result.error_message
    );
    let summary = result
        .result
        .as_ref()
        .map(|r| r.summary.clone())
        .unwrap_or_default();
    assert!(
        summary.contains("became"),
        "completion carried no fragment counts: {summary}"
    );
    eprintln!("compaction result: {summary}");
}

fn fallback() -> weed_lance_worker::dataset::FallbackOptions {
    let mut options = weed_lance_worker::dataset::FallbackOptions::new();
    options.insert("aws_access_key_id".to_string(), "any".to_string());
    options.insert("aws_secret_access_key".to_string(), "any".to_string());
    options
}

/// A table with more versions than the floor is proposed, and running the job
/// reports what it removed. The compaction test above leaves one behind.
#[tokio::test]
async fn cleans_up_old_versions() {
    let Some(url) = namespace_url() else {
        eprintln!("WEED_LANCE_NAMESPACE is unset, skipping");
        return;
    };
    let handler = CleanupVersionsHandler::new(url).with_fallback(fallback());
    let recorder = Recorder::default();

    let request = RunDetectionRequest {
        request_id: "detect-cleanup".to_string(),
        job_type: "lance_cleanup_versions".to_string(),
        worker_config_values: int_config("min_versions_to_keep", 2),
        ..Default::default()
    };
    handler
        .detect(&request, &recorder)
        .await
        .expect("detection failed");
    let proposals = recorder.proposals.lock().unwrap().clone();
    let Some(proposal) = proposals.first().cloned() else {
        eprintln!("no table has enough versions to clean up, skipping execution");
        return;
    };

    // Retain nothing, so every version outside the current one is fair game and
    // the job has something to report rather than a no-op.
    let execute = ExecuteJobRequest {
        request_id: "execute-cleanup".to_string(),
        job: Some(JobSpec {
            job_id: "job-cleanup".to_string(),
            job_type: "lance_cleanup_versions".to_string(),
            parameters: proposal.parameters.clone(),
            ..Default::default()
        }),
        worker_config_values: int_config("retain_hours", 0),
        ..Default::default()
    };
    handler
        .execute(&execute, &recorder)
        .await
        .expect("cleanup failed");

    let completed = recorder.completed.lock().unwrap().clone();
    let result = completed.last().expect("no completion reported");
    assert!(
        result.success,
        "cleanup reported failure: {}",
        result.error_message
    );
    eprintln!(
        "cleanup result: {}",
        result.result.as_ref().unwrap().summary
    );
}

/// A table with no indices has nothing to optimize, so detection proposes
/// nothing rather than queueing work that would do nothing.
#[tokio::test]
async fn skips_tables_without_indices() {
    let Some(url) = namespace_url() else {
        eprintln!("WEED_LANCE_NAMESPACE is unset, skipping");
        return;
    };
    let handler = OptimizeIndicesHandler::new(url).with_fallback(fallback());
    let recorder = Recorder::default();

    let request = RunDetectionRequest {
        request_id: "detect-indices".to_string(),
        job_type: "lance_optimize_indices".to_string(),
        worker_config_values: int_config("max_unindexed_rows", 1),
        ..Default::default()
    };
    handler
        .detect(&request, &recorder)
        .await
        .expect("detection failed");
    assert!(
        recorder.proposals.lock().unwrap().is_empty(),
        "a table with no indices must not be proposed for reindexing"
    );
}

/// The job with no Iceberg equivalent: rows appended after an index was built
/// are invisible to a search of it until this runs. Needs a table with an index
/// and rows outside it, which `indexed.py` in the scratchpad seeds.
#[tokio::test]
async fn reindexes_rows_an_index_does_not_cover() {
    let Some(url) = namespace_url() else {
        eprintln!("WEED_LANCE_NAMESPACE is unset, skipping");
        return;
    };
    let encoded = seed_table(&url, "reindexme", 2, 512, true)
        .await
        .expect("seed an indexed table with uncovered rows");
    let handler = OptimizeIndicesHandler::new(url).with_fallback(fallback());
    let recorder = Recorder::default();

    let request = RunDetectionRequest {
        request_id: "detect-reindex".to_string(),
        job_type: "lance_optimize_indices".to_string(),
        worker_config_values: int_config("max_unindexed_rows", 100),
        ..Default::default()
    };
    handler
        .detect(&request, &recorder)
        .await
        .expect("detection failed");
    let proposals = recorder.proposals.lock().unwrap().clone();
    let proposal = proposals
        .iter()
        .find(|p| p.summary.contains(encoded.as_str()))
        .cloned()
        .expect("the seeded indexed table was not proposed");

    let execute = ExecuteJobRequest {
        request_id: "execute-reindex".to_string(),
        job: Some(JobSpec {
            job_id: "job-reindex".to_string(),
            job_type: "lance_optimize_indices".to_string(),
            parameters: proposal.parameters.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };
    handler
        .execute(&execute, &recorder)
        .await
        .expect("reindex failed");

    let completed = recorder.completed.lock().unwrap().clone();
    let result = completed.last().expect("no completion reported");
    assert!(
        result.success,
        "reindex reported failure: {}",
        result.error_message
    );
    let output = &result.result.as_ref().unwrap().output_values;
    let after = match output
        .get("unindexed_rows_after")
        .and_then(|v| v.kind.as_ref())
    {
        Some(Kind::Int64Value(value)) => *value,
        other => panic!("no unindexed_rows_after in {other:?}"),
    };
    assert_eq!(
        after, 0,
        "rows are still outside the index after optimizing"
    );
    eprintln!(
        "reindex result: {}",
        result.result.as_ref().unwrap().summary
    );
}

/// The UI's whole reason for asking a worker: admin cannot read a Lance table,
/// so the rows have to come back already rendered.
#[tokio::test]
async fn previews_rows_of_a_table() {
    let Some(url) = namespace_url() else {
        eprintln!("set WEED_LANCE_NAMESPACE to run this test");
        return;
    };
    seed_table(&url, "previewme", 2, 3, false)
        .await
        .expect("seed a table to preview");

    let provider = LancePreview::new(url, fallback());
    let id = vec!["vec".to_string(), "ml".to_string(), "previewme".to_string()];
    let preview = provider.preview(&id, 4).await.expect("preview the table");

    assert_eq!(preview.columns, vec!["id".to_string(), "vec".to_string()]);
    assert_eq!(preview.total_rows, 6, "total is the table, not the sample");
    assert_eq!(preview.rows.len(), 4, "row_limit bounds the sample");
    assert!(
        preview.rows[0][1].starts_with('['),
        "a vector column should render as a list, got {:?}",
        preview.rows[0][1]
    );
}
