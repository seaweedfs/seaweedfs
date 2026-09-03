//! Drives the sort handler against a live namespace.
//!
//! Skipped unless WEED_LANCE_NAMESPACE names one, like the other integration
//! tests here: the job rewrites every fragment of a real table and commits, and
//! the commit is the half worth testing.

use anyhow::Result;
use seaweed_worker_core::pb::{
    config_value::Kind, ConfigValue, ExecuteJobRequest, JobSpec, RunDetectionRequest,
};
use seaweed_worker_core::JobHandler;
use weed_lance_worker::catalog::NamespaceClient;
use weed_lance_worker::jobs::sort::{SortHandler, JOB_TYPE};

mod common;
use common::{fallback, namespace_url, Recorder};

/// One live gateway and one shared catalog, and `list_all_tables` sweeps
/// everything, so these tests take a lock the way the compaction ones do.
static GATEWAY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn config(values: &[(&str, ConfigValue)]) -> std::collections::HashMap<String, ConfigValue> {
    values
        .iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect()
}

fn int(value: i64) -> ConfigValue {
    ConfigValue {
        kind: Some(Kind::Int64Value(value)),
    }
}

fn text(value: &str) -> ConfigValue {
    ConfigValue {
        kind: Some(Kind::StringValue(value.to_string())),
    }
}

/// Declares a table and writes `batches` appends whose ids descend, so the rows
/// land in an order no sort would produce. `declared` is written into the
/// dataset's own configuration, which is where a table says how it wants to be
/// sorted.
async fn seed_unsorted_table(
    url: &str,
    name: &str,
    batches: i64,
    rows_each: i64,
    declared: Option<&str>,
) -> Result<String> {
    use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::{Dataset, WriteMode, WriteParams};
    use lance::io::{ObjectStoreParams, StorageOptionsAccessor};
    use std::sync::Arc;

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

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let total = batches * rows_each;
    let mut dataset = None;
    for batch_index in 0..batches {
        // Ids descend across and within the batches, so the physical order is
        // the reverse of the order the table declares.
        let ids: Vec<i64> = (0..rows_each)
            .map(|row| total - 1 - (batch_index * rows_each + row))
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))])?;
        let params = WriteParams {
            mode: if batch_index == 0 {
                WriteMode::Overwrite
            } else {
                WriteMode::Append
            },
            store_params: Some(ObjectStoreParams {
                storage_options_accessor: Some(Arc::new(
                    StorageOptionsAccessor::with_static_options(options.clone()),
                )),
                ..Default::default()
            }),
            ..Default::default()
        };
        dataset = Some(
            Dataset::write(
                RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
                description.location.as_str(),
                Some(params),
            )
            .await?,
        );
    }

    if let Some(declared) = declared {
        let mut dataset = dataset.expect("at least one batch was written");
        dataset
            .update_config([("seaweedfs.sort.fields", declared)])
            .await?;
    }
    Ok(encoded)
}

/// Reads the ids back in the order they are stored, which is what a sort is
/// supposed to change.
async fn stored_ids(url: &str, name: &str) -> Result<Vec<i64>> {
    use arrow_array::Int64Array;
    use futures::TryStreamExt;

    let client = NamespaceClient::new(url.to_string());
    let id = vec!["vec".to_string(), "ml".to_string(), name.to_string()];
    let table = weed_lance_worker::dataset::open(&client, &id, &fallback()).await?;
    let stream = table.dataset.scan().try_into_stream().await?;
    let batches: Vec<_> = stream.try_collect().await?;
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name("id")
            .expect("the seeded table has an id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is an Int64 column")
            .clone();
        ids.extend(column.values().iter().copied());
    }
    Ok(ids)
}

/// The whole loop: a table declaring an order its rows do not follow is
/// proposed, sorting it stores the rows in that order, and the marker the same
/// commit carries is what stops the next sweep proposing it again.
#[tokio::test]
async fn sorts_a_table_and_stops_proposing_it() {
    let _gateway = GATEWAY.lock().await;
    let Some(url) = namespace_url() else {
        eprintln!("WEED_LANCE_NAMESPACE is unset, skipping");
        return;
    };
    let encoded = seed_unsorted_table(&url, "sortme", 4, 250, Some("id asc"))
        .await
        .expect("seed an unsorted table");

    let before = stored_ids(&url, "sortme").await.expect("read the ids back");
    assert!(
        before.windows(2).any(|pair| pair[0] > pair[1]),
        "the seeded table was already sorted, so the test proves nothing"
    );

    let handler = SortHandler::new(url.clone()).with_fallback(fallback());
    let recorder = Recorder::default();
    let detection = RunDetectionRequest {
        request_id: "detect-sort".to_string(),
        job_type: JOB_TYPE.to_string(),
        worker_config_values: config(&[("min_unsorted_rows", int(1))]),
        ..Default::default()
    };
    handler
        .detect(&detection, &recorder)
        .await
        .expect("detection failed");

    let proposal = recorder
        .proposals
        .lock()
        .unwrap()
        .iter()
        .find(|p| p.summary.contains(encoded.as_str()))
        .cloned()
        .expect("the unsorted table was not proposed");
    assert!(
        proposal.summary.contains("id asc"),
        "the proposal does not say what order it would use: {}",
        proposal.summary
    );

    let execute = ExecuteJobRequest {
        request_id: "execute-sort".to_string(),
        job: Some(JobSpec {
            job_id: "job-sort".to_string(),
            job_type: JOB_TYPE.to_string(),
            parameters: proposal.parameters.clone(),
            ..Default::default()
        }),
        worker_config_values: config(&[
            ("memory_budget_mb", int(64)),
            ("max_rows_per_file", int(1024)),
        ]),
        ..Default::default()
    };
    handler
        .execute(&execute, &recorder)
        .await
        .expect("sorting failed");

    let completed = recorder.completed.lock().unwrap().clone();
    let result = completed.last().expect("no completion reported");
    assert!(
        result.success,
        "the sort reported failure: {}",
        result.error_message
    );

    let after = stored_ids(&url, "sortme")
        .await
        .expect("read the sorted ids back");
    assert_eq!(after.len(), before.len(), "the sort lost or invented rows");
    assert!(
        after.windows(2).all(|pair| pair[0] <= pair[1]),
        "the rows are not stored in ascending id order"
    );

    // The marker rode in the same commit as the data, so a second sweep finds
    // the table up to date without anything else having written it.
    let second = Recorder::default();
    handler
        .detect(&detection, &second)
        .await
        .expect("second detection failed");
    assert!(
        !second
            .proposals
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.summary.contains(encoded.as_str())),
        "a table that was just sorted must not be proposed again"
    );
    eprintln!("sort result: {}", result.result.as_ref().unwrap().summary);
}

/// A table that declares no order, with a worker configuring none either, is
/// not something to rewrite on a guess.
#[tokio::test]
async fn leaves_a_table_that_declares_no_order_alone() {
    let _gateway = GATEWAY.lock().await;
    let Some(url) = namespace_url() else {
        eprintln!("WEED_LANCE_NAMESPACE is unset, skipping");
        return;
    };
    let encoded = seed_unsorted_table(&url, "unsorted", 2, 16, None)
        .await
        .expect("seed a table with no declared order");

    let handler = SortHandler::new(url.clone()).with_fallback(fallback());
    let recorder = Recorder::default();
    handler
        .detect(
            &RunDetectionRequest {
                request_id: "detect-none".to_string(),
                job_type: JOB_TYPE.to_string(),
                worker_config_values: config(&[
                    ("min_unsorted_rows", int(1)),
                    ("sort_fields", text("")),
                ]),
                ..Default::default()
            },
            &recorder,
        )
        .await
        .expect("detection failed");

    assert!(
        !recorder
            .proposals
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.summary.contains(encoded.as_str())),
        "a table with no order named anywhere must not be proposed"
    );
}

/// The case a row-count threshold cannot see: the table replaced by a write of
/// its own, growing by far less than `min_unsorted_rows`. Every row moved, so
/// the table is unsorted, and detection has to say so however small the delta.
#[tokio::test]
async fn proposes_a_replacement_that_barely_changes_the_row_count() {
    let _gateway = GATEWAY.lock().await;
    let Some(url) = namespace_url() else {
        eprintln!("WEED_LANCE_NAMESPACE is unset, skipping");
        return;
    };
    let encoded = seed_unsorted_table(&url, "replaceme", 4, 250, Some("id asc"))
        .await
        .expect("seed an unsorted table");

    let handler = SortHandler::new(url.clone()).with_fallback(fallback());
    // A threshold no realistic append would cross, so nothing but the identity
    // of the data can be what proposes this table.
    let detection = RunDetectionRequest {
        request_id: "detect-replace".to_string(),
        job_type: JOB_TYPE.to_string(),
        worker_config_values: config(&[("min_unsorted_rows", int(1_000_000))]),
        ..Default::default()
    };

    let first = Recorder::default();
    handler.detect(&detection, &first).await.expect("detection");
    let proposal = first
        .proposals
        .lock()
        .unwrap()
        .iter()
        .find(|p| p.summary.contains(encoded.as_str()))
        .cloned()
        .expect("a never-sorted table was not proposed");

    handler
        .execute(
            &ExecuteJobRequest {
                request_id: "execute-replace".to_string(),
                job: Some(JobSpec {
                    job_id: "job-replace".to_string(),
                    job_type: JOB_TYPE.to_string(),
                    parameters: proposal.parameters.clone(),
                    ..Default::default()
                }),
                worker_config_values: config(&[("max_rows_per_file", int(1024))]),
                ..Default::default()
            },
            &first,
        )
        .await
        .expect("sorting failed");

    // Sorted, and left alone by the next sweep.
    let after_sort = Recorder::default();
    handler
        .detect(&detection, &after_sort)
        .await
        .expect("detection");
    assert!(
        !after_sort
            .proposals
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.summary.contains(encoded.as_str())),
        "a table that was just sorted must not be proposed again"
    );

    // Now replace the data: 1002 rows where there were 1000, shuffled again.
    seed_unsorted_table(&url, "replaceme", 3, 334, Some("id asc"))
        .await
        .expect("replace the table's data");

    let after_replace = Recorder::default();
    handler
        .detect(&detection, &after_replace)
        .await
        .expect("detection");
    assert!(
        after_replace
            .proposals
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.summary.contains(encoded.as_str())),
        "a replaced table must be proposed even when the row count barely moved"
    );
}
