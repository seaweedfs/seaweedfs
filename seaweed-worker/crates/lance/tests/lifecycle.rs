//! Maintains one table someone else wrote, and nothing else.
//!
//! The Go suite in test/s3tables/lifecycle drives a table through its whole
//! life - declared through the namespace, written by a Lance client, maintained,
//! read again, dropped - and this is the maintenance step of it, so that step
//! goes through the handlers a deployed worker runs rather than through the two
//! lance calls they wrap. The table to work on comes in as WEED_LANCE_TABLE
//! because the table is the Go test's, seeded and checked there.
//!
//! Skipped, like the rest of this crate's integration tests, unless
//! WEED_LANCE_NAMESPACE names a live gateway.

use std::collections::HashMap;

use seaweed_worker_core::pb::{ConfigValue, ExecuteJobRequest, JobSpec, RunDetectionRequest};
use seaweed_worker_core::JobHandler;
use weed_lance_worker::jobs::cleanup::{self, CleanupVersionsHandler};
use weed_lance_worker::jobs::compact::{CompactHandler, JOB_TYPE as COMPACT_JOB_TYPE};

mod common;
use common::{fallback, int_config, namespace_url, Recorder};

fn table() -> Option<String> {
    std::env::var("WEED_LANCE_TABLE")
        .ok()
        .filter(|s| !s.is_empty())
}

/// Compacts the named table's fragments, then drops the versions compaction
/// left behind. Both go through the handler's own detect-then-execute path: a
/// proposal the worker would not have made is not one worth running.
#[tokio::test]
async fn maintains_the_named_table() {
    let (Some(url), Some(name)) = (namespace_url(), table()) else {
        eprintln!("WEED_LANCE_NAMESPACE or WEED_LANCE_TABLE is unset, skipping");
        return;
    };

    // A table written as a fragment per append is the case this exists for, so
    // anything above one fragment is worth merging here.
    let mut config = int_config("min_fragments", 2);
    config.extend(int_config("target_rows_per_fragment", 1_048_576));
    let compact = CompactHandler::new(url.clone()).with_fallback(fallback());
    run(&compact, COMPACT_JOB_TYPE, &name, config, true).await;

    // Keep the current version and retain nothing else, so the versions
    // compaction superseded are actually removed rather than counted.
    let mut config = int_config("min_versions_to_keep", 1);
    config.extend(int_config("retain_hours", 0));
    let cleanup = CleanupVersionsHandler::new(url).with_fallback(fallback());
    run(&cleanup, cleanup::JOB_TYPE, &name, config, true).await;
}

/// Detects, finds the proposal for `name`, and executes it. The config carries
/// every key both halves read; each ignores what it does not know.
async fn run<H: JobHandler>(
    handler: &H,
    job_type: &str,
    name: &str,
    config: HashMap<String, ConfigValue>,
    required: bool,
) {
    let recorder = Recorder::default();
    handler
        .detect(
            &RunDetectionRequest {
                request_id: format!("detect-{job_type}"),
                job_type: job_type.to_string(),
                worker_config_values: config.clone(),
                ..Default::default()
            },
            &recorder,
        )
        .await
        .unwrap_or_else(|err| panic!("{job_type} detection failed: {err}"));

    let proposals = recorder.proposals.lock().unwrap().clone();
    let Some(proposal) = proposals.iter().find(|p| p.summary.contains(name)) else {
        assert!(
            !required,
            "{job_type} proposed nothing for {name}, out of {} proposals",
            proposals.len()
        );
        eprintln!("{job_type}: nothing to do for {name}");
        return;
    };

    handler
        .execute(
            &ExecuteJobRequest {
                request_id: format!("execute-{job_type}"),
                job: Some(JobSpec {
                    job_id: format!("job-{job_type}"),
                    job_type: job_type.to_string(),
                    parameters: proposal.parameters.clone(),
                    ..Default::default()
                }),
                worker_config_values: config,
                ..Default::default()
            },
            &recorder,
        )
        .await
        .unwrap_or_else(|err| panic!("{job_type} execution failed: {err}"));

    let completed = recorder.completed.lock().unwrap().clone();
    let result = completed
        .last()
        .unwrap_or_else(|| panic!("{job_type} reported no completion"));
    assert!(
        result.success,
        "{job_type} reported failure: {}",
        result.error_message
    );
    eprintln!(
        "{job_type}: {}",
        result
            .result
            .as_ref()
            .map(|r| r.summary.clone())
            .unwrap_or_default()
    );
}
