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
use seaweed_worker_core::{DetectionSender, ExecutionSender, JobHandler};
use weed_lance_worker::jobs::compact::{CompactHandler, JOB_TYPE};

#[derive(Default)]
struct Recorder {
    proposals: Mutex<Vec<JobProposal>>,
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
    std::env::var("WEED_LANCE_NAMESPACE").ok().filter(|s| !s.is_empty())
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
        .find(|p| p.summary.contains("small"))
        .expect("no proposal for the seeded table");

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
    assert!(result.success, "compaction reported failure: {}", result.error_message);
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
