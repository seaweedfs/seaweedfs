//! What every integration test in this crate needs to drive a handler: a sender
//! that keeps what the handler sent, and the two settings that make a handler
//! usable against a test gateway.
//!
//! Each test binary compiles this module on its own and uses part of it.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Result;
use seaweed_worker_core::pb::{
    config_value::Kind, ActivityEvent, ConfigValue, DetectionComplete, DetectionProposals,
    JobCompleted, JobProgressUpdate, JobProposal, ObjectObservation, WorkerObservations,
};
use seaweed_worker_core::{DetectionSender, ExecutionSender};

/// Keeps everything a handler reports, so a test can assert on it afterwards.
#[derive(Default)]
pub struct Recorder {
    pub proposals: Mutex<Vec<JobProposal>>,
    pub observations: Mutex<Vec<ObjectObservation>>,
    pub completed: Mutex<Vec<JobCompleted>>,
}

impl DetectionSender for Recorder {
    fn send_proposals(&self, proposals: DetectionProposals) -> Result<()> {
        self.proposals.lock().unwrap().extend(proposals.proposals);
        Ok(())
    }
    fn send_complete(&self, _complete: DetectionComplete) -> Result<()> {
        Ok(())
    }
    fn send_activity(&self, _activity: ActivityEvent) -> Result<()> {
        Ok(())
    }
    fn send_observations(&self, observations: WorkerObservations) -> Result<()> {
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

/// The gateway to run against, or nothing, in which case the caller skips:
/// these handlers rewrite real files, and there is nothing to learn from them
/// against a fake.
pub fn namespace_url() -> Option<String> {
    std::env::var("WEED_LANCE_NAMESPACE")
        .ok()
        .filter(|s| !s.is_empty())
}

pub fn int_config(name: &str, value: i64) -> HashMap<String, ConfigValue> {
    let mut values = HashMap::new();
    values.insert(
        name.to_string(),
        ConfigValue {
            kind: Some(Kind::Int64Value(value)),
        },
    );
    values
}

/// What to reach the data with where the namespace vends nothing of its own -
/// a gateway without STS vends no credentials, and one bound to a wildcard
/// address vends no endpoint either. Taken from the environment so a harness
/// can point these tests at a gateway that does check what it is given.
pub fn fallback() -> weed_lance_worker::dataset::FallbackOptions {
    let mut options = weed_lance_worker::dataset::FallbackOptions::new();
    let from_env =
        |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
    options.insert(
        "aws_access_key_id".to_string(),
        from_env("AWS_ACCESS_KEY_ID", "any"),
    );
    options.insert(
        "aws_secret_access_key".to_string(),
        from_env("AWS_SECRET_ACCESS_KEY", "any"),
    );
    if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
        options.insert("aws_endpoint".to_string(), endpoint);
        options.insert("allow_http".to_string(), "true".to_string());
    }
    options
}
