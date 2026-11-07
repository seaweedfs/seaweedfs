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
    ActivityEvent, ConfigValue, DetectionComplete, DetectionProposals, JobCompleted,
    JobProgressUpdate, JobProposal, ObjectObservation, WorkerObservations, config_value::Kind,
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

/// The cluster's storage, when WEED_MASTERS names the masters a harness
/// started; otherwise none, and the tests that need storage are skipped by
/// `namespace_url` being unset too.
pub async fn storage() -> weed_lance_worker::dataset::Storage {
    let Ok(masters) = std::env::var("WEED_MASTERS") else {
        return weed_lance_worker::dataset::Storage::default();
    };
    let security = weed_lance_worker::security::Security::load(None, None).expect("security.toml");
    let cluster =
        weed_lance_worker::cluster::Cluster::new(&masters, "", &security).expect("masters");
    weed_lance_worker::dataset::Storage(Some(
        cluster.storage(&security, false).await.expect("storage"),
    ))
}
