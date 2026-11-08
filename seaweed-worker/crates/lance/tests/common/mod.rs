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
use weed_lance_worker::catalog::{NamespaceClient, SigningKey};

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
    // The handlers say why they skip a table only in their logs.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();
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

/// The cluster's storage, from the masters WEED_MASTERS names. A live
/// namespace without them would have the tests seed tables through lance's
/// own S3 store, which reaches nothing, so the pair is required together.
pub async fn storage() -> weed_lance_worker::dataset::Storage {
    let masters = std::env::var("WEED_MASTERS")
        .ok()
        .filter(|s| !s.is_empty())
        .expect("WEED_LANCE_NAMESPACE names a live namespace; WEED_MASTERS must name its masters");
    let security = security();
    let cluster =
        weed_lance_worker::cluster::Cluster::new(&masters, "", &security).expect("masters");
    weed_lance_worker::dataset::Storage(Some(cluster.storage(&security).await.expect("storage")))
}

fn security() -> weed_lance_worker::security::Security {
    weed_lance_worker::security::Security::load(None, None).expect("security.toml")
}

/// The cluster token the namespace authenticates its processes by, from
/// `jwt.filer_signing.key` (WEED_JWT_FILER_SIGNING_KEY or security.toml),
/// as the worker reads it.
pub fn signing_key() -> Option<SigningKey> {
    SigningKey::new(security().get("jwt.filer_signing.key"))
}

/// The namespace client the handlers would use.
pub fn client(url: &str) -> NamespaceClient {
    NamespaceClient::new(url.to_string()).with_signing_key(signing_key().as_ref())
}
