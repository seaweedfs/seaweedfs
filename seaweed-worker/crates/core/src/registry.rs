use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::pb::{ExecuteJobRequest, JobTypeCapability, JobTypeDescriptor, RunDetectionRequest};
use crate::senders::{DetectionSender, ExecutionSender};

/// One job type, worker side. Mirrors the Go JobHandler interface in
/// weed/plugin/worker/worker.go so the two stay readable against each other.
#[async_trait]
pub trait JobHandler: Send + Sync {
    fn capability(&self) -> JobTypeCapability;
    /// The descriptor admin renders as this job's settings page.
    fn descriptor(&self) -> JobTypeDescriptor;
    async fn detect(
        &self,
        request: &RunDetectionRequest,
        sender: &dyn DetectionSender,
    ) -> Result<()>;
    async fn execute(
        &self,
        request: &ExecuteJobRequest,
        sender: &dyn ExecutionSender,
    ) -> Result<()>;
}

/// The handlers one worker process serves. A process may serve several job
/// types, which is why WorkerHello carries a list of capabilities.
#[derive(Default, Clone)]
pub struct Registry {
    handlers: HashMap<String, Arc<dyn JobHandler>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(mut self, handler: Arc<dyn JobHandler>) -> Self {
        self.handlers.insert(handler.capability().job_type, handler);
        self
    }

    pub fn get(&self, job_type: &str) -> Option<Arc<dyn JobHandler>> {
        self.handlers.get(job_type).cloned()
    }

    pub fn capabilities(&self) -> Vec<JobTypeCapability> {
        self.handlers.values().map(|h| h.capability()).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}
