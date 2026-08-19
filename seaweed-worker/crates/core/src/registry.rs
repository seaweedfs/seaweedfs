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

/// Sample rows of one object, already rendered as text. The worker formats
/// them because it is the only side that knows the object's types.
pub struct Preview {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    /// Rows in the object, which is not the number sampled.
    pub total_rows: i64,
}

/// Reads sample rows of a format admin cannot parse itself.
///
/// This is deliberately not a JobHandler: a preview is answered while someone
/// waits on a page, so it neither schedules nor reports progress.
#[async_trait]
pub trait PreviewProvider: Send + Sync {
    /// The format this provider reads, matched case-insensitively against what
    /// the catalog recorded.
    fn format(&self) -> &str;
    async fn preview(&self, object_id: &[String], row_limit: usize) -> Result<Preview>;
}

/// The handlers one worker process serves. A process may serve several job
/// types, which is why WorkerHello carries a list of capabilities.
#[derive(Default, Clone)]
pub struct Registry {
    handlers: HashMap<String, Arc<dyn JobHandler>>,
    previews: HashMap<String, Arc<dyn PreviewProvider>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(mut self, handler: Arc<dyn JobHandler>) -> Self {
        self.handlers.insert(handler.capability().job_type, handler);
        self
    }

    pub fn with_preview(mut self, provider: Arc<dyn PreviewProvider>) -> Self {
        self.previews
            .insert(provider.format().to_ascii_uppercase(), provider);
        self
    }

    pub fn get(&self, job_type: &str) -> Option<Arc<dyn JobHandler>> {
        self.handlers.get(job_type).cloned()
    }

    pub fn preview_provider(&self, format: &str) -> Option<Arc<dyn PreviewProvider>> {
        self.previews.get(&format.to_ascii_uppercase()).cloned()
    }

    pub fn capabilities(&self) -> Vec<JobTypeCapability> {
        self.handlers.values().map(|h| h.capability()).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}
