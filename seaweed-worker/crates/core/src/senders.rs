use anyhow::Result;
use tokio::sync::mpsc;

use crate::pb::{
    worker_to_admin_message::Body, ActivityEvent, DetectionComplete, DetectionProposals,
    JobCompleted, JobProgressUpdate, WorkerObservations, WorkerToAdminMessage,
};

/// Replies to one detection request.
pub trait DetectionSender: Send + Sync {
    fn send_proposals(&self, proposals: DetectionProposals) -> Result<()>;
    fn send_complete(&self, complete: DetectionComplete) -> Result<()>;
    fn send_activity(&self, activity: ActivityEvent) -> Result<()>;
    /// Reports what the worker saw while deciding. Admin caches the last one
    /// per object and serves it back for display; nothing is scheduled from it.
    fn send_observations(&self, observations: WorkerObservations) -> Result<()>;
}

/// Replies to one execution request.
pub trait ExecutionSender: Send + Sync {
    fn send_progress(&self, progress: JobProgressUpdate) -> Result<()>;
    fn send_completed(&self, completed: JobCompleted) -> Result<()>;
}

/// Both senders write to the single outbound stream, so they share one channel.
#[derive(Clone)]
pub struct StreamSender {
    worker_id: String,
    tx: mpsc::UnboundedSender<WorkerToAdminMessage>,
}

impl StreamSender {
    pub fn new(worker_id: String, tx: mpsc::UnboundedSender<WorkerToAdminMessage>) -> Self {
        Self { worker_id, tx }
    }

    pub fn send(&self, body: Body) -> Result<()> {
        self.tx.send(WorkerToAdminMessage {
            worker_id: self.worker_id.clone(),
            sent_at: Some(std::time::SystemTime::now().into()),
            body: Some(body),
        })?;
        Ok(())
    }
}

impl DetectionSender for StreamSender {
    fn send_proposals(&self, proposals: DetectionProposals) -> Result<()> {
        self.send(Body::DetectionProposals(proposals))
    }

    fn send_complete(&self, complete: DetectionComplete) -> Result<()> {
        self.send(Body::DetectionComplete(complete))
    }

    fn send_activity(&self, _activity: ActivityEvent) -> Result<()> {
        // Activity rides inside progress and completion messages rather than
        // being a body of its own, so there is nothing to send on its own here.
        Ok(())
    }

    fn send_observations(&self, observations: WorkerObservations) -> Result<()> {
        self.send(Body::Observations(observations))
    }
}

impl ExecutionSender for StreamSender {
    fn send_progress(&self, progress: JobProgressUpdate) -> Result<()> {
        self.send(Body::JobProgressUpdate(progress))
    }

    fn send_completed(&self, completed: JobCompleted) -> Result<()> {
        self.send(Body::JobCompleted(completed))
    }
}
