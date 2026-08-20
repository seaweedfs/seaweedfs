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

/// Wraps a sender so what passes through it is counted. The handlers report
/// their results to admin and nowhere else, so this is where a scrape can learn
/// what happened without every handler having to know about metrics.
pub struct MeteredSender<'a> {
    inner: &'a StreamSender,
    metrics: crate::metrics::Metrics,
    job_type: String,
    proposals: std::sync::atomic::AtomicUsize,
    failed: std::sync::atomic::AtomicBool,
}

impl<'a> MeteredSender<'a> {
    pub fn new(inner: &'a StreamSender, metrics: crate::metrics::Metrics, job_type: &str) -> Self {
        Self {
            inner,
            metrics,
            job_type: job_type.to_string(),
            proposals: std::sync::atomic::AtomicUsize::new(0),
            failed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// How many proposals went out, for the detection counter.
    pub fn proposals(&self) -> usize {
        self.proposals.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Whether the handler reported a failure of its own. A handler that fails
    /// by returning an error is counted by the caller; this catches the one that
    /// reports failure and returns Ok.
    pub fn reported_failure(&self) -> bool {
        self.failed.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl DetectionSender for MeteredSender<'_> {
    fn send_proposals(&self, proposals: DetectionProposals) -> Result<()> {
        self.proposals.fetch_add(
            proposals.proposals.len(),
            std::sync::atomic::Ordering::Relaxed,
        );
        self.inner.send_proposals(proposals)
    }

    fn send_complete(&self, complete: DetectionComplete) -> Result<()> {
        if !complete.success {
            self.failed
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        self.inner.send_complete(complete)
    }

    fn send_activity(&self, activity: ActivityEvent) -> Result<()> {
        self.inner.send_activity(activity)
    }

    fn send_observations(&self, observations: WorkerObservations) -> Result<()> {
        self.inner.send_observations(observations)
    }
}

impl ExecutionSender for MeteredSender<'_> {
    fn send_progress(&self, progress: JobProgressUpdate) -> Result<()> {
        self.inner.send_progress(progress)
    }

    fn send_completed(&self, completed: JobCompleted) -> Result<()> {
        if !completed.success {
            self.failed
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let _ = &self.job_type;
        let _ = &self.metrics;
        self.inner.send_completed(completed)
    }
}
