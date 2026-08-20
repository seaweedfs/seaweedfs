//! What a worker will say about itself when nobody is watching the logs.
//!
//! The Go plugin worker serves `/health`, `/ready` and `/metrics` on an optional
//! port (`weed worker -metricsPort`); this is the same contract, so one scrape
//! config covers workers in either language. Names follow the Go side's
//! convention, `SeaweedFS_<subsystem>_<name>`, under a `worker` subsystem that
//! nothing else uses.
//!
//! Nothing here is Lance-specific. A worker for another format registers its own
//! collectors on the same registry and gets the same endpoint.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{info, warn};

const NAMESPACE: &str = "SeaweedFS";
const SUBSYSTEM: &str = "worker";

/// The metrics one worker process publishes, and the registry they live on.
///
/// Cloneable because the stream loop, the job handlers and the HTTP server all
/// hold it; everything inside is already shared.
#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    connected: IntGauge,
    connects: IntCounterVec,
    slots_used: IntGaugeVec,
    slots_total: IntGaugeVec,
    detections: IntCounterVec,
    detection_seconds: HistogramVec,
    proposals: IntCounterVec,
    objects_seen: IntCounterVec,
    objects_skipped: IntCounterVec,
    jobs: IntCounterVec,
    job_seconds: HistogramVec,
    previews: IntCounterVec,
    ready: Arc<AtomicBool>,
}

impl Metrics {
    pub fn new(worker_id: &str, worker_version: &str) -> Result<Self> {
        let registry = Registry::new();

        // Build info as a constant 1, the way the Go side does it, so a scrape
        // can tell which worker and which build answered.
        let build = IntGaugeVec::new(
            Opts::new("build_info", "Worker build information.")
                .namespace(NAMESPACE)
                .subsystem(SUBSYSTEM),
            &["worker_id", "version"],
        )?;
        build.with_label_values(&[worker_id, worker_version]).set(1);
        registry.register(Box::new(build))?;

        let connected = IntGauge::with_opts(
            Opts::new("connected", "1 while the admin control stream is up.")
                .namespace(NAMESPACE)
                .subsystem(SUBSYSTEM),
        )?;
        let connects = IntCounterVec::new(
            Opts::new(
                "stream_events_total",
                "Control stream lifecycle events by outcome (connected, closed, failed, shutdown).",
            )
            .namespace(NAMESPACE)
            .subsystem(SUBSYSTEM),
            &["event"],
        )?;
        let slots_used = IntGaugeVec::new(
            Opts::new("slots_used", "Slots currently held, by lane.")
                .namespace(NAMESPACE)
                .subsystem(SUBSYSTEM),
            &["lane"],
        )?;
        let slots_total = IntGaugeVec::new(
            Opts::new("slots_total", "Slots this worker advertises, by lane.")
                .namespace(NAMESPACE)
                .subsystem(SUBSYSTEM),
            &["lane"],
        )?;
        let detections = IntCounterVec::new(
            Opts::new(
                "detections_total",
                "Detection sweeps by job type and outcome.",
            )
            .namespace(NAMESPACE)
            .subsystem(SUBSYSTEM),
            &["job_type", "result"],
        )?;
        let detection_seconds = HistogramVec::new(
            HistogramOpts::new("detection_seconds", "How long a detection sweep took.")
                .namespace(NAMESPACE)
                .subsystem(SUBSYSTEM)
                .buckets(prometheus::exponential_buckets(0.01, 2.0, 14)?),
            &["job_type"],
        )?;
        let proposals = IntCounterVec::new(
            Opts::new("proposals_total", "Jobs proposed to admin, by job type.")
                .namespace(NAMESPACE)
                .subsystem(SUBSYSTEM),
            &["job_type"],
        )?;
        let objects_seen = IntCounterVec::new(
            Opts::new(
                "objects_seen_total",
                "Objects a detection sweep read, by job type.",
            )
            .namespace(NAMESPACE)
            .subsystem(SUBSYSTEM),
            &["job_type"],
        )?;
        let objects_skipped = IntCounterVec::new(
            Opts::new(
                "objects_skipped_total",
                "Objects a sweep could not read, by job type and reason. A sweep that \
                 proposes nothing looks the same as one that could read nothing; this is \
                 what tells them apart.",
            )
            .namespace(NAMESPACE)
            .subsystem(SUBSYSTEM),
            &["job_type", "reason"],
        )?;
        let jobs = IntCounterVec::new(
            Opts::new("jobs_total", "Jobs executed by job type and outcome.")
                .namespace(NAMESPACE)
                .subsystem(SUBSYSTEM),
            &["job_type", "result"],
        )?;
        let job_seconds = HistogramVec::new(
            HistogramOpts::new("job_seconds", "How long a job took to run.")
                .namespace(NAMESPACE)
                .subsystem(SUBSYSTEM)
                .buckets(prometheus::exponential_buckets(0.05, 2.0, 16)?),
            &["job_type"],
        )?;
        let previews = IntCounterVec::new(
            Opts::new(
                "previews_total",
                "Object previews served to admin, by outcome.",
            )
            .namespace(NAMESPACE)
            .subsystem(SUBSYSTEM),
            &["result"],
        )?;

        for collector in [
            Box::new(connected.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(connects.clone()),
            Box::new(slots_used.clone()),
            Box::new(slots_total.clone()),
            Box::new(detections.clone()),
            Box::new(detection_seconds.clone()),
            Box::new(proposals.clone()),
            Box::new(objects_seen.clone()),
            Box::new(objects_skipped.clone()),
            Box::new(jobs.clone()),
            Box::new(job_seconds.clone()),
            Box::new(previews.clone()),
        ] {
            registry.register(collector)?;
        }

        Ok(Self {
            registry,
            connected,
            connects,
            slots_used,
            slots_total,
            detections,
            detection_seconds,
            proposals,
            objects_seen,
            objects_skipped,
            jobs,
            job_seconds,
            previews,
            ready: Arc::new(AtomicBool::new(false)),
        })
    }

    /// The registry, so a worker can add collectors of its own.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    pub fn stream_connected(&self) {
        self.connected.set(1);
        self.ready.store(true, Ordering::Relaxed);
        self.connects.with_label_values(&["connected"]).inc();
    }

    /// `event` is why the stream ended: closed, failed, or shutdown.
    pub fn stream_ended(&self, event: &str) {
        self.connected.set(0);
        self.ready.store(false, Ordering::Relaxed);
        self.connects.with_label_values(&[event]).inc();
    }

    pub fn set_slots(&self, lane: &str, used: i64, total: i64) {
        self.slots_used.with_label_values(&[lane]).set(used);
        self.slots_total.with_label_values(&[lane]).set(total);
    }

    pub fn detection_finished(&self, job_type: &str, result: &str, seconds: f64, proposals: usize) {
        self.detections
            .with_label_values(&[job_type, result])
            .inc_by(1);
        self.detection_seconds
            .with_label_values(&[job_type])
            .observe(seconds);
        self.proposals
            .with_label_values(&[job_type])
            .inc_by(proposals as u64);
    }

    pub fn object_seen(&self, job_type: &str) {
        self.objects_seen.with_label_values(&[job_type]).inc();
    }

    pub fn object_skipped(&self, job_type: &str, reason: &str) {
        self.objects_skipped
            .with_label_values(&[job_type, reason])
            .inc();
    }

    pub fn job_finished(&self, job_type: &str, result: &str, seconds: f64) {
        self.jobs.with_label_values(&[job_type, result]).inc();
        self.job_seconds
            .with_label_values(&[job_type])
            .observe(seconds);
    }

    pub fn preview_finished(&self, result: &str) {
        self.previews.with_label_values(&[result]).inc();
    }

    /// A counter this worker's own jobs can raise, e.g. fragments removed.
    /// Registered lazily so a format's numbers live beside the generic ones
    /// without core having to know what they are.
    pub fn counter(&self, name: &str, help: &str) -> Result<IntCounter> {
        let counter = IntCounter::with_opts(
            Opts::new(name, help)
                .namespace(NAMESPACE)
                .subsystem(SUBSYSTEM),
        )?;
        self.registry.register(Box::new(counter.clone()))?;
        Ok(counter)
    }

    fn gather(&self) -> Result<String> {
        let mut buffer = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }
}

/// Serves the metrics endpoints until the process stops. Failing to bind is
/// logged rather than fatal: a worker that cannot publish metrics should still
/// do its work.
pub async fn serve(metrics: Metrics, addr: SocketAddr) -> Result<()> {
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::Router;

    let app = Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .route(
            "/ready",
            get(|State(metrics): State<Metrics>| async move {
                if metrics.is_ready() {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        )
        .route(
            "/metrics",
            get(|State(metrics): State<Metrics>| async move {
                match metrics.gather() {
                    Ok(body) => (StatusCode::OK, body),
                    Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")),
                }
            }),
        )
        .with_state(metrics);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind the metrics server to {addr}"))?;
    info!("serving worker metrics on http://{addr}/metrics");
    axum::serve(listener, app)
        .await
        .context("metrics server stopped")
}

/// Starts the metrics server in the background, warning rather than failing when
/// it cannot start.
pub fn spawn(metrics: Metrics, addr: SocketAddr) {
    tokio::spawn(async move {
        if let Err(err) = serve(metrics, addr).await {
            warn!("worker metrics server: {err:#}");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics() -> Metrics {
        Metrics::new("worker-1", "0.1.0").expect("build metrics")
    }

    // A scrape has to be able to tell these apart: a worker that swept and found
    // nothing to do, and one that could not read anything it swept.
    #[test]
    fn a_sweep_reports_what_it_read_and_what_it_could_not() {
        let metrics = metrics();
        metrics.object_seen("lance_compact");
        metrics.object_seen("lance_compact");
        metrics.object_skipped("lance_compact", "open");
        metrics.detection_finished("lance_compact", "ok", 0.5, 0);

        let text = metrics.gather().expect("gather");
        assert!(text.contains("SeaweedFS_worker_objects_seen_total{job_type=\"lance_compact\"} 2"));
        assert!(text.contains(
            "SeaweedFS_worker_objects_skipped_total{job_type=\"lance_compact\",reason=\"open\"} 1"
        ));
        assert!(text.contains(
            "SeaweedFS_worker_detections_total{job_type=\"lance_compact\",result=\"ok\"} 1"
        ));
        assert!(text.contains("SeaweedFS_worker_proposals_total{job_type=\"lance_compact\"} 0"));
    }

    // /ready follows the stream, because a worker with no admin behind it is
    // running but not doing anything.
    #[test]
    fn readiness_follows_the_stream() {
        let metrics = metrics();
        assert!(!metrics.is_ready(), "not ready before the stream is up");

        metrics.stream_connected();
        assert!(metrics.is_ready());
        assert!(metrics
            .gather()
            .unwrap()
            .contains("SeaweedFS_worker_connected 1"));

        metrics.stream_ended("closed");
        assert!(!metrics.is_ready());
        assert!(metrics
            .gather()
            .unwrap()
            .contains("SeaweedFS_worker_connected 0"));
        assert!(metrics
            .gather()
            .unwrap()
            .contains("SeaweedFS_worker_stream_events_total{event=\"closed\"} 1"));
    }

    #[test]
    fn build_info_names_the_worker() {
        let text = metrics().gather().expect("gather");
        assert!(text
            .contains("SeaweedFS_worker_build_info{version=\"0.1.0\",worker_id=\"worker-1\"} 1"));
    }

    // A format's own numbers land on the same registry, so one endpoint serves
    // both and a second worker implementation needs no new plumbing.
    #[test]
    fn a_worker_can_add_counters_of_its_own() {
        let metrics = metrics();
        let counter = metrics
            .counter("lance_fragments_removed_total", "Fragments merged away.")
            .expect("register");
        counter.inc_by(16);

        let text = metrics.gather().expect("gather");
        assert!(text.contains("SeaweedFS_worker_lance_fragments_removed_total 16"));
    }
}
