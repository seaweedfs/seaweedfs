use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use tokio::sync::{mpsc, Semaphore};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tracing::{info, warn};

use crate::config::WorkerOptions;
use crate::metrics::Metrics;
use crate::pb::{
    admin_to_worker_message::Body as AdminBody,
    plugin_control_service_client::PluginControlServiceClient,
    worker_to_admin_message::Body as WorkerBody, ConfigSchemaResponse, ExecuteJobRequest,
    JobCompleted, ObjectPreviewResponse, PreviewRow, RequestObjectPreview, RunDetectionRequest,
    RunningWork, WorkerHeartbeat, WorkerHello,
};
use crate::registry::Registry;
use crate::senders::{MeteredSender, StreamSender};

/// The protocol version this worker speaks, sent in WorkerHello.
const PROTOCOL_VERSION: &str = "1";

/// Connect to admin and serve the registry until the context is cancelled,
/// reconnecting on failure. The stream is the only channel: everything admin
/// asks for and everything the worker reports flows through it.
pub async fn run(options: WorkerOptions, registry: Registry) -> Result<()> {
    let metrics = Metrics::new(&options.worker_id, &options.worker_version)?;
    run_with_metrics(options, registry, metrics).await
}

/// Runs with metrics a caller has already made, so it can serve them and add
/// collectors of its own before the stream starts.
pub async fn run_with_metrics(
    options: WorkerOptions,
    registry: Registry,
    metrics: Metrics,
) -> Result<()> {
    if registry.is_empty() {
        return Err(anyhow!("no job handlers registered"));
    }
    if options.max_detection_concurrency < 1 || options.max_execution_concurrency < 1 {
        return Err(anyhow!(
            "concurrency limits must be at least 1, got detection={} execution={}",
            options.max_detection_concurrency,
            options.max_execution_concurrency
        ));
    }
    let slots = Slots::new(&options);
    metrics.set_slots("detection", 0, slots.detection_total as i64);
    metrics.set_slots("execution", 0, slots.execution_total as i64);
    loop {
        match serve_once(&options, &registry, &slots, &metrics).await {
            Err(err) => {
                metrics.stream_ended("failed");
                warn!("worker stream ended: {err:#}")
            }
            // Admin asked this worker to stop, so stop. Reconnecting here would
            // make shutdown impossible: the worker would log back in.
            Ok(Outcome::ShutdownRequested) => {
                metrics.stream_ended("shutdown");
                return Ok(());
            }
            // Admin closing a healthy stream is not an error, but reconnecting
            // in silence hides the reason - two workers sharing an id evict
            // each other and produce nothing but a login every few seconds.
            Ok(Outcome::StreamClosed) => {
                metrics.stream_ended("closed");
                warn!(
                    "admin closed the stream; reconnecting in {:?}. If this repeats, check for \
                     another worker using the id {}",
                    options.reconnect_delay, options.worker_id
                )
            }
        }
        tokio::time::sleep(options.reconnect_delay).await;
    }
}

/// Why a stream ended. Only one of these means "do not come back".
enum Outcome {
    StreamClosed,
    ShutdownRequested,
}

/// The capacity this worker advertises in WorkerHello. Admin schedules against
/// those numbers, so the worker has to actually hold to them - and the heartbeat
/// has to report what is in use, or admin is scheduling blind.
#[derive(Clone)]
struct Slots {
    detection: Arc<Semaphore>,
    execution: Arc<Semaphore>,
    detection_total: i32,
    execution_total: i32,
}

impl Slots {
    fn new(options: &WorkerOptions) -> Self {
        Self {
            detection: Arc::new(Semaphore::new(options.max_detection_concurrency as usize)),
            execution: Arc::new(Semaphore::new(options.max_execution_concurrency as usize)),
            detection_total: options.max_detection_concurrency,
            execution_total: options.max_execution_concurrency,
        }
    }

    fn detection_used(&self) -> i32 {
        self.detection_total - self.detection.available_permits() as i32
    }

    fn execution_used(&self) -> i32 {
        self.execution_total - self.execution.available_permits() as i32
    }
}

/// Dials admin, over mTLS when certificates are configured. Plaintext is the
/// default and is fine over loopback; anything else carries preview rows and
/// execution commands in the clear, and a cluster with grpc TLS on refuses the
/// connection anyway.
async fn connect(options: &WorkerOptions, grpc_address: &str) -> Result<Channel> {
    let Some(tls) = options.tls.as_ref() else {
        return Ok(Channel::from_shared(format!("http://{grpc_address}"))?
            .connect_timeout(Duration::from_secs(10))
            .connect()
            .await?);
    };

    let ca = tokio::fs::read(&tls.ca_path)
        .await
        .with_context(|| format!("read CA certificate {}", tls.ca_path))?;
    let cert = tokio::fs::read(&tls.client_cert_path)
        .await
        .with_context(|| format!("read client certificate {}", tls.client_cert_path))?;
    let key = tokio::fs::read(&tls.client_key_path)
        .await
        .with_context(|| format!("read client key {}", tls.client_key_path))?;

    let mut config = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(cert, key));
    if let Some(server_name) = tls.server_name.as_ref() {
        config = config.domain_name(server_name.clone());
    }

    Ok(Channel::from_shared(format!("https://{grpc_address}"))?
        .tls_config(config)?
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await?)
}

async fn serve_once(
    options: &WorkerOptions,
    registry: &Registry,
    slots: &Slots,
    metrics: &Metrics,
) -> Result<Outcome> {
    // Operators give the admin's HTTP address; the gRPC port is derived, the
    // same way the Go worker does it.
    let grpc_address = crate::address::server_to_grpc_address(&options.admin_address)
        .ok_or_else(|| anyhow!("cannot parse admin address {}", options.admin_address))?;
    let channel = connect(options, &grpc_address).await?;
    let mut client = PluginControlServiceClient::new(channel);

    let (tx, rx) = mpsc::unbounded_channel();
    let sender = StreamSender::new(options.worker_id.clone(), tx);

    sender.send(WorkerBody::Hello(WorkerHello {
        worker_id: options.worker_id.clone(),
        worker_instance_id: options.worker_id.clone(),
        address: options.worker_address.clone(),
        worker_version: options.worker_version.clone(),
        protocol_version: PROTOCOL_VERSION.to_string(),
        capabilities: registry.capabilities(),
        metadata: Default::default(),
    }))?;

    let mut inbound = client
        .worker_stream(UnboundedReceiverStream::new(rx))
        .await?
        .into_inner();

    let heartbeat = spawn_heartbeat(
        sender.clone(),
        options.clone(),
        slots.clone(),
        metrics.clone(),
    );

    while let Some(message) = inbound.message().await? {
        let request_id = message.request_id.clone();
        match message.body {
            Some(AdminBody::Hello(hello)) => {
                if !hello.accepted {
                    return Err(anyhow!("admin rejected this worker: {}", hello.message));
                }
                metrics.stream_connected();
                info!(
                    "connected to admin at {} ({})",
                    options.admin_address, grpc_address
                );
            }
            Some(AdminBody::RequestConfigSchema(request)) => {
                let response = match registry.get(&request.job_type) {
                    Some(handler) => ConfigSchemaResponse {
                        request_id: request_id.clone(),
                        job_type: request.job_type.clone(),
                        success: true,
                        error_message: String::new(),
                        job_type_descriptor: Some(handler.descriptor()),
                    },
                    None => ConfigSchemaResponse {
                        request_id: request_id.clone(),
                        job_type: request.job_type.clone(),
                        success: false,
                        error_message: format!("unknown job type {}", request.job_type),
                        job_type_descriptor: None,
                    },
                };
                sender.send(WorkerBody::ConfigSchemaResponse(response))?;
            }
            Some(AdminBody::RequestObjectPreview(request)) => {
                spawn_preview(
                    registry.clone(),
                    sender.clone(),
                    metrics.clone(),
                    request_id.clone(),
                    request,
                );
            }
            Some(AdminBody::RunDetectionRequest(request)) => {
                spawn_detection(
                    registry.clone(),
                    sender.clone(),
                    slots.clone(),
                    metrics.clone(),
                    request,
                );
            }
            Some(AdminBody::ExecuteJobRequest(request)) => {
                spawn_execution(
                    registry.clone(),
                    sender.clone(),
                    slots.clone(),
                    metrics.clone(),
                    request,
                );
            }
            Some(AdminBody::CancelRequest(request)) => {
                // Cancellation needs a per-request handle to be honoured; until
                // then say so rather than silently continuing to run the job.
                warn!(
                    "cancel requested for {} ({}) but is not implemented",
                    request.target_id, request.reason
                );
            }
            Some(AdminBody::Shutdown(shutdown)) => {
                info!("admin asked this worker to stop: {}", shutdown.reason);
                heartbeat.abort();
                return Ok(Outcome::ShutdownRequested);
            }
            None => {}
        }
    }

    heartbeat.abort();
    Ok(Outcome::StreamClosed)
}

/// Answers one preview request off the stream loop. Reading rows takes as long
/// as it takes, and the stream has heartbeats to keep up meanwhile.
fn spawn_preview(
    registry: Registry,
    sender: StreamSender,
    metrics: Metrics,
    request_id: String,
    request: RequestObjectPreview,
) {
    tokio::spawn(async move {
        let limit = request.row_limit.max(1) as usize;
        let response = match registry.preview_provider(&request.format) {
            None => ObjectPreviewResponse {
                request_id,
                success: false,
                error_message: format!("this worker does not read {} objects", request.format),
                ..Default::default()
            },
            Some(provider) => match provider.preview(&request.object_id, limit).await {
                Ok(preview) => ObjectPreviewResponse {
                    request_id,
                    success: true,
                    error_message: String::new(),
                    columns: preview.columns,
                    rows: preview
                        .rows
                        .into_iter()
                        .map(|values| PreviewRow { values })
                        .collect(),
                    total_rows: preview.total_rows,
                },
                Err(err) => ObjectPreviewResponse {
                    request_id,
                    success: false,
                    error_message: format!("{err:#}"),
                    ..Default::default()
                },
            },
        };
        metrics.preview_finished(if response.success { "ok" } else { "failed" });
        let _ = sender.send(WorkerBody::ObjectPreviewResponse(response));
    });
}

fn spawn_heartbeat(
    sender: StreamSender,
    options: WorkerOptions,
    slots: Slots,
    metrics: Metrics,
) -> tokio::task::JoinHandle<()> {
    // The handle has to be the heartbeat's own, or aborting it aborts nothing
    // and every reconnect leaves another ticker running.
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(options.heartbeat_interval);
        loop {
            ticker.tick().await;
            // The heartbeat already computes this for admin; publish the same
            // numbers so a scrape and the admin UI cannot disagree.
            metrics.set_slots(
                "detection",
                slots.detection_used() as i64,
                slots.detection_total as i64,
            );
            metrics.set_slots(
                "execution",
                slots.execution_used() as i64,
                slots.execution_total as i64,
            );
            let beat = WorkerHeartbeat {
                worker_id: options.worker_id.clone(),
                running_work: Vec::<RunningWork>::new(),
                detection_slots_used: slots.detection_used(),
                detection_slots_total: slots.detection_total,
                execution_slots_used: slots.execution_used(),
                execution_slots_total: slots.execution_total,
                queued_jobs_by_type: Default::default(),
                metadata: Default::default(),
            };
            if sender.send(WorkerBody::Heartbeat(beat)).is_err() {
                return;
            }
        }
    })
}

fn spawn_detection(
    registry: Registry,
    sender: StreamSender,
    slots: Slots,
    metrics: Metrics,
    request: RunDetectionRequest,
) {
    tokio::spawn(async move {
        let Some(handler) = registry.get(&request.job_type) else {
            return;
        };
        // Held until the sweep finishes, so the worker keeps to the capacity it
        // advertised and the heartbeat reports the truth while it works.
        let _permit = slots.detection.acquire().await;
        let metered = MeteredSender::new(&sender);
        let started = Instant::now();
        let outcome = handler.detect(&request, &metered).await;
        let result = if let Err(err) = &outcome {
            warn!("detection for {} failed: {err:#}", request.job_type);
            let _ = sender.send(WorkerBody::DetectionComplete(
                crate::pb::DetectionComplete {
                    request_id: request.request_id.clone(),
                    job_type: request.job_type.clone(),
                    success: false,
                    error_message: format!("{err:#}"),
                    total_proposals: 0,
                },
            ));
            "failed"
        } else if metered.reported_failure() {
            "failed"
        } else {
            "ok"
        };
        metrics.detection_finished(
            &request.job_type,
            result,
            started.elapsed().as_secs_f64(),
            metered.proposals(),
        );
    });
}

fn spawn_execution(
    registry: Registry,
    sender: StreamSender,
    slots: Slots,
    metrics: Metrics,
    request: ExecuteJobRequest,
) {
    tokio::spawn(async move {
        let _permit = slots.execution.acquire().await;
        let job_type = request
            .job
            .as_ref()
            .map(|job| job.job_type.clone())
            .unwrap_or_default();
        let job_id = request
            .job
            .as_ref()
            .map(|job| job.job_id.clone())
            .unwrap_or_default();
        let Some(handler) = registry.get(&job_type) else {
            return;
        };
        let metered = MeteredSender::new(&sender);
        let started = Instant::now();
        let outcome = handler.execute(&request, &metered).await;
        let result = if let Err(err) = &outcome {
            warn!("job {job_id} failed: {err:#}");
            let _ = sender.send(WorkerBody::JobCompleted(JobCompleted {
                request_id: request.request_id.clone(),
                job_id,
                job_type: job_type.clone(),
                success: false,
                error_message: format!("{err:#}"),
                ..Default::default()
            }));
            "failed"
        } else if metered.reported_failure() {
            "failed"
        } else {
            "ok"
        };
        metrics.job_finished(&job_type, result, started.elapsed().as_secs_f64());
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(detection: i32, execution: i32) -> WorkerOptions {
        WorkerOptions {
            max_detection_concurrency: detection,
            max_execution_concurrency: execution,
            ..Default::default()
        }
    }

    // The heartbeat is admin's only view of how busy this worker is; before the
    // permits existed it reported zero however much was running.
    #[tokio::test]
    async fn slots_report_what_is_held() {
        let slots = Slots::new(&options(2, 3));
        assert_eq!(slots.detection_used(), 0);
        assert_eq!(slots.execution_used(), 0);

        let held = slots.detection.acquire().await.unwrap();
        assert_eq!(slots.detection_used(), 1);
        assert_eq!(slots.execution_used(), 0, "the two do not share capacity");

        drop(held);
        assert_eq!(slots.detection_used(), 0);
    }

    // A limit of one means the second request waits, rather than running anyway
    // as it did when every request simply spawned a task.
    #[tokio::test]
    async fn a_full_lane_makes_the_next_request_wait() {
        let slots = Slots::new(&options(1, 1));
        let held = slots.execution.clone().acquire_owned().await.unwrap();
        assert_eq!(slots.execution_used(), 1);

        let waiter = tokio::spawn({
            let execution = slots.execution.clone();
            async move { execution.acquire_owned().await.unwrap() }
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "the second request must not start");

        drop(held);
        let _second = waiter.await.expect("the waiter should be handed the slot");
        assert_eq!(slots.execution_used(), 1);
    }
}
