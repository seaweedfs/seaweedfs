use std::time::Duration;

use anyhow::{anyhow, Result};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;
use tracing::{info, warn};

use crate::config::WorkerOptions;
use crate::pb::{
    admin_to_worker_message::Body as AdminBody,
    plugin_control_service_client::PluginControlServiceClient,
    worker_to_admin_message::Body as WorkerBody, ConfigSchemaResponse, ExecuteJobRequest,
    JobCompleted, RunDetectionRequest, RunningWork, WorkerHeartbeat, WorkerHello,
};
use crate::registry::Registry;
use crate::senders::StreamSender;

/// The protocol version this worker speaks, sent in WorkerHello.
const PROTOCOL_VERSION: &str = "1";

/// Connect to admin and serve the registry until the context is cancelled,
/// reconnecting on failure. The stream is the only channel: everything admin
/// asks for and everything the worker reports flows through it.
pub async fn run(options: WorkerOptions, registry: Registry) -> Result<()> {
    if registry.is_empty() {
        return Err(anyhow!("no job handlers registered"));
    }
    loop {
        match serve_once(&options, &registry).await {
            Err(err) => warn!("worker stream ended: {err:#}"),
            // Admin closing a healthy stream is not an error, but reconnecting
            // in silence hides the reason - two workers sharing an id evict
            // each other and produce nothing but a login every few seconds.
            Ok(()) => warn!(
                "admin closed the stream; reconnecting in {:?}. If this repeats, check for \
                 another worker using the id {}",
                options.reconnect_delay, options.worker_id
            ),
        }
        tokio::time::sleep(options.reconnect_delay).await;
    }
}

async fn serve_once(options: &WorkerOptions, registry: &Registry) -> Result<()> {
    // Operators give the admin's HTTP address; the gRPC port is derived, the
    // same way the Go worker does it.
    let grpc_address = crate::address::server_to_grpc_address(&options.admin_address)
        .ok_or_else(|| anyhow!("cannot parse admin address {}", options.admin_address))?;
    let endpoint = format!("http://{grpc_address}");
    let channel = Channel::from_shared(endpoint)?
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await?;
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

    let heartbeat = spawn_heartbeat(sender.clone(), options.clone());

    while let Some(message) = inbound.message().await? {
        let request_id = message.request_id.clone();
        match message.body {
            Some(AdminBody::Hello(hello)) => {
                if !hello.accepted {
                    return Err(anyhow!("admin rejected this worker: {}", hello.message));
                }
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
            Some(AdminBody::RunDetectionRequest(request)) => {
                spawn_detection(registry.clone(), sender.clone(), request);
            }
            Some(AdminBody::ExecuteJobRequest(request)) => {
                spawn_execution(registry.clone(), sender.clone(), request);
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
                return Ok(());
            }
            None => {}
        }
    }

    heartbeat.abort();
    Ok(())
}

fn spawn_heartbeat(sender: StreamSender, options: WorkerOptions) -> tokio::task::JoinHandle<()> {
    // The handle has to be the heartbeat's own, or aborting it aborts nothing
    // and every reconnect leaves another ticker running.
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(options.heartbeat_interval);
        loop {
            ticker.tick().await;
            let beat = WorkerHeartbeat {
                worker_id: options.worker_id.clone(),
                running_work: Vec::<RunningWork>::new(),
                detection_slots_used: 0,
                detection_slots_total: options.max_detection_concurrency,
                execution_slots_used: 0,
                execution_slots_total: options.max_execution_concurrency,
                queued_jobs_by_type: Default::default(),
                metadata: Default::default(),
            };
            if sender.send(WorkerBody::Heartbeat(beat)).is_err() {
                return;
            }
        }
    })
}

fn spawn_detection(registry: Registry, sender: StreamSender, request: RunDetectionRequest) {
    tokio::spawn(async move {
        let Some(handler) = registry.get(&request.job_type) else {
            return;
        };
        if let Err(err) = handler.detect(&request, &sender).await {
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
        }
    });
}

fn spawn_execution(registry: Registry, sender: StreamSender, request: ExecuteJobRequest) {
    tokio::spawn(async move {
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
        if let Err(err) = handler.execute(&request, &sender).await {
            warn!("job {job_id} failed: {err:#}");
            let _ = sender.send(WorkerBody::JobCompleted(JobCompleted {
                request_id: request.request_id.clone(),
                job_id,
                job_type,
                success: false,
                error_message: format!("{err:#}"),
                ..Default::default()
            }));
        }
    });
}
