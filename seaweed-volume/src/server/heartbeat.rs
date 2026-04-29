//! Heartbeat client: registers the volume server with the master.
//!
//! Implements the bidirectional streaming `SendHeartbeat` RPC to the master,
//! matching Go's `server/volume_grpc_client_to_master.go`.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tracing::{error, info, warn};

use super::grpc_client::{build_grpc_endpoint, GRPC_MAX_MESSAGE_SIZE};
use super::volume_server::VolumeServerState;
use crate::pb::master_pb;
use crate::pb::master_pb::seaweed_client::SeaweedClient;
use crate::pb::volume_server_pb;
use crate::remote_storage::s3_tier::{S3TierBackend, S3TierConfig};
use crate::storage::store::Store;
use crate::storage::types::NeedleId;

const DUPLICATE_UUID_RETRY_MESSAGE: &str = "duplicate UUIDs detected, retrying connection";
const MAX_DUPLICATE_UUID_RETRIES: u32 = 3;

/// Configuration for the heartbeat client.
pub struct HeartbeatConfig {
    pub ip: String,
    pub port: u16,
    pub grpc_port: u16,
    pub public_url: String,
    pub data_center: String,
    pub rack: String,
    pub master_addresses: Vec<String>,
    pub pulse_seconds: u64,
}

/// Run the heartbeat loop using VolumeServerState.
///
/// Mirrors Go's `volume_grpc_client_to_master.go` heartbeat():
/// - On leader redirect: sleep 3s, then connect directly to the new leader
/// - On duplicate UUID error: exponential backoff (2s, 4s, 8s), exit after 3 retries
/// - On other errors: sleep pulse interval, reset to seed master list iteration
pub async fn run_heartbeat_with_state(
    config: HeartbeatConfig,
    state: Arc<VolumeServerState>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    info!(
        "Starting heartbeat to master nodes: {:?}",
        config.master_addresses
    );

    let pulse = Duration::from_secs(config.pulse_seconds.max(1));
    let mut new_leader: Option<String> = None;
    let mut duplicate_retry_count: u32 = 0;

    loop {
        for master_addr in &config.master_addresses {
            if is_stopping(&state) {
                state.is_heartbeating.store(false, Ordering::Relaxed);
                info!("Heartbeat stopping");
                return;
            }
            if shutdown_rx.try_recv().is_ok() {
                state.is_heartbeating.store(false, Ordering::Relaxed);
                info!("Heartbeat shutting down");
                return;
            }

            // If we have a leader redirect, sleep 3s then connect to the leader
            // instead of iterating through the seed list
            let target_addr = if let Some(ref leader) = new_leader {
                tokio::time::sleep(Duration::from_secs(3)).await;
                leader.clone()
            } else {
                master_addr.clone()
            };

            let grpc_addr = to_grpc_address(&target_addr);
            info!("Connecting heartbeat to master {}", grpc_addr);

            // Determine what action to take after the heartbeat attempt.
            // We convert the error to a string immediately so the non-Send
            // Box<dyn Error> is dropped before any .await point.
            enum PostAction {
                LeaderRedirect(String),
                Done,
                SleepDuplicate(Duration),
                SleepPulse,
            }
            let action = match do_heartbeat(&config, &state, &grpc_addr, &target_addr, pulse, &mut shutdown_rx)
                .await
            {
                Ok(Some(leader)) => {
                    info!("Master leader changed to {}", leader);
                    PostAction::LeaderRedirect(leader)
                }
                Ok(None) => {
                    duplicate_retry_count = 0;
                    PostAction::Done
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    // Drop `e` (non-Send) before any .await
                    drop(e);
                    warn!("Heartbeat to {} error: {}", grpc_addr, err_msg);

                    if err_msg.contains(DUPLICATE_UUID_RETRY_MESSAGE) {
                        if duplicate_retry_count >= MAX_DUPLICATE_UUID_RETRIES {
                            error!("Shut down Volume Server due to persistent duplicate volume directories after 3 retries");
                            error!(
                                "Please check if another volume server is using the same directory"
                            );
                            std::process::exit(1);
                        }
                        let retry_delay = duplicate_uuid_retry_delay(duplicate_retry_count);
                        duplicate_retry_count += 1;
                        warn!(
                            "Waiting {:?} before retrying due to duplicate UUID detection (attempt {}/3)...",
                            retry_delay, duplicate_retry_count
                        );
                        PostAction::SleepDuplicate(retry_delay)
                    } else {
                        duplicate_retry_count = 0;
                        PostAction::SleepPulse
                    }
                }
            };

            match action {
                PostAction::LeaderRedirect(leader) => {
                    new_leader = Some(leader);
                    break;
                }
                PostAction::Done => {
                    new_leader = None;
                }
                PostAction::SleepDuplicate(delay) => {
                    new_leader = None;
                    tokio::time::sleep(delay).await;
                }
                PostAction::SleepPulse => {
                    new_leader = None;
                    tokio::time::sleep(pulse).await;
                }
            }

            // If we connected to a leader (not seed list), break out after one attempt
            // so we either reconnect to the new leader or fall back to seed list
            if new_leader.is_some() {
                break;
            }
        }

        // If we have a leader redirect, skip the sleep and reconnect immediately
        if new_leader.is_some() {
            continue;
        }

        tokio::select! {
            _ = tokio::time::sleep(pulse) => {}
            _ = shutdown_rx.recv() => {
                state.is_heartbeating.store(false, Ordering::Relaxed);
                info!("Heartbeat shutting down");
                return;
            }
        }
    }
}

/// Convert a master address to a gRPC `host:port` target.
///
/// Mirrors Go's `pb.ServerToGrpcAddress()`:
/// - `host:port.grpcPort` returns `host:grpcPort` (explicit gRPC port).
/// - `host:port` returns `host:(port+10000)` (Go's default offset).
/// - Anything that fails to parse is returned unchanged.
pub fn to_grpc_address(master_addr: &str) -> String {
    if let Some((host, port_str)) = master_addr.rsplit_once(':') {
        // "host:port.grpcPort" — the part after the last '.' is the gRPC port.
        if let Some((_, grpc_port)) = port_str.rsplit_once('.') {
            if grpc_port.parse::<u16>().is_ok() {
                return format!("{}:{}", host, grpc_port);
            }
        }
        if let Ok(port) = port_str.parse::<u16>() {
            let grpc_port = port + 10000;
            return format!("{}:{}", host, grpc_port);
        }
    }
    master_addr.to_string()
}

/// Call GetMasterConfiguration on seed masters before starting the heartbeat loop.
/// Mirrors Go's `checkWithMaster()` in `volume_grpc_client_to_master.go`.
/// Retries across all seed masters with a 1790ms sleep between rounds (matching Go).
/// Stores metrics address/interval from the response into server state.
async fn check_with_master(config: &HeartbeatConfig, state: &Arc<VolumeServerState>) {
    loop {
        for master_addr in &config.master_addresses {
            let grpc_addr = to_grpc_address(master_addr);
            match try_get_master_configuration(&grpc_addr, state.outgoing_grpc_tls.as_ref()).await {
                Ok(resp) => {
                    let changed = apply_metrics_push_settings(
                        state,
                        &resp.metrics_address,
                        resp.metrics_interval_seconds,
                    );
                    if changed {
                        state.metrics_notify.notify_waiters();
                    }
                    apply_storage_backends(state, &resp.storage_backends);
                    info!(
                        "Got master configuration from {}: metrics_address={}, metrics_interval={}s",
                        master_addr, resp.metrics_address, resp.metrics_interval_seconds
                    );
                    return;
                }
                Err(e) => {
                    warn!("checkWithMaster {}: {}", master_addr, e);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(1790)).await;
    }
}

pub async fn prime_master_configuration(config: &HeartbeatConfig, state: &Arc<VolumeServerState>) {
    check_with_master(config, state).await;
}

pub async fn try_get_master_configuration(
    grpc_addr: &str,
    tls: Option<&super::grpc_client::OutgoingGrpcTlsConfig>,
) -> Result<master_pb::GetMasterConfigurationResponse, Box<dyn std::error::Error>> {
    let channel = build_grpc_endpoint(grpc_addr, tls)?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .connect()
        .await?;
    let mut client = SeaweedClient::with_interceptor(
        channel,
        super::request_id::outgoing_request_id_interceptor,
    )
    .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
    .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE);
    let resp = client
        .get_master_configuration(master_pb::GetMasterConfigurationRequest {})
        .await?;
    Ok(resp.into_inner())
}

fn is_stopping(state: &VolumeServerState) -> bool {
    *state.is_stopping.read().unwrap()
}

fn duplicate_uuid_retry_delay(retry_count: u32) -> Duration {
    Duration::from_secs((1u64 << retry_count) * 2)
}

fn duplicate_directories(store: &Store, duplicated_uuids: &[String]) -> Vec<String> {
    let mut duplicate_dirs = Vec::new();
    for loc in &store.locations {
        if duplicated_uuids
            .iter()
            .any(|uuid| uuid == &loc.directory_uuid)
        {
            duplicate_dirs.push(loc.directory.clone());
        }
    }
    duplicate_dirs
}

fn apply_master_volume_options(store: &Store, hb_resp: &master_pb::HeartbeatResponse) -> bool {
    let mut volume_opts_changed = false;
    if store.get_preallocate() != hb_resp.preallocate {
        store.set_preallocate(hb_resp.preallocate);
        volume_opts_changed = true;
    }
    if hb_resp.volume_size_limit > 0
        && store.volume_size_limit.load(Ordering::Relaxed) != hb_resp.volume_size_limit
    {
        store
            .volume_size_limit
            .store(hb_resp.volume_size_limit, Ordering::Relaxed);
        volume_opts_changed = true;
    }

    volume_opts_changed && store.maybe_adjust_volume_max()
}

type EcShardDeltaKey = (u32, String, u32, u32);

fn collect_ec_shard_delta_messages(
    store: &Store,
) -> HashMap<EcShardDeltaKey, master_pb::VolumeEcShardInformationMessage> {
    let mut messages = HashMap::new();

    for (disk_id, loc) in store.locations.iter().enumerate() {
        for (_, ec_vol) in loc.ec_volumes() {
            for shard in ec_vol.shards.iter().flatten() {
                messages.insert(
                    (
                        ec_vol.volume_id.0,
                        ec_vol.collection.clone(),
                        disk_id as u32,
                        shard.shard_id as u32,
                    ),
                    master_pb::VolumeEcShardInformationMessage {
                        id: ec_vol.volume_id.0,
                        collection: ec_vol.collection.clone(),
                        ec_index_bits: 1u32 << shard.shard_id,
                        shard_sizes: vec![shard.file_size()],
                        disk_type: ec_vol.disk_type.to_string(),
                        expire_at_sec: ec_vol.expire_at_sec,
                        disk_id: disk_id as u32,
                        ..Default::default()
                    },
                );
            }
        }
    }

    messages
}

fn diff_ec_shard_delta_messages(
    previous: &HashMap<EcShardDeltaKey, master_pb::VolumeEcShardInformationMessage>,
    current: &HashMap<EcShardDeltaKey, master_pb::VolumeEcShardInformationMessage>,
) -> (
    Vec<master_pb::VolumeEcShardInformationMessage>,
    Vec<master_pb::VolumeEcShardInformationMessage>,
) {
    let mut new_ec_shards = Vec::new();
    let mut deleted_ec_shards = Vec::new();

    for (key, message) in current {
        if previous.get(key) != Some(message) {
            new_ec_shards.push(message.clone());
        }
    }

    for (key, message) in previous {
        if !current.contains_key(key) {
            let mut deleted = message.clone();
            deleted.shard_sizes = vec![0];
            deleted_ec_shards.push(deleted);
        }
    }

    (new_ec_shards, deleted_ec_shards)
}

/// Perform one heartbeat session with a master server.
async fn do_heartbeat(
    config: &HeartbeatConfig,
    state: &Arc<VolumeServerState>,
    grpc_addr: &str,
    current_master: &str,
    pulse: Duration,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let channel = build_grpc_endpoint(grpc_addr, state.outgoing_grpc_tls.as_ref())?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .connect()
        .await?;

    let mut client = SeaweedClient::with_interceptor(
        channel,
        super::request_id::outgoing_request_id_interceptor,
    )
    .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
    .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE);

    let (tx, rx) = tokio::sync::mpsc::channel::<master_pb::Heartbeat>(32);

    // Keep track of what we sent, to generate delta updates
    let initial_hb = collect_heartbeat(config, state);
    let mut last_volumes: HashMap<u32, master_pb::VolumeInformationMessage> = initial_hb
        .volumes
        .iter()
        .map(|v| (v.id, v.clone()))
        .collect();
    let mut last_ec_shards = {
        let store = state.store.read().unwrap();
        collect_ec_shard_delta_messages(&store)
    };

    // Send initial heartbeats BEFORE calling send_heartbeat to avoid deadlock:
    // the server won't send response headers until it receives the first message,
    // but send_heartbeat().await waits for response headers.
    tx.send(initial_hb).await?;
    tx.send(collect_ec_heartbeat(config, state)).await?;

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let mut response_stream = client.send_heartbeat(stream).await?.into_inner();

    info!("Heartbeat stream established with {}", grpc_addr);
    if is_stopping(state) {
        state.is_heartbeating.store(false, Ordering::Relaxed);
        send_deregister_heartbeat(config, state, &tx).await;
        info!("Heartbeat stopping");
        return Ok(None);
    }
    state.is_heartbeating.store(true, Ordering::Relaxed);

    let mut volume_tick = tokio::time::interval(pulse);
    let mut ec_tick = tokio::time::interval(pulse * 17);
    volume_tick.tick().await;
    ec_tick.tick().await;

    loop {
        tokio::select! {
            resp = response_stream.message() => {
                match resp {
                    Ok(Some(hb_resp)) => {
                        // Match Go ordering: DuplicatedUuids first, then volume
                        // options, then leader redirect.
                        if !hb_resp.duplicated_uuids.is_empty() {
                            let duplicate_dirs = {
                                let store = state.store.read().unwrap();
                                duplicate_directories(&store, &hb_resp.duplicated_uuids)
                            };
                            error!(
                                "Master reported duplicate volume directories: {:?}",
                                duplicate_dirs
                            );
                            return Err(format!(
                                "{}: {:?}",
                                DUPLICATE_UUID_RETRY_MESSAGE, duplicate_dirs
                            )
                            .into());
                        }
                        let changed = {
                            let s = state.store.read().unwrap();
                            apply_master_volume_options(&s, &hb_resp)
                        };
                        if changed {
                            let adjusted_hb = collect_heartbeat(config, state);
                            last_volumes =
                                adjusted_hb.volumes.iter().map(|v| (v.id, v.clone())).collect();
                            last_ec_shards = {
                                let store = state.store.read().unwrap();
                                collect_ec_shard_delta_messages(&store)
                            };
                            if tx.send(adjusted_hb).await.is_err() {
                                return Ok(None);
                            }
                        }
                        let metrics_changed = apply_metrics_push_settings(
                            state,
                            &hb_resp.metrics_address,
                            hb_resp.metrics_interval_seconds,
                        );
                        if metrics_changed {
                            state.metrics_notify.notify_waiters();
                        }
                        // Match Go: only redirect if leader is non-empty AND
                        // different from the current master we're connected to.
                        if !hb_resp.leader.is_empty() && current_master != hb_resp.leader {
                            return Ok(Some(hb_resp.leader));
                        }
                    }
                    Ok(None) => return Ok(None),
                    Err(e) => return Err(Box::new(e)),
                }
            }

            _ = volume_tick.tick() => {
                {
                    let s = state.store.read().unwrap();
                    s.maybe_adjust_volume_max();
                }
                let current_hb = collect_heartbeat(config, state);
                last_volumes = current_hb.volumes.iter().map(|v| (v.id, v.clone())).collect();
                last_ec_shards = {
                    let store = state.store.read().unwrap();
                    collect_ec_shard_delta_messages(&store)
                };
                if tx.send(current_hb).await.is_err() {
                    return Ok(None);
                }
            }

            _ = ec_tick.tick() => {
                let current_ec_hb = collect_ec_heartbeat(config, state);
                last_ec_shards = {
                    let store = state.store.read().unwrap();
                    collect_ec_shard_delta_messages(&store)
                };
                if tx.send(current_ec_hb).await.is_err() {
                    return Ok(None);
                }
            }

            _ = state.volume_state_notify.notified() => {
                if is_stopping(state) {
                    state.is_heartbeating.store(false, Ordering::Relaxed);
                    send_deregister_heartbeat(config, state, &tx).await;
                    info!("Heartbeat stopping");
                    return Ok(None);
                }
                let current_hb = collect_heartbeat(config, state);
                let current_volumes: HashMap<u32, _> = current_hb.volumes.iter().map(|v| (v.id, v.clone())).collect();
                let current_ec_shards = {
                    let store = state.store.read().unwrap();
                    collect_ec_shard_delta_messages(&store)
                };

                let mut new_vols = Vec::new();
                let mut del_vols = Vec::new();

                for (id, vol) in &current_volumes {
                    if !last_volumes.contains_key(id) {
                        new_vols.push(master_pb::VolumeShortInformationMessage {
                            id: *id,
                            collection: vol.collection.clone(),
                            version: vol.version,
                            replica_placement: vol.replica_placement,
                            ttl: vol.ttl,
                            disk_type: vol.disk_type.clone(),
                            disk_id: vol.disk_id,
                        });
                    }
                }

                for (id, vol) in &last_volumes {
                    if !current_volumes.contains_key(id) {
                        del_vols.push(master_pb::VolumeShortInformationMessage {
                            id: *id,
                            collection: vol.collection.clone(),
                            version: vol.version,
                            replica_placement: vol.replica_placement,
                            ttl: vol.ttl,
                            disk_type: vol.disk_type.clone(),
                            disk_id: vol.disk_id,
                        });
                    }
                }

                let (new_ec_shards, deleted_ec_shards) =
                    diff_ec_shard_delta_messages(&last_ec_shards, &current_ec_shards);

                // Collect current state for state-only or combined delta heartbeats.
                // Mirrors Go's StateUpdateChan case which sends state changes immediately.
                let current_state = Some(volume_server_pb::VolumeServerState {
                    maintenance: state.maintenance.load(Ordering::Relaxed),
                    version: state.state_version.load(Ordering::Relaxed),
                });

                if !new_vols.is_empty()
                    || !del_vols.is_empty()
                    || !new_ec_shards.is_empty()
                    || !deleted_ec_shards.is_empty()
                {
                    let delta_hb = master_pb::Heartbeat {
                        ip: config.ip.clone(),
                        port: config.port as u32,
                        grpc_port: config.grpc_port as u32,
                        public_url: config.public_url.clone(),
                        data_center: config.data_center.clone(),
                        rack: config.rack.clone(),
                        new_volumes: new_vols,
                        deleted_volumes: del_vols,
                        new_ec_shards,
                        deleted_ec_shards,
                        state: current_state,
                        ..Default::default()
                    };
                    if tx.send(delta_hb).await.is_err() {
                        return Ok(None);
                    }
                    last_volumes = current_volumes;
                    last_ec_shards = current_ec_shards;
                } else {
                    // State-only heartbeat (e.g., MarkReadonly/MarkWritable changed state
                    // without adding/removing volumes). Mirrors Go's StateUpdateChan case.
                    let state_hb = master_pb::Heartbeat {
                        ip: config.ip.clone(),
                        port: config.port as u32,
                        grpc_port: config.grpc_port as u32,
                        data_center: config.data_center.clone(),
                        rack: config.rack.clone(),
                        state: current_state,
                        ..Default::default()
                    };
                    if tx.send(state_hb).await.is_err() {
                        return Ok(None);
                    }
                }
            }

            _ = shutdown_rx.recv() => {
                state.is_heartbeating.store(false, Ordering::Relaxed);
                send_deregister_heartbeat(config, state, &tx).await;
                info!("Sent deregistration heartbeat");
                return Ok(None);
            }
        }
    }
}

async fn send_deregister_heartbeat(
    config: &HeartbeatConfig,
    state: &Arc<VolumeServerState>,
    tx: &tokio::sync::mpsc::Sender<master_pb::Heartbeat>,
) {
    let empty = {
        let store = state.store.read().unwrap();
        let (location_uuids, disk_tags) = collect_location_metadata(&store);
        master_pb::Heartbeat {
            id: store.id.clone(),
            ip: config.ip.clone(),
            port: config.port as u32,
            public_url: config.public_url.clone(),
            max_file_key: 0,
            data_center: config.data_center.clone(),
            rack: config.rack.clone(),
            has_no_volumes: true,
            has_no_ec_shards: true,
            grpc_port: config.grpc_port as u32,
            location_uuids,
            disk_tags,
            ..Default::default()
        }
    };
    let _ = tx.send(empty).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
}

fn apply_metrics_push_settings(
    state: &VolumeServerState,
    address: &str,
    interval_seconds: u32,
) -> bool {
    let mut runtime = state.metrics_runtime.write().unwrap();
    if runtime.push_gateway.address == address
        && runtime.push_gateway.interval_seconds == interval_seconds
    {
        return false;
    }
    runtime.push_gateway.address = address.to_string();
    runtime.push_gateway.interval_seconds = interval_seconds;
    true
}

fn apply_storage_backends(
    state: &VolumeServerState,
    storage_backends: &[master_pb::StorageBackend],
) {
    if storage_backends.is_empty() {
        return;
    }

    let mut registry = state.s3_tier_registry.write().unwrap();
    let mut global_registry = crate::remote_storage::s3_tier::global_s3_tier_registry()
        .write()
        .unwrap();
    for backend in storage_backends {
        if backend.r#type != "s3" {
            continue;
        }

        let properties = &backend.properties;
        let config = S3TierConfig {
            access_key: properties
                .get("aws_access_key_id")
                .cloned()
                .unwrap_or_default(),
            secret_key: properties
                .get("aws_secret_access_key")
                .cloned()
                .unwrap_or_default(),
            region: properties.get("region").cloned().unwrap_or_default(),
            bucket: properties.get("bucket").cloned().unwrap_or_default(),
            endpoint: properties.get("endpoint").cloned().unwrap_or_default(),
            storage_class: properties.get("storage_class").cloned().unwrap_or_default(),
            force_path_style: parse_bool_property(properties.get("force_path_style")),
        };

        let backend_id = if backend.id.is_empty() {
            "default"
        } else {
            backend.id.as_str()
        };
        register_s3_backend(&mut registry, backend, backend_id, &config);
        register_s3_backend(&mut global_registry, backend, backend_id, &config);
    }
}

fn register_s3_backend(
    registry: &mut crate::remote_storage::s3_tier::S3TierRegistry,
    backend: &master_pb::StorageBackend,
    backend_id: &str,
    config: &S3TierConfig,
) {
    let qualified_name = format!("{}.{}", backend.r#type, backend_id);
    if registry.get(&qualified_name).is_none() {
        registry.register(qualified_name, S3TierBackend::new(config));
    }
    if backend_id == "default" && registry.get(&backend.r#type).is_none() {
        registry.register(backend.r#type.clone(), S3TierBackend::new(config));
    }
}

fn parse_bool_property(value: Option<&String>) -> bool {
    value
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "t" | "true" | "y" | "yes" | "on"
            )
        })
        .unwrap_or(true)
}

/// Collect volume information into a Heartbeat message.
fn collect_heartbeat(
    config: &HeartbeatConfig,
    state: &Arc<VolumeServerState>,
) -> master_pb::Heartbeat {
    let mut store = state.store.write().unwrap();
    let (ec_shards, deleted_ec_shards) = store.delete_expired_ec_volumes();
    build_heartbeat_with_ec_status(
        config,
        &mut store,
        deleted_ec_shards,
        ec_shards.is_empty(),
    )
}

fn collect_location_metadata(store: &Store) -> (Vec<String>, Vec<master_pb::DiskTag>) {
    let location_uuids = store
        .locations
        .iter()
        .map(|loc| loc.directory_uuid.clone())
        .collect();
    let disk_tags = store
        .locations
        .iter()
        .enumerate()
        .map(|(disk_id, loc)| master_pb::DiskTag {
            disk_id: disk_id as u32,
            tags: loc.tags.clone(),
        })
        .collect();
    (location_uuids, disk_tags)
}

#[cfg(test)]
fn build_heartbeat(config: &HeartbeatConfig, store: &mut Store) -> master_pb::Heartbeat {
    let has_no_ec_shards = collect_live_ec_shards(store, false).is_empty();
    build_heartbeat_with_ec_status(config, store, Vec::new(), has_no_ec_shards)
}

fn build_heartbeat_with_ec_status(
    config: &HeartbeatConfig,
    store: &mut Store,
    deleted_ec_shards: Vec<master_pb::VolumeEcShardInformationMessage>,
    has_no_ec_shards: bool,
) -> master_pb::Heartbeat {
    const MAX_TTL_VOLUME_REMOVAL_DELAY: u32 = 10;

    #[derive(Default)]
    struct ReadOnlyCounts {
        is_read_only: u32,
        no_write_or_delete: u32,
        no_write_can_delete: u32,
        is_disk_space_low: u32,
    }

    let mut volumes = Vec::new();
    let mut max_file_key = NeedleId(0);
    let mut max_volume_counts: HashMap<String, u32> = HashMap::new();

    // Collect per-collection disk size and read-only counts for metrics
    let mut disk_sizes: HashMap<String, (u64, u64)> = HashMap::new(); // (normal, deleted)
    let mut ro_counts: HashMap<String, ReadOnlyCounts> = HashMap::new();

    let volume_size_limit = store.volume_size_limit.load(Ordering::Relaxed);

    for (disk_id, loc) in store.locations.iter_mut().enumerate() {
        let disk_type_str = loc.disk_type.to_string();
        let mut effective_max_count = loc.max_volume_count.load(Ordering::Relaxed);
        if loc.is_disk_space_low.load(Ordering::Relaxed) {
            let used_slots = loc.volumes_len() as i32
                + ((loc.ec_shard_count()
                    + crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT
                    - 1)
                    / crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT)
                    as i32;
            effective_max_count = used_slots;
        }
        if effective_max_count < 0 {
            effective_max_count = 0;
        }
        *max_volume_counts.entry(disk_type_str).or_insert(0) += effective_max_count as u32;

        let mut delete_vids = Vec::new();
        for (_, vol) in loc.iter_volumes() {
            let cur_max = vol.max_file_key();
            if cur_max > max_file_key {
                max_file_key = cur_max;
            }

            let volume_size = vol.dat_file_size().unwrap_or(0);
            let mut should_delete_volume = false;

            if vol.last_io_error().is_some() {
                delete_vids.push(vol.id);
                should_delete_volume = true;
            } else if !vol.is_expired(volume_size, volume_size_limit) {
                let (remote_storage_name, remote_storage_key) = vol.remote_storage_name_key();
                volumes.push(master_pb::VolumeInformationMessage {
                    id: vol.id.0,
                    size: volume_size,
                    collection: vol.collection.clone(),
                    file_count: vol.file_count() as u64,
                    delete_count: vol.deleted_count() as u64,
                    deleted_byte_count: vol.deleted_size(),
                    read_only: vol.is_read_only(),
                    replica_placement: vol.super_block.replica_placement.to_byte() as u32,
                    version: vol.super_block.version.0 as u32,
                    ttl: vol.super_block.ttl.to_u32(),
                    compact_revision: vol.last_compact_revision() as u32,
                    modified_at_second: vol.last_modified_ts() as i64,
                    disk_type: loc.disk_type.to_string(),
                    disk_id: disk_id as u32,
                    remote_storage_name,
                    remote_storage_key,
                    ..Default::default()
                });
            } else if vol.is_expired_long_enough(MAX_TTL_VOLUME_REMOVAL_DELAY) {
                delete_vids.push(vol.id);
                should_delete_volume = true;
            }

            // Track disk size by collection
            let entry = disk_sizes.entry(vol.collection.clone()).or_insert((0, 0));
            if !should_delete_volume {
                entry.0 += volume_size;
                entry.1 += vol.deleted_size();
            }

            let read_only = ro_counts.entry(vol.collection.clone()).or_default();
            if !should_delete_volume && vol.is_read_only() {
                read_only.is_read_only += 1;
                if vol.is_no_write_or_delete() {
                    read_only.no_write_or_delete += 1;
                }
                if vol.is_no_write_can_delete() {
                    read_only.no_write_can_delete += 1;
                }
                if loc.is_disk_space_low.load(Ordering::Relaxed) {
                    read_only.is_disk_space_low += 1;
                }
            }

        }

        for vid in delete_vids {
            let _ = loc.delete_volume(vid, false);
        }
    }

    // Update disk size and read-only gauges
    for (col, (normal, deleted)) in &disk_sizes {
        crate::metrics::DISK_SIZE_GAUGE
            .with_label_values(&[col, crate::metrics::DISK_SIZE_LABEL_NORMAL])
            .set(*normal as f64);
        crate::metrics::DISK_SIZE_GAUGE
            .with_label_values(&[col, crate::metrics::DISK_SIZE_LABEL_DELETED_BYTES])
            .set(*deleted as f64);
    }
    for (col, counts) in &ro_counts {
        crate::metrics::READ_ONLY_VOLUME_GAUGE
            .with_label_values(&[col, crate::metrics::READ_ONLY_LABEL_IS_READ_ONLY])
            .set(counts.is_read_only as f64);
        crate::metrics::READ_ONLY_VOLUME_GAUGE
            .with_label_values(&[col, crate::metrics::READ_ONLY_LABEL_NO_WRITE_OR_DELETE])
            .set(counts.no_write_or_delete as f64);
        crate::metrics::READ_ONLY_VOLUME_GAUGE
            .with_label_values(&[col, crate::metrics::READ_ONLY_LABEL_NO_WRITE_CAN_DELETE])
            .set(counts.no_write_can_delete as f64);
        crate::metrics::READ_ONLY_VOLUME_GAUGE
            .with_label_values(&[col, crate::metrics::READ_ONLY_LABEL_IS_DISK_SPACE_LOW])
            .set(counts.is_disk_space_low as f64);
    }
    // Update max volumes gauge
    let total_max: i64 = max_volume_counts.values().map(|v| *v as i64).sum();
    crate::metrics::MAX_VOLUMES.set(total_max);

    let has_no_volumes = volumes.is_empty();
    let (location_uuids, disk_tags) = collect_location_metadata(store);

    master_pb::Heartbeat {
        id: store.id.clone(),
        ip: config.ip.clone(),
        port: config.port as u32,
        public_url: config.public_url.clone(),
        max_file_key: max_file_key.0,
        data_center: config.data_center.clone(),
        rack: config.rack.clone(),
        admin_port: config.port as u32,
        volumes,
        deleted_ec_shards,
        has_no_volumes,
        has_no_ec_shards,
        max_volume_counts,
        grpc_port: config.grpc_port as u32,
        location_uuids,
        disk_tags,
        ..Default::default()
    }
}

fn collect_live_ec_shards(
    store: &Store,
    update_metrics: bool,
) -> Vec<master_pb::VolumeEcShardInformationMessage> {
    let mut ec_shards = Vec::new();
    let mut ec_sizes: HashMap<String, u64> = HashMap::new();

    for (disk_id, loc) in store.locations.iter().enumerate() {
        for (_, ec_vol) in loc.ec_volumes() {
            for message in ec_vol.to_volume_ec_shard_information_messages(disk_id as u32) {
                if update_metrics {
                    let total_size: u64 = message
                        .shard_sizes
                        .iter()
                        .map(|size| (*size).max(0) as u64)
                        .sum();
                    *ec_sizes.entry(message.collection.clone()).or_insert(0) += total_size;
                }
                ec_shards.push(message);
            }
        }
    }

    if update_metrics {
        for (col, size) in &ec_sizes {
            crate::metrics::DISK_SIZE_GAUGE
                .with_label_values(&[col, crate::metrics::DISK_SIZE_LABEL_EC])
                .set(*size as f64);
        }
    }

    ec_shards
}

/// Collect EC shard information into a Heartbeat message.
fn collect_ec_heartbeat(config: &HeartbeatConfig, state: &Arc<VolumeServerState>) -> master_pb::Heartbeat {
    let store = state.store.read().unwrap();
    let ec_shards = collect_live_ec_shards(&store, true);

    let has_no = ec_shards.is_empty();
    master_pb::Heartbeat {
        ip: config.ip.clone(),
        port: config.port as u32,
        grpc_port: config.grpc_port as u32,
        data_center: config.data_center.clone(),
        rack: config.rack.clone(),
        ec_shards,
        has_no_ec_shards: has_no,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MinFreeSpace;
    use crate::config::ReadMode;
    use crate::metrics::{
        DISK_SIZE_GAUGE, DISK_SIZE_LABEL_DELETED_BYTES, DISK_SIZE_LABEL_EC,
        DISK_SIZE_LABEL_NORMAL, READ_ONLY_LABEL_IS_DISK_SPACE_LOW,
        READ_ONLY_LABEL_IS_READ_ONLY, READ_ONLY_LABEL_NO_WRITE_CAN_DELETE,
        READ_ONLY_LABEL_NO_WRITE_OR_DELETE, READ_ONLY_VOLUME_GAUGE,
    };
    use crate::remote_storage::s3_tier::S3TierRegistry;
    use crate::security::{Guard, SigningKey};
    use crate::storage::needle_map::NeedleMapKind;
    use crate::storage::types::{DiskType, Version, VolumeId};
    use std::sync::atomic::Ordering;
    use std::sync::RwLock;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_config() -> HeartbeatConfig {
        HeartbeatConfig {
            ip: "127.0.0.1".to_string(),
            port: 8080,
            grpc_port: 18080,
            public_url: "127.0.0.1:8080".to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            master_addresses: Vec::new(),
            pulse_seconds: 5,
        }
    }

    fn test_state_with_store(store: Store) -> Arc<VolumeServerState> {
        Arc::new(VolumeServerState {
            store: RwLock::new(store),
            guard: RwLock::new(Guard::new(
                &[],
                SigningKey(vec![]),
                0,
                SigningKey(vec![]),
                0,
            )),
            is_stopping: RwLock::new(false),
            maintenance: std::sync::atomic::AtomicBool::new(false),
            state_version: std::sync::atomic::AtomicU32::new(0),
            concurrent_upload_limit: 0,
            concurrent_download_limit: 0,
            inflight_upload_data_timeout: std::time::Duration::from_secs(60),
            inflight_download_data_timeout: std::time::Duration::from_secs(60),
            inflight_upload_bytes: std::sync::atomic::AtomicI64::new(0),
            inflight_download_bytes: std::sync::atomic::AtomicI64::new(0),
            upload_notify: tokio::sync::Notify::new(),
            download_notify: tokio::sync::Notify::new(),
            data_center: String::new(),
            rack: String::new(),
            file_size_limit_bytes: 0,
            maintenance_byte_per_second: 0,
            is_heartbeating: std::sync::atomic::AtomicBool::new(false),
            has_master: true,
            pre_stop_seconds: 0,
            volume_state_notify: tokio::sync::Notify::new(),
            write_queue: std::sync::OnceLock::new(),
            s3_tier_registry: std::sync::RwLock::new(S3TierRegistry::new()),
            read_mode: ReadMode::Local,
            master_url: String::new(),
            master_urls: Vec::new(),
            self_url: String::new(),
            http_client: reqwest::Client::new(),
            outgoing_http_scheme: "http".to_string(),
            outgoing_grpc_tls: None,
            metrics_runtime: std::sync::RwLock::new(Default::default()),
            metrics_notify: tokio::sync::Notify::new(),
            fix_jpg_orientation: false,
            has_slow_read: true,
            read_buffer_size_bytes: 4 * 1024 * 1024,
            security_file: String::new(),
            cli_white_list: vec![],
            state_file_path: String::new(),
        })
    }

    #[test]
    fn test_to_grpc_address_default_offset() {
        assert_eq!(to_grpc_address("10.0.0.1:9333"), "10.0.0.1:19333");
        assert_eq!(to_grpc_address("localhost:9333"), "localhost:19333");
    }

    #[test]
    fn test_to_grpc_address_explicit_grpc_port() {
        // host:port.grpcPort form — gRPC port is what's after the dot.
        assert_eq!(to_grpc_address("10.85.183.6:5300.6300"), "10.85.183.6:6300");
        assert_eq!(to_grpc_address("master.local:9333.19333"), "master.local:19333");
    }

    #[test]
    fn test_to_grpc_address_returns_input_when_unparseable() {
        assert_eq!(to_grpc_address(""), "");
        assert_eq!(to_grpc_address("no-port"), "no-port");
        assert_eq!(to_grpc_address("host:not-a-port"), "host:not-a-port");
    }

    #[test]
    fn test_build_heartbeat_includes_store_identity_and_disk_metadata() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path().to_str().unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store.id = "volume-node-a".to_string();
        store
            .add_location(
                dir,
                dir,
                3,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                vec!["fast".to_string(), "ssd".to_string()],
            )
            .unwrap();
        store
            .add_volume(
                VolumeId(7),
                "pics",
                None,
                None,
                0,
                DiskType::HardDrive,
                Version::current(),
            )
            .unwrap();

        let heartbeat = build_heartbeat(&test_config(), &mut store);

        assert_eq!(heartbeat.id, "volume-node-a");
        assert_eq!(heartbeat.volumes.len(), 1);
        assert!(!heartbeat.has_no_volumes);
        assert_eq!(
            heartbeat.location_uuids,
            vec![store.locations[0].directory_uuid.clone()]
        );
        assert_eq!(heartbeat.disk_tags.len(), 1);
        assert_eq!(heartbeat.disk_tags[0].disk_id, 0);
        assert_eq!(
            heartbeat.disk_tags[0].tags,
            vec!["fast".to_string(), "ssd".to_string()]
        );
    }

    #[test]
    fn test_build_heartbeat_marks_empty_store_as_has_no_volumes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path().to_str().unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store.id = "volume-node-b".to_string();
        store
            .add_location(
                dir,
                dir,
                2,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();

        let heartbeat = build_heartbeat(&test_config(), &mut store);

        assert!(heartbeat.volumes.is_empty());
        assert!(heartbeat.has_no_volumes);
    }

    #[test]
    fn test_build_heartbeat_tracks_go_read_only_labels_and_disk_id() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path().to_str().unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir,
                dir,
                8,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();
        store
            .add_volume(
                VolumeId(17),
                "heartbeat_metrics_case",
                None,
                None,
                0,
                DiskType::HardDrive,
                Version::current(),
            )
            .unwrap();
        store.locations[0]
            .is_disk_space_low
            .store(true, Ordering::Relaxed);

        {
            let (_, volume) = store.find_volume_mut(VolumeId(17)).unwrap();
            volume.set_read_only().unwrap();
            volume.volume_info.files.push(Default::default());
            volume.refresh_remote_write_mode();
        }

        let heartbeat = build_heartbeat(&test_config(), &mut store);
        let collection = "heartbeat_metrics_case";
        let disk_type = store.locations[0].disk_type.to_string();

        assert_eq!(heartbeat.volumes.len(), 1);
        assert_eq!(heartbeat.volumes[0].disk_id, 0);
        assert_eq!(heartbeat.max_volume_counts[&disk_type], 1);
        assert_eq!(
            READ_ONLY_VOLUME_GAUGE
                .with_label_values(&[collection, READ_ONLY_LABEL_IS_READ_ONLY])
                .get(),
            1.0
        );
        assert_eq!(
            READ_ONLY_VOLUME_GAUGE
                .with_label_values(&[collection, READ_ONLY_LABEL_NO_WRITE_OR_DELETE])
                .get(),
            0.0
        );
        assert_eq!(
            READ_ONLY_VOLUME_GAUGE
                .with_label_values(&[collection, READ_ONLY_LABEL_NO_WRITE_CAN_DELETE])
                .get(),
            1.0
        );
        assert_eq!(
            READ_ONLY_VOLUME_GAUGE
                .with_label_values(&[collection, READ_ONLY_LABEL_IS_DISK_SPACE_LOW])
                .get(),
            1.0
        );
        assert_eq!(
            DISK_SIZE_GAUGE
                .with_label_values(&[collection, DISK_SIZE_LABEL_NORMAL])
                .get(),
            crate::storage::super_block::SUPER_BLOCK_SIZE as f64
        );
        assert_eq!(
            DISK_SIZE_GAUGE
                .with_label_values(&[collection, DISK_SIZE_LABEL_DELETED_BYTES])
                .get(),
            0.0
        );
    }

    #[test]
    fn test_collect_ec_heartbeat_sets_go_metadata_and_ec_metrics() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path().to_str().unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir,
                dir,
                8,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();

        let shard_path = format!("{}/ec_metrics_case_27.ec00", dir);
        std::fs::write(&shard_path, b"ec-shard").unwrap();
        store.locations[0]
            .mount_ec_shards(VolumeId(27), "ec_metrics_case", &[0])
            .unwrap();

        let state = test_state_with_store(store);
        let heartbeat = collect_ec_heartbeat(&test_config(), &state);

        assert_eq!(heartbeat.ec_shards.len(), 1);
        assert!(!heartbeat.has_no_ec_shards);
        assert_eq!(heartbeat.ec_shards[0].disk_id, 0);
        assert_eq!(
            heartbeat.ec_shards[0].disk_type,
            state.store.read().unwrap().locations[0].disk_type.to_string()
        );
        assert_eq!(heartbeat.ec_shards[0].ec_index_bits, 1);
        assert_eq!(heartbeat.ec_shards[0].shard_sizes, vec![8]);
        assert_eq!(
            DISK_SIZE_GAUGE
                .with_label_values(&["ec_metrics_case", DISK_SIZE_LABEL_EC])
                .get(),
            8.0
        );
    }

    #[test]
    fn test_collect_heartbeat_deletes_expired_ec_volumes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path().to_str().unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir,
                dir,
                8,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();

        std::fs::write(format!("{}/expired_heartbeat_ec_31.ec00", dir), b"expired").unwrap();
        store.locations[0]
            .mount_ec_shards(VolumeId(31), "expired_heartbeat_ec", &[0])
            .unwrap();
        store
            .find_ec_volume_mut(VolumeId(31))
            .unwrap()
            .expire_at_sec = 1;

        let state = test_state_with_store(store);
        let heartbeat = collect_heartbeat(&test_config(), &state);

        assert!(heartbeat.has_no_ec_shards);
        assert_eq!(heartbeat.deleted_ec_shards.len(), 1);
        assert_eq!(heartbeat.deleted_ec_shards[0].id, 31);
        assert!(!state.store.read().unwrap().has_ec_volume(VolumeId(31)));
    }

    #[test]
    fn test_collect_heartbeat_excludes_expired_volume_until_removal_delay() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path().to_str().unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir,
                dir,
                8,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();
        store.volume_size_limit.store(1, Ordering::Relaxed);
        store
            .add_volume(
                VolumeId(41),
                "expired_volume_case",
                None,
                Some(crate::storage::needle::ttl::TTL::read("20m").unwrap()),
                1024,
                DiskType::HardDrive,
                Version::current(),
            )
            .unwrap();
        let dat_path = {
            let (_, volume) = store.find_volume_mut(VolumeId(41)).unwrap();
            volume.set_last_io_error_for_test(None);
            volume.set_last_modified_ts_for_test(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
                    .saturating_sub(21 * 60),
            );
            volume.dat_path()
        };
        std::fs::OpenOptions::new()
            .write(true)
            .open(&dat_path)
            .unwrap()
            .set_len((crate::storage::super_block::SUPER_BLOCK_SIZE + 1) as u64)
            .unwrap();
        let volume_size_limit = store.volume_size_limit.load(Ordering::Relaxed);
        let (_, volume) = store.find_volume(VolumeId(41)).unwrap();
        assert!(volume.is_expired(volume.dat_file_size().unwrap_or(0), volume_size_limit));
        assert!(!volume.is_expired_long_enough(10));

        let heartbeat = build_heartbeat(&test_config(), &mut store);

        assert!(heartbeat.volumes.is_empty());
        assert!(store.has_volume(VolumeId(41)));
    }

    #[test]
    fn test_collect_heartbeat_deletes_io_error_volume() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path().to_str().unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir,
                dir,
                8,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();
        store
            .add_volume(
                VolumeId(51),
                "io_error_case",
                None,
                None,
                0,
                DiskType::HardDrive,
                Version::current(),
            )
            .unwrap();
        let (_, volume) = store.find_volume_mut(VolumeId(51)).unwrap();
        volume.set_last_io_error_for_test(Some("input/output error"));

        let heartbeat = build_heartbeat(&test_config(), &mut store);

        assert!(heartbeat.volumes.is_empty());
        assert!(!store.has_volume(VolumeId(51)));
    }

    #[test]
    fn test_build_heartbeat_includes_remote_storage_name_and_key() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path().to_str().unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir,
                dir,
                8,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();
        store
            .add_volume(
                VolumeId(71),
                "remote_volume_case",
                None,
                None,
                0,
                DiskType::HardDrive,
                Version::current(),
            )
            .unwrap();
        let (_, volume) = store.find_volume_mut(VolumeId(71)).unwrap();
        volume.volume_info.files.push(crate::storage::volume::PbRemoteFile {
            backend_type: "s3".to_string(),
            backend_id: "archive".to_string(),
            key: "volumes/71.dat".to_string(),
            ..Default::default()
        });
        volume.refresh_remote_write_mode();

        let heartbeat = build_heartbeat(&test_config(), &mut store);

        assert_eq!(heartbeat.volumes.len(), 1);
        assert_eq!(heartbeat.volumes[0].remote_storage_name, "s3.archive");
        assert_eq!(heartbeat.volumes[0].remote_storage_key, "volumes/71.dat");
    }

    #[test]
    fn test_apply_storage_backends_registers_s3_default_aliases() {
        let state = test_state_with_store(Store::new(NeedleMapKind::InMemory));
        // Do not call clear() on the global registry — other tests may be
        // running concurrently.  Just register our entries and verify them.

        apply_storage_backends(
            &state,
            &[master_pb::StorageBackend {
                r#type: "s3".to_string(),
                id: "default".to_string(),
                properties: std::collections::HashMap::from([
                    ("aws_access_key_id".to_string(), "access".to_string()),
                    ("aws_secret_access_key".to_string(), "secret".to_string()),
                    ("bucket".to_string(), "bucket-a".to_string()),
                    ("region".to_string(), "us-west-2".to_string()),
                    ("endpoint".to_string(), "http://127.0.0.1:8333".to_string()),
                    ("storage_class".to_string(), "STANDARD".to_string()),
                    ("force_path_style".to_string(), "false".to_string()),
                ]),
            }],
        );

        let registry = state.s3_tier_registry.read().unwrap();
        assert!(registry.get("s3.default").is_some());
        assert!(registry.get("s3").is_some());
        let global_registry = crate::remote_storage::s3_tier::global_s3_tier_registry()
            .read()
            .unwrap();
        assert!(global_registry.get("s3.default").is_some());
        assert!(global_registry.get("s3").is_some());
    }

    #[test]
    fn test_apply_storage_backends_ignores_unsupported_types() {
        let state = test_state_with_store(Store::new(NeedleMapKind::InMemory));
        // Do not call clear() on the global registry — other tests may be
        // running concurrently.

        apply_storage_backends(
            &state,
            &[master_pb::StorageBackend {
                r#type: "rclone".to_string(),
                id: "default".to_string(),
                properties: std::collections::HashMap::new(),
            }],
        );

        // The per-state registry is freshly created and should have no entries
        // since "rclone" is unsupported.
        let registry = state.s3_tier_registry.read().unwrap();
        assert!(registry.names().is_empty());
        // Only check that the unsupported type was not added to the global
        // registry.  Other tests may have their own entries present.
        let global_registry = crate::remote_storage::s3_tier::global_s3_tier_registry()
            .read()
            .unwrap();
        assert!(global_registry.get("rclone.default").is_none());
        assert!(global_registry.get("rclone").is_none());
    }

    #[test]
    fn test_apply_metrics_push_settings_updates_runtime_state() {
        let store = Store::new(NeedleMapKind::InMemory);
        let state = test_state_with_store(store);

        assert!(apply_metrics_push_settings(&state, "pushgateway:9091", 15,));
        {
            let runtime = state.metrics_runtime.read().unwrap();
            assert_eq!(runtime.push_gateway.address, "pushgateway:9091");
            assert_eq!(runtime.push_gateway.interval_seconds, 15);
        }

        assert!(!apply_metrics_push_settings(&state, "pushgateway:9091", 15,));
    }

    #[test]
    fn test_duplicate_uuid_retry_delay_matches_go_backoff() {
        assert_eq!(duplicate_uuid_retry_delay(0), Duration::from_secs(2));
        assert_eq!(duplicate_uuid_retry_delay(1), Duration::from_secs(4));
        assert_eq!(duplicate_uuid_retry_delay(2), Duration::from_secs(8));
    }

    #[test]
    fn test_duplicate_directories_maps_master_uuids_to_paths() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path().to_str().unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir,
                dir,
                1,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();

        let duplicate_dirs = duplicate_directories(
            &store,
            &[
                store.locations[0].directory_uuid.clone(),
                "missing-uuid".to_string(),
            ],
        );

        assert_eq!(duplicate_dirs, vec![dir.to_string()]);
    }

    #[test]
    fn test_apply_master_volume_options_updates_preallocate_and_size_limit() {
        let store = Store::new(NeedleMapKind::InMemory);
        store.volume_size_limit.store(1024, Ordering::Relaxed);

        let changed = apply_master_volume_options(
            &store,
            &master_pb::HeartbeatResponse {
                volume_size_limit: 2048,
                preallocate: true,
                ..Default::default()
            },
        );

        assert!(store.get_preallocate());
        assert_eq!(store.volume_size_limit.load(Ordering::Relaxed), 2048);
        assert!(!changed);
    }

    #[test]
    fn test_diff_ec_shard_delta_messages_reports_mounts_and_unmounts() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir = temp_dir.path().to_str().unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir,
                dir,
                8,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();

        let previous = collect_ec_shard_delta_messages(&store);

        std::fs::write(format!("{}/ec_delta_case_81.ec00", dir), b"delta").unwrap();
        store.locations[0]
            .mount_ec_shards(VolumeId(81), "ec_delta_case", &[0])
            .unwrap();
        let current = collect_ec_shard_delta_messages(&store);
        let (new_ec_shards, deleted_ec_shards) =
            diff_ec_shard_delta_messages(&previous, &current);

        assert_eq!(new_ec_shards.len(), 1);
        assert!(deleted_ec_shards.is_empty());
        assert_eq!(new_ec_shards[0].ec_index_bits, 1);
        assert_eq!(new_ec_shards[0].shard_sizes, vec![5]);

        let (new_after_delete, deleted_after_delete) =
            diff_ec_shard_delta_messages(&current, &HashMap::new());
        assert!(new_after_delete.is_empty());
        assert_eq!(deleted_after_delete.len(), 1);
        assert_eq!(deleted_after_delete[0].ec_index_bits, 1);
        assert_eq!(deleted_after_delete[0].shard_sizes, vec![0]);
    }
}
