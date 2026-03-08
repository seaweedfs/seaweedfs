//! Heartbeat client: registers the volume server with the master.
//!
//! Implements the bidirectional streaming `SendHeartbeat` RPC to the master,
//! matching Go's `server/volume_grpc_client_to_master.go`.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tonic::transport::Channel;
use tracing::{error, info, warn};

use super::volume_server::VolumeServerState;
use crate::pb::master_pb;
use crate::pb::master_pb::seaweed_client::SeaweedClient;
use crate::storage::store::Store;
use crate::storage::types::NeedleId;

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

    loop {
        for master_addr in &config.master_addresses {
            if shutdown_rx.try_recv().is_ok() {
                state.is_heartbeating.store(false, Ordering::Relaxed);
                info!("Heartbeat shutting down");
                return;
            }

            let grpc_addr = to_grpc_address(master_addr);
            info!("Connecting heartbeat to master {}", grpc_addr);

            match do_heartbeat(&config, &state, &grpc_addr, pulse, &mut shutdown_rx).await {
                Ok(Some(leader)) => {
                    info!("Master leader changed to {}", leader);
                }
                Ok(None) => {}
                Err(e) => {
                    state.is_heartbeating.store(false, Ordering::Relaxed);
                    warn!("Heartbeat to {} error: {}", grpc_addr, e);
                }
            }
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

/// Convert a master address "host:port" to a gRPC endpoint URL.
/// The Go master uses port + 10000 for gRPC by default.
fn to_grpc_address(master_addr: &str) -> String {
    if let Some((host, port_str)) = master_addr.rsplit_once(':') {
        if let Ok(port) = port_str.parse::<u16>() {
            let grpc_port = port + 10000;
            return format!("http://{}:{}", host, grpc_port);
        }
    }
    format!("http://{}", master_addr)
}

/// Perform one heartbeat session with a master server.
async fn do_heartbeat(
    config: &HeartbeatConfig,
    state: &Arc<VolumeServerState>,
    grpc_addr: &str,
    pulse: Duration,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let channel = Channel::from_shared(grpc_addr.to_string())?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .connect()
        .await?;

    let mut client = SeaweedClient::new(channel);

    let (tx, rx) = tokio::sync::mpsc::channel::<master_pb::Heartbeat>(32);

    // Send initial heartbeats BEFORE calling send_heartbeat to avoid deadlock:
    // the server won't send response headers until it receives the first message,
    // but send_heartbeat().await waits for response headers.
    tx.send(collect_heartbeat(config, state)).await?;
    tx.send(collect_ec_heartbeat(state)).await?;

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let mut response_stream = client.send_heartbeat(stream).await?.into_inner();

    info!("Heartbeat stream established with {}", grpc_addr);
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
                        if hb_resp.volume_size_limit > 0 {
                            let s = state.store.read().unwrap();
                            s.volume_size_limit.store(
                                hb_resp.volume_size_limit,
                                std::sync::atomic::Ordering::Relaxed,
                            );
                        }
                        let metrics_changed = apply_metrics_push_settings(
                            state,
                            &hb_resp.metrics_address,
                            hb_resp.metrics_interval_seconds,
                        );
                        if metrics_changed {
                            state.metrics_notify.notify_waiters();
                        }
                        if !hb_resp.leader.is_empty() {
                            return Ok(Some(hb_resp.leader));
                        }
                        if !hb_resp.duplicated_uuids.is_empty() {
                            error!("Master reported duplicate volume directory UUIDs: {:?}", hb_resp.duplicated_uuids);
                        }
                    }
                    Ok(None) => return Ok(None),
                    Err(e) => return Err(Box::new(e)),
                }
            }

            _ = volume_tick.tick() => {
                if tx.send(collect_heartbeat(config, state)).await.is_err() {
                    return Ok(None);
                }
            }

            _ = ec_tick.tick() => {
                if tx.send(collect_ec_heartbeat(state)).await.is_err() {
                    return Ok(None);
                }
            }

            _ = state.volume_state_notify.notified() => {
                if tx.send(collect_heartbeat(config, state)).await.is_err() {
                    return Ok(None);
                }
            }

            _ = shutdown_rx.recv() => {
                state.is_heartbeating.store(false, Ordering::Relaxed);
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
                info!("Sent deregistration heartbeat");
                return Ok(None);
            }
        }
    }
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

/// Collect volume information into a Heartbeat message.
fn collect_heartbeat(
    config: &HeartbeatConfig,
    state: &Arc<VolumeServerState>,
) -> master_pb::Heartbeat {
    let store = state.store.read().unwrap();
    build_heartbeat(config, &store)
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

fn build_heartbeat(config: &HeartbeatConfig, store: &Store) -> master_pb::Heartbeat {
    let mut volumes = Vec::new();
    let mut max_file_key = NeedleId(0);
    let mut max_volume_counts: HashMap<String, u32> = HashMap::new();

    for loc in &store.locations {
        let disk_type_str = loc.disk_type.to_string();
        let max_count = loc
            .max_volume_count
            .load(std::sync::atomic::Ordering::Relaxed);
        *max_volume_counts.entry(disk_type_str).or_insert(0) += max_count as u32;

        for (_, vol) in loc.iter_volumes() {
            let cur_max = vol.max_file_key();
            if cur_max > max_file_key {
                max_file_key = cur_max;
            }

            volumes.push(master_pb::VolumeInformationMessage {
                id: vol.id.0,
                size: vol.content_size(),
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
                ..Default::default()
            });
        }
    }
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
        has_no_volumes,
        max_volume_counts,
        grpc_port: config.grpc_port as u32,
        location_uuids,
        disk_tags,
        ..Default::default()
    }
}

/// Collect EC shard information into a Heartbeat message.
fn collect_ec_heartbeat(state: &Arc<VolumeServerState>) -> master_pb::Heartbeat {
    let store = state.store.read().unwrap();

    let mut ec_shards = Vec::new();
    for (vid, ec_vol) in &store.ec_volumes {
        let mut ec_index_bits: u32 = 0;
        for shard_opt in &ec_vol.shards {
            if let Some(shard) = shard_opt {
                ec_index_bits |= 1u32 << shard.shard_id;
            }
        }
        if ec_index_bits > 0 {
            ec_shards.push(master_pb::VolumeEcShardInformationMessage {
                id: vid.0,
                collection: ec_vol.collection.clone(),
                ec_index_bits,
                ..Default::default()
            });
        }
    }

    let has_no = ec_shards.is_empty();
    master_pb::Heartbeat {
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
    use crate::remote_storage::s3_tier::S3TierRegistry;
    use crate::security::{Guard, SigningKey};
    use crate::storage::needle_map::NeedleMapKind;
    use crate::storage::types::{DiskType, VolumeId};
    use std::sync::RwLock;

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
            guard: Guard::new(&[], SigningKey(vec![]), 0, SigningKey(vec![]), 0),
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
            is_heartbeating: std::sync::atomic::AtomicBool::new(false),
            has_master: true,
            pre_stop_seconds: 0,
            volume_state_notify: tokio::sync::Notify::new(),
            write_queue: std::sync::OnceLock::new(),
            s3_tier_registry: std::sync::RwLock::new(S3TierRegistry::new()),
            read_mode: ReadMode::Local,
            master_url: String::new(),
            self_url: String::new(),
            http_client: reqwest::Client::new(),
            metrics_runtime: std::sync::RwLock::new(Default::default()),
            metrics_notify: tokio::sync::Notify::new(),
            has_slow_read: true,
            read_buffer_size_bytes: 4 * 1024 * 1024,
        })
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
            .add_volume(VolumeId(7), "pics", None, None, 0, DiskType::HardDrive)
            .unwrap();

        let heartbeat = build_heartbeat(&test_config(), &store);

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

        let heartbeat = build_heartbeat(&test_config(), &store);

        assert!(heartbeat.volumes.is_empty());
        assert!(heartbeat.has_no_volumes);
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
}
