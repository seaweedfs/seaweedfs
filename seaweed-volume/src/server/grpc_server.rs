//! gRPC service implementation for the volume server.
//!
//! Implements the VolumeServer trait generated from volume_server.proto.
//! 48 RPCs: core volume operations are fully implemented, streaming and
//! EC operations are stubbed with appropriate error messages.

use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use crate::pb::volume_server_pb;
use crate::pb::volume_server_pb::volume_server_server::VolumeServer;
use crate::storage::needle::needle::{self, Needle};
use crate::storage::types::*;

use super::volume_server::VolumeServerState;

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

pub struct VolumeGrpcService {
    pub state: Arc<VolumeServerState>,
}

#[tonic::async_trait]
impl VolumeServer for VolumeGrpcService {
    // ---- Core volume operations ----

    async fn batch_delete(
        &self,
        request: Request<volume_server_pb::BatchDeleteRequest>,
    ) -> Result<Response<volume_server_pb::BatchDeleteResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let mut results = Vec::new();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        for fid_str in &req.file_ids {
            let file_id = match needle::FileId::parse(fid_str) {
                Ok(fid) => fid,
                Err(e) => {
                    results.push(volume_server_pb::DeleteResult {
                        file_id: fid_str.clone(),
                        status: 400, // Bad Request
                        error: e,
                        size: 0,
                        version: 0,
                    });
                    continue;
                }
            };

            let mut n = Needle {
                id: file_id.key,
                cookie: file_id.cookie,
                ..Needle::default()
            };

            // Check if this is an EC volume
            let is_ec_volume = {
                let store = self.state.store.read().unwrap();
                store.ec_volumes.contains_key(&file_id.volume_id)
            };

            // Cookie validation (unless skip_cookie_check)
            if !req.skip_cookie_check {
                let original_cookie = n.cookie;
                if !is_ec_volume {
                    let store = self.state.store.read().unwrap();
                    match store.read_volume_needle(file_id.volume_id, &mut n) {
                        Ok(_) => {}
                        Err(e) => {
                            results.push(volume_server_pb::DeleteResult {
                                file_id: fid_str.clone(),
                                status: 404,
                                error: e.to_string(),
                                size: 0,
                                version: 0,
                            });
                            continue;
                        }
                    }
                } else {
                    // For EC volumes, verify needle exists in ecx index
                    let store = self.state.store.read().unwrap();
                    if let Some(ec_vol) = store.ec_volumes.get(&file_id.volume_id) {
                        match ec_vol.find_needle_from_ecx(n.id) {
                            Ok(Some((_, size))) if !size.is_deleted() => {
                                // Needle exists and is not deleted — cookie check not possible
                                // for EC volumes without distributed read, so we accept it
                                n.data_size = size.0 as u32;
                            }
                            Ok(_) => {
                                results.push(volume_server_pb::DeleteResult {
                                    file_id: fid_str.clone(),
                                    status: 404,
                                    error: format!("ec needle {} not found", fid_str),
                                    size: 0,
                                    version: 0,
                                });
                                continue;
                            }
                            Err(e) => {
                                results.push(volume_server_pb::DeleteResult {
                                    file_id: fid_str.clone(),
                                    status: 404,
                                    error: e.to_string(),
                                    size: 0,
                                    version: 0,
                                });
                                continue;
                            }
                        }
                    } else {
                        results.push(volume_server_pb::DeleteResult {
                            file_id: fid_str.clone(),
                            status: 404,
                            error: format!("ec volume {} not found", file_id.volume_id),
                            size: 0,
                            version: 0,
                        });
                        continue;
                    }
                }
                if n.cookie != original_cookie {
                    results.push(volume_server_pb::DeleteResult {
                        file_id: fid_str.clone(),
                        status: 400,
                        error: "File Random Cookie does not match.".to_string(),
                        size: 0,
                        version: 0,
                    });
                    break;
                }
            }

            // Reject chunk manifest needles
            if n.is_chunk_manifest() {
                results.push(volume_server_pb::DeleteResult {
                    file_id: fid_str.clone(),
                    status: 406,
                    error: "ChunkManifest: not allowed in batch delete mode.".to_string(),
                    size: 0,
                    version: 0,
                });
                continue;
            }

            n.last_modified = now;
            n.set_has_last_modified_date();

            if !is_ec_volume {
                let mut store = self.state.store.write().unwrap();
                match store.delete_volume_needle(file_id.volume_id, &mut n) {
                    Ok(size) => {
                        if size.0 == 0 {
                            results.push(volume_server_pb::DeleteResult {
                                file_id: fid_str.clone(),
                                status: 304,
                                error: String::new(),
                                size: 0,
                                version: 0,
                            });
                        } else {
                            results.push(volume_server_pb::DeleteResult {
                                file_id: fid_str.clone(),
                                status: 202,
                                error: String::new(),
                                size: size.0 as u32,
                                version: 0,
                            });
                        }
                    }
                    Err(e) => {
                        results.push(volume_server_pb::DeleteResult {
                            file_id: fid_str.clone(),
                            status: 500,
                            error: e.to_string(),
                            size: 0,
                            version: 0,
                        });
                    }
                }
            } else {
                // EC volume deletion: journal the delete locally (with cookie validation, matching Go)
                let mut store = self.state.store.write().unwrap();
                if let Some(ec_vol) = store.ec_volumes.get_mut(&file_id.volume_id) {
                    match ec_vol.journal_delete_with_cookie(n.id, n.cookie) {
                        Ok(()) => {
                            results.push(volume_server_pb::DeleteResult {
                                file_id: fid_str.clone(),
                                status: 202,
                                error: String::new(),
                                size: n.data_size,
                                version: 0,
                            });
                        }
                        Err(e) => {
                            results.push(volume_server_pb::DeleteResult {
                                file_id: fid_str.clone(),
                                status: 500,
                                error: e.to_string(),
                                size: 0,
                                version: 0,
                            });
                        }
                    }
                } else {
                    results.push(volume_server_pb::DeleteResult {
                        file_id: fid_str.clone(),
                        status: 404,
                        error: format!("ec volume {} not found", file_id.volume_id),
                        size: 0,
                        version: 0,
                    });
                }
            }
        }

        Ok(Response::new(volume_server_pb::BatchDeleteResponse {
            results,
        }))
    }

    async fn vacuum_volume_check(
        &self,
        request: Request<volume_server_pb::VacuumVolumeCheckRequest>,
    ) -> Result<Response<volume_server_pb::VacuumVolumeCheckResponse>, Status> {
        let vid = VolumeId(request.into_inner().volume_id);
        let store = self.state.store.read().unwrap();
        let garbage_ratio = match store.find_volume(vid) {
            Some((_, vol)) => vol.garbage_level(),
            None => return Err(Status::not_found(format!("not found volume id {}", vid))),
        };
        Ok(Response::new(volume_server_pb::VacuumVolumeCheckResponse {
            garbage_ratio,
        }))
    }

    type VacuumVolumeCompactStream = BoxStream<volume_server_pb::VacuumVolumeCompactResponse>;
    async fn vacuum_volume_compact(
        &self,
        request: Request<volume_server_pb::VacuumVolumeCompactRequest>,
    ) -> Result<Response<Self::VacuumVolumeCompactStream>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let preallocate = req.preallocate as u64;
        let state = self.state.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(16);

        tokio::task::spawn_blocking(move || {
            let report_interval: i64 = 128 * 1024 * 1024;
            let next_report = std::sync::atomic::AtomicI64::new(report_interval);

            let tx_clone = tx.clone();
            let result = {
                let mut store = state.store.write().unwrap();
                store.compact_volume(vid, preallocate, 0, |processed| {
                    let target = next_report.load(std::sync::atomic::Ordering::Relaxed);
                    if processed > target {
                        let resp = volume_server_pb::VacuumVolumeCompactResponse {
                            processed_bytes: processed,
                            load_avg_1m: 0.0,
                        };
                        let _ = tx_clone.blocking_send(Ok(resp));
                        next_report.store(
                            processed + report_interval,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                    true
                })
            };

            if let Err(e) = result {
                let _ = tx.blocking_send(Err(Status::internal(e)));
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(stream) as Self::VacuumVolumeCompactStream
        ))
    }

    async fn vacuum_volume_commit(
        &self,
        request: Request<volume_server_pb::VacuumVolumeCommitRequest>,
    ) -> Result<Response<volume_server_pb::VacuumVolumeCommitResponse>, Status> {
        self.state.check_maintenance()?;
        let vid = VolumeId(request.into_inner().volume_id);
        let mut store = self.state.store.write().unwrap();
        match store.commit_compact_volume(vid) {
            Ok((is_read_only, volume_size)) => Ok(Response::new(
                volume_server_pb::VacuumVolumeCommitResponse {
                    is_read_only,
                    volume_size,
                },
            )),
            Err(e) => Err(Status::internal(e)),
        }
    }

    async fn vacuum_volume_cleanup(
        &self,
        request: Request<volume_server_pb::VacuumVolumeCleanupRequest>,
    ) -> Result<Response<volume_server_pb::VacuumVolumeCleanupResponse>, Status> {
        self.state.check_maintenance()?;
        let vid = VolumeId(request.into_inner().volume_id);
        let mut store = self.state.store.write().unwrap();
        match store.cleanup_compact_volume(vid) {
            Ok(()) => Ok(Response::new(
                volume_server_pb::VacuumVolumeCleanupResponse {},
            )),
            Err(e) => Err(Status::internal(e)),
        }
    }

    async fn delete_collection(
        &self,
        request: Request<volume_server_pb::DeleteCollectionRequest>,
    ) -> Result<Response<volume_server_pb::DeleteCollectionResponse>, Status> {
        let collection = &request.into_inner().collection;
        let mut store = self.state.store.write().unwrap();
        store.delete_collection(collection);
        Ok(Response::new(volume_server_pb::DeleteCollectionResponse {}))
    }

    async fn allocate_volume(
        &self,
        request: Request<volume_server_pb::AllocateVolumeRequest>,
    ) -> Result<Response<volume_server_pb::AllocateVolumeResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let rp = crate::storage::super_block::ReplicaPlacement::from_string(&req.replication)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let ttl = if req.ttl.is_empty() {
            None
        } else {
            Some(
                crate::storage::needle::ttl::TTL::read(&req.ttl)
                    .map_err(|e| Status::invalid_argument(e))?,
            )
        };
        let disk_type = DiskType::from_string(&req.disk_type);

        let version = if req.version > 0 {
            crate::storage::types::Version(req.version as u8)
        } else {
            crate::storage::types::Version::current()
        };

        let mut store = self.state.store.write().unwrap();
        store
            .add_volume(
                vid,
                &req.collection,
                Some(rp),
                ttl,
                req.preallocate as u64,
                disk_type,
                version,
            )
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(volume_server_pb::AllocateVolumeResponse {}))
    }

    async fn volume_sync_status(
        &self,
        request: Request<volume_server_pb::VolumeSyncStatusRequest>,
    ) -> Result<Response<volume_server_pb::VolumeSyncStatusResponse>, Status> {
        let vid = VolumeId(request.into_inner().volume_id);
        let store = self.state.store.read().unwrap();
        let (_, vol) = store
            .find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;

        Ok(Response::new(volume_server_pb::VolumeSyncStatusResponse {
            volume_id: vid.0,
            collection: vol.collection.clone(),
            replication: vol.super_block.replica_placement.to_string(),
            ttl: vol.super_block.ttl.to_string(),
            tail_offset: vol.content_size(),
            compact_revision: vol.super_block.compaction_revision as u32,
            idx_file_size: vol.idx_file_size(),
            version: vol.version().0 as u32,
        }))
    }

    type VolumeIncrementalCopyStream = BoxStream<volume_server_pb::VolumeIncrementalCopyResponse>;
    async fn volume_incremental_copy(
        &self,
        request: Request<volume_server_pb::VolumeIncrementalCopyRequest>,
    ) -> Result<Response<Self::VolumeIncrementalCopyStream>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        // Sync to disk first
        {
            let mut store = self.state.store.write().unwrap();
            if let Some((_, v)) = store.find_volume_mut(vid) {
                let _ = v.sync_to_disk();
            }
        }

        let store = self.state.store.read().unwrap();
        let (_, v) = store
            .find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;

        let dat_size = v.dat_file_size().unwrap_or(0);
        let dat_path = v.file_name(".dat");
        let super_block_size = v.super_block.block_size() as u64;

        // If since_ns is very large (after all data), return empty
        if req.since_ns == u64::MAX || dat_size <= super_block_size {
            drop(store);
            let stream = tokio_stream::iter(Vec::new());
            return Ok(Response::new(Box::pin(stream)));
        }

        // Use binary search to find the starting offset
        let start_offset = if req.since_ns == 0 {
            super_block_size
        } else {
            match v.binary_search_by_append_at_ns(req.since_ns) {
                Ok((_offset, true)) => {
                    // All entries are before since_ns — nothing to send
                    drop(store);
                    let stream = tokio_stream::iter(Vec::new());
                    return Ok(Response::new(Box::pin(stream)));
                }
                Ok((offset, false)) => {
                    let actual = offset.to_actual_offset();
                    if actual <= 0 {
                        super_block_size
                    } else {
                        actual as u64
                    }
                }
                Err(_e) => {
                    // On error, fall back to streaming from superblock end
                    super_block_size
                }
            }
        };
        drop(store);

        // Read the .dat file
        let file = std::fs::File::open(&dat_path).map_err(|e| Status::internal(e.to_string()))?;

        let mut reader = std::io::BufReader::new(file);
        use std::io::{Read, Seek, SeekFrom};
        reader
            .seek(SeekFrom::Start(start_offset))
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut results = Vec::new();
        let mut bytes_to_read = (dat_size - start_offset) as i64;
        let buffer_size = 2 * 1024 * 1024;

        while bytes_to_read > 0 {
            let chunk = std::cmp::min(bytes_to_read as usize, buffer_size);
            let mut buf = vec![0u8; chunk];
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    buf.truncate(n);
                    results.push(Ok(volume_server_pb::VolumeIncrementalCopyResponse {
                        file_content: buf,
                    }));
                    bytes_to_read -= n as i64;
                }
                Err(e) => return Err(Status::internal(e.to_string())),
            }
        }

        let stream = tokio_stream::iter(results);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn volume_mount(
        &self,
        request: Request<volume_server_pb::VolumeMountRequest>,
    ) -> Result<Response<volume_server_pb::VolumeMountResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        let mut store = self.state.store.write().unwrap();
        store
            .mount_volume(vid, "", DiskType::HardDrive)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(volume_server_pb::VolumeMountResponse {}))
    }

    async fn volume_unmount(
        &self,
        request: Request<volume_server_pb::VolumeUnmountRequest>,
    ) -> Result<Response<volume_server_pb::VolumeUnmountResponse>, Status> {
        let vid = VolumeId(request.into_inner().volume_id);
        let mut store = self.state.store.write().unwrap();
        // Unmount is idempotent — success even if volume not found
        store.unmount_volume(vid);
        Ok(Response::new(volume_server_pb::VolumeUnmountResponse {}))
    }

    async fn volume_delete(
        &self,
        request: Request<volume_server_pb::VolumeDeleteRequest>,
    ) -> Result<Response<volume_server_pb::VolumeDeleteResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let mut store = self.state.store.write().unwrap();
        if req.only_empty {
            let (_, vol) = store
                .find_volume(vid)
                .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;
            if vol.file_count() > 0 {
                return Err(Status::failed_precondition("volume not empty"));
            }
        }
        store
            .delete_volume(vid)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(volume_server_pb::VolumeDeleteResponse {}))
    }

    async fn volume_mark_readonly(
        &self,
        request: Request<volume_server_pb::VolumeMarkReadonlyRequest>,
    ) -> Result<Response<volume_server_pb::VolumeMarkReadonlyResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let mut store = self.state.store.write().unwrap();
        let (_, vol) = store
            .find_volume_mut(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;
        vol.set_read_only_persist(req.persist);
        drop(store);
        self.state.volume_state_notify.notify_one();
        Ok(Response::new(
            volume_server_pb::VolumeMarkReadonlyResponse {},
        ))
    }

    async fn volume_mark_writable(
        &self,
        request: Request<volume_server_pb::VolumeMarkWritableRequest>,
    ) -> Result<Response<volume_server_pb::VolumeMarkWritableResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let mut store = self.state.store.write().unwrap();
        let (_, vol) = store
            .find_volume_mut(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;
        vol.set_writable();
        drop(store);
        self.state.volume_state_notify.notify_one();
        Ok(Response::new(
            volume_server_pb::VolumeMarkWritableResponse {},
        ))
    }

    async fn volume_configure(
        &self,
        request: Request<volume_server_pb::VolumeConfigureRequest>,
    ) -> Result<Response<volume_server_pb::VolumeConfigureResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        // Validate replication string — return response error, not gRPC error
        let rp = match crate::storage::super_block::ReplicaPlacement::from_string(&req.replication)
        {
            Ok(rp) => rp,
            Err(e) => {
                return Ok(Response::new(volume_server_pb::VolumeConfigureResponse {
                    error: format!("volume configure replication {}: {}", req.replication, e),
                }));
            }
        };

        let mut store = self.state.store.write().unwrap();

        // Unmount the volume (Go returns nil for non-existent volumes, so we don't
        // treat a missing volume as an error here — configure_volume will catch it)
        store.unmount_volume(vid);

        // Modify the super block on disk (replica_placement byte)
        if let Err(e) = store.configure_volume(vid, rp) {
            let mut error = format!("volume configure {}: {}", vid, e);
            // Error recovery: try to re-mount anyway
            if let Err(mount_err) = store.mount_volume(vid, "", DiskType::HardDrive) {
                error += &format!(". Also failed to restore mount: {}", mount_err);
            }
            return Ok(Response::new(volume_server_pb::VolumeConfigureResponse {
                error,
            }));
        }

        // Re-mount the volume
        if let Err(e) = store.mount_volume(vid, "", DiskType::HardDrive) {
            return Ok(Response::new(volume_server_pb::VolumeConfigureResponse {
                error: format!("volume configure mount {}: {}", vid, e),
            }));
        }

        Ok(Response::new(volume_server_pb::VolumeConfigureResponse {
            error: String::new(),
        }))
    }

    async fn volume_status(
        &self,
        request: Request<volume_server_pb::VolumeStatusRequest>,
    ) -> Result<Response<volume_server_pb::VolumeStatusResponse>, Status> {
        let vid = VolumeId(request.into_inner().volume_id);
        let store = self.state.store.read().unwrap();
        let (_, vol) = store
            .find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;

        Ok(Response::new(volume_server_pb::VolumeStatusResponse {
            is_read_only: vol.is_read_only(),
            volume_size: vol.content_size(),
            file_count: vol.file_count() as u64,
            file_deleted_count: vol.deleted_count() as u64,
        }))
    }

    async fn get_state(
        &self,
        _request: Request<volume_server_pb::GetStateRequest>,
    ) -> Result<Response<volume_server_pb::GetStateResponse>, Status> {
        Ok(Response::new(volume_server_pb::GetStateResponse {
            state: Some(volume_server_pb::VolumeServerState {
                maintenance: self.state.maintenance.load(Ordering::Relaxed),
                version: self.state.state_version.load(Ordering::Relaxed),
            }),
        }))
    }

    async fn set_state(
        &self,
        request: Request<volume_server_pb::SetStateRequest>,
    ) -> Result<Response<volume_server_pb::SetStateResponse>, Status> {
        let req = request.into_inner();

        if let Some(new_state) = &req.state {
            // Check version matches (optimistic concurrency)
            let current_version = self.state.state_version.load(Ordering::Relaxed);
            if new_state.version != current_version {
                return Err(Status::failed_precondition(format!(
                    "version mismatch: expected {}, got {}",
                    current_version, new_state.version
                )));
            }

            // Apply state changes
            self.state
                .maintenance
                .store(new_state.maintenance, Ordering::Relaxed);
            let new_version = self.state.state_version.fetch_add(1, Ordering::Relaxed) + 1;

            Ok(Response::new(volume_server_pb::SetStateResponse {
                state: Some(volume_server_pb::VolumeServerState {
                    maintenance: new_state.maintenance,
                    version: new_version,
                }),
            }))
        } else {
            // nil state = no-op, return current state
            Ok(Response::new(volume_server_pb::SetStateResponse {
                state: Some(volume_server_pb::VolumeServerState {
                    maintenance: self.state.maintenance.load(Ordering::Relaxed),
                    version: self.state.state_version.load(Ordering::Relaxed),
                }),
            }))
        }
    }

    type VolumeCopyStream = BoxStream<volume_server_pb::VolumeCopyResponse>;
    async fn volume_copy(
        &self,
        request: Request<volume_server_pb::VolumeCopyRequest>,
    ) -> Result<Response<Self::VolumeCopyStream>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        // If volume already exists locally, delete it first
        {
            let store = self.state.store.read().unwrap();
            if store.find_volume(vid).is_some() {
                drop(store);
                let mut store = self.state.store.write().unwrap();
                store.delete_volume(vid).map_err(|e| {
                    Status::internal(format!("failed to delete existing volume {}: {}", vid, e))
                })?;
            }
        }

        // Parse source_data_node address: "ip:port.grpcPort" or "ip:port" (grpc = port + 10000)
        let source = &req.source_data_node;
        let grpc_addr = parse_grpc_address(source).map_err(|e| {
            Status::internal(format!(
                "VolumeCopy volume {} invalid source_data_node {}: {}",
                vid, source, e
            ))
        })?;

        let channel = tonic::transport::Channel::from_shared(format!("http://{}", grpc_addr))
            .map_err(|e| {
                Status::internal(format!("VolumeCopy volume {} parse source: {}", vid, e))
            })?
            .connect()
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "VolumeCopy volume {} connect to {}: {}",
                    vid, grpc_addr, e
                ))
            })?;

        let mut client = volume_server_pb::volume_server_client::VolumeServerClient::new(channel);

        // Get file status from source
        let vol_info = client
            .read_volume_file_status(volume_server_pb::ReadVolumeFileStatusRequest {
                volume_id: req.volume_id,
            })
            .await
            .map_err(|e| Status::internal(format!("read volume file status failed, {}", e)))?
            .into_inner();

        let disk_type = if !req.disk_type.is_empty() {
            &req.disk_type
        } else {
            &vol_info.disk_type
        };

        // Find a free disk location
        let (data_base, idx_base) = {
            let store = self.state.store.read().unwrap();
            let mut found = None;
            for loc in &store.locations {
                if format!("{:?}", loc.disk_type) == *disk_type
                    || disk_type.is_empty()
                    || disk_type == "0"
                {
                    if loc.available_space.load(Ordering::Relaxed) > vol_info.dat_file_size {
                        found = Some((loc.directory.clone(), loc.idx_directory.clone()));
                        break;
                    }
                }
            }
            // Fallback: use first location if no disk type match
            if found.is_none() && !store.locations.is_empty() {
                let loc = &store.locations[0];
                found = Some((loc.directory.clone(), loc.idx_directory.clone()));
            }
            found.ok_or_else(|| Status::internal(format!("no space left {}", disk_type)))?
        };

        let data_base_name =
            crate::storage::volume::volume_file_name(&data_base, &vol_info.collection, vid);
        let idx_base_name =
            crate::storage::volume::volume_file_name(&idx_base, &vol_info.collection, vid);

        // Write a .note file to indicate copy in progress
        let note_path = format!("{}.note", data_base_name);
        let _ = std::fs::write(&note_path, format!("copying from {}", source));

        let has_remote_dat = vol_info
            .volume_info
            .as_ref()
            .map(|vi| !vi.files.is_empty())
            .unwrap_or(false);

        let (tx, rx) =
            tokio::sync::mpsc::channel::<Result<volume_server_pb::VolumeCopyResponse, Status>>(16);
        let state = self.state.clone();

        tokio::spawn(async move {
            let result = async {
                let report_interval: i64 = 128 * 1024 * 1024;
                let mut next_report_target: i64 = report_interval;

                // Copy .dat file
                if !has_remote_dat {
                    let dat_path = format!("{}.dat", data_base_name);
                    let dat_modified_ts_ns = copy_file_from_source(
                        &mut client,
                        false,
                        &req.collection,
                        req.volume_id,
                        vol_info.compaction_revision,
                        vol_info.dat_file_size,
                        &dat_path,
                        ".dat",
                        false,
                        true,
                        Some(&tx),
                        &mut next_report_target,
                        report_interval,
                    )
                    .await
                    .map_err(|e| Status::internal(e))?;
                    if dat_modified_ts_ns > 0 {
                        set_file_mtime(&dat_path, dat_modified_ts_ns);
                    }
                }

                // Copy .idx file
                let idx_path = format!("{}.idx", idx_base_name);
                let idx_modified_ts_ns = copy_file_from_source(
                    &mut client,
                    false,
                    &req.collection,
                    req.volume_id,
                    vol_info.compaction_revision,
                    vol_info.idx_file_size,
                    &idx_path,
                    ".idx",
                    false,
                    false,
                    None,
                    &mut next_report_target,
                    report_interval,
                )
                .await
                .map_err(|e| Status::internal(e))?;
                if idx_modified_ts_ns > 0 {
                    set_file_mtime(&idx_path, idx_modified_ts_ns);
                }

                // Copy .vif file (ignore if not found on source)
                let vif_path = format!("{}.vif", data_base_name);
                let vif_modified_ts_ns = copy_file_from_source(
                    &mut client,
                    false,
                    &req.collection,
                    req.volume_id,
                    vol_info.compaction_revision,
                    1024 * 1024,
                    &vif_path,
                    ".vif",
                    false,
                    true,
                    None,
                    &mut next_report_target,
                    report_interval,
                )
                .await
                .map_err(|e| Status::internal(e))?;
                if vif_modified_ts_ns > 0 {
                    set_file_mtime(&vif_path, vif_modified_ts_ns);
                }

                // Remove the .note file
                let _ = std::fs::remove_file(&note_path);

                // Verify file sizes
                if !has_remote_dat {
                    let dat_path = format!("{}.dat", data_base_name);
                    check_copy_file_size(&dat_path, vol_info.dat_file_size)?;
                }
                if vol_info.idx_file_size > 0 {
                    check_copy_file_size(&idx_path, vol_info.idx_file_size)?;
                }

                // Find last_append_at_ns from copied files
                let last_append_at_ns = if !has_remote_dat {
                    find_last_append_at_ns(&idx_path, &format!("{}.dat", data_base_name))
                        .unwrap_or(vol_info.dat_file_timestamp_seconds * 1_000_000_000)
                } else {
                    vol_info.dat_file_timestamp_seconds * 1_000_000_000
                };

                // Mount the volume
                {
                    let disk_type_enum = DiskType::default();
                    let mut store = state.store.write().unwrap();
                    store
                        .mount_volume(vid, &vol_info.collection, disk_type_enum)
                        .map_err(|e| {
                            Status::internal(format!("failed to mount volume {}: {}", vid, e))
                        })?;
                }

                // Send final response with last_append_at_ns
                let _ = tx
                    .send(Ok(volume_server_pb::VolumeCopyResponse {
                        last_append_at_ns: last_append_at_ns,
                        processed_bytes: 0,
                    }))
                    .await;

                Ok::<(), Status>(())
            }
            .await;

            if let Err(e) = result {
                // Clean up on error
                let _ = std::fs::remove_file(format!("{}.dat", data_base_name));
                let _ = std::fs::remove_file(format!("{}.idx", idx_base_name));
                let _ = std::fs::remove_file(format!("{}.vif", data_base_name));
                let _ = std::fs::remove_file(&note_path);
                let _ = tx.send(Err(e)).await;
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn read_volume_file_status(
        &self,
        request: Request<volume_server_pb::ReadVolumeFileStatusRequest>,
    ) -> Result<Response<volume_server_pb::ReadVolumeFileStatusResponse>, Status> {
        let vid = VolumeId(request.into_inner().volume_id);
        let store = self.state.store.read().unwrap();
        let (_, vol) = store
            .find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;

        let mod_time = vol.dat_file_mod_time();
        Ok(Response::new(
            volume_server_pb::ReadVolumeFileStatusResponse {
                volume_id: vid.0,
                idx_file_timestamp_seconds: mod_time,
                idx_file_size: vol.idx_file_size(),
                dat_file_timestamp_seconds: mod_time,
                dat_file_size: vol.dat_file_size().unwrap_or(0),
                file_count: vol.file_count() as u64,
                compaction_revision: vol.super_block.compaction_revision as u32,
                collection: vol.collection.clone(),
                disk_type: String::new(),
                volume_info: None,
                version: vol.version().0 as u32,
            },
        ))
    }

    type CopyFileStream = BoxStream<volume_server_pb::CopyFileResponse>;
    async fn copy_file(
        &self,
        request: Request<volume_server_pb::CopyFileRequest>,
    ) -> Result<Response<Self::CopyFileStream>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        let file_name: String;

        if !req.is_ec_volume {
            // Sync volume to disk before copying (matching Go's v.SyncToDisk())
            {
                let mut store = self.state.store.write().unwrap();
                if let Some((_, v)) = store.find_volume_mut(vid) {
                    let _ = v.sync_to_disk();
                }
            }

            let store = self.state.store.read().unwrap();
            let (_, v) = store
                .find_volume(vid)
                .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;

            // Check compaction revision
            if req.compaction_revision != u32::MAX
                && v.last_compact_revision() != req.compaction_revision as u16
            {
                return Err(Status::failed_precondition(format!(
                    "volume {} is compacted",
                    vid.0
                )));
            }

            file_name = v.file_name(&req.ext);
            drop(store);
        } else {
            // EC volume: search disk locations for the file
            let store = self.state.store.read().unwrap();
            let mut found_path = None;
            let ec_base = if req.collection.is_empty() {
                format!("{}{}", vid.0, req.ext)
            } else {
                format!("{}_{}{}", req.collection, vid.0, req.ext)
            };
            for loc in &store.locations {
                let path = format!("{}/{}", loc.directory, ec_base);
                if std::path::Path::new(&path).exists() {
                    found_path = Some(path);
                }
                let idx_path = format!("{}/{}", loc.idx_directory, ec_base);
                if std::path::Path::new(&idx_path).exists() {
                    found_path = Some(idx_path);
                }
            }
            drop(store);

            match found_path {
                Some(p) => file_name = p,
                None => {
                    if req.ignore_source_file_not_found {
                        let stream = tokio_stream::iter(Vec::new());
                        return Ok(Response::new(Box::pin(stream)));
                    }
                    return Err(Status::not_found(format!(
                        "CopyFile not found ec volume id {}",
                        vid.0
                    )));
                }
            }
        }

        // StopOffset 0 means nothing to read
        if req.stop_offset == 0 {
            let stream = tokio_stream::iter(Vec::new());
            return Ok(Response::new(Box::pin(stream)));
        }

        // Open file and read content
        let file = match std::fs::File::open(&file_name) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if req.ignore_source_file_not_found || req.stop_offset == 0 {
                    let stream = tokio_stream::iter(Vec::new());
                    return Ok(Response::new(Box::pin(stream)));
                }
                return Err(Status::not_found(format!("{}", e)));
            }
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        let metadata = file
            .metadata()
            .map_err(|e| Status::internal(e.to_string()))?;
        let mod_ts_ns = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);

        let mut results: Vec<Result<volume_server_pb::CopyFileResponse, Status>> = Vec::new();
        let mut bytes_to_read = req.stop_offset as i64;
        let mut reader = std::io::BufReader::new(file);
        let buffer_size = 2 * 1024 * 1024; // 2MB chunks
        let mut first = true;

        use std::io::Read;
        while bytes_to_read > 0 {
            let chunk_size = std::cmp::min(bytes_to_read as usize, buffer_size);
            let mut buf = vec![0u8; chunk_size];
            match reader.read(&mut buf) {
                Ok(0) => break, // EOF
                Ok(n) => {
                    buf.truncate(n);
                    if n as i64 > bytes_to_read {
                        buf.truncate(bytes_to_read as usize);
                    }
                    results.push(Ok(volume_server_pb::CopyFileResponse {
                        file_content: buf,
                        modified_ts_ns: if first { mod_ts_ns } else { 0 },
                    }));
                    first = false;
                    bytes_to_read -= n as i64;
                }
                Err(e) => return Err(Status::internal(e.to_string())),
            }
        }

        // If no data was sent, still send ModifiedTsNs
        if first && mod_ts_ns != 0 {
            results.push(Ok(volume_server_pb::CopyFileResponse {
                file_content: vec![],
                modified_ts_ns: mod_ts_ns,
            }));
        }

        let stream = tokio_stream::iter(results);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn receive_file(
        &self,
        request: Request<Streaming<volume_server_pb::ReceiveFileRequest>>,
    ) -> Result<Response<volume_server_pb::ReceiveFileResponse>, Status> {
        self.state.check_maintenance()?;

        let mut stream = request.into_inner();
        let mut target_file: Option<std::fs::File> = None;
        let mut file_path;
        let mut bytes_written: u64 = 0;

        while let Some(req) = stream.message().await? {
            match req.data {
                Some(volume_server_pb::receive_file_request::Data::Info(info)) => {
                    // Determine file path
                    if info.is_ec_volume {
                        let store = self.state.store.read().unwrap();
                        let dir = store
                            .locations
                            .first()
                            .map(|loc| loc.directory.clone())
                            .unwrap_or_default();
                        drop(store);
                        let ec_base = if info.collection.is_empty() {
                            format!("{}", info.volume_id)
                        } else {
                            format!("{}_{}", info.collection, info.volume_id)
                        };
                        file_path = format!("{}/{}{}", dir, ec_base, info.ext);
                    } else {
                        let store = self.state.store.read().unwrap();
                        let (_, v) =
                            store.find_volume(VolumeId(info.volume_id)).ok_or_else(|| {
                                Status::not_found(format!("volume {} not found", info.volume_id))
                            })?;
                        file_path = v.file_name(&info.ext);
                        drop(store);
                    }

                    target_file =
                        Some(std::fs::File::create(&file_path).map_err(|e| {
                            Status::internal(format!("failed to create file: {}", e))
                        })?);
                }
                Some(volume_server_pb::receive_file_request::Data::FileContent(content)) => {
                    if let Some(ref mut f) = target_file {
                        use std::io::Write;
                        let n = f.write(&content).map_err(|e| {
                            Status::internal(format!("failed to write file: {}", e))
                        })?;
                        bytes_written += n as u64;
                    } else {
                        return Ok(Response::new(volume_server_pb::ReceiveFileResponse {
                            error: "file info must be sent first".to_string(),
                            bytes_written: 0,
                        }));
                    }
                }
                None => {
                    return Ok(Response::new(volume_server_pb::ReceiveFileResponse {
                        error: "unknown message type".to_string(),
                        bytes_written: 0,
                    }));
                }
            }
        }

        if let Some(ref f) = target_file {
            let _ = f.sync_all();
        }

        Ok(Response::new(volume_server_pb::ReceiveFileResponse {
            error: String::new(),
            bytes_written,
        }))
    }

    async fn read_needle_blob(
        &self,
        request: Request<volume_server_pb::ReadNeedleBlobRequest>,
    ) -> Result<Response<volume_server_pb::ReadNeedleBlobResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let offset = req.offset;
        let size = Size(req.size);

        let store = self.state.store.read().unwrap();
        let (_, vol) = store
            .find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;

        let blob = vol
            .read_needle_blob(offset, size)
            .map_err(|e| Status::internal(format!("read needle blob: {}", e)))?;

        Ok(Response::new(volume_server_pb::ReadNeedleBlobResponse {
            needle_blob: blob,
        }))
    }

    async fn read_needle_meta(
        &self,
        request: Request<volume_server_pb::ReadNeedleMetaRequest>,
    ) -> Result<Response<volume_server_pb::ReadNeedleMetaResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let needle_id = NeedleId(req.needle_id);

        let store = self.state.store.read().unwrap();
        let (_, vol) = store.find_volume(vid).ok_or_else(|| {
            Status::not_found(format!(
                "not found volume id {} and read needle metadata at ec shards is not supported",
                vid
            ))
        })?;

        let offset = req.offset;
        let size = crate::storage::types::Size(req.size);

        let mut n = Needle {
            id: needle_id,
            flags: 0x08,
            ..Needle::default()
        };
        vol.read_needle_data_at(&mut n, offset, size)
            .map_err(|e| Status::internal(format!("read needle meta: {}", e)))?;

        let ttl_str = n.ttl.as_ref().map_or(String::new(), |t| t.to_string());
        Ok(Response::new(volume_server_pb::ReadNeedleMetaResponse {
            cookie: n.cookie.0,
            last_modified: n.last_modified,
            crc: n.checksum.0,
            ttl: ttl_str,
            append_at_ns: n.append_at_ns,
        }))
    }

    async fn write_needle_blob(
        &self,
        request: Request<volume_server_pb::WriteNeedleBlobRequest>,
    ) -> Result<Response<volume_server_pb::WriteNeedleBlobResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let needle_id = NeedleId(req.needle_id);
        let size = Size(req.size);

        let mut store = self.state.store.write().unwrap();
        let (_, vol) = store
            .find_volume_mut(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;

        vol.write_needle_blob_and_index(needle_id, &req.needle_blob, size)
            .map_err(|e| {
                Status::internal(format!(
                    "write blob needle {} size {}: {}",
                    needle_id.0, size.0, e
                ))
            })?;

        Ok(Response::new(volume_server_pb::WriteNeedleBlobResponse {}))
    }

    type ReadAllNeedlesStream = BoxStream<volume_server_pb::ReadAllNeedlesResponse>;
    async fn read_all_needles(
        &self,
        request: Request<volume_server_pb::ReadAllNeedlesRequest>,
    ) -> Result<Response<Self::ReadAllNeedlesStream>, Status> {
        let req = request.into_inner();
        let mut results: Vec<Result<volume_server_pb::ReadAllNeedlesResponse, Status>> = Vec::new();

        let store = self.state.store.read().unwrap();
        for &raw_vid in &req.volume_ids {
            let vid = VolumeId(raw_vid);
            let v = match store.find_volume(vid) {
                Some((_, v)) => v,
                None => {
                    // Push error as last item so previous volumes' data is streamed first
                    results.push(Err(Status::not_found(format!(
                        "not found volume id {}",
                        vid
                    ))));
                    break;
                }
            };

            let needles = match v.read_all_needles() {
                Ok(n) => n,
                Err(e) => {
                    results.push(Err(Status::internal(e.to_string())));
                    break;
                }
            };

            for n in needles {
                let compressed = n.is_compressed();
                results.push(Ok(volume_server_pb::ReadAllNeedlesResponse {
                    volume_id: raw_vid,
                    needle_id: n.id.into(),
                    cookie: n.cookie.0,
                    needle_blob: n.data,
                    needle_blob_compressed: compressed,
                    last_modified: n.last_modified,
                    crc: n.checksum.0,
                    name: n.name,
                    mime: n.mime,
                }));
            }
        }
        drop(store);

        let stream = tokio_stream::iter(results);
        Ok(Response::new(Box::pin(stream)))
    }

    type VolumeTailSenderStream = BoxStream<volume_server_pb::VolumeTailSenderResponse>;
    async fn volume_tail_sender(
        &self,
        request: Request<volume_server_pb::VolumeTailSenderRequest>,
    ) -> Result<Response<Self::VolumeTailSenderStream>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        let (version, sb_size) = {
            let store = self.state.store.read().unwrap();
            let (_, vol) = store
                .find_volume(vid)
                .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;
            (vol.version().0 as u32, vol.super_block.block_size() as u64)
        };

        let state = self.state.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        const BUFFER_SIZE_LIMIT: usize = 2 * 1024 * 1024;

        tokio::spawn(async move {
            let since_ns = req.since_ns;
            let idle_timeout = req.idle_timeout_seconds;
            let mut last_timestamp_ns = since_ns;
            let mut draining_seconds = idle_timeout;

            loop {
                // Use binary search to find starting offset, then scan from there
                let scan_result = {
                    let store = state.store.read().unwrap();
                    if let Some((_, vol)) = store.find_volume(vid) {
                        let start_offset = if last_timestamp_ns > 0 {
                            match vol.binary_search_by_append_at_ns(last_timestamp_ns) {
                                Ok((offset, _is_last)) => {
                                    if offset.is_zero() {
                                        sb_size
                                    } else {
                                        offset.to_actual_offset() as u64
                                    }
                                }
                                Err(_) => sb_size,
                            }
                        } else {
                            sb_size
                        };
                        vol.scan_raw_needles_from(start_offset)
                    } else {
                        break;
                    }
                };

                let entries = match scan_result {
                    Ok(e) => e,
                    Err(_) => break,
                };

                // Filter entries since last_timestamp_ns
                let mut last_processed_ns = last_timestamp_ns;
                let mut sent_any = false;
                for (header, body, append_at_ns) in &entries {
                    if *append_at_ns <= last_timestamp_ns && last_timestamp_ns > 0 {
                        continue;
                    }
                    sent_any = true;
                    // Send body in chunks of BUFFER_SIZE_LIMIT
                    let mut i = 0;
                    while i < body.len() {
                        let end = std::cmp::min(i + BUFFER_SIZE_LIMIT, body.len());
                        let is_last_chunk = end >= body.len();
                        let msg = volume_server_pb::VolumeTailSenderResponse {
                            needle_header: header.clone(),
                            needle_body: body[i..end].to_vec(),
                            is_last_chunk,
                            version,
                        };
                        if tx.send(Ok(msg)).await.is_err() {
                            return;
                        }
                        i = end;
                    }
                    if *append_at_ns > last_processed_ns {
                        last_processed_ns = *append_at_ns;
                    }
                }

                if !sent_any {
                    // Send heartbeat
                    let msg = volume_server_pb::VolumeTailSenderResponse {
                        is_last_chunk: true,
                        version,
                        ..Default::default()
                    };
                    if tx.send(Ok(msg)).await.is_err() {
                        return;
                    }
                }

                tokio::time::sleep(std::time::Duration::from_secs(2)).await;

                if idle_timeout == 0 {
                    last_timestamp_ns = last_processed_ns;
                    continue;
                }
                if last_processed_ns == last_timestamp_ns {
                    draining_seconds = draining_seconds.saturating_sub(1);
                    if draining_seconds == 0 {
                        return; // EOF
                    }
                } else {
                    last_timestamp_ns = last_processed_ns;
                    draining_seconds = idle_timeout;
                }
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn volume_tail_receiver(
        &self,
        request: Request<volume_server_pb::VolumeTailReceiverRequest>,
    ) -> Result<Response<volume_server_pb::VolumeTailReceiverResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        // Check volume exists
        {
            let store = self.state.store.read().unwrap();
            store.find_volume(vid).ok_or_else(|| {
                Status::not_found(format!("receiver not found volume id {}", vid))
            })?;
        }

        // Parse source address and connect
        let source = &req.source_volume_server;
        let grpc_addr = parse_grpc_address(source)
            .map_err(|e| Status::internal(format!("invalid source address {}: {}", source, e)))?;

        let channel = tonic::transport::Channel::from_shared(format!("http://{}", grpc_addr))
            .map_err(|e| Status::internal(format!("parse source: {}", e)))?
            .connect()
            .await
            .map_err(|e| Status::internal(format!("connect to {}: {}", grpc_addr, e)))?;

        let mut client = volume_server_pb::volume_server_client::VolumeServerClient::new(channel);

        // Call VolumeTailSender on source
        let mut stream = client
            .volume_tail_sender(volume_server_pb::VolumeTailSenderRequest {
                volume_id: req.volume_id,
                since_ns: req.since_ns,
                idle_timeout_seconds: req.idle_timeout_seconds,
            })
            .await
            .map_err(|e| Status::internal(format!("volume_tail_sender: {}", e)))?
            .into_inner();

        let state = self.state.clone();

        // Receive needles from source and write locally
        while let Some(resp) = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("recv from tail sender: {}", e)))?
        {
            let needle_header = resp.needle_header;
            let mut needle_body = resp.needle_body;

            if needle_header.is_empty() {
                continue;
            }

            // Collect all chunks if not last
            if !resp.is_last_chunk {
                // Need to receive remaining chunks
                loop {
                    let chunk = stream
                        .message()
                        .await
                        .map_err(|e| Status::internal(format!("recv chunk: {}", e)))?
                        .ok_or_else(|| Status::internal("unexpected end of tail stream"))?;
                    needle_body.extend_from_slice(&chunk.needle_body);
                    if chunk.is_last_chunk {
                        break;
                    }
                }
            }

            // Parse needle from header + body
            let mut n = Needle::default();
            n.read_header(&needle_header);
            n.read_body_v2(&needle_body)
                .map_err(|e| Status::internal(format!("parse needle body: {}", e)))?;

            // Write needle to local volume
            let mut store = state.store.write().unwrap();
            store
                .write_volume_needle(vid, &mut n)
                .map_err(|e| Status::internal(format!("write needle: {}", e)))?;
        }

        Ok(Response::new(
            volume_server_pb::VolumeTailReceiverResponse {},
        ))
    }

    // ---- EC operations ----

    async fn volume_ec_shards_generate(
        &self,
        request: Request<volume_server_pb::VolumeEcShardsGenerateRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsGenerateResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let collection = &req.collection;

        // Find the volume's directory and validate collection
        let (dir, vol_version, dat_file_size) = {
            let store = self.state.store.read().unwrap();
            let (loc_idx, vol) = store
                .find_volume(vid)
                .ok_or_else(|| Status::not_found(format!("volume {} not found", vid)))?;
            if vol.collection != req.collection {
                return Err(Status::internal(format!(
                    "existing collection:{} unexpected input: {}",
                    vol.collection, req.collection
                )));
            }
            let version = vol.version().0 as u32;
            let dat_size = vol.dat_file_size().unwrap_or(0) as i64;
            (
                store.locations[loc_idx].directory.clone(),
                version,
                dat_size,
            )
        };

        // Check existing .vif for EC shard config (matching Go's MaybeLoadVolumeInfo)
        let (data_shards, parity_shards) = {
            let base = crate::storage::volume::volume_file_name(&dir, collection, vid);
            let vif_path = format!("{}.vif", base);
            let mut ds = crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT as u32;
            let mut ps = crate::storage::erasure_coding::ec_shard::PARITY_SHARDS_COUNT as u32;
            if let Ok(vif_content) = std::fs::read_to_string(&vif_path) {
                if let Ok(vif_info) =
                    serde_json::from_str::<crate::storage::volume::VifVolumeInfo>(&vif_content)
                {
                    if let Some(ec) = vif_info.ec_shard_config {
                        if ec.data_shards > 0 && ec.parity_shards > 0 {
                            ds = ec.data_shards;
                            ps = ec.parity_shards;
                        }
                    }
                }
            }
            (ds, ps)
        };

        crate::storage::erasure_coding::ec_encoder::write_ec_files(
            &dir,
            collection,
            vid,
            data_shards as usize,
            parity_shards as usize,
        )
        .map_err(|e| Status::internal(e.to_string()))?;

        // Write .vif file with EC shard metadata
        {
            let base = crate::storage::volume::volume_file_name(&dir, collection, vid);
            let vif_path = format!("{}.vif", base);
            let vif = crate::storage::volume::VifVolumeInfo {
                version: vol_version,
                dat_file_size,
                ec_shard_config: Some(crate::storage::volume::VifEcShardConfig {
                    data_shards: data_shards,
                    parity_shards: parity_shards,
                }),
                ..Default::default()
            };
            let content = serde_json::to_string_pretty(&vif)
                .map_err(|e| Status::internal(format!("serialize vif: {}", e)))?;
            std::fs::write(&vif_path, content)
                .map_err(|e| Status::internal(format!("write vif: {}", e)))?;
        }

        Ok(Response::new(
            volume_server_pb::VolumeEcShardsGenerateResponse {},
        ))
    }

    async fn volume_ec_shards_rebuild(
        &self,
        request: Request<volume_server_pb::VolumeEcShardsRebuildRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsRebuildResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let collection = &req.collection;

        // Find the directory with EC files
        let store = self.state.store.read().unwrap();
        let dir = store.find_ec_dir(vid, collection);
        drop(store);

        let dir = match dir {
            Some(d) => d,
            None => {
                return Ok(Response::new(
                    volume_server_pb::VolumeEcShardsRebuildResponse {
                        rebuilt_shard_ids: vec![],
                    },
                ));
            }
        };

        // Check which shards are missing
        let (data_shards, parity_shards) =
            crate::storage::erasure_coding::ec_volume::read_ec_shard_config(&dir, vid);
        let total_shards = data_shards + parity_shards;

        let mut missing: Vec<u32> = Vec::new();
        for shard_id in 0..total_shards as u8 {
            let shard = crate::storage::erasure_coding::ec_shard::EcVolumeShard::new(
                &dir, collection, vid, shard_id,
            );
            if !std::path::Path::new(&shard.file_name()).exists() {
                missing.push(shard_id as u32);
            }
        }

        if missing.is_empty() {
            return Ok(Response::new(
                volume_server_pb::VolumeEcShardsRebuildResponse {
                    rebuilt_shard_ids: vec![],
                },
            ));
        }

        // Rebuild missing shards by regenerating all missing EC files via Reed-Solomon reconstruct
        crate::storage::erasure_coding::ec_encoder::rebuild_ec_files(
            &dir,
            collection,
            vid,
            &missing,
            data_shards as usize,
            parity_shards as usize,
        )
        .map_err(|e| Status::internal(format!("rebuild ec shards: {}", e)))?;

        Ok(Response::new(
            volume_server_pb::VolumeEcShardsRebuildResponse {
                rebuilt_shard_ids: missing,
            },
        ))
    }

    async fn volume_ec_shards_copy(
        &self,
        request: Request<volume_server_pb::VolumeEcShardsCopyRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsCopyResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        // Validate disk_id
        let (_loc_count, dest_dir) = {
            let store = self.state.store.read().unwrap();
            let count = store.locations.len();
            let dir = if (req.disk_id as usize) < count {
                store.locations[req.disk_id as usize].directory.clone()
            } else {
                return Err(Status::invalid_argument(format!(
                    "invalid disk_id {}: only have {} disks",
                    req.disk_id, count
                )));
            };
            (count, dir)
        };

        // Connect to source and copy shard files via CopyFile
        let source = &req.source_data_node;
        // Parse source address: "ip:port.grpc_port"
        let parts: Vec<&str> = source.split('.').collect();
        if parts.len() != 2 {
            return Err(Status::internal(format!(
                "VolumeEcShardsCopy volume {} invalid source_data_node {}",
                vid, source
            )));
        }
        let grpc_addr = format!(
            "{}:{}",
            parts[0]
                .rsplit_once(':')
                .map(|(h, _)| h)
                .unwrap_or(parts[0]),
            parts[1]
        );

        let channel = tonic::transport::Channel::from_shared(format!("http://{}", grpc_addr))
            .map_err(|e| {
                Status::internal(format!(
                    "VolumeEcShardsCopy volume {} parse source: {}",
                    vid, e
                ))
            })?
            .connect()
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "VolumeEcShardsCopy volume {} connect to {}: {}",
                    vid, grpc_addr, e
                ))
            })?;

        let mut client = volume_server_pb::volume_server_client::VolumeServerClient::new(channel);

        // Copy each shard
        for &shard_id in &req.shard_ids {
            let ext = format!(".ec{:02}", shard_id);
            let copy_req = volume_server_pb::CopyFileRequest {
                volume_id: req.volume_id,
                collection: req.collection.clone(),
                is_ec_volume: true,
                ext: ext.clone(),
                compaction_revision: u32::MAX,
                stop_offset: i64::MAX as u64,
                ..Default::default()
            };
            let mut stream = client
                .copy_file(copy_req)
                .await
                .map_err(|e| {
                    Status::internal(format!(
                        "VolumeEcShardsCopy volume {} copy {}: {}",
                        vid, ext, e
                    ))
                })?
                .into_inner();

            let file_path = {
                let base =
                    crate::storage::volume::volume_file_name(&dest_dir, &req.collection, vid);
                format!("{}{}", base, ext)
            };
            let mut file = std::fs::File::create(&file_path)
                .map_err(|e| Status::internal(format!("create {}: {}", file_path, e)))?;
            while let Some(chunk) = stream
                .message()
                .await
                .map_err(|e| Status::internal(format!("recv {}: {}", ext, e)))?
            {
                use std::io::Write;
                file.write_all(&chunk.file_content)
                    .map_err(|e| Status::internal(format!("write {}: {}", file_path, e)))?;
            }
        }

        // Copy .ecx file if requested
        if req.copy_ecx_file {
            let copy_req = volume_server_pb::CopyFileRequest {
                volume_id: req.volume_id,
                collection: req.collection.clone(),
                is_ec_volume: true,
                ext: ".ecx".to_string(),
                compaction_revision: u32::MAX,
                stop_offset: i64::MAX as u64,
                ..Default::default()
            };
            let mut stream = client
                .copy_file(copy_req)
                .await
                .map_err(|e| {
                    Status::internal(format!(
                        "VolumeEcShardsCopy volume {} copy .ecx: {}",
                        vid, e
                    ))
                })?
                .into_inner();

            let file_path = {
                let base =
                    crate::storage::volume::volume_file_name(&dest_dir, &req.collection, vid);
                format!("{}.ecx", base)
            };
            let mut file = std::fs::File::create(&file_path)
                .map_err(|e| Status::internal(format!("create {}: {}", file_path, e)))?;
            while let Some(chunk) = stream
                .message()
                .await
                .map_err(|e| Status::internal(format!("recv .ecx: {}", e)))?
            {
                use std::io::Write;
                file.write_all(&chunk.file_content)
                    .map_err(|e| Status::internal(format!("write {}: {}", file_path, e)))?;
            }
        }

        // Copy .ecj file if requested
        if req.copy_ecj_file {
            let copy_req = volume_server_pb::CopyFileRequest {
                volume_id: req.volume_id,
                collection: req.collection.clone(),
                is_ec_volume: true,
                ext: ".ecj".to_string(),
                compaction_revision: u32::MAX,
                stop_offset: i64::MAX as u64,
                ignore_source_file_not_found: true,
                ..Default::default()
            };
            let mut stream = client
                .copy_file(copy_req)
                .await
                .map_err(|e| {
                    Status::internal(format!(
                        "VolumeEcShardsCopy volume {} copy .ecj: {}",
                        vid, e
                    ))
                })?
                .into_inner();

            let file_path = {
                let base =
                    crate::storage::volume::volume_file_name(&dest_dir, &req.collection, vid);
                format!("{}.ecj", base)
            };
            let mut file = std::fs::File::create(&file_path)
                .map_err(|e| Status::internal(format!("create {}: {}", file_path, e)))?;
            while let Some(chunk) = stream
                .message()
                .await
                .map_err(|e| Status::internal(format!("recv .ecj: {}", e)))?
            {
                use std::io::Write;
                file.write_all(&chunk.file_content)
                    .map_err(|e| Status::internal(format!("write {}: {}", file_path, e)))?;
            }
        }

        // Copy .vif file if requested
        if req.copy_vif_file {
            let copy_req = volume_server_pb::CopyFileRequest {
                volume_id: req.volume_id,
                collection: req.collection.clone(),
                is_ec_volume: true,
                ext: ".vif".to_string(),
                compaction_revision: u32::MAX,
                stop_offset: i64::MAX as u64,
                ..Default::default()
            };
            let mut stream = client
                .copy_file(copy_req)
                .await
                .map_err(|e| {
                    Status::internal(format!(
                        "VolumeEcShardsCopy volume {} copy .vif: {}",
                        vid, e
                    ))
                })?
                .into_inner();

            let file_path = {
                let base =
                    crate::storage::volume::volume_file_name(&dest_dir, &req.collection, vid);
                format!("{}.vif", base)
            };
            let mut file = std::fs::File::create(&file_path)
                .map_err(|e| Status::internal(format!("create {}: {}", file_path, e)))?;
            while let Some(chunk) = stream
                .message()
                .await
                .map_err(|e| Status::internal(format!("recv .vif: {}", e)))?
            {
                use std::io::Write;
                file.write_all(&chunk.file_content)
                    .map_err(|e| Status::internal(format!("write {}: {}", file_path, e)))?;
            }
        }

        Ok(Response::new(
            volume_server_pb::VolumeEcShardsCopyResponse {},
        ))
    }

    async fn volume_ec_shards_delete(
        &self,
        request: Request<volume_server_pb::VolumeEcShardsDeleteRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsDeleteResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let mut store = self.state.store.write().unwrap();
        store.delete_ec_shards(vid, &req.collection, &req.shard_ids);
        Ok(Response::new(
            volume_server_pb::VolumeEcShardsDeleteResponse {},
        ))
    }

    async fn volume_ec_shards_mount(
        &self,
        request: Request<volume_server_pb::VolumeEcShardsMountRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsMountResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        // Check all requested shards exist on disk
        {
            let store = self.state.store.read().unwrap();
            for &shard_id in &req.shard_ids {
                if store
                    .find_ec_shard_dir(vid, &req.collection, shard_id as u8)
                    .is_none()
                {
                    return Err(Status::not_found(format!(
                        "ec volume {} shards {:?} not found",
                        req.volume_id, req.shard_ids
                    )));
                }
            }
        }

        let mut store = self.state.store.write().unwrap();
        store
            .mount_ec_shards(vid, &req.collection, &req.shard_ids)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(
            volume_server_pb::VolumeEcShardsMountResponse {},
        ))
    }

    async fn volume_ec_shards_unmount(
        &self,
        request: Request<volume_server_pb::VolumeEcShardsUnmountRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsUnmountResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let mut store = self.state.store.write().unwrap();
        store.unmount_ec_shards(vid, &req.shard_ids);
        Ok(Response::new(
            volume_server_pb::VolumeEcShardsUnmountResponse {},
        ))
    }

    type VolumeEcShardReadStream = BoxStream<volume_server_pb::VolumeEcShardReadResponse>;
    async fn volume_ec_shard_read(
        &self,
        request: Request<volume_server_pb::VolumeEcShardReadRequest>,
    ) -> Result<Response<Self::VolumeEcShardReadStream>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        let store = self.state.store.read().unwrap();
        let ec_vol = store.ec_volumes.get(&vid).ok_or_else(|| {
            Status::not_found(format!(
                "ec volume {} shard {} not found",
                req.volume_id, req.shard_id
            ))
        })?;

        // Check if the requested needle is deleted
        if req.file_key > 0 {
            let needle_id = NeedleId(req.file_key);
            let deleted_needles = ec_vol
                .read_deleted_needles()
                .map_err(|e| Status::internal(e.to_string()))?;
            if deleted_needles.contains(&needle_id) {
                let results = vec![Ok(volume_server_pb::VolumeEcShardReadResponse {
                    is_deleted: true,
                    ..Default::default()
                })];
                return Ok(Response::new(Box::pin(tokio_stream::iter(results))));
            }
        }

        // Read from the shard
        let shard = ec_vol
            .shards
            .get(req.shard_id as usize)
            .and_then(|s| s.as_ref())
            .ok_or_else(|| {
                Status::not_found(format!(
                    "ec volume {} shard {} not mounted",
                    req.volume_id, req.shard_id
                ))
            })?;

        let read_size = if req.size > 0 {
            req.size as usize
        } else {
            1024 * 1024
        };
        let mut buf = vec![0u8; read_size];
        let n = shard
            .read_at(&mut buf, req.offset as u64)
            .map_err(|e| Status::internal(e.to_string()))?;
        buf.truncate(n);

        let results = vec![Ok(volume_server_pb::VolumeEcShardReadResponse {
            data: buf,
            is_deleted: false,
        })];
        Ok(Response::new(Box::pin(tokio_stream::iter(results))))
    }

    async fn volume_ec_blob_delete(
        &self,
        request: Request<volume_server_pb::VolumeEcBlobDeleteRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcBlobDeleteResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let needle_id = NeedleId(req.file_key);

        let mut store = self.state.store.write().unwrap();
        if let Some(ec_vol) = store.ec_volumes.get_mut(&vid) {
            ec_vol
                .journal_delete(needle_id)
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        // If EC volume not mounted, it's a no-op (matching Go behavior)
        Ok(Response::new(
            volume_server_pb::VolumeEcBlobDeleteResponse {},
        ))
    }

    async fn volume_ec_shards_to_volume(
        &self,
        request: Request<volume_server_pb::VolumeEcShardsToVolumeRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsToVolumeResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        let store = self.state.store.read().unwrap();
        let ec_vol = store
            .ec_volumes
            .get(&vid)
            .ok_or_else(|| Status::not_found(format!("ec volume {} not found", req.volume_id)))?;

        if ec_vol.collection != req.collection {
            return Err(Status::internal(format!(
                "existing collection:{} unexpected input: {}",
                ec_vol.collection, req.collection
            )));
        }

        // Use EC context data shard count from the volume
        let data_shards = ec_vol.data_shards as usize;

        // Check that all data shards are present
        for shard_id in 0..data_shards {
            if ec_vol
                .shards
                .get(shard_id)
                .map(|s| s.is_none())
                .unwrap_or(true)
            {
                return Err(Status::internal(format!(
                    "ec volume {} missing shard {}",
                    req.volume_id, shard_id
                )));
            }
        }

        // Read the .ecx index to check for live entries
        let ecx_path = ec_vol.ecx_file_name();
        let ecx_data =
            std::fs::read(&ecx_path).map_err(|e| Status::internal(format!("read ecx: {}", e)))?;
        let entry_count = ecx_data.len() / NEEDLE_MAP_ENTRY_SIZE;

        let mut has_live = false;
        for i in 0..entry_count {
            let start = i * NEEDLE_MAP_ENTRY_SIZE;
            let (_, _, size) =
                idx_entry_from_bytes(&ecx_data[start..start + NEEDLE_MAP_ENTRY_SIZE]);
            if !size.is_deleted() {
                has_live = true;
                break;
            }
        }

        if !has_live {
            return Err(Status::failed_precondition(format!(
                "ec volume {} has no live entries",
                req.volume_id
            )));
        }

        // Reconstruct the volume from EC shards
        let dir = ec_vol.dir.clone();
        let collection = ec_vol.collection.clone();
        drop(store);

        // Calculate .dat file size from .ecx entries
        let dat_file_size =
            crate::storage::erasure_coding::ec_decoder::find_dat_file_size(&dir, &collection, vid)
                .map_err(|e| Status::internal(format!("FindDatFileSize: {}", e)))?;

        // Write .dat file using block-interleaved reading from shards
        crate::storage::erasure_coding::ec_decoder::write_dat_file_from_shards(
            &dir,
            &collection,
            vid,
            dat_file_size,
            data_shards,
        )
        .map_err(|e| Status::internal(format!("WriteDatFile: {}", e)))?;

        // Write .idx file from .ecx and .ecj files
        crate::storage::erasure_coding::ec_decoder::write_idx_file_from_ec_index(
            &dir,
            &collection,
            vid,
        )
        .map_err(|e| Status::internal(format!("WriteIdxFileFromEcIndex: {}", e)))?;

        // Unmount EC shards and mount the reconstructed volume
        {
            let mut store = self.state.store.write().unwrap();
            // Remove EC volume
            if let Some(mut ec_vol) = store.ec_volumes.remove(&vid) {
                ec_vol.close();
            }
            // Unmount existing volume if any, then mount fresh
            store.unmount_volume(vid);
            store
                .mount_volume(vid, &collection, DiskType::HardDrive)
                .map_err(|e| Status::internal(format!("mount volume: {}", e)))?;
        }

        Ok(Response::new(
            volume_server_pb::VolumeEcShardsToVolumeResponse {},
        ))
    }

    async fn volume_ec_shards_info(
        &self,
        request: Request<volume_server_pb::VolumeEcShardsInfoRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsInfoResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        let store = self.state.store.read().unwrap();
        let ec_vol = store
            .ec_volumes
            .get(&vid)
            .ok_or_else(|| Status::not_found(format!("ec volume {} not found", req.volume_id)))?;

        let mut shard_infos = Vec::new();
        for (i, shard) in ec_vol.shards.iter().enumerate() {
            if let Some(s) = shard {
                shard_infos.push(volume_server_pb::EcShardInfo {
                    shard_id: i as u32,
                    size: s.file_size(),
                    collection: ec_vol.collection.clone(),
                    volume_id: req.volume_id,
                });
            }
        }

        // Calculate volume size from shards
        let volume_size = ec_vol
            .shards
            .iter()
            .filter_map(|s| s.as_ref())
            .map(|s| s.file_size())
            .sum::<i64>() as u64;

        Ok(Response::new(
            volume_server_pb::VolumeEcShardsInfoResponse {
                ec_shard_infos: shard_infos,
                volume_size,
                file_count: 0,
                file_deleted_count: 0,
            },
        ))
    }

    // ---- Tiered storage ----

    type VolumeTierMoveDatToRemoteStream =
        BoxStream<volume_server_pb::VolumeTierMoveDatToRemoteResponse>;
    async fn volume_tier_move_dat_to_remote(
        &self,
        request: Request<volume_server_pb::VolumeTierMoveDatToRemoteRequest>,
    ) -> Result<Response<Self::VolumeTierMoveDatToRemoteStream>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        // Validate volume exists and collection matches
        let dat_path = {
            let store = self.state.store.read().unwrap();
            let (_, vol) = store
                .find_volume(vid)
                .ok_or_else(|| Status::not_found(format!("volume {} not found", req.volume_id)))?;

            if vol.collection != req.collection {
                return Err(Status::invalid_argument(format!(
                    "existing collection:{} unexpected input: {}",
                    vol.collection, req.collection
                )));
            }

            // Check if already on remote
            if vol.has_remote_file {
                // Already on remote -- return empty stream (matches Go: returns nil)
                let stream = tokio_stream::empty();
                return Ok(Response::new(
                    Box::pin(stream) as Self::VolumeTierMoveDatToRemoteStream
                ));
            }

            // Check if the destination backend already exists in volume info
            let (backend_type, backend_id) =
                crate::remote_storage::s3_tier::backend_name_to_type_id(
                    &req.destination_backend_name,
                );
            for rf in &vol.volume_info.files {
                if rf.backend_type == backend_type && rf.backend_id == backend_id {
                    return Err(Status::already_exists(format!(
                        "destination {} already exists",
                        req.destination_backend_name
                    )));
                }
            }

            vol.dat_path()
        };

        // Look up the S3 tier backend
        let backend = {
            let registry = self.state.s3_tier_registry.read().unwrap();
            registry.get(&req.destination_backend_name).ok_or_else(|| {
                let keys = registry.names();
                Status::not_found(format!(
                    "destination {} not found, supported: {:?}",
                    req.destination_backend_name, keys
                ))
            })?
        };

        let (backend_type, backend_id) =
            crate::remote_storage::s3_tier::backend_name_to_type_id(&req.destination_backend_name);

        let (tx, rx) = tokio::sync::mpsc::channel::<
            Result<volume_server_pb::VolumeTierMoveDatToRemoteResponse, Status>,
        >(16);
        let state = self.state.clone();
        let keep_local = req.keep_local_dat_file;
        let dest_backend_name = req.destination_backend_name.clone();

        tokio::spawn(async move {
            let result: Result<(), Status> = async {
                // Upload the .dat file to S3 with progress
                let tx_progress = tx.clone();
                let mut last_report = std::time::Instant::now();
                let (key, size) = backend
                    .upload_file(&dat_path, move |processed, percentage| {
                        let now = std::time::Instant::now();
                        if now.duration_since(last_report) >= std::time::Duration::from_secs(1) {
                            last_report = now;
                            let _ = tx_progress.try_send(Ok(
                                volume_server_pb::VolumeTierMoveDatToRemoteResponse {
                                    processed,
                                    processed_percentage: percentage,
                                },
                            ));
                        }
                    })
                    .await
                    .map_err(|e| {
                        Status::internal(format!(
                            "backend {} copy file {}: {}",
                            dest_backend_name, dat_path, e
                        ))
                    })?;

                // Update volume info with remote file reference
                {
                    let mut store = state.store.write().unwrap();
                    if let Some((_, vol)) = store.find_volume_mut(vid) {
                        let now_unix = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();

                        vol.volume_info.files.push(volume_server_pb::RemoteFile {
                            backend_type: backend_type.clone(),
                            backend_id: backend_id.clone(),
                            key,
                            offset: 0,
                            file_size: size,
                            modified_time: now_unix,
                            extension: ".dat".to_string(),
                        });
                        vol.has_remote_file = true;

                        if let Err(e) = vol.save_volume_info() {
                            return Err(Status::internal(format!(
                                "volume {} failed to save remote file info: {}",
                                vid, e
                            )));
                        }

                        // Optionally remove local .dat file
                        if !keep_local {
                            let dat = vol.dat_path();
                            let _ = std::fs::remove_file(&dat);
                        }
                    }
                }

                // Go does NOT send a final 100% progress message after upload completion
                Ok(())
            }
            .await;

            if let Err(e) = result {
                let _ = tx.send(Err(e)).await;
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(stream) as Self::VolumeTierMoveDatToRemoteStream
        ))
    }

    type VolumeTierMoveDatFromRemoteStream =
        BoxStream<volume_server_pb::VolumeTierMoveDatFromRemoteResponse>;
    async fn volume_tier_move_dat_from_remote(
        &self,
        request: Request<volume_server_pb::VolumeTierMoveDatFromRemoteRequest>,
    ) -> Result<Response<Self::VolumeTierMoveDatFromRemoteStream>, Status> {
        // Note: Go does NOT check maintenance mode for TierMoveDatFromRemote
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        // Validate volume and get remote storage info
        let (dat_path, storage_name, storage_key) = {
            let store = self.state.store.read().unwrap();
            let (_, vol) = store
                .find_volume(vid)
                .ok_or_else(|| Status::not_found(format!("volume {} not found", req.volume_id)))?;

            if vol.collection != req.collection {
                return Err(Status::invalid_argument(format!(
                    "existing collection:{} unexpected input: {}",
                    vol.collection, req.collection
                )));
            }

            let (storage_name, storage_key) = vol.remote_storage_name_key();
            if storage_name.is_empty() || storage_key.is_empty() {
                return Err(Status::failed_precondition(format!(
                    "volume {} is already on local disk",
                    vid
                )));
            }

            // Check if the dat file already exists locally (matches Go's DataBackend DiskFile check)
            let dat_path = vol.dat_path();
            if std::path::Path::new(&dat_path).exists() {
                return Err(Status::failed_precondition(format!(
                    "volume {} is already on local disk",
                    vid
                )));
            }

            (dat_path, storage_name, storage_key)
        };

        // Look up the S3 tier backend
        let backend = {
            let registry = self.state.s3_tier_registry.read().unwrap();
            registry.get(&storage_name).ok_or_else(|| {
                let keys = registry.names();
                Status::not_found(format!(
                    "remote storage {} not found from supported: {:?}",
                    storage_name, keys
                ))
            })?
        };

        let (tx, rx) = tokio::sync::mpsc::channel::<
            Result<volume_server_pb::VolumeTierMoveDatFromRemoteResponse, Status>,
        >(16);
        let state = self.state.clone();
        let keep_remote = req.keep_remote_dat_file;

        tokio::spawn(async move {
            let result: Result<(), Status> = async {
                // Download the .dat file from S3 with progress
                let tx_progress = tx.clone();
                let mut last_report = std::time::Instant::now();
                let storage_name_clone = storage_name.clone();
                let _size = backend
                    .download_file(&dat_path, &storage_key, move |processed, percentage| {
                        let now = std::time::Instant::now();
                        if now.duration_since(last_report) >= std::time::Duration::from_secs(1) {
                            last_report = now;
                            let _ = tx_progress.try_send(Ok(
                                volume_server_pb::VolumeTierMoveDatFromRemoteResponse {
                                    processed,
                                    processed_percentage: percentage,
                                },
                            ));
                        }
                    })
                    .await
                    .map_err(|e| {
                        Status::internal(format!(
                            "backend {} copy file {}: {}",
                            storage_name_clone, dat_path, e
                        ))
                    })?;

                if !keep_remote {
                    // Delete remote file
                    backend.delete_file(&storage_key).await.map_err(|e| {
                        Status::internal(format!(
                            "volume {} failed to delete remote file {}: {}",
                            vid, storage_key, e
                        ))
                    })?;

                    // Update volume info: remove remote file reference
                    {
                        let mut store = state.store.write().unwrap();
                        if let Some((_, vol)) = store.find_volume_mut(vid) {
                            if !vol.volume_info.files.is_empty() {
                                vol.volume_info.files.remove(0);
                            }
                            vol.has_remote_file = !vol.volume_info.files.is_empty();

                            if let Err(e) = vol.save_volume_info() {
                                return Err(Status::internal(format!(
                                    "volume {} failed to save remote file info: {}",
                                    vid, e
                                )));
                            }
                        }
                    }
                }

                // Go does NOT send a final 100% progress message after download completion
                Ok(())
            }
            .await;

            if let Err(e) = result {
                let _ = tx.send(Err(e)).await;
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(stream) as Self::VolumeTierMoveDatFromRemoteStream
        ))
    }

    // ---- Server management ----

    async fn volume_server_status(
        &self,
        _request: Request<volume_server_pb::VolumeServerStatusRequest>,
    ) -> Result<Response<volume_server_pb::VolumeServerStatusResponse>, Status> {
        let store = self.state.store.read().unwrap();

        let mut disk_statuses = Vec::new();
        for loc in &store.locations {
            let (all, free) = get_disk_usage(&loc.directory);
            let used = all.saturating_sub(free);
            let percent_free = if all > 0 {
                (free as f32 / all as f32) * 100.0
            } else {
                0.0
            };
            let percent_used = if all > 0 {
                (used as f32 / all as f32) * 100.0
            } else {
                0.0
            };
            disk_statuses.push(volume_server_pb::DiskStatus {
                dir: loc.directory.clone(),
                all,
                used,
                free,
                percent_free,
                percent_used,
                disk_type: loc.disk_type.to_string(),
            });
        }

        Ok(Response::new(
            volume_server_pb::VolumeServerStatusResponse {
                disk_statuses,
                memory_status: Some(get_mem_status()),
                version: crate::version::full_version().to_string(),
                data_center: self.state.data_center.clone(),
                rack: self.state.rack.clone(),
                state: Some(volume_server_pb::VolumeServerState {
                    maintenance: self.state.maintenance.load(Ordering::Relaxed),
                    version: self.state.state_version.load(Ordering::Relaxed),
                }),
            },
        ))
    }

    async fn volume_server_leave(
        &self,
        _request: Request<volume_server_pb::VolumeServerLeaveRequest>,
    ) -> Result<Response<volume_server_pb::VolumeServerLeaveResponse>, Status> {
        *self.state.is_stopping.write().unwrap() = true;
        self.state.is_heartbeating.store(false, Ordering::Relaxed);
        // Wake heartbeat loop to send deregistration.
        self.state.volume_state_notify.notify_one();
        Ok(Response::new(
            volume_server_pb::VolumeServerLeaveResponse {},
        ))
    }

    async fn fetch_and_write_needle(
        &self,
        request: Request<volume_server_pb::FetchAndWriteNeedleRequest>,
    ) -> Result<Response<volume_server_pb::FetchAndWriteNeedleResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        // Check volume exists
        {
            let store = self.state.store.read().unwrap();
            store
                .find_volume(vid)
                .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;
        }

        // Get remote storage configuration
        let remote_conf = req
            .remote_conf
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("remote storage configuration is required"))?;

        // Create remote storage client
        let client =
            crate::remote_storage::make_remote_storage_client(remote_conf).map_err(|e| {
                Status::internal(format!(
                    "get remote client: make remote storage client {}: {}",
                    remote_conf.name, e,
                ))
            })?;

        let remote_location = req
            .remote_location
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("remote storage location is required"))?;

        // Read data from remote storage
        let data = client
            .read_file(remote_location, req.offset, req.size)
            .await
            .map_err(|e| {
                Status::internal(format!("read from remote {:?}: {}", remote_location, e))
            })?;

        // Build needle and write locally
        let mut n = Needle {
            id: NeedleId(req.needle_id),
            cookie: Cookie(req.cookie),
            data_size: data.len() as u32,
            data: data.clone(),
            ..Needle::default()
        };
        n.checksum = crate::storage::needle::crc::CRC::new(&n.data);
        n.size = crate::storage::types::Size(4 + n.data_size as i32 + 1);
        n.last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        n.set_has_last_modified_date();

        let e_tag;
        {
            let mut store = self.state.store.write().unwrap();
            match store.write_volume_needle(vid, &mut n) {
                Ok(_) => {
                    e_tag = n.etag();
                }
                Err(e) => {
                    return Err(Status::internal(format!(
                        "local write needle {} size {}: {}",
                        req.needle_id, req.size, e
                    )));
                }
            }
        }

        // Replicate to peers concurrently (matches Go's sync.WaitGroup + goroutines)
        if !req.replicas.is_empty() {
            let file_id = format!("{},{:x}{:08x}", vid, req.needle_id, req.cookie);
            let http_client = reqwest::Client::new();
            let mut handles = Vec::new();
            for replica in &req.replicas {
                let url = format!("http://{}/{}?type=replicate", replica.url, file_id);
                let data_clone = data.clone();
                let client_clone = http_client.clone();
                let needle_id = req.needle_id;
                let size = req.size;
                handles.push(tokio::spawn(async move {
                    let form = reqwest::multipart::Form::new()
                        .part("file", reqwest::multipart::Part::bytes(data_clone));
                    client_clone
                        .post(&url)
                        .multipart(form)
                        .send()
                        .await
                        .map_err(|e| {
                            format!("remote write needle {} size {}: {}", needle_id, size, e)
                        })
                }));
            }
            for handle in handles {
                match handle.await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => return Err(Status::internal(e)),
                    Err(e) => {
                        return Err(Status::internal(format!("replication task failed: {}", e)))
                    }
                }
            }
        }

        Ok(Response::new(
            volume_server_pb::FetchAndWriteNeedleResponse { e_tag },
        ))
    }

    async fn scrub_volume(
        &self,
        request: Request<volume_server_pb::ScrubVolumeRequest>,
    ) -> Result<Response<volume_server_pb::ScrubVolumeResponse>, Status> {
        let req = request.into_inner();
        let store = self.state.store.read().unwrap();

        let vids: Vec<VolumeId> = if req.volume_ids.is_empty() {
            store.all_volume_ids()
        } else {
            req.volume_ids.iter().map(|&id| VolumeId(id)).collect()
        };

        // Validate mode
        let mode = req.mode;
        match mode {
            1 | 2 | 3 => {} // INDEX=1, FULL=2, LOCAL=3
            _ => {
                return Err(Status::invalid_argument(format!(
                    "unsupported volume scrub mode {}",
                    mode
                )))
            }
        }

        let mut total_volumes: u64 = 0;
        let mut total_files: u64 = 0;
        let mut broken_volume_ids: Vec<u32> = Vec::new();
        let mut details: Vec<String> = Vec::new();

        for vid in &vids {
            let (_, v) = store
                .find_volume(*vid)
                .ok_or_else(|| Status::not_found(format!("volume id {} not found", vid.0)))?;
            total_volumes += 1;

            match v.scrub() {
                Ok((files, broken)) => {
                    total_files += files;
                    if !broken.is_empty() {
                        broken_volume_ids.push(vid.0);
                        for msg in broken {
                            details.push(format!("vol {}: {}", vid.0, msg));
                        }
                    }
                }
                Err(e) => {
                    broken_volume_ids.push(vid.0);
                    details.push(format!("vol {}: scrub error: {}", vid.0, e));
                }
            }
        }

        Ok(Response::new(volume_server_pb::ScrubVolumeResponse {
            total_volumes,
            total_files,
            broken_volume_ids,
            details,
        }))
    }

    async fn scrub_ec_volume(
        &self,
        request: Request<volume_server_pb::ScrubEcVolumeRequest>,
    ) -> Result<Response<volume_server_pb::ScrubEcVolumeResponse>, Status> {
        let req = request.into_inner();

        // Validate mode
        let mode = req.mode;
        match mode {
            1 | 2 | 3 => {} // INDEX=1, FULL=2, LOCAL=3
            _ => {
                return Err(Status::invalid_argument(format!(
                    "unsupported EC volume scrub mode {}",
                    mode
                )))
            }
        }

        let store = self.state.store.read().unwrap();
        let vids: Vec<VolumeId> = if req.volume_ids.is_empty() {
            store.ec_volumes.keys().copied().collect()
        } else {
            req.volume_ids.iter().map(|&id| VolumeId(id)).collect()
        };

        let mut total_volumes: u64 = 0;
        let total_files: u64 = 0;
        let mut broken_volume_ids: Vec<u32> = Vec::new();
        let mut broken_shard_infos: Vec<volume_server_pb::EcShardInfo> = Vec::new();
        let mut details: Vec<String> = Vec::new();

        for vid in &vids {
            let collection = {
                if let Some(ecv) = store.ec_volumes.get(vid) {
                    ecv.collection.clone()
                } else {
                    return Err(Status::not_found(format!(
                        "EC volume id {} not found",
                        vid.0
                    )));
                }
            };

            let dir = store
                .find_ec_dir(*vid, &collection)
                .unwrap_or_else(|| String::from(""));
            if dir.is_empty() {
                continue;
            }

            total_volumes += 1;
            let (data_shards, parity_shards) =
                crate::storage::erasure_coding::ec_volume::read_ec_shard_config(&dir, *vid);

            match crate::storage::erasure_coding::ec_encoder::verify_ec_shards(
                &dir,
                &collection,
                *vid,
                data_shards as usize,
                parity_shards as usize,
            ) {
                Ok((broken, msgs)) => {
                    if !broken.is_empty() {
                        broken_volume_ids.push(vid.0);
                        for b in broken {
                            broken_shard_infos.push(volume_server_pb::EcShardInfo {
                                volume_id: vid.0,
                                collection: collection.clone(),
                                shard_id: b,
                                ..Default::default()
                            });
                        }
                    }
                    for msg in msgs {
                        details.push(format!("ecvol {}: {}", vid.0, msg));
                    }
                }
                Err(e) => {
                    broken_volume_ids.push(vid.0);
                    details.push(format!("ecvol {}: scrub error: {}", vid.0, e));
                }
            }
        }

        Ok(Response::new(volume_server_pb::ScrubEcVolumeResponse {
            total_volumes,
            total_files,
            broken_volume_ids,
            broken_shard_infos,
            details,
        }))
    }

    type QueryStream = BoxStream<volume_server_pb::QueriedStripe>;
    async fn query(
        &self,
        request: Request<volume_server_pb::QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let req = request.into_inner();
        let mut stripes: Vec<Result<volume_server_pb::QueriedStripe, Status>> = Vec::new();

        for fid_str in &req.from_file_ids {
            let file_id =
                needle::FileId::parse(fid_str).map_err(|e| Status::invalid_argument(e))?;

            let mut n = Needle {
                id: file_id.key,
                cookie: file_id.cookie,
                ..Needle::default()
            };
            let original_cookie = n.cookie;

            let store = self.state.store.read().unwrap();
            store
                .read_volume_needle(file_id.volume_id, &mut n)
                .map_err(|e| Status::internal(e.to_string()))?;
            drop(store);

            // Cookie mismatch: return empty stream (matching Go behavior where err is nil)
            if n.cookie != original_cookie {
                let stream = tokio_stream::iter(stripes);
                return Ok(Response::new(Box::pin(stream)));
            }

            let input = req.input_serialization.as_ref();

            // CSV input: no output (Go does nothing for CSV)
            if input.map_or(false, |i| i.csv_input.is_some()) {
                // No stripes emitted for CSV
                continue;
            }

            // JSON input: process lines
            if input.map_or(false, |i| i.json_input.is_some()) {
                let filter = req.filter.as_ref();
                let data_str = String::from_utf8_lossy(&n.data);
                let mut records: Vec<u8> = Vec::new();

                for line in data_str.lines() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let parsed: serde_json::Value = match serde_json::from_str(line) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    // Apply filter
                    if let Some(f) = filter {
                        if !f.field.is_empty() && !f.operand.is_empty() {
                            let field_val = &parsed[&f.field];
                            let pass = match f.operand.as_str() {
                                ">" => {
                                    if let (Some(fv), Ok(tv)) =
                                        (field_val.as_f64(), f.value.parse::<f64>())
                                    {
                                        fv > tv
                                    } else {
                                        false
                                    }
                                }
                                ">=" => {
                                    if let (Some(fv), Ok(tv)) =
                                        (field_val.as_f64(), f.value.parse::<f64>())
                                    {
                                        fv >= tv
                                    } else {
                                        false
                                    }
                                }
                                "<" => {
                                    if let (Some(fv), Ok(tv)) =
                                        (field_val.as_f64(), f.value.parse::<f64>())
                                    {
                                        fv < tv
                                    } else {
                                        false
                                    }
                                }
                                "<=" => {
                                    if let (Some(fv), Ok(tv)) =
                                        (field_val.as_f64(), f.value.parse::<f64>())
                                    {
                                        fv <= tv
                                    } else {
                                        false
                                    }
                                }
                                "=" => {
                                    if let (Some(fv), Ok(tv)) =
                                        (field_val.as_f64(), f.value.parse::<f64>())
                                    {
                                        fv == tv
                                    } else {
                                        field_val.as_str().map_or(false, |s| s == f.value)
                                    }
                                }
                                "!=" => {
                                    if let (Some(fv), Ok(tv)) =
                                        (field_val.as_f64(), f.value.parse::<f64>())
                                    {
                                        fv != tv
                                    } else {
                                        field_val.as_str().map_or(true, |s| s != f.value)
                                    }
                                }
                                _ => true,
                            };
                            if !pass {
                                continue;
                            }
                        }
                    }

                    // Build output record: {selection:value,...} (Go's ToJson format)
                    records.push(b'{');
                    for (i, sel) in req.selections.iter().enumerate() {
                        if i > 0 {
                            records.push(b',');
                        }
                        records.extend_from_slice(sel.as_bytes());
                        records.push(b':');
                        let val = &parsed[sel];
                        let raw = if val.is_null() {
                            "null".to_string()
                        } else {
                            // Use the raw JSON representation
                            val.to_string()
                        };
                        records.extend_from_slice(raw.as_bytes());
                    }
                    records.push(b'}');
                }

                stripes.push(Ok(volume_server_pb::QueriedStripe { records }));
            }
        }

        let stream = tokio_stream::iter(stripes);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn volume_needle_status(
        &self,
        request: Request<volume_server_pb::VolumeNeedleStatusRequest>,
    ) -> Result<Response<volume_server_pb::VolumeNeedleStatusResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let needle_id = NeedleId(req.needle_id);

        let store = self.state.store.read().unwrap();

        // Try normal volume first
        if let Some(_) = store.find_volume(vid) {
            let mut n = Needle {
                id: needle_id,
                ..Needle::default()
            };
            match store.read_volume_needle(vid, &mut n) {
                Ok(_) => {
                    return Ok(Response::new(
                        volume_server_pb::VolumeNeedleStatusResponse {
                            needle_id: n.id.0,
                            cookie: n.cookie.0,
                            size: n.data_size,
                            last_modified: n.last_modified,
                            crc: n.checksum.0,
                            ttl: String::new(),
                        },
                    ))
                }
                Err(_) => {
                    return Err(Status::not_found(format!(
                        "needle {} not found in volume {}",
                        needle_id, vid
                    )))
                }
            }
        }

        // Fall back to EC shards
        if let Some(ec_vol) = store.ec_volumes.get(&vid) {
            match ec_vol.find_needle_from_ecx(needle_id) {
                Ok(Some((offset, size))) if !size.is_deleted() && !offset.is_zero() => {
                    // Read the needle header from EC shards to get cookie
                    // The needle is at the actual offset in the reconstructed data
                    let actual_offset = offset.to_actual_offset();
                    let shard_size = ec_vol
                        .shards
                        .iter()
                        .filter_map(|s| s.as_ref())
                        .map(|s| s.file_size())
                        .next()
                        .unwrap_or(0) as u64;

                    if shard_size > 0 {
                        // Determine which shard and offset
                        let shard_id = (actual_offset as u64 / shard_size) as usize;
                        let shard_offset = actual_offset as u64 % shard_size;

                        if let Some(Some(shard)) = ec_vol.shards.get(shard_id) {
                            let mut header_buf = [0u8; NEEDLE_HEADER_SIZE];
                            if shard.read_at(&mut header_buf, shard_offset).is_ok() {
                                let (cookie, id, _) = Needle::parse_header(&header_buf);
                                return Ok(Response::new(
                                    volume_server_pb::VolumeNeedleStatusResponse {
                                        needle_id: id.0,
                                        cookie: cookie.0,
                                        size: size.0 as u32,
                                        last_modified: 0,
                                        crc: 0,
                                        ttl: String::new(),
                                    },
                                ));
                            }
                        }
                    }

                    // Fallback: return index info without cookie
                    return Ok(Response::new(
                        volume_server_pb::VolumeNeedleStatusResponse {
                            needle_id: needle_id.0,
                            cookie: 0,
                            size: size.0 as u32,
                            last_modified: 0,
                            crc: 0,
                            ttl: String::new(),
                        },
                    ));
                }
                _ => {
                    return Err(Status::not_found(format!(
                        "needle {} not found in ec volume {}",
                        needle_id, vid
                    )));
                }
            }
        }

        Err(Status::not_found(format!("volume not found {}", vid)))
    }

    async fn ping(
        &self,
        request: Request<volume_server_pb::PingRequest>,
    ) -> Result<Response<volume_server_pb::PingResponse>, Status> {
        let req = request.into_inner();
        let now_ns = || {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64
        };

        let start = now_ns();

        // Route ping based on target type
        let remote_time_ns = if req.target.is_empty() || req.target_type == "volumeServer" {
            // Volume self-ping: return our own time
            now_ns()
        } else if req.target_type == "master" {
            // Ping the master server
            match ping_grpc_target(&req.target).await {
                Ok(t) => t,
                Err(e) => {
                    return Err(Status::internal(format!(
                        "ping master {}: {}",
                        req.target, e
                    )))
                }
            }
        } else if req.target_type == "filer" {
            match ping_grpc_target(&req.target).await {
                Ok(t) => t,
                Err(e) => {
                    return Err(Status::internal(format!(
                        "ping filer {}: {}",
                        req.target, e
                    )))
                }
            }
        } else {
            // Unknown target type → return 0
            0
        };

        let stop = now_ns();
        Ok(Response::new(volume_server_pb::PingResponse {
            start_time_ns: start,
            remote_time_ns,
            stop_time_ns: stop,
        }))
    }
}

/// Ping a remote gRPC target and return its time_ns.
async fn ping_grpc_target(target: &str) -> Result<i64, String> {
    // For now, just verify the target is reachable by attempting a gRPC connection.
    // The Go implementation actually calls Ping on the target, but we simplify here.
    let addr = if target.starts_with("http") {
        target.to_string()
    } else {
        format!("http://{}", target)
    };
    match tonic::transport::Channel::from_shared(addr) {
        Ok(endpoint) => {
            match tokio::time::timeout(std::time::Duration::from_secs(5), endpoint.connect()).await
            {
                Ok(Ok(_channel)) => Ok(std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as i64),
                Ok(Err(e)) => Err(e.to_string()),
                Err(_) => Err("connection timeout".to_string()),
            }
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Parse a SeaweedFS server address ("ip:port.grpcPort" or "ip:port") into a gRPC address.
fn parse_grpc_address(source: &str) -> Result<String, String> {
    if let Some(colon_idx) = source.rfind(':') {
        let port_part = &source[colon_idx + 1..];
        if let Some(dot_idx) = port_part.rfind('.') {
            // Format: "ip:port.grpcPort"
            let host = &source[..colon_idx];
            let grpc_port = &port_part[dot_idx + 1..];
            grpc_port
                .parse::<u16>()
                .map_err(|e| format!("invalid grpc port: {}", e))?;
            return Ok(format!("{}:{}", host, grpc_port));
        }
        // Format: "ip:port" → grpc = port + 10000
        let port: u16 = port_part
            .parse()
            .map_err(|e| format!("invalid port: {}", e))?;
        let grpc_port = port as u32 + 10000;
        let host = &source[..colon_idx];
        return Ok(format!("{}:{}", host, grpc_port));
    }
    Err(format!("cannot parse address: {}", source))
}

/// Set the modification time of a file from nanoseconds since Unix epoch.
fn set_file_mtime(path: &str, modified_ts_ns: i64) {
    use std::time::{Duration, SystemTime};
    let ts = if modified_ts_ns >= 0 {
        SystemTime::UNIX_EPOCH + Duration::from_nanos(modified_ts_ns as u64)
    } else {
        SystemTime::UNIX_EPOCH
    };
    if let Ok(file) = std::fs::File::open(path) {
        let ft = std::fs::FileTimes::new().set_accessed(ts).set_modified(ts);
        let _ = file.set_times(ft);
    }
}

/// Copy a file from a remote volume server via CopyFile streaming RPC.
/// Returns the modified_ts_ns received from the source.
async fn copy_file_from_source(
    client: &mut volume_server_pb::volume_server_client::VolumeServerClient<
        tonic::transport::Channel,
    >,
    is_ec_volume: bool,
    collection: &str,
    volume_id: u32,
    compaction_revision: u32,
    stop_offset: u64,
    dest_path: &str,
    ext: &str,
    _is_append: bool,
    ignore_source_not_found: bool,
    progress_tx: Option<
        &tokio::sync::mpsc::Sender<Result<volume_server_pb::VolumeCopyResponse, Status>>,
    >,
    next_report_target: &mut i64,
    report_interval: i64,
) -> Result<i64, String> {
    let copy_req = volume_server_pb::CopyFileRequest {
        volume_id,
        ext: ext.to_string(),
        compaction_revision,
        stop_offset,
        collection: collection.to_string(),
        is_ec_volume,
        ignore_source_file_not_found: ignore_source_not_found,
    };

    let mut stream = client
        .copy_file(copy_req)
        .await
        .map_err(|e| {
            format!(
                "failed to start copying volume {} {} file: {}",
                volume_id, ext, e
            )
        })?
        .into_inner();

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dest_path)
        .map_err(|e| format!("open file {}: {}", dest_path, e))?;

    let mut progressed_bytes: i64 = 0;
    let mut modified_ts_ns: i64 = 0;

    while let Some(resp) = stream
        .message()
        .await
        .map_err(|e| format!("receiving {}: {}", dest_path, e))?
    {
        if resp.modified_ts_ns != 0 {
            modified_ts_ns = resp.modified_ts_ns;
        }
        if !resp.file_content.is_empty() {
            use std::io::Write;
            file.write_all(&resp.file_content)
                .map_err(|e| format!("write file {}: {}", dest_path, e))?;
            progressed_bytes += resp.file_content.len() as i64;

            if let Some(tx) = progress_tx {
                if progressed_bytes > *next_report_target {
                    let _ = tx
                        .send(Ok(volume_server_pb::VolumeCopyResponse {
                            last_append_at_ns: 0,
                            processed_bytes: progressed_bytes,
                        }))
                        .await;
                    *next_report_target = progressed_bytes + report_interval;
                }
            }
        }
    }

    // If source file didn't exist (no modifiedTsNs received), remove empty file
    if modified_ts_ns == 0 {
        let _ = std::fs::remove_file(dest_path);
    }

    Ok(modified_ts_ns)
}

/// Verify that a copied file has the expected size.
fn check_copy_file_size(path: &str, expected: u64) -> Result<(), Status> {
    match std::fs::metadata(path) {
        Ok(meta) => {
            if meta.len() != expected {
                Err(Status::internal(format!(
                    "file {} size [{}] is not same as origin file size [{}]",
                    path,
                    meta.len(),
                    expected
                )))
            } else {
                Ok(())
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && expected == 0 => Ok(()),
        Err(e) => Err(Status::internal(format!(
            "stat file {} failed: {}",
            path, e
        ))),
    }
}

/// Find the last append timestamp from copied .idx and .dat files.
fn find_last_append_at_ns(idx_path: &str, dat_path: &str) -> Option<u64> {
    use std::io::{Read, Seek, SeekFrom};

    let mut idx_file = std::fs::File::open(idx_path).ok()?;
    let idx_size = idx_file.metadata().ok()?.len();
    if idx_size == 0 || idx_size % (NEEDLE_MAP_ENTRY_SIZE as u64) != 0 {
        return None;
    }

    // Read the last index entry
    let mut buf = [0u8; NEEDLE_MAP_ENTRY_SIZE];
    idx_file
        .seek(SeekFrom::End(-(NEEDLE_MAP_ENTRY_SIZE as i64)))
        .ok()?;
    idx_file.read_exact(&mut buf).ok()?;

    let (_key, offset, _size) = idx_entry_from_bytes(&buf);
    if offset.is_zero() {
        return None;
    }

    // Read needle header from .dat to get the append timestamp
    let mut dat_file = std::fs::File::open(dat_path).ok()?;
    let actual_offset = offset.to_actual_offset();

    // Skip to the needle at the given offset, read header to get size
    dat_file.seek(SeekFrom::Start(actual_offset as u64)).ok()?;

    // Read cookie (4) + id (8) + size (4) = 16 bytes header
    let mut header = [0u8; 16];
    dat_file.read_exact(&mut header).ok()?;
    let needle_size = i32::from_be_bytes([header[12], header[13], header[14], header[15]]);
    if needle_size <= 0 {
        return None;
    }

    // Seek to tail: offset + 16 (header) + size -> checksum (4) + timestamp (8)
    let tail_offset = actual_offset as u64 + 16 + needle_size as u64;
    dat_file.seek(SeekFrom::Start(tail_offset)).ok()?;

    let mut tail = [0u8; 12]; // 4 bytes checksum + 8 bytes timestamp
    dat_file.read_exact(&mut tail).ok()?;

    // Timestamp is the last 8 bytes, big-endian
    let ts = u64::from_be_bytes([
        tail[4], tail[5], tail[6], tail[7], tail[8], tail[9], tail[10], tail[11],
    ]);
    if ts > 0 {
        Some(ts)
    } else {
        None
    }
}

/// Get disk usage (total, free) in bytes for the given path.
fn get_disk_usage(path: &str) -> (u64, u64) {
    use sysinfo::Disks;
    let disks = Disks::new_with_refreshed_list();
    let path = std::path::Path::new(path);
    // Find the disk that contains this path (longest mount point prefix match)
    let mut best: Option<&sysinfo::Disk> = None;
    let mut best_len = 0;
    for disk in disks.list() {
        let mount = disk.mount_point();
        if path.starts_with(mount) && mount.as_os_str().len() > best_len {
            best_len = mount.as_os_str().len();
            best = Some(disk);
        }
    }
    match best {
        Some(disk) => (disk.total_space(), disk.available_space()),
        None => (0, 0),
    }
}

/// Build memory status info similar to Go's stats.MemStat().
fn get_mem_status() -> volume_server_pb::MemStatus {
    let mut mem = volume_server_pb::MemStatus {
        goroutines: 1,
        ..Default::default()
    };

    #[cfg(target_os = "linux")]
    {
        if let Some((all, free)) = get_system_memory_linux() {
            mem.all = all;
            mem.free = free;
            mem.used = all.saturating_sub(free);
        }
        if let Some(rss) = get_process_rss_linux() {
            mem.self_ = rss;
            mem.heap = rss;
        }
    }

    mem
}

#[cfg(target_os = "linux")]
fn get_system_memory_linux() -> Option<(u64, u64)> {
    unsafe {
        let mut info: libc::sysinfo = std::mem::zeroed();
        if libc::sysinfo(&mut info) == 0 {
            let unit = info.mem_unit as u64;
            let total = info.totalram as u64 * unit;
            let free = info.freeram as u64 * unit;
            return Some((total, free));
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn get_process_rss_linux() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let mut parts = statm.split_whitespace();
    let _size = parts.next()?;
    let resident = parts.next()?.parse::<u64>().ok()?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as i64;
    if page_size <= 0 {
        return None;
    }
    Some(resident * page_size as u64)
}
