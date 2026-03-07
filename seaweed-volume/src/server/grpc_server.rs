//! gRPC service implementation for the volume server.
//!
//! Implements the VolumeServer trait generated from volume_server.proto.
//! 48 RPCs: core volume operations are fully implemented, streaming and
//! EC operations are stubbed with appropriate error messages.

use std::pin::Pin;
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
        let req = request.into_inner();
        let mut results = Vec::new();

        for fid_str in &req.file_ids {
            let result = match needle::FileId::parse(fid_str) {
                Ok(file_id) => {
                    let mut n = Needle {
                        id: file_id.key,
                        cookie: file_id.cookie,
                        ..Needle::default()
                    };
                    let mut store = self.state.store.write().unwrap();
                    match store.delete_volume_needle(file_id.volume_id, &mut n) {
                        Ok(size) => volume_server_pb::DeleteResult {
                            file_id: fid_str.clone(),
                            status: 0,
                            error: String::new(),
                            size: size.0 as u32,
                            version: 0,
                        },
                        Err(e) => volume_server_pb::DeleteResult {
                            file_id: fid_str.clone(),
                            status: 1,
                            error: e.to_string(),
                            size: 0,
                            version: 0,
                        },
                    }
                }
                Err(e) => volume_server_pb::DeleteResult {
                    file_id: fid_str.clone(),
                    status: 1,
                    error: e,
                    size: 0,
                    version: 0,
                },
            };
            results.push(result);
        }

        Ok(Response::new(volume_server_pb::BatchDeleteResponse { results }))
    }

    async fn vacuum_volume_check(
        &self,
        request: Request<volume_server_pb::VacuumVolumeCheckRequest>,
    ) -> Result<Response<volume_server_pb::VacuumVolumeCheckResponse>, Status> {
        let vid = VolumeId(request.into_inner().volume_id);
        let store = self.state.store.read().unwrap();
        let garbage_ratio = match store.find_volume(vid) {
            Some((_, vol)) => vol.garbage_level(),
            None => return Err(Status::not_found(format!("volume {} not found", vid))),
        };
        Ok(Response::new(volume_server_pb::VacuumVolumeCheckResponse { garbage_ratio }))
    }

    type VacuumVolumeCompactStream = BoxStream<volume_server_pb::VacuumVolumeCompactResponse>;
    async fn vacuum_volume_compact(
        &self,
        _request: Request<volume_server_pb::VacuumVolumeCompactRequest>,
    ) -> Result<Response<Self::VacuumVolumeCompactStream>, Status> {
        Err(Status::unimplemented("vacuum_volume_compact not yet implemented"))
    }

    async fn vacuum_volume_commit(
        &self,
        _request: Request<volume_server_pb::VacuumVolumeCommitRequest>,
    ) -> Result<Response<volume_server_pb::VacuumVolumeCommitResponse>, Status> {
        Err(Status::unimplemented("vacuum_volume_commit not yet implemented"))
    }

    async fn vacuum_volume_cleanup(
        &self,
        _request: Request<volume_server_pb::VacuumVolumeCleanupRequest>,
    ) -> Result<Response<volume_server_pb::VacuumVolumeCleanupResponse>, Status> {
        Err(Status::unimplemented("vacuum_volume_cleanup not yet implemented"))
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
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let rp = crate::storage::super_block::ReplicaPlacement::from_string(&req.replication)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let ttl = if req.ttl.is_empty() {
            None
        } else {
            Some(crate::storage::needle::ttl::TTL::read(&req.ttl)
                .map_err(|e| Status::invalid_argument(e))?)
        };
        let disk_type = DiskType::from_string(&req.disk_type);

        let mut store = self.state.store.write().unwrap();
        store.add_volume(vid, &req.collection, Some(rp), ttl, req.preallocate as u64, disk_type)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(volume_server_pb::AllocateVolumeResponse {}))
    }

    async fn volume_sync_status(
        &self,
        request: Request<volume_server_pb::VolumeSyncStatusRequest>,
    ) -> Result<Response<volume_server_pb::VolumeSyncStatusResponse>, Status> {
        let vid = VolumeId(request.into_inner().volume_id);
        let store = self.state.store.read().unwrap();
        let (_, vol) = store.find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("volume {} not found", vid)))?;

        Ok(Response::new(volume_server_pb::VolumeSyncStatusResponse {
            volume_id: vid.0,
            collection: vol.collection.clone(),
            replication: vol.super_block.replica_placement.to_string(),
            ttl: String::new(),
            tail_offset: vol.content_size(),
            compact_revision: vol.super_block.compaction_revision as u32,
            idx_file_size: 0,
            version: vol.version().0 as u32,
        }))
    }

    type VolumeIncrementalCopyStream = BoxStream<volume_server_pb::VolumeIncrementalCopyResponse>;
    async fn volume_incremental_copy(
        &self,
        _request: Request<volume_server_pb::VolumeIncrementalCopyRequest>,
    ) -> Result<Response<Self::VolumeIncrementalCopyStream>, Status> {
        Err(Status::unimplemented("volume_incremental_copy not yet implemented"))
    }

    async fn volume_mount(
        &self,
        request: Request<volume_server_pb::VolumeMountRequest>,
    ) -> Result<Response<volume_server_pb::VolumeMountResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        let mut store = self.state.store.write().unwrap();
        store.mount_volume(vid, "", DiskType::HardDrive)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(volume_server_pb::VolumeMountResponse {}))
    }

    async fn volume_unmount(
        &self,
        request: Request<volume_server_pb::VolumeUnmountRequest>,
    ) -> Result<Response<volume_server_pb::VolumeUnmountResponse>, Status> {
        let vid = VolumeId(request.into_inner().volume_id);
        let mut store = self.state.store.write().unwrap();
        if !store.unmount_volume(vid) {
            return Err(Status::not_found(format!("volume {} not found", vid)));
        }
        Ok(Response::new(volume_server_pb::VolumeUnmountResponse {}))
    }

    async fn volume_delete(
        &self,
        request: Request<volume_server_pb::VolumeDeleteRequest>,
    ) -> Result<Response<volume_server_pb::VolumeDeleteResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let mut store = self.state.store.write().unwrap();
        if req.only_empty {
            let (_, vol) = store.find_volume(vid)
                .ok_or_else(|| Status::not_found(format!("volume {} not found", vid)))?;
            if vol.file_count() > 0 {
                return Err(Status::failed_precondition("volume is not empty"));
            }
        }
        store.delete_volume(vid)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(volume_server_pb::VolumeDeleteResponse {}))
    }

    async fn volume_mark_readonly(
        &self,
        request: Request<volume_server_pb::VolumeMarkReadonlyRequest>,
    ) -> Result<Response<volume_server_pb::VolumeMarkReadonlyResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let mut store = self.state.store.write().unwrap();
        let (_, vol) = store.find_volume_mut(vid)
            .ok_or_else(|| Status::not_found(format!("volume {} not found", vid)))?;
        vol.set_read_only();
        Ok(Response::new(volume_server_pb::VolumeMarkReadonlyResponse {}))
    }

    async fn volume_mark_writable(
        &self,
        request: Request<volume_server_pb::VolumeMarkWritableRequest>,
    ) -> Result<Response<volume_server_pb::VolumeMarkWritableResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let mut store = self.state.store.write().unwrap();
        let (_, vol) = store.find_volume_mut(vid)
            .ok_or_else(|| Status::not_found(format!("volume {} not found", vid)))?;
        vol.set_writable();
        Ok(Response::new(volume_server_pb::VolumeMarkWritableResponse {}))
    }

    async fn volume_configure(
        &self,
        request: Request<volume_server_pb::VolumeConfigureRequest>,
    ) -> Result<Response<volume_server_pb::VolumeConfigureResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let rp = crate::storage::super_block::ReplicaPlacement::from_string(&req.replication)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let mut store = self.state.store.write().unwrap();
        let (_, vol) = store.find_volume_mut(vid)
            .ok_or_else(|| Status::not_found(format!("volume {} not found", vid)))?;

        match vol.set_replica_placement(rp) {
            Ok(()) => Ok(Response::new(volume_server_pb::VolumeConfigureResponse {
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(volume_server_pb::VolumeConfigureResponse {
                error: e.to_string(),
            })),
        }
    }

    async fn volume_status(
        &self,
        request: Request<volume_server_pb::VolumeStatusRequest>,
    ) -> Result<Response<volume_server_pb::VolumeStatusResponse>, Status> {
        let vid = VolumeId(request.into_inner().volume_id);
        let store = self.state.store.read().unwrap();
        let (_, vol) = store.find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("volume {} not found", vid)))?;

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
                maintenance: false,
                version: 0,
            }),
        }))
    }

    async fn set_state(
        &self,
        request: Request<volume_server_pb::SetStateRequest>,
    ) -> Result<Response<volume_server_pb::SetStateResponse>, Status> {
        // TODO: Persist state changes. Currently echoes back the request state.
        let req = request.into_inner();
        Ok(Response::new(volume_server_pb::SetStateResponse {
            state: req.state,
        }))
    }

    type VolumeCopyStream = BoxStream<volume_server_pb::VolumeCopyResponse>;
    async fn volume_copy(
        &self,
        _request: Request<volume_server_pb::VolumeCopyRequest>,
    ) -> Result<Response<Self::VolumeCopyStream>, Status> {
        Err(Status::unimplemented("volume_copy not yet implemented"))
    }

    async fn read_volume_file_status(
        &self,
        request: Request<volume_server_pb::ReadVolumeFileStatusRequest>,
    ) -> Result<Response<volume_server_pb::ReadVolumeFileStatusResponse>, Status> {
        let vid = VolumeId(request.into_inner().volume_id);
        let store = self.state.store.read().unwrap();
        let (_, vol) = store.find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("volume {} not found", vid)))?;

        Ok(Response::new(volume_server_pb::ReadVolumeFileStatusResponse {
            volume_id: vid.0,
            idx_file_timestamp_seconds: 0,
            idx_file_size: 0,
            dat_file_timestamp_seconds: 0,
            dat_file_size: vol.dat_file_size().unwrap_or(0),
            file_count: vol.file_count() as u64,
            compaction_revision: vol.super_block.compaction_revision as u32,
            collection: vol.collection.clone(),
            disk_type: String::new(),
            volume_info: None,
            version: vol.version().0 as u32,
        }))
    }

    type CopyFileStream = BoxStream<volume_server_pb::CopyFileResponse>;
    async fn copy_file(
        &self,
        _request: Request<volume_server_pb::CopyFileRequest>,
    ) -> Result<Response<Self::CopyFileStream>, Status> {
        Err(Status::unimplemented("copy_file not yet implemented"))
    }

    async fn receive_file(
        &self,
        _request: Request<Streaming<volume_server_pb::ReceiveFileRequest>>,
    ) -> Result<Response<volume_server_pb::ReceiveFileResponse>, Status> {
        Err(Status::unimplemented("receive_file not yet implemented"))
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
        let (_, vol) = store.find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("volume {} not found", vid)))?;

        let blob = vol.read_needle_blob(offset, size)
            .map_err(|e| Status::internal(e.to_string()))?;

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
        let mut n = Needle { id: needle_id, ..Needle::default() };
        match store.read_volume_needle(vid, &mut n) {
            Ok(_) => {
                let ttl_str = n.ttl.as_ref().map_or(String::new(), |t| t.to_string());
                Ok(Response::new(volume_server_pb::ReadNeedleMetaResponse {
                    cookie: n.cookie.0,
                    last_modified: n.last_modified,
                    crc: n.checksum.0,
                    ttl: ttl_str,
                    append_at_ns: n.append_at_ns,
                }))
            }
            Err(e) => Err(Status::not_found(e.to_string())),
        }
    }

    async fn write_needle_blob(
        &self,
        request: Request<volume_server_pb::WriteNeedleBlobRequest>,
    ) -> Result<Response<volume_server_pb::WriteNeedleBlobResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        let mut store = self.state.store.write().unwrap();
        let (_, vol) = store.find_volume_mut(vid)
            .ok_or_else(|| Status::not_found(format!("volume {} not found", vid)))?;

        // Write the raw needle blob at the end of the dat file (append)
        let dat_size = vol.dat_file_size()
            .map_err(|e| Status::internal(e.to_string()))? as i64;
        vol.write_needle_blob(dat_size, &req.needle_blob)
            .map_err(|e| Status::internal(e.to_string()))?;

        // Update the needle index so the written blob is discoverable
        let needle_id = NeedleId(req.needle_id);
        let size = Size(req.size);
        vol.put_needle_index(needle_id, Offset::from_actual_offset(dat_size), size)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(volume_server_pb::WriteNeedleBlobResponse {}))
    }

    type ReadAllNeedlesStream = BoxStream<volume_server_pb::ReadAllNeedlesResponse>;
    async fn read_all_needles(
        &self,
        _request: Request<volume_server_pb::ReadAllNeedlesRequest>,
    ) -> Result<Response<Self::ReadAllNeedlesStream>, Status> {
        Err(Status::unimplemented("read_all_needles not yet implemented"))
    }

    type VolumeTailSenderStream = BoxStream<volume_server_pb::VolumeTailSenderResponse>;
    async fn volume_tail_sender(
        &self,
        _request: Request<volume_server_pb::VolumeTailSenderRequest>,
    ) -> Result<Response<Self::VolumeTailSenderStream>, Status> {
        Err(Status::unimplemented("volume_tail_sender not yet implemented"))
    }

    async fn volume_tail_receiver(
        &self,
        _request: Request<volume_server_pb::VolumeTailReceiverRequest>,
    ) -> Result<Response<volume_server_pb::VolumeTailReceiverResponse>, Status> {
        Err(Status::unimplemented("volume_tail_receiver not yet implemented"))
    }

    // ---- EC operations ----

    async fn volume_ec_shards_generate(
        &self,
        request: Request<volume_server_pb::VolumeEcShardsGenerateRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsGenerateResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let collection = &req.collection;

        // Find the volume's directory
        let dir = {
            let store = self.state.store.read().unwrap();
            let (loc_idx, _) = store.find_volume(vid)
                .ok_or_else(|| Status::not_found(format!("volume {} not found", vid)))?;
            store.locations[loc_idx].directory.clone()
        };

        crate::storage::erasure_coding::ec_encoder::write_ec_files(&dir, collection, vid)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(volume_server_pb::VolumeEcShardsGenerateResponse {}))
    }

    async fn volume_ec_shards_rebuild(
        &self,
        _request: Request<volume_server_pb::VolumeEcShardsRebuildRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsRebuildResponse>, Status> {
        Err(Status::unimplemented("volume_ec_shards_rebuild not yet implemented"))
    }

    async fn volume_ec_shards_copy(
        &self,
        _request: Request<volume_server_pb::VolumeEcShardsCopyRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsCopyResponse>, Status> {
        Err(Status::unimplemented("volume_ec_shards_copy not yet implemented"))
    }

    async fn volume_ec_shards_delete(
        &self,
        _request: Request<volume_server_pb::VolumeEcShardsDeleteRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsDeleteResponse>, Status> {
        Err(Status::unimplemented("volume_ec_shards_delete not yet implemented"))
    }

    async fn volume_ec_shards_mount(
        &self,
        _request: Request<volume_server_pb::VolumeEcShardsMountRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsMountResponse>, Status> {
        Err(Status::unimplemented("volume_ec_shards_mount not yet implemented"))
    }

    async fn volume_ec_shards_unmount(
        &self,
        _request: Request<volume_server_pb::VolumeEcShardsUnmountRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsUnmountResponse>, Status> {
        Err(Status::unimplemented("volume_ec_shards_unmount not yet implemented"))
    }

    type VolumeEcShardReadStream = BoxStream<volume_server_pb::VolumeEcShardReadResponse>;
    async fn volume_ec_shard_read(
        &self,
        _request: Request<volume_server_pb::VolumeEcShardReadRequest>,
    ) -> Result<Response<Self::VolumeEcShardReadStream>, Status> {
        Err(Status::unimplemented("volume_ec_shard_read not yet implemented"))
    }

    async fn volume_ec_blob_delete(
        &self,
        _request: Request<volume_server_pb::VolumeEcBlobDeleteRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcBlobDeleteResponse>, Status> {
        Err(Status::unimplemented("volume_ec_blob_delete not yet implemented"))
    }

    async fn volume_ec_shards_to_volume(
        &self,
        _request: Request<volume_server_pb::VolumeEcShardsToVolumeRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsToVolumeResponse>, Status> {
        Err(Status::unimplemented("volume_ec_shards_to_volume not yet implemented"))
    }

    async fn volume_ec_shards_info(
        &self,
        _request: Request<volume_server_pb::VolumeEcShardsInfoRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsInfoResponse>, Status> {
        Err(Status::unimplemented("volume_ec_shards_info not yet implemented"))
    }

    // ---- Tiered storage ----

    type VolumeTierMoveDatToRemoteStream = BoxStream<volume_server_pb::VolumeTierMoveDatToRemoteResponse>;
    async fn volume_tier_move_dat_to_remote(
        &self,
        _request: Request<volume_server_pb::VolumeTierMoveDatToRemoteRequest>,
    ) -> Result<Response<Self::VolumeTierMoveDatToRemoteStream>, Status> {
        Err(Status::unimplemented("volume_tier_move_dat_to_remote not yet implemented"))
    }

    type VolumeTierMoveDatFromRemoteStream = BoxStream<volume_server_pb::VolumeTierMoveDatFromRemoteResponse>;
    async fn volume_tier_move_dat_from_remote(
        &self,
        _request: Request<volume_server_pb::VolumeTierMoveDatFromRemoteRequest>,
    ) -> Result<Response<Self::VolumeTierMoveDatFromRemoteStream>, Status> {
        Err(Status::unimplemented("volume_tier_move_dat_from_remote not yet implemented"))
    }

    // ---- Server management ----

    async fn volume_server_status(
        &self,
        _request: Request<volume_server_pb::VolumeServerStatusRequest>,
    ) -> Result<Response<volume_server_pb::VolumeServerStatusResponse>, Status> {
        let store = self.state.store.read().unwrap();

        let mut disk_statuses = Vec::new();
        for loc in &store.locations {
            let free = loc.available_space.load(std::sync::atomic::Ordering::Relaxed);
            // TODO: DiskLocation does not yet track total disk size.
            // Once implemented, compute all/used/percent from real values.
            disk_statuses.push(volume_server_pb::DiskStatus {
                dir: loc.directory.clone(),
                all: 0,
                used: 0,
                free,
                percent_free: 0.0,
                percent_used: 0.0,
                disk_type: loc.disk_type.to_string(),
            });
        }

        Ok(Response::new(volume_server_pb::VolumeServerStatusResponse {
            disk_statuses,
            memory_status: None,
            version: env!("CARGO_PKG_VERSION").to_string(),
            data_center: String::new(),
            rack: String::new(),
            state: Some(volume_server_pb::VolumeServerState {
                maintenance: false,
                version: 0,
            }),
        }))
    }

    async fn volume_server_leave(
        &self,
        _request: Request<volume_server_pb::VolumeServerLeaveRequest>,
    ) -> Result<Response<volume_server_pb::VolumeServerLeaveResponse>, Status> {
        *self.state.is_stopping.write().unwrap() = true;
        Ok(Response::new(volume_server_pb::VolumeServerLeaveResponse {}))
    }

    async fn fetch_and_write_needle(
        &self,
        _request: Request<volume_server_pb::FetchAndWriteNeedleRequest>,
    ) -> Result<Response<volume_server_pb::FetchAndWriteNeedleResponse>, Status> {
        Err(Status::unimplemented("fetch_and_write_needle not yet implemented"))
    }

    async fn scrub_volume(
        &self,
        _request: Request<volume_server_pb::ScrubVolumeRequest>,
    ) -> Result<Response<volume_server_pb::ScrubVolumeResponse>, Status> {
        Err(Status::unimplemented("scrub_volume not yet implemented"))
    }

    async fn scrub_ec_volume(
        &self,
        _request: Request<volume_server_pb::ScrubEcVolumeRequest>,
    ) -> Result<Response<volume_server_pb::ScrubEcVolumeResponse>, Status> {
        Err(Status::unimplemented("scrub_ec_volume not yet implemented"))
    }

    type QueryStream = BoxStream<volume_server_pb::QueriedStripe>;
    async fn query(
        &self,
        _request: Request<volume_server_pb::QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        Err(Status::unimplemented("query not yet implemented"))
    }

    async fn volume_needle_status(
        &self,
        request: Request<volume_server_pb::VolumeNeedleStatusRequest>,
    ) -> Result<Response<volume_server_pb::VolumeNeedleStatusResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let needle_id = NeedleId(req.needle_id);

        let store = self.state.store.read().unwrap();
        let mut n = Needle { id: needle_id, ..Needle::default() };
        match store.read_volume_needle(vid, &mut n) {
            Ok(_) => Ok(Response::new(volume_server_pb::VolumeNeedleStatusResponse {
                needle_id: needle_id.0,
                cookie: n.cookie.0,
                size: n.data_size,
                last_modified: n.last_modified,
                crc: n.checksum.0,
                ttl: String::new(),
            })),
            Err(e) => Err(Status::not_found(e.to_string())),
        }
    }

    async fn ping(
        &self,
        _request: Request<volume_server_pb::PingRequest>,
    ) -> Result<Response<volume_server_pb::PingResponse>, Status> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        Ok(Response::new(volume_server_pb::PingResponse {
            start_time_ns: now,
            remote_time_ns: now,
            stop_time_ns: now,
        }))
    }
}
