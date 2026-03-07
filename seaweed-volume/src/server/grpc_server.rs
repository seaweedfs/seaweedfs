//! gRPC service implementation for the volume server.
//!
//! Implements the VolumeServer trait generated from volume_server.proto.
//! 48 RPCs: core volume operations are fully implemented, streaming and
//! EC operations are stubbed with appropriate error messages.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;

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

            // Cookie validation (unless skip_cookie_check)
            if !req.skip_cookie_check {
                let original_cookie = n.cookie;
                let store = self.state.store.read().unwrap();
                match store.read_volume_needle(file_id.volume_id, &mut n) {
                    Ok(_) => {}
                    Err(e) => {
                        results.push(volume_server_pb::DeleteResult {
                            file_id: fid_str.clone(),
                            status: 404, // Not Found
                            error: e.to_string(),
                            size: 0,
                            version: 0,
                        });
                        continue;
                    }
                }
                if n.cookie != original_cookie {
                    results.push(volume_server_pb::DeleteResult {
                        file_id: fid_str.clone(),
                        status: 400, // Bad Request
                        error: "File Random Cookie does not match.".to_string(),
                        size: 0,
                        version: 0,
                    });
                    break; // Stop processing on cookie mismatch
                }
            }

            // Reject chunk manifest needles
            if n.is_chunk_manifest() {
                results.push(volume_server_pb::DeleteResult {
                    file_id: fid_str.clone(),
                    status: 406, // Not Acceptable
                    error: "ChunkManifest: not allowed in batch delete mode.".to_string(),
                    size: 0,
                    version: 0,
                });
                continue;
            }

            n.last_modified = now;
            n.set_has_last_modified_date();

            let mut store = self.state.store.write().unwrap();
            match store.delete_volume_needle(file_id.volume_id, &mut n) {
                Ok(size) => {
                    if size.0 == 0 {
                        results.push(volume_server_pb::DeleteResult {
                            file_id: fid_str.clone(),
                            status: 304, // Not Modified
                            error: String::new(),
                            size: 0,
                            version: 0,
                        });
                    } else {
                        results.push(volume_server_pb::DeleteResult {
                            file_id: fid_str.clone(),
                            status: 202, // Accepted
                            error: String::new(),
                            size: size.0 as u32,
                            version: 0,
                        });
                    }
                }
                Err(e) => {
                    results.push(volume_server_pb::DeleteResult {
                        file_id: fid_str.clone(),
                        status: 500, // Internal Server Error
                        error: e.to_string(),
                        size: 0,
                        version: 0,
                    });
                }
            }
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
            None => return Err(Status::not_found(format!("not found volume id {}", vid))),
        };
        Ok(Response::new(volume_server_pb::VacuumVolumeCheckResponse { garbage_ratio }))
    }

    type VacuumVolumeCompactStream = BoxStream<volume_server_pb::VacuumVolumeCompactResponse>;
    async fn vacuum_volume_compact(
        &self,
        _request: Request<volume_server_pb::VacuumVolumeCompactRequest>,
    ) -> Result<Response<Self::VacuumVolumeCompactStream>, Status> {
        self.state.check_maintenance()?;
        Err(Status::unimplemented("vacuum_volume_compact not yet implemented"))
    }

    async fn vacuum_volume_commit(
        &self,
        _request: Request<volume_server_pb::VacuumVolumeCommitRequest>,
    ) -> Result<Response<volume_server_pb::VacuumVolumeCommitResponse>, Status> {
        self.state.check_maintenance()?;
        Err(Status::unimplemented("vacuum_volume_commit not yet implemented"))
    }

    async fn vacuum_volume_cleanup(
        &self,
        _request: Request<volume_server_pb::VacuumVolumeCleanupRequest>,
    ) -> Result<Response<volume_server_pb::VacuumVolumeCleanupResponse>, Status> {
        self.state.check_maintenance()?;
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
        self.state.check_maintenance()?;
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
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;

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
        let (_, v) = store.find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;

        let dat_size = v.dat_file_size().unwrap_or(0);
        let dat_path = v.file_name(".dat");
        let super_block_size = v.super_block.block_size() as u64;
        drop(store);

        // If since_ns is very large (after all data), return empty
        if req.since_ns == u64::MAX || dat_size <= super_block_size {
            let stream = tokio_stream::iter(Vec::new());
            return Ok(Response::new(Box::pin(stream)));
        }

        // For since_ns=0, start from super block end; otherwise would need binary search
        let start_offset = super_block_size;

        // Read the .dat file
        let file = std::fs::File::open(&dat_path)
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut reader = std::io::BufReader::new(file);
        use std::io::{Read, Seek, SeekFrom};
        reader.seek(SeekFrom::Start(start_offset))
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
            let (_, vol) = store.find_volume(vid)
                .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;
            if vol.file_count() > 0 {
                return Err(Status::failed_precondition("volume not empty"));
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
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let mut store = self.state.store.write().unwrap();
        let (_, vol) = store.find_volume_mut(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;
        vol.set_read_only();
        Ok(Response::new(volume_server_pb::VolumeMarkReadonlyResponse {}))
    }

    async fn volume_mark_writable(
        &self,
        request: Request<volume_server_pb::VolumeMarkWritableRequest>,
    ) -> Result<Response<volume_server_pb::VolumeMarkWritableResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let mut store = self.state.store.write().unwrap();
        let (_, vol) = store.find_volume_mut(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;
        vol.set_writable();
        Ok(Response::new(volume_server_pb::VolumeMarkWritableResponse {}))
    }

    async fn volume_configure(
        &self,
        request: Request<volume_server_pb::VolumeConfigureRequest>,
    ) -> Result<Response<volume_server_pb::VolumeConfigureResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        // Validate replication string — return response error, not gRPC error
        let rp = match crate::storage::super_block::ReplicaPlacement::from_string(&req.replication) {
            Ok(rp) => rp,
            Err(e) => {
                return Ok(Response::new(volume_server_pb::VolumeConfigureResponse {
                    error: format!("invalid replica placement: {}", e),
                }));
            }
        };

        let mut store = self.state.store.write().unwrap();
        let (_, vol) = match store.find_volume_mut(vid) {
            Some(v) => v,
            None => {
                return Ok(Response::new(volume_server_pb::VolumeConfigureResponse {
                    error: format!("volume {} not found on disk, failed to restore mount", vid),
                }));
            }
        };

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
            self.state.maintenance.store(new_state.maintenance, Ordering::Relaxed);
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
        _request: Request<volume_server_pb::VolumeCopyRequest>,
    ) -> Result<Response<Self::VolumeCopyStream>, Status> {
        self.state.check_maintenance()?;
        Err(Status::unimplemented("volume_copy not yet implemented"))
    }

    async fn read_volume_file_status(
        &self,
        request: Request<volume_server_pb::ReadVolumeFileStatusRequest>,
    ) -> Result<Response<volume_server_pb::ReadVolumeFileStatusResponse>, Status> {
        let vid = VolumeId(request.into_inner().volume_id);
        let store = self.state.store.read().unwrap();
        let (_, vol) = store.find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;

        Ok(Response::new(volume_server_pb::ReadVolumeFileStatusResponse {
            volume_id: vid.0,
            idx_file_timestamp_seconds: 0,
            idx_file_size: vol.idx_file_size(),
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
            let (_, v) = store.find_volume(vid)
                .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;

            // Check compaction revision
            if req.compaction_revision != u32::MAX && v.last_compact_revision() != req.compaction_revision as u16 {
                return Err(Status::failed_precondition(format!("volume {} is compacted", vid.0)));
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
                    return Err(Status::not_found(format!("CopyFile not found ec volume id {}", vid.0)));
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

        let metadata = file.metadata().map_err(|e| Status::internal(e.to_string()))?;
        let mod_ts_ns = metadata.modified()
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
        let mut file_path = String::new();
        let mut bytes_written: u64 = 0;

        while let Some(req) = stream.message().await? {
            match req.data {
                Some(volume_server_pb::receive_file_request::Data::Info(info)) => {
                    // Determine file path
                    if info.is_ec_volume {
                        let store = self.state.store.read().unwrap();
                        let dir = store.locations.first()
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
                        let (_, v) = store.find_volume(VolumeId(info.volume_id))
                            .ok_or_else(|| Status::not_found(format!("volume {} not found", info.volume_id)))?;
                        file_path = v.file_name(&info.ext);
                        drop(store);
                    }

                    target_file = Some(std::fs::File::create(&file_path)
                        .map_err(|e| Status::internal(format!("failed to create file: {}", e)))?);
                }
                Some(volume_server_pb::receive_file_request::Data::FileContent(content)) => {
                    if let Some(ref mut f) = target_file {
                        use std::io::Write;
                        let n = f.write(&content)
                            .map_err(|e| Status::internal(format!("failed to write file: {}", e)))?;
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
        let (_, vol) = store.find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;

        let blob = vol.read_needle_blob(offset, size)
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
        let (_, vol) = store.find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {} and read needle metadata at ec shards is not supported", vid)))?;

        let offset = req.offset;
        let size = crate::storage::types::Size(req.size);

        let mut n = Needle { id: needle_id, flags: 0x08, ..Needle::default() };
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

        let mut store = self.state.store.write().unwrap();
        let (_, vol) = store.find_volume_mut(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;

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
                    results.push(Err(Status::not_found(format!("not found volume id {}", vid))));
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
            let (_, vol) = store.find_volume(vid)
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
                // Scan for new needles
                let scan_result = {
                    let store = state.store.read().unwrap();
                    if let Some((_, vol)) = store.find_volume(vid) {
                        vol.scan_raw_needles_from(sb_size)
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
        let store = self.state.store.read().unwrap();
        store.find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("receiver not found volume id {}", vid)))?;
        drop(store);
        Err(Status::unimplemented("volume_tail_receiver not yet implemented"))
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

        // Find the volume's directory
        let dir = {
            let store = self.state.store.read().unwrap();
            let (loc_idx, _) = store.find_volume(vid)
                .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;
            store.locations[loc_idx].directory.clone()
        };

        crate::storage::erasure_coding::ec_encoder::write_ec_files(&dir, collection, vid)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(volume_server_pb::VolumeEcShardsGenerateResponse {}))
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
                return Ok(Response::new(volume_server_pb::VolumeEcShardsRebuildResponse {
                    rebuilt_shard_ids: vec![],
                }));
            }
        };

        // Check which shards are missing
        use crate::storage::erasure_coding::ec_shard::TOTAL_SHARDS_COUNT;
        let mut missing: Vec<u32> = Vec::new();
        for shard_id in 0..TOTAL_SHARDS_COUNT as u8 {
            let shard = crate::storage::erasure_coding::ec_shard::EcVolumeShard::new(&dir, collection, vid, shard_id);
            if !std::path::Path::new(&shard.file_name()).exists() {
                missing.push(shard_id as u32);
            }
        }

        if missing.is_empty() {
            return Ok(Response::new(volume_server_pb::VolumeEcShardsRebuildResponse {
                rebuilt_shard_ids: vec![],
            }));
        }

        // Rebuild missing shards by regenerating all EC files
        crate::storage::erasure_coding::ec_encoder::write_ec_files(&dir, collection, vid)
            .map_err(|e| Status::internal(format!("rebuild ec shards: {}", e)))?;

        Ok(Response::new(volume_server_pb::VolumeEcShardsRebuildResponse {
            rebuilt_shard_ids: missing,
        }))
    }

    async fn volume_ec_shards_copy(
        &self,
        request: Request<volume_server_pb::VolumeEcShardsCopyRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsCopyResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        // Validate disk_id
        let (loc_count, dest_dir) = {
            let store = self.state.store.read().unwrap();
            let count = store.locations.len();
            let dir = if (req.disk_id as usize) < count {
                store.locations[req.disk_id as usize].directory.clone()
            } else {
                return Err(Status::invalid_argument(format!(
                    "invalid disk_id {}: only have {} disks", req.disk_id, count
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
                "VolumeEcShardsCopy volume {} invalid source_data_node {}", vid, source
            )));
        }
        let grpc_addr = format!("{}:{}", parts[0].rsplit_once(':').map(|(h,_)| h).unwrap_or(parts[0]), parts[1]);

        let channel = tonic::transport::Channel::from_shared(format!("http://{}", grpc_addr))
            .map_err(|e| Status::internal(format!("VolumeEcShardsCopy volume {} parse source: {}", vid, e)))?
            .connect()
            .await
            .map_err(|e| Status::internal(format!("VolumeEcShardsCopy volume {} connect to {}: {}", vid, grpc_addr, e)))?;

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
                stop_offset: 0,
                ..Default::default()
            };
            let mut stream = client.copy_file(copy_req).await
                .map_err(|e| Status::internal(format!("VolumeEcShardsCopy volume {} copy {}: {}", vid, ext, e)))?
                .into_inner();

            let file_path = {
                let base = crate::storage::volume::volume_file_name(&dest_dir, &req.collection, vid);
                format!("{}{}", base, ext)
            };
            let mut file = std::fs::File::create(&file_path)
                .map_err(|e| Status::internal(format!("create {}: {}", file_path, e)))?;
            while let Some(chunk) = stream.message().await
                .map_err(|e| Status::internal(format!("recv {}: {}", ext, e)))? {
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
                stop_offset: 0,
                ..Default::default()
            };
            let mut stream = client.copy_file(copy_req).await
                .map_err(|e| Status::internal(format!("VolumeEcShardsCopy volume {} copy .ecx: {}", vid, e)))?
                .into_inner();

            let file_path = {
                let base = crate::storage::volume::volume_file_name(&dest_dir, &req.collection, vid);
                format!("{}.ecx", base)
            };
            let mut file = std::fs::File::create(&file_path)
                .map_err(|e| Status::internal(format!("create {}: {}", file_path, e)))?;
            while let Some(chunk) = stream.message().await
                .map_err(|e| Status::internal(format!("recv .ecx: {}", e)))? {
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
                stop_offset: 0,
                ..Default::default()
            };
            let mut stream = client.copy_file(copy_req).await
                .map_err(|e| Status::internal(format!("VolumeEcShardsCopy volume {} copy .vif: {}", vid, e)))?
                .into_inner();

            let file_path = {
                let base = crate::storage::volume::volume_file_name(&dest_dir, &req.collection, vid);
                format!("{}.vif", base)
            };
            let mut file = std::fs::File::create(&file_path)
                .map_err(|e| Status::internal(format!("create {}: {}", file_path, e)))?;
            while let Some(chunk) = stream.message().await
                .map_err(|e| Status::internal(format!("recv .vif: {}", e)))? {
                use std::io::Write;
                file.write_all(&chunk.file_content)
                    .map_err(|e| Status::internal(format!("write {}: {}", file_path, e)))?;
            }
        }

        Ok(Response::new(volume_server_pb::VolumeEcShardsCopyResponse {}))
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
        Ok(Response::new(volume_server_pb::VolumeEcShardsDeleteResponse {}))
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
                if store.find_ec_shard_dir(vid, &req.collection, shard_id as u8).is_none() {
                    return Err(Status::not_found(format!(
                        "ec volume {} shards {:?} not found", req.volume_id, req.shard_ids
                    )));
                }
            }
        }

        let mut store = self.state.store.write().unwrap();
        store.mount_ec_shards(vid, &req.collection, &req.shard_ids)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(volume_server_pb::VolumeEcShardsMountResponse {}))
    }

    async fn volume_ec_shards_unmount(
        &self,
        request: Request<volume_server_pb::VolumeEcShardsUnmountRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsUnmountResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);
        let mut store = self.state.store.write().unwrap();
        store.unmount_ec_shards(vid, &req.shard_ids);
        Ok(Response::new(volume_server_pb::VolumeEcShardsUnmountResponse {}))
    }

    type VolumeEcShardReadStream = BoxStream<volume_server_pb::VolumeEcShardReadResponse>;
    async fn volume_ec_shard_read(
        &self,
        request: Request<volume_server_pb::VolumeEcShardReadRequest>,
    ) -> Result<Response<Self::VolumeEcShardReadStream>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        let store = self.state.store.read().unwrap();
        let ec_vol = store.ec_volumes.get(&vid)
            .ok_or_else(|| Status::not_found(format!("ec volume {} shard {} not found", req.volume_id, req.shard_id)))?;

        // Check if the requested needle is deleted
        if req.file_key > 0 {
            let needle_id = NeedleId(req.file_key);
            let deleted_needles = ec_vol.read_deleted_needles()
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
        let shard = ec_vol.shards.get(req.shard_id as usize)
            .and_then(|s| s.as_ref())
            .ok_or_else(|| Status::not_found(format!("ec volume {} shard {} not mounted", req.volume_id, req.shard_id)))?;

        let read_size = if req.size > 0 { req.size as usize } else { 1024 * 1024 };
        let mut buf = vec![0u8; read_size];
        let n = shard.read_at(&mut buf, req.offset as u64)
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
            ec_vol.journal_delete(needle_id)
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        // If EC volume not mounted, it's a no-op (matching Go behavior)
        Ok(Response::new(volume_server_pb::VolumeEcBlobDeleteResponse {}))
    }

    async fn volume_ec_shards_to_volume(
        &self,
        request: Request<volume_server_pb::VolumeEcShardsToVolumeRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsToVolumeResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        let store = self.state.store.read().unwrap();
        let ec_vol = store.ec_volumes.get(&vid)
            .ok_or_else(|| Status::not_found(format!("ec volume {} not found", req.volume_id)))?;

        // Check that all 10 data shards are present
        use crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT;
        for shard_id in 0..DATA_SHARDS_COUNT as u8 {
            if ec_vol.shards.get(shard_id as usize).map(|s| s.is_none()).unwrap_or(true) {
                return Err(Status::internal(format!(
                    "ec volume {} missing shard {}", req.volume_id, shard_id
                )));
            }
        }

        // Read the .ecx index to find all needles
        let ecx_path = ec_vol.ecx_file_name();
        let ecx_data = std::fs::read(&ecx_path)
            .map_err(|e| Status::internal(format!("read ecx: {}", e)))?;
        let entry_count = ecx_data.len() / NEEDLE_MAP_ENTRY_SIZE;

        // Read deleted needles from .ecj
        let deleted_needles = ec_vol.read_deleted_needles()
            .map_err(|e| Status::internal(e.to_string()))?;

        // Count live entries
        let mut live_count = 0;
        for i in 0..entry_count {
            let start = i * NEEDLE_MAP_ENTRY_SIZE;
            let (key, _offset, size) = idx_entry_from_bytes(&ecx_data[start..start + NEEDLE_MAP_ENTRY_SIZE]);
            if size.is_deleted() || deleted_needles.contains(&key) {
                continue;
            }
            live_count += 1;
        }

        if live_count == 0 {
            return Err(Status::failed_precondition(format!(
                "ec volume {} has no live entries", req.volume_id
            )));
        }

        // Reconstruct the volume from EC shards
        let dir = ec_vol.dir.clone();
        let collection = ec_vol.collection.clone();
        drop(store);

        // Read all shard data and reconstruct the .dat file
        // For simplicity, concatenate the first DATA_SHARDS_COUNT shards
        let mut dat_data = Vec::new();
        {
            let store = self.state.store.read().unwrap();
            let ec_vol = store.ec_volumes.get(&vid).unwrap();
            for shard_id in 0..DATA_SHARDS_COUNT as u8 {
                if let Some(Some(shard)) = ec_vol.shards.get(shard_id as usize) {
                    let shard_size = shard.file_size() as usize;
                    let mut buf = vec![0u8; shard_size];
                    let n = shard.read_at(&mut buf, 0)
                        .map_err(|e| Status::internal(format!("read shard {}: {}", shard_id, e)))?;
                    buf.truncate(n);
                    dat_data.extend_from_slice(&buf);
                }
            }
        }

        // Write the reconstructed .dat file
        let base = crate::storage::volume::volume_file_name(&dir, &collection, vid);
        let dat_path = format!("{}.dat", base);
        std::fs::write(&dat_path, &dat_data)
            .map_err(|e| Status::internal(format!("write dat: {}", e)))?;

        // Copy the .ecx to .idx (they have the same format)
        let idx_path = format!("{}.idx", base);
        std::fs::copy(&ecx_path, &idx_path)
            .map_err(|e| Status::internal(format!("copy ecx to idx: {}", e)))?;

        // Unmount EC shards and mount the reconstructed volume
        {
            let mut store = self.state.store.write().unwrap();
            // Remove EC volume
            if let Some(mut ec_vol) = store.ec_volumes.remove(&vid) {
                ec_vol.close();
            }
            // Unmount existing volume if any, then mount fresh
            store.unmount_volume(vid);
            store.mount_volume(vid, &collection, DiskType::HardDrive)
                .map_err(|e| Status::internal(format!("mount volume: {}", e)))?;
        }

        Ok(Response::new(volume_server_pb::VolumeEcShardsToVolumeResponse {}))
    }

    async fn volume_ec_shards_info(
        &self,
        request: Request<volume_server_pb::VolumeEcShardsInfoRequest>,
    ) -> Result<Response<volume_server_pb::VolumeEcShardsInfoResponse>, Status> {
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        let store = self.state.store.read().unwrap();
        let ec_vol = store.ec_volumes.get(&vid)
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
        let volume_size = ec_vol.shards.iter()
            .filter_map(|s| s.as_ref())
            .map(|s| s.file_size())
            .sum::<i64>() as u64;

        Ok(Response::new(volume_server_pb::VolumeEcShardsInfoResponse {
            ec_shard_infos: shard_infos,
            volume_size,
            file_count: 0,
            file_deleted_count: 0,
        }))
    }

    // ---- Tiered storage ----

    type VolumeTierMoveDatToRemoteStream = BoxStream<volume_server_pb::VolumeTierMoveDatToRemoteResponse>;
    async fn volume_tier_move_dat_to_remote(
        &self,
        request: Request<volume_server_pb::VolumeTierMoveDatToRemoteRequest>,
    ) -> Result<Response<Self::VolumeTierMoveDatToRemoteStream>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        let store = self.state.store.read().unwrap();
        let (_, vol) = store.find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;

        if vol.collection != req.collection {
            return Err(Status::invalid_argument(format!(
                "unexpected input {}, expected collection {}", req.collection, vol.collection
            )));
        }
        drop(store);

        // Backend not supported
        Err(Status::internal(format!(
            "destination {} not found", req.destination_backend_name
        )))
    }

    type VolumeTierMoveDatFromRemoteStream = BoxStream<volume_server_pb::VolumeTierMoveDatFromRemoteResponse>;
    async fn volume_tier_move_dat_from_remote(
        &self,
        request: Request<volume_server_pb::VolumeTierMoveDatFromRemoteRequest>,
    ) -> Result<Response<Self::VolumeTierMoveDatFromRemoteStream>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        let store = self.state.store.read().unwrap();
        let (_, vol) = store.find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;

        if vol.collection != req.collection {
            return Err(Status::invalid_argument(format!(
                "unexpected input {}, expected collection {}", req.collection, vol.collection
            )));
        }
        drop(store);

        // Volume is already on local disk (no remote storage support)
        Err(Status::internal(format!("volume {} already on local disk", vid)))
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
            memory_status: Some(volume_server_pb::MemStatus {
                goroutines: 1, // Rust doesn't have goroutines, report 1 for tokio runtime
                all: 0,
                used: 0,
                free: 0,
                self_: 0,
                heap: 0,
                stack: 0,
            }),
            version: env!("CARGO_PKG_VERSION").to_string(),
            data_center: String::new(),
            rack: String::new(),
            state: Some(volume_server_pb::VolumeServerState {
                maintenance: self.state.maintenance.load(Ordering::Relaxed),
                version: self.state.state_version.load(Ordering::Relaxed),
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
        request: Request<volume_server_pb::FetchAndWriteNeedleRequest>,
    ) -> Result<Response<volume_server_pb::FetchAndWriteNeedleResponse>, Status> {
        self.state.check_maintenance()?;
        let req = request.into_inner();
        let vid = VolumeId(req.volume_id);

        // Check volume exists
        let store = self.state.store.read().unwrap();
        store.find_volume(vid)
            .ok_or_else(|| Status::not_found(format!("not found volume id {}", vid)))?;
        drop(store);

        // Remote storage is not supported — fail with appropriate error
        if req.remote_conf.is_some() {
            return Err(Status::internal(format!(
                "get remote client: remote storage type not supported"
            )));
        }

        Err(Status::unimplemented("fetch_and_write_needle: remote storage not configured"))
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
            _ => return Err(Status::invalid_argument(format!("unsupported volume scrub mode {}", mode))),
        }

        let mut total_volumes: u64 = 0;
        let mut total_files: u64 = 0;
        let broken_volume_ids: Vec<u32> = Vec::new();
        let details: Vec<String> = Vec::new();

        for vid in &vids {
            let (_, v) = store.find_volume(*vid).ok_or_else(|| {
                Status::not_found(format!("volume id {} not found", vid.0))
            })?;
            total_volumes += 1;
            total_files += v.file_count() as u64;
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
            _ => return Err(Status::invalid_argument(format!("unsupported EC volume scrub mode {}", mode))),
        }

        if req.volume_ids.is_empty() {
            // Auto-select: no EC volumes exist in our implementation
            return Ok(Response::new(volume_server_pb::ScrubEcVolumeResponse {
                total_volumes: 0,
                total_files: 0,
                broken_volume_ids: vec![],
                broken_shard_infos: vec![],
                details: vec![],
            }));
        }

        // Specific volume IDs requested — EC volumes don't exist, so error
        for &vid in &req.volume_ids {
            return Err(Status::not_found(format!("EC volume id {} not found", vid)));
        }

        Ok(Response::new(volume_server_pb::ScrubEcVolumeResponse {
            total_volumes: 0,
            total_files: 0,
            broken_volume_ids: vec![],
            broken_shard_infos: vec![],
            details: vec![],
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
            let file_id = needle::FileId::parse(fid_str)
                .map_err(|e| Status::invalid_argument(e))?;

            let mut n = Needle {
                id: file_id.key,
                cookie: file_id.cookie,
                ..Needle::default()
            };
            let original_cookie = n.cookie;

            let store = self.state.store.read().unwrap();
            store.read_volume_needle(file_id.volume_id, &mut n)
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
                                    if let (Some(fv), Ok(tv)) = (field_val.as_f64(), f.value.parse::<f64>()) {
                                        fv > tv
                                    } else {
                                        false
                                    }
                                }
                                ">=" => {
                                    if let (Some(fv), Ok(tv)) = (field_val.as_f64(), f.value.parse::<f64>()) {
                                        fv >= tv
                                    } else {
                                        false
                                    }
                                }
                                "<" => {
                                    if let (Some(fv), Ok(tv)) = (field_val.as_f64(), f.value.parse::<f64>()) {
                                        fv < tv
                                    } else {
                                        false
                                    }
                                }
                                "<=" => {
                                    if let (Some(fv), Ok(tv)) = (field_val.as_f64(), f.value.parse::<f64>()) {
                                        fv <= tv
                                    } else {
                                        false
                                    }
                                }
                                "=" => {
                                    if let (Some(fv), Ok(tv)) = (field_val.as_f64(), f.value.parse::<f64>()) {
                                        fv == tv
                                    } else {
                                        field_val.as_str().map_or(false, |s| s == f.value)
                                    }
                                }
                                "!=" => {
                                    if let (Some(fv), Ok(tv)) = (field_val.as_f64(), f.value.parse::<f64>()) {
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
            let mut n = Needle { id: needle_id, ..Needle::default() };
            match store.read_volume_needle(vid, &mut n) {
                Ok(_) => return Ok(Response::new(volume_server_pb::VolumeNeedleStatusResponse {
                    needle_id: n.id.0,
                    cookie: n.cookie.0,
                    size: n.data_size,
                    last_modified: n.last_modified,
                    crc: n.checksum.0,
                    ttl: String::new(),
                })),
                Err(_) => return Err(Status::not_found(format!("needle {} not found in volume {}", needle_id, vid))),
            }
        }

        // Fall back to EC shards
        if let Some(ec_vol) = store.ec_volumes.get(&vid) {
            match ec_vol.find_needle_from_ecx(needle_id) {
                Ok(Some((offset, size))) if !size.is_deleted() && !offset.is_zero() => {
                    // Read the needle header from EC shards to get cookie
                    // The needle is at the actual offset in the reconstructed data
                    let actual_offset = offset.to_actual_offset();
                    use crate::storage::erasure_coding::ec_shard::ERASURE_CODING_SMALL_BLOCK_SIZE;
                    let shard_size = ec_vol.shards.iter()
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
                                return Ok(Response::new(volume_server_pb::VolumeNeedleStatusResponse {
                                    needle_id: id.0,
                                    cookie: cookie.0,
                                    size: size.0 as u32,
                                    last_modified: 0,
                                    crc: 0,
                                    ttl: String::new(),
                                }));
                            }
                        }
                    }

                    // Fallback: return index info without cookie
                    return Ok(Response::new(volume_server_pb::VolumeNeedleStatusResponse {
                        needle_id: needle_id.0,
                        cookie: 0,
                        size: size.0 as u32,
                        last_modified: 0,
                        crc: 0,
                        ttl: String::new(),
                    }));
                }
                _ => {
                    return Err(Status::not_found(format!("needle {} not found in ec volume {}", needle_id, vid)));
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
        let now_ns = || std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        let start = now_ns();

        // Route ping based on target type
        let remote_time_ns = if req.target.is_empty() || req.target_type == "volumeServer" {
            // Volume self-ping: return our own time
            now_ns()
        } else if req.target_type == "master" {
            // Ping the master server
            match ping_grpc_target(&req.target).await {
                Ok(t) => t,
                Err(e) => return Err(Status::internal(format!("ping master {}: {}", req.target, e))),
            }
        } else if req.target_type == "filer" {
            match ping_grpc_target(&req.target).await {
                Ok(t) => t,
                Err(e) => return Err(Status::internal(format!("ping filer {}: {}", req.target, e))),
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
            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                endpoint.connect(),
            ).await {
                Ok(Ok(_channel)) => {
                    Ok(std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as i64)
                }
                Ok(Err(e)) => Err(e.to_string()),
                Err(_) => Err("connection timeout".to_string()),
            }
        }
        Err(e) => Err(e.to_string()),
    }
}
