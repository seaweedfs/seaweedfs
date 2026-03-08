# Rust Volume Server — Dev Plan

## Current Status (2026-03-07)

**HTTP tests**: 53/53 pass (100%)
**gRPC tests**: 56/56 pass (100%) — includes TestVolumeMoveHandlesInFlightWrites with Rust multi-volume cluster
**Rust integration tests**: 8/8 pass
**S3 remote storage tests**: 3/3 pass
**Total**: 117/117 (100%) + 8 Rust + 3 S3 tests
**Rust unit tests**: 126 lib + 7 integration = 133

## Completed Features

All phases from the original plan are complete:

- **Phase 1** — HTTP Core: CORS, OPTIONS, unsupported methods, static assets, path routing,
  cookie validation, conditional headers, range requests, dedup 204, content-encoding,
  readDeleted, chunk manifests, multipart validation, MD5 check, file size limit,
  upload/download throttling, image resize/crop, download disposition
- **Phase 2** — JWT/Security: signing keys from security.toml, token source precedence
  (query > header > cookie), file_id claims, leeway=0
- **Phase 3** — gRPC: maintenance mode, error message parity, ping routing, batch delete,
  VolumeServerStatus (with real disk stats, data_center, rack), ReadVolumeFileStatus
  (with timestamps)
- **Phase 4** — Streaming gRPC: VolumeIncrementalCopy, CopyFile, ReceiveFile, ReadAllNeedles,
  VolumeTailSender, VolumeCopy, VolumeTailReceiver, VacuumVolumeCheck
- **Phase 5** — EC Shards: mount/unmount, delete, read, blob delete, rebuild, shards-to-volume,
  copy, info
- **Phase 6** — Advanced gRPC: ScrubVolume, ScrubEcVolume, Query, FetchAndWriteNeedle,
  VolumeTierMoveDat (error paths)
- **Phase 7** — Remote Storage: S3-compatible backend via aws-sdk-s3,
  FetchAndWriteNeedle reads from S3/MinIO/SeaweedFS S3 and writes locally.
  Supports all S3-compatible providers (AWS, Wasabi, Backblaze, Aliyun, etc.)
- **Master Heartbeat** — Bidirectional streaming SendHeartbeat RPC, volume/EC registration,
  leader changes, shutdown deregistration. Tested end-to-end with Go master.
- **Production Sprint 1** — Quick wins:
  - VolumeMarkReadonly master notification (triggers immediate heartbeat)
  - Compaction throttling (`maybe_throttle_compaction()`)
  - File size limit enforcement on upload
  - `ts` query param for custom timestamps (upload + delete)
  - TTL expiration check (was already implemented)
  - Health check heartbeat status (returns 503 if disconnected from master)
  - preStopSeconds graceful drain before shutdown
  - S3 response passthrough headers (content-encoding, expires, content-language, content-disposition)
  - .vif persistence for readonly state across restarts
  - Webp image support for resize
- **Production Sprint 2** — Compatibility:
  - MIME type extraction from Content-Type header
  - Stats endpoints (/stats/counter, /stats/memory, /stats/disk)
  - JSON pretty print (?pretty=y) and JSONP (?callback=fn)
  - Request ID generation (UUID if x-amz-request-id missing)
  - Advanced Prometheus metrics (INFLIGHT_REQUESTS, VOLUME_FILE_COUNT gauges)
- **Production Sprint 3** — Streaming & Multi-node:
  - Streaming reads for large files (>1MB) via http_body::Body trait with spawn_blocking
  - Meta-only needle reads (NeedleStreamInfo) to avoid loading full body for streaming
  - Multi-volume Rust cluster support (RustMultiVolumeCluster test framework)
  - TestVolumeMoveHandlesInFlightWrites now uses Rust volume servers
  - CI skip list cleaned up (all tests pass with Rust)

- **Production Sprint 4** — Advanced Features:
  - BatchDelete EC shard support (ecx index lookup + ecj journal deletion)
  - JPEG EXIF orientation auto-fix on upload (kamadak-exif + image crate)
  - Async batched write processing (mpsc queue, up to 128 entries per batch)
  - VolumeTierMoveDatToRemote/FromRemote (S3 multipart upload/download)
  - S3TierRegistry for managing remote storage backends
  - VolumeInfo (.vif) persistence for remote file references
- **Production Sprint 5** — Upload Compatibility:
  - TTL query parameter extraction during upload (`ttl=3m`)
  - Auto-compression for compressible file types (text/*, .js, .css, .json, .svg, etc.)
  - Seaweed-* custom metadata headers stored as needle pairs (JSON, max 64KB)
  - Filename extraction from URL path stored in needle name field
  - Upload response includes filename

## Remaining Work (Production Readiness)

### Medium Priority (nice to have)

1. **LevelDB needle maps** — For volumes with millions of needles.

2. **Volume backup/sync** — Streaming backup, binary search.

3. **EC distribution/rebalancing** — Advanced EC operations.

## Test Commands

```bash
# Build
cd seaweed-volume && cargo build --release

# Run all Go integration tests with Rust volume server
VOLUME_SERVER_IMPL=rust go test -v -count=1 -timeout 1200s ./test/volume_server/grpc/... ./test/volume_server/http/...

# Run S3 remote storage tests
VOLUME_SERVER_IMPL=rust go test -v -count=1 -timeout 180s -run "TestFetchAndWriteNeedle(FromS3|S3NotFound)" ./test/volume_server/grpc/...

# Run specific test
VOLUME_SERVER_IMPL=rust go test -v -count=1 -timeout 60s -run "TestName" ./test/volume_server/http/...

# Run Rust unit tests
cd seaweed-volume && cargo test

# Test heartbeat with Go master
weed master -port=9333 &
seaweed-volume --port 8080 --master localhost:9333 --dir /tmp/vol1 --max 7
curl http://localhost:9333/dir/status  # should show Rust volume server registered
```
