# Rust Volume Server — Dev Plan

## Current Status (2026-03-07)

**HTTP tests**: 54/55 pass (98.2%) — 1 unfixable: CONNECT method is a hyper/axum limitation
**gRPC tests**: 74/75 pass (98.7%) — 1 Go-only: TestVolumeMoveHandlesInFlightWrites uses Go binaries exclusively
**S3 remote storage tests**: 3/3 pass (new)
**Total**: 131/133 (98.5%) + 3 S3 tests
**Rust unit tests**: 112 lib + 7 integration = 119

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

## Remaining Work (Production Readiness)

### Medium Priority (nice to have)

1. **VolumeTierMoveDatToRemote/FromRemote** — Move volume data to/from remote storage
   backends (S3, etc.). Currently returns error paths only. Would need to implement
   full dat file upload/download to S3.

2. **BatchDelete EC shards** — BatchDelete currently only handles regular volumes.
   Go also checks EC volumes and calls DeleteEcShardNeedle.

3. **Streaming / meta-only reads** — Go reads large files in pages/streams.
   Rust reads entire needle into memory. OOM risk for large files.

4. **TLS/HTTPS** — rustls + tokio-rustls for both HTTP and gRPC.

5. **JPEG orientation fix** — Auto-fix EXIF orientation on upload.

6. **Async request processing** — Batched writes with 128-entry queue.

### Low Priority

7. **TestUnsupportedMethodConnectParity** — HTTP CONNECT method returns 400 in Go but
   hyper rejects it before reaching the router. Would need a custom hyper service wrapper.

8. **LevelDB needle maps** — For volumes with millions of needles.

9. **Volume backup/sync** — Streaming backup, binary search.

10. **EC distribution/rebalancing** — Advanced EC operations.

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
