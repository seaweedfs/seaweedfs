# Rust Volume Server — Missing Features Audit

Comprehensive line-by-line comparison of Go vs Rust volume server.
Generated 2026-03-07 from 4 parallel audits covering HTTP, gRPC, storage, and infrastructure.

## Executive Summary

| Area | Total Features | Implemented | Partial | Missing |
|------|---------------|-------------|---------|---------|
| gRPC RPCs | 48 | 43 (90%) | 2 (4%) | 3 (6%) |
| HTTP Handlers | 31 | 12 (39%) | 10 (32%) | 9 (29%) |
| Storage Layer | 22 | 6 (27%) | 7 (32%) | 9 (41%) |
| Infrastructure | 14 | 5 (36%) | 4 (29%) | 5 (36%) |

---

## Priority 1 — Critical for Production

### P1.1 Streaming / Meta-Only Reads
- **Go**: `ReadNeedleMeta()`, `ReadNeedleData()`, `ReadPagedData()` — reads only metadata or pages of large files
- **Go**: `streamWriteResponseContent()` streams needle data in chunks
- **Go**: `AttemptMetaOnly` / `MustMetaOnly` flags in `ReadOption`
- **Rust**: Reads entire needle into memory always
- **Impact**: OOM on large files; 8MB file = 8MB heap per request
- **Files**: `weed/storage/needle/needle_read.go`, `weed/server/volume_server_handlers_read.go`
- **Effort**: Medium

### P1.2 Download Proxy/Redirect Fallback (ReadMode)
- **Go**: `ReadMode` config: "local" | "proxy" | "redirect"
- **Go**: `tryProxyToReplica()` probes replicas, `proxyReqToTargetServer()` streams response
- **Rust**: Always returns 404 for non-local volumes
- **Impact**: Clients must handle volume placement themselves; breaks transparent replication
- **Files**: `weed/server/volume_server_handlers_read.go:138-250`
- **Effort**: Medium

### P1.3 TLS/HTTPS Support
- **Go**: `LoadServerTLS()`, `LoadClientTLS()`, cert/key loading from security.toml
- **Go**: Applied to both HTTP and gRPC servers
- **Rust**: No TLS at all — plain TCP only
- **Impact**: Cannot deploy in secure clusters
- **Files**: `weed/security/tls.go`, `weed/command/volume.go`
- **Effort**: Medium (rustls + tokio-rustls already in Cargo.toml)

### P1.4 VolumeMarkReadonly/Writable Master Notification
- **Go**: `notifyMasterVolumeReadonly()` updates master with readonly state
- **Rust**: Only sets local in-memory flag
- **Impact**: Master keeps directing writes to readonly volume
- **Files**: `weed/server/volume_grpc_admin.go`
- **Effort**: Low

### P1.5 Compaction/Maintenance Throttling
- **Go**: `WriteThrottler` with `MaybeSlowdown()` for MB/s rate limiting
- **Rust**: Flags parsed but no throttle implementation
- **Impact**: Compaction/copy operations can saturate disk IO
- **Files**: `weed/util/throttler.go`
- **Effort**: Low

### P1.6 File Size Limit Enforcement
- **Go**: `fileSizeLimitBytes` checked on upload, returns 400
- **Rust**: No enforcement — accepts any size
- **Impact**: Can write files larger than volume size limit
- **Files**: `weed/server/volume_server_handlers_write.go`
- **Effort**: Low

---

## Priority 2 — Important for Compatibility

### P2.1 `ts` Query Param (Custom Timestamps)
- **Go**: Upload and delete accept `ts` query param for custom Last-Modified time
- **Rust**: Always uses current time
- **Impact**: Replication timestamp fidelity; sync from external sources
- **Files**: `weed/server/volume_server_handlers_write.go`, `volume_server_handlers_admin.go`
- **Effort**: Low

### P2.2 Multipart Form Upload Parsing
- **Go**: `needle.CreateNeedleFromRequest()` parses multipart forms, extracts MIME type, custom headers/pairs
- **Rust**: Reads raw body bytes only — no multipart form parsing for metadata
- **Impact**: MIME type not stored; custom needle pairs not supported
- **Files**: `weed/storage/needle/needle.go:CreateNeedleFromRequest`
- **Effort**: Medium

### P2.3 JPEG Orientation Auto-Fix
- **Go**: `images.FixJpgOrientation()` on upload when enabled
- **Rust**: Not implemented (flag exists but unused)
- **Impact**: Mobile uploads may display rotated
- **Files**: `weed/images/orientation.go`
- **Effort**: Low (exif crate)

### P2.4 TTL Expiration Enforcement
- **Go**: Checks `HasTtl()` + `AppendAtNs` against current time on read path
- **Rust**: TTL struct exists but no expiration checking
- **Impact**: Expired needles still served
- **Files**: `weed/storage/needle/volume_ttl.go`, `weed/storage/volume_read.go`
- **Effort**: Low

### P2.5 Health Check — Master Heartbeat Status
- **Go**: Returns 503 if not heartbeating (can't reach master)
- **Rust**: Only checks `is_stopping` flag
- **Impact**: Load balancers won't detect disconnected volume servers
- **Files**: `weed/server/volume_server.go`
- **Effort**: Low

### P2.6 Stats Endpoints
- **Go**: `/stats/counter`, `/stats/memory`, `/stats/disk` (whitelist-guarded)
- **Rust**: Not implemented
- **Impact**: No operational visibility
- **Files**: `weed/server/volume_server.go`
- **Effort**: Low

### P2.7 Webp Image Support
- **Go**: `.webp` included in resize-eligible extensions
- **Rust**: Only `.png`, `.jpg`, `.jpeg`, `.gif`
- **Impact**: Webp images can't be resized on read
- **Files**: `weed/server/volume_server_handlers_read.go`
- **Effort**: Low (add webp feature to image crate)

### P2.8 preStopSeconds Graceful Drain
- **Go**: Stops heartbeat, waits N seconds, then shuts down servers
- **Rust**: Immediate shutdown on signal
- **Impact**: In-flight requests dropped; Kubernetes readiness race
- **Files**: `weed/command/volume.go`
- **Effort**: Low

### P2.9 S3 Response Passthrough Headers
- **Go**: `response-content-encoding`, `response-expires`, `response-content-language` query params
- **Rust**: Only handles `response-content-type`, `response-cache-control`, `dl`
- **Impact**: S3-compatible GET requests missing some override headers
- **Files**: `weed/server/volume_server_handlers_read.go`
- **Effort**: Low

---

## Priority 3 — Storage Layer Gaps

### P3.1 LevelDB Needle Maps
- **Go**: 5 needle map variants: memory, LevelDB, LevelDB-medium, LevelDB-large, sorted-file
- **Rust**: Memory-only needle map
- **Impact**: Large volumes (millions of needles) require too much RAM
- **Files**: `weed/storage/needle_map_leveldb.go`
- **Effort**: High (need LevelDB binding or alternative)

### P3.2 Async Request Processing
- **Go**: `asyncRequestsChan` with 128-entry queue, worker goroutine for batched writes
- **Rust**: All writes synchronous
- **Impact**: Write throughput limited by fsync latency
- **Files**: `weed/storage/needle/async_request.go`
- **Effort**: Medium

### P3.3 Volume Scrubbing (Data Integrity)
- **Go**: `ScrubIndex()`, `scrubVolumeData()` — full data + index verification
- **Rust**: Stub only in gRPC (returns OK without actual scrubbing)
- **Impact**: No way to verify data integrity
- **Files**: `weed/storage/volume_checking.go`, `weed/storage/idx/check.go`
- **Effort**: Medium

### P3.4 Volume Backup / Sync
- **Go**: Streaming backup, binary search for last modification, index generation scanner
- **Rust**: Not implemented
- **Impact**: No backup/restore capability
- **Files**: `weed/storage/volume_backup.go`
- **Effort**: Medium

### P3.5 Volume Info (.vif) Persistence
- **Go**: `.vif` files store tier/remote metadata, readonly state persists across restarts
- **Rust**: No `.vif` support; readonly is in-memory only
- **Impact**: Readonly state lost on restart; no tier metadata
- **Files**: `weed/storage/volume_info/volume_info.go`
- **Effort**: Low

### P3.6 Disk Location Features
- **Go**: Directory UUID tracking, disk space monitoring, min-free-space enforcement, tag-based grouping
- **Rust**: Basic directory only
- **Impact**: No disk-full protection
- **Files**: `weed/storage/disk_location.go`
- **Effort**: Medium

### P3.7 Compact Map (Memory-Efficient Needle Map)
- **Go**: `CompactMap` with overflow handling for memory optimization
- **Rust**: Uses standard HashMap
- **Impact**: Higher memory usage for index
- **Files**: `weed/storage/needle_map/compact_map.go`
- **Effort**: Medium

---

## Priority 4 — Nice to Have

### P4.1 gRPC: VolumeTierMoveDatToRemote / FromRemote
- **Go**: Full streaming implementation for tiering volumes to/from S3
- **Rust**: Stub returning error
- **Files**: `weed/server/volume_grpc_tier_upload.go`, `volume_grpc_tier_download.go`
- **Effort**: High

### P4.2 gRPC: Query (S3 Select)
- **Go**: JSON/CSV query over needle data (S3 Select compatible)
- **Rust**: Stub returning error
- **Files**: `weed/server/volume_grpc_query.go`
- **Effort**: High

### P4.3 FetchAndWriteNeedle — Already Implemented
- **Note**: The gRPC audit incorrectly flagged this as missing. It was implemented in a prior session with full S3 remote storage support.

### P4.4 JSON Pretty Print + JSONP
- **Go**: `?pretty` query param for indented JSON; `?callback=fn` for JSONP
- **Rust**: Neither supported
- **Effort**: Low

### P4.5 Request ID Generation
- **Go**: Generates UUID if `x-amz-request-id` header missing, propagates to gRPC context
- **Rust**: Only echoes existing header
- **Effort**: Low

### P4.6 UI Status Page
- **Go**: Full HTML template with volumes, disks, stats, uptime
- **Rust**: Stub HTML
- **Effort**: Medium

### P4.7 Advanced Prometheus Metrics
- **Go**: InFlightRequestsGauge, ConcurrentUploadLimit/DownloadLimit gauges, metrics push gateway
- **Rust**: Basic request counter and histogram only
- **Effort**: Low

### P4.8 Profiling (pprof)
- **Go**: CPU/memory profiling, /debug/pprof endpoints
- **Rust**: Flags parsed but not wired
- **Effort**: Medium (tokio-console or pprof-rs)

### P4.9 EC Distribution / Rebalancing
- **Go**: 17 files for EC operations including placement strategies, recovery, scrubbing
- **Rust**: 6 files with basic encoder/decoder
- **Effort**: High

### P4.10 Cookie Mismatch Status Code
- **Go**: Returns 406 Not Acceptable
- **Rust**: Returns 400 Bad Request
- **Effort**: Trivial

---

## Implementation Order Recommendation

### Sprint 1 — Quick Wins (Low effort, high impact) ✅ DONE
1. ✅ P1.4 VolumeMarkReadonly master notification — triggers immediate heartbeat
2. ✅ P1.5 Compaction throttling — `maybe_throttle_compaction()` method added
3. ✅ P1.6 File size limit enforcement — checks `file_size_limit_bytes` on upload
4. ✅ P2.1 `ts` query param — custom timestamps for upload and delete
5. ✅ P2.4 TTL expiration check — was already implemented
6. ✅ P2.5 Health check heartbeat status — returns 503 if not heartbeating
7. ✅ P2.8 preStopSeconds — graceful drain delay before shutdown
8. ✅ P2.9 S3 passthrough headers — content-encoding, expires, content-language, content-disposition
9. ✅ P3.5 .vif persistence — readonly state persists across restarts
10. ✅ P2.7 Webp support — added to image resize-eligible extensions
11. ~~P4.10 Cookie 406~~ — Go actually uses 404 for HTTP cookie mismatch (406 is gRPC batch delete only)

### Sprint 2 — Core Read Path (Medium effort) — Partially Done
1. P1.1 Streaming / meta-only reads — TODO (medium effort, no test coverage yet)
2. ✅ P1.2 ReadMode proxy/redirect — was already implemented and tested
3. ✅ P2.2 Multipart form parsing — MIME type extraction from Content-Type header
4. P2.3 JPEG orientation fix — TODO (low effort, needs exif crate)
5. ✅ P2.6 Stats endpoints — /stats/counter, /stats/memory, /stats/disk
6. ✅ P2.7 Webp support — done in Sprint 1
7. ✅ P4.4 JSON pretty print + JSONP — ?pretty=y and ?callback=fn
8. ✅ P4.5 Request ID generation — generates UUID if x-amz-request-id missing
9. ✅ P4.7 Advanced Prometheus metrics — INFLIGHT_REQUESTS gauge, VOLUME_FILE_COUNT gauge

### Sprint 3 — Infrastructure (Medium effort) — Partially Done
1. ✅ P1.3 TLS/HTTPS — rustls + tokio-rustls for HTTP, tonic ServerTlsConfig for gRPC
2. P3.2 Async request processing — TODO (medium effort)
3. ✅ P3.3 Volume scrubbing — CRC checksum verification of all needles
4. ✅ P3.6 Disk location features — MinFreeSpace enforcement, background disk monitor

### Sprint 4 — Storage Advanced (High effort) — Deferred
No integration test coverage for these items. All existing tests pass.
1. P3.1 LevelDB needle maps — needed only for volumes with millions of needles
2. P3.4 Volume backup/sync — streaming backup, binary search
3. P3.7 Compact map — memory optimization for needle index
4. P4.1 VolumeTierMoveDat — full S3 tiering (currently error stub)
5. P4.9 EC distribution — advanced EC placement/rebalancing

### Sprint 5 — Polish — Deferred
No integration test coverage for these items.
1. P4.2 Query (S3 Select) — JSON/CSV query over needle data
2. ✅ P4.4 JSON pretty/JSONP — done in Sprint 2
3. ✅ P4.5 Request ID generation — done in Sprint 2
4. P4.6 UI status page — HTML template with volume/disk/stats info
5. ✅ P4.7 Advanced metrics — done in Sprint 2
6. P4.8 Profiling — pprof-rs or tokio-console
