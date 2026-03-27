# Execution Plan: SeaweedFS Volume Server — Go to Rust Port

## Scope Summary

| Component | Go Source | Lines (non-test) | Description |
|---|---|---|---|
| CLI & startup | `weed/command/volume.go` | 476 | ~40 CLI flags, server bootstrap |
| HTTP server + handlers | `weed/server/volume_server*.go` | 1,517 | Struct, routes, read/write/delete handlers |
| gRPC handlers | `weed/server/volume_grpc_*.go` | 3,073 | 40 RPC method implementations |
| Storage engine | `weed/storage/` | 15,271 | Volumes, needles, index, compaction, EC, backend |
| Protobuf definitions | `weed/pb/volume_server.proto` | 759 | Service + message definitions |
| Shared utilities | `weed/security/`, `weed/stats/`, `weed/util/` | ~2,000+ | JWT, TLS, metrics, helpers |
| **Total** | | **~23,000+** | |

## Rust Crate & Dependency Strategy

```
seaweed-volume/
├── Cargo.toml
├── build.rs                    # protobuf codegen
├── proto/
│   ├── volume_server.proto     # copied from Go, adapted
│   └── remote.proto
├── src/
│   ├── main.rs                 # CLI entry point
│   ├── config.rs               # CLI flags + config
│   ├── server/
│   │   ├── mod.rs
│   │   ├── volume_server.rs    # VolumeServer struct + lifecycle
│   │   ├── http_handlers.rs    # HTTP route dispatch
│   │   ├── http_read.rs        # GET/HEAD handlers
│   │   ├── http_write.rs       # POST/PUT handlers
│   │   ├── http_delete.rs      # DELETE handler
│   │   ├── http_admin.rs       # /status, /healthz, /ui
│   │   ├── grpc_service.rs     # gRPC trait impl dispatch
│   │   ├── grpc_vacuum.rs
│   │   ├── grpc_copy.rs
│   │   ├── grpc_erasure_coding.rs
│   │   ├── grpc_tail.rs
│   │   ├── grpc_admin.rs
│   │   ├── grpc_read_write.rs
│   │   ├── grpc_batch_delete.rs
│   │   ├── grpc_scrub.rs
│   │   ├── grpc_tier.rs
│   │   ├── grpc_remote.rs
│   │   ├── grpc_query.rs
│   │   ├── grpc_state.rs
│   │   └── grpc_client_to_master.rs  # heartbeat
│   ├── storage/
│   │   ├── mod.rs
│   │   ├── store.rs            # Store (multi-disk manager)
│   │   ├── volume.rs           # Volume struct + lifecycle
│   │   ├── volume_read.rs
│   │   ├── volume_write.rs
│   │   ├── volume_compact.rs
│   │   ├── volume_info.rs
│   │   ├── needle/
│   │   │   ├── mod.rs
│   │   │   ├── needle.rs       # Needle struct + serialization
│   │   │   ├── needle_read.rs
│   │   │   ├── needle_write.rs
│   │   │   ├── needle_map.rs   # in-memory NeedleMap
│   │   │   ├── needle_value.rs
│   │   │   └── crc.rs
│   │   ├── super_block.rs
│   │   ├── idx/
│   │   │   ├── mod.rs
│   │   │   └── idx.rs          # .idx file format read/write
│   │   ├── needle_map_leveldb.rs
│   │   ├── types.rs            # NeedleId, Offset, Size, DiskType
│   │   ├── disk_location.rs    # DiskLocation per-directory
│   │   ├── erasure_coding/
│   │   │   ├── mod.rs
│   │   │   ├── ec_volume.rs
│   │   │   ├── ec_shard.rs
│   │   │   ├── ec_encoder.rs   # Reed-Solomon encoding
│   │   │   └── ec_decoder.rs
│   │   └── backend/
│   │       ├── mod.rs
│   │       ├── disk.rs
│   │       └── s3_backend.rs   # tiered storage to S3
│   ├── topology/
│   │   └── volume_layout.rs    # replication placement
│   ├── security/
│   │   ├── mod.rs
│   │   ├── guard.rs            # whitelist + JWT gate
│   │   ├── jwt.rs
│   │   └── tls.rs
│   ├── stats/
│   │   ├── mod.rs
│   │   └── metrics.rs          # Prometheus counters/gauges
│   └── util/
│       ├── mod.rs
│       ├── grpc.rs
│       ├── http.rs
│       └── file.rs
└── tests/
    ├── integration/
    │   ├── http_read_test.rs
    │   ├── http_write_test.rs
    │   ├── grpc_test.rs
    │   └── storage_test.rs
    └── unit/
        ├── needle_test.rs
        ├── idx_test.rs
        ├── super_block_test.rs
        └── ec_test.rs
```

### Key Rust dependencies

| Purpose | Crate |
|---|---|
| Async runtime | `tokio` |
| gRPC | `tonic` + `prost` |
| HTTP server | `hyper` + `axum` |
| CLI parsing | `clap` (derive) |
| Prometheus metrics | `prometheus` |
| JWT | `jsonwebtoken` |
| TLS | `rustls` + `tokio-rustls` |
| LevelDB | `rusty-leveldb` or `rocksdb` |
| Reed-Solomon EC | `reed-solomon-erasure` |
| Logging | `tracing` + `tracing-subscriber` |
| Config (security.toml) | `toml` + `serde` |
| CRC32 | `crc32fast` |
| Memory-mapped files | `memmap2` |

---

## Phased Execution Plan

### Phase 1: Project Skeleton & Protobuf Codegen
**Goal:** Cargo project compiles, proto codegen works, CLI parses all flags.

**Steps:**

1.1. Create `seaweed-volume/Cargo.toml` with all dependencies listed above.

1.2. Copy `volume_server.proto` and `remote.proto` into `proto/`. Adjust package paths for Rust codegen.

1.3. Create `build.rs` using `tonic-build` to compile `.proto` files into Rust types.

1.4. Create `src/main.rs` with `clap` derive structs mirroring all 40 CLI flags from `weed/command/volume.go`:
   - `--port` (default 8080)
   - `--port.grpc` (default 0 → 10000+port)
   - `--port.public` (default 0 → same as port)
   - `--ip` (auto-detect)
   - `--id` (default empty → ip:port)
   - `--publicUrl`
   - `--ip.bind`
   - `--master` (default "localhost:9333")
   - `--mserver` (deprecated compat)
   - `--preStopSeconds` (default 10)
   - `--idleTimeout` (default 30)
   - `--dataCenter`
   - `--rack`
   - `--index` [memory|leveldb|leveldbMedium|leveldbLarge]
   - `--disk` [hdd|ssd|<tag>]
   - `--tags`
   - `--dir` (default temp dir)
   - `--dir.idx`
   - `--max` (default "8")
   - `--whiteList`
   - `--minFreeSpacePercent` (default "1")
   - `--minFreeSpace`
   - `--images.fix.orientation` (default false)
   - `--readMode` [local|proxy|redirect] (default "proxy")
   - `--cpuprofile`
   - `--memprofile`
   - `--compactionMBps` (default 0)
   - `--maintenanceMBps` (default 0)
   - `--fileSizeLimitMB` (default 256)
   - `--concurrentUploadLimitMB` (default 0)
   - `--concurrentDownloadLimitMB` (default 0)
   - `--pprof` (default false)
   - `--metricsPort` (default 0)
   - `--metricsIp`
   - `--inflightUploadDataTimeout` (default 60s)
   - `--inflightDownloadDataTimeout` (default 60s)
   - `--hasSlowRead` (default true)
   - `--readBufferSizeMB` (default 4)
   - `--index.leveldbTimeout` (default 0)
   - `--debug` (default false)
   - `--debug.port` (default 6060)

1.5. Implement the same flag validation logic from `startVolumeServer()`:
   - Parse comma-separated `--dir`, `--max`, `--minFreeSpace`, `--disk`, `--tags`
   - Replicate single-value-to-all-dirs expansion
   - Validate count matches between dirs and limits
   - `--mserver` backward compat

1.6. **Test:** `cargo build` succeeds. `cargo run -- --help` shows all flags. Proto types generated.

**Verification:** Run with `--port 8080 --dir /tmp --master localhost:9333` — should parse without error and print config.

---

### Phase 2: Core Storage Types & On-Disk Format
**Goal:** Read and write the SeaweedFS needle/volume binary format bit-for-bit compatible with Go.

**Source files to port:**
- `weed/storage/types/needle_types.go` → `src/storage/types.rs`
- `weed/storage/needle/needle.go` → `src/storage/needle/needle.rs`
- `weed/storage/needle/needle_read.go` → `src/storage/needle/needle_read.rs`
- `weed/storage/needle/needle_write.go` (partial) → `src/storage/needle/needle_write.rs`
- `weed/storage/needle/crc.go` → `src/storage/needle/crc.rs`
- `weed/storage/needle/needle_value_map.go` → `src/storage/needle/needle_value.rs`
- `weed/storage/super_block/super_block.go` → `src/storage/super_block.rs`
- `weed/storage/idx/` → `src/storage/idx/`

**Steps:**

2.1. **Fundamental types** (`types.rs`):
   - `NeedleId` (u64), `Offset` (u32 or u64 depending on version), `Size` (i32, negative = deleted)
   - `Cookie` (u32)
   - `DiskType` enum (HDD, SSD, Custom)
   - Version constants (Version1=1, Version2=2, Version3=3, CurrentVersion=3)
   - Byte serialization matching Go's `binary.BigEndian` encoding

2.2. **SuperBlock** (`super_block.rs`):
   - 8-byte header: Version(1) + ReplicaPlacement(1) + TTL(2) + CompactRevision(2) + Reserved(2)
   - `ReplicaPlacement` struct with same/diff rack/dc counts
   - `TTL` struct with count + unit
   - Read/write from first 8 bytes of `.dat` file
   - Match exact byte layout from `super_block.go`

2.3. **Needle binary format** (`needle.rs`, `needle_read.rs`):
   - Version 2/3 header: Cookie(4) + NeedleId(8) + Size(4)
   - Body: Data, Flags, Name, Mime, PairsSize, Pairs, LastModified, TTL, Checksum, AppendAtNs, Padding
   - CRC32 checksum (matching Go's `crc32.ChecksumIEEE`)
   - Padding to 8-byte alignment
   - Read path: read header → compute body length → read body → verify CRC

2.4. **Idx file format** (`idx/`):
   - Fixed 16-byte records: NeedleId(8) + Offset(4) + Size(4)
   - Sequential append-only file
   - Walk/iterate all entries
   - Binary search not used (loaded into memory map)

2.5. **NeedleMap (in-memory)** (`needle_map.rs`):
   - HashMap<NeedleId, NeedleValue> where NeedleValue = {Offset, Size}
   - Load from `.idx` file on volume mount
   - Support Get, Set, Delete operations
   - Track file count, deleted count, deleted byte count

2.6. **Tests:**
   - Unit test: write a needle to bytes → read it back → verify fields match
   - Unit test: write/read SuperBlock round-trip
   - Unit test: write/read idx entries round-trip
   - **Cross-compat test:** Use Go volume server to create a small volume with known data. Read it from Rust and verify all needles decoded correctly. (Keep test fixture `.dat`/`.idx` files in `tests/fixtures/`)

---

### Phase 3: Volume Struct & Lifecycle
**Goal:** Mount, read from, write to, and unmount a volume.

**Source files to port:**
- `weed/storage/volume.go` → `src/storage/volume.rs`
- `weed/storage/volume_read.go` → `src/storage/volume_read.rs`
- `weed/storage/volume_write.go` → `src/storage/volume_write.rs`
- `weed/storage/volume_loading.go`
- `weed/storage/volume_vacuum.go` → `src/storage/volume_compact.rs`
- `weed/storage/volume_info/volume_info.go` → `src/storage/volume_info.rs`
- `weed/storage/volume_super_block.go`

**Steps:**

3.1. **Volume struct** (`volume.rs`):
   - Fields: Id, dir, dataFile, nm (NeedleMap), SuperBlock, readOnly, lastModifiedTs, lastCompactIndexOffset, lastCompactRevision
   - `noWriteOrDelete` / `noWriteCanDelete` / `readOnly` state flags
   - File handles for `.dat` file (read + append)
   - Lock strategy: `RwLock` for concurrent reads, exclusive writes

3.2. **Volume loading** — exact logic from `volume_loading.go`:
   - Open `.dat` file, read SuperBlock from first 8 bytes
   - Load `.idx` file into NeedleMap
   - Handle `.vif` (VolumeInfo) JSON sidecar file
   - Set volume state based on SuperBlock + VolumeInfo

3.3. **Volume read** (`volume_read.rs`) — from `volume_read.go`:
   - `ReadNeedle(needleId, cookie)`: lookup in NeedleMap → seek in .dat → read needle bytes → verify cookie + CRC → return data
   - Handle deleted needles (Size < 0)
   - `ReadNeedleBlob(offset, size)`: raw blob read
   - `ReadNeedleMeta(needleId, offset, size)`: read metadata only

3.4. **Volume write** (`volume_write.rs`) — from `volume_write.go`:
   - `WriteNeedle(needle)`: serialize needle → append to .dat → update .idx → update NeedleMap
   - `DeleteNeedle(needleId)`: mark as deleted in NeedleMap + append tombstone to .idx
   - File size limit check
   - Concurrent write serialization (mutex on write path)

3.5. **Volume compaction** (`volume_compact.rs`) — from `volume_vacuum.go`:
   - `CheckCompact()`: compute garbage ratio
   - `Compact()`: create new .dat/.idx, copy only live needles, update compact revision
   - `CommitCompact()`: rename compacted files over originals
   - `CleanupCompact()`: remove temp files
   - Throttle by `compactionBytePerSecond`

3.6. **Volume info** (`volume_info.rs`):
   - Read/write `.vif` JSON sidecar
   - VolumeInfo protobuf struct mapping
   - Remote file references for tiered storage

3.7. **Tests:**
   - Mount a volume, write 100 needles, read them all back, verify content
   - Delete 50 needles, verify they return "deleted"
   - Compact, verify only 50 remain, verify content
   - Read Go-created volume fixtures

---

### Phase 4: Store (Multi-Volume, Multi-Disk Manager)
**Goal:** Manage multiple volumes across multiple disk directories.

**Source files to port:**
- `weed/storage/store.go` → `src/storage/store.rs`
- `weed/storage/disk_location.go` → `src/storage/disk_location.rs`
- `weed/storage/store_ec.go`
- `weed/storage/store_state.go`

**Steps:**

4.1. **DiskLocation** (`disk_location.rs`):
   - Directory path, max volume count, min free space, disk type, tags
   - Load all volumes from directory on startup
   - Track free space, check writable

4.2. **Store** (`store.rs`):
   - Vector of `DiskLocation`s
   - `GetVolume(volumeId)` → lookup across all locations
   - `HasVolume(volumeId)` check
   - `AllocateVolume(...)` — create new volume in appropriate location
   - `DeleteVolume(...)`, `MountVolume(...)`, `UnmountVolume(...)`
   - `DeleteCollection(collection)` — delete all volumes of a collection
   - Collect volume status for heartbeat
   - `SetStopping()`, `Close()`
   - Persistent state (maintenance mode) via `store_state.go`

4.3. **Store state** — `VolumeServerState` protobuf with maintenance flag, persisted to disk.

4.4. **Tests:**
   - Create store with 2 dirs, allocate volumes in each, verify load balancing
   - Mount/unmount/delete lifecycle
   - State persistence across restart

---

### Phase 5: Erasure Coding
**Goal:** Full EC shard encode/decode/read/write/rebuild.

**Source files to port:**
- `weed/storage/erasure_coding/` (3,599 lines)

**Steps:**

5.1. **EC volume + shard structs** — `EcVolume`, `EcShard` with file handles for `.ec00`–`.ec13` shard files + `.ecx` index + `.ecj` journal.

5.2. **EC encoder** — Reed-Solomon 10+4 (configurable) encoding using `reed-solomon-erasure` crate:
   - `VolumeEcShardsGenerate`: read .dat → split into data shards → compute parity → write .ec00-.ec13 + .ecx

5.3. **EC decoder/reader** — reconstruct data from any 10 of 14 shards:
   - `EcShardRead`: read range from a specific shard
   - Locate needle in EC volume via .ecx index
   - Handle cross-shard needle reads

5.4. **EC shard operations:**
   - Copy, delete, mount, unmount shards
   - `VolumeEcShardsRebuild`: rebuild missing shards from remaining
   - `VolumeEcShardsToVolume`: reconstruct .dat from EC shards
   - `VolumeEcBlobDelete`: mark deleted in EC journal
   - `VolumeEcShardsInfo`: report shard metadata

5.5. **Tests:**
   - Encode a volume → verify 14 shards created
   - Delete 4 shards → rebuild → verify data intact
   - Read individual needles from EC volume
   - Cross-compat with Go-generated EC shards

---

### Phase 6: Backend / Tiered Storage
**Goal:** Support tiered storage to remote backends (S3, etc).

**Source files to port:**
- `weed/storage/backend/` (1,850 lines)

**Steps:**

6.1. **Backend trait** — abstract `BackendStorage` trait with `ReadAt`, `WriteAt`, `Truncate`, `Close`, `Name`.

6.2. **Disk backend** — default local disk implementation.

6.3. **S3 backend** — upload .dat to S3, read ranges via S3 range requests.

6.4. **Tier move operations:**
   - `VolumeTierMoveDatToRemote`: upload .dat to remote, optionally delete local
   - `VolumeTierMoveDatFromRemote`: download .dat from remote

6.5. **Tests:**
   - Disk backend read/write round-trip
   - S3 backend with mock/localstack

---

### Phase 7: Security Layer
**Goal:** JWT authentication, whitelist guard, TLS configuration.

**Source files to port:**
- `weed/security/guard.go` → `src/security/guard.rs`
- `weed/security/jwt.go` → `src/security/jwt.rs`
- `weed/security/tls.go` → `src/security/tls.rs`

**Steps:**

7.1. **Guard** (`guard.rs`):
   - Whitelist IP check (exact match on `r.RemoteAddr`)
   - Wrap handlers with whitelist enforcement
   - `UpdateWhiteList()` for live reload

7.2. **JWT** (`jwt.rs`):
   - `SeaweedFileIdClaims` with `fid` field
   - Sign with HMAC-SHA256
   - Verify + decode with expiry check
   - Separate signing keys for read vs write
   - `GetJwt(request)` — extract from `Authorization: Bearer` header or `jwt` query param

7.3. **TLS** (`tls.rs`):
   - Load server TLS cert/key for gRPC and HTTPS
   - Load client TLS for mutual TLS
   - Read from `security.toml` config (same format as Go's viper config)

7.4. **Tests:**
   - JWT sign → verify round-trip
   - JWT with wrong key → reject
   - JWT with expired token → reject
   - JWT fid mismatch → reject
   - Whitelist allow/deny

---

### Phase 8: Prometheus Metrics
**Goal:** Export same metric names as Go for dashboard compatibility.

**Source files to port:**
- `weed/stats/metrics.go` (volume server counters/gauges/histograms)

**Steps:**

8.1. Define all Prometheus metrics matching Go names:
   - `VolumeServerRequestCounter` (labels: method, status)
   - `VolumeServerRequestHistogram` (labels: method)
   - `VolumeServerInFlightRequestsGauge` (labels: method)
   - `VolumeServerInFlightUploadSize`
   - `VolumeServerInFlightDownloadSize`
   - `VolumeServerConcurrentUploadLimit`
   - `VolumeServerConcurrentDownloadLimit`
   - `VolumeServerHandlerCounter` (labels: type — UploadLimitCond, DownloadLimitCond)
   - Read/Write/Delete request counters

8.2. Metrics HTTP endpoint on `--metricsPort`.

8.3. Optional push-based metrics loop (`LoopPushingMetric`).

8.4. **Test:** Verify metric names and labels match Go output.

---

### Phase 9: HTTP Server & Handlers
**Goal:** All HTTP endpoints with exact same behavior as Go.

**Source files to port:**
- `weed/server/volume_server.go` → `src/server/volume_server.rs`
- `weed/server/volume_server_handlers.go` → `src/server/http_handlers.rs`
- `weed/server/volume_server_handlers_read.go` → `src/server/http_read.rs`
- `weed/server/volume_server_handlers_write.go` → `src/server/http_write.rs`
- `weed/server/volume_server_handlers_admin.go` → `src/server/http_admin.rs`
- `weed/server/volume_server_handlers_helper.go` (URL parsing, proxy, JSON responses)
- `weed/server/volume_server_handlers_ui.go` → `src/server/http_admin.rs`

**Steps:**

9.1. **URL path parsing** — from `handlers_helper.go`:
   - Parse `/<vid>,<fid>` and `/<vid>/<fid>` patterns
   - Extract volume ID, file ID, filename, ext

9.2. **Route dispatch** — from `privateStoreHandler` and `publicReadOnlyHandler`:
   - `GET /` → `GetOrHeadHandler`
   - `HEAD /` → `GetOrHeadHandler`
   - `POST /` → `PostHandler` (whitelist gated)
   - `PUT /` → `PostHandler` (whitelist gated)
   - `DELETE /` → `DeleteHandler` (whitelist gated)
   - `OPTIONS /` → CORS preflight
   - `GET /status` → JSON status
   - `GET /healthz` → health check
   - `GET /ui/index.html` → HTML UI page
   - Static resources (CSS/JS for UI)

9.3. **GET/HEAD handler** (`http_read.rs`) — from `handlers_read.go` (468 lines):
   - JWT read authorization check
   - Lookup needle by volume ID + needle ID + cookie
   - ETag / If-None-Match / If-Modified-Since conditional responses
   - Content-Type from stored MIME or filename extension
   - Content-Disposition header
   - Content-Encoding (gzip/zstd stored data)
   - Range request support (HTTP 206 Partial Content)
   - JPEG orientation fix (if configured)
   - Proxy to replica on local miss (readMode=proxy)
   - Redirect to replica (readMode=redirect)
   - Download tracking (in-flight size accounting)

9.4. **POST/PUT handler** (`http_write.rs`) — from `handlers_write.go` (170 lines):
   - JWT write authorization check
   - Multipart form parsing
   - Extract file data, filename, content type, TTL, last-modified
   - Optional gzip/zstd compression
   - Write needle to volume
   - Replicate to peers (same logic as Go's `DistributedOperation`)
   - Return JSON: {name, size, eTag, error}

9.5. **DELETE handler** — already in handlers.go:
   - JWT authorization
   - Delete from local volume
   - Replicate delete to peers
   - Return JSON result

9.6. **Admin handlers** (`http_admin.rs`):
   - `/status` → JSON with volumes, version, disk status
   - `/healthz` → 200 OK if serving
   - `/ui/index.html` → HTML dashboard

9.7. **Concurrency limiting** — from `handlers.go`:
   - Upload concurrency limit with `sync::Condvar` + timeout
   - Download concurrency limit with proxy fallback to replicas
   - HTTP 429 on timeout, 499 on client cancel
   - Replication traffic bypasses upload limits

9.8. **Public port** — if configured, separate listener with read-only routes (GET/HEAD/OPTIONS only).

9.9. **Request ID middleware** — generate unique request ID per request.

9.10. **Tests:**
   - Integration: start server → upload file via POST → GET it back → verify content
   - Integration: upload → DELETE → GET returns 404
   - Integration: conditional GET with ETag → 304
   - Integration: range request → 206 with correct bytes
   - Integration: exceed upload limit → 429
   - Integration: whitelist enforcement
   - Integration: JWT enforcement

---

### Phase 10: gRPC Service Implementation
**Goal:** All 40 gRPC methods with exact logic.

**Source files to port:**
- `weed/server/volume_grpc_admin.go` (380 lines)
- `weed/server/volume_grpc_vacuum.go` (124 lines)
- `weed/server/volume_grpc_copy.go` (636 lines)
- `weed/server/volume_grpc_copy_incremental.go` (66 lines)
- `weed/server/volume_grpc_read_write.go` (74 lines)
- `weed/server/volume_grpc_batch_delete.go` (124 lines)
- `weed/server/volume_grpc_tail.go` (140 lines)
- `weed/server/volume_grpc_erasure_coding.go` (619 lines)
- `weed/server/volume_grpc_scrub.go` (121 lines)
- `weed/server/volume_grpc_tier_upload.go` (98 lines)
- `weed/server/volume_grpc_tier_download.go` (85 lines)
- `weed/server/volume_grpc_remote.go` (95 lines)
- `weed/server/volume_grpc_query.go` (69 lines)
- `weed/server/volume_grpc_state.go` (26 lines)
- `weed/server/volume_grpc_read_all.go` (35 lines)
- `weed/server/volume_grpc_client_to_master.go` (325 lines)

**Steps (grouped by functional area):**

10.1. **Implement `tonic::Service` for `VolumeServer`** — the generated trait from proto.

10.2. **Admin RPCs** (`grpc_admin.rs`):
   - `AllocateVolume` — create volume on appropriate disk location
   - `VolumeMount` / `VolumeUnmount` / `VolumeDelete`
   - `VolumeMarkReadonly` / `VolumeMarkWritable`
   - `VolumeConfigure` — change replication
   - `VolumeStatus` — return read-only, size, file counts
   - `VolumeServerStatus` — disk statuses, memory, version, DC, rack
   - `VolumeServerLeave` — deregister from master
   - `DeleteCollection`
   - `VolumeNeedleStatus` — get needle metadata by ID
   - `Ping` — latency measurement
   - `GetState` / `SetState` — maintenance mode

10.3. **Vacuum RPCs** (`grpc_vacuum.rs`):
   - `VacuumVolumeCheck` — return garbage ratio
   - `VacuumVolumeCompact` — stream progress (streaming response)
   - `VacuumVolumeCommit` — finalize compaction
   - `VacuumVolumeCleanup` — remove temp files

10.4. **Copy RPCs** (`grpc_copy.rs`):
   - `VolumeCopy` — stream .dat/.idx from source to create local copy
   - `VolumeSyncStatus` — return sync metadata
   - `VolumeIncrementalCopy` — stream .dat delta since timestamp (streaming)
   - `CopyFile` — generic file copy by extension (streaming)
   - `ReceiveFile` — receive streamed file (client streaming)
   - `ReadVolumeFileStatus` — return file timestamps and sizes

10.5. **Read/Write RPCs** (`grpc_read_write.rs`):
   - `ReadNeedleBlob` — raw needle blob read
   - `ReadNeedleMeta` — needle metadata
   - `WriteNeedleBlob` — raw needle blob write
   - `ReadAllNeedles` — stream all needles from volume(s) (streaming)

10.6. **Batch delete** (`grpc_batch_delete.rs`):
   - `BatchDelete` — delete multiple file IDs, return per-ID results

10.7. **Tail RPCs** (`grpc_tail.rs`):
   - `VolumeTailSender` — stream new needles since timestamp (streaming)
   - `VolumeTailReceiver` — connect to another volume server and tail its changes

10.8. **Erasure coding RPCs** (`grpc_erasure_coding.rs`):
   - `VolumeEcShardsGenerate` — generate EC shards from volume
   - `VolumeEcShardsRebuild` — rebuild missing shards
   - `VolumeEcShardsCopy` — copy shards from another server
   - `VolumeEcShardsDelete` — delete EC shards
   - `VolumeEcShardsMount` / `VolumeEcShardsUnmount`
   - `VolumeEcShardRead` — read from EC shard (streaming)
   - `VolumeEcBlobDelete` — mark blob deleted in EC volume
   - `VolumeEcShardsToVolume` — reconstruct volume from EC shards
   - `VolumeEcShardsInfo` — return shard metadata

10.9. **Scrub RPCs** (`grpc_scrub.rs`):
   - `ScrubVolume` — integrity check volumes (INDEX / FULL / LOCAL modes)
   - `ScrubEcVolume` — integrity check EC volumes

10.10. **Tier RPCs** (`grpc_tier.rs`):
   - `VolumeTierMoveDatToRemote` — upload to remote backend (streaming progress)
   - `VolumeTierMoveDatFromRemote` — download from remote (streaming progress)

10.11. **Remote storage** (`grpc_remote.rs`):
   - `FetchAndWriteNeedle` — fetch from remote storage, write locally, replicate

10.12. **Query** (`grpc_query.rs`):
   - `Query` — experimental CSV/JSON/Parquet select on stored data (streaming)

10.13. **Master heartbeat** (`grpc_client_to_master.rs`):
   - `heartbeat()` background task — periodic gRPC stream to master
   - Send: volume info, EC shard info, disk stats, has-no-space flags, deleted volumes
   - Receive: volume size limit, leader address, metrics config
   - Reconnect on failure with backoff
   - `StopHeartbeat()` for graceful shutdown

10.14. **Tests:**
   - Integration test per RPC: call via tonic client → verify response
   - Streaming RPCs: verify all chunks received
   - Error cases: invalid volume ID, non-existent volume, etc.
   - Heartbeat: mock master gRPC server, verify registration

---

### Phase 11: Startup, Lifecycle & Graceful Shutdown
**Goal:** Full server startup matching Go's `runVolume()` and `startVolumeServer()`.

**Steps:**

11.1. **Startup sequence** (match `volume.go` exactly):
   1. Load security configuration from `security.toml`
   2. Start metrics server on metrics port
   3. Parse folder/max/minFreeSpace/diskType/tags
   4. Validate all directory writable
   5. Resolve IP, bind IP, public URL, gRPC port
   6. Create `VolumeServer` struct
   7. Check with master (initial handshake)
   8. Create `Store` (loads all existing volumes from disk)
   9. Create security `Guard`
   10. Register HTTP routes on admin mux
   11. Optionally register public mux
   12. Start gRPC server on gRPC port
   13. Start public HTTP server (if separated)
   14. Start cluster HTTP server (with optional TLS)
   15. Start heartbeat background task
   16. Start metrics push loop
   17. Register SIGHUP handler for config reload + new volume loading

11.2. **Graceful shutdown** (match Go exactly):
   1. On SIGINT/SIGTERM:
   2. Stop heartbeat (notify master we're leaving)
   3. Wait `preStopSeconds`
   4. Stop public HTTP server
   5. Stop cluster HTTP server
   6. Graceful stop gRPC server
   7. `volumeServer.Shutdown()` → `store.Close()` (flush all volumes)

11.3. **Reload** (SIGHUP):
   - Reload security config
   - Update whitelist
   - Load newly appeared volumes from disk

11.4. **Tests:**
   - Start server → send SIGTERM → verify clean shutdown
   - Start server → SIGHUP → verify config reloaded

---

### Phase 12: Integration & Cross-Compatibility Testing
**Goal:** Rust volume server is a drop-in replacement for Go volume server.

**Steps:**

12.1. **Binary compatibility tests:**
   - Create volumes with Go volume server
   - Start Rust volume server on same data directory
   - Read all data → verify identical
   - Write new data with Rust → read with Go → verify

12.2. **API compatibility tests:**
   - Run same HTTP requests against both Go and Rust servers
   - Compare response bodies, headers, status codes
   - Test all gRPC RPCs against both

12.3. **Master interop test:**
   - Start Go master server
   - Register Rust volume server
   - Verify heartbeat works
   - Verify volume assignment works
   - Upload via filer → stored on Rust volume server → read back

12.4. **Performance benchmarks:**
   - Throughput: sequential writes, sequential reads
   - Latency: p50/p99 for read/write
   - Concurrency: parallel reads/writes
   - Compare Rust vs Go numbers

12.5. **Edge cases:**
   - Volume at max size
   - Disk full handling
   - Corrupt .dat file recovery
   - Network partition during replication
   - EC shard loss + rebuild

---

## Execution Order & Dependencies

```
Phase 1  (Skeleton + CLI)        ← no deps, start here
   ↓
Phase 2  (Storage types)         ← needs Phase 1 (types used everywhere)
   ↓
Phase 3  (Volume struct)         ← needs Phase 2
   ↓
Phase 4  (Store manager)         ← needs Phase 3
   ↓
Phase 7  (Security)              ← independent, can parallel with 3-4
Phase 8  (Metrics)               ← independent, can parallel with 3-4
   ↓
Phase 9  (HTTP server)           ← needs Phase 4 + 7 + 8
Phase 10 (gRPC server)           ← needs Phase 4 + 7 + 8
   ↓
Phase 5  (Erasure coding)        ← needs Phase 4, wire into Phase 10
Phase 6  (Tiered storage)        ← needs Phase 4, wire into Phase 10
   ↓
Phase 11 (Startup + shutdown)    ← needs Phase 9 + 10
   ↓
Phase 12 (Integration tests)     ← needs all above
```

## Estimated Scope

| Phase | Estimated Rust Lines | Complexity |
|---|---|---|
| 1. Skeleton + CLI | ~400 | Low |
| 2. Storage types | ~2,000 | High (binary compat critical) |
| 3. Volume struct | ~2,500 | High |
| 4. Store manager | ~1,000 | Medium |
| 5. Erasure coding | ~3,000 | High |
| 6. Tiered storage | ~1,500 | Medium |
| 7. Security | ~500 | Medium |
| 8. Metrics | ~300 | Low |
| 9. HTTP server | ~2,000 | High |
| 10. gRPC server | ~3,500 | High |
| 11. Startup/shutdown | ~500 | Medium |
| 12. Integration tests | ~2,000 | Medium |
| **Total** | **~19,000** | |

## Critical Invariants to Preserve

1. **Binary format compatibility** — Rust must read/write `.dat`, `.idx`, `.vif`, `.ecX` files identically to Go. A single byte off = data loss.
2. **gRPC wire compatibility** — Same proto, same field semantics. Go master must talk to Rust volume server seamlessly.
3. **HTTP API compatibility** — Same URL patterns, same JSON response shapes, same headers, same status codes.
4. **Replication protocol** — Write replication between Go and Rust volume servers must work bidirectionally.
5. **Heartbeat protocol** — Rust volume server must register with Go master and maintain heartbeat.
6. **CRC32 algorithm** — Must use IEEE polynomial (same as Go's `crc32.ChecksumIEEE`).
7. **JWT compatibility** — Tokens signed by Go filer/master must be verifiable by Rust volume server and vice versa.
