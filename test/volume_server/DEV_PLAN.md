# Volume Server Integration Test Dev Plan (Go)

## Goal
Create a Go integration test suite under `test/volume_server` that validates **drop-in behavior parity** for the Volume Server HTTP and gRPC APIs, so a Rust rewrite can be verified against the current Go behavior.

## Hard Requirements
- Tests live under `test/volume_server`.
- Tests are written in Go.
- HTTP + gRPC APIs are both covered.
- Coverage targets execution-path parity (happy path + edge/failure/state variants), not only API reachability.
- During implementation, commit each logic change separately for reviewability.

## Ground Truth (API Surface)
- HTTP handlers:
  - `weed/server/volume_server.go`
  - `weed/server/volume_server_handlers.go`
  - `weed/server/volume_server_handlers_read.go`
  - `weed/server/volume_server_handlers_write.go`
  - `weed/server/volume_server_handlers_admin.go`
  - `weed/server/common.go` (path parsing and range handling)
- gRPC service and handlers:
  - `weed/pb/volume_server.proto`
  - `weed/server/volume_grpc_*.go`

## Proposed Test Directory Tree

```text
test/volume_server/
  DEV_PLAN.md
  README.md
  Makefile
  framework/
    cluster.go              # test cluster lifecycle, ports, process control
    volume_fixture.go       # test data provisioning and seed volumes
    http_client.go          # request helpers, auth helpers, assertions
    grpc_client.go          # grpc dial + common call wrappers
    fault_injection.go      # kill/restart peers, network/error simulation hooks
  matrix/
    config_profiles.go      # runtime matrix profiles (jwt, readMode, public port split, etc.)
  http/
    admin_test.go           # /status, /healthz, /ui, static resources
    read_test.go            # GET/HEAD read behaviors + variations
    write_test.go           # POST/PUT write behaviors + variations
    delete_test.go          # DELETE behaviors + variations
    cors_and_options_test.go
    throttling_test.go      # upload/download in-flight limit paths
    auth_test.go            # jwt/no-jwt/bad-jwt/fid mismatch
  grpc/
    admin_lifecycle_test.go
    vacuum_test.go
    data_rw_test.go
    copy_sync_test.go
    tail_test.go
    erasure_coding_test.go
    tiering_test.go
    remote_fetch_test.go
    scrub_test.go
    query_test.go
    health_state_test.go
  compatibility/
    golden_behavior_test.go # protocol-level parity checks for selected canonical flows
```

## Environment/Matrix Profiles (Execution Dimensions)
Each API should be exercised across the smallest set of profiles that still covers behavior divergence:

- `P1`: Single volume server, no JWT, `readMode=proxy`, single HTTP port.
- `P2`: Public/admin port split (`port.public != port`) to verify public read-only behavior.
- `P3`: JWT enabled (`jwt.signing.key` and `jwt.signing.read.key`) with valid/invalid/missing tokens.
- `P4`: Replicated volume layout (>=2 volume servers) to cover proxy/redirect/replicate paths.
- `P5`: Erasure coding volumes present.
- `P6`: Remote tier backend configured.
- `P7`: Maintenance mode enabled.
- `P8`: Upload/download throttling limits enabled.

## HTTP API Test Case Tree

### 1. Admin and service endpoints
- [ ] `GET /status`
  - [ ] baseline payload fields (`Version`, `DiskStatuses`, `Volumes`)
  - [ ] response headers (`Server`, request ID)
- [ ] `GET /healthz`
  - [ ] healthy -> `200`
  - [ ] server stopping -> `503`
  - [ ] heartbeat disabled -> `503`
- [ ] `GET /ui/index.html`
  - [ ] enabled path renders page
  - [ ] disabled/secured behavior matches current config
- [ ] static assets (`/favicon.ico`, `/seaweedfsstatic/*`) reachable

### 2. Data read endpoints (`GET`/`HEAD` on `/...`)
- [ ] URL shape variants
  - [ ] `/{vid},{fid}`
  - [ ] `/{vid}/{fid}`
  - [ ] `/{vid}/{fid}/{filename}`
  - [ ] malformed vid/fid -> `400`
- [ ] auth variants
  - [ ] no JWT required
  - [ ] read JWT missing/invalid/fid-mismatch -> `401`
  - [ ] valid JWT -> success
- [ ] locality/read mode variants
  - [ ] local volume read success
  - [ ] missing local volume with `readMode=local` -> `404`
  - [ ] missing local volume with `readMode=proxy` -> proxied response
  - [ ] missing local volume with `readMode=redirect` -> `301`
  - [ ] proxied-loop guard (`proxied=true`) behavior
- [ ] object state variants
  - [ ] not found/deleted -> `404`
  - [ ] cookie mismatch -> `404`
  - [ ] internal read failure -> `500`
  - [ ] `readDeleted=true` behavior
- [ ] conditional headers
  - [ ] `If-Modified-Since` -> `304`
  - [ ] `If-None-Match` -> `304`
  - [ ] normal ETag emission
- [ ] range handling
  - [ ] full read (no `Range`)
  - [ ] single range -> `206`, `Content-Range`
  - [ ] multi-range -> `206`, multipart body
  - [ ] invalid range -> `416`
  - [ ] oversized-range-sum behavior parity
- [ ] content transformations
  - [ ] compressed data + `Accept-Encoding=gzip`
  - [ ] compressed data without accepted encoding (decompress path)
  - [ ] image resize (`width`,`height`,`mode`)
  - [ ] image crop (`crop_*`)
- [ ] chunk manifest behavior
  - [ ] manifest auto-expansion path
  - [ ] `cm=false` bypass path
- [ ] response header passthrough queries
  - [ ] `response-*` overrides and `dl` content-disposition behavior
- [ ] `HEAD` parity
  - [ ] headers same as `GET` minus body
  - [ ] content-length behavior parity

### 3. Data write endpoints (`PUT`/`POST` on `/...`)
- [ ] URL shape and parse validation
- [ ] write JWT variants (missing/invalid/mismatch)
- [ ] payload variants
  - [ ] standard upload success -> `201`
  - [ ] unchanged write -> `204` with ETag
  - [ ] oversize file rejected (file-size limit)
  - [ ] malformed multipart/form data -> `400`
- [ ] metadata variants (name/mime/pairs/md5 headers)
- [ ] replication path
  - [ ] `type=replicate` bypasses upload throttling
  - [ ] replication write failure behavior
- [ ] throttling paths
  - [ ] limit disabled
  - [ ] over limit wait then proceed
  - [ ] timeout -> `429`
  - [ ] canceled request -> `499`

### 4. Data delete endpoint (`DELETE` on `/...`)
- [ ] normal volume delete success (`202` + size)
- [ ] non-existing needle (`404` size=0)
- [ ] EC volume delete path
- [ ] cookie mismatch -> `400`
- [ ] chunk manifest delete (children first)
  - [ ] chunk delete success
  - [ ] chunk delete failure -> `500`
- [ ] `ts` override behavior
- [ ] auth variants as in write path

### 5. Method/CORS behavior
- [ ] private port `OPTIONS` -> allow `PUT,POST,GET,DELETE,OPTIONS`
- [ ] public port `OPTIONS` -> allow `GET,OPTIONS`
- [ ] CORS origin headers on requests with `Origin`
- [ ] unsupported method behavior parity (private and public ports)

### 6. Concurrency-limit and replica fallback behavior
- [ ] download over-limit + replica available -> proxy/redirect fallback
- [ ] download over-limit + no replica -> wait/timeout/cancel outcomes
- [ ] upload/download inflight counters update and release (no leaks)

## gRPC API Test Case Tree

For each RPC below: cover baseline success, validation/argument errors, state preconditions (maintenance, missing volume), and stream interruption where applicable.

### A. Admin/Lifecycle
- [ ] `DeleteCollection`
  - [ ] existing collection
  - [ ] non-existing collection idempotence/error parity
- [ ] `AllocateVolume`
  - [ ] success
  - [ ] maintenance mode reject
  - [ ] duplicate/invalid allocation parity
- [ ] `VolumeMount`
  - [ ] success
  - [ ] missing/not-mountable volume
- [ ] `VolumeUnmount`
  - [ ] success
  - [ ] missing/not-mounted volume
- [ ] `VolumeDelete`
  - [ ] `only_empty=true` and `false`
  - [ ] maintenance mode reject
- [ ] `VolumeConfigure`
  - [ ] success
  - [ ] invalid replication string -> `resp.Error` path
  - [ ] unmount failure path
  - [ ] configure failure + remount rollback path
  - [ ] mount failure path
- [ ] `VolumeMarkReadonly`
  - [ ] success with `persist=false/true`
  - [ ] volume not found
  - [ ] notify-master failure (pre/post local transition)
- [ ] `VolumeMarkWritable`
  - [ ] success
  - [ ] volume not found
  - [ ] notify-master failure
- [ ] `VolumeStatus`
  - [ ] success
  - [ ] volume not found
  - [ ] data backend missing
- [ ] `VolumeServerStatus`
  - [ ] payload completeness (`State`, `MemoryStatus`, disk statuses)
- [ ] `VolumeServerLeave`
  - [ ] heartbeat stopped effect
- [ ] `GetState`
- [ ] `SetState`
  - [ ] state transition success
  - [ ] invalid update error path
- [ ] `Ping`
  - [ ] target type: filer / volume / master
  - [ ] unreachable target error wrapping
  - [ ] unknown target type behavior parity

### B. Vacuum / compaction
- [ ] `VacuumVolumeCheck`
  - [ ] success + garbage ratio
  - [ ] missing volume/error path
- [ ] `VacuumVolumeCompact` (stream)
  - [ ] progress events emitted
  - [ ] maintenance mode reject
  - [ ] compact failure
  - [ ] client stream receive interruption
- [ ] `VacuumVolumeCommit`
  - [ ] success (readonly + size fields)
  - [ ] maintenance mode reject
  - [ ] commit failure
- [ ] `VacuumVolumeCleanup`
  - [ ] success
  - [ ] maintenance mode reject
  - [ ] cleanup failure

### C. Data read/write
- [ ] `ReadNeedleBlob`
  - [ ] success
  - [ ] missing volume
  - [ ] invalid offset/size path
- [ ] `ReadNeedleMeta`
  - [ ] success
  - [ ] missing volume
  - [ ] EC-only volume unsupported path
  - [ ] read metadata failure
- [ ] `WriteNeedleBlob`
  - [ ] success
  - [ ] maintenance mode reject
  - [ ] missing volume
  - [ ] write failure
- [ ] `VolumeNeedleStatus`
  - [ ] normal volume success
  - [ ] EC volume success
  - [ ] volume missing
  - [ ] needle missing/read error

### D. Batch and scan
- [ ] `BatchDelete`
  - [ ] `skip_cookie_check=true/false`
  - [ ] invalid fid parse
  - [ ] not found
  - [ ] cookie mismatch path
  - [ ] chunk manifest reject path (`406` in result)
  - [ ] regular and EC delete paths
- [ ] `ReadAllNeedles` (stream)
  - [ ] multiple volumes success
  - [ ] one missing volume abort behavior

### E. Copy/sync/replication streams
- [ ] `VolumeSyncStatus`
  - [ ] success
  - [ ] missing volume
- [ ] `VolumeIncrementalCopy` (stream)
  - [ ] data streamed from `since_ns`
  - [ ] `isLastOne` no-data path
  - [ ] missing volume/error path
- [ ] `VolumeCopy` (stream)
  - [ ] full copy success (`.dat/.idx/.vif`) + mount
  - [ ] existing destination volume delete-before-copy path
  - [ ] source unavailable / read status failure
  - [ ] no free location
  - [ ] remote-dat-file branch
  - [ ] copy integrity mismatch failures
  - [ ] final progress/append timestamp behavior
- [ ] `ReadVolumeFileStatus`
  - [ ] success field validation
  - [ ] missing volume
- [ ] `CopyFile` (stream)
  - [ ] normal volume path
  - [ ] EC volume path
  - [ ] compaction revision mismatch
  - [ ] missing source file with ignore flag true/false
  - [ ] zero-byte and stop-offset edge cases
- [ ] `ReceiveFile` (client stream)
  - [ ] happy path regular volume
  - [ ] happy path EC file target
  - [ ] info-first protocol violation
  - [ ] unknown message type
  - [ ] maintenance mode reject
  - [ ] write/create failure cleanup behavior

### F. Tailing
- [ ] `VolumeTailSender` (stream)
  - [ ] volume not found
  - [ ] heartbeat chunks when no updates
  - [ ] idle-timeout drain completion
  - [ ] large needle chunking behavior
- [ ] `VolumeTailReceiver`
  - [ ] success applies streamed writes
  - [ ] destination volume missing
  - [ ] source stream/connect failure

### G. Erasure coding
- [ ] `VolumeEcShardsGenerate`
  - [ ] success default config
  - [ ] success with existing `.vif` EC config
  - [ ] maintenance mode reject
  - [ ] collection mismatch
  - [ ] generate/write cleanup on failure
- [ ] `VolumeEcShardsRebuild`
  - [ ] rebuild missing shards success
  - [ ] no shards found path
  - [ ] rebuild failures
- [ ] `VolumeEcShardsCopy`
  - [ ] shard copy success
  - [ ] `copy_ecx`/`copy_ecj`/`copy_vif` toggles
  - [ ] explicit `disk_id` valid/invalid
  - [ ] no-space/source-copy failure
- [ ] `VolumeEcShardsDelete`
  - [ ] delete selected shard ids
  - [ ] delete-last-shard cleanup (`.ecx/.ecj` + optional `.vif`)
  - [ ] missing shard no-op parity
- [ ] `VolumeEcShardsMount`
  - [ ] multi-shard success
  - [ ] per-shard failure abort behavior
- [ ] `VolumeEcShardsUnmount`
  - [ ] multi-shard success
  - [ ] per-shard failure abort behavior
- [ ] `VolumeEcShardRead` (stream)
  - [ ] success
  - [ ] not-found volume/shard
  - [ ] deleted file key returns `IsDeleted`
  - [ ] chunked streaming for large reads
- [ ] `VolumeEcBlobDelete`
  - [ ] delete existing blob
  - [ ] already deleted idempotence
  - [ ] locate failure
- [ ] `VolumeEcShardsToVolume`
  - [ ] success path from EC -> normal
  - [ ] missing EC volume/shard
  - [ ] invalid data-shard config
  - [ ] no-live-entries failed-precondition path
  - [ ] write dat/idx failures
- [ ] `VolumeEcShardsInfo`
  - [ ] success counts (including deleted)
  - [ ] missing EC volume
  - [ ] walk-index failure

### H. Tiering and remote
- [ ] `VolumeTierMoveDatToRemote` (stream)
  - [ ] success with progress events
  - [ ] maintenance mode reject
  - [ ] volume missing / collection mismatch
  - [ ] destination backend missing
  - [ ] destination exists already
  - [ ] keep-local true/false branches
- [ ] `VolumeTierMoveDatFromRemote` (stream)
  - [ ] success with progress events
  - [ ] volume missing / collection mismatch
  - [ ] already-local path
  - [ ] backend missing/download failure
  - [ ] keep-remote true/false branches
- [ ] `FetchAndWriteNeedle`
  - [ ] success without replicas
  - [ ] success with replica fanout
  - [ ] maintenance mode reject
  - [ ] missing volume
  - [ ] remote client/read failure
  - [ ] local write failure
  - [ ] one replica write failure behavior

### I. Query and scrub
- [ ] `Query` (stream)
  - [ ] JSON input selection/filter success
  - [ ] malformed fid parse failure
  - [ ] read/cookie mismatch failure
  - [ ] CSV-input current behavior parity
- [ ] `ScrubVolume`
  - [ ] auto-select all volumes when request empty
  - [ ] mode `INDEX`
  - [ ] mode `LOCAL` (not-implemented detail reporting)
  - [ ] mode `FULL` (not-implemented detail reporting)
  - [ ] unsupported mode error
- [ ] `ScrubEcVolume`
  - [ ] auto-select all EC volumes when request empty
  - [ ] mode `INDEX`
  - [ ] mode `LOCAL`
  - [ ] mode `FULL` (not-implemented detail reporting)
  - [ ] unsupported mode error

## Phased Implementation Plan (Tracking)

### Phase 0: Harness and scaffolding
- [x] Create `framework/` cluster bootstrap and teardown
- [x] Add profile-based environment builder (P1..P8)
- [x] Add common assertion helpers (HTTP and gRPC)
- [x] Add `README.md` with run instructions
- [x] Add `Makefile` targets (`test-volume-server`, profile filters)

### Phase 1: HTTP parity suites
- [ ] Admin/status/health/UI/static
- [ ] Read path variants and headers/range/transforms
- [ ] Write/delete/auth/throttling/public-port behavior

### Phase 2: gRPC parity suites (core)
- [x] Admin/lifecycle/state/ping
- [ ] Vacuum + batch + data rw
- [ ] Copy/sync/tail

### Phase 3: gRPC parity suites (advanced)
- [ ] Erasure coding family
- [ ] Tiering + remote fetch
- [ ] Query + scrub

### Phase 4: Compatibility hardening
- [ ] Golden behavior assertions for canonical flows
- [ ] Flake reduction and deterministic retries/timeouts
- [ ] CI runtime tuning and sharding

## Commit Strategy (for implementation)
Use one commit per logical change set. Suggested sequence:

1. `test(volume_server): add integration framework and cluster lifecycle`
2. `test(volume_server): add config profile matrix and test utilities`
3. `test(volume_server/http): add admin and health endpoint coverage`
4. `test(volume_server/http): add read-path matrix coverage`
5. `test(volume_server/http): add write/delete/auth/throttling coverage`
6. `test(volume_server/grpc): add admin lifecycle and state/ping coverage`
7. `test(volume_server/grpc): add vacuum batch and data rw coverage`
8. `test(volume_server/grpc): add copy sync and tail coverage`
9. `test(volume_server/grpc): add erasure coding coverage`
10. `test(volume_server/grpc): add tiering and remote fetch coverage`
11. `test(volume_server/grpc): add query and scrub coverage`
12. `test(volume_server): add compatibility golden scenarios and docs`

## Progress Log
Update this section during implementation:

- Date: 2026-02-12
- Change: Added initial dev plan and project scaffold.
- APIs covered: Planning only.
- Profiles covered: Planning only.
- Gaps introduced/remaining: Implementation not started yet.
- Commit: `21c10a9ec`

- Date: 2026-02-12
- Change: Added integration harness, profile matrix, and auto-build support for missing `weed` binary. Master now starts with `-peers=none` and low `-volumeSizeLimitMB`.
- APIs covered: Harness only.
- Profiles covered: P1, P2, P3, P8 definitions in place.
- Gaps introduced/remaining: Full API case matrix still pending.
- Commit: `5e9c437e0`, `1be296139`

- Date: 2026-02-12
- Change: Added HTTP integration coverage for admin endpoints, method options, and upload/read/range/head/delete roundtrip.
- APIs covered: `/status`, `/healthz`, `/ui/index.html`, `OPTIONS /`, `POST/GET/HEAD/DELETE /{fid}`.
- Profiles covered: P1, P2.
- Gaps introduced/remaining: Remaining HTTP branch variants (JWT/proxy/redirect/throttling/etc.) still pending.
- Commit: `038e9161e`, `1df1e3812`

- Date: 2026-02-12
- Change: Added gRPC integration coverage for state/status/ping and admin lifecycle/maintenance checks.
- APIs covered: `GetState`, `SetState`, `VolumeServerStatus`, `Ping`, `AllocateVolume`, `VolumeStatus`, `VolumeMount`, `VolumeUnmount`, `VolumeDelete`.
- Profiles covered: P1.
- Gaps introduced/remaining: Remaining gRPC methods and advanced branches still pending.
- Commit: `9a9b8c500`, `3c562a64c`

- Date: 2026-02-12
- Change: Added HTTP cache/range branch tests.
- APIs covered: `GET /{fid}` with `If-None-Match` (`304`) and invalid `Range` (`416`).
- Profiles covered: P1.
- Gaps introduced/remaining: Remaining HTTP auth/proxy/redirect/throttling branches pending.
- Commit: `317346b51`

- Date: 2026-02-12
- Change: Added gRPC `BatchDelete` integration checks for invalid fid mapping and maintenance-mode rejection.
- APIs covered: `BatchDelete`.
- Profiles covered: P1.
- Gaps introduced/remaining: Remaining gRPC method families still pending.
- Commit: `7a8aed127`

- Date: 2026-02-12
- Change: Added gRPC integration tests for needle status, configure validation branch, ping volume-target branch, and leave/health interaction.
- APIs covered: `VolumeNeedleStatus`, `VolumeConfigure` (invalid replication response path), `Ping` (`volumeServer` target), `VolumeServerLeave`.
- Profiles covered: P1.
- Gaps introduced/remaining: Still pending large RPC groups (vacuum/copy/tail/ec/tiering/query/scrub).
- Commit: `59a571a10`
