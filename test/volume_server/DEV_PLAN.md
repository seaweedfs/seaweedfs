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

- Date: 2026-02-12
- Change: Added gRPC vacuum integration coverage for success/missing-volume and maintenance-mode rejection branches.
- APIs covered: `VacuumVolumeCheck`, `VacuumVolumeCompact`, `VacuumVolumeCommit`, `VacuumVolumeCleanup`.
- Profiles covered: P1.
- Gaps introduced/remaining: Copy/sync/tail, EC, tiering, query, scrub, and many HTTP matrix branches still pending.
- Commit: `0f7cc53dd`

- Date: 2026-02-12
- Change: Added gRPC data read/write error-path coverage for missing-volume and maintenance-mode branches.
- APIs covered: `ReadNeedleBlob`, `ReadNeedleMeta`, `WriteNeedleBlob`.
- Profiles covered: P1.
- Gaps introduced/remaining: Positive-path blob/meta and stream/copy/tail/EC/tiering/query/scrub families remain.
- Commit: `f83ad41b5`

- Date: 2026-02-12
- Change: Added HTTP JWT integration coverage for missing/invalid/valid token behavior across write and read paths.
- APIs covered: HTTP `POST /{fid}` and `GET /{fid}` auth paths with read/write signing keys.
- Profiles covered: P3.
- Gaps introduced/remaining: Remaining HTTP proxy/redirect/throttling branches still pending.
- Commit: `def509acb`

- Date: 2026-02-12
- Change: Added gRPC sync/copy family tests for success and missing-volume or maintenance-mode stream error paths.
- APIs covered: `VolumeSyncStatus`, `VolumeIncrementalCopy`, `ReadAllNeedles`, `ReadVolumeFileStatus`, `CopyFile`, `VolumeCopy`, `ReceiveFile`.
- Profiles covered: P1.
- Gaps introduced/remaining: Tail/EC/tiering/query/scrub and positive-path copy/tail flows still pending.
- Commit: `b13642838`

- Date: 2026-02-12
- Change: Added gRPC scrub and query integration coverage for supported/unsupported modes and invalid/missing fid paths.
- APIs covered: `ScrubVolume`, `ScrubEcVolume`, `Query`.
- Profiles covered: P1.
- Gaps introduced/remaining: Query success path and broader scrub mode matrix remain pending.
- Commit: `a2ab4cde8`

- Date: 2026-02-12
- Change: Added gRPC tail integration coverage for sender heartbeat/EOF behavior and sender/receiver missing-volume errors.
- APIs covered: `VolumeTailSender`, `VolumeTailReceiver`.
- Profiles covered: P1.
- Gaps introduced/remaining: Tail success replication path and large-needle chunking remain pending.
- Commit: `fd582ba58`

- Date: 2026-02-12
- Change: Expanded gRPC admin/lifecycle coverage with readonly/writable transitions, collection delete behavior, non-empty delete `only_empty` branch, and ping unknown/unreachable target variants.
- APIs covered: `VolumeMarkReadonly`, `VolumeMarkWritable`, `DeleteCollection`, `VolumeDelete` (`only_empty=true/false`), `Ping` (unknown and unreachable master target).
- Profiles covered: P1.
- Gaps introduced/remaining: Notify-master failure paths for readonly/writable and additional admin error branches still pending.
- Commit: `2e6d577f7`, `a3a2da791`, `9f887f25c`, `724bbe2d9`

- Date: 2026-02-12
- Change: Added gRPC positive-path data coverage for blob/meta read/write roundtrip and stream-all-needles payload validation.
- APIs covered: `ReadNeedleBlob`, `ReadNeedleMeta`, `WriteNeedleBlob`, `ReadAllNeedles`.
- Profiles covered: P1.
- Gaps introduced/remaining: Additional blob/meta offset/size corruption branches remain.
- Commit: `21e94d1d2`

- Date: 2026-02-12
- Change: Expanded gRPC scrub/query coverage for auto-select volume behavior, local/full scrub detail branches, JSON query success filtering, CSV no-output behavior, and EC auto-select empty result.
- APIs covered: `ScrubVolume` (`INDEX`, `LOCAL`, `FULL`, auto-select), `ScrubEcVolume` (missing-volume + auto-select empty), `Query` (JSON success + CSV no-output + invalid paths).
- Profiles covered: P1.
- Gaps introduced/remaining: EC scrub full/local positive paths need EC fixture setup.
- Commit: `8cdf3589a`, `12150a9a2`

- Date: 2026-02-12
- Change: Added gRPC tiering/remote early-branch error coverage.
- APIs covered: `FetchAndWriteNeedle` (maintenance + missing volume), `VolumeTierMoveDatToRemote` (missing volume, collection mismatch, maintenance), `VolumeTierMoveDatFromRemote` (missing volume, collection mismatch, already-local path).
- Profiles covered: P1.
- Gaps introduced/remaining: Tier upload/download success flows with real remote backend remain.
- Commit: `51e6fa749`

- Date: 2026-02-12
- Change: Expanded HTTP behavior coverage for split public port semantics, CORS on origin requests, unsupported-method parity, unchanged-write `204`, delete edge branches, and JWT fid-mismatch auth rejection.
- APIs covered: public/admin method divergence (`GET/HEAD/POST/DELETE/PATCH`), CORS headers, write unchanged response path, delete cookie-mismatch/missing-needle paths, JWT fid mismatch for write/read.
- Profiles covered: P1, P2, P3.
- Gaps introduced/remaining: Remaining HTTP proxy/redirect/throttling and transformation branches still pending.
- Commit: `2de39c548`, `9998d19dd`, `ea5d8b7b3`

- Date: 2026-02-12
- Change: Expanded tier/remote gRPC variation coverage with invalid remote config and missing destination backend branches.
- APIs covered: `FetchAndWriteNeedle` (invalid `RemoteConf`), `VolumeTierMoveDatToRemote` (destination backend not found).
- Profiles covered: P1.
- Gaps introduced/remaining: Tier upload/download success flows with an actual remote backend and replica fanout behavior remain.
- Commit: `855c84f31`

- Date: 2026-02-12
- Change: Expanded copy/receive stream coverage with incremental-copy data/no-data branches and receive-file protocol violation handling.
- APIs covered: `VolumeIncrementalCopy` (stream data + EOF no-data), `CopyFile` (ignore missing source + `stop_offset=0`), `ReceiveFile` (content-before-info and unknown message type response errors).
- Profiles covered: P1.
- Gaps introduced/remaining: Full `VolumeCopy` happy path with a real source volume node remains.
- Commit: `1e99407e1`

- Date: 2026-02-12
- Change: Added additional copy/receive branches for compaction mismatch and regular-volume receive-file success with byte-for-byte verification via `CopyFile`.
- APIs covered: `CopyFile` (compaction revision mismatch), `ReceiveFile` (successful regular volume write path).
- Profiles covered: P1.
- Gaps introduced/remaining: EC receive-file success path and cleanup failure branches remain.
- Commit: `4c710463e`

- Date: 2026-02-12
- Change: Added HTTP read-path variants and conditional request coverage.
- APIs covered: `GET /{vid}/{fid}`, `GET /{vid}/{fid}/{filename}`, malformed `/{vid}/{fid}` parse error path, `If-Modified-Since` (`304`) behavior.
- Profiles covered: P1.
- Gaps introduced/remaining: Proxy/redirect read mode matrix and image/chunk-manifest transformation branches remain.
- Commit: `1f64ebe1d`

- Date: 2026-02-12
- Change: Added HTTP passthrough header and static resource coverage.
- APIs covered: query-based `response-*` header passthrough, `dl=true` content-disposition attachment handling, `/favicon.ico`, `/seaweedfsstatic/seaweed50x50.png`.
- Profiles covered: P1.
- Gaps introduced/remaining: Additional static resource variants and multi-range response formatting checks remain.
- Commit: `f1ad1ec50`

- Date: 2026-02-12
- Change: Added gRPC ping branch coverage for unreachable filer target.
- APIs covered: `Ping` (`target_type=filer` unreachable target path).
- Profiles covered: P1.
- Gaps introduced/remaining: Successful ping path for filer/master targets in multi-service integration setup remains.
- Commit: `c6ace0331`

- Date: 2026-02-12
- Change: Added initial erasure-coding RPC integration coverage for maintenance-gate, missing-volume, invalid-disk, and no-op behaviors.
- APIs covered: `VolumeEcShardsGenerate`, `VolumeEcShardsRebuild`, `VolumeEcShardsCopy`, `VolumeEcShardsDelete`, `VolumeEcShardsMount`, `VolumeEcShardsUnmount`, `VolumeEcShardRead`, `VolumeEcBlobDelete`, `VolumeEcShardsToVolume`, `VolumeEcShardsInfo`.
- Profiles covered: P1.
- Gaps introduced/remaining: Positive EC data-path flows (generate/copy/mount/read/delete/to-volume/info with actual shard files) still require EC fixture setup.
- Commit: `c7592d118`

- Date: 2026-02-12
- Change: Added HTTP multi-range response coverage for multipart `206` behavior.
- APIs covered: `GET /{fid}` with multi-range header (`Range: bytes=0-1,4-5`) and multipart response validation.
- Profiles covered: P1.
- Gaps introduced/remaining: Oversized multi-range sum behavior and deeper range-edge normalization remain.
- Commit: `39c68c679`

- Date: 2026-02-12
- Change: Added query no-match parity coverage to lock current stream semantics.
- APIs covered: `Query` JSON filter no-match path (returns one empty stripe, then EOF).
- Profiles covered: P1.
- Gaps introduced/remaining: CSV parsing behavior beyond current no-output branch still pending.
- Commit: `39895cb84`

- Date: 2026-02-12
- Change: Added HTTP upload throttling integration coverage with deterministic timeout and replicate-bypass behavior.
- APIs covered: upload limit timeout path (`429`) and `type=replicate` bypass branch under concurrent upload pressure.
- Profiles covered: P8 (with short inflight timeout in test profile).
- Gaps introduced/remaining: download throttling wait/proxy branches remain.
- Commit: `464d0b2b6`

- Date: 2026-02-12
- Change: Added HTTP download throttling timeout coverage under concurrent large-read pressure.
- APIs covered: download limit timeout path (`429`) when another large response keeps in-flight download data above limit.
- Profiles covered: P8 (short inflight download timeout).
- Gaps introduced/remaining: download replica proxy fallback branch (`proxied=true`/replica redirect) remains.
- Commit: `a929e6ddc`

- Date: 2026-02-12
- Change: Expanded JWT auth mismatch variations for same-needle wrong-cookie tokens.
- APIs covered: write/read JWT rejection when token fid differs only by cookie from requested fid.
- Profiles covered: P3.
- Gaps introduced/remaining: token expiry boundary behavior remains untested.
- Commit: `61fe52398`

- Date: 2026-02-12
- Change: Added JWT expired-token rejection coverage for both write and read auth paths.
- APIs covered: write/read auth rejection when token signature is valid but `exp` is in the past.
- Profiles covered: P3.
- Gaps introduced/remaining: additional JWT transport variants (query/cookie token sources) remain.
- Commit: `6e808623f`

- Date: 2026-02-12
- Change: Added JWT token transport coverage via query parameter and HTTP-only cookie.
- APIs covered: write auth using `?jwt=` token and read auth using `AT` cookie token.
- Profiles covered: P3.
- Gaps introduced/remaining: JWT precedence rules when multiple token sources are present remain.
- Commit: `ccefdfe8d`

- Date: 2026-02-12
- Change: Added JWT token-source precedence coverage when both query and header tokens are present.
- APIs covered: query-token precedence over header-token for write/read auth checks.
- Profiles covered: P3.
- Gaps introduced/remaining: explicit query-vs-cookie precedence combination remains.
- Commit: `605054e5d`

- Date: 2026-02-12
- Change: Added JWT token-source precedence coverage when both header and cookie tokens are present.
- APIs covered: header-token precedence over cookie-token for write/read auth checks.
- Profiles covered: P3.
- Gaps introduced/remaining: JWT transport precedence matrix for query/header/cookie is now covered for tested combinations.
- Commit: `3fcaf845c`

- Date: 2026-02-12
- Change: Added JWT token-source precedence coverage when both query and cookie tokens are present.
- APIs covered: query-token precedence over cookie-token for write/read auth checks, including positive path when query is valid and cookie is invalid.
- Profiles covered: P3.
- Gaps introduced/remaining: none in current JWT token source precedence matrix.
- Commit: `4ea552973`

- Date: 2026-02-12
- Change: Added gRPC state update validation coverage for optimistic versioning and nil-state requests.
- APIs covered: `SetState` stale-version mismatch error path and nil-state no-op path.
- Profiles covered: P1.
- Gaps introduced/remaining: persistent state save failure branch remains environment-dependent.
- Commit: `34ff97996`

- Date: 2026-02-12
- Change: Added readonly lifecycle variation for persisted readonly flag path.
- APIs covered: `VolumeMarkReadonly` success path with `persist=true`.
- Profiles covered: P1.
- Gaps introduced/remaining: notify-master failure branches remain untested.
- Commit: `c37e6cd95`

- Date: 2026-02-12
- Change: Added CORS header validation on `OPTIONS` requests with `Origin` for admin and public ports.
- APIs covered: `OPTIONS /` CORS headers (`Access-Control-Allow-Origin`, `Access-Control-Allow-Credentials`) for split-port profile.
- Profiles covered: P2.
- Gaps introduced/remaining: unsupported-method parity for additional verbs beyond `PATCH`/`TRACE` remains.
- Commit: `ca08af7ba`

- Date: 2026-02-12
- Change: Added unsupported-method parity coverage for `TRACE` on admin/public split ports.
- APIs covered: admin `TRACE` error (`400`) vs public `TRACE` passthrough (`200`) behavior.
- Profiles covered: P2.
- Gaps introduced/remaining: broader unsupported verb matrix remains.
- Commit: `b03ddf855`

- Date: 2026-02-12
- Change: Added gRPC batch-delete cookie-check variation coverage.
- APIs covered: `BatchDelete` mismatch-cookie rejection path (`skip_cookie_check=false`) and skip-cookie-check acceptance/deletion path (`skip_cookie_check=true`).
- Profiles covered: P1.
- Gaps introduced/remaining: batch-delete malformed entry combinations are partially covered; mixed per-entry status permutations can be expanded.
- Commit: `87d75e786`

- Date: 2026-02-12
- Change: Expanded gRPC admin lifecycle variants for allocate/mount/unmount/delete edge cases.
- APIs covered: duplicate `AllocateVolume` rejection, missing-volume `VolumeMount` error, idempotent `VolumeUnmount` behavior for missing/already-unmounted volumes, and `VolumeDelete` maintenance-mode rejection.
- Profiles covered: P1.
- Gaps introduced/remaining: `VolumeConfigure` rollback/mount-failure branches still need dedicated fault-path coverage.
- Commit: `bc1faec8e`

- Date: 2026-02-12
- Change: Added mixed gRPC `BatchDelete` result-matrix coverage including early-stop behavior on cookie mismatch.
- APIs covered: per-entry status matrix in one request (`400` invalid fid, `202` accepted delete, `404` missing fid) and early break semantics when cookie mismatch occurs before later entries.
- Profiles covered: P1.
- Gaps introduced/remaining: chunk-manifest rejection (`406`) and EC batch-delete success path still require dedicated fixtures.
- Commit: `450f63ac4`

- Date: 2026-02-12
- Change: Added secured-UI HTTP behavior coverage under JWT-enabled profile.
- APIs covered: `/ui/index.html` route behavior when admin UI is not exposed due signing key; verified fallback auth-gated response path (`401`).
- Profiles covered: P3.
- Gaps introduced/remaining: explicit `access.ui=true` override scenario remains untested.
- Commit: `9c10ccb38`

- Date: 2026-02-12
- Change: Expanded split-port unsupported HTTP method matrix with non-standard verb coverage.
- APIs covered: admin/public parity for `PROPFIND` (`400` on admin, passthrough `200` on public) with post-call data-integrity verification.
- Profiles covered: P2.
- Gaps introduced/remaining: remaining unsupported-verb breadth now primarily around less common methods (e.g., `CONNECT`) and proxy-specific edge semantics.
- Commit: `1d7afd11e`

- Date: 2026-02-12
- Change: Expanded gRPC `VolumeConfigure` coverage for both success and configure-failure rollback reporting.
- APIs covered: valid replication success path and missing-volume configure failure path with remount-restore failure detail propagation.
- Profiles covered: P1.
- Gaps introduced/remaining: explicit unmount-failure and mount-failure branches via injected I/O faults are still pending.
- Commit: `287a60197`

- Date: 2026-02-12
- Change: Added `VolumeNeedleStatus` error-path coverage.
- APIs covered: missing-volume error path and missing-needle error path on existing normal volumes.
- Profiles covered: P1.
- Gaps introduced/remaining: EC-backed positive/error status permutations still require dedicated EC fixture state.
- Commit: `bf0c609a7`

- Date: 2026-02-12
- Change: Added HTTP deleted-needle read recovery coverage.
- APIs covered: `GET` with `readDeleted=true` returning deleted needle content, alongside normal post-delete `404` behavior.
- Profiles covered: P1.
- Gaps introduced/remaining: proxy/redirect interactions with `readDeleted` remain unverified.
- Commit: `2ed9434cf`

- Date: 2026-02-12
- Change: Added HTTP delete `ts` query parity coverage for deleted-read metadata behavior.
- APIs covered: `DELETE ?ts=` followed by `GET ?readDeleted=true`, asserting current Last-Modified parity with pre-delete reads.
- Profiles covered: P1.
- Gaps introduced/remaining: explicit externally visible timestamp override effects remain limited in current API responses.
- Commit: `225b8e800`

- Date: 2026-02-12
- Change: Added gRPC invalid-offset coverage for needle blob/meta reads.
- APIs covered: `ReadNeedleBlob` and `ReadNeedleMeta` failure paths on existing volumes with out-of-range offsets.
- Profiles covered: P1.
- Gaps introduced/remaining: low-level corrupted-size and backend I/O fault branches still require fault-injection hooks.
- Commit: `33ed77ad6`

- Date: 2026-02-12
- Change: Added mixed-volume `ReadAllNeedles` stream abort coverage.
- APIs covered: stream progression from an existing volume followed by missing-volume abort error in the same request.
- Profiles covered: P1.
- Gaps introduced/remaining: multi-volume happy-path ordering/volume-boundary assertions can be expanded further.
- Commit: `7799b28b1`

- Date: 2026-02-12
- Change: Tightened HTTP `HEAD` parity assertions for read path.
- APIs covered: `HEAD` behavior now verifies empty response body while retaining `Content-Length` parity expectations.
- Profiles covered: P1.
- Gaps introduced/remaining: additional conditional-header parity checks on `HEAD` can still be expanded.
- Commit: `9499e5400`

- Date: 2026-02-12
- Change: Expanded `VolumeServerStatus` payload assertions.
- APIs covered: `VolumeServerStatus` now validates presence of `State` and `MemoryStatus` (including non-zero goroutine count), in addition to version/disk payload checks.
- Profiles covered: P1.
- Gaps introduced/remaining: heartbeat-disabled and stopping-state transitions are still exercised indirectly rather than in a dedicated status-payload transition test.
- Commit: `374411418`

- Date: 2026-02-12
- Change: Added gRPC `BatchDelete` chunk-manifest rejection coverage.
- APIs covered: `BatchDelete` result status/error path for chunk-manifest needles (`406`, `ChunkManifest` message) and non-deletion parity after rejection.
- Profiles covered: P1.
- Gaps introduced/remaining: EC-backed `BatchDelete` positive path still pending dedicated EC fixture setup.
- Commit: `326be22a9`

- Date: 2026-02-12
- Change: Added gRPC `Query` cookie-mismatch branch parity coverage.
- APIs covered: `Query` behavior when fid id exists but cookie mismatches; verified current EOF/no-record stream outcome.
- Profiles covered: P1.
- Gaps introduced/remaining: CSV parsing behavior beyond current no-output path remains pending.
- Commit: `2aaf0a339`

- Date: 2026-02-12
- Change: Added positive gRPC `Ping` coverage for master targets.
- APIs covered: `Ping` success path for `target_type=master` with non-zero remote timestamp and valid timing envelope.
- Profiles covered: P1.
- Gaps introduced/remaining: positive filer-target ping path still requires a filer fixture in the integration harness.
- Commit: `fa5cad6dc`

- Date: 2026-02-12
- Change: Expanded HTTP conditional-header parity for `HEAD`.
- APIs covered: `HEAD` with `If-None-Match` now verifies `304` behavior and empty-body semantics.
- Profiles covered: P1.
- Gaps introduced/remaining: explicit `HEAD` + `If-Modified-Since` parity remains expandable.
- Commit: `9984f2ec4`

- Date: 2026-02-12
- Change: Added HTTP `HEAD` + `If-Modified-Since` conditional parity coverage.
- APIs covered: `HEAD` conditional path returning `304` with empty-body semantics when `If-Modified-Since` matches Last-Modified.
- Profiles covered: P1.
- Gaps introduced/remaining: deeper conditional-header combinations (`If-None-Match` + `If-Modified-Since` precedence) remain expandable.
- Commit: `e87563a3c`

- Date: 2026-02-12
- Change: Expanded split-port unsupported-method matrix with `CONNECT` parity coverage.
- APIs covered: admin/public behavior for `CONNECT` (`400` on admin, passthrough `200` on public) with post-call data-integrity verification.
- Profiles covered: P2.
- Gaps introduced/remaining: unsupported-method parity now covers `PATCH`, `TRACE`, `PROPFIND`, and `CONNECT`; additional uncommon verbs can still be sampled as needed.
- Commit: `2a893d10d`

- Date: 2026-02-12
- Change: Added explicit CORS `Access-Control-Allow-Headers` assertions for `OPTIONS`.
- APIs covered: admin/public `OPTIONS` now verify `Access-Control-Allow-Headers: *` in addition to allowed-method matrices.
- Profiles covered: P2.
- Gaps introduced/remaining: CORS method/header semantics are now covered for baseline split-port flows.
- Commit: `6fcb9fa9c`

- Date: 2026-02-12
- Change: Added dual-volume integration harness and read-mode matrix tests for missing-local volume behavior.
- APIs covered: HTTP read path when volume is missing locally across `readMode=proxy` (forward success), `readMode=redirect` (`301` + `proxied=true`), and `readMode=local` (`404`).
- Profiles covered: custom P1-derived profiles with `ReadMode` overrides.
- Gaps introduced/remaining: throttling-specific proxy fallback (`checkDownloadLimit` replica path) is still pending targeted pressure setup.
- Commits: `74b04a3f8`, `70ce0c8b8`

- Date: 2026-02-12
- Change: Added HTTP download-throttling replica fallback coverage under over-limit pressure.
- APIs covered: `checkDownloadLimit` replica-proxy branch (`download over limit + replica available -> proxy fallback`) with replicated dual-node setup.
- Profiles covered: P8-derived profile (`readMode=proxy`) with dual volume servers.
- Gaps introduced/remaining: cancellation branch (`499`) for download-limit waiting remains pending.
- Commit: `316cfb7a3`

- Date: 2026-02-12
- Change: Expanded missing-local deleted-read parity across proxy and redirect modes.
- APIs covered: `readDeleted=true` behavior from non-owning servers in `readMode=proxy` (forwarded success) and `readMode=redirect` (redirect query-drop parity leading to `404` on follow).
- Profiles covered: custom P1-derived profiles with `ReadMode` overrides.
- Gaps introduced/remaining: explicit proxied-loop edge behavior remains pending dedicated setup.
- Commit: `0164a383d`

- Date: 2026-02-12
- Change: Added filer-enabled harness and positive gRPC `Ping` coverage for filer targets.
- APIs covered: `Ping` success path for `target_type=filer` with non-zero remote timestamp and valid timing envelope.
- Profiles covered: P1 (single volume + filer auxiliary process).
- Gaps introduced/remaining: no additional ping target-type gaps remain in current harness scope.
- Commits: `5f09d86a8`, `2fc1dde3f`

- Date: 2026-02-12
- Change: Added download-limit proxied-loop guard coverage.
- APIs covered: over-limit download path with `proxied=true` now verifies replica fallback is skipped and timeout returns `429`.
- Profiles covered: P8-derived profile (`readMode=proxy`) with dual volume servers.
- Gaps introduced/remaining: explicit cancellation (`499`) branch for wait loops remains difficult to assert over HTTP transport semantics.
- Commit: `6d532eddc`

- Date: 2026-02-12
- Change: Added explicit no-limit throttling coverage for baseline profile.
- APIs covered: upload/download limit-disabled branches (`concurrent*Limit=0`) under concurrent pressure, verifying requests proceed (`200`/`201`) without throttling.
- Profiles covered: P1.
- Gaps introduced/remaining: cancellation (`499`) path remains pending due client-transport observability constraints.
- Commit: `2cd9a9c6f`

- Date: 2026-02-12
- Change: Added gRPC `VolumeServerLeave` idempotence coverage.
- APIs covered: repeated `VolumeServerLeave` calls (already-stopped heartbeat path) with persistent `healthz=503` verification.
- Profiles covered: P1.
- Gaps introduced/remaining: none for leave semantics in current harness.
- Commit: `0fd666916`

- Date: 2026-02-12
- Change: Expanded redirect read-mode query handling coverage for collection-aware redirects.
- APIs covered: non-owning redirect path now verifies `collection` query parameter preservation in `Location` alongside `proxied=true`.
- Profiles covered: P1-derived profile with `ReadMode=redirect` using dual volume servers.
- Gaps introduced/remaining: redirect branch currently preserves only `collection`; broader query propagation is intentionally untested for parity with current behavior.
- Commit: `ad287b392`

- Date: 2026-02-12
- Change: Tightened HTTP admin endpoint header parity checks.
- APIs covered: `/status` and `/healthz` now assert `Server` header format (`SeaweedFS Volume ...`) in addition to status and payload checks.
- Profiles covered: P1.
- Gaps introduced/remaining: none for baseline admin header checks.
- Commit: `cad34314b`

- Date: 2026-02-12
- Change: Expanded admin middleware parity checks for request-id propagation.
- APIs covered: `/healthz` now explicitly verifies request-id echo behavior via `x-amz-request-id` response header.
- Profiles covered: P1.
- Gaps introduced/remaining: none for request-id propagation on covered admin endpoints.
- Commit: `e0268a5b7`

- Date: 2026-02-12
- Change: Added over-limit invalid-vid branch coverage in download throttling proxy path.
- APIs covered: `checkDownloadLimit` -> `tryProxyToReplica` invalid volume-id parse path now explicitly verified as `400` under over-limit pressure.
- Profiles covered: P8.
- Gaps introduced/remaining: cancellation (`499`) branch remains pending due client-side transport observability limits.
- Commit: `b4984b335`

- Date: 2026-02-12
- Change: Expanded static-resource coverage to split public-port topology.
- APIs covered: public-port static endpoints (`/favicon.ico`, `/seaweedfsstatic/seaweed50x50.png`) under P2.
- Profiles covered: P2.
- Gaps introduced/remaining: static asset baseline coverage is now present for both admin and public ports.
- Commit: `e4c329811`

- Date: 2026-02-12
- Change: Added split public-port `HEAD` method parity coverage.
- APIs covered: public-port `HEAD` read behavior (`200`, content-length parity, empty-body semantics) for existing files.
- Profiles covered: P2.
- Gaps introduced/remaining: none for baseline public-port `GET/HEAD/OPTIONS` method coverage.
- Commit: `127c43b1a`
