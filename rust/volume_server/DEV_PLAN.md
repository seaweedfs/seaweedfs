# Rust Volume Server Parity Implementation Plan

## Objective
Implement a native Rust volume server that replicates Go volume-server behavior for HTTP and gRPC APIs, so it can become a drop-in replacement validated by existing integration suites.

## Current Focus (2026-02-16)
- Program focus is now Rust implementation parity, not broad test expansion.
- `test/volume_server` is treated as the parity gate.
- Existing Rust launcher modes (`exec`, `proxy`) are transition tools; they are not the final target.

## Current Status
- Rust crate and launcher are in place.
- Integration harness can run:
  - Go master + Go volume (default)
  - Go master + Rust launcher (`VOLUME_SERVER_IMPL=rust`)
- Rust launcher `proxy` mode has full-suite integration pass while delegating backend handlers to Go.
- Rust launcher `native` mode is wired as the default Rust entrypoint and currently bootstraps via supervised Go backend delegation.
- Native Rust HTTP control handlers now serve `/status` and `/healthz` directly in `native` mode.
- Native Rust HTTP control handlers now serve `/status`, `/healthz`, and `OPTIONS` (admin/public method+CORS behavior) in `native` mode.
- Native Rust API/storage handlers are not implemented yet.

## Parity Exit Criteria
1. Native mode passes:
   - `env VOLUME_SERVER_IMPL=rust VOLUME_SERVER_RUST_MODE=native go test -count=1 ./test/volume_server/http`
   - `env VOLUME_SERVER_IMPL=rust VOLUME_SERVER_RUST_MODE=native go test -count=1 ./test/volume_server/grpc`
2. CI runs native Rust mode integration coverage (at least smoke, then expanded shards).
3. Rust mode defaults to native behavior for integration harness.
4. Go-backend delegation is removed (or retained only as explicit fallback mode).

## Architecture Workstreams

### A. Runtime and Configuration Parity
- [x] Add `native` runtime mode in `weed-volume-rs` (bootstrap delegation path).
- [ ] Parse and honor volume-server CLI/config flags used by integration harness:
  - [ ] network/bind ports (`-ip`, `-port`, `-port.grpc`, `-port.public`)
  - [ ] master target/config dir/read mode/throttling/JWT-related config
  - [ ] size/timeout controls and maintenance state defaults
- [ ] Implement graceful lifecycle behavior (signals, shutdown, readiness).

### B. Native HTTP Surface
- [ ] Admin/control endpoints:
  - [x] `GET /status` (native Rust in `native` mode)
  - [x] `GET /healthz` (native Rust in `native` mode)
  - [x] `OPTIONS` admin/public method+CORS control behavior (native Rust in `native` mode)
  - [ ] static/UI endpoints currently exercised
- [ ] Data read path parity:
  - [ ] fid parsing/path variants
  - [ ] conditional headers (`If-Modified-Since`, `If-None-Match`)
  - [ ] range handling (single/multi/invalid)
  - [ ] deleted reads, auth checks, read-mode branches
  - [ ] chunk-manifest and compression/image transformation branches
- [ ] Data write/delete parity:
  - [ ] write success/unchanged/error paths
  - [ ] replication and file-size-limit paths
  - [ ] delete and chunk-manifest delete branches
- [ ] Method/CORS/public-port parity for split admin/public behavior.

### C. Native gRPC Surface
- [ ] Control-plane RPCs:
  - [ ] `GetState`, `SetState`, `VolumeServerStatus`, `Ping`, `VolumeServerLeave`
  - [ ] admin lifecycle: allocate/mount/unmount/delete/configure/readonly/writable
- [ ] Data RPCs:
  - [ ] `ReadNeedleBlob`, `ReadNeedleMeta`, `WriteNeedleBlob`
  - [ ] `BatchDelete`, `ReadAllNeedles`
  - [ ] sync/copy/receive and status endpoints
- [ ] Stream RPCs:
  - [ ] tail sender/receiver
  - [ ] vacuum streams
  - [ ] query streams
- [ ] Advanced families:
  - [ ] erasure coding RPC set
  - [ ] tiering/remote fetch
  - [ ] scrub/query mode matrix

### D. Storage Compatibility Layer
- [ ] Implement volume data/index handling compatible with Go on-disk format.
- [ ] Preserve cookie/checksum/timestamp semantics used by tests.
- [ ] Match read/write/delete consistency and error mapping behavior.
- [ ] Ensure EC metadata/data-path compatibility with existing files.

### E. Operational Hardening
- [ ] Deterministic startup/readiness and shutdown semantics.
- [ ] Log/error parity sufficient for debugging and CI triage.
- [ ] Concurrency/timeout behavior alignment for throttling and streams.
- [ ] Performance baseline checks vs Go for key flows.

## Milestone Plan

### M0 (Completed): Harness + Launcher Transition
- [x] Rust launcher integrated into harness.
- [x] Proxy mode full-suite validation with Go backend delegation.

### M1: Native Skeleton (Control Plane First)
- [ ] `native` mode boots and serves:
  - [x] `/status`, `/healthz`
  - [ ] `GetState`, `SetState`, `VolumeServerStatus`, `Ping`, `VolumeServerLeave`
- Gate:
  - [x] targeted HTTP/grpc control tests pass in `native` mode (delegated backend path).

### M2: Native Core Data Paths
- [ ] Native HTTP read/write/delete baseline parity.
- [ ] Native gRPC data baseline parity (`Read/WriteNeedle*`, `BatchDelete`, `ReadAllNeedles`).
- Gate:
  - core HTTP and gRPC data suites pass in `native` mode.

### M3: Native Stream + Copy/Sync
- [ ] Tail/copy/receive/sync paths in native mode.
- Gate:
  - stream/copy families pass in `native` mode.

### M4: Native Advanced Feature Families
- [ ] EC, tiering, scrub/query advanced branches.
- Gate:
  - full `/test/volume_server/http` and `/test/volume_server/grpc` pass in `native` mode.

### M5: CI/Cutover
- [x] Add/expand native-mode CI jobs (smoke matrix includes `native`).
- [x] Make native mode default for Rust integration runs.
- [ ] Keep `exec`/`proxy` only as explicit fallback modes during rollout.

## Immediate Next Steps
1. Implement minimal native gRPC state/ping RPC handlers (`GetState`, `SetState`, `VolumeServerStatus`, `Ping`, `VolumeServerLeave`).
2. Expand native HTTP control surface beyond `/status` and `/healthz` (UI/static/auth-sensitive paths).
3. Keep rerunning native-mode integration suites as each delegated branch is replaced.
4. Add mismatch triage notes for each API moved from delegation to native implementation.

## Risk Register
- On-disk format mismatch risk:
  - Mitigation: implement format-level compatibility tests early (idx/dat/needle encoding).
- Behavioral drift in edge branches:
  - Mitigation: use integration suite failures as primary truth; only add tests for newly discovered untracked branches.
- Stream/concurrency semantic mismatch:
  - Mitigation: stabilize with focused interruption/timeout parity tests.

## Progress Log
- Date: 2026-02-15
- Change: Added Rust launcher integration (`exec`) and harness wiring.
- Validation: Rust launcher mode passed smoke and full integration suites while delegating to Go backend.
- Commits: `7beab85c2`, `880c2e1da`, `63d08e8a9`, `d402573ea`, `3bd20e6a1`, `6ce4d7ede`

- Date: 2026-02-15
- Change: Added Rust proxy supervisor mode and validated full integration suite.
- Validation:
  - `env VOLUME_SERVER_IMPL=rust VOLUME_SERVER_RUST_MODE=proxy go test -count=1 ./test/volume_server/http`
  - `env VOLUME_SERVER_IMPL=rust VOLUME_SERVER_RUST_MODE=proxy go test -count=1 ./test/volume_server/grpc`
- Commits: `a7f50d23b`, `548b3d9a3`

- Date: 2026-02-16
- Change: Re-focused plan from test expansion to native Rust implementation parity.
- Validation basis: latest Rust proxy full-suite pass keeps regression baseline stable while native implementation starts.
- Commits: `14c863dbf`

- Date: 2026-02-16
- Change: Added native Rust launcher mode bootstrap, set Rust launcher default mode to `native`, and expanded CI Rust smoke matrix to include `native`.
- Validation:
  - `env VOLUME_SERVER_IMPL=rust VOLUME_SERVER_RUST_MODE=native go test -count=1 ./test/volume_server/http`
  - `env VOLUME_SERVER_IMPL=rust VOLUME_SERVER_RUST_MODE=native go test -count=1 ./test/volume_server/grpc`
  - `env VOLUME_SERVER_IMPL=rust go test -count=1 ./test/volume_server/http -run '^TestAdminStatusAndHealthz$'`
  - `env VOLUME_SERVER_IMPL=rust go test -count=1 ./test/volume_server/grpc -run '^TestStateAndStatusRPCs$'`
- Commits: `70ddbee37`, `61befd10f`, `2e65966c0`

- Date: 2026-02-16
- Change: Implemented first native Rust HTTP handlers in `native` mode for `/status` and `/healthz` on the admin listener while preserving proxy delegation for other APIs.
- Validation:
  - `env VOLUME_SERVER_IMPL=rust VOLUME_SERVER_RUST_MODE=native go test -count=1 ./test/volume_server/http -run '^TestAdminStatusAndHealthz$'`
  - `env VOLUME_SERVER_IMPL=rust VOLUME_SERVER_RUST_MODE=native go test -count=1 ./test/volume_server/http`
  - `env VOLUME_SERVER_IMPL=rust VOLUME_SERVER_RUST_MODE=native go test -count=1 ./test/volume_server/grpc`
- Commits: `7e6e0261a`

- Date: 2026-02-16
- Change: Implemented native Rust `OPTIONS` handling for admin and public listeners in `native` mode, including method allow-list and origin-driven CORS response parity used by integration tests.
- Validation:
  - `env VOLUME_SERVER_IMPL=rust VOLUME_SERVER_RUST_MODE=native go test -count=1 ./test/volume_server/http -run '^TestOptionsMethodsByPort$|^TestOptionsWithOriginIncludesCorsHeaders$'`
  - `env VOLUME_SERVER_IMPL=rust VOLUME_SERVER_RUST_MODE=native go test -count=1 ./test/volume_server/http`
  - `env VOLUME_SERVER_IMPL=rust VOLUME_SERVER_RUST_MODE=native go test -count=1 ./test/volume_server/grpc`
- Commits: `fbff2cb39`
