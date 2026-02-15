# Rust Volume Server Rewrite Dev Plan

## Goal
Build a Rust implementation of SeaweedFS volume server that is behavior-compatible with the current Go implementation and can pass the existing integration suites under `/Users/chris/dev/seaweedfs2/test/volume_server/http` and `/Users/chris/dev/seaweedfs2/test/volume_server/grpc`.

## Compatibility Target
- CLI compatibility for volume-server startup flags used by integration harness.
- HTTP and gRPC behavioral parity for tested paths.
- Drop-in process integration with current Go master in transition phases.

## Phases

### Phase 0: Bootstrap and Harness Integration
- [x] Add Rust volume-server crate.
- [x] Implement Rust launcher that can run as a volume-server process entrypoint.
- [x] Add integration harness switches so tests can run with:
  - Go master + Go volume (default)
  - Go master + Rust volume (`VOLUME_SERVER_IMPL=rust` or `VOLUME_SERVER_BINARY=...`)
- [ ] Add CI smoke coverage for Rust volume-server mode.

### Phase 1: Native Rust Control Plane Skeleton
- [ ] Native Rust HTTP server with admin endpoints:
  - [ ] `GET /status`
  - [ ] `GET /healthz`
  - [ ] static/UI endpoints used by tests
- [ ] Native Rust gRPC server with basic lifecycle/state RPCs:
  - [ ] `GetState`, `SetState`, `VolumeServerStatus`, `Ping`, `VolumeServerLeave`
- [ ] Flag/config parser parity for currently exercised startup options.

### Phase 2: Native Data Path (HTTP + core gRPC)
- [ ] HTTP read/write/delete parity:
  - [ ] path variants, conditional headers, ranges, auth, throttling
  - [ ] chunk manifest read/delete behavior
  - [ ] image and compression transform branches
- [ ] gRPC data RPC parity:
  - [ ] `ReadNeedleBlob`, `ReadNeedleMeta`, `WriteNeedleBlob`
  - [ ] `BatchDelete`, `ReadAllNeedles`
  - [ ] copy/receive/sync baseline

### Phase 3: Advanced gRPC Surface
- [ ] Vacuum RPC family.
- [ ] Tail sender/receiver.
- [ ] Erasure coding family.
- [ ] Tiering/remote fetch family.
- [ ] Query/Scrub family.

### Phase 4: Hardening and Cutover
- [ ] Determinism/flake hardening in integration runtime.
- [ ] Performance and resource-baseline checks versus Go.
- [ ] Optional dual-run diff tooling for payload/header parity.
- [ ] Default harness/CI mode switch to Rust volume server once parity threshold is met.

## Integration Test Mapping
- HTTP suite: `/Users/chris/dev/seaweedfs2/test/volume_server/http`
- gRPC suite: `/Users/chris/dev/seaweedfs2/test/volume_server/grpc`
- Harness: `/Users/chris/dev/seaweedfs2/test/volume_server/framework`

## Progress Log
- Date: 2026-02-15
- Change: Created Rust volume-server crate (`weed-volume-rs`) as compatibility launcher and wired harness binary selection (`VOLUME_SERVER_IMPL`/`VOLUME_SERVER_BINARY`).
- Validation: Rust-mode conformance smoke execution pending CI and local subset runs.
