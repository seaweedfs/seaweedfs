# Rust Volume Server Parity Plan

Generated: 2026-03-08

## Goal

Make `seaweed-volume` a drop-in replacement for the Go volume server by:

- comparing every Go volume-server code path against the Rust implementation,
- recording file-level ownership and verification status,
- closing verified behavior gaps one logic change per commit,
- extending tests so regressions are caught by Go parity suites and Rust unit/integration tests.

## Ground Truth

Primary Go sources:

- `weed/server/volume_server.go`
- `weed/server/volume_server_handlers*.go`
- `weed/server/volume_grpc_*.go`
- `weed/server/constants/volume.go`
- `weed/storage/store*.go`
- `weed/storage/disk_location*.go`
- `weed/storage/volume*.go`
- `weed/storage/needle/*.go`
- `weed/storage/idx/*.go`
- `weed/storage/needle_map*.go`
- `weed/storage/needle_map/*.go`
- `weed/storage/super_block/*.go`
- `weed/storage/erasure_coding/*.go`

Supporting Go dependencies that affect drop-in behavior:

- `weed/command/volume.go`
- `weed/security/*.go`
- `weed/images/*.go`
- `weed/stats/*.go`

Primary Rust sources:

- `seaweed-volume/src/main.rs`
- `seaweed-volume/src/config.rs`
- `seaweed-volume/src/security.rs`
- `seaweed-volume/src/images.rs`
- `seaweed-volume/src/server/*.rs`
- `seaweed-volume/src/storage/*.rs`
- `seaweed-volume/src/storage/needle/*.rs`
- `seaweed-volume/src/storage/idx/*.rs`
- `seaweed-volume/src/storage/erasure_coding/*.rs`
- `seaweed-volume/src/remote_storage/*.rs`

## Audit Method

For each Go file:

1. Map it to the Rust file or files that should own the same behavior.
2. Compare exported entry points, helper functions, state transitions, wire fields, and persistence side effects.
3. Mark each file `implemented`, `partial`, `missing`, or `needs verification`.
4. Link each behavior to an existing test or add a missing test.
5. Only treat a gap as closed after code review plus local verification.

## Acceptance Criteria

The Rust server is a drop-in replacement only when all of these hold:

- HTTP routes, status codes, headers, and body semantics match Go.
- gRPC RPCs match Go request validation, response fields, streaming behavior, and maintenance/read-only semantics.
- Master heartbeat and topology metadata match Go closely enough that the Go master treats Rust and Go volume servers the same.
- On-disk volume behavior matches Go for normal volumes, EC shards, tiering metadata, and readonly persistence.
- Startup flags and operational endpoints that affect production deployment behave equivalently or are explicitly documented as unsupported.
- Existing Go integration suites pass with `VOLUME_SERVER_IMPL=rust`.

## File Matrix

### HTTP server surface

| Go file | Rust counterpart | Status | Comparison focus |
| --- | --- | --- | --- |
| `weed/server/volume_server.go` | `seaweed-volume/src/main.rs`, `seaweed-volume/src/server/volume_server.rs`, `seaweed-volume/src/server/heartbeat.rs` | partial | startup wiring, routers, heartbeat, shutdown, metrics/debug listeners |
| `weed/server/volume_server_handlers.go` | `seaweed-volume/src/server/volume_server.rs`, `seaweed-volume/src/server/handlers.rs` | needs verification | method dispatch, OPTIONS behavior, public/admin split |
| `weed/server/volume_server_handlers_admin.go` | `seaweed-volume/src/server/handlers.rs` | implemented | `/status`, `/healthz`, stats, server headers |
| `weed/server/volume_server_handlers_helper.go` | `seaweed-volume/src/server/handlers.rs` | needs verification | JSON encoding, request parsing, helper parity |
| `weed/server/volume_server_handlers_read.go` | `seaweed-volume/src/server/handlers.rs` | needs verification | JWT, conditional reads, range reads, proxy/redirect, chunk manifests, image transforms |
| `weed/server/volume_server_handlers_ui.go` | `seaweed-volume/src/server/handlers.rs`, embedded assets | partial | UI payload and HTML parity |
| `weed/server/volume_server_handlers_write.go` | `seaweed-volume/src/server/handlers.rs`, `seaweed-volume/src/images.rs` | needs verification | multipart parsing, metadata, compression, ts, delete semantics |
| `weed/server/constants/volume.go` | `seaweed-volume/src/server/heartbeat.rs`, config defaults | needs verification | heartbeat timing, constants parity |

### gRPC server surface

| Go file | Rust counterpart | Status | Comparison focus |
| --- | --- | --- | --- |
| `weed/server/volume_grpc_admin.go` | `seaweed-volume/src/server/grpc_server.rs` | needs verification | readonly/writable, allocate/delete/configure/mount/unmount |
| `weed/server/volume_grpc_batch_delete.go` | `seaweed-volume/src/server/grpc_server.rs` | implemented | batch delete, EC delete path |
| `weed/server/volume_grpc_client_to_master.go` | `seaweed-volume/src/server/heartbeat.rs` | partial | heartbeat fields, leader changes, metrics settings from master |
| `weed/server/volume_grpc_copy.go` | `seaweed-volume/src/server/grpc_server.rs` | needs verification | full copy streams |
| `weed/server/volume_grpc_copy_incremental.go` | `seaweed-volume/src/server/grpc_server.rs` | needs verification | incremental copy binary search, timestamps |
| `weed/server/volume_grpc_erasure_coding.go` | `seaweed-volume/src/server/grpc_server.rs`, `seaweed-volume/src/storage/erasure_coding/*.rs` | needs verification | shard read/write/delete/mount/unmount/rebuild |
| `weed/server/volume_grpc_query.go` | `seaweed-volume/src/server/grpc_server.rs` | needs verification | query validation and error parity |
| `weed/server/volume_grpc_read_all.go` | `seaweed-volume/src/server/grpc_server.rs` | needs verification | read-all ordering and tail semantics |
| `weed/server/volume_grpc_read_write.go` | `seaweed-volume/src/server/grpc_server.rs`, `seaweed-volume/src/storage/*.rs` | needs verification | blob/meta/page reads, write blob semantics |
| `weed/server/volume_grpc_remote.go` | `seaweed-volume/src/server/grpc_server.rs`, `seaweed-volume/src/remote_storage/*.rs` | needs verification | remote fetch/write and tier metadata |
| `weed/server/volume_grpc_scrub.go` | `seaweed-volume/src/server/grpc_server.rs`, `seaweed-volume/src/storage/*.rs` | needs verification | scrub result semantics |
| `weed/server/volume_grpc_state.go` | `seaweed-volume/src/server/grpc_server.rs` | implemented | GetState/SetState/Status |
| `weed/server/volume_grpc_tail.go` | `seaweed-volume/src/server/grpc_server.rs` | needs verification | tail streaming and idle timeout |
| `weed/server/volume_grpc_tier_download.go` | `seaweed-volume/src/server/grpc_server.rs`, `seaweed-volume/src/remote_storage/*.rs` | needs verification | tier download stream/error paths |
| `weed/server/volume_grpc_tier_upload.go` | `seaweed-volume/src/server/grpc_server.rs`, `seaweed-volume/src/remote_storage/*.rs` | needs verification | tier upload stream/error paths |
| `weed/server/volume_grpc_vacuum.go` | `seaweed-volume/src/server/grpc_server.rs`, `seaweed-volume/src/storage/*.rs` | needs verification | compact/commit/cleanup progress and readonly transitions |

### Storage and persistence surface

| Go file group | Rust counterpart | Status | Comparison focus |
| --- | --- | --- | --- |
| `weed/storage/store.go`, `store_state.go` | `seaweed-volume/src/storage/store.rs`, `seaweed-volume/src/server/heartbeat.rs` | partial | topology metadata, disk tags, server id, state persistence |
| `weed/storage/store_vacuum.go` | `seaweed-volume/src/storage/store.rs`, `seaweed-volume/src/storage/volume.rs` | needs verification | vacuum sequencing |
| `weed/storage/store_ec.go`, `store_ec_delete.go`, `store_ec_scrub.go` | `seaweed-volume/src/storage/store.rs`, `seaweed-volume/src/storage/erasure_coding/*.rs` | needs verification | EC lifecycle and scrub behavior |
| `weed/storage/disk_location.go`, `disk_location_ec.go` | `seaweed-volume/src/storage/disk_location.rs`, `seaweed-volume/src/storage/store.rs` | partial | directory UUIDs, tags, load rules, disk space checks |
| `weed/storage/volume.go`, `volume_loading.go` | `seaweed-volume/src/storage/volume.rs` | needs verification | load/reload/readonly/remote metadata |
| `weed/storage/volume_super_block.go` | `seaweed-volume/src/storage/super_block.rs`, `seaweed-volume/src/storage/volume.rs` | implemented | super block parity |
| `weed/storage/volume_read.go`, `volume_read_all.go` | `seaweed-volume/src/storage/volume.rs`, `seaweed-volume/src/server/handlers.rs` | needs verification | full/meta/page reads, TTL, streaming |
| `weed/storage/volume_write.go` | `seaweed-volume/src/storage/volume.rs`, `seaweed-volume/src/server/write_queue.rs` | needs verification | dedup, sync/async writes, metadata flags |
| `weed/storage/volume_vacuum.go` | `seaweed-volume/src/storage/volume.rs` | needs verification | compact and commit parity |
| `weed/storage/volume_backup.go` | `seaweed-volume/src/storage/volume.rs`, `seaweed-volume/src/server/grpc_server.rs` | needs verification | backup/search logic |
| `weed/storage/volume_checking.go` | `seaweed-volume/src/storage/volume.rs`, `seaweed-volume/src/storage/idx/mod.rs`, `seaweed-volume/src/server/grpc_server.rs` | needs verification | scrub and integrity checks |
| `weed/storage/volume_info.go`, `volume_info/volume_info.go`, `volume_tier.go` | `seaweed-volume/src/storage/volume.rs`, `seaweed-volume/src/remote_storage/*.rs` | needs verification | `.vif` format and tiered file metadata |
| `weed/storage/needle/*.go` | `seaweed-volume/src/storage/needle/*.rs` | needs verification | needle parsing, CRC, TTL, multipart metadata |
| `weed/storage/idx/*.go` | `seaweed-volume/src/storage/idx/*.rs` | needs verification | index walking and binary search |
| `weed/storage/needle_map*.go`, `needle_map/*.go` | `seaweed-volume/src/storage/needle_map.rs` | needs verification | map kind parity, persistence, memory behavior |
| `weed/storage/super_block/*.go` | `seaweed-volume/src/storage/super_block.rs` | implemented | replica placement and TTL metadata |
| `weed/storage/erasure_coding/*.go` | `seaweed-volume/src/storage/erasure_coding/*.rs` | needs verification | EC shard placement, encode/decode, journal deletes |

### Supporting runtime surface

| Go file | Rust counterpart | Status | Comparison focus |
| --- | --- | --- | --- |
| `weed/command/volume.go` | `seaweed-volume/src/config.rs`, `seaweed-volume/src/main.rs` | partial | flags, metrics/debug listeners, startup behavior |
| `weed/security/*.go` | `seaweed-volume/src/security.rs`, `seaweed-volume/src/main.rs` | implemented | JWT and TLS loading |
| `weed/images/*.go` | `seaweed-volume/src/images.rs`, `seaweed-volume/src/server/handlers.rs` | implemented | JPEG orientation and transforms |
| `weed/stats/*.go` | `seaweed-volume/src/metrics.rs`, `seaweed-volume/src/server/handlers.rs` | partial | metrics endpoints, push-gateway integration |

## Verified Gaps As Of 2026-03-08

These are real code gaps found in the current Rust source, not stale items from older docs.

1. Heartbeat metadata parity
   Go sends stable server `Id`, `LocationUuids`, per-disk `DiskTags`, and correct `HasNoVolumes`.
   Rust currently omits those fields, even though `--id` and `--tags` are parsed.
   Risk: Go master may not treat Rust volume servers identically for identity, placement, and duplicate-directory detection.

2. Dedicated metrics/debug listener parity
   Go honors `--metricsPort`, `--metricsIp`, `--pprof`, and `--debug`.
   Rust parses these flags but currently serves metrics only on the admin router and does not expose equivalent debug listeners.
   Risk: deployment scripts that rely on dedicated metrics or profiling ports will not behave the same.

3. Master-provided metrics push settings
   Go updates `metricsAddress` and `metricsIntervalSec` from master heartbeat responses and pushes metrics.
   Rust does not consume those fields.
   Risk: Prometheus push-gateway setups configured via master are silently ignored.

4. Slow-read tuning parity
   Go uses `hasSlowRead` and `readBufferSizeMB` in read paths and storage locking behavior.
   Rust accepts both flags but the values are not currently wired into the read implementation.
   Risk: production latency and lock behavior can diverge under large-file read pressure.

## Execution Plan

### Batch 1: startup and heartbeat

- Compare `weed/command/volume.go`, `weed/server/volume_server.go`, `weed/server/volume_grpc_client_to_master.go`, `weed/storage/store.go`, and `weed/storage/disk_location.go`.
- Close metadata and startup parity gaps that affect master registration and deployment compatibility.
- Add Rust unit tests for heartbeat payloads and config wiring.

### Batch 2: HTTP read path

- Compare `volume_server_handlers_read.go`, `volume_server_handlers_helper.go`, and related storage read functions line by line.
- Verify JWT, path parsing, proxy/redirect, ranges, streaming, chunk manifests, image transforms, and response-header overrides.
- Extend `test/volume_server/http/...` and Rust handler tests where parity is not covered.

### Batch 3: HTTP write/delete path

- Compare `volume_server_handlers_write.go` and write-related storage functions.
- Verify multipart behavior, metadata, md5, compression, unchanged writes, delete edge cases, and timestamp handling.

### Batch 4: gRPC admin and lifecycle

- Compare `volume_grpc_admin.go`, `volume_grpc_state.go`, and `volume_grpc_vacuum.go`.
- Verify readonly/writable flows, maintenance mode, status payloads, mount/unmount/delete/configure, and vacuum transitions.

### Batch 5: gRPC data movement

- Compare `volume_grpc_read_write.go`, `copy*.go`, `read_all.go`, `tail.go`, `remote.go`, and `query.go`.
- Verify stream framing, binary search, idle timeout, and remote-storage semantics.

### Batch 6: storage internals

- Compare all `weed/storage` volume, needle, idx, needle map, and EC files line by line.
- Focus on persistence rules, readonly semantics, TTL, recovery/scrub, backup, and memory/disk map behavior.

## Commit Strategy

- One commit for the audit/plan document if the document itself changes.
- One commit per logic fix.
- Every logic commit must include the smallest test addition that proves the new parity claim.
