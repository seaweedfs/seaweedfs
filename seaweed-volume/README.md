# SeaweedFS Volume Server (Rust)

A drop-in replacement for the [SeaweedFS](https://github.com/seaweedfs/seaweedfs) Go volume server, rewritten in Rust. It uses binary-compatible storage formats (`.dat`, `.idx`, `.vif`) and speaks the same HTTP and gRPC protocols, so it works with an unmodified Go master server.

## Building

Requires Rust 1.75+ (2021 edition).

```bash
cd seaweed-volume
cargo build --release
```

The binary is produced at `target/release/seaweed-volume`.

## Running

Start a Go master server first, then point the Rust volume server at it:

```bash
# Minimal
seaweed-volume --port 8080 --master localhost:9333 --dir /data/vol1 --max 7

# Multiple data directories
seaweed-volume --port 8080 --master localhost:9333 \
  --dir /mnt/ssd1,/mnt/ssd2 --max 100,100 --disk ssd

# With datacenter/rack topology
seaweed-volume --port 8080 --master localhost:9333 --dir /data/vol1 --max 7 \
  --dataCenter dc1 --rack rack1

# With JWT authentication
seaweed-volume --port 8080 --master localhost:9333 --dir /data/vol1 --max 7 \
  --securityFile /etc/seaweedfs/security.toml

# With TLS (configured in security.toml via [https.volume] and [grpc.volume] sections)
seaweed-volume --port 8080 --master localhost:9333 --dir /data/vol1 --max 7 \
  --securityFile /etc/seaweedfs/security.toml
```

### Common flags

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `8080` | HTTP listen port |
| `--port.grpc` | `port+10000` | gRPC listen port |
| `--master` | `localhost:9333` | Comma-separated master server addresses |
| `--dir` | `/tmp` | Comma-separated data directories |
| `--max` | `8` | Max volumes per directory (comma-separated) |
| `--ip` | auto-detect | Server IP / identifier |
| `--ip.bind` | same as `--ip` | Bind address |
| `--dataCenter` | | Datacenter name |
| `--rack` | | Rack name |
| `--disk` | | Disk type tag: `hdd`, `ssd`, or custom |
| `--index` | `memory` | Needle map type: `memory`, `leveldb`, `leveldbMedium`, `leveldbLarge` |
| `--readMode` | `proxy` | Non-local read mode: `local`, `proxy`, `redirect` |
| `--fileSizeLimitMB` | `256` | Max upload file size |
| `--minFreeSpace` | `1` (percent) | Min free disk space before marking volumes read-only |
| `--securityFile` | | Path to `security.toml` for JWT keys and TLS certs |
| `--metricsPort` | `0` (disabled) | Prometheus metrics endpoint port |
| `--whiteList` | | Comma-separated IPs with write permission |
| `--preStopSeconds` | `10` | Graceful drain period before shutdown |
| `--compactionMBps` | `0` (unlimited) | Compaction I/O rate limit |
| `--pprof` | `false` | Enable pprof HTTP handlers |

Set `RUST_LOG=debug` (or `trace`, `info`, `warn`) for log level control.
Set `SEAWEED_WRITE_QUEUE=1` to enable batched async write processing.

## Features

- **Binary compatible** -- reads and writes the same `.dat`/`.idx`/`.vif` files as the Go server; seamless migration with no data conversion.
- **HTTP + gRPC** -- full implementation of the volume server HTTP API and all gRPC RPCs including streaming operations (copy, tail, incremental copy, vacuum).
- **Master heartbeat** -- bidirectional streaming heartbeat with the Go master server; volume and EC shard registration, leader failover, graceful shutdown deregistration.
- **JWT authentication** -- signing key configuration via `security.toml` with token source precedence (query > header > cookie), file_id claims validation, and separate read/write keys.
- **TLS** -- HTTPS for the HTTP API and mTLS for gRPC, configured through `security.toml`.
- **Erasure coding** -- Reed-Solomon EC shard management: mount/unmount, read, rebuild, copy, delete, and shard-to-volume reconstruction.
- **S3 remote storage** -- `FetchAndWriteNeedle` reads from any S3-compatible backend (AWS, MinIO, Wasabi, Backblaze, etc.) and writes locally. Supports `VolumeTierMoveDatToRemote`/`FromRemote` for tiered storage.
- **Needle map backends** -- in-memory HashMap, LevelDB (via `rusty-leveldb`), or redb (pure Rust disk-backed) needle maps.
- **Image processing** -- on-the-fly resize/crop, JPEG EXIF orientation auto-fix, WebP support.
- **Streaming reads** -- large files (>1MB) are streamed via `spawn_blocking` to avoid blocking the async runtime.
- **Auto-compression** -- compressible file types (text, JSON, CSS, JS, SVG, etc.) are gzip-compressed on upload.
- **Prometheus metrics** -- counters, histograms, and gauges exported at a dedicated metrics port; optional push gateway support.
- **Graceful shutdown** -- SIGINT/SIGTERM handling with configurable `preStopSeconds` drain period.

## Testing

### Rust unit tests

```bash
cd seaweed-volume
cargo test
```

### Go integration tests

The Go test suite can target either the Go or Rust volume server via the `VOLUME_SERVER_IMPL` environment variable:

```bash
# Run all HTTP + gRPC integration tests against the Rust server
VOLUME_SERVER_IMPL=rust go test -v -count=1 -timeout 1200s \
  ./test/volume_server/grpc/... ./test/volume_server/http/...

# Run a single test
VOLUME_SERVER_IMPL=rust go test -v -count=1 -timeout 60s \
  -run "TestName" ./test/volume_server/http/...

# Run S3 remote storage tests
VOLUME_SERVER_IMPL=rust go test -v -count=1 -timeout 180s \
  -run "TestFetchAndWriteNeedle" ./test/volume_server/grpc/...
```

## Load testing

A load test harness is available at `test/volume_server/loadtest/`. See that directory for usage instructions and scenarios.

## Architecture

The server runs three listeners concurrently:

- **HTTP** (Axum 0.7) -- admin and public routers for file upload/download, status, and stats endpoints.
- **gRPC** (Tonic 0.12) -- all `VolumeServer` RPCs from the SeaweedFS protobuf definition.
- **Metrics** (optional) -- Prometheus scrape endpoint on a separate port.

Key source modules:

| Path | Description |
|------|-------------|
| `src/main.rs` | Entry point, server startup, signal handling |
| `src/config.rs` | CLI parsing and configuration resolution |
| `src/server/volume_server.rs` | HTTP router setup and middleware |
| `src/server/handlers.rs` | HTTP request handlers (read, write, delete, status) |
| `src/server/grpc_server.rs` | gRPC service implementation |
| `src/server/heartbeat.rs` | Master heartbeat loop |
| `src/storage/volume.rs` | Volume read/write/delete logic |
| `src/storage/needle.rs` | Needle (file entry) serialization |
| `src/storage/store.rs` | Multi-volume store management |
| `src/security.rs` | JWT validation and IP whitelist guard |
| `src/remote_storage/` | S3 remote storage backend |

See [DEV_PLAN.md](DEV_PLAN.md) for the full development history and feature checklist.
