# SeaweedFS Rust workers

`weed/pb/plugin.proto` is a language-agnostic contract: a maintenance worker
connects out to admin, announces the job types it can detect and execute, and
answers requests on that one stream. `weed worker -admin=host:12535` is the Go
implementation of it from outside the admin process. This workspace is the Rust
one.

    crates/core     the contract: stream, handshake, heartbeat, registry, config forms
    crates/lance    maintenance jobs for Lance tables, and a binary

It also depends on `../seaweed-common`, a small crate outside this workspace
holding the few helpers the Rust volume server needs identically — the
HTTP<->gRPC address rule and the rustls provider choice.

`core` knows nothing about any job. A second worker is a new crate beside
`lance` that depends on it, not a fork of the protocol.

## Building

Requires Rust 1.94.1+ (2024 edition), matching `rust-version` in `Cargo.toml`.
The patch release matters: 1.94.0 does not build. The edition itself only needs
1.85; the higher floor comes from the dependency tree — lance's `aws` feature
pulls in the AWS SDK — so it moves with those crates. CI builds on the latest
stable.

`core` compiles `plugin.proto` with the protoc that protoc-bin-vendored ships,
the way seaweed-volume does, so it needs no system install.

The lance crates compile protos of their own, in their own build-script
processes, which nothing our build script sets can reach. They need a protoc of
their own: either one on PATH — `brew install protobuf`, `apt install
protobuf-compiler` — or `PROTOC` naming one. CI points it at the vendored
binary for the runner's platform, resolved from the version in `Cargo.lock`.

## Running

    cargo run -p weed-lance-worker -- --admin 127.0.0.1:12535 --masters 127.0.0.1:9333

The admin's *HTTP* address is what an operator has; the gRPC port is derived
from it the way the Go side does. Dialling the HTTP port fails as "frame with
invalid size", which reads like a protocol bug rather than a wrong port.

The binary is `weed-worker`, not `weed-lance-worker`: it is the Rust side of
`weed worker`, and lance is the first family of jobs it carries rather than the
only one it ever will.

Released builds do not need a toolchain. The worker ships inside the SeaweedFS
image, beside the Rust volume server, under the verb that mirrors
`volume-rust`:

    docker run chrislusf/seaweedfs worker-rust --admin admin:12535 --masters master:9333

and as `weed-worker_linux_{amd64,arm64}.tar.gz` on each GitHub release. Both are
linux amd64/arm64 only — lance, arrow and datafusion make every extra target an
expensive build, and the worker runs beside the cluster it maintains. On an
architecture without a build the image carries an empty placeholder and the
entrypoint says so rather than failing as "not found".

## Metrics

    cargo run -p weed-lance-worker -- --admin 127.0.0.1:12535 --masters 127.0.0.1:9333 --metrics-port 9328

Serves `/health`, `/ready` and `/metrics` on that port, the same three the Go
worker serves under `weed worker -metricsPort`, so one scrape config covers
workers in either language. Off by default, and bound to loopback unless
`--metrics-ip` says otherwise, because the endpoint is unauthenticated. 9328
continues the series the other components use (master 9324, volume 9325, filer
9326, s3 9327); an IPv6 address works with or without brackets.

Grafana: the "Plugin Workers" row of `other/metrics/grafana_seaweedfs.json`
graphs these. Its panels filter on `$cluster`, which comes from the scrape job's
labels, so scrape the worker the way the rest of the cluster is scraped or the
row stays empty.

Names are `SeaweedFS_worker_*`, matching the Go side's convention. The pair
worth alerting on is `objects_seen_total` and `objects_skipped_total`: a sweep
that proposes nothing and a sweep that could read nothing look identical from
`proposals_total` alone.

    SeaweedFS_worker_connected 1
    SeaweedFS_worker_objects_seen_total{job_type="lance_compact"} 7
    SeaweedFS_worker_proposals_total{job_type="lance_compact"} 2
    SeaweedFS_worker_jobs_total{job_type="lance_compact",result="ok"} 2
    SeaweedFS_worker_lance_fragments_removed_total 25

`/ready` follows the control stream: a worker whose admin has gone away is
running but is not going to do anything.

## Credentials

The worker holds no S3 credentials. The namespace tells it where a table is;
its files are read and written on the cluster's filers and volume servers,
which `--masters` names the masters of (`--filer-group` picks the filers). What
that takes comes from the same security.toml the weed processes use:
`grpc.client` for the filers, `https.client` for volume servers under HTTPS,
`jwt.signing.read.key` where volume reads are signed, and `s3.sse.kek` or
`s3.sse.key` where a bucket encrypts with SSE-S3. Chunks are encrypted for the
volume servers when the filer is (`weed filer -encryptVolumeData`), as the S3
gateway's are.

The namespace authenticates the worker by the cluster token it derives from
`jwt.filer_signing.key`; the same key signs the admin stream when admin requires
it. The worker reads it from where `weed worker` does: `WEED_JWT_FILER_SIGNING_KEY`,
or `[jwt.filer_signing]` in the security.toml found in the working directory,
`~/.seaweedfs`, `/usr/local/etc/seaweedfs` or `/etc/seaweedfs`.
`--filer-signing-key` overrides both.

## State

All three jobs are implemented and tested end to end against a live gateway:

    compaction result: 12 fragments became 1
    reindex result:    512 uncovered rows became 0
    cleanup result:    removed 14 versions and 24272 bytes

`cargo test -p weed-lance-worker` runs them when `WEED_LANCE_NAMESPACE` names a
live namespace and skips otherwise, the way the Go integration tests skip
without Docker. A live namespace needs `WEED_MASTERS` naming its cluster's
masters, and `WEED_JWT_FILER_SIGNING_KEY` (or security.toml) where the
namespace authenticates cluster processes; the tests fail without the first
and are refused without the second. Each test seeds the table it needs, including building a vector
index and then appending rows outside it, so a run does not depend on what the
previous one left behind — the first version of these did, and quietly stopped
testing anything once it had done its job.

The handshake, descriptor exchange and heartbeat work against a live admin,
which logs the worker connecting and prefetches all three descriptors.
