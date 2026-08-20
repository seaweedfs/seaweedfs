# SeaweedFS Rust workers

`weed/pb/plugin.proto` is a language-agnostic contract: a maintenance worker
connects out to admin, announces the job types it can detect and execute, and
answers requests on that one stream. `weed worker -admin=host:23646` is the Go
implementation of it from outside the admin process. This workspace is the Rust
one.

    crates/core     the contract: stream, handshake, heartbeat, registry, config forms
    crates/lance    maintenance jobs for Lance tables, and a binary

`core` knows nothing about any job. A second worker is a new crate beside
`lance` that depends on it, not a fork of the protocol.

## Running

    cargo run -p weed-lance-worker -- --admin 127.0.0.1:23646

The admin's *HTTP* address is what an operator has; the gRPC port is derived
from it the way the Go side does. Dialling the HTTP port fails as "frame with
invalid size", which reads like a protocol bug rather than a wrong port.

## Metrics

    cargo run -p weed-lance-worker -- --admin 127.0.0.1:23646 --metrics-port 9328

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

The worker holds none. It asks the namespace to describe a table with
`vend_credentials` and hands the `storage_options` that come back to lance. A
gateway without STS configured vends no credentials at all, so `--access-key`
and `--secret-key` supply a fallback; anything the namespace does vend wins over
them.

## State

All three jobs are implemented and tested end to end against a live gateway:

    compaction result: 12 fragments became 1
    reindex result:    512 uncovered rows became 0
    cleanup result:    removed 14 versions and 24272 bytes

`cargo test -p weed-lance-worker` runs them when `WEED_LANCE_NAMESPACE` names a
live namespace and skips otherwise, the way the Go integration tests skip
without Docker. Each test seeds the table it needs, including building a vector
index and then appending rows outside it, so a run does not depend on what the
previous one left behind — the first version of these did, and quietly stopped
testing anything once it had done its job.

The handshake, descriptor exchange and heartbeat work against a live admin,
which logs the worker connecting and prefetches all three descriptors.
