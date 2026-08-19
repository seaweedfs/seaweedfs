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

## Credentials

The worker holds none. It asks the namespace to describe a table with
`vend_credentials` and hands the `storage_options` that come back to lance. A
gateway without STS configured vends no credentials at all, so `--access-key`
and `--secret-key` supply a fallback; anything the namespace does vend wins over
them.

## State

`lance_compact` is implemented and tested end to end: a twelve-fragment dataset
on SeaweedFS became one fragment with all twelve rows intact. `cargo test -p
weed-lance-worker` runs it when `WEED_LANCE_NAMESPACE` names a live namespace
and skips otherwise, the way the Go integration tests skip without Docker.

`lance_optimize_indices` and `lance_cleanup_versions` still report failure
rather than claiming success. The handshake, descriptor exchange and heartbeat
work against a live admin, which logs the worker connecting and prefetches all
three descriptors.
