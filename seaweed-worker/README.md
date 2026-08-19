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

## State

The handshake, descriptor exchange and heartbeat work: admin logs the worker
connecting and prefetches all three descriptors, so the job settings pages
render from the Rust side. The job bodies are stubs. Doing the work means adding
the `lance` crate and opening the dataset, which is the next step; until then
they report failure rather than claiming success.
