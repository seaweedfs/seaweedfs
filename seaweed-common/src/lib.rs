//! Helpers the SeaweedFS Rust volume server and the Rust plugin workers both need.
//!
//! `seaweed-volume` and `seaweed-worker` are separate cargo trees with separate
//! lockfiles and no root manifest, so anything both of them need was, until this
//! crate existed, written twice. The two things in here are the ones where a
//! second copy is a correctness risk rather than a typing cost: the HTTP↔gRPC
//! address rule, which two copies had already drifted on, and the process-wide
//! rustls provider, which only works if every binary installs the same one.

pub mod address;
pub mod tls;
