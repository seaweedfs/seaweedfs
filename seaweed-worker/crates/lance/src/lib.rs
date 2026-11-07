//! Maintenance jobs for Lance tables.
//!
//! A Lance dataset needs three things done to it over time: its fragments
//! compacted, its indices extended to cover rows written after they were built,
//! and its old versions removed. None can run in the Go worker, because all
//! three read and rewrite Lance files. This crate is the worker that can.

pub mod catalog;
pub mod cluster;
pub mod dataset;
pub mod direct_read;
pub mod grpc_tls;
pub mod jobs;
pub mod metrics;
pub mod preview;
pub mod security;
pub mod sse;
pub mod tls;
pub mod vector_index;

pub use jobs::handlers;

/// The master's protocol, for the filers it lists.
pub mod master_pb {
    tonic::include_proto!("master_pb");
}
pub mod volume_server_pb {
    tonic::include_proto!("volume_server_pb");
}
pub mod remote_pb {
    tonic::include_proto!("remote_pb");
}
