//! The SeaweedFS plugin worker contract, in Rust.
//!
//! `weed/pb/plugin.proto` is a language-agnostic gRPC stream: a worker connects
//! out to admin, announces the job types it can detect and execute, and then
//! answers requests on that one stream. `weed worker -admin=host:23646` is the
//! Go implementation of the same contract from outside the admin process; this
//! is the Rust one, and it knows nothing about any particular job.

pub mod address;
pub mod config;
pub mod config_form;
pub mod metrics;
pub mod registry;
pub mod senders;
pub mod stream;

/// Generated plugin.proto types.
pub mod pb {
    tonic::include_proto!("plugin");
}

pub use config::{TlsOptions, WorkerOptions};
pub use metrics::Metrics;
pub use registry::{JobHandler, Preview, PreviewProvider, Registry};
pub use senders::{DetectionSender, ExecutionSender};
pub use stream::run;
