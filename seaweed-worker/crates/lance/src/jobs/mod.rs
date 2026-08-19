//! One module per job type. Each declares its capability and the settings form
//! admin renders for it, then does the work.
//!
//! The bodies are stubs: doing the work means depending on the `lance` crate and
//! opening the dataset, which is the next step rather than this one. They report
//! failure honestly instead of claiming success.

pub mod cleanup;
pub mod compact;
pub mod indices;

use std::sync::Arc;

use seaweed_worker_core::JobHandler;

/// Every handler this worker serves. A worker process may serve several job
/// types, which is why WorkerHello carries a list.
pub fn handlers(namespace_url: String) -> Vec<Arc<dyn JobHandler>> {
    vec![
        Arc::new(compact::CompactHandler::new(namespace_url.clone())),
        Arc::new(indices::OptimizeIndicesHandler::new(namespace_url.clone())),
        Arc::new(cleanup::CleanupVersionsHandler::new(namespace_url)),
    ]
}
