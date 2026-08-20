//! What the Lance jobs reclaimed, in numbers a scrape can add up.
//!
//! The generic worker metrics say a job ran and how long it took. These say what
//! it did: a compaction sweep that runs every minute and removes nothing is a
//! different thing from one that never runs, and only these tell them apart.

use anyhow::Result;
use prometheus::IntCounter;
use seaweed_worker_core::Metrics;

/// Counters the Lance jobs raise, registered on the worker's own registry so
/// they are served from the same endpoint.
#[derive(Clone)]
pub struct LanceMetrics {
    /// The worker's own metrics, so a job can also record what it read and what
    /// it had to skip - the pair that tells "nothing needed doing" apart from
    /// "nothing could be read".
    pub worker: Metrics,
    pub fragments_removed: IntCounter,
    pub rows_indexed: IntCounter,
    pub versions_removed: IntCounter,
    pub bytes_reclaimed: IntCounter,
}

impl LanceMetrics {
    pub fn new(metrics: &Metrics) -> Result<Self> {
        Ok(Self {
            worker: metrics.clone(),
            fragments_removed: metrics.counter(
                "lance_fragments_removed_total",
                "Fragments merged away by compaction.",
            )?,
            rows_indexed: metrics.counter(
                "lance_rows_indexed_total",
                "Rows brought under an index that did not cover them.",
            )?,
            versions_removed: metrics.counter(
                "lance_versions_removed_total",
                "Dataset versions removed by cleanup.",
            )?,
            bytes_reclaimed: metrics
                .counter("lance_bytes_reclaimed_total", "Bytes freed by cleanup.")?,
        })
    }
}
