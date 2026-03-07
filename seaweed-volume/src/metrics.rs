//! Prometheus metrics for the volume server.
//!
//! Mirrors the Go SeaweedFS volume server metrics.

use prometheus::{
    self, Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    /// Request counter with label `type` = read | write | delete.
    pub static ref REQUEST_COUNTER: IntCounterVec = IntCounterVec::new(
        Opts::new("volume_server_request_counter", "Volume server request counter"),
        &["type"],
    ).expect("metric can be created");

    /// Request duration histogram with label `type` = read | write | delete.
    pub static ref REQUEST_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new("volume_server_request_duration", "Volume server request duration in seconds"),
        &["type"],
    ).expect("metric can be created");

    /// Total number of volumes on this server.
    pub static ref VOLUMES_TOTAL: IntGauge = IntGauge::new(
        "volume_server_volumes_total",
        "Total number of volumes",
    ).expect("metric can be created");

    /// Maximum number of volumes this server can hold.
    pub static ref MAX_VOLUMES: IntGauge = IntGauge::new(
        "volume_server_max_volumes",
        "Maximum number of volumes",
    ).expect("metric can be created");

    /// Disk size in bytes per directory.
    pub static ref DISK_SIZE_BYTES: IntGaugeVec = IntGaugeVec::new(
        Opts::new("volume_server_disk_size_bytes", "Disk size in bytes"),
        &["dir"],
    ).expect("metric can be created");

    /// Disk free bytes per directory.
    pub static ref DISK_FREE_BYTES: IntGaugeVec = IntGaugeVec::new(
        Opts::new("volume_server_disk_free_bytes", "Disk free space in bytes"),
        &["dir"],
    ).expect("metric can be created");

    /// Current number of in-flight requests.
    pub static ref INFLIGHT_REQUESTS: IntGauge = IntGauge::new(
        "volume_server_inflight_requests",
        "Current number of in-flight requests",
    ).expect("metric can be created");

    /// Total number of files stored across all volumes.
    pub static ref VOLUME_FILE_COUNT: IntGauge = IntGauge::new(
        "volume_server_volume_file_count",
        "Total number of files stored across all volumes",
    ).expect("metric can be created");
}

/// Register all metrics with the custom registry.
/// Call this once at startup.
pub fn register_metrics() {
    REGISTRY
        .register(Box::new(REQUEST_COUNTER.clone()))
        .expect("REQUEST_COUNTER registered");
    REGISTRY
        .register(Box::new(REQUEST_DURATION.clone()))
        .expect("REQUEST_DURATION registered");
    REGISTRY
        .register(Box::new(VOLUMES_TOTAL.clone()))
        .expect("VOLUMES_TOTAL registered");
    REGISTRY
        .register(Box::new(MAX_VOLUMES.clone()))
        .expect("MAX_VOLUMES registered");
    REGISTRY
        .register(Box::new(DISK_SIZE_BYTES.clone()))
        .expect("DISK_SIZE_BYTES registered");
    REGISTRY
        .register(Box::new(DISK_FREE_BYTES.clone()))
        .expect("DISK_FREE_BYTES registered");
    REGISTRY
        .register(Box::new(INFLIGHT_REQUESTS.clone()))
        .expect("INFLIGHT_REQUESTS registered");
    REGISTRY
        .register(Box::new(VOLUME_FILE_COUNT.clone()))
        .expect("VOLUME_FILE_COUNT registered");
}

/// Gather all metrics and encode them in Prometheus text exposition format.
pub fn gather_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder
        .encode(&metric_families, &mut buffer)
        .expect("encoding metrics");
    String::from_utf8(buffer).expect("metrics are valid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gather_metrics_returns_text() {
        register_metrics();
        REQUEST_COUNTER.with_label_values(&["read"]).inc();
        let output = gather_metrics();
        assert!(output.contains("volume_server_request_counter"));
    }
}
