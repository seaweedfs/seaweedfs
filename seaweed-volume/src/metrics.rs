//! Prometheus metrics for the volume server.
//!
//! Mirrors the Go SeaweedFS volume server metrics.

use prometheus::{
    self, Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec,
    Opts, Registry, TextEncoder,
};
use std::sync::Once;

use crate::version;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PushGatewayConfig {
    pub address: String,
    pub interval_seconds: u32,
}

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // ---- Request metrics (Go: VolumeServerRequestCounter, VolumeServerRequestHistogram) ----

    /// Request counter with labels `type` (HTTP method) and `code` (HTTP status).
    pub static ref REQUEST_COUNTER: IntCounterVec = IntCounterVec::new(
        Opts::new("SeaweedFS_volumeServer_request_total", "Volume server requests"),
        &["type", "code"],
    ).expect("metric can be created");

    /// Request duration histogram with label `type` (HTTP method).
    pub static ref REQUEST_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "SeaweedFS_volumeServer_request_seconds",
            "Volume server request duration in seconds",
        ).buckets(exponential_buckets(0.0001, 2.0, 24)),
        &["type"],
    ).expect("metric can be created");

    // ---- Handler counters (Go: VolumeServerHandlerCounter) ----

    /// Handler-level operation counter with label `type`.
    pub static ref HANDLER_COUNTER: IntCounterVec = IntCounterVec::new(
        Opts::new("SeaweedFS_volumeServer_handler_total", "Volume server handler counters"),
        &["type"],
    ).expect("metric can be created");

    // ---- Vacuuming metrics (Go: VolumeServerVacuuming*) ----

    /// Vacuuming compact counter with label `success` (true/false).
    pub static ref VACUUMING_COMPACT_COUNTER: IntCounterVec = IntCounterVec::new(
        Opts::new("SeaweedFS_volumeServer_vacuuming_compact_count", "Counter of volume vacuuming Compact counter"),
        &["success"],
    ).expect("metric can be created");

    /// Vacuuming commit counter with label `success` (true/false).
    pub static ref VACUUMING_COMMIT_COUNTER: IntCounterVec = IntCounterVec::new(
        Opts::new("SeaweedFS_volumeServer_vacuuming_commit_count", "Counter of volume vacuuming commit counter"),
        &["success"],
    ).expect("metric can be created");

    /// Vacuuming duration histogram with label `type` (compact/commit).
    pub static ref VACUUMING_HISTOGRAM: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "SeaweedFS_volumeServer_vacuuming_seconds",
            "Volume vacuuming duration in seconds",
        ).buckets(exponential_buckets(0.0001, 2.0, 24)),
        &["type"],
    ).expect("metric can be created");

    // ---- Volume gauges (Go: VolumeServerVolumeGauge, VolumeServerReadOnlyVolumeGauge) ----

    /// Volumes per collection and type (volume/ec_shards).
    pub static ref VOLUME_GAUGE: GaugeVec = GaugeVec::new(
        Opts::new("SeaweedFS_volumeServer_volumes", "Number of volumes"),
        &["collection", "type"],
    ).expect("metric can be created");

    /// Read-only volumes per collection and type.
    pub static ref READ_ONLY_VOLUME_GAUGE: GaugeVec = GaugeVec::new(
        Opts::new("SeaweedFS_volumeServer_read_only_volumes", "Number of read-only volumes."),
        &["collection", "type"],
    ).expect("metric can be created");

    /// Maximum number of volumes this server can hold.
    pub static ref MAX_VOLUMES: IntGauge = IntGauge::new(
        "SeaweedFS_volumeServer_max_volumes",
        "Maximum number of volumes",
    ).expect("metric can be created");

    // ---- Disk size gauges (Go: VolumeServerDiskSizeGauge) ----

    /// Actual disk size used by volumes per collection and type (normal/deleted_bytes/ec).
    pub static ref DISK_SIZE_GAUGE: GaugeVec = GaugeVec::new(
        Opts::new("SeaweedFS_volumeServer_total_disk_size", "Actual disk size used by volumes"),
        &["collection", "type"],
    ).expect("metric can be created");

    // ---- Resource gauges (Go: VolumeServerResourceGauge) ----

    /// Disk resource usage per directory and type (all/used/free/avail).
    pub static ref RESOURCE_GAUGE: GaugeVec = GaugeVec::new(
        Opts::new("SeaweedFS_volumeServer_resource", "Server resource usage"),
        &["name", "type"],
    ).expect("metric can be created");

    // ---- In-flight gauges (Go: VolumeServerInFlightRequestsGauge, InFlightDownload/UploadSize) ----

    /// In-flight requests per HTTP method.
    pub static ref INFLIGHT_REQUESTS_GAUGE: IntGaugeVec = IntGaugeVec::new(
        Opts::new("SeaweedFS_volumeServer_in_flight_requests", "Current number of in-flight requests being handled by volume server."),
        &["type"],
    ).expect("metric can be created");

    /// Concurrent download limit in bytes.
    pub static ref CONCURRENT_DOWNLOAD_LIMIT: IntGauge = IntGauge::new(
        "SeaweedFS_volumeServer_concurrent_download_limit",
        "Limit for total concurrent download size in bytes",
    ).expect("metric can be created");

    /// Concurrent upload limit in bytes.
    pub static ref CONCURRENT_UPLOAD_LIMIT: IntGauge = IntGauge::new(
        "SeaweedFS_volumeServer_concurrent_upload_limit",
        "Limit for total concurrent upload size in bytes",
    ).expect("metric can be created");

    /// Current in-flight download bytes.
    pub static ref INFLIGHT_DOWNLOAD_SIZE: IntGauge = IntGauge::new(
        "SeaweedFS_volumeServer_in_flight_download_size",
        "In flight total download size.",
    ).expect("metric can be created");

    /// Current in-flight upload bytes.
    pub static ref INFLIGHT_UPLOAD_SIZE: IntGauge = IntGauge::new(
        "SeaweedFS_volumeServer_in_flight_upload_size",
        "In flight total upload size.",
    ).expect("metric can be created");

    /// Upload error counter by HTTP status code. Code "0" = transport error (no response).
    pub static ref UPLOAD_ERROR_COUNTER: IntCounterVec = IntCounterVec::new(
        Opts::new("SeaweedFS_upload_error_total",
            "Counter of upload errors by HTTP status code. Code 0 means transport error (no response received)."),
        &["code"],
    ).expect("metric can be created");

    // ---- Legacy aliases for backward compat with existing code ----

    /// Total number of volumes on this server (flat gauge).
    pub static ref VOLUMES_TOTAL: IntGauge = IntGauge::new(
        "volume_server_volumes_total",
        "Total number of volumes",
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

    /// Current number of in-flight requests (flat gauge).
    pub static ref INFLIGHT_REQUESTS: IntGauge = IntGauge::new(
        "volume_server_inflight_requests",
        "Current number of in-flight requests",
    ).expect("metric can be created");

    /// Total number of files stored across all volumes.
    pub static ref VOLUME_FILE_COUNT: IntGauge = IntGauge::new(
        "volume_server_volume_file_count",
        "Total number of files stored across all volumes",
    ).expect("metric can be created");

    // ---- Build info (Go: BuildInfo) ----

    /// Build information gauge, always set to 1. Matches Go:
    /// Namespace="SeaweedFS", Subsystem="build", Name="info",
    /// labels: version, commit, sizelimit, goos, goarch.
    pub static ref BUILD_INFO: GaugeVec = GaugeVec::new(
        Opts::new("SeaweedFS_build_info", "A metric with a constant '1' value labeled by version, commit, sizelimit, goos, and goarch from which SeaweedFS was built."),
        &["version", "commit", "sizelimit", "goos", "goarch"],
    ).expect("metric can be created");
}

/// Generate exponential bucket boundaries for histograms.
fn exponential_buckets(start: f64, factor: f64, count: usize) -> Vec<f64> {
    let mut buckets = Vec::with_capacity(count);
    let mut val = start;
    for _ in 0..count {
        buckets.push(val);
        val *= factor;
    }
    buckets
}

// Handler counter type constants (matches Go's metrics_names.go).
pub const WRITE_TO_LOCAL_DISK: &str = "writeToLocalDisk";
pub const WRITE_TO_REPLICAS: &str = "writeToReplicas";
pub const DOWNLOAD_LIMIT_COND: &str = "downloadLimitCondition";
pub const UPLOAD_LIMIT_COND: &str = "uploadLimitCondition";
pub const READ_PROXY_REQ: &str = "readProxyRequest";
pub const READ_REDIRECT_REQ: &str = "readRedirectRequest";
pub const EMPTY_READ_PROXY_LOC: &str = "emptyReadProxyLocaction";
pub const FAILED_READ_PROXY_REQ: &str = "failedReadProxyRequest";

// Error metric name constants.
pub const ERROR_SIZE_MISMATCH_OFFSET_SIZE: &str = "errorSizeMismatchOffsetSize";
pub const ERROR_SIZE_MISMATCH: &str = "errorSizeMismatch";
pub const ERROR_CRC: &str = "errorCRC";
pub const ERROR_INDEX_OUT_OF_RANGE: &str = "errorIndexOutOfRange";
pub const ERROR_GET_NOT_FOUND: &str = "errorGetNotFound";
pub const ERROR_GET_INTERNAL: &str = "errorGetInternal";
pub const ERROR_WRITE_TO_LOCAL_DISK: &str = "errorWriteToLocalDisk";
pub const ERROR_UNMARSHAL_PAIRS: &str = "errorUnmarshalPairs";
pub const ERROR_WRITE_TO_REPLICAS: &str = "errorWriteToReplicas";

// Go volume heartbeat metric label values.
pub const READ_ONLY_LABEL_IS_READ_ONLY: &str = "IsReadOnly";
pub const READ_ONLY_LABEL_NO_WRITE_OR_DELETE: &str = "noWriteOrDelete";
pub const READ_ONLY_LABEL_NO_WRITE_CAN_DELETE: &str = "noWriteCanDelete";
pub const READ_ONLY_LABEL_IS_DISK_SPACE_LOW: &str = "isDiskSpaceLow";
pub const DISK_SIZE_LABEL_NORMAL: &str = "normal";
pub const DISK_SIZE_LABEL_DELETED_BYTES: &str = "deleted_bytes";
pub const DISK_SIZE_LABEL_EC: &str = "ec";

static REGISTER_METRICS: Once = Once::new();

/// Register all metrics with the custom registry.
/// Call this once at startup.
pub fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        let metrics: Vec<Box<dyn prometheus::core::Collector>> = vec![
            // New Go-compatible metrics
            Box::new(REQUEST_COUNTER.clone()),
            Box::new(REQUEST_DURATION.clone()),
            Box::new(HANDLER_COUNTER.clone()),
            Box::new(VACUUMING_COMPACT_COUNTER.clone()),
            Box::new(VACUUMING_COMMIT_COUNTER.clone()),
            Box::new(VACUUMING_HISTOGRAM.clone()),
            Box::new(VOLUME_GAUGE.clone()),
            Box::new(READ_ONLY_VOLUME_GAUGE.clone()),
            Box::new(MAX_VOLUMES.clone()),
            Box::new(DISK_SIZE_GAUGE.clone()),
            Box::new(RESOURCE_GAUGE.clone()),
            Box::new(INFLIGHT_REQUESTS_GAUGE.clone()),
            Box::new(CONCURRENT_DOWNLOAD_LIMIT.clone()),
            Box::new(CONCURRENT_UPLOAD_LIMIT.clone()),
            Box::new(INFLIGHT_DOWNLOAD_SIZE.clone()),
            Box::new(INFLIGHT_UPLOAD_SIZE.clone()),
            Box::new(UPLOAD_ERROR_COUNTER.clone()),
            // Legacy metrics
            Box::new(VOLUMES_TOTAL.clone()),
            Box::new(DISK_SIZE_BYTES.clone()),
            Box::new(DISK_FREE_BYTES.clone()),
            Box::new(INFLIGHT_REQUESTS.clone()),
            Box::new(VOLUME_FILE_COUNT.clone()),
            // Build info
            Box::new(BUILD_INFO.clone()),
        ];
        for m in metrics {
            REGISTRY.register(m).expect("metric registered");
        }

        // Set build info gauge to 1 with version/commit/sizelimit/os/arch labels (matches Go).
        BUILD_INFO
            .with_label_values(&[
                version::version(),
                version::commit(),
                version::size_limit(),
                std::env::consts::OS,
                std::env::consts::ARCH,
            ])
            .set(1.0);
    });
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

pub fn delete_collection_metrics(collection: &str) {
    // Mirrors Go's DeletePartialMatch(prometheus.Labels{"collection": collection})
    // which removes ALL metric entries matching the collection label, regardless
    // of other label values (like "type"). We gather the metric families to discover
    // all type values dynamically, matching Go's partial-match behavior.
    delete_partial_match_collection(&VOLUME_GAUGE, collection);
    delete_partial_match_collection(&READ_ONLY_VOLUME_GAUGE, collection);
    delete_partial_match_collection(&DISK_SIZE_GAUGE, collection);
}

/// Remove all metric entries from a GaugeVec where the "collection" label matches.
/// This emulates Go's `DeletePartialMatch(prometheus.Labels{"collection": collection})`.
fn delete_partial_match_collection(gauge: &GaugeVec, collection: &str) {
    use prometheus::core::Collector;
    let families = gauge.collect();
    for family in &families {
        for metric in family.get_metric() {
            let labels = metric.get_label();
            let mut matches_collection = false;
            let mut type_value = None;
            for label in labels {
                if label.get_name() == "collection" && label.get_value() == collection {
                    matches_collection = true;
                }
                if label.get_name() == "type" {
                    type_value = Some(label.get_value().to_string());
                }
            }
            if matches_collection {
                if let Some(ref tv) = type_value {
                    let _ = gauge.remove_label_values(&[collection, tv]);
                }
            }
        }
    }
}

pub fn build_pushgateway_url(address: &str, job: &str, instance: &str) -> String {
    let base = if address.starts_with("http://") || address.starts_with("https://") {
        address.to_string()
    } else {
        format!("http://{}", address)
    };
    let base = base.trim_end_matches('/');
    format!("{}/metrics/job/{}/instance/{}", base, job, instance)
}

pub async fn push_metrics_once(
    client: &reqwest::Client,
    address: &str,
    job: &str,
    instance: &str,
) -> Result<(), String> {
    let url = build_pushgateway_url(address, job, instance);
    let response = client
        .put(&url)
        .header(
            reqwest::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )
        .body(gather_metrics())
        .send()
        .await
        .map_err(|e| format!("push metrics request failed: {}", e))?;

    if response.status().is_success() {
        Ok(())
    } else {
        Err(format!(
            "push metrics failed with status {}",
            response.status()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::put, Router};
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_gather_metrics_returns_text() {
        register_metrics();
        REQUEST_COUNTER.with_label_values(&["GET", "200"]).inc();
        let output = gather_metrics();
        assert!(output.contains("SeaweedFS_volumeServer_request_total"));
    }

    #[test]
    fn test_build_pushgateway_url() {
        assert_eq!(
            build_pushgateway_url("localhost:9091", "volumeServer", "test-instance"),
            "http://localhost:9091/metrics/job/volumeServer/instance/test-instance"
        );
        assert_eq!(
            build_pushgateway_url("https://push.example", "volumeServer", "node-a"),
            "https://push.example/metrics/job/volumeServer/instance/node-a"
        );
    }

    #[tokio::test]
    async fn test_push_metrics_once() {
        register_metrics();

        let captured = Arc::new(Mutex::new(None::<String>));
        let captured_clone = captured.clone();

        let app = Router::new().route(
            "/metrics/job/volumeServer/instance/test-instance",
            put(move |body: String| {
                let captured = captured_clone.clone();
                async move {
                    *captured.lock().unwrap() = Some(body);
                    "ok"
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = reqwest::Client::new();
        push_metrics_once(
            &client,
            &format!("127.0.0.1:{}", addr.port()),
            "volumeServer",
            "test-instance",
        )
        .await
        .unwrap();

        let body = captured.lock().unwrap().clone().unwrap();
        assert!(body.contains("SeaweedFS_volumeServer_request_total"));

        server.abort();
    }

    #[test]
    fn test_delete_collection_metrics_removes_collection_labelsets() {
        register_metrics();

        VOLUME_GAUGE.with_label_values(&["pics", "volume"]).set(2.0);
        VOLUME_GAUGE.with_label_values(&["pics", "ec_shards"]).set(3.0);
        READ_ONLY_VOLUME_GAUGE
            .with_label_values(&["pics", "volume"])
            .set(1.0);
        DISK_SIZE_GAUGE
            .with_label_values(&["pics", "normal"])
            .set(10.0);
        DISK_SIZE_GAUGE
            .with_label_values(&["pics", "deleted_bytes"])
            .set(4.0);

        delete_collection_metrics("pics");

        let output = gather_metrics();
        assert!(!output.contains("collection=\"pics\",type=\"volume\""));
        assert!(!output.contains("collection=\"pics\",type=\"ec_shards\""));
        assert!(!output.contains("collection=\"pics\",type=\"normal\""));
        assert!(!output.contains("collection=\"pics\",type=\"deleted_bytes\""));
    }
}
