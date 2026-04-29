//! VolumeServer: the main HTTP server for volume operations.
//!
//! Routes:
//!   GET/HEAD /{vid},{fid}     — read a file
//!   POST/PUT /{vid},{fid}     — write a file
//!   DELETE   /{vid},{fid}     — delete a file
//!   GET      /status          — server status
//!   GET      /healthz         — health check
//!
//! Matches Go's server/volume_server.go.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use axum::{
    extract::{connect_info::ConnectInfo, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{any, get},
    Router,
};

use crate::config::ReadMode;
use crate::security::Guard;
use crate::storage::store::Store;

use super::grpc_client::OutgoingGrpcTlsConfig;
use super::handlers;
use super::write_queue::WriteQueue;

#[derive(Clone, Debug, Default)]
pub struct RuntimeMetricsConfig {
    pub push_gateway: crate::metrics::PushGatewayConfig,
}

/// Shared state for the volume server.
pub struct VolumeServerState {
    pub store: RwLock<Store>,
    pub guard: RwLock<Guard>,
    pub is_stopping: RwLock<bool>,
    /// Maintenance mode flag.
    pub maintenance: AtomicBool,
    /// State version — incremented on each SetState call.
    pub state_version: AtomicU32,
    /// Throttling: concurrent upload/download limits (in bytes, 0 = disabled).
    pub concurrent_upload_limit: i64,
    pub concurrent_download_limit: i64,
    pub inflight_upload_data_timeout: std::time::Duration,
    pub inflight_download_data_timeout: std::time::Duration,
    /// Current in-flight upload/download bytes.
    pub inflight_upload_bytes: AtomicI64,
    pub inflight_download_bytes: AtomicI64,
    /// Notify waiters when inflight bytes decrease.
    pub upload_notify: tokio::sync::Notify,
    pub download_notify: tokio::sync::Notify,
    /// Data center name from config.
    pub data_center: String,
    /// Rack name from config.
    pub rack: String,
    /// File size limit in bytes (0 = no limit).
    pub file_size_limit_bytes: i64,
    /// Default IO rate limit for maintenance copy/replication work.
    pub maintenance_byte_per_second: i64,
    /// Whether the server is connected to master (heartbeat active).
    pub is_heartbeating: AtomicBool,
    /// Whether master addresses are configured.
    pub has_master: bool,
    /// Seconds to wait before shutting down servers (graceful drain).
    pub pre_stop_seconds: u32,
    /// Notify heartbeat to send an immediate update when volume state changes.
    pub volume_state_notify: tokio::sync::Notify,
    /// Optional batched write queue for improved throughput under load.
    pub write_queue: std::sync::OnceLock<WriteQueue>,
    /// Registry of S3 tier backends for tiered storage operations.
    pub s3_tier_registry: std::sync::RwLock<crate::remote_storage::s3_tier::S3TierRegistry>,
    /// Read mode: local, proxy, or redirect for non-local volumes.
    pub read_mode: ReadMode,
    /// First master address for volume lookups (e.g., "localhost:9333").
    pub master_url: String,
    /// Seed master addresses for UI rendering.
    pub master_urls: Vec<String>,
    /// This server's own address (ip:port) for filtering self from lookup results.
    pub self_url: String,
    /// HTTP client for proxy requests and master lookups.
    pub http_client: reqwest::Client,
    /// Scheme used for outgoing master and peer HTTP requests ("http" or "https").
    pub outgoing_http_scheme: String,
    /// Optional client TLS material for outgoing gRPC connections.
    pub outgoing_grpc_tls: Option<OutgoingGrpcTlsConfig>,
    /// Metrics push settings learned from master heartbeat responses.
    pub metrics_runtime: std::sync::RwLock<RuntimeMetricsConfig>,
    pub metrics_notify: tokio::sync::Notify,
    /// Whether JPEG uploads should be normalized using EXIF orientation.
    pub fix_jpg_orientation: bool,
    /// Read tuning flags for large-file streaming.
    pub has_slow_read: bool,
    pub read_buffer_size_bytes: usize,
    /// Path to security.toml — stored for SIGHUP reload.
    pub security_file: String,
    /// Original CLI whitelist entries — stored for SIGHUP reload.
    pub cli_white_list: Vec<String>,
    /// Path to state.pb file for persisting VolumeServerState across restarts.
    pub state_file_path: String,
}

impl VolumeServerState {
    /// Check if the server is in maintenance mode; return gRPC error if so.
    pub fn check_maintenance(&self) -> Result<(), tonic::Status> {
        if self.maintenance.load(Ordering::Relaxed) {
            let id = self.store.read().unwrap().id.clone();
            return Err(tonic::Status::unavailable(format!(
                "volume server {} is in maintenance mode",
                id
            )));
        }
        Ok(())
    }
}

pub fn build_metrics_router() -> Router {
    Router::new().route("/metrics", get(handlers::metrics_handler))
}

pub fn normalize_outgoing_http_url(scheme: &str, raw_target: &str) -> Result<String, String> {
    if raw_target.starts_with("http://") || raw_target.starts_with("https://") {
        let mut url = reqwest::Url::parse(raw_target)
            .map_err(|e| format!("invalid url {}: {}", raw_target, e))?;
        url.set_scheme(scheme)
            .map_err(|_| format!("invalid scheme {}", scheme))?;
        return Ok(url.to_string());
    }
    Ok(format!("{}://{}", scheme, raw_target))
}

/// Convert a SeaweedFS server address to its HTTP `host:port` form.
///
/// Mirrors Go's `pb.ServerAddress.ToHttpAddress()`: SeaweedFS encodes a
/// server's gRPC port by appending `.grpcPort` to the HTTP port (e.g.
/// `host:9333.19333`). For HTTP requests we want only the HTTP `host:port`.
/// - `host:port.grpcPort` -> `host:port`
/// - `host:port` -> `host:port` (unchanged)
/// - Anything that does not look like `host:port[.grpcPort]` is returned unchanged.
///
/// Returns a `Cow<str>` so the common (no-suffix) case borrows from `addr`
/// without allocating; only the rewrite branch produces a new `String`.
pub fn to_http_address(addr: &str) -> std::borrow::Cow<'_, str> {
    let Some(ports_sep_index) = addr.rfind(':') else {
        return std::borrow::Cow::Borrowed(addr);
    };
    let ports = &addr[ports_sep_index + 1..];
    if let Some(dot_idx) = ports.rfind('.') {
        let http_port = &ports[..dot_idx];
        let grpc_port = &ports[dot_idx + 1..];
        // Only strip the suffix when both parts parse as real ports — leave
        // anything else (e.g. "host:abc.def") untouched so bad config surfaces
        // rather than being silently rewritten. Mirrors the validation already
        // done in `to_grpc_address` for the inverse direction.
        if http_port.parse::<u16>().is_ok() && grpc_port.parse::<u16>().is_ok() {
            return std::borrow::Cow::Owned(
                addr[..ports_sep_index + 1 + dot_idx].to_string(),
            );
        }
    }
    std::borrow::Cow::Borrowed(addr)
}

fn request_remote_addr(request: &Request) -> Option<SocketAddr> {
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0)
}

fn request_is_whitelisted(state: &VolumeServerState, request: &Request) -> bool {
    request_remote_addr(request)
        .map(|remote_addr| {
            state
                .guard
                .read()
                .unwrap()
                .check_whitelist(&remote_addr.to_string())
        })
        .unwrap_or(true)
}

/// Middleware: set Server header, echo x-amz-request-id, set CORS if Origin present.
async fn common_headers_middleware(request: Request, next: Next) -> Response {
    let origin = request.headers().get("origin").cloned();
    let request_id = super::request_id::generate_http_request_id();

    let mut response =
        super::request_id::scope_request_id(
            request_id.clone(),
            async move { next.run(request).await },
        )
        .await;

    let headers = response.headers_mut();
    if let Ok(val) = HeaderValue::from_str(crate::version::server_header()) {
        headers.insert("Server", val);
    }

    if let Ok(val) = HeaderValue::from_str(&request_id) {
        headers.insert("X-Request-Id", val.clone());
        headers.insert("x-amz-request-id", val);
    }

    if origin.is_some() {
        headers.insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));
        headers.insert(
            "Access-Control-Allow-Credentials",
            HeaderValue::from_static("true"),
        );
    }

    response
}

/// Admin store handler — dispatches based on HTTP method.
/// Matches Go's privateStoreHandler: GET/HEAD → read, POST/PUT → write,
/// DELETE → delete, OPTIONS → CORS headers, anything else → 400.
async fn admin_store_handler(state: State<Arc<VolumeServerState>>, request: Request) -> Response {
    let start = std::time::Instant::now();
    let method = request.method().clone();
    let mut method_str = method.as_str().to_string();
    let request_bytes = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(0);
    super::server_stats::record_request_open();
    crate::metrics::INFLIGHT_REQUESTS_GAUGE
        .with_label_values(&[&method_str])
        .inc();
    let whitelist_rejected = matches!(method, Method::POST | Method::PUT | Method::DELETE)
        && !request_is_whitelisted(&state, &request);
    let response = match method.clone() {
        _ if whitelist_rejected => StatusCode::UNAUTHORIZED.into_response(),
        Method::GET | Method::HEAD => {
            super::server_stats::record_read_request();
            handlers::get_or_head_handler_from_request(state, request).await
        }
        Method::POST | Method::PUT => {
            super::server_stats::record_write_request();
            if request_bytes > 0 {
                super::server_stats::record_bytes_in(request_bytes);
            }
            handlers::post_handler(state, request).await
        }
        Method::DELETE => {
            super::server_stats::record_delete_request();
            handlers::delete_handler(state, request).await
        }
        Method::OPTIONS => {
            super::server_stats::record_read_request();
            admin_options_response()
        }
        _ => {
            let method_name = request.method().to_string();
            let query = request.uri().query().map(|q| q.to_string());
            method_str = "INVALID".to_string();
            handlers::json_error_with_query(
                StatusCode::BAD_REQUEST,
                format!("unsupported method {}", method_name),
                query.as_deref(),
            )
        }
    };
    if method == Method::GET {
        if let Some(response_bytes) = response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value > 0)
        {
            super::server_stats::record_bytes_out(response_bytes);
        }
    }
    super::server_stats::record_request_close();
    crate::metrics::INFLIGHT_REQUESTS_GAUGE
        .with_label_values(&[&method_str])
        .dec();
    crate::metrics::REQUEST_COUNTER
        .with_label_values(&[&method_str, response.status().as_str()])
        .inc();
    crate::metrics::REQUEST_DURATION
        .with_label_values(&[&method_str])
        .observe(start.elapsed().as_secs_f64());
    response
}

/// Public store handler — dispatches based on HTTP method.
/// Matches Go's publicReadOnlyHandler: GET/HEAD → read, OPTIONS → CORS,
/// anything else → 200 (passthrough no-op).
async fn public_store_handler(state: State<Arc<VolumeServerState>>, request: Request) -> Response {
    let start = std::time::Instant::now();
    let method = request.method().clone();
    let method_str = method.as_str().to_string();
    super::server_stats::record_request_open();
    crate::metrics::INFLIGHT_REQUESTS_GAUGE
        .with_label_values(&[&method_str])
        .inc();
    let response = match method.clone() {
        Method::GET | Method::HEAD => {
            super::server_stats::record_read_request();
            handlers::get_or_head_handler_from_request(state, request).await
        }
        Method::OPTIONS => {
            super::server_stats::record_read_request();
            public_options_response()
        }
        _ => StatusCode::OK.into_response(),
    };
    if method == Method::GET {
        if let Some(response_bytes) = response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value > 0)
        {
            super::server_stats::record_bytes_out(response_bytes);
        }
    }
    super::server_stats::record_request_close();
    crate::metrics::INFLIGHT_REQUESTS_GAUGE
        .with_label_values(&[&method_str])
        .dec();
    crate::metrics::REQUEST_COUNTER
        .with_label_values(&[&method_str, response.status().as_str()])
        .inc();
    crate::metrics::REQUEST_DURATION
        .with_label_values(&[&method_str])
        .observe(start.elapsed().as_secs_f64());
    response
}

/// Build OPTIONS response for admin port.
fn admin_options_response() -> Response {
    let mut response = StatusCode::OK.into_response();
    let headers = response.headers_mut();
    headers.insert(
        "Access-Control-Allow-Methods",
        HeaderValue::from_static("PUT, POST, GET, DELETE, OPTIONS"),
    );
    headers.insert(
        "Access-Control-Allow-Headers",
        HeaderValue::from_static("*"),
    );
    response
}

/// Build OPTIONS response for public port.
fn public_options_response() -> Response {
    let mut response = StatusCode::OK.into_response();
    let headers = response.headers_mut();
    headers.insert(
        "Access-Control-Allow-Methods",
        HeaderValue::from_static("GET, OPTIONS"),
    );
    headers.insert(
        "Access-Control-Allow-Headers",
        HeaderValue::from_static("*"),
    );
    response
}

/// Build the admin (private) HTTP router — supports all operations.
/// UI route is only registered when no signing keys are configured,
/// matching Go's `if signingKey == "" || enableUiAccess` check.
pub fn build_admin_router(state: Arc<VolumeServerState>) -> Router {
    let guard = state.guard.read().unwrap();
    // This helper can only derive the default Go behavior from the guard state:
    // UI stays enabled when the write signing key is empty. The explicit
    // `access.ui` override is handled by `build_admin_router_with_ui(...)`.
    let ui_enabled = guard.signing_key.0.is_empty();
    drop(guard);
    build_admin_router_with_ui(state, ui_enabled)
}

/// Build the admin router with an explicit UI exposure flag.
pub fn build_admin_router_with_ui(state: Arc<VolumeServerState>, ui_enabled: bool) -> Router {
    let mut router = Router::new()
        .route("/status", get(handlers::status_handler))
        .route("/healthz", get(handlers::healthz_handler))
        .route("/favicon.ico", get(handlers::favicon_handler))
        .route(
            "/seaweedfsstatic/*path",
            get(handlers::static_asset_handler),
        )
        .route("/", any(admin_store_handler))
        .route("/:path", any(admin_store_handler))
        .route("/:vid/:fid", any(admin_store_handler))
        .route("/:vid/:fid/:filename", any(admin_store_handler))
        .fallback(admin_store_handler);
    if ui_enabled {
        // Note: /stats/* endpoints are commented out in Go's volume_server.go (L130-134).
        // Only the UI endpoint is registered when UI access is enabled.
        router = router.route("/ui/index.html", get(handlers::ui_handler));
    }
    router
        .layer(middleware::from_fn(common_headers_middleware))
        .with_state(state)
}

/// Build the public (read-only) HTTP router — only GET/HEAD.
pub fn build_public_router(state: Arc<VolumeServerState>) -> Router {
    Router::new()
        .route("/favicon.ico", get(handlers::favicon_handler))
        .route(
            "/seaweedfsstatic/*path",
            get(handlers::static_asset_handler),
        )
        .route("/", any(public_store_handler))
        .route("/:path", any(public_store_handler))
        .route("/:vid/:fid", any(public_store_handler))
        .route("/:vid/:fid/:filename", any(public_store_handler))
        .fallback(public_store_handler)
        .layer(middleware::from_fn(common_headers_middleware))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_http_address_strips_grpc_port_suffix() {
        assert_eq!(to_http_address("10.0.0.1:9333.19333"), "10.0.0.1:9333");
        assert_eq!(
            to_http_address("master.local:9333.19333"),
            "master.local:9333"
        );
        assert_eq!(to_http_address("10.85.183.6:5300.6300"), "10.85.183.6:5300");
    }

    #[test]
    fn test_to_http_address_passthrough_without_grpc_suffix() {
        assert_eq!(to_http_address("10.0.0.1:9333"), "10.0.0.1:9333");
        assert_eq!(to_http_address("master.local:9333"), "master.local:9333");
    }

    #[test]
    fn test_to_http_address_returns_input_when_unparseable() {
        assert_eq!(to_http_address(""), "");
        assert_eq!(to_http_address("no-port"), "no-port");
        // Trailing colon: nothing after the separator, treat as unparseable.
        assert_eq!(to_http_address("host:"), "host:");
    }

    #[test]
    fn test_to_http_address_borrows_when_unchanged_and_owns_when_stripped() {
        // The common case (no suffix) must not allocate.
        let result = to_http_address("10.0.0.1:9333");
        assert!(matches!(result, std::borrow::Cow::Borrowed(_)));
        // Stripping requires a new string.
        let result = to_http_address("10.0.0.1:9333.19333");
        assert!(matches!(result, std::borrow::Cow::Owned(_)));
        assert_eq!(result, "10.0.0.1:9333");
        // Unparseable / passthrough also borrows.
        let result = to_http_address("host:abc.def");
        assert!(matches!(result, std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn test_to_http_address_keeps_non_numeric_dotted_suffix() {
        // The dotted form is only valid when both sides are real port numbers.
        // Otherwise the address is malformed config (e.g. a hostname like
        // "host:abc.def"), and silently rewriting it would just hide the bug.
        assert_eq!(to_http_address("host:abc.def"), "host:abc.def");
        assert_eq!(to_http_address("host:9333.notaport"), "host:9333.notaport");
        assert_eq!(to_http_address("host:notaport.19333"), "host:notaport.19333");
        // Out-of-range ports must not be silently truncated either.
        assert_eq!(to_http_address("host:99999.19333"), "host:99999.19333");
    }

    #[test]
    fn test_to_http_address_handles_bracketed_ipv6_literals() {
        // The function uses `rfind(':')`, so for bracketed IPv6 the port
        // separator is correctly identified as the colon AFTER the closing
        // bracket — making IPv4 and IPv6 behave the same.
        assert_eq!(to_http_address("[::1]:9333.19333"), "[::1]:9333");
        assert_eq!(
            to_http_address("[2001:db8::10]:5300.6300"),
            "[2001:db8::10]:5300"
        );
        // Plain bracketed IPv6 without a dotted suffix is borrowed unchanged.
        let result = to_http_address("[2001:db8::1]:9333");
        assert!(matches!(result, std::borrow::Cow::Borrowed(_)));
        assert_eq!(result, "[2001:db8::1]:9333");
        // Non-numeric / out-of-range / missing suffix all preserve the input.
        assert_eq!(to_http_address("[::1]:"), "[::1]:");
        assert_eq!(to_http_address("[::1]:abc.def"), "[::1]:abc.def");
        assert_eq!(to_http_address("[::1]:99999.19333"), "[::1]:99999.19333");
    }
}
