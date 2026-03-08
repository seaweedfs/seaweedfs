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

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use axum::{
    extract::{Request, State},
    http::{HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{any, get},
    Router,
};

use crate::config::ReadMode;
use crate::security::Guard;
use crate::storage::store::Store;

use super::handlers;
use super::write_queue::WriteQueue;

#[derive(Clone, Debug, Default)]
pub struct RuntimeMetricsConfig {
    pub push_gateway: crate::metrics::PushGatewayConfig,
}

/// Shared state for the volume server.
pub struct VolumeServerState {
    pub store: RwLock<Store>,
    pub guard: Guard,
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
    /// This server's own address (ip:port) for filtering self from lookup results.
    pub self_url: String,
    /// HTTP client for proxy requests and master lookups.
    pub http_client: reqwest::Client,
    /// Metrics push settings learned from master heartbeat responses.
    pub metrics_runtime: std::sync::RwLock<RuntimeMetricsConfig>,
    pub metrics_notify: tokio::sync::Notify,
    /// Read tuning flags for large-file streaming.
    pub has_slow_read: bool,
    pub read_buffer_size_bytes: usize,
}

impl VolumeServerState {
    /// Check if the server is in maintenance mode; return gRPC error if so.
    pub fn check_maintenance(&self) -> Result<(), tonic::Status> {
        if self.maintenance.load(Ordering::Relaxed) {
            return Err(tonic::Status::unavailable("maintenance mode"));
        }
        Ok(())
    }
}

pub fn build_metrics_router() -> Router {
    Router::new().route("/metrics", get(handlers::metrics_handler))
}

/// Middleware: set Server header, echo x-amz-request-id, set CORS if Origin present.
async fn common_headers_middleware(request: Request, next: Next) -> Response {
    let origin = request.headers().get("origin").cloned();
    let request_id = request.headers().get("x-amz-request-id").cloned();

    let mut response = next.run(request).await;

    let headers = response.headers_mut();
    headers.insert("Server", HeaderValue::from_static("SeaweedFS Volume 0.1.0"));

    if let Some(rid) = request_id {
        headers.insert("x-amz-request-id", rid);
    } else {
        let id = uuid::Uuid::new_v4().to_string();
        if let Ok(val) = HeaderValue::from_str(&id) {
            headers.insert("x-amz-request-id", val);
        }
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
    match request.method().clone() {
        Method::GET | Method::HEAD => {
            handlers::get_or_head_handler_from_request(state, request).await
        }
        Method::POST | Method::PUT => handlers::post_handler(state, request).await,
        Method::DELETE => handlers::delete_handler(state, request).await,
        Method::OPTIONS => admin_options_response(),
        _ => (
            StatusCode::BAD_REQUEST,
            format!("{{\"error\":\"unsupported method {}\"}}", request.method()),
        )
            .into_response(),
    }
}

/// Public store handler — dispatches based on HTTP method.
/// Matches Go's publicReadOnlyHandler: GET/HEAD → read, OPTIONS → CORS,
/// anything else → 200 (passthrough no-op).
async fn public_store_handler(state: State<Arc<VolumeServerState>>, request: Request) -> Response {
    match request.method().clone() {
        Method::GET | Method::HEAD => {
            handlers::get_or_head_handler_from_request(state, request).await
        }
        Method::OPTIONS => public_options_response(),
        _ => StatusCode::OK.into_response(),
    }
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
pub fn build_admin_router(state: Arc<VolumeServerState>) -> Router {
    build_admin_router_with_ui(state.clone(), state.guard.signing_key.0.is_empty())
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
        .route("/:vid/:fid/:filename", any(admin_store_handler));
    if ui_enabled {
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
        .layer(middleware::from_fn(common_headers_middleware))
        .with_state(state)
}
