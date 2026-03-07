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

use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};

use axum::{
    Router,
    routing::{get, any},
    middleware::{self, Next},
    extract::{Request, State},
    response::{IntoResponse, Response},
    http::{StatusCode, HeaderValue, Method},
};

use crate::security::Guard;
use crate::storage::store::Store;

use super::handlers;

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

/// Middleware: set Server header, echo x-amz-request-id, set CORS if Origin present.
async fn common_headers_middleware(request: Request, next: Next) -> Response {
    let origin = request.headers().get("origin").cloned();
    let request_id = request.headers().get("x-amz-request-id").cloned();

    let mut response = next.run(request).await;

    let headers = response.headers_mut();
    headers.insert(
        "Server",
        HeaderValue::from_static("SeaweedFS Volume 0.1.0"),
    );

    if let Some(rid) = request_id {
        headers.insert("x-amz-request-id", rid);
    }

    if origin.is_some() {
        headers.insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));
        headers.insert("Access-Control-Allow-Credentials", HeaderValue::from_static("true"));
    }

    response
}

/// Admin store handler — dispatches based on HTTP method.
/// Matches Go's privateStoreHandler: GET/HEAD → read, POST/PUT → write,
/// DELETE → delete, OPTIONS → CORS headers, anything else → 400.
async fn admin_store_handler(
    state: State<Arc<VolumeServerState>>,
    request: Request,
) -> Response {
    match request.method().clone() {
        Method::GET | Method::HEAD => handlers::get_or_head_handler_from_request(state, request).await,
        Method::POST | Method::PUT => handlers::post_handler(state, request).await,
        Method::DELETE => handlers::delete_handler(state, request).await,
        Method::OPTIONS => admin_options_response(),
        _ => (StatusCode::BAD_REQUEST, format!("{{\"error\":\"unsupported method {}\"}}", request.method())).into_response(),
    }
}

/// Public store handler — dispatches based on HTTP method.
/// Matches Go's publicReadOnlyHandler: GET/HEAD → read, OPTIONS → CORS,
/// anything else → 200 (passthrough no-op).
async fn public_store_handler(
    state: State<Arc<VolumeServerState>>,
    request: Request,
) -> Response {
    match request.method().clone() {
        Method::GET | Method::HEAD => handlers::get_or_head_handler_from_request(state, request).await,
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
    Router::new()
        .route("/status", get(handlers::status_handler))
        .route("/healthz", get(handlers::healthz_handler))
        .route("/metrics", get(handlers::metrics_handler))
        .route("/favicon.ico", get(handlers::favicon_handler))
        .route("/seaweedfsstatic/*path", get(handlers::static_asset_handler))
        .route("/ui/index.html", get(handlers::ui_handler))
        .route("/", any(|state: State<Arc<VolumeServerState>>, request: Request| async move {
            match request.method().clone() {
                Method::OPTIONS => admin_options_response(),
                Method::GET => StatusCode::OK.into_response(),
                _ => (StatusCode::BAD_REQUEST, format!("{{\"error\":\"unsupported method {}\"}}", request.method())).into_response(),
            }
        }))
        .route("/:path", any(admin_store_handler))
        .route("/:vid/:fid", any(admin_store_handler))
        .route("/:vid/:fid/:filename", any(admin_store_handler))
        .layer(middleware::from_fn(common_headers_middleware))
        .with_state(state)
}

/// Build the public (read-only) HTTP router — only GET/HEAD.
pub fn build_public_router(state: Arc<VolumeServerState>) -> Router {
    Router::new()
        .route("/healthz", get(handlers::healthz_handler))
        .route("/favicon.ico", get(handlers::favicon_handler))
        .route("/seaweedfsstatic/*path", get(handlers::static_asset_handler))
        .route("/", any(|_state: State<Arc<VolumeServerState>>, request: Request| async move {
            match request.method().clone() {
                Method::OPTIONS => public_options_response(),
                Method::GET => StatusCode::OK.into_response(),
                _ => StatusCode::OK.into_response(),
            }
        }))
        .route("/:path", any(public_store_handler))
        .route("/:vid/:fid", any(public_store_handler))
        .route("/:vid/:fid/:filename", any(public_store_handler))
        .layer(middleware::from_fn(common_headers_middleware))
        .with_state(state)
}
