//! Integration tests for the volume server HTTP handlers.
//!
//! Uses axum's Router with tower::ServiceExt::oneshot to test
//! end-to-end without starting a real TCP server.

use std::sync::{Arc, RwLock};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt; // for `oneshot`

use seaweed_volume::security::{Guard, SigningKey};
use seaweed_volume::server::volume_server::{
    build_admin_router, build_admin_router_with_ui, build_metrics_router, build_public_router,
    VolumeServerState,
};
use seaweed_volume::storage::needle_map::NeedleMapKind;
use seaweed_volume::storage::store::Store;
use seaweed_volume::storage::types::{DiskType, VolumeId};

use tempfile::TempDir;

/// Create a test VolumeServerState with a temp directory, a single disk
/// location, and one pre-created volume (VolumeId 1).
fn test_state() -> (Arc<VolumeServerState>, TempDir) {
    test_state_with_signing_key(Vec::new())
}

fn test_state_with_signing_key(signing_key: Vec<u8>) -> (Arc<VolumeServerState>, TempDir) {
    let tmp = TempDir::new().expect("failed to create temp dir");
    let dir = tmp.path().to_str().unwrap();

    let mut store = Store::new(NeedleMapKind::InMemory);
    store
        .add_location(
            dir,
            dir,
            10,
            DiskType::HardDrive,
            seaweed_volume::config::MinFreeSpace::Percent(1.0),
            Vec::new(),
        )
        .expect("failed to add location");
    store
        .add_volume(VolumeId(1), "", None, None, 0, DiskType::HardDrive)
        .expect("failed to create volume");

    let guard = Guard::new(&[], SigningKey(signing_key), 0, SigningKey(vec![]), 0);
    let state = Arc::new(VolumeServerState {
        store: RwLock::new(store),
        guard,
        is_stopping: RwLock::new(false),
        maintenance: std::sync::atomic::AtomicBool::new(false),
        state_version: std::sync::atomic::AtomicU32::new(0),
        concurrent_upload_limit: 0,
        concurrent_download_limit: 0,
        inflight_upload_data_timeout: std::time::Duration::from_secs(60),
        inflight_download_data_timeout: std::time::Duration::from_secs(60),
        inflight_upload_bytes: std::sync::atomic::AtomicI64::new(0),
        inflight_download_bytes: std::sync::atomic::AtomicI64::new(0),
        upload_notify: tokio::sync::Notify::new(),
        download_notify: tokio::sync::Notify::new(),
        data_center: String::new(),
        rack: String::new(),
        file_size_limit_bytes: 0,
        is_heartbeating: std::sync::atomic::AtomicBool::new(false),
        has_master: false,
        pre_stop_seconds: 0,
        volume_state_notify: tokio::sync::Notify::new(),
        write_queue: std::sync::OnceLock::new(),
        s3_tier_registry: std::sync::RwLock::new(
            seaweed_volume::remote_storage::s3_tier::S3TierRegistry::new(),
        ),
        read_mode: seaweed_volume::config::ReadMode::Local,
        master_url: String::new(),
        self_url: String::new(),
        http_client: reqwest::Client::new(),
        metrics_runtime: std::sync::RwLock::new(
            seaweed_volume::server::volume_server::RuntimeMetricsConfig::default(),
        ),
        metrics_notify: tokio::sync::Notify::new(),
        has_slow_read: false,
        read_buffer_size_bytes: 1024 * 1024,
    });
    (state, tmp)
}

/// Helper: read the entire response body as bytes.
async fn body_bytes(response: axum::response::Response) -> Vec<u8> {
    let body = response.into_body();
    axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("failed to read body")
        .to_vec()
}

// ============================================================================
// 1. GET /healthz returns 200 when server is running
// ============================================================================

#[tokio::test]
async fn healthz_returns_200_when_running() {
    let (state, _tmp) = test_state();
    let app = build_admin_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ============================================================================
// 2. GET /healthz returns 503 when is_stopping=true
// ============================================================================

#[tokio::test]
async fn healthz_returns_503_when_stopping() {
    let (state, _tmp) = test_state();
    *state.is_stopping.write().unwrap() = true;
    let app = build_admin_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

// ============================================================================
// 3. GET /status returns JSON with version and volumes array
// ============================================================================

#[tokio::test]
async fn status_returns_json_with_version_and_volumes() {
    let (state, _tmp) = test_state();
    let app = build_admin_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_bytes(response).await;
    let json: serde_json::Value =
        serde_json::from_slice(&body).expect("response is not valid JSON");

    assert!(json.get("Version").is_some(), "missing 'Version' field");
    assert!(json["Version"].is_string(), "'Version' should be a string");

    assert!(json.get("Volumes").is_some(), "missing 'Volumes' field");
    assert!(json["Volumes"].is_array(), "'Volumes' should be an array");

    // We created one volume in test_state, so the array should have one entry
    let volumes = json["Volumes"].as_array().unwrap();
    assert_eq!(volumes.len(), 1, "expected 1 volume");
    assert_eq!(volumes[0]["Id"], 1);
}

#[tokio::test]
async fn admin_router_does_not_expose_metrics() {
    let (state, _tmp) = test_state();
    let app = build_admin_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn metrics_router_serves_metrics() {
    let app = build_metrics_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ============================================================================
// 4. POST writes data, then GET reads it back
// ============================================================================

#[tokio::test]
async fn write_then_read_needle() {
    let (state, _tmp) = test_state();

    // The fid "01637037d6" encodes NeedleId=0x01, Cookie=0x637037d6
    let uri = "/1,01637037d6";
    let payload = b"hello, seaweedfs!";

    // --- POST (write) ---
    let app = build_admin_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .body(Body::from(payload.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "POST should return 201 Created"
    );

    let body = body_bytes(response).await;
    let json: serde_json::Value =
        serde_json::from_slice(&body).expect("POST response is not valid JSON");
    assert_eq!(json["size"], payload.len() as u64);

    // --- GET (read back) ---
    let app = build_admin_router(state.clone());
    let response = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK, "GET should return 200");

    let body = body_bytes(response).await;
    assert_eq!(body, payload, "GET body should match written data");
}

// ============================================================================
// 5. DELETE deletes a needle, subsequent GET returns 404
// ============================================================================

#[tokio::test]
async fn delete_then_get_returns_404() {
    let (state, _tmp) = test_state();
    let uri = "/1,01637037d6";
    let payload = b"to be deleted";

    // Write the needle first
    let app = build_admin_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .body(Body::from(payload.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // Delete
    let app = build_admin_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::ACCEPTED,
        "DELETE should return 202 Accepted"
    );

    // GET should now return 404
    let app = build_admin_router(state.clone());
    let response = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "GET after DELETE should return 404"
    );
}

// ============================================================================
// 6. HEAD returns headers without body
// ============================================================================

#[tokio::test]
async fn head_returns_headers_without_body() {
    let (state, _tmp) = test_state();
    let uri = "/1,01637037d6";
    let payload = b"head test data";

    // Write needle
    let app = build_admin_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .body(Body::from(payload.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // HEAD
    let app = build_admin_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK, "HEAD should return 200");

    // Content-Length header should be present
    let content_length = response
        .headers()
        .get("content-length")
        .expect("HEAD should include Content-Length header");
    let len: usize = content_length
        .to_str()
        .unwrap()
        .parse()
        .expect("Content-Length should be a number");
    assert_eq!(
        len,
        payload.len(),
        "Content-Length should match payload size"
    );

    // Body should be empty for HEAD
    let body = body_bytes(response).await;
    assert!(body.is_empty(), "HEAD body should be empty");
}

// ============================================================================
// 7. Invalid URL path returns 400
// ============================================================================

#[tokio::test]
async fn invalid_url_path_returns_400() {
    let (state, _tmp) = test_state();
    let app = build_admin_router(state);

    // "invalidpath" has no comma or slash separator so parse_url_path returns None
    let response = app
        .oneshot(
            Request::builder()
                .uri("/invalidpath")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "invalid URL path should return 400"
    );
}

#[tokio::test]
async fn deep_invalid_url_path_returns_400() {
    let (state, _tmp) = test_state();
    let app = build_admin_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/not/a/valid/volume/path")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn admin_root_get_returns_400() {
    let (state, _tmp) = test_state();
    let app = build_admin_router(state);

    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn public_root_get_returns_400() {
    let (state, _tmp) = test_state();
    let app = build_public_router(state);

    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn public_router_does_not_expose_healthz() {
    let (state, _tmp) = test_state();
    let app = build_public_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn admin_router_does_not_expose_stats_routes() {
    let (state, _tmp) = test_state();
    let app = build_admin_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/stats/counter")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn admin_router_hides_ui_when_write_jwt_is_configured() {
    let (state, _tmp) = test_state_with_signing_key(b"secret".to_vec());
    let app = build_admin_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ui/index.html")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn admin_router_can_expose_ui_with_explicit_override() {
    let (state, _tmp) = test_state_with_signing_key(b"secret".to_vec());
    let app = build_admin_router_with_ui(state, true);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ui/index.html")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}
