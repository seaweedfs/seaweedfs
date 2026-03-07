//! HTTP handlers for volume server operations.
//!
//! Implements GET/HEAD (read), POST/PUT (write), DELETE, /status, /healthz.
//! Matches Go's volume_server_handlers_read.go, volume_server_handlers_write.go,
//! volume_server_handlers_admin.go.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::metrics;
use crate::security::Guard;
use crate::storage::needle::needle::Needle;
use crate::storage::types::*;
use super::volume_server::VolumeServerState;

// ============================================================================
// URL Parsing
// ============================================================================

/// Parse volume ID and file ID from URL path.
/// Supports: "vid,fid", "vid/fid", "vid,fid.ext", "vid/fid/filename.ext"
fn parse_url_path(path: &str) -> Option<(VolumeId, NeedleId, Cookie)> {
    let path = path.trim_start_matches('/');

    // Try "vid,fid" or "vid/fid" or "vid/fid/filename" formats
    let (vid_str, fid_part) = if let Some(pos) = path.find(',') {
        (&path[..pos], &path[pos + 1..])
    } else if let Some(pos) = path.find('/') {
        (&path[..pos], &path[pos + 1..])
    } else {
        return None;
    };

    // For fid part, strip extension from the fid (not from filename)
    // "vid,fid.ext" -> fid is before dot
    // "vid/fid/filename.ext" -> fid is the part before the second slash
    let fid_str = if let Some(slash_pos) = fid_part.find('/') {
        // "fid/filename.ext" - fid is before the slash
        &fid_part[..slash_pos]
    } else if let Some(dot) = fid_part.rfind('.') {
        // "fid.ext" - strip extension
        &fid_part[..dot]
    } else {
        fid_part
    };

    let vid = VolumeId::parse(vid_str).ok()?;
    let (needle_id, cookie) = crate::storage::needle::needle::parse_needle_id_cookie(fid_str).ok()?;

    Some((vid, needle_id, cookie))
}

// ============================================================================
// Query parameters
// ============================================================================

#[derive(Deserialize, Default)]
pub struct ReadQueryParams {
    #[serde(rename = "response-content-type")]
    pub response_content_type: Option<String>,
    #[serde(rename = "response-cache-control")]
    pub response_cache_control: Option<String>,
    pub dl: Option<String>,
    #[serde(rename = "readDeleted")]
    pub read_deleted: Option<String>,
}

// ============================================================================
// Read Handler (GET/HEAD)
// ============================================================================

/// Called from the method-dispatching store handler with a full Request.
pub async fn get_or_head_handler_from_request(
    State(state): State<Arc<VolumeServerState>>,
    request: Request<Body>,
) -> Response {
    let uri = request.uri().clone();
    let headers = request.headers().clone();

    // Parse query params manually from URI
    let query_params: ReadQueryParams = uri.query()
        .and_then(|q| serde_urlencoded::from_str(q).ok())
        .unwrap_or_default();

    get_or_head_handler_inner(state, headers, query_params, request).await
}

pub async fn get_or_head_handler(
    State(state): State<Arc<VolumeServerState>>,
    headers: HeaderMap,
    query: Query<ReadQueryParams>,
    request: Request<Body>,
) -> Response {
    get_or_head_handler_inner(state, headers, query.0, request).await
}

async fn get_or_head_handler_inner(
    state: Arc<VolumeServerState>,
    headers: HeaderMap,
    query: ReadQueryParams,
    request: Request<Body>,
) -> Response {
    let start = std::time::Instant::now();
    metrics::REQUEST_COUNTER.with_label_values(&["read"]).inc();

    let path = request.uri().path().to_string();
    let method = request.method().clone();

    let (vid, needle_id, cookie) = match parse_url_path(&path) {
        Some(parsed) => parsed,
        None => return (StatusCode::BAD_REQUEST, "invalid URL path").into_response(),
    };

    // JWT check for reads
    let token = extract_jwt(&headers, request.uri());
    if let Err(e) = state.guard.check_jwt(token.as_deref(), false) {
        return (StatusCode::UNAUTHORIZED, format!("JWT error: {}", e)).into_response();
    }

    // Read needle
    let mut n = Needle {
        id: needle_id,
        cookie,
        ..Needle::default()
    };

    let read_deleted = query.read_deleted.as_deref() == Some("true");

    let store = state.store.read().unwrap();
    match store.read_volume_needle_opt(vid, &mut n, read_deleted) {
        Ok(count) => {
            if count <= 0 {
                return StatusCode::NOT_FOUND.into_response();
            }
        }
        Err(crate::storage::volume::VolumeError::NotFound) => {
            return StatusCode::NOT_FOUND.into_response();
        }
        Err(crate::storage::volume::VolumeError::Deleted) => {
            return StatusCode::NOT_FOUND.into_response();
        }
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("read error: {}", e)).into_response();
        }
    }

    // Validate cookie
    if n.cookie != cookie {
        return StatusCode::NOT_FOUND.into_response();
    }

    // Build ETag
    let etag = format!("\"{}\"", n.etag());

    // Build Last-Modified header (RFC 1123 format) — must be done before conditional checks
    let last_modified_str = if n.last_modified > 0 {
        use chrono::{TimeZone, Utc};
        if let Some(dt) = Utc.timestamp_opt(n.last_modified as i64, 0).single() {
            Some(dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        } else {
            None
        }
    } else {
        None
    };

    // Check If-Modified-Since FIRST (Go checks this before If-None-Match)
    if n.last_modified > 0 {
        if let Some(ims_header) = headers.get(header::IF_MODIFIED_SINCE) {
            if let Ok(ims_str) = ims_header.to_str() {
                // Parse HTTP date format: "Mon, 02 Jan 2006 15:04:05 GMT"
                if let Ok(ims_time) = chrono::NaiveDateTime::parse_from_str(ims_str, "%a, %d %b %Y %H:%M:%S GMT") {
                    if (n.last_modified as i64) <= ims_time.and_utc().timestamp() {
                        return StatusCode::NOT_MODIFIED.into_response();
                    }
                }
            }
        }
    }

    // Check If-None-Match SECOND
    if let Some(if_none_match) = headers.get(header::IF_NONE_MATCH) {
        if let Ok(inm) = if_none_match.to_str() {
            if inm == etag || inm == "*" {
                return StatusCode::NOT_MODIFIED.into_response();
            }
        }
    }

    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::ETAG, etag.parse().unwrap());

    // Set Content-Type: use response-content-type query param override, else from needle mime
    let content_type = if let Some(ref ct) = query.response_content_type {
        ct.clone()
    } else if !n.mime.is_empty() {
        String::from_utf8_lossy(&n.mime).to_string()
    } else {
        "application/octet-stream".to_string()
    };
    response_headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());

    // Cache-Control override from query param
    if let Some(ref cc) = query.response_cache_control {
        response_headers.insert(header::CACHE_CONTROL, cc.parse().unwrap());
    }

    // Last-Modified
    if let Some(ref lm) = last_modified_str {
        response_headers.insert(header::LAST_MODIFIED, lm.parse().unwrap());
    }

    // Content-Disposition for download
    if query.dl.is_some() {
        // Extract filename from URL path
        let filename = extract_filename_from_path(&path);
        let disposition = if filename.is_empty() {
            "attachment".to_string()
        } else {
            format!("attachment; filename=\"{}\"", filename)
        };
        response_headers.insert(header::CONTENT_DISPOSITION, disposition.parse().unwrap());
    }

    // Handle compressed data: if needle is compressed, either pass through or decompress
    let is_compressed = n.is_compressed();
    let mut data = n.data;
    if is_compressed {
        let accept_encoding = headers.get(header::ACCEPT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if accept_encoding.contains("gzip") {
            response_headers.insert(header::CONTENT_ENCODING, "gzip".parse().unwrap());
        } else {
            // Decompress for client
            use flate2::read::GzDecoder;
            use std::io::Read as _;
            let mut decoder = GzDecoder::new(&data[..]);
            let mut decompressed = Vec::new();
            if decoder.read_to_end(&mut decompressed).is_ok() {
                data = decompressed;
            }
        }
    }

    // Accept-Ranges
    response_headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());

    // Check Range header
    if let Some(range_header) = headers.get(header::RANGE) {
        if let Ok(range_str) = range_header.to_str() {
            return handle_range_request(range_str, &data, response_headers);
        }
    }

    if method == Method::HEAD {
        response_headers.insert(header::CONTENT_LENGTH, data.len().to_string().parse().unwrap());
        return (StatusCode::OK, response_headers).into_response();
    }

    metrics::REQUEST_DURATION
        .with_label_values(&["read"])
        .observe(start.elapsed().as_secs_f64());

    (StatusCode::OK, response_headers, data).into_response()
}

/// Handle HTTP Range requests. Returns 206 Partial Content or 416 Range Not Satisfiable.
fn handle_range_request(range_str: &str, data: &[u8], mut headers: HeaderMap) -> Response {
    let total = data.len();

    // Parse "bytes=start-end"
    let range_spec = match range_str.strip_prefix("bytes=") {
        Some(s) => s,
        None => return (StatusCode::OK, headers, data.to_vec()).into_response(),
    };

    // Parse individual ranges
    let ranges: Vec<(usize, usize)> = range_spec
        .split(',')
        .filter_map(|part| {
            let part = part.trim();
            if let Some(pos) = part.find('-') {
                let start_str = &part[..pos];
                let end_str = &part[pos + 1..];

                if start_str.is_empty() {
                    // Suffix range: -N means last N bytes
                    let suffix: usize = end_str.parse().ok()?;
                    if suffix > total {
                        return None;
                    }
                    Some((total - suffix, total - 1))
                } else {
                    let start: usize = start_str.parse().ok()?;
                    let end = if end_str.is_empty() {
                        total - 1
                    } else {
                        end_str.parse().ok()?
                    };
                    Some((start, end))
                }
            } else {
                None
            }
        })
        .collect();

    if ranges.is_empty() {
        return (StatusCode::OK, headers, data.to_vec()).into_response();
    }

    // Check all ranges are valid
    for &(start, end) in &ranges {
        if start >= total || end >= total || start > end {
            headers.insert(
                "Content-Range",
                format!("bytes */{}", total).parse().unwrap(),
            );
            return (StatusCode::RANGE_NOT_SATISFIABLE, headers).into_response();
        }
    }

    // If combined range bytes exceed content size, ignore the range (return 200 empty)
    let combined_bytes: usize = ranges.iter().map(|&(s, e)| e - s + 1).sum();
    if combined_bytes > total {
        return (StatusCode::OK, headers).into_response();
    }

    if ranges.len() == 1 {
        let (start, end) = ranges[0];
        let slice = &data[start..=end];
        headers.insert(
            "Content-Range",
            format!("bytes {}-{}/{}", start, end, total).parse().unwrap(),
        );
        headers.insert(header::CONTENT_LENGTH, slice.len().to_string().parse().unwrap());
        (StatusCode::PARTIAL_CONTENT, headers, slice.to_vec()).into_response()
    } else {
        // Multi-range: build multipart/byteranges response
        let boundary = "SeaweedFSBoundary";
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();

        let mut body = Vec::new();
        for &(start, end) in &ranges {
            body.extend_from_slice(format!("\r\n--{}\r\n", boundary).as_bytes());
            body.extend_from_slice(
                format!("Content-Type: {}\r\n", content_type).as_bytes(),
            );
            body.extend_from_slice(
                format!("Content-Range: bytes {}-{}/{}\r\n\r\n", start, end, total).as_bytes(),
            );
            body.extend_from_slice(&data[start..=end]);
        }
        body.extend_from_slice(format!("\r\n--{}--\r\n", boundary).as_bytes());

        headers.insert(
            header::CONTENT_TYPE,
            format!("multipart/byteranges; boundary={}", boundary)
                .parse()
                .unwrap(),
        );
        headers.insert(header::CONTENT_LENGTH, body.len().to_string().parse().unwrap());
        (StatusCode::PARTIAL_CONTENT, headers, body).into_response()
    }
}

/// Extract filename from URL path like "/vid/fid/filename.ext"
fn extract_filename_from_path(path: &str) -> String {
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if parts.len() >= 3 {
        parts[2].to_string()
    } else {
        String::new()
    }
}

// ============================================================================
// Write Handler (POST/PUT)
// ============================================================================

#[derive(Serialize)]
struct UploadResult {
    name: String,
    size: u32,
    #[serde(rename = "eTag")]
    etag: String,
}

pub async fn post_handler(
    State(state): State<Arc<VolumeServerState>>,
    request: Request<Body>,
) -> Response {
    let start = std::time::Instant::now();
    metrics::REQUEST_COUNTER.with_label_values(&["write"]).inc();

    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or("").to_string();
    let headers = request.headers().clone();

    let (vid, needle_id, cookie) = match parse_url_path(&path) {
        Some(parsed) => parsed,
        None => return (StatusCode::BAD_REQUEST, "invalid URL path").into_response(),
    };

    // JWT check for writes
    let token = extract_jwt(&headers, request.uri());
    if let Err(e) = state.guard.check_jwt(token.as_deref(), true) {
        return (StatusCode::UNAUTHORIZED, format!("JWT error: {}", e)).into_response();
    }

    // Check for chunk manifest flag
    let is_chunk_manifest = query.split('&')
        .any(|p| p == "cm=true" || p == "cm=1");

    // Validate multipart/form-data has a boundary
    if let Some(ct) = headers.get(header::CONTENT_TYPE) {
        if let Ok(ct_str) = ct.to_str() {
            if ct_str.starts_with("multipart/form-data") && !ct_str.contains("boundary=") {
                return (StatusCode::BAD_REQUEST, "no multipart boundary param in Content-Type").into_response();
            }
        }
    }

    let content_md5 = headers.get("Content-MD5").and_then(|v| v.to_str().ok()).map(|s| s.to_string());

    // Read body
    let body = match axum::body::to_bytes(request.into_body(), usize::MAX).await {
        Ok(b) => b,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("read body: {}", e)).into_response(),
    };

    // Validate Content-MD5 if provided
    if let Some(ref expected_md5) = content_md5 {
        use md5::{Md5, Digest};
        use base64::Engine;
        let mut hasher = Md5::new();
        hasher.update(&body);
        let actual = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());
        if actual != *expected_md5 {
            return (StatusCode::BAD_REQUEST, format!("Content-MD5 mismatch: expected {} got {}", expected_md5, actual)).into_response();
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Check if upload is pre-compressed
    let is_gzipped = headers.get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|s| s == "gzip")
        .unwrap_or(false);

    let mut n = Needle {
        id: needle_id,
        cookie,
        data: body.to_vec(),
        data_size: body.len() as u32,
        last_modified: now,
        ..Needle::default()
    };
    n.set_has_last_modified_date();
    if is_chunk_manifest {
        n.set_is_chunk_manifest();
    }
    if is_gzipped {
        n.set_is_compressed();
    }

    let mut store = state.store.write().unwrap();
    match store.write_volume_needle(vid, &mut n) {
        Ok((_offset, _size, is_unchanged)) => {
            if is_unchanged {
                let etag = format!("\"{}\"", n.etag());
                return (StatusCode::NO_CONTENT, [(header::ETAG, etag)]).into_response();
            }

            let result = UploadResult {
                name: String::new(),
                size: n.data_size,
                etag: n.etag(),
            };
            metrics::REQUEST_DURATION
                .with_label_values(&["write"])
                .observe(start.elapsed().as_secs_f64());
            (StatusCode::CREATED, axum::Json(result)).into_response()
        }
        Err(crate::storage::volume::VolumeError::NotFound) => {
            (StatusCode::NOT_FOUND, "volume not found").into_response()
        }
        Err(crate::storage::volume::VolumeError::ReadOnly) => {
            (StatusCode::FORBIDDEN, "volume is read-only").into_response()
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("write error: {}", e)).into_response()
        }
    }
}

// ============================================================================
// Delete Handler
// ============================================================================

#[derive(Serialize)]
struct DeleteResult {
    size: i32,
}

pub async fn delete_handler(
    State(state): State<Arc<VolumeServerState>>,
    request: Request<Body>,
) -> Response {
    let start = std::time::Instant::now();
    metrics::REQUEST_COUNTER.with_label_values(&["delete"]).inc();

    let path = request.uri().path().to_string();
    let headers = request.headers().clone();

    let (vid, needle_id, cookie) = match parse_url_path(&path) {
        Some(parsed) => parsed,
        None => return (StatusCode::BAD_REQUEST, "invalid URL path").into_response(),
    };

    // JWT check for writes (deletes use write key)
    let token = extract_jwt(&headers, request.uri());
    if let Err(e) = state.guard.check_jwt(token.as_deref(), true) {
        return (StatusCode::UNAUTHORIZED, format!("JWT error: {}", e)).into_response();
    }

    let mut n = Needle {
        id: needle_id,
        cookie,
        ..Needle::default()
    };

    // Read needle first to validate cookie (matching Go behavior)
    let original_cookie = cookie;
    {
        let store = state.store.read().unwrap();
        match store.read_volume_needle(vid, &mut n) {
            Ok(_) => {}
            Err(_) => {
                let result = DeleteResult { size: 0 };
                return (StatusCode::NOT_FOUND, axum::Json(result)).into_response();
            }
        }
    }
    if n.cookie != original_cookie {
        return (StatusCode::BAD_REQUEST, "File Random Cookie does not match.").into_response();
    }

    let mut store = state.store.write().unwrap();
    match store.delete_volume_needle(vid, &mut n) {
        Ok(size) => {
            if size.0 == 0 {
                let result = DeleteResult { size: 0 };
                return (StatusCode::NOT_FOUND, axum::Json(result)).into_response();
            }
            metrics::REQUEST_DURATION
                .with_label_values(&["delete"])
                .observe(start.elapsed().as_secs_f64());
            let result = DeleteResult { size: size.0 };
            (StatusCode::ACCEPTED, axum::Json(result)).into_response()
        }
        Err(crate::storage::volume::VolumeError::NotFound) => {
            let result = DeleteResult { size: 0 };
            (StatusCode::NOT_FOUND, axum::Json(result)).into_response()
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("delete error: {}", e)).into_response()
        }
    }
}

// ============================================================================
// Status Handler
// ============================================================================

pub async fn status_handler(
    State(state): State<Arc<VolumeServerState>>,
) -> Response {
    let store = state.store.read().unwrap();
    let mut volumes = Vec::new();

    for loc in &store.locations {
        for (_vid, vol) in loc.volumes() {
            let mut vol_info = serde_json::Map::new();
            vol_info.insert("Id".to_string(), serde_json::Value::from(vol.id.0));
            vol_info.insert("Collection".to_string(), serde_json::Value::from(vol.collection.clone()));
            vol_info.insert("Size".to_string(), serde_json::Value::from(vol.content_size()));
            vol_info.insert("FileCount".to_string(), serde_json::Value::from(vol.file_count()));
            vol_info.insert("DeleteCount".to_string(), serde_json::Value::from(vol.deleted_count()));
            vol_info.insert("ReadOnly".to_string(), serde_json::Value::from(vol.is_read_only()));
            vol_info.insert("Version".to_string(), serde_json::Value::from(vol.version().0));
            volumes.push(serde_json::Value::Object(vol_info));
        }
    }

    // Build disk statuses
    let mut disk_statuses = Vec::new();
    for loc in &store.locations {
        let dir = &loc.directory;
        let mut ds = serde_json::Map::new();
        ds.insert("dir".to_string(), serde_json::Value::from(dir.clone()));
        // Add disk stats if available
        if let Ok(path) = std::path::Path::new(&dir).canonicalize() {
            ds.insert("dir".to_string(), serde_json::Value::from(path.to_string_lossy().to_string()));
        }
        disk_statuses.push(serde_json::Value::Object(ds));
    }

    let mut m = serde_json::Map::new();
    m.insert("Version".to_string(), serde_json::Value::from(env!("CARGO_PKG_VERSION")));
    m.insert("Volumes".to_string(), serde_json::Value::Array(volumes));
    m.insert("DiskStatuses".to_string(), serde_json::Value::Array(disk_statuses));

    axum::Json(serde_json::Value::Object(m)).into_response()
}

// ============================================================================
// Health Check Handler
// ============================================================================

pub async fn healthz_handler(
    State(state): State<Arc<VolumeServerState>>,
) -> Response {
    let is_stopping = *state.is_stopping.read().unwrap();
    if is_stopping {
        return (StatusCode::SERVICE_UNAVAILABLE, "stopping").into_response();
    }
    StatusCode::OK.into_response()
}

// ============================================================================
// Metrics Handler
// ============================================================================

pub async fn metrics_handler() -> Response {
    let body = metrics::gather_metrics();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
        .into_response()
}

// ============================================================================
// Static Asset Handlers
// ============================================================================

pub async fn favicon_handler() -> Response {
    // Return a minimal valid ICO (1x1 transparent)
    let ico = include_bytes!("favicon.ico");
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "image/x-icon")],
        ico.as_ref(),
    )
        .into_response()
}

pub async fn static_asset_handler() -> Response {
    // Return a minimal valid PNG (1x1 transparent)
    let png: &[u8] = &[
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
        0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00,
        0x00, 0x1F, 0x15, 0xC4, 0x89, 0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41, 0x54, 0x78,
        0x9C, 0x62, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0xE5, 0x27, 0xDE, 0xFC, 0x00, 0x00,
        0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
    ];
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "image/png")],
        png,
    )
        .into_response()
}

pub async fn ui_handler(
    State(state): State<Arc<VolumeServerState>>,
    headers: HeaderMap,
) -> Response {
    // If JWT signing is enabled, require auth
    let token = extract_jwt(&headers, &axum::http::Uri::from_static("/ui/index.html"));
    if let Err(e) = state.guard.check_jwt(token.as_deref(), false) {
        if state.guard.has_read_signing_key() {
            return (StatusCode::UNAUTHORIZED, format!("JWT error: {}", e)).into_response();
        }
    }

    let html = r#"<!DOCTYPE html>
<html><head><title>SeaweedFS Volume Server</title></head>
<body><h1>SeaweedFS Volume Server</h1><p>Rust implementation</p></body></html>"#;
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
        .into_response()
}

// ============================================================================
// Helpers
// ============================================================================

/// Extract JWT token from query param, Authorization header, or Cookie.
/// Query param takes precedence over header, header over cookie.
fn extract_jwt(headers: &HeaderMap, uri: &axum::http::Uri) -> Option<String> {
    // 1. Check ?jwt= query parameter
    if let Some(query) = uri.query() {
        for pair in query.split('&') {
            if let Some(value) = pair.strip_prefix("jwt=") {
                if !value.is_empty() {
                    return Some(value.to_string());
                }
            }
        }
    }

    // 2. Check Authorization: Bearer <token>
    if let Some(auth) = headers.get(header::AUTHORIZATION) {
        if let Ok(auth_str) = auth.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                return Some(token.to_string());
            }
        }
    }

    // 3. Check Cookie
    if let Some(cookie_header) = headers.get(header::COOKIE) {
        if let Ok(cookie_str) = cookie_header.to_str() {
            for cookie in cookie_str.split(';') {
                let cookie = cookie.trim();
                if let Some(value) = cookie.strip_prefix("jwt=") {
                    if !value.is_empty() {
                        return Some(value.to_string());
                    }
                }
            }
        }
    }

    None
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_url_path_comma() {
        let (vid, nid, cookie) = parse_url_path("/3,01637037d6").unwrap();
        assert_eq!(vid, VolumeId(3));
        assert_eq!(nid, NeedleId(0x01));
        assert_eq!(cookie, Cookie(0x637037d6));
    }

    #[test]
    fn test_parse_url_path_with_ext() {
        let (vid, _, _) = parse_url_path("/3,01637037d6.jpg").unwrap();
        assert_eq!(vid, VolumeId(3));
    }

    #[test]
    fn test_parse_url_path_slash() {
        let result = parse_url_path("3/01637037d6");
        assert!(result.is_some());
    }

    #[test]
    fn test_parse_url_path_slash_with_filename() {
        let result = parse_url_path("3/01637037d6/report.txt");
        assert!(result.is_some());
        let (vid, _, _) = result.unwrap();
        assert_eq!(vid, VolumeId(3));
    }

    #[test]
    fn test_parse_url_path_invalid() {
        assert!(parse_url_path("/invalid").is_none());
        assert!(parse_url_path("").is_none());
    }

    #[test]
    fn test_extract_jwt_bearer() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer abc123".parse().unwrap());
        let uri: axum::http::Uri = "/test".parse().unwrap();
        assert_eq!(extract_jwt(&headers, &uri), Some("abc123".to_string()));
    }

    #[test]
    fn test_extract_jwt_query_param() {
        let headers = HeaderMap::new();
        let uri: axum::http::Uri = "/test?jwt=mytoken".parse().unwrap();
        assert_eq!(extract_jwt(&headers, &uri), Some("mytoken".to_string()));
    }

    #[test]
    fn test_extract_jwt_query_over_header() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer header_token".parse().unwrap());
        let uri: axum::http::Uri = "/test?jwt=query_token".parse().unwrap();
        assert_eq!(extract_jwt(&headers, &uri), Some("query_token".to_string()));
    }

    #[test]
    fn test_extract_jwt_none() {
        let headers = HeaderMap::new();
        let uri: axum::http::Uri = "/test".parse().unwrap();
        assert_eq!(extract_jwt(&headers, &uri), None);
    }

    #[test]
    fn test_handle_range_single() {
        let data = b"hello world";
        let headers = HeaderMap::new();
        let resp = handle_range_request("bytes=0-4", data, headers);
        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    }

    #[test]
    fn test_handle_range_invalid() {
        let data = b"hello";
        let headers = HeaderMap::new();
        let resp = handle_range_request("bytes=999-1000", data, headers);
        assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    }
}
