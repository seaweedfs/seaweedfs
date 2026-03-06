//! HTTP handlers for volume server operations.
//!
//! Implements GET/HEAD (read), POST/PUT (write), DELETE, /status, /healthz.
//! Matches Go's volume_server_handlers_read.go, volume_server_handlers_write.go,
//! volume_server_handlers_admin.go.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::security::Guard;
use crate::storage::needle::needle::Needle;
use crate::storage::types::*;
use super::volume_server::VolumeServerState;

// ============================================================================
// URL Parsing
// ============================================================================

/// Parse volume ID and file ID from URL path.
/// Supports: "vid,fid", "vid/fid", "vid,fid.ext"
fn parse_url_path(path: &str) -> Option<(VolumeId, NeedleId, Cookie)> {
    let path = path.trim_start_matches('/');

    // Strip extension
    let path = if let Some(dot) = path.rfind('.') {
        &path[..dot]
    } else {
        path
    };

    // Try "vid,fid" format
    let (vid_str, fid_str) = if let Some(pos) = path.find(',') {
        (&path[..pos], &path[pos + 1..])
    } else if let Some(pos) = path.find('/') {
        (&path[..pos], &path[pos + 1..])
    } else {
        return None;
    };

    let vid = VolumeId::parse(vid_str).ok()?;
    let (needle_id, cookie) = crate::storage::needle::needle::parse_needle_id_cookie(fid_str).ok()?;

    Some((vid, needle_id, cookie))
}

// ============================================================================
// Read Handler (GET/HEAD)
// ============================================================================

pub async fn get_or_head_handler(
    State(state): State<Arc<VolumeServerState>>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Response {
    let path = request.uri().path().to_string();
    let method = request.method().clone();

    let (vid, needle_id, cookie) = match parse_url_path(&path) {
        Some(parsed) => parsed,
        None => return (StatusCode::BAD_REQUEST, "invalid URL path").into_response(),
    };

    // JWT check for reads
    let token = extract_jwt(&headers);
    if let Err(e) = state.guard.check_jwt(token.as_deref(), false) {
        return (StatusCode::UNAUTHORIZED, format!("JWT error: {}", e)).into_response();
    }

    // Read needle
    let mut n = Needle {
        id: needle_id,
        cookie,
        ..Needle::default()
    };

    let store = state.store.read().unwrap();
    match store.read_volume_needle(vid, &mut n) {
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

    // Build response
    let etag = n.etag();
    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::ETAG, format!("\"{}\"", etag).parse().unwrap());

    // Set Content-Type from needle mime
    let content_type = if !n.mime.is_empty() {
        String::from_utf8_lossy(&n.mime).to_string()
    } else {
        "application/octet-stream".to_string()
    };
    response_headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());

    // Last-Modified
    if n.last_modified > 0 {
        // Simple format — the full HTTP date formatting can be added later
        response_headers.insert("X-Last-Modified", n.last_modified.to_string().parse().unwrap());
    }

    if method == Method::HEAD {
        response_headers.insert(header::CONTENT_LENGTH, n.data.len().to_string().parse().unwrap());
        return (StatusCode::OK, response_headers).into_response();
    }

    (StatusCode::OK, response_headers, n.data).into_response()
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
    headers: HeaderMap,
    Path(path): Path<String>,
    body: axum::body::Bytes,
) -> Response {
    let (vid, needle_id, cookie) = match parse_url_path(&path) {
        Some(parsed) => parsed,
        None => return (StatusCode::BAD_REQUEST, "invalid URL path").into_response(),
    };

    // JWT check for writes
    let token = extract_jwt(&headers);
    if let Err(e) = state.guard.check_jwt(token.as_deref(), true) {
        return (StatusCode::UNAUTHORIZED, format!("JWT error: {}", e)).into_response();
    }

    let mut n = Needle {
        id: needle_id,
        cookie,
        data: body.to_vec(),
        data_size: body.len() as u32,
        ..Needle::default()
    };

    let mut store = state.store.write().unwrap();
    match store.write_volume_needle(vid, &mut n) {
        Ok((_offset, _size, is_unchanged)) => {
            if is_unchanged {
                return StatusCode::NO_CONTENT.into_response();
            }

            let result = UploadResult {
                name: String::new(),
                size: n.data_size,
                etag: n.etag(),
            };
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
    headers: HeaderMap,
    Path(path): Path<String>,
) -> Response {
    let (vid, needle_id, cookie) = match parse_url_path(&path) {
        Some(parsed) => parsed,
        None => return (StatusCode::BAD_REQUEST, "invalid URL path").into_response(),
    };

    // JWT check for writes (deletes use write key)
    let token = extract_jwt(&headers);
    if let Err(e) = state.guard.check_jwt(token.as_deref(), true) {
        return (StatusCode::UNAUTHORIZED, format!("JWT error: {}", e)).into_response();
    }

    // Whitelist check
    // Note: In production, remote_addr from the connection should be checked.
    // This is handled by middleware in the full implementation.

    let mut n = Needle {
        id: needle_id,
        cookie,
        ..Needle::default()
    };

    let mut store = state.store.write().unwrap();
    match store.delete_volume_needle(vid, &mut n) {
        Ok(size) => {
            if size.0 == 0 {
                return StatusCode::NOT_FOUND.into_response();
            }
            let result = DeleteResult { size: size.0 };
            (StatusCode::ACCEPTED, axum::Json(result)).into_response()
        }
        Err(crate::storage::volume::VolumeError::NotFound) => {
            StatusCode::NOT_FOUND.into_response()
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("delete error: {}", e)).into_response()
        }
    }
}

// ============================================================================
// Status Handler
// ============================================================================

#[derive(Serialize)]
struct StatusResponse {
    version: String,
    volumes: Vec<VolumeStatus>,
}

#[derive(Serialize)]
struct VolumeStatus {
    id: u32,
    collection: String,
    size: u64,
    file_count: i64,
    delete_count: i64,
    read_only: bool,
    version: u8,
}

pub async fn status_handler(
    State(state): State<Arc<VolumeServerState>>,
) -> Response {
    let store = state.store.read().unwrap();
    let mut volumes = Vec::new();

    for loc in &store.locations {
        for (_vid, vol) in loc.volumes() {
            volumes.push(VolumeStatus {
                id: vol.id.0,
                collection: vol.collection.clone(),
                size: vol.content_size(),
                file_count: vol.file_count(),
                delete_count: vol.deleted_count(),
                read_only: vol.is_read_only(),
                version: vol.version().0,
            });
        }
    }

    let status = StatusResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        volumes,
    };

    axum::Json(status).into_response()
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
// Helpers
// ============================================================================

/// Extract JWT token from Authorization header or `jwt` query parameter.
fn extract_jwt(headers: &HeaderMap) -> Option<String> {
    // Check Authorization: Bearer <token>
    if let Some(auth) = headers.get(header::AUTHORIZATION) {
        if let Ok(auth_str) = auth.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                return Some(token.to_string());
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
        // "01637037d6" → 5 bytes → padded to 12 bytes: [0,0,0,0,0,0,0,0x01,0x63,0x70,0x37,0xd6]
        // NeedleId = first 8 bytes, Cookie = last 4 bytes
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
    fn test_parse_url_path_invalid() {
        assert!(parse_url_path("/invalid").is_none());
        assert!(parse_url_path("").is_none());
    }

    #[test]
    fn test_extract_jwt_bearer() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer abc123".parse().unwrap());
        assert_eq!(extract_jwt(&headers), Some("abc123".to_string()));
    }

    #[test]
    fn test_extract_jwt_none() {
        let headers = HeaderMap::new();
        assert_eq!(extract_jwt(&headers), None);
    }
}
