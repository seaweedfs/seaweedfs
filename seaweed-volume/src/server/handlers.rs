//! HTTP handlers for volume server operations.
//!
//! Implements GET/HEAD (read), POST/PUT (write), DELETE, /status, /healthz.
//! Matches Go's volume_server_handlers_read.go, volume_server_handlers_write.go,
//! volume_server_handlers_admin.go.

use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use super::volume_server::{normalize_outgoing_http_url, VolumeServerState};
use crate::config::ReadMode;
use crate::metrics;
use crate::storage::needle::needle::Needle;
use crate::storage::types::*;

// ============================================================================
// Inflight Throttle Guard
// ============================================================================

/// RAII guard that subtracts bytes from an atomic counter and notifies waiters on drop.
struct InflightGuard<'a> {
    counter: &'a std::sync::atomic::AtomicI64,
    bytes: i64,
    notify: &'a tokio::sync::Notify,
    metric: &'a prometheus::IntGauge,
}

impl<'a> Drop for InflightGuard<'a> {
    fn drop(&mut self) {
        let new_val = self.counter.fetch_sub(self.bytes, Ordering::Relaxed) - self.bytes;
        self.metric.set(new_val);
        self.notify.notify_waiters();
    }
}

/// Body wrapper that tracks download inflight bytes and releases them when dropped.
struct TrackedBody {
    data: Vec<u8>,
    state: Arc<VolumeServerState>,
    bytes: i64,
}

impl http_body::Body for TrackedBody {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if self.data.is_empty() {
            return std::task::Poll::Ready(None);
        }
        let data = std::mem::take(&mut self.data);
        std::task::Poll::Ready(Some(Ok(http_body::Frame::data(bytes::Bytes::from(data)))))
    }
}

impl Drop for TrackedBody {
    fn drop(&mut self) {
        let new_val = self
            .state
            .inflight_download_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed)
            - self.bytes;
        metrics::INFLIGHT_DOWNLOAD_SIZE.set(new_val);
        self.state.download_notify.notify_waiters();
    }
}

// ============================================================================
// Streaming Body for Large Files
// ============================================================================

/// Threshold in bytes above which we stream needle data instead of buffering.
const STREAMING_THRESHOLD: u32 = 1024 * 1024; // 1 MB

/// Default chunk size for streaming reads from the dat file.
const DEFAULT_STREAMING_CHUNK_SIZE: usize = 64 * 1024; // 64 KB

/// A body that streams needle data from the dat file in chunks using pread,
/// avoiding loading the entire payload into memory at once.
struct StreamingBody {
    dat_file: std::fs::File,
    data_offset: u64,
    data_size: u32,
    pos: usize,
    chunk_size: usize,
    data_file_access_control: Arc<crate::storage::volume::DataFileAccessControl>,
    hold_read_lock_for_stream: bool,
    _held_read_lease: Option<crate::storage::volume::DataFileReadLease>,
    /// Pending result from spawn_blocking, polled to completion.
    pending: Option<tokio::task::JoinHandle<Result<bytes::Bytes, std::io::Error>>>,
    /// For download throttling — released on drop.
    state: Option<Arc<VolumeServerState>>,
    tracked_bytes: i64,
    /// Server state used to re-lookup needle offset if compaction occurs during streaming.
    server_state: Arc<VolumeServerState>,
    /// Volume ID for compaction-revision re-lookup.
    volume_id: crate::storage::types::VolumeId,
    /// Needle ID for compaction-revision re-lookup.
    needle_id: crate::storage::types::NeedleId,
    /// Compaction revision at the time of the initial read; if the volume's revision
    /// changes between chunks, the needle may have moved and we must re-lookup its offset.
    compaction_revision: u16,
}

impl http_body::Body for StreamingBody {
    type Data = bytes::Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        loop {
            // If we have a pending read, poll it
            if let Some(ref mut handle) = self.pending {
                match std::pin::Pin::new(handle).poll(cx) {
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                    std::task::Poll::Ready(result) => {
                        self.pending = None;
                        match result {
                            Ok(Ok(chunk)) => {
                                let len = chunk.len();
                                self.pos += len;
                                return std::task::Poll::Ready(Some(Ok(http_body::Frame::data(
                                    chunk,
                                ))));
                            }
                            Ok(Err(e)) => return std::task::Poll::Ready(Some(Err(e))),
                            Err(e) => {
                                return std::task::Poll::Ready(Some(Err(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    e,
                                ))))
                            }
                        }
                    }
                }
            }

            let total = self.data_size as usize;
            if self.pos >= total {
                return std::task::Poll::Ready(None);
            }

            // Check if compaction has changed the needle's disk location (Go parity:
            // readNeedleDataInto re-reads the needle offset when CompactionRevision changes).
            let relookup_result = {
                let store = self.server_state.store.read().unwrap();
                if let Some((_, vol)) = store.find_volume(self.volume_id) {
                    if vol.super_block.compaction_revision != self.compaction_revision {
                        // Compaction occurred — re-lookup the needle's data offset
                        Some(vol.re_lookup_needle_data_offset(self.needle_id))
                    } else {
                        None
                    }
                } else {
                    None
                }
            };
            if let Some(result) = relookup_result {
                match result {
                    Ok((new_offset, new_rev)) => {
                        self.data_offset = new_offset;
                        self.compaction_revision = new_rev;
                    }
                    Err(_) => {
                        return std::task::Poll::Ready(Some(Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "needle not found after compaction",
                        ))));
                    }
                }
            }

            let chunk_len = std::cmp::min(self.chunk_size, total - self.pos);
            let file_offset = self.data_offset + self.pos as u64;

            let file_clone = match self.dat_file.try_clone() {
                Ok(f) => f,
                Err(e) => return std::task::Poll::Ready(Some(Err(e))),
            };
            let data_file_access_control = self.data_file_access_control.clone();
            let hold_read_lock_for_stream = self.hold_read_lock_for_stream;

            let handle = tokio::task::spawn_blocking(move || {
                let _lease = if hold_read_lock_for_stream {
                    None
                } else {
                    Some(data_file_access_control.read_lock())
                };
                let mut buf = vec![0u8; chunk_len];
                #[cfg(unix)]
                {
                    use std::os::unix::fs::FileExt;
                    file_clone.read_exact_at(&mut buf, file_offset)?;
                }
                #[cfg(windows)]
                {
                    use std::os::windows::fs::FileExt;
                    file_clone.seek_read(&mut buf, file_offset)?;
                }
                Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from(buf))
            });

            self.pending = Some(handle);
            // Loop back to poll the newly created future
        }
    }
}

impl Drop for StreamingBody {
    fn drop(&mut self) {
        if let Some(ref st) = self.state {
            let new_val = st
                .inflight_download_bytes
                .fetch_sub(self.tracked_bytes, Ordering::Relaxed)
                - self.tracked_bytes;
            metrics::INFLIGHT_DOWNLOAD_SIZE.set(new_val);
            st.download_notify.notify_waiters();
        }
    }
}

// ============================================================================
// URL Parsing
// ============================================================================

/// Parse volume ID and file ID from URL path.
/// Supports: "vid,fid", "vid/fid", "vid,fid.ext", "vid/fid/filename.ext"
/// Extract the file_id string (e.g., "3,01637037d6") from a URL path for JWT validation.
fn extract_file_id(path: &str) -> String {
    let path = path.trim_start_matches('/');
    // Strip extension and filename after second slash
    if let Some(comma) = path.find(',') {
        let after_comma = &path[comma + 1..];
        let fid_part = if let Some(slash) = after_comma.find('/') {
            &after_comma[..slash]
        } else if let Some(dot) = after_comma.rfind('.') {
            &after_comma[..dot]
        } else {
            after_comma
        };
        // Strip "_suffix" from fid (Go does this for filenames appended with underscore)
        let fid_part = if let Some(underscore) = fid_part.rfind('_') {
            &fid_part[..underscore]
        } else {
            fid_part
        };
        format!("{},{}", &path[..comma], fid_part)
    } else {
        path.to_string()
    }
}

fn streaming_chunk_size(read_buffer_size_bytes: usize, data_size: usize) -> usize {
    std::cmp::min(
        read_buffer_size_bytes.max(DEFAULT_STREAMING_CHUNK_SIZE),
        data_size.max(1),
    )
}

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
    let (needle_id, cookie) =
        crate::storage::needle::needle::parse_needle_id_cookie(fid_str).ok()?;

    Some((vid, needle_id, cookie))
}

// ============================================================================
// Volume Lookup + Proxy/Redirect
// ============================================================================

/// A volume location returned by master lookup.
#[derive(Debug, Deserialize)]
struct VolumeLocation {
    url: String,
    #[serde(rename = "publicUrl")]
    public_url: String,
}

/// Master /dir/lookup response.
#[derive(Debug, Deserialize)]
struct LookupResult {
    #[serde(default)]
    locations: Option<Vec<VolumeLocation>>,
    #[serde(default)]
    error: Option<String>,
}

/// Look up volume locations from the master via HTTP /dir/lookup.
async fn lookup_volume(
    client: &reqwest::Client,
    scheme: &str,
    master_url: &str,
    volume_id: u32,
) -> Result<Vec<VolumeLocation>, String> {
    let url = normalize_outgoing_http_url(
        scheme,
        &format!("{}/dir/lookup?volumeId={}", master_url, volume_id),
    )?;
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("lookup request failed: {}", e))?;
    let result: LookupResult = resp
        .json()
        .await
        .map_err(|e| format!("lookup parse failed: {}", e))?;
    if let Some(err) = result.error {
        if !err.is_empty() {
            return Err(err);
        }
    }
    Ok(result.locations.unwrap_or_default())
}

/// Helper to synchronously replicate a request to peer volume servers.
async fn do_replicated_request(
    state: &VolumeServerState,
    vid: u32,
    method: axum::http::Method,
    path: &str,
    query: &str,
    headers: &axum::http::HeaderMap,
    body: Option<bytes::Bytes>,
) -> Result<(), String> {
    let locations = lookup_volume(
        &state.http_client,
        &state.outgoing_http_scheme,
        &state.master_url,
        vid,
    )
    .await
    .map_err(|e| format!("lookup volume failed: {}", e))?;

    let remote_locations: Vec<_> = locations
        .into_iter()
        .filter(|loc| loc.url != state.self_url && loc.public_url != state.self_url)
        .collect();

    if remote_locations.is_empty() {
        return Ok(());
    }

    let new_query = if query.is_empty() {
        String::from("type=replicate")
    } else {
        format!("{}&type=replicate", query)
    };

    let mut futures = Vec::new();
    for loc in remote_locations {
        let url = normalize_outgoing_http_url(
            &state.outgoing_http_scheme,
            &format!("{}{}?{}", loc.url, path, new_query),
        )?;
        let client = state.http_client.clone();

        let mut req_builder = client.request(method.clone(), &url);

        // Forward relevant headers
        if let Some(ct) = headers.get(axum::http::header::CONTENT_TYPE) {
            req_builder = req_builder.header(axum::http::header::CONTENT_TYPE, ct);
        }
        if let Some(ce) = headers.get(axum::http::header::CONTENT_ENCODING) {
            req_builder = req_builder.header(axum::http::header::CONTENT_ENCODING, ce);
        }
        if let Some(md5) = headers.get("Content-MD5") {
            req_builder = req_builder.header("Content-MD5", md5);
        }
        if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION) {
            req_builder = req_builder.header(axum::http::header::AUTHORIZATION, auth);
        }

        if let Some(ref b) = body {
            req_builder = req_builder.body(b.clone());
        }

        futures.push(async move {
            match req_builder.send().await {
                Ok(r) if r.status().is_success() => Ok(()),
                Ok(r) => Err(format!("{} returned status {}", url, r.status())),
                Err(e) => Err(format!("{} failed: {}", url, e)),
            }
        });
    }

    let results = futures::future::join_all(futures).await;
    let mut errors = Vec::new();
    for res in results {
        if let Err(e) = res {
            errors.push(e);
        }
    }

    if !errors.is_empty() {
        return Err(errors.join(", "));
    }

    Ok(())
}

/// Extracted request info needed for proxy/redirect (avoids borrowing Request across await).
struct ProxyRequestInfo {
    original_headers: HeaderMap,
    original_query: String,
    path: String,
    vid_str: String,
    fid_str: String,
}

/// Handle proxy or redirect for a non-local volume read.
async fn proxy_or_redirect_to_target(
    state: &VolumeServerState,
    info: ProxyRequestInfo,
    vid: VolumeId,
) -> Response {
    // Look up volume locations from master
    let locations = match lookup_volume(
        &state.http_client,
        &state.outgoing_http_scheme,
        &state.master_url,
        vid.0,
    )
    .await
    {
        Ok(locs) => locs,
        Err(e) => {
            tracing::warn!("volume lookup failed for {}: {}", vid.0, e);
            return StatusCode::NOT_FOUND.into_response();
        }
    };

    if locations.is_empty() {
        return StatusCode::NOT_FOUND.into_response();
    }

    // Filter out self, then shuffle remaining
    let mut candidates: Vec<&VolumeLocation> = locations
        .iter()
        .filter(|loc| !loc.url.contains(&state.self_url))
        .collect();

    if candidates.is_empty() {
        return StatusCode::NOT_FOUND.into_response();
    }

    // Shuffle for load balancing
    if candidates.len() >= 2 {
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        candidates.shuffle(&mut rng);
    }

    let target = candidates[0];

    match state.read_mode {
        ReadMode::Proxy => proxy_request(state, &info, target).await,
        ReadMode::Redirect => redirect_request(&info, target, &state.outgoing_http_scheme),
        ReadMode::Local => unreachable!(),
    }
}

/// Proxy the request to the target volume server.
async fn proxy_request(
    state: &VolumeServerState,
    info: &ProxyRequestInfo,
    target: &VolumeLocation,
) -> Response {
    // Build target URL, adding proxied=true query param
    let path = info.path.trim_start_matches('/');

    let raw_target = if info.original_query.is_empty() {
        format!("{}/{}?proxied=true", target.url, path)
    } else {
        format!(
            "{}/{}?{}&proxied=true",
            target.url, path, info.original_query
        )
    };
    let target_url = match normalize_outgoing_http_url(&state.outgoing_http_scheme, &raw_target) {
        Ok(url) => url,
        Err(e) => {
            tracing::warn!("proxy target url {} invalid: {}", raw_target, e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Build the proxy request
    let mut req_builder = state.http_client.get(&target_url);

    // Forward all original headers
    for (name, value) in &info.original_headers {
        if let Ok(v) = value.to_str() {
            req_builder = req_builder.header(name.as_str(), v);
        }
    }

    let resp = match req_builder.send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("proxy request to {} failed: {}", target_url, e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Build response, copying headers and body from remote
    let status =
        StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response_headers = HeaderMap::new();
    for (name, value) in resp.headers() {
        if name.as_str().eq_ignore_ascii_case("server") {
            continue;
        }
        response_headers.insert(name.clone(), value.clone());
    }

    let body_bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("proxy response read failed: {}", e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let mut response = Response::new(Body::from(body_bytes));
    *response.status_mut() = status;
    *response.headers_mut() = response_headers;
    response
}

/// Return a redirect response to the target volume server.
fn redirect_request(info: &ProxyRequestInfo, target: &VolumeLocation, scheme: &str) -> Response {
    // Build query string: preserve collection, add proxied=true, drop readDeleted (Go parity)
    let mut query_params = Vec::new();
    if !info.original_query.is_empty() {
        for param in info.original_query.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                if key == "collection" {
                    query_params.push(format!("collection={}", value));
                }
                // Intentionally drop readDeleted and other params (Go parity)
            }
        }
    }
    query_params.push("proxied=true".to_string());
    let query = query_params.join("&");

    let raw_target = format!(
        "{}/{},{}?{}",
        target.public_url, &info.vid_str, &info.fid_str, query
    );
    let location = match normalize_outgoing_http_url(scheme, &raw_target) {
        Ok(url) => url,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    Response::builder()
        .status(StatusCode::MOVED_PERMANENTLY)
        .header("Location", &location)
        .body(Body::from(format!(
            "<a href=\"{}\">Moved Permanently</a>.\n\n",
            location
        )))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
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
    /// cm=false disables chunk manifest expansion (returns raw manifest JSON).
    pub cm: Option<String>,
    /// Image resize width
    pub width: Option<u32>,
    /// Image resize height
    pub height: Option<u32>,
    /// Image resize mode: "fit" or "fill"
    pub mode: Option<String>,
    /// Image crop parameters
    pub crop_x1: Option<u32>,
    pub crop_y1: Option<u32>,
    pub crop_x2: Option<u32>,
    pub crop_y2: Option<u32>,
    /// S3 response passthrough headers
    #[serde(rename = "response-content-encoding")]
    pub response_content_encoding: Option<String>,
    #[serde(rename = "response-expires")]
    pub response_expires: Option<String>,
    #[serde(rename = "response-content-language")]
    pub response_content_language: Option<String>,
    #[serde(rename = "response-content-disposition")]
    pub response_content_disposition: Option<String>,
    /// Pretty print JSON response
    pub pretty: Option<String>,
    /// JSONP callback function name
    pub callback: Option<String>,
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
    let query_params: ReadQueryParams = uri
        .query()
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
    let path = request.uri().path().to_string();
    let method = request.method().clone();

    // JWT check for reads — must happen BEFORE path parsing to match Go behavior.
    // Go's GetOrHeadHandler calls maybeCheckJwtAuthorization before NewVolumeId,
    // so invalid paths with JWT enabled return 401, not 400.
    let file_id = extract_file_id(&path);
    let token = extract_jwt(&headers, request.uri());
    if let Err(e) =
        state
            .guard
            .read()
            .unwrap()
            .check_jwt_for_file(token.as_deref(), &file_id, false)
    {
        return (StatusCode::UNAUTHORIZED, format!("JWT error: {}", e)).into_response();
    }

    let (vid, needle_id, cookie) = match parse_url_path(&path) {
        Some(parsed) => parsed,
        None => return (StatusCode::BAD_REQUEST, "invalid URL path").into_response(),
    };

    // Check if volume exists locally; if not, proxy/redirect based on read_mode.
    // This mirrors Go's hasVolume check in GetOrHeadHandler.
    // NOTE: The RwLockReadGuard must be dropped before any .await to keep the future Send.
    let has_volume = state.store.read().unwrap().has_volume(vid);

    if !has_volume {
        // Check if already proxied (loop prevention)
        let query_string = request.uri().query().unwrap_or("").to_string();
        let is_proxied = query_string.contains("proxied=true");

        if is_proxied || state.read_mode == ReadMode::Local || state.master_url.is_empty() {
            return StatusCode::NOT_FOUND.into_response();
        }

        // Extract vid_str and fid_str from path for redirect URL construction.
        // For redirect, fid must be stripped of extension (Go parity: parseURLPath returns raw fid).
        let trimmed = path.trim_start_matches('/');
        let (vid_str, fid_str) = if let Some(pos) = trimmed.find(',') {
            let raw_fid = &trimmed[pos + 1..];
            // Strip filename after slash: "fid/filename.ext" -> "fid"
            let fid = if let Some(slash) = raw_fid.find('/') {
                &raw_fid[..slash]
            } else if let Some(dot) = raw_fid.rfind('.') {
                // Strip extension: "fid.ext" -> "fid"
                &raw_fid[..dot]
            } else {
                raw_fid
            };
            (trimmed[..pos].to_string(), fid.to_string())
        } else if let Some(pos) = trimmed.find('/') {
            let after = &trimmed[pos + 1..];
            let fid_part = if let Some(slash) = after.find('/') {
                &after[..slash]
            } else {
                after
            };
            (trimmed[..pos].to_string(), fid_part.to_string())
        } else {
            return StatusCode::NOT_FOUND.into_response();
        };

        let info = ProxyRequestInfo {
            original_headers: request.headers().clone(),
            original_query: query_string,
            path: path.clone(),
            vid_str,
            fid_str,
        };

        return proxy_or_redirect_to_target(&state, info, vid).await;
    }

    // Download throttling
    let download_guard = if state.concurrent_download_limit > 0 {
        let timeout = if state.inflight_download_data_timeout.is_zero() {
            std::time::Duration::from_secs(2)
        } else {
            state.inflight_download_data_timeout
        };
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let current = state.inflight_download_bytes.load(Ordering::Relaxed);
            if current < state.concurrent_download_limit {
                break;
            }
            if tokio::time::timeout_at(deadline, state.download_notify.notified())
                .await
                .is_err()
            {
                metrics::HANDLER_COUNTER
                    .with_label_values(&[metrics::DOWNLOAD_LIMIT_COND])
                    .inc();
                return (StatusCode::TOO_MANY_REQUESTS, "download limit exceeded").into_response();
            }
        }
        // We'll set the actual bytes after reading the needle (once we know the size)
        Some(state.clone())
    } else {
        None
    };

    // Read needle — first do a meta-only read to check if streaming is appropriate
    let mut n = Needle {
        id: needle_id,
        cookie,
        ..Needle::default()
    };

    let read_deleted = query.read_deleted.as_deref() == Some("true");
    let has_range = headers.contains_key(header::RANGE);
    let ext = extract_extension_from_path(&path);
    let is_image = is_image_ext(&ext);
    let has_image_ops = query.width.is_some()
        || query.height.is_some()
        || query.crop_x1.is_some()
        || query.crop_y1.is_some();

    // Try meta-only read first for potential streaming
    let store = state.store.read().unwrap();
    let stream_info = store.read_volume_needle_stream_info(vid, &mut n, read_deleted);
    let stream_info = match stream_info {
        Ok(info) => Some(info),
        Err(crate::storage::volume::VolumeError::NotFound) => {
            metrics::HANDLER_COUNTER
                .with_label_values(&[metrics::ERROR_GET_NOT_FOUND])
                .inc();
            return StatusCode::NOT_FOUND.into_response();
        }
        Err(crate::storage::volume::VolumeError::Deleted) => {
            metrics::HANDLER_COUNTER
                .with_label_values(&[metrics::ERROR_GET_NOT_FOUND])
                .inc();
            return StatusCode::NOT_FOUND.into_response();
        }
        Err(e) => {
            metrics::HANDLER_COUNTER
                .with_label_values(&[metrics::ERROR_GET_INTERNAL])
                .inc();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("read error: {}", e),
            )
                .into_response();
        }
    };
    drop(store);

    // Validate cookie
    if n.cookie != cookie {
        return StatusCode::NOT_FOUND.into_response();
    }

    // Determine if we can stream (large, uncompressed, not manifest, not image needing ops, no range)
    let bypass_cm = query.cm.as_deref() == Some("false");
    let can_stream = stream_info.is_some()
        && n.data_size > STREAMING_THRESHOLD
        && !n.is_compressed()
        && !(n.is_chunk_manifest() && !bypass_cm)
        && !(is_image && has_image_ops)
        && !has_range
        && method != Method::HEAD;

    // For chunk manifest or any non-streaming path, we need the full data.
    // If we can't stream, do a full read now.
    if !can_stream {
        // Re-read with full data
        let mut n_full = Needle {
            id: needle_id,
            cookie,
            ..Needle::default()
        };
        let store = state.store.read().unwrap();
        match store.read_volume_needle_opt(vid, &mut n_full, read_deleted) {
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
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("read error: {}", e),
                )
                    .into_response();
            }
        }
        drop(store);
        // Use the full needle from here (it has the same metadata + data)
        n = n_full;
    }

    // Build ETag and Last-Modified BEFORE conditional checks and chunk manifest expansion
    // (matches Go order: conditional checks first, then chunk manifest)
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
                if let Ok(ims_time) =
                    chrono::NaiveDateTime::parse_from_str(ims_str, "%a, %d %b %Y %H:%M:%S GMT")
                {
                    if (n.last_modified as i64) <= ims_time.and_utc().timestamp() {
                        let mut not_modified_headers = HeaderMap::new();
                        not_modified_headers.insert(header::ETAG, etag.parse().unwrap());
                        if let Some(ref lm) = last_modified_str {
                            not_modified_headers.insert(header::LAST_MODIFIED, lm.parse().unwrap());
                        }
                        return (StatusCode::NOT_MODIFIED, not_modified_headers).into_response();
                    }
                }
            }
        }
    }

    // Check If-None-Match SECOND
    if let Some(if_none_match) = headers.get(header::IF_NONE_MATCH) {
        if let Ok(inm) = if_none_match.to_str() {
            if inm == etag {
                let mut not_modified_headers = HeaderMap::new();
                not_modified_headers.insert(header::ETAG, etag.parse().unwrap());
                if let Some(ref lm) = last_modified_str {
                    not_modified_headers.insert(header::LAST_MODIFIED, lm.parse().unwrap());
                }
                return (StatusCode::NOT_MODIFIED, not_modified_headers).into_response();
            }
        }
    }

    // Chunk manifest expansion (needs full data) — after conditional checks, before response
    if n.is_chunk_manifest() && !bypass_cm {
        if let Some(resp) = try_expand_chunk_manifest(&state, &n, &headers, &method) {
            return resp;
        }
        // If manifest expansion fails (invalid JSON etc.), fall through to raw data
    }

    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::ETAG, etag.parse().unwrap());

    // H1: Emit pairs as response headers
    if n.has_pairs() && !n.pairs.is_empty() {
        if let Ok(pair_map) =
            serde_json::from_slice::<std::collections::HashMap<String, String>>(&n.pairs)
        {
            for (k, v) in &pair_map {
                if let (Ok(hname), Ok(hval)) = (
                    axum::http::HeaderName::from_bytes(k.as_bytes()),
                    axum::http::HeaderValue::from_str(v),
                ) {
                    response_headers.insert(hname, hval);
                }
            }
        }
    }

    // H8: Use needle stored name when URL path has no filename (only vid,fid)
    let mut filename = extract_filename_from_path(&path);
    let mut ext = ext;
    if n.name_size > 0 && filename.is_empty() {
        filename = String::from_utf8_lossy(&n.name).to_string();
        if ext.is_empty() {
            if let Some(dot_pos) = filename.rfind('.') {
                ext = filename[dot_pos..].to_lowercase();
            }
        }
    }

    // H6: Determine Content-Type: filter application/octet-stream, use mime_guess
    // For chunk manifests, skip extension-based MIME override — use stored MIME as-is (Go parity)
    let content_type = if let Some(ref ct) = query.response_content_type {
        Some(ct.clone())
    } else if n.is_chunk_manifest() {
        // Chunk manifests: use the stored MIME directly without filtering or extension detection
        if !n.mime.is_empty() {
            Some(String::from_utf8_lossy(&n.mime).to_string())
        } else {
            None
        }
    } else {
        // Get MIME from needle, but filter out application/octet-stream
        let needle_mime = if !n.mime.is_empty() {
            let mt = String::from_utf8_lossy(&n.mime).to_string();
            if mt.starts_with("application/octet-stream") {
                String::new()
            } else {
                mt
            }
        } else {
            String::new()
        };

        if !needle_mime.is_empty() {
            Some(needle_mime)
        } else {
            // Fall through to extension-based detection
            let detect_ext = if !ext.is_empty() {
                ext.clone()
            } else if !filename.is_empty() {
                if let Some(dot_pos) = filename.rfind('.') {
                    filename[dot_pos..].to_lowercase()
                } else {
                    String::new()
                }
            } else {
                String::new()
            };
            if !detect_ext.is_empty() {
                mime_guess::from_ext(detect_ext.trim_start_matches('.'))
                    .first()
                    .map(|m| m.to_string())
            } else {
                None // Omit Content-Type entirely
            }
        }
    };
    if let Some(ref ct) = content_type {
        response_headers.insert(header::CONTENT_TYPE, ct.parse().unwrap());
    }

    // Cache-Control override from query param
    if let Some(ref cc) = query.response_cache_control {
        response_headers.insert(header::CACHE_CONTROL, cc.parse().unwrap());
    }

    // S3 response passthrough headers
    if let Some(ref ce) = query.response_content_encoding {
        response_headers.insert(header::CONTENT_ENCODING, ce.parse().unwrap());
    }
    if let Some(ref exp) = query.response_expires {
        response_headers.insert(header::EXPIRES, exp.parse().unwrap());
    }
    if let Some(ref cl) = query.response_content_language {
        response_headers.insert("Content-Language", cl.parse().unwrap());
    }
    if let Some(ref cd) = query.response_content_disposition {
        response_headers.insert(header::CONTENT_DISPOSITION, cd.parse().unwrap());
    }

    // Last-Modified
    if let Some(ref lm) = last_modified_str {
        response_headers.insert(header::LAST_MODIFIED, lm.parse().unwrap());
    }

    // H7: Content-Disposition — inline by default, attachment only when dl is truthy
    // Only set if not already set by response-content-disposition query param
    if !response_headers.contains_key(header::CONTENT_DISPOSITION) && !filename.is_empty() {
        let disposition_type = if let Some(ref dl_val) = query.dl {
            if parse_go_bool(dl_val).unwrap_or(false) {
                "attachment"
            } else {
                "inline"
            }
        } else {
            "inline"
        };
        let disposition = format!("{}; filename=\"{}\"", disposition_type, filename);
        if let Ok(hval) = disposition.parse() {
            response_headers.insert(header::CONTENT_DISPOSITION, hval);
        }
    }

    // ---- Streaming path: large uncompressed files ----
    if can_stream {
        if let Some(info) = stream_info {
            response_headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
            response_headers.insert(
                header::CONTENT_LENGTH,
                info.data_size.to_string().parse().unwrap(),
            );

            let tracked_bytes = info.data_size as i64;
            let tracking_state = if download_guard.is_some() {
                let new_val = state
                    .inflight_download_bytes
                    .fetch_add(tracked_bytes, Ordering::Relaxed)
                    + tracked_bytes;
                metrics::INFLIGHT_DOWNLOAD_SIZE.set(new_val);
                Some(state.clone())
            } else {
                None
            };

            let streaming = StreamingBody {
                dat_file: info.dat_file,
                data_offset: info.data_file_offset,
                data_size: info.data_size,
                pos: 0,
                chunk_size: streaming_chunk_size(
                    state.read_buffer_size_bytes,
                    info.data_size as usize,
                ),
                _held_read_lease: if state.has_slow_read {
                    None
                } else {
                    Some(info.data_file_access_control.read_lock())
                },
                data_file_access_control: info.data_file_access_control,
                hold_read_lock_for_stream: !state.has_slow_read,
                pending: None,
                state: tracking_state,
                tracked_bytes,
                server_state: state.clone(),
                volume_id: info.volume_id,
                needle_id: info.needle_id,
                compaction_revision: info.compaction_revision,
            };

            let body = Body::new(streaming);
            let mut resp = Response::new(body);
            *resp.status_mut() = StatusCode::OK;
            *resp.headers_mut() = response_headers;
            return resp;
        }
    }

    // ---- Buffered path: small files, compressed, images, range requests ----

    // Handle compressed data: if needle is compressed, either pass through or decompress
    let is_compressed = n.is_compressed();
    let mut data = n.data;

    // Check if image operations are needed — must decompress first regardless of Accept-Encoding
    let needs_image_ops =
        is_image && (query.width.is_some() || query.height.is_some() || query.mode.is_some());

    if is_compressed {
        if needs_image_ops {
            // Always decompress for image operations (Go decompresses before resize/crop)
            use flate2::read::GzDecoder;
            use std::io::Read as _;
            let mut decoder = GzDecoder::new(&data[..]);
            let mut decompressed = Vec::new();
            if decoder.read_to_end(&mut decompressed).is_ok() {
                data = decompressed;
            }
        } else {
            let accept_encoding = headers
                .get(header::ACCEPT_ENCODING)
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
    }

    // Image crop and resize (only for supported image formats)
    if is_image {
        data = maybe_crop_image(&data, &ext, &query);
        data = maybe_resize_image(&data, &ext, &query);
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
        response_headers.insert(
            header::CONTENT_LENGTH,
            data.len().to_string().parse().unwrap(),
        );
        return (StatusCode::OK, response_headers).into_response();
    }

    // If download throttling is active, wrap the body so we track when it's fully sent
    if download_guard.is_some() {
        let data_len = data.len() as i64;
        let new_val = state
            .inflight_download_bytes
            .fetch_add(data_len, Ordering::Relaxed)
            + data_len;
        metrics::INFLIGHT_DOWNLOAD_SIZE.set(new_val);
        let tracked_body = TrackedBody {
            data,
            state: state.clone(),
            bytes: data_len,
        };
        let body = Body::new(tracked_body);
        let mut resp = Response::new(body);
        *resp.status_mut() = StatusCode::OK;
        *resp.headers_mut() = response_headers;
        return resp;
    }

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
                    let mut suffix: usize = end_str.parse().ok()?;
                    // Go clamps suffix to file size
                    if suffix > total {
                        suffix = total;
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

    // Clamp range ends and validate (Go clamps end to size-1 instead of returning 416)
    let ranges: Vec<(usize, usize)> = ranges
        .into_iter()
        .map(|(start, mut end)| {
            if end >= total {
                end = total - 1;
            }
            (start, end)
        })
        .collect();
    for &(start, end) in &ranges {
        if start >= total || start > end {
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
            format!("bytes {}-{}/{}", start, end, total)
                .parse()
                .unwrap(),
        );
        headers.insert(
            header::CONTENT_LENGTH,
            slice.len().to_string().parse().unwrap(),
        );
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
        for (i, &(start, end)) in ranges.iter().enumerate() {
            // First boundary has no leading CRLF per RFC 2046
            if i == 0 {
                body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
            } else {
                body.extend_from_slice(format!("\r\n--{}\r\n", boundary).as_bytes());
            }
            body.extend_from_slice(format!("Content-Type: {}\r\n", content_type).as_bytes());
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
        headers.insert(
            header::CONTENT_LENGTH,
            body.len().to_string().parse().unwrap(),
        );
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

fn parse_go_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "TRUE" | "True" | "true" => Some(true),
        "0" | "f" | "F" | "FALSE" | "False" | "false" => Some(false),
        _ => None,
    }
}

// ============================================================================
// Image processing helpers
// ============================================================================

fn is_image_ext(ext: &str) -> bool {
    matches!(ext, ".png" | ".jpg" | ".jpeg" | ".gif" | ".webp")
}

fn extract_extension_from_path(path: &str) -> String {
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if parts.len() >= 3 {
        let filename = parts[2];
        if let Some(dot_pos) = filename.rfind('.') {
            return filename[dot_pos..].to_lowercase();
        }
    }
    String::new()
}

fn maybe_resize_image(data: &[u8], ext: &str, query: &ReadQueryParams) -> Vec<u8> {
    let width = query.width.unwrap_or(0);
    let height = query.height.unwrap_or(0);
    if width == 0 && height == 0 {
        return data.to_vec();
    }

    let img = match image::load_from_memory(data) {
        Ok(img) => img,
        Err(_) => return data.to_vec(),
    };

    let (src_w, src_h) = (img.width(), img.height());
    // Only resize if source is larger than target
    if (width == 0 || src_w <= width) && (height == 0 || src_h <= height) {
        return data.to_vec();
    }

    let mode = query.mode.as_deref().unwrap_or("");
    let resized = match mode {
        "fit" => img.resize(width, height, image::imageops::FilterType::Lanczos3),
        "fill" => img.resize_to_fill(width, height, image::imageops::FilterType::Lanczos3),
        _ => {
            if width > 0 && height > 0 && width == height && src_w != src_h {
                img.resize_to_fill(width, height, image::imageops::FilterType::Lanczos3)
            } else {
                img.resize(width, height, image::imageops::FilterType::Lanczos3)
            }
        }
    };

    encode_image(&resized, ext).unwrap_or_else(|| data.to_vec())
}

fn maybe_crop_image(data: &[u8], ext: &str, query: &ReadQueryParams) -> Vec<u8> {
    let (x1, y1, x2, y2) = match (query.crop_x1, query.crop_y1, query.crop_x2, query.crop_y2) {
        (Some(x1), Some(y1), Some(x2), Some(y2)) if x2 > x1 && y2 > y1 => (x1, y1, x2, y2),
        _ => return data.to_vec(),
    };

    let img = match image::load_from_memory(data) {
        Ok(img) => img,
        Err(_) => return data.to_vec(),
    };

    let (src_w, src_h) = (img.width(), img.height());
    if x2 > src_w || y2 > src_h {
        return data.to_vec();
    }

    let cropped = img.crop_imm(x1, y1, x2 - x1, y2 - y1);
    encode_image(&cropped, ext).unwrap_or_else(|| data.to_vec())
}

fn encode_image(img: &image::DynamicImage, ext: &str) -> Option<Vec<u8>> {
    use std::io::Cursor;
    let mut buf = Cursor::new(Vec::new());
    let format = match ext {
        ".png" => image::ImageFormat::Png,
        ".jpg" | ".jpeg" => image::ImageFormat::Jpeg,
        ".gif" => image::ImageFormat::Gif,
        ".webp" => image::ImageFormat::WebP,
        _ => return None,
    };
    img.write_to(&mut buf, format).ok()?;
    Some(buf.into_inner())
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
    #[serde(skip_serializing_if = "Option::is_none")]
    mime: Option<String>,
    #[serde(rename = "contentMd5", skip_serializing_if = "Option::is_none")]
    content_md5: Option<String>,
}

pub async fn post_handler(
    State(state): State<Arc<VolumeServerState>>,
    request: Request<Body>,
) -> Response {
    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or("").to_string();
    let headers = request.headers().clone();
    let query_fields: Vec<(String, String)> =
        serde_urlencoded::from_str(&query).unwrap_or_default();

    let (vid, needle_id, cookie) = match parse_url_path(&path) {
        Some(parsed) => parsed,
        None => {
            return json_error_with_query(StatusCode::BAD_REQUEST, "invalid URL path", Some(&query))
        }
    };

    // JWT check for writes
    let file_id = extract_file_id(&path);
    let token = extract_jwt(&headers, request.uri());
    if let Err(e) = state
        .guard
        .read()
        .unwrap()
        .check_jwt_for_file(token.as_deref(), &file_id, true)
    {
        return json_error_with_query(
            StatusCode::UNAUTHORIZED,
            format!("JWT error: {}", e),
            Some(&query),
        );
    }

    // Upload throttling: check inflight bytes against limit
    let is_replicate = query.split('&').any(|p| p == "type=replicate");
    let content_length = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);

    if !is_replicate && state.concurrent_upload_limit > 0 {
        // Wait for inflight bytes to drop below limit, or timeout
        let timeout = if state.inflight_upload_data_timeout.is_zero() {
            std::time::Duration::from_secs(2)
        } else {
            state.inflight_upload_data_timeout
        };
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let current = state.inflight_upload_bytes.load(Ordering::Relaxed);
            if current < state.concurrent_upload_limit {
                break;
            }
            // Wait for notification or timeout
            if tokio::time::timeout_at(deadline, state.upload_notify.notified())
                .await
                .is_err()
            {
                metrics::HANDLER_COUNTER
                    .with_label_values(&[metrics::UPLOAD_LIMIT_COND])
                    .inc();
                return json_error_with_query(
                    StatusCode::TOO_MANY_REQUESTS,
                    "upload limit exceeded",
                    Some(&query),
                );
            }
        }
        let new_val = state
            .inflight_upload_bytes
            .fetch_add(content_length, Ordering::Relaxed)
            + content_length;
        metrics::INFLIGHT_UPLOAD_SIZE.set(new_val);
    }

    // RAII guard to release upload throttle on any exit path
    let _upload_guard = if !is_replicate && state.concurrent_upload_limit > 0 {
        Some(InflightGuard {
            counter: &state.inflight_upload_bytes,
            bytes: content_length,
            notify: &state.upload_notify,
            metric: &metrics::INFLIGHT_UPLOAD_SIZE,
        })
    } else {
        None
    };

    // Validate multipart/form-data has a boundary
    if let Some(ct) = headers.get(header::CONTENT_TYPE) {
        if let Ok(ct_str) = ct.to_str() {
            if ct_str.starts_with("multipart/form-data") && !ct_str.contains("boundary=") {
                return json_error_with_query(
                    StatusCode::BAD_REQUEST,
                    "no multipart boundary param in Content-Type",
                    Some(&query),
                );
            }
        }
    }

    let content_md5 = headers
        .get("Content-MD5")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Read body
    let body = match axum::body::to_bytes(request.into_body(), usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            return json_error_with_query(
                StatusCode::BAD_REQUEST,
                format!("read body: {}", e),
                Some(&query),
            )
        }
    };

    // H5: Multipart form-data parsing
    let content_type_str = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let (
        body_data_raw,
        parsed_filename,
        parsed_content_type,
        parsed_content_encoding,
        parsed_content_md5,
        multipart_form_fields,
    ) = if content_type_str.starts_with("multipart/form-data") {
        // Extract boundary from Content-Type
        let boundary = content_type_str
            .split(';')
            .find_map(|part| {
                let part = part.trim();
                if let Some(val) = part.strip_prefix("boundary=") {
                    Some(val.trim_matches('"').to_string())
                } else {
                    None
                }
            })
            .unwrap_or_default();

        let mut multipart = multer::Multipart::new(
            futures::stream::once(async { Ok::<_, std::io::Error>(body.clone()) }),
            boundary,
        );

        let mut file_data: Option<Vec<u8>> = None;
        let mut file_name: Option<String> = None;
        let mut file_content_type: Option<String> = None;
        let mut file_content_encoding: Option<String> = None;
        let mut file_content_md5: Option<String> = None;
        let mut form_fields = std::collections::HashMap::new();

        while let Ok(Some(field)) = multipart.next_field().await {
            let field_name = field.name().map(|s| s.to_string());
            let fname = field.file_name().map(|s| {
                // Clean Windows backslashes
                let cleaned = s.replace('\\', "/");
                cleaned.rsplit('/').next().unwrap_or(&cleaned).to_string()
            });
            let fct = field.content_type().map(|m| m.to_string());
            let field_headers = field.headers().clone();
            let fce = field_headers
                .get(header::CONTENT_ENCODING)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            let fmd5 = field_headers
                .get("Content-MD5")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            if let Ok(data) = field.bytes().await {
                if file_data.is_none() && fname.is_some() {
                    // First file field
                    file_data = Some(data.to_vec());
                    file_name = fname;
                    file_content_type = fct;
                    file_content_encoding = fce;
                    file_content_md5 = fmd5;
                } else if let Some(name) = field_name {
                    form_fields
                        .entry(name)
                        .or_insert_with(|| String::from_utf8_lossy(&data).to_string());
                }
            }
        }

        if let Some(data) = file_data {
            (
                data,
                file_name.unwrap_or_default(),
                file_content_type,
                file_content_encoding,
                file_content_md5,
                form_fields,
            )
        } else {
            // No file field found, use raw body
            (body.to_vec(), String::new(), None, None, None, form_fields)
        }
    } else {
        (
            body.to_vec(),
            String::new(),
            None,
            None,
            None,
            std::collections::HashMap::new(),
        )
    };

    let form_value = |name: &str| {
        query_fields
            .iter()
            .find_map(|(k, v)| if k == name { Some(v.clone()) } else { None })
            .or_else(|| multipart_form_fields.get(name).cloned())
    };

    // Check for chunk manifest flag.
    // Go uses r.FormValue("cm"), which falls back to multipart fields when present.
    let is_chunk_manifest = matches!(
        form_value("cm").as_deref(),
        Some("1" | "t" | "T" | "TRUE" | "True" | "true")
    );

    // Check file size limit
    if state.file_size_limit_bytes > 0 && body_data_raw.len() as i64 > state.file_size_limit_bytes {
        return json_error_with_query(
            StatusCode::BAD_REQUEST,
            "file size limit exceeded",
            Some(&query),
        );
    }

    // Validate Content-MD5 if provided
    let content_md5 = content_md5.or(parsed_content_md5);
    if let Some(ref expected_md5) = content_md5 {
        use base64::Engine;
        use md5::{Digest, Md5};
        let mut hasher = Md5::new();
        hasher.update(&body_data_raw);
        let actual = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());
        if actual != *expected_md5 {
            return json_error_with_query(
                StatusCode::BAD_REQUEST,
                format!(
                    "Content-MD5 mismatch: expected {} got {}",
                    expected_md5, actual
                ),
                Some(&query),
            );
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Parse custom timestamp from query param
    let ts_str = form_value("ts").unwrap_or_default();
    let last_modified = if !ts_str.is_empty() {
        ts_str.parse::<u64>().unwrap_or(now)
    } else {
        now
    };

    // Check if upload is pre-compressed
    let is_gzipped = if content_type_str.starts_with("multipart/form-data") {
        parsed_content_encoding.as_deref() == Some("gzip")
    } else {
        headers
            .get(header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .map(|s| s == "gzip")
            .unwrap_or(false)
    };

    // Prefer the multipart filename before deriving MIME and other metadata.
    let filename = if !parsed_filename.is_empty() {
        parsed_filename
    } else {
        extract_filename_from_path(&path)
    };

    // Extract MIME type: prefer multipart-parsed content type, else from Content-Type header
    let mime_type = if let Some(ref pct) = parsed_content_type {
        pct.clone()
    } else {
        let multipart_fallback = if content_type_str.starts_with("multipart/form-data")
            && !filename.is_empty()
            && !is_chunk_manifest
        {
            mime_guess::from_path(&filename)
                .first()
                .map(|m| m.to_string())
                .unwrap_or_default()
        } else {
            String::new()
        };
        headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|ct| {
                if ct.starts_with("multipart/") {
                    multipart_fallback.clone()
                } else {
                    ct.to_string()
                }
            })
            .unwrap_or(multipart_fallback)
    };

    // Parse TTL from query param (matches Go's r.FormValue("ttl"))
    let ttl_str = form_value("ttl").unwrap_or_default();
    let ttl = if !ttl_str.is_empty() {
        crate::storage::needle::TTL::read(&ttl_str).ok()
    } else {
        None
    };

    // Extract Seaweed-* custom metadata headers (pairs)
    let pair_map: std::collections::HashMap<String, String> = headers
        .iter()
        .filter_map(|(k, v)| {
            let key = k.as_str();
            if key.len() > 8 && key[..8].eq_ignore_ascii_case("seaweed-") {
                if let Ok(val) = v.to_str() {
                    // Store with the prefix stripped (matching Go's trimmedPairMap)
                    Some((key[8..].to_string(), val.to_string()))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    // Fix JPEG orientation from EXIF data before storing (matches Go behavior).
    // Only for non-compressed uploads that are JPEG files.
    let body_data = if state.fix_jpg_orientation
        && !is_gzipped
        && crate::images::is_jpeg(&mime_type, &path)
    {
        crate::images::fix_jpg_orientation(&body_data_raw)
    } else {
        body_data_raw
    };

    // H3: Capture original data size BEFORE auto-compression
    let original_data_size = body_data.len() as u32;

    // H1: Compute Content-MD5 from uncompressed data BEFORE auto-compression
    let original_content_md5 = {
        use base64::Engine;
        use md5::{Digest, Md5};
        let mut hasher = Md5::new();
        hasher.update(&body_data);
        base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
    };

    // Auto-compress compressible file types (matches Go's IsCompressableFileType).
    // Only compress if not already gzipped and compression saves >10%.
    let (final_data, final_is_gzipped) = if !is_gzipped && !is_chunk_manifest {
        let ext = {
            let dot_pos = path.rfind('.');
            dot_pos
                .map(|p| path[p..].to_lowercase())
                .unwrap_or_default()
        };
        if is_compressible_file_type(&ext, &mime_type) {
            if let Some(compressed) = try_gzip_data(&body_data) {
                if compressed.len() * 10 < body_data.len() * 9 {
                    (compressed, true)
                } else {
                    (body_data, false)
                }
            } else {
                (body_data, false)
            }
        } else {
            (body_data, false)
        }
    } else {
        (body_data, is_gzipped)
    };

    let mut n = Needle {
        id: needle_id,
        cookie,
        data_size: final_data.len() as u32,
        data: final_data,
        last_modified: last_modified,
        ..Needle::default()
    };
    n.set_has_last_modified_date();
    if is_chunk_manifest {
        n.set_is_chunk_manifest();
    }
    if final_is_gzipped {
        n.set_is_compressed();
    }

    if !mime_type.is_empty() {
        n.mime = mime_type.as_bytes().to_vec();
        n.set_has_mime();
    }

    // Set TTL on needle
    if let Some(ref t) = ttl {
        if !t.is_empty() {
            n.ttl = Some(*t);
            n.set_has_ttl();
        }
    }

    // Set pairs on needle
    if !pair_map.is_empty() {
        if let Ok(pairs_json) = serde_json::to_vec(&pair_map) {
            if pairs_json.len() < 65536 {
                n.pairs_size = pairs_json.len() as u16;
                n.pairs = pairs_json;
                n.set_has_pairs();
            }
        }
    }

    // Set filename on needle (matches Go: n.Name = []byte(pu.FileName))
    if !filename.is_empty() && filename.len() < 256 {
        n.name = filename.as_bytes().to_vec();
        n.name_size = filename.len() as u8;
        n.set_has_name();
    }

    let write_result = if let Some(wq) = state.write_queue.get() {
        wq.submit(vid, n.clone()).await
    } else {
        let mut store = state.store.write().unwrap();
        store.write_volume_needle(vid, &mut n)
    };

    // Replicate to remote volume servers if this volume has replicas.
    // Matches Go's GetWritableRemoteReplications: skip if copy_count == 1.
    if !is_replicate && write_result.is_ok() && !state.master_url.is_empty() {
        let needs_replication = {
            let store = state.store.read().unwrap();
            store.find_volume(vid).map_or(false, |(_, v)| {
                v.super_block.replica_placement.get_copy_count() > 1
            })
        };
        if needs_replication {
            if let Err(e) = do_replicated_request(
                &state,
                vid.0,
                Method::POST,
                &path,
                &query,
                &headers,
                Some(body.clone()),
            )
            .await
            {
                tracing::error!("replicated write failed: {}", e);
                return json_error_with_query(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("replication failed: {}", e),
                    Some(&query),
                );
            }
        }
    }

    let resp = match write_result {
        Ok((_offset, _size, is_unchanged)) => {
            if is_unchanged {
                let etag = format!("\"{}\"", n.etag());
                (StatusCode::NO_CONTENT, [(header::ETAG, etag)]).into_response()
            } else {
                // H2: Use Content-MD5 computed from original uncompressed data
                let content_md5_value = original_content_md5;
                let result = UploadResult {
                    name: filename.clone(),
                    size: original_data_size, // H3: use original size, not compressed
                    etag: n.etag(),
                    mime: if mime_type.is_empty() {
                        None
                    } else {
                        Some(mime_type.clone())
                    },
                    content_md5: Some(content_md5_value.clone()),
                };
                let etag = n.etag();
                let etag_header = if etag.starts_with('"') {
                    etag.clone()
                } else {
                    format!("\"{}\"", etag)
                };
                let mut resp = (StatusCode::CREATED, axum::Json(result)).into_response();
                resp.headers_mut()
                    .insert(header::ETAG, etag_header.parse().unwrap());
                resp.headers_mut()
                    .insert("Content-MD5", content_md5_value.parse().unwrap());
                resp
            }
        }
        Err(crate::storage::volume::VolumeError::NotFound) => {
            json_error_with_query(StatusCode::NOT_FOUND, "volume not found", Some(&query))
        }
        Err(crate::storage::volume::VolumeError::ReadOnly) => {
            json_error_with_query(StatusCode::FORBIDDEN, "volume is read-only", Some(&query))
        }
        Err(e) => {
            metrics::HANDLER_COUNTER
                .with_label_values(&[metrics::ERROR_WRITE_TO_LOCAL_DISK])
                .inc();
            json_error_with_query(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("write error: {}", e),
                Some(&query),
            )
        }
    };

    // _upload_guard drops here, releasing inflight bytes
    resp
}

// ============================================================================
// Delete Handler
// ============================================================================

#[derive(Serialize)]
struct DeleteResult {
    size: i64,
}

pub async fn delete_handler(
    State(state): State<Arc<VolumeServerState>>,
    request: Request<Body>,
) -> Response {
    let path = request.uri().path().to_string();
    let del_query = request.uri().query().unwrap_or("").to_string();
    let headers = request.headers().clone();

    let (vid, needle_id, cookie) = match parse_url_path(&path) {
        Some(parsed) => parsed,
        None => {
            return json_error_with_query(
                StatusCode::BAD_REQUEST,
                "invalid URL path",
                Some(&del_query),
            )
        }
    };

    // JWT check for writes (deletes use write key)
    let file_id = extract_file_id(&path);
    let token = extract_jwt(&headers, request.uri());
    if let Err(e) = state
        .guard
        .read()
        .unwrap()
        .check_jwt_for_file(token.as_deref(), &file_id, true)
    {
        return json_error_with_query(
            StatusCode::UNAUTHORIZED,
            format!("JWT error: {}", e),
            Some(&del_query),
        );
    }

    // H9: Parse custom timestamp from query param; default to now (not 0)
    let del_ts_str = del_query
        .split('&')
        .find_map(|p| p.strip_prefix("ts="))
        .unwrap_or("");
    let del_last_modified = if !del_ts_str.is_empty() {
        del_ts_str.parse::<u64>().unwrap_or_else(|_| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        })
    } else {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    };

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
        return json_error_with_query(
            StatusCode::BAD_REQUEST,
            "File Random Cookie does not match.",
            Some(&del_query),
        );
    }

    // Apply custom timestamp (always set — defaults to now per H9)
    n.last_modified = del_last_modified;
    n.set_has_last_modified_date();

    // If this is a chunk manifest, delete child chunks first
    if n.is_chunk_manifest() {
        let manifest_data = if n.is_compressed() {
            use flate2::read::GzDecoder;
            use std::io::Read as _;
            let mut decoder = GzDecoder::new(&n.data[..]);
            let mut decompressed = Vec::new();
            if decoder.read_to_end(&mut decompressed).is_ok() {
                decompressed
            } else {
                n.data.clone()
            }
        } else {
            n.data.clone()
        };

        if let Ok(manifest) = serde_json::from_slice::<ChunkManifest>(&manifest_data) {
            // Delete all child chunks first
            for chunk in &manifest.chunks {
                let (chunk_vid, chunk_nid, chunk_cookie) = match parse_url_path(&chunk.fid) {
                    Some(p) => p,
                    None => {
                        return json_error_with_query(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("invalid chunk fid: {}", chunk.fid),
                            Some(&del_query),
                        );
                    }
                };
                let mut chunk_needle = Needle {
                    id: chunk_nid,
                    cookie: chunk_cookie,
                    ..Needle::default()
                };
                // Read the chunk to validate it exists
                {
                    let store = state.store.read().unwrap();
                    if let Err(e) = store.read_volume_needle(chunk_vid, &mut chunk_needle) {
                        return json_error_with_query(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("read chunk {}: {}", chunk.fid, e),
                            Some(&del_query),
                        );
                    }
                }
                // Delete the chunk
                let mut store = state.store.write().unwrap();
                if let Err(e) = store.delete_volume_needle(chunk_vid, &mut chunk_needle) {
                    return json_error_with_query(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("delete chunk {}: {}", chunk.fid, e),
                        Some(&del_query),
                    );
                }
            }
            // Delete the manifest itself
            let mut store = state.store.write().unwrap();
            if let Err(e) = store.delete_volume_needle(vid, &mut n) {
                return json_error_with_query(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("delete manifest: {}", e),
                    Some(&del_query),
                );
            }
            // Return the manifest's declared size (matches Go behavior)
            let result = DeleteResult {
                size: manifest.size as i64,
            };
            return (StatusCode::ACCEPTED, axum::Json(result)).into_response();
        }
    }

    let delete_result = {
        let mut store = state.store.write().unwrap();
        store.delete_volume_needle(vid, &mut n)
    };

    let is_replicate = del_query.split('&').any(|p| p == "type=replicate");
    if !is_replicate && delete_result.is_ok() && !state.master_url.is_empty() {
        let needs_replication = {
            let store = state.store.read().unwrap();
            store.find_volume(vid).map_or(false, |(_, v)| {
                v.super_block.replica_placement.get_copy_count() > 1
            })
        };
        if needs_replication {
            if let Err(e) = do_replicated_request(
                &state,
                vid.0,
                Method::DELETE,
                &path,
                &del_query,
                &headers,
                None,
            )
            .await
            {
                tracing::error!("replicated delete failed: {}", e);
                return json_error_with_query(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("replication failed: {}", e),
                    Some(&del_query),
                );
            }
        }
    }

    match delete_result {
        Ok(size) => {
            let result = DeleteResult {
                size: size.0 as i64,
            };
            (StatusCode::ACCEPTED, axum::Json(result)).into_response()
        }
        Err(crate::storage::volume::VolumeError::NotFound) => {
            let result = DeleteResult { size: 0 };
            (StatusCode::NOT_FOUND, axum::Json(result)).into_response()
        }
        Err(e) => json_error_with_query(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("delete error: {}", e),
            Some(&del_query),
        ),
    }
}

// ============================================================================
// Status Handler
// ============================================================================

pub async fn status_handler(
    Query(params): Query<ReadQueryParams>,
    State(state): State<Arc<VolumeServerState>>,
) -> Response {
    let store = state.store.read().unwrap();
    let mut volumes = Vec::new();

    for loc in &store.locations {
        for (_vid, vol) in loc.volumes() {
            let mut vol_info = serde_json::Map::new();
            vol_info.insert("Id".to_string(), serde_json::Value::from(vol.id.0));
            vol_info.insert(
                "Collection".to_string(),
                serde_json::Value::from(vol.collection.clone()),
            );
            vol_info.insert(
                "Size".to_string(),
                serde_json::Value::from(vol.content_size()),
            );
            vol_info.insert(
                "FileCount".to_string(),
                serde_json::Value::from(vol.file_count()),
            );
            vol_info.insert(
                "DeleteCount".to_string(),
                serde_json::Value::from(vol.deleted_count()),
            );
            vol_info.insert(
                "DeletedByteCount".to_string(),
                serde_json::Value::from(vol.deleted_size()),
            );
            vol_info.insert(
                "ReadOnly".to_string(),
                serde_json::Value::from(vol.is_read_only()),
            );
            vol_info.insert(
                "Version".to_string(),
                serde_json::Value::from(vol.version().0),
            );
            vol_info.insert(
                "CompactRevision".to_string(),
                serde_json::Value::from(vol.super_block.compaction_revision),
            );
            vol_info.insert(
                "ModifiedAtSecond".to_string(),
                serde_json::Value::from(vol.last_modified_ts()),
            );
            vol_info.insert(
                "DiskType".to_string(),
                serde_json::Value::from(loc.disk_type.to_string()),
            );

            let replica = &vol.super_block.replica_placement;
            let mut replica_value = serde_json::Map::new();
            if replica.diff_data_center_count > 0 {
                replica_value.insert(
                    "dc".to_string(),
                    serde_json::Value::from(replica.diff_data_center_count),
                );
            }
            if replica.diff_rack_count > 0 {
                replica_value.insert(
                    "rack".to_string(),
                    serde_json::Value::from(replica.diff_rack_count),
                );
            }
            if replica.same_rack_count > 0 {
                replica_value.insert(
                    "node".to_string(),
                    serde_json::Value::from(replica.same_rack_count),
                );
            }
            vol_info.insert(
                "ReplicaPlacement".to_string(),
                serde_json::Value::Object(replica_value),
            );

            let ttl = vol.super_block.ttl;
            let mut ttl_value = serde_json::Map::new();
            if ttl.count > 0 {
                ttl_value.insert("Count".to_string(), serde_json::Value::from(ttl.count));
            }
            if ttl.unit > 0 {
                ttl_value.insert("Unit".to_string(), serde_json::Value::from(ttl.unit));
            }
            vol_info.insert("Ttl".to_string(), serde_json::Value::Object(ttl_value));

            let (remote_storage_name, remote_storage_key) = vol.remote_storage_name_key();
            vol_info.insert(
                "RemoteStorageName".to_string(),
                serde_json::Value::from(remote_storage_name),
            );
            vol_info.insert(
                "RemoteStorageKey".to_string(),
                serde_json::Value::from(remote_storage_key),
            );
            volumes.push(serde_json::Value::Object(vol_info));
        }
    }
    volumes.sort_by(|a, b| {
        let left = a.get("Id").and_then(|v| v.as_u64()).unwrap_or_default();
        let right = b.get("Id").and_then(|v| v.as_u64()).unwrap_or_default();
        left.cmp(&right)
    });

    let mut m = serde_json::Map::new();
    m.insert(
        "Version".to_string(),
        serde_json::Value::from(crate::version::version()),
    );
    m.insert("Volumes".to_string(), serde_json::Value::Array(volumes));
    m.insert(
        "DiskStatuses".to_string(),
        serde_json::Value::Array(build_disk_statuses(&store)),
    );
    json_response_with_params(StatusCode::OK, &serde_json::Value::Object(m), Some(&params))
}

// ============================================================================
// Health Check Handler
// ============================================================================

pub async fn healthz_handler(State(state): State<Arc<VolumeServerState>>) -> Response {
    let is_stopping = *state.is_stopping.read().unwrap();
    if is_stopping {
        return (StatusCode::SERVICE_UNAVAILABLE, "stopping").into_response();
    }
    // If not heartbeating, return 503 (matches Go health check behavior)
    if !state.is_heartbeating.load(Ordering::Relaxed) {
        return (StatusCode::SERVICE_UNAVAILABLE, "lost connection to master").into_response();
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
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

// ============================================================================
// Stats Handlers
// ============================================================================

pub async fn stats_counter_handler(Query(params): Query<ReadQueryParams>) -> Response {
    let payload = serde_json::json!({
        "Version": crate::version::version(),
        "Counters": super::server_stats::snapshot(),
    });
    json_response_with_params(StatusCode::OK, &payload, Some(&params))
}

pub async fn stats_memory_handler(Query(params): Query<ReadQueryParams>) -> Response {
    let mem = super::memory_status::collect_mem_status();
    let payload = serde_json::json!({
        "Version": crate::version::version(),
        "Memory": {
            "goroutines": mem.goroutines,
            "all": mem.all,
            "used": mem.used,
            "free": mem.free,
            "self": mem.self_,
            "heap": mem.heap,
            "stack": mem.stack,
        },
    });
    json_response_with_params(StatusCode::OK, &payload, Some(&params))
}

pub async fn stats_disk_handler(
    Query(params): Query<ReadQueryParams>,
    State(state): State<Arc<VolumeServerState>>,
) -> Response {
    let store = state.store.read().unwrap();
    let payload = serde_json::json!({
        "Version": crate::version::version(),
        "DiskStatuses": build_disk_statuses(&store),
    });
    json_response_with_params(StatusCode::OK, &payload, Some(&params))
}

// ============================================================================
// Static Asset Handlers
// ============================================================================

pub async fn favicon_handler() -> Response {
    let asset = super::ui::favicon_asset();
    (StatusCode::OK, [(header::CONTENT_TYPE, asset.content_type)], asset.bytes).into_response()
}

pub async fn static_asset_handler(Path(path): Path<String>) -> Response {
    match super::ui::lookup_static_asset(&path) {
        Some(asset) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, asset.content_type)],
            asset.bytes,
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

pub async fn ui_handler(
    State(state): State<Arc<VolumeServerState>>,
    headers: HeaderMap,
) -> Response {
    // If JWT signing is enabled, require auth
    let token = extract_jwt(&headers, &axum::http::Uri::from_static("/ui/index.html"));
    let guard = state.guard.read().unwrap();
    if let Err(e) = guard.check_jwt(token.as_deref(), false) {
        if guard.has_read_signing_key() {
            return (StatusCode::UNAUTHORIZED, format!("JWT error: {}", e)).into_response();
        }
    }
    drop(guard);

    let html = super::ui::render_volume_server_html(&state);
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
        .into_response()
}

// ============================================================================
// Chunk Manifest
// ============================================================================

#[derive(Deserialize)]
#[allow(dead_code)]
struct ChunkManifest {
    #[serde(default)]
    name: String,
    #[serde(default)]
    mime: String,
    #[serde(default)]
    size: i64,
    #[serde(default)]
    chunks: Vec<ChunkInfo>,
}

#[derive(Deserialize)]
struct ChunkInfo {
    fid: String,
    offset: i64,
    #[allow(dead_code)]
    size: i64,
}

/// Try to expand a chunk manifest needle. Returns None if manifest can't be parsed.
fn try_expand_chunk_manifest(
    state: &Arc<VolumeServerState>,
    n: &Needle,
    _headers: &HeaderMap,
    method: &Method,
) -> Option<Response> {
    let data = if n.is_compressed() {
        use flate2::read::GzDecoder;
        use std::io::Read as _;
        let mut decoder = GzDecoder::new(&n.data[..]);
        let mut decompressed = Vec::new();
        if decoder.read_to_end(&mut decompressed).is_err() {
            return None;
        }
        decompressed
    } else {
        n.data.clone()
    };

    let manifest: ChunkManifest = match serde_json::from_slice(&data) {
        Ok(m) => m,
        Err(_) => return None,
    };

    // Read and concatenate all chunks
    let mut result = vec![0u8; manifest.size as usize];
    let store = state.store.read().unwrap();
    for chunk in &manifest.chunks {
        let (chunk_vid, chunk_nid, chunk_cookie) = match parse_url_path(&chunk.fid) {
            Some(p) => p,
            None => {
                return Some(
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("invalid chunk fid: {}", chunk.fid),
                    )
                        .into_response(),
                )
            }
        };
        let mut chunk_needle = Needle {
            id: chunk_nid,
            cookie: chunk_cookie,
            ..Needle::default()
        };
        match store.read_volume_needle(chunk_vid, &mut chunk_needle) {
            Ok(_) => {}
            Err(e) => {
                return Some(
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("read chunk {}: {}", chunk.fid, e),
                    )
                        .into_response(),
                )
            }
        }
        let chunk_data = if chunk_needle.is_compressed() {
            use flate2::read::GzDecoder;
            use std::io::Read as _;
            let mut decoder = GzDecoder::new(&chunk_needle.data[..]);
            let mut decompressed = Vec::new();
            if decoder.read_to_end(&mut decompressed).is_ok() {
                decompressed
            } else {
                chunk_needle.data.clone()
            }
        } else {
            chunk_needle.data.clone()
        };
        let offset = chunk.offset as usize;
        let end = std::cmp::min(offset + chunk_data.len(), result.len());
        let copy_len = end - offset;
        if copy_len > 0 {
            result[offset..offset + copy_len].copy_from_slice(&chunk_data[..copy_len]);
        }
    }

    let content_type = if !manifest.mime.is_empty() {
        manifest.mime.clone()
    } else {
        "application/octet-stream".to_string()
    };

    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());
    response_headers.insert("X-File-Store", "chunked".parse().unwrap());

    if *method == Method::HEAD {
        response_headers.insert(
            header::CONTENT_LENGTH,
            result.len().to_string().parse().unwrap(),
        );
        return Some((StatusCode::OK, response_headers).into_response());
    }

    Some((StatusCode::OK, response_headers, result).into_response())
}

// ============================================================================
// Helpers
// ============================================================================

fn absolute_display_path(path: &str) -> String {
    let p = std::path::Path::new(path);
    if p.is_absolute() {
        return path.to_string();
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(p).to_string_lossy().to_string())
        .unwrap_or_else(|_| path.to_string())
}

fn build_disk_statuses(store: &crate::storage::store::Store) -> Vec<serde_json::Value> {
    let mut disk_statuses = Vec::new();
    for loc in &store.locations {
        let resolved_dir = absolute_display_path(&loc.directory);
        let (all, free) = crate::storage::disk_location::get_disk_stats(&resolved_dir);
        let used = all.saturating_sub(free);
        let percent_free = if all > 0 {
            (free as f64 / all as f64) * 100.0
        } else {
            0.0
        };
        let percent_used = if all > 0 {
            (used as f64 / all as f64) * 100.0
        } else {
            0.0
        };

        disk_statuses.push(serde_json::json!({
            "dir": resolved_dir,
            "all": all,
            "used": used,
            "free": free,
            "percent_free": percent_free,
            "percent_used": percent_used,
            "disk_type": loc.disk_type.to_string(),
        }));
    }
    disk_statuses
}

fn json_response_with_params<T: Serialize>(
    status: StatusCode,
    body: &T,
    params: Option<&ReadQueryParams>,
) -> Response {
    let is_pretty = params
        .and_then(|params| params.pretty.as_ref())
        .is_some_and(|value| !value.is_empty());
    let callback = params
        .and_then(|params| params.callback.as_ref())
        .filter(|value| !value.is_empty())
        .cloned();

    let json_body = if is_pretty {
        serde_json::to_string_pretty(body).unwrap()
    } else {
        serde_json::to_string(body).unwrap()
    };

    if let Some(callback) = callback {
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/javascript")
            .body(Body::from(format!("{}({})", callback, json_body)))
            .unwrap()
    } else {
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(json_body))
            .unwrap()
    }
}

/// Return a JSON error response with optional query string for pretty/JSONP support.
/// Supports `?pretty=<any non-empty value>` for pretty-printed JSON and `?callback=fn` for JSONP,
/// matching Go's writeJsonError behavior.
fn json_error_with_query(
    status: StatusCode,
    msg: impl Into<String>,
    query: Option<&str>,
) -> Response {
    let body = serde_json::json!({"error": msg.into()});

    let (is_pretty, callback) = if let Some(q) = query {
        let pretty = q
            .split('&')
            .any(|p| p.starts_with("pretty=") && p.len() > "pretty=".len());
        let cb = q
            .split('&')
            .find_map(|p| p.strip_prefix("callback="))
            .map(|s| s.to_string());
        (pretty, cb)
    } else {
        (false, None)
    };

    let json_body = if is_pretty {
        serde_json::to_string_pretty(&body).unwrap()
    } else {
        serde_json::to_string(&body).unwrap()
    };

    if let Some(cb) = callback {
        let jsonp = format!("{}({})", cb, json_body);
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/javascript")
            .body(Body::from(jsonp))
            .unwrap()
    } else {
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(json_body))
            .unwrap()
    }
}

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

    // 2. Check Authorization: Bearer <token> (case-insensitive prefix)
    if let Some(auth) = headers.get(header::AUTHORIZATION) {
        if let Ok(auth_str) = auth.to_str() {
            if auth_str.len() >= 7 && auth_str[..7].eq_ignore_ascii_case("bearer ") {
                return Some(auth_str[7..].to_string());
            }
        }
    }

    // 3. Check Cookie
    if let Some(cookie_header) = headers.get(header::COOKIE) {
        if let Ok(cookie_str) = cookie_header.to_str() {
            for cookie in cookie_str.split(';') {
                let cookie = cookie.trim();
                if let Some(value) = cookie.strip_prefix("AT=") {
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
// Auto-compression helpers (matches Go's util.IsCompressableFileType)
// ============================================================================

/// Check if a file type should be compressed based on extension and MIME type.
/// Returns true only when we are sure the type is compressible.
fn is_compressible_file_type(ext: &str, mtype: &str) -> bool {
    // text/*
    if mtype.starts_with("text/") {
        return true;
    }
    // Compressible image/audio formats
    match ext {
        ".svg" | ".bmp" | ".wav" => return true,
        _ => {}
    }
    // Most image/* formats are already compressed
    if mtype.starts_with("image/") {
        return false;
    }
    // By file extension
    match ext {
        ".zip" | ".rar" | ".gz" | ".bz2" | ".xz" | ".zst" | ".br" => return false,
        ".pdf" | ".txt" | ".html" | ".htm" | ".css" | ".js" | ".json" => return true,
        ".php" | ".java" | ".go" | ".rb" | ".c" | ".cpp" | ".h" | ".hpp" => return true,
        ".png" | ".jpg" | ".jpeg" => return false,
        _ => {}
    }
    // By MIME type
    if mtype.starts_with("application/") {
        if mtype.ends_with("zstd") {
            return false;
        }
        if mtype.ends_with("xml") {
            return true;
        }
        if mtype.ends_with("script") {
            return true;
        }
        if mtype.ends_with("vnd.rar") {
            return false;
        }
    }
    if mtype.starts_with("audio/") {
        let sub = mtype.strip_prefix("audio/").unwrap_or("");
        if matches!(sub, "wave" | "wav" | "x-wav" | "x-pn-wav") {
            return true;
        }
    }
    false
}

/// Try to gzip data. Returns None on error.
fn try_gzip_data(data: &[u8]) -> Option<Vec<u8>> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).ok()?;
    encoder.finish().ok()
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
        headers.insert(
            header::AUTHORIZATION,
            "Bearer header_token".parse().unwrap(),
        );
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

    #[tokio::test]
    async fn test_stats_memory_handler_matches_go_memstatus_shape() {
        let response = stats_memory_handler(Query(ReadQueryParams::default())).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let memory = payload.get("Memory").unwrap();

        for key in ["goroutines", "all", "used", "free", "self", "heap", "stack"] {
            assert!(memory.get(key).is_some(), "missing key {}", key);
        }
    }

    #[tokio::test]
    async fn test_stats_counter_handler_matches_go_json_shape() {
        super::super::server_stats::reset_for_tests();
        super::super::server_stats::record_read_request();

        let response = stats_counter_handler(Query(ReadQueryParams::default())).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(
            payload.get("Version").and_then(|value| value.as_str()),
            Some(crate::version::version())
        );
        let counters = payload.get("Counters").unwrap();
        assert!(counters.get("ReadRequests").is_some());
        assert!(counters.get("Requests").is_some());
    }

    #[test]
    fn test_is_compressible_file_type() {
        // Text types
        assert!(is_compressible_file_type("", "text/html"));
        assert!(is_compressible_file_type("", "text/plain"));
        assert!(is_compressible_file_type("", "text/css"));

        // Compressible by extension
        assert!(is_compressible_file_type(".svg", ""));
        assert!(is_compressible_file_type(".bmp", ""));
        assert!(is_compressible_file_type(".js", ""));
        assert!(is_compressible_file_type(".json", ""));
        assert!(is_compressible_file_type(".html", ""));
        assert!(is_compressible_file_type(".css", ""));
        assert!(is_compressible_file_type(".c", ""));
        assert!(is_compressible_file_type(".go", ""));

        // Already compressed — should NOT compress
        assert!(!is_compressible_file_type(".zip", ""));
        assert!(!is_compressible_file_type(".gz", ""));
        assert!(!is_compressible_file_type(".jpg", ""));
        assert!(!is_compressible_file_type(".png", ""));
        assert!(!is_compressible_file_type("", "image/jpeg"));
        assert!(!is_compressible_file_type("", "image/png"));

        // Application subtypes
        assert!(is_compressible_file_type("", "application/xml"));
        assert!(is_compressible_file_type("", "application/javascript"));
        assert!(!is_compressible_file_type("", "application/zstd"));
        assert!(!is_compressible_file_type("", "application/vnd.rar"));

        // Audio
        assert!(is_compressible_file_type(".wav", "audio/wav"));
        assert!(!is_compressible_file_type("", "audio/mpeg"));

        // Unknown
        assert!(!is_compressible_file_type(
            ".xyz",
            "application/octet-stream"
        ));
    }

    #[test]
    fn test_try_gzip_data() {
        let data = b"hello world hello world hello world";
        let compressed = try_gzip_data(data);
        assert!(compressed.is_some());
        let compressed = compressed.unwrap();
        // Compressed data should be different from original
        assert!(!compressed.is_empty());

        // Verify we can decompress it
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut decoder = GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_streaming_chunk_size_respects_configured_read_buffer() {
        assert_eq!(
            streaming_chunk_size(4 * 1024 * 1024, 8 * 1024 * 1024),
            4 * 1024 * 1024
        );
        assert_eq!(
            streaming_chunk_size(32 * 1024, 512 * 1024),
            DEFAULT_STREAMING_CHUNK_SIZE
        );
        assert_eq!(
            streaming_chunk_size(8 * 1024 * 1024, 128 * 1024),
            128 * 1024
        );
    }

    #[test]
    fn test_normalize_outgoing_http_url_rewrites_scheme() {
        let url = normalize_outgoing_http_url(
            "https",
            "http://master.example.com:9333/dir/lookup?volumeId=7",
        )
        .unwrap();
        assert_eq!(url, "https://master.example.com:9333/dir/lookup?volumeId=7");
    }

    #[test]
    fn test_redirect_request_uses_outgoing_http_scheme() {
        let info = ProxyRequestInfo {
            original_headers: HeaderMap::new(),
            original_query: "collection=photos&readDeleted=true".to_string(),
            path: "/3,01637037d6".to_string(),
            vid_str: "3".to_string(),
            fid_str: "01637037d6".to_string(),
        };
        let target = VolumeLocation {
            url: "volume.internal:8080".to_string(),
            public_url: "volume.public:8080".to_string(),
        };

        let response = redirect_request(&info, &target, "https");
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "https://volume.public:8080/3,01637037d6?collection=photos&proxied=true"
        );
    }
}
