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

use axum::{Router, routing::get};

use crate::security::Guard;
use crate::storage::store::Store;

use super::handlers;

/// Shared state for the volume server.
pub struct VolumeServerState {
    pub store: RwLock<Store>,
    pub guard: Guard,
    pub is_stopping: RwLock<bool>,
}

/// Build the admin (private) HTTP router — supports all operations.
pub fn build_admin_router(state: Arc<VolumeServerState>) -> Router {
    Router::new()
        .route("/status", get(handlers::status_handler))
        .route("/healthz", get(handlers::healthz_handler))
        .route("/metrics", get(handlers::metrics_handler))
        // Volume operations: GET/HEAD/POST/PUT/DELETE on /{vid},{fid}
        .route(
            "/:path",
            get(handlers::get_or_head_handler)
                .head(handlers::get_or_head_handler)
                .post(handlers::post_handler)
                .put(handlers::post_handler)
                .delete(handlers::delete_handler),
        )
        // Also support /{vid}/{fid} and /{vid}/{fid}/{filename} paths
        .route(
            "/:vid/:fid",
            get(handlers::get_or_head_handler)
                .head(handlers::get_or_head_handler)
                .post(handlers::post_handler)
                .put(handlers::post_handler)
                .delete(handlers::delete_handler),
        )
        .route(
            "/:vid/:fid/:filename",
            get(handlers::get_or_head_handler)
                .head(handlers::get_or_head_handler),
        )
        .with_state(state)
}

/// Build the public (read-only) HTTP router — only GET/HEAD.
pub fn build_public_router(state: Arc<VolumeServerState>) -> Router {
    Router::new()
        .route("/healthz", get(handlers::healthz_handler))
        .route(
            "/:path",
            get(handlers::get_or_head_handler)
                .head(handlers::get_or_head_handler),
        )
        .route(
            "/:vid/:fid",
            get(handlers::get_or_head_handler)
                .head(handlers::get_or_head_handler),
        )
        .route(
            "/:vid/:fid/:filename",
            get(handlers::get_or_head_handler)
                .head(handlers::get_or_head_handler),
        )
        .with_state(state)
}
