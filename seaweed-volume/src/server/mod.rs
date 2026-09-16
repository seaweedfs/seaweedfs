#[cfg(unix)]
pub mod debug;
pub mod grpc_client;
pub mod grpc_server;
pub mod handlers;
pub mod heartbeat;
pub mod memory_status;
#[cfg(unix)]
pub mod profiling;
pub mod request_id;
pub mod server_stats;
pub mod store_ec;
pub mod ui;
pub mod volume_server;
pub mod write_queue;

/// Render a configured disk directory as an absolute path for display, so the
/// status JSON and the UI show the same thing for a relative `-dir`. Falls
/// back to the configured spelling when the current directory cannot be read.
pub(crate) fn absolute_display_path(path: &str) -> String {
    let p = std::path::Path::new(path);
    if p.is_absolute() {
        return path.to_string();
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(p).to_string_lossy().to_string())
        .unwrap_or_else(|_| path.to_string())
}
