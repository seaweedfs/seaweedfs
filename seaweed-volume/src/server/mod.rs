use tonic::Status;

use crate::storage::volume::VolumeError;

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

/// Map a storage error onto the gRPC code that describes it.
impl From<VolumeError> for Status {
    fn from(err: VolumeError) -> Self {
        let message = err.to_string();
        match err {
            VolumeError::NotFound | VolumeError::VolumeNotFound(_) => Status::not_found(message),
            VolumeError::ReadOnly | VolumeError::NotEmpty => Status::failed_precondition(message),
            VolumeError::InsufficientSpace { .. } => Status::resource_exhausted(message),
            VolumeError::AlreadyExists => Status::already_exists(message),
            _ => Status::internal(message),
        }
    }
}

/// Same mapping, with the RPC's own context prefixed (`compact volume 7: ...`).
pub fn status_with_context(context: &str, err: VolumeError) -> Status {
    let status = Status::from(err);
    Status::new(status.code(), format!("{context}: {}", status.message()))
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::types::VolumeId;

    #[test]
    fn test_volume_error_maps_to_grpc_code() {
        use tonic::Code;

        let code = |e: VolumeError| Status::from(e).code();
        assert_eq!(
            code(VolumeError::VolumeNotFound(VolumeId(7))),
            Code::NotFound
        );
        assert_eq!(code(VolumeError::NotFound), Code::NotFound);
        assert_eq!(code(VolumeError::ReadOnly), Code::FailedPrecondition);
        assert_eq!(
            code(VolumeError::InsufficientSpace {
                vid: VolumeId(7),
                required: 2,
                free: 1,
            }),
            Code::ResourceExhausted
        );
        assert_eq!(code(VolumeError::AlreadyExists), Code::AlreadyExists);
        assert_eq!(code(VolumeError::NotInitialized), Code::Internal);

        let status = status_with_context(
            "compact volume 7",
            VolumeError::InsufficientSpace {
                vid: VolumeId(7),
                required: 2,
                free: 1,
            },
        );
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(
            status.message(),
            "compact volume 7: not enough free space: required 2, free 1"
        );
    }
}
