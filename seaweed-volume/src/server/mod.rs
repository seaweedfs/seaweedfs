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
///
/// Every store call used to reach the wire as `Status::internal`, so a client
/// could not tell "that volume is not on this server" (retry elsewhere) from
/// "this disk is failing" (page someone). The message is the error's `Display`
/// so operator logs keep the wording the storage layer produced.
impl From<VolumeError> for Status {
    fn from(err: VolumeError) -> Self {
        let message = err.to_string();
        match err {
            VolumeError::NotFound | VolumeError::VolumeNotFound(_) => Status::not_found(message),
            VolumeError::ReadOnly => Status::failed_precondition(message),
            VolumeError::InsufficientSpace { .. } => Status::resource_exhausted(message),
            VolumeError::AlreadyExists => Status::already_exists(message),
            _ => Status::internal(message),
        }
    }
}

/// Same mapping, with the RPC's own context prefixed onto the message the way
/// the storage layer used to format it in-place (`compact volume 7: ...`).
pub fn status_with_context(context: &str, err: VolumeError) -> Status {
    let status = Status::from(err);
    Status::new(status.code(), format!("{context}: {}", status.message()))
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
        // Anything the mapping does not name stays `internal`, which is what
        // every store error used to be.
        assert_eq!(code(VolumeError::NotInitialized), Code::Internal);

        // The context prefix keeps the mapped code and does not restate what
        // the error already says.
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
