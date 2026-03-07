pub mod config;
pub mod images;
pub mod storage;
pub mod security;
pub mod server;
pub mod metrics;
pub mod remote_storage;

/// Generated protobuf modules.
pub mod pb {
    pub mod remote_pb {
        tonic::include_proto!("remote_pb");
    }
    pub mod volume_server_pb {
        tonic::include_proto!("volume_server_pb");
    }
    pub mod master_pb {
        tonic::include_proto!("master_pb");
    }
}
