pub mod config;
pub mod images;
pub mod metrics;
pub mod remote_storage;
pub mod security;
pub mod server;
pub mod storage;
pub mod version;

/// Generated protobuf modules.
pub mod pb {
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("seaweed_descriptor");

    pub mod remote_pb {
        tonic::include_proto!("remote_pb");
    }
    pub mod volume_server_pb {
        tonic::include_proto!("volume_server_pb");
    }
    pub mod master_pb {
        tonic::include_proto!("master_pb");
    }
    pub mod filer_pb {
        tonic::include_proto!("filer_pb");
    }
}
