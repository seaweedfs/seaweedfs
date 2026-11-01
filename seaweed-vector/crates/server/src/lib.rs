//! The VectorService gRPC server the S3 gateway talks to.

pub mod read_cache;
pub mod service;

pub mod remote_pb {
    tonic::include_proto!("remote_pb");
}
pub mod volume_server_pb {
    tonic::include_proto!("volume_server_pb");
}
pub mod master_pb {
    tonic::include_proto!("master_pb");
}
pub mod master_client;

pub mod pb {
    pub mod vector {
        tonic::include_proto!("vector_pb");
    }
}
