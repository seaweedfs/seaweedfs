//! The VectorService gRPC server the S3 gateway talks to.

pub mod metrics;
pub mod observed;
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
pub mod security {
    pub use weed_lance_worker::security::*;
}
pub mod tls {
    pub use weed_lance_worker::grpc_tls::*;
}

pub mod pb {
    pub mod vector {
        tonic::include_proto!("vector_pb");
    }
}
