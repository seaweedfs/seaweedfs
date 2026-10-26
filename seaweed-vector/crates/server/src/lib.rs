//! The VectorService gRPC server the S3 gateway talks to.

pub mod service;

pub mod pb {
    pub mod vector {
        tonic::include_proto!("vector_pb");
    }
}
