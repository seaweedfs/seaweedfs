//! SeaweedFS addresses the way the Go tree does.
//!
//! An operator gives a worker the admin's HTTP address, and the gRPC port is
//! derived from it rather than asked for separately. Dialling the HTTP port
//! instead fails as "frame with invalid size", which reads like a protocol bug
//! rather than a wrong port, so getting this right is worth its own module.
//! Mirrors pb.ServerToGrpcAddress in weed/pb/grpc_client_server.go.
//!
//! The rule itself now lives in `seaweed_common::address`, shared with the Rust
//! volume server, which had its own copy of it. What stays here is the `Option`
//! shape this crate's callers expect, and the tests that pin it.

/// Converts `host:port` to the gRPC address, and accepts the explicit
/// `host:port.grpcPort` form the Go side also understands.
pub fn server_to_grpc_address(server: &str) -> Option<String> {
    seaweed_common::address::to_grpc_address(server).ok()
}

#[cfg(test)]
mod tests {
    use super::server_to_grpc_address;

    #[test]
    fn derives_the_grpc_port() {
        assert_eq!(
            server_to_grpc_address("localhost:23646").as_deref(),
            Some("localhost:33646")
        );
        assert_eq!(
            server_to_grpc_address("127.0.0.1:9333").as_deref(),
            Some("127.0.0.1:19333")
        );
    }

    #[test]
    fn honours_an_explicit_grpc_port() {
        assert_eq!(
            server_to_grpc_address("localhost:23646.33999").as_deref(),
            Some("localhost:33999")
        );
    }

    #[test]
    fn brackets_ipv6_literals() {
        assert_eq!(
            server_to_grpc_address("::1:23646").as_deref(),
            Some("[::1]:33646")
        );
    }

    #[test]
    fn rejects_what_it_cannot_parse() {
        assert!(server_to_grpc_address("localhost").is_none());
        assert!(server_to_grpc_address("localhost:notaport").is_none());
    }

    #[test]
    fn rejects_a_dotted_form_whose_http_port_is_not_a_port() {
        // Tightened by the move to seaweed-common: this copy used to ignore the
        // HTTP port of the dotted form and answer Some("host:18080").
        assert!(server_to_grpc_address("host:abc.18080").is_none());
    }
}
