//! SeaweedFS addresses the way the Go tree does.
//!
//! An operator gives a worker the admin's HTTP address, and the gRPC port is
//! derived from it rather than asked for separately. Dialling the HTTP port
//! instead fails as "frame with invalid size", which reads like a protocol bug
//! rather than a wrong port, so getting this right is worth its own module.
//! Mirrors pb.ServerToGrpcAddress in weed/pb/grpc_client_server.go.

const GRPC_PORT_OFFSET: u16 = 10000;

/// Converts `host:port` to the gRPC address, and accepts the explicit
/// `host:port.grpcPort` form the Go side also understands.
pub fn server_to_grpc_address(server: &str) -> Option<String> {
    let (host, port_part) = server.rsplit_once(':')?;

    // "port.grpcPort" states the gRPC port outright.
    if let Some((_, grpc_port)) = port_part.split_once('.') {
        if let Ok(port) = grpc_port.parse::<u16>() {
            return Some(join_host_port(host, port));
        }
    }

    let port: u16 = port_part.parse().ok()?;
    Some(join_host_port(host, port.checked_add(GRPC_PORT_OFFSET)?))
}

fn join_host_port(host: &str, port: u16) -> String {
    // An IPv6 literal has to keep its brackets or the port reads as part of it.
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
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
}
