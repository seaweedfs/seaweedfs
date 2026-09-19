//! SeaweedFS server addresses, the way the Go tree does them.
//!
//! An operator gives a SeaweedFS process an HTTP address and the gRPC port is
//! derived from it rather than asked for separately: `host:port` means gRPC on
//! `port + 10000`, and the explicit `host:port.grpcPort` form names it outright.
//! Dialling the HTTP port by mistake fails as "frame with invalid size", which
//! reads like a protocol bug rather than a wrong port, so the rule is worth its
//! own module. Mirrors `pb.ServerToGrpcAddress` in
//! `weed/pb/grpc_client_server.go`.
//!
//! The volume server and the workers each had their own copy of this and the
//! copies had drifted: the worker's bracketed IPv6 literals and the volume
//! server's did not, so `::1:19333` produced `::1:29333`, which the HTTP
//! authority parser rejects. One implementation, two thin wrappers.

use std::fmt;
use std::num::ParseIntError;

/// SeaweedFS's HTTP↔gRPC port-offset convention.
pub const GRPC_PORT_OFFSET: u16 = 10000;

/// Why an address could not be turned into a gRPC address.
///
/// The `Display` text is the volume server's original wording, because its
/// `parse_grpc_address` wrapper hands it straight to callers that put it in a
/// `Status` or an `io::Error`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AddressError {
    /// No `:` at all, so there is no port to translate.
    MissingPort(String),
    /// The HTTP port of the `host:port.grpcPort` form is not a `u16`. It is
    /// validated even though it is then discarded, so that a malformed address
    /// is rejected here instead of failing later as an opaque connect error.
    InvalidHttpPort { port: String, source: ParseIntError },
    /// The gRPC port of the `host:port.grpcPort` form is not a `u16`.
    InvalidGrpcPort { port: String, source: ParseIntError },
    /// The port of the `host:port` form is not a `u16`.
    InvalidPort { port: String, source: ParseIntError },
    /// `port + GRPC_PORT_OFFSET` leaves the TCP port range, e.g. `host:60000`.
    /// Without the check the cast would wrap silently.
    ImplicitGrpcPortOutOfRange(u16),
}

impl fmt::Display for AddressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingPort(address) => write!(f, "cannot parse address: {address}"),
            Self::InvalidHttpPort { port, source } => {
                write!(f, "invalid http port {port:?}: {source}")
            }
            Self::InvalidGrpcPort { port, source } => {
                write!(f, "invalid grpc port {port:?}: {source}")
            }
            Self::InvalidPort { port, source } => write!(f, "invalid port {port:?}: {source}"),
            Self::ImplicitGrpcPortOutOfRange(port) => write!(
                f,
                "implicit grpc port out of range: {port} + {GRPC_PORT_OFFSET} = {}",
                u32::from(*port) + u32::from(GRPC_PORT_OFFSET)
            ),
        }
    }
}

impl std::error::Error for AddressError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidHttpPort { source, .. }
            | Self::InvalidGrpcPort { source, .. }
            | Self::InvalidPort { source, .. } => Some(source),
            Self::MissingPort(_) | Self::ImplicitGrpcPortOutOfRange(_) => None,
        }
    }
}

/// Turn a SeaweedFS server address (`"host:port.grpcPort"` or `"host:port"`)
/// into the `host:grpcPort` form the endpoint builders expect.
///
/// With the trailing `.grpcPort` segment that segment *is* the gRPC port;
/// without it the gRPC port is `port + GRPC_PORT_OFFSET`. An unbracketed IPv6
/// literal comes back bracketed, because otherwise the port reads as part of
/// the address.
pub fn to_grpc_address(server: &str) -> Result<String, AddressError> {
    // rfind, not find: an IPv6 literal is full of colons and the port is after
    // the last one.
    let colon_idx = server
        .rfind(':')
        .ok_or_else(|| AddressError::MissingPort(server.to_string()))?;
    let host = &server[..colon_idx];
    let port_part = &server[colon_idx + 1..];

    // rfind again rather than split_once: the host may be an IPv4 address, and
    // only the part after the last colon is being split here anyway.
    if let Some(dot_idx) = port_part.rfind('.') {
        let http_port = &port_part[..dot_idx];
        let grpc_port = &port_part[dot_idx + 1..];
        http_port
            .parse::<u16>()
            .map_err(|source| AddressError::InvalidHttpPort {
                port: http_port.to_string(),
                source,
            })?;
        let grpc_port =
            grpc_port
                .parse::<u16>()
                .map_err(|source| AddressError::InvalidGrpcPort {
                    port: grpc_port.to_string(),
                    source,
                })?;
        return Ok(join_host_port(host, grpc_port));
    }

    let port: u16 = port_part
        .parse()
        .map_err(|source| AddressError::InvalidPort {
            port: port_part.to_string(),
            source,
        })?;
    let grpc_port = port
        .checked_add(GRPC_PORT_OFFSET)
        .ok_or(AddressError::ImplicitGrpcPortOutOfRange(port))?;
    Ok(join_host_port(host, grpc_port))
}

/// Join a host and a port, bracketing an IPv6 literal that is not bracketed
/// already. Public because the address rule is not the only place that has to
/// put a host and a port back together.
pub fn join_host_port(host: &str, port: u16) -> String {
    // An IPv6 literal has to keep its brackets or the port reads as part of it.
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::{AddressError, GRPC_PORT_OFFSET, join_host_port, to_grpc_address};

    // ---- the volume server's cases -------------------------------------

    #[test]
    fn dotted_form_states_the_grpc_port() {
        assert_eq!(
            to_grpc_address("127.0.0.1:8080.18080").unwrap(),
            "127.0.0.1:18080"
        );
        assert_eq!(
            to_grpc_address("192.168.1.66:8080.18080").unwrap(),
            "192.168.1.66:18080"
        );
    }

    #[test]
    fn implicit_form_adds_the_offset() {
        assert_eq!(
            to_grpc_address("127.0.0.1:8080").unwrap(),
            "127.0.0.1:18080"
        );
        assert_eq!(
            to_grpc_address("192.168.1.66:8080").unwrap(),
            "192.168.1.66:18080"
        );
        assert_eq!(
            to_grpc_address("localhost:9333").unwrap(),
            "localhost:19333"
        );
    }

    #[test]
    fn the_dotted_grpc_port_comes_back_normalised() {
        // The volume server's copy validated this segment as a u16 and then
        // emitted the original text, so a padded or signed port produced an
        // authority the URI parser rejects. The parsed value is emitted now.
        assert_eq!(to_grpc_address("host:8080.018080").unwrap(), "host:18080");
        assert_eq!(to_grpc_address("host:8080.+18080").unwrap(), "host:18080");
    }

    #[test]
    fn an_ipv4_host_is_not_confused_with_the_dotted_port() {
        // Regression: a naive split on '.' breaks on IP addresses.
        assert_eq!(
            to_grpc_address("10.0.0.1:8080.18080").unwrap(),
            "10.0.0.1:18080"
        );
        assert_eq!(to_grpc_address("10.0.0.1:8080").unwrap(), "10.0.0.1:18080");
    }

    #[test]
    fn rejects_a_non_numeric_http_port_in_the_dotted_form() {
        let err = to_grpc_address("host:abc.18080").unwrap_err();
        assert!(
            matches!(err, AddressError::InvalidHttpPort { .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains("invalid http port"), "{err}");
    }

    #[test]
    fn rejects_a_non_numeric_grpc_port_in_the_dotted_form() {
        let err = to_grpc_address("host:8080.xyz").unwrap_err();
        assert!(
            matches!(err, AddressError::InvalidGrpcPort { .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains("invalid grpc port"), "{err}");
    }

    #[test]
    fn rejects_an_implicit_port_that_leaves_the_tcp_range() {
        let err = to_grpc_address("127.0.0.1:60000").unwrap_err();
        assert!(
            matches!(err, AddressError::ImplicitGrpcPortOutOfRange(60000)),
            "{err:?}"
        );
        assert!(err.to_string().contains("out of range"), "{err}");
    }

    #[test]
    fn the_messages_are_the_volume_servers_wording_verbatim() {
        // parse_grpc_address hands these straight to callers that put them in a
        // Status or an io::Error, so the whole string is the contract, not just
        // the substring the older tests match on. Only the two variants whose
        // text is entirely ours are pinned exactly; the other three end in a
        // std ParseIntError message, which is std's to reword.
        assert_eq!(
            to_grpc_address("127.0.0.1:60000").unwrap_err().to_string(),
            "implicit grpc port out of range: 60000 + 10000 = 70000"
        );
        assert_eq!(
            to_grpc_address("hostname").unwrap_err().to_string(),
            "cannot parse address: hostname"
        );
    }

    #[test]
    fn rejects_an_address_without_a_port() {
        for source in ["hostname", "no-colon", "localhost"] {
            let err = to_grpc_address(source).unwrap_err();
            assert!(matches!(err, AddressError::MissingPort(_)), "{err:?}");
            assert!(err.to_string().contains("cannot parse"), "{err}");
        }
    }

    // ---- the worker's cases --------------------------------------------

    #[test]
    fn derives_the_grpc_port() {
        assert_eq!(
            to_grpc_address("localhost:23646").unwrap(),
            "localhost:33646"
        );
        assert_eq!(
            to_grpc_address("127.0.0.1:9333").unwrap(),
            "127.0.0.1:19333"
        );
    }

    #[test]
    fn honours_an_explicit_grpc_port() {
        assert_eq!(
            to_grpc_address("localhost:23646.33999").unwrap(),
            "localhost:33999"
        );
    }

    #[test]
    fn rejects_what_it_cannot_parse() {
        let err = to_grpc_address("localhost:notaport").unwrap_err();
        assert!(matches!(err, AddressError::InvalidPort { .. }), "{err:?}");
        assert!(err.to_string().contains("invalid port"), "{err}");
    }

    // ---- IPv6, which only the worker's copy handled --------------------

    #[test]
    fn brackets_ipv6_literals() {
        assert_eq!(to_grpc_address("::1:23646").unwrap(), "[::1]:33646");
        assert_eq!(to_grpc_address("::1:9333").unwrap(), "[::1]:19333");
        assert_eq!(
            to_grpc_address("fe80::1:9333.19333").unwrap(),
            "[fe80::1]:19333"
        );
    }

    #[test]
    fn leaves_an_already_bracketed_literal_alone() {
        assert_eq!(to_grpc_address("[::1]:9333").unwrap(), "[::1]:19333");
        assert_eq!(to_grpc_address("[::1]:9333.19333").unwrap(), "[::1]:19333");
    }

    #[test]
    fn join_host_port_brackets_only_unbracketed_literals() {
        assert_eq!(join_host_port("127.0.0.1", 19333), "127.0.0.1:19333");
        assert_eq!(join_host_port("localhost", 19333), "localhost:19333");
        assert_eq!(join_host_port("::1", 19333), "[::1]:19333");
        assert_eq!(join_host_port("[::1]", 19333), "[::1]:19333");
    }

    // ---- the offset itself ---------------------------------------------

    #[test]
    fn the_offset_is_the_seaweedfs_convention() {
        assert_eq!(GRPC_PORT_OFFSET, 10000);
    }
}
