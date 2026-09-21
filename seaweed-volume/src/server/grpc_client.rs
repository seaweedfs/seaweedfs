//! Construction of the volume server's *outgoing* gRPC clients: TLS material,
//! endpoint tuning, dial bounds, and the three client constructors every call
//! site goes through.
//!
//! The keepalive, window-size and message-size constants below are shared with
//! the *inbound* server built in `main.rs`, which imports them from here rather
//! than declaring its own. Changing one therefore changes both directions at
//! once, which is deliberate: a volume server talks to its peers with the same
//! HTTP/2 settings it offers them.

use std::error::Error;
use std::fmt;
use std::time::Duration;

use hyper::http::Uri;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Request, Status};

use crate::config::VolumeServerConfig;
use crate::pb::filer_pb::seaweed_filer_client::SeaweedFilerClient;
use crate::pb::master_pb::seaweed_client::SeaweedClient;
use crate::pb::volume_server_pb::volume_server_client::VolumeServerClient;
use crate::server::request_id::outgoing_request_id_interceptor;

pub const GRPC_MAX_MESSAGE_SIZE: usize = 1 << 30;
pub const GRPC_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);
pub const GRPC_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);
pub const GRPC_INITIAL_WINDOW_SIZE: u32 = 16 * 1024 * 1024;

/// Bound on the TCP connect of every outgoing dial. `build_grpc_endpoint` is
/// private and `connect_channel` is the only way out of this module, so every
/// call site picks this up whether it thinks about timeouts or not.
///
/// It bounds the TCP handshake only — tonic hands it to
/// `HttpConnector::set_connect_timeout`. A peer that completes the handshake
/// and then stalls in the TLS or HTTP/2 exchange is not covered; callers that
/// need that bound wrap the whole dial (see `connect_ping_target`).
const GRPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct OutgoingGrpcTlsConfig {
    cert_pem: String,
    key_pem: String,
    ca_pem: String,
}

#[derive(Debug)]
pub struct GrpcClientError(String);

impl fmt::Display for GrpcClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for GrpcClientError {}

pub fn load_outgoing_grpc_tls(
    config: &VolumeServerConfig,
) -> Result<Option<OutgoingGrpcTlsConfig>, GrpcClientError> {
    // prefer a dedicated client certificate: CAs may issue certs with only one of the serverAuth/clientAuth EKUs
    let (cert_file, key_file) = if !config.grpc_client_cert_file.is_empty()
        && !config.grpc_client_key_file.is_empty()
    {
        (&config.grpc_client_cert_file, &config.grpc_client_key_file)
    } else {
        if !config.grpc_client_cert_file.is_empty() || !config.grpc_client_key_file.is_empty() {
            tracing::warn!(
                "grpc.volume.client_cert and grpc.volume.client_key must both be set, falling back to grpc.volume.cert and grpc.volume.key"
            );
        }
        (&config.grpc_cert_file, &config.grpc_key_file)
    };
    if cert_file.is_empty() || key_file.is_empty() || config.grpc_ca_file.is_empty() {
        return Ok(None);
    }

    let cert_pem = std::fs::read_to_string(cert_file).map_err(|e| {
        GrpcClientError(format!(
            "Failed to read outgoing gRPC cert '{}': {}",
            cert_file, e
        ))
    })?;
    let key_pem = std::fs::read_to_string(key_file).map_err(|e| {
        GrpcClientError(format!(
            "Failed to read outgoing gRPC key '{}': {}",
            key_file, e
        ))
    })?;
    let ca_pem = std::fs::read_to_string(&config.grpc_ca_file).map_err(|e| {
        GrpcClientError(format!(
            "Failed to read outgoing gRPC CA '{}': {}",
            config.grpc_ca_file, e
        ))
    })?;

    Ok(Some(OutgoingGrpcTlsConfig {
        cert_pem,
        key_pem,
        ca_pem,
    }))
}

pub fn grpc_endpoint_uri(grpc_host_port: &str, tls: Option<&OutgoingGrpcTlsConfig>) -> String {
    let scheme = if tls.is_some() { "https" } else { "http" };
    format!("{}://{}", scheme, grpc_host_port)
}

fn build_grpc_endpoint(
    grpc_host_port: &str,
    tls: Option<&OutgoingGrpcTlsConfig>,
) -> Result<Endpoint, GrpcClientError> {
    let uri = grpc_endpoint_uri(grpc_host_port, tls);
    let mut endpoint = Channel::from_shared(uri.clone())
        .map_err(|e| GrpcClientError(format!("invalid gRPC endpoint {}: {}", uri, e)))?
        .http2_keep_alive_interval(GRPC_KEEPALIVE_INTERVAL)
        .keep_alive_timeout(GRPC_KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(false)
        .initial_stream_window_size(Some(GRPC_INITIAL_WINDOW_SIZE))
        .initial_connection_window_size(Some(GRPC_INITIAL_WINDOW_SIZE))
        .http2_adaptive_window(false);

    if let Some(tls) = tls {
        let parsed = uri
            .parse::<Uri>()
            .map_err(|e| GrpcClientError(format!("invalid gRPC endpoint {}: {}", uri, e)))?;
        let host = parsed
            .host()
            .ok_or_else(|| GrpcClientError(format!("missing host in gRPC endpoint {}", uri)))?;
        let tls_config = ClientTlsConfig::new()
            .identity(Identity::from_pem(
                tls.cert_pem.clone(),
                tls.key_pem.clone(),
            ))
            .ca_certificate(Certificate::from_pem(tls.ca_pem.clone()))
            .domain_name(host.to_string());
        endpoint = endpoint.tls_config(tls_config).map_err(|e| {
            GrpcClientError(format!("configure gRPC TLS for {} failed: {}", uri, e))
        })?;
    }

    Ok(endpoint)
}

/// Connect `endpoint` through a connector that re-validates every resolved
/// address at connect time (Go's `guardedDialerPolicy` mirror), pinning a
/// validated copy/tail source against DNS rebinding. `allow_untrusted`
/// preserves the plain connect for operators that opted out.
pub async fn connect_guarded(
    endpoint: Endpoint,
    target: &str,
    allow_untrusted: bool,
) -> Result<Channel, GrpcClientError> {
    if allow_untrusted {
        return endpoint
            .connect()
            .await
            .map_err(|e| GrpcClientError(format!("connect {} failed: {}", target, e)));
    }
    let target_owned = target.to_string();
    let connector = tower::service_fn(move |uri: Uri| {
        let target = target_owned.clone();
        async move {
            let host = uri.host().unwrap_or_default().to_string();
            let port = uri.port_u16().unwrap_or(80);
            crate::remote_storage::guarded_tcp_connect(&host, port, &target)
                .await
                .map(hyper_util::rt::TokioIo::new)
        }
    });
    endpoint
        .connect_with_connector(connector)
        .await
        .map_err(|e| GrpcClientError(format!("connect {} failed: {}", target, e)))
}

/// How a dial is bounded.
///
/// `connect_timeout` is handed to the TCP connector. `request_timeout` becomes
/// [`Endpoint::timeout`], which tonic installs as a `GrpcTimeout` layer in
/// front of *every* request the resulting channel carries — it is not a
/// property of one call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GrpcDialOptions {
    /// Bound on establishing the connection to the peer.
    pub connect_timeout: Duration,
    /// Deadline applied to each RPC on the channel, or `None` to leave them
    /// unbounded.
    pub request_timeout: Option<Duration>,
}

impl GrpcDialOptions {
    /// A short request/response call: connect within 5 s, answer within 10 s.
    pub fn unary() -> Self {
        Self {
            connect_timeout: GRPC_CONNECT_TIMEOUT,
            request_timeout: Some(Duration::from_secs(10)),
        }
    }

    /// A call the peer may take a while to answer: connect within 5 s, answer
    /// within 30 s.
    pub fn long() -> Self {
        Self {
            connect_timeout: GRPC_CONNECT_TIMEOUT,
            request_timeout: Some(Duration::from_secs(30)),
        }
    }

    /// A bounded connect with no deadline on the RPCs themselves.
    ///
    /// `request_timeout` must stay `None` here. [`Endpoint::timeout`] is not a
    /// transfer budget: tonic layers it as a `GrpcTimeout` around the
    /// response future, which resolves when the server's *first response
    /// headers* arrive, so it bounds how long the peer may take to start
    /// answering — per request, for every request the channel carries. A 10 s
    /// value picked to suit one short call would therefore also be the header
    /// deadline for the `VolumeCopy` that shares the dial, and a busy source
    /// that takes longer than that to open its file would lose the whole copy.
    /// `VolumeCopy`, `VolumeTailSender` and `VolumeEcShardsCopy` have never
    /// carried one.
    pub fn stream() -> Self {
        Self {
            connect_timeout: GRPC_CONNECT_TIMEOUT,
            request_timeout: None,
        }
    }
}

/// Dial a peer and return a connected channel.
///
/// The error carries only the transport failure: every caller already wraps it
/// with the address and the operation it was attempting.
pub async fn connect_channel(
    grpc_host_port: &str,
    tls: Option<&OutgoingGrpcTlsConfig>,
    opts: GrpcDialOptions,
) -> Result<Channel, GrpcClientError> {
    let mut endpoint =
        build_grpc_endpoint(grpc_host_port, tls)?.connect_timeout(opts.connect_timeout);
    if let Some(request_timeout) = opts.request_timeout {
        endpoint = endpoint.timeout(request_timeout);
    }
    endpoint
        .connect()
        .await
        .map_err(|e| GrpcClientError(e.to_string()))
}

/// Dial a copy/tail source and return a connected channel, re-validating every
/// resolved address at connect time.
///
/// The guarded equivalent of [`connect_channel`]: same `opts` bounds, but the
/// dial goes through [`connect_guarded`] so a source address that passed
/// validation cannot be re-pointed by DNS between the check and the connect.
/// The bounds are applied to the endpoint *before* delegating, so the
/// `allow_untrusted` opt-out is timed too.
///
/// `target` is the caller-facing source address (the unparsed
/// `"ip:port.grpcPort"` form), which is what the guard pins against; the error
/// carries only the transport failure, as every caller already wraps it with
/// the address and the operation it was attempting.
pub async fn connect_channel_guarded(
    grpc_host_port: &str,
    target: &str,
    tls: Option<&OutgoingGrpcTlsConfig>,
    opts: GrpcDialOptions,
    allow_untrusted: bool,
) -> Result<Channel, GrpcClientError> {
    let mut endpoint =
        build_grpc_endpoint(grpc_host_port, tls)?.connect_timeout(opts.connect_timeout);
    if let Some(request_timeout) = opts.request_timeout {
        endpoint = endpoint.timeout(request_timeout);
    }
    connect_guarded(endpoint, target, allow_untrusted).await
}

/// The outgoing request-id interceptor as a concrete type, so the client
/// aliases below can name it.
pub type RequestIdInterceptor = fn(Request<()>) -> Result<Request<()>, Status>;

/// A volume-server client with the request-id interceptor attached.
pub type VolumeServerGrpcClient =
    VolumeServerClient<InterceptedService<Channel, RequestIdInterceptor>>;
/// A master client with the request-id interceptor attached.
pub type MasterGrpcClient = SeaweedClient<InterceptedService<Channel, RequestIdInterceptor>>;
/// A filer client with the request-id interceptor attached.
pub type FilerGrpcClient = SeaweedFilerClient<InterceptedService<Channel, RequestIdInterceptor>>;

/// Wrap a connected channel in a volume-server client that forwards the
/// current request id and lifts both message-size limits.
pub fn volume_server_client(channel: Channel) -> VolumeServerGrpcClient {
    VolumeServerClient::with_interceptor(
        channel,
        outgoing_request_id_interceptor as RequestIdInterceptor,
    )
    .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
    .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
}

/// Wrap a connected channel in a master client that forwards the current
/// request id and lifts both message-size limits.
pub fn master_client(channel: Channel) -> MasterGrpcClient {
    SeaweedClient::with_interceptor(
        channel,
        outgoing_request_id_interceptor as RequestIdInterceptor,
    )
    .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
    .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
}

/// Wrap a connected channel in a filer client that forwards the current
/// request id and lifts both message-size limits.
pub fn filer_client(channel: Channel) -> FilerGrpcClient {
    SeaweedFilerClient::with_interceptor(
        channel,
        outgoing_request_id_interceptor as RequestIdInterceptor,
    )
    .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
    .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
}

/// Parse a SeaweedFS server address (`"ip:port.grpcPort"` or
/// `"ip:port"`) into the `host:grpcPort` form `build_grpc_endpoint`
/// expects. With the trailing `.grpcPort` segment, that segment IS
/// the gRPC port; without it, the gRPC port is `port + 10000`
/// (SeaweedFS's HTTP↔gRPC port-offset convention).
///
/// Shared between `grpc_server.rs` and the distributed-EC-read path
/// in `store_ec.rs` — keep this as the single source of truth so the
/// HTTP↔gRPC port translation can't drift between callers.
///
/// The rule itself lives in `seaweed_common::address`, which the Rust
/// plugin workers share; this wrapper only flattens the typed error
/// back to the `String` its callers already handle. Unbracketed IPv6
/// literals come back bracketed, which this copy used to get wrong.
pub fn parse_grpc_address(source: &str) -> Result<String, String> {
    seaweed_common::address::to_grpc_address(source).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        GrpcDialOptions, build_grpc_endpoint, connect_channel, grpc_endpoint_uri,
        load_outgoing_grpc_tls, volume_server_client,
    };
    use crate::config::{NeedleMapKind, ReadMode, VolumeServerConfig};
    use crate::pb::volume_server_pb;
    use crate::security::tls::TlsPolicy;
    use crate::server::request_id::scope_request_id;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIBPDCB76ADAgECAhRuRPQgeAu43BT/M7EfAWSdapVdYDAFBgMrZXAwFDESMBAG\nA1UEAwwJbG9jYWxob3N0MB4XDTI2MDcwNTE2MTUyOVoXDTM2MDcwMjE2MTUyOVow\nFDESMBAGA1UEAwwJbG9jYWxob3N0MCowBQYDK2VwAyEAr/3bNIFI+8V32oCiY6y+\nXRFmZpdNQ2g//VtRkT+nQg+jUzBRMB0GA1UdDgQWBBTsy9tLf1zPiXCQfgci6zNi\ndEzRSjAfBgNVHSMEGDAWgBTsy9tLf1zPiXCQfgci6zNidEzRSjAPBgNVHRMBAf8E\nBTADAQH/MAUGAytlcANBAIvsdw0IbvOBBkb9cd7BfMJfIP9pQQrAL03pCRWJFnFh\nSysaLVgFXI4T078IiaM874oO+iB+5vNbWEpc7CkGow4=\n-----END CERTIFICATE-----\n";
    const TEST_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIHbyn71Kk+Y7KT3sBctit7uZpErpoH6qDbFj6P8qGaZH\n-----END PRIVATE KEY-----\n";

    #[test]
    fn test_build_grpc_endpoint_with_tls_resolves_crypto_provider() {
        crate::security::tls::install_default_crypto_provider();
        let tls = super::OutgoingGrpcTlsConfig {
            cert_pem: TEST_CERT_PEM.to_string(),
            key_pem: TEST_KEY_PEM.to_string(),
            ca_pem: TEST_CERT_PEM.to_string(),
        };
        let endpoint = build_grpc_endpoint("127.0.0.1:19333", Some(&tls)).unwrap();
        assert_eq!(endpoint.uri().scheme_str(), Some("https"));
    }

    fn sample_config() -> VolumeServerConfig {
        VolumeServerConfig {
            port: 8080,
            grpc_port: 18080,
            public_port: 8080,
            ip: "127.0.0.1".to_string(),
            bind_ip: String::new(),
            public_url: "127.0.0.1:8080".to_string(),
            id: String::new(),
            masters: vec![],
            pre_stop_seconds: 0,
            idle_timeout: 0,
            data_center: String::new(),
            rack: String::new(),
            index_type: NeedleMapKind::InMemory,
            disk_type: String::new(),
            folders: vec![],
            folder_max_limits: vec![],
            folder_tags: vec![],
            min_free_spaces: vec![],
            disk_types: vec![],
            idx_folder: String::new(),
            white_list: vec![],
            fix_jpg_orientation: false,
            read_mode: ReadMode::Local,
            allow_untrusted_remote_endpoints: false,
            cpu_profile: String::new(),
            mem_profile: String::new(),
            compaction_byte_per_second: 0,
            maintenance_byte_per_second: 0,
            file_size_limit_bytes: 0,
            concurrent_upload_limit: 0,
            concurrent_download_limit: 0,
            inflight_upload_data_timeout: std::time::Duration::from_secs(0),
            inflight_download_data_timeout: std::time::Duration::from_secs(0),
            has_slow_read: false,
            read_buffer_size_mb: 0,
            ldb_timeout: 0,
            pprof: false,
            metrics_port: 0,
            metrics_ip: String::new(),
            debug: false,
            debug_port: 0,
            ui_enabled: false,
            jwt_signing_key: vec![],
            jwt_signing_expires_seconds: 0,
            jwt_read_signing_key: vec![],
            jwt_read_signing_expires_seconds: 0,
            https_cert_file: String::new(),
            https_key_file: String::new(),
            https_ca_file: String::new(),
            https_client_enabled: false,
            https_client_cert_file: String::new(),
            https_client_key_file: String::new(),
            https_client_ca_file: String::new(),
            grpc_cert_file: String::new(),
            grpc_key_file: String::new(),
            grpc_client_cert_file: String::new(),
            grpc_client_key_file: String::new(),
            grpc_ca_file: String::new(),
            grpc_allowed_wildcard_domain: String::new(),
            grpc_volume_allowed_common_names: vec![],
            tls_policy: TlsPolicy::default(),
            enable_write_queue: false,
            security_file: String::new(),
        }
    }

    #[test]
    fn test_grpc_endpoint_uri_uses_https_when_tls_enabled() {
        let tls = super::OutgoingGrpcTlsConfig {
            cert_pem: "cert".to_string(),
            key_pem: "key".to_string(),
            ca_pem: "ca".to_string(),
        };
        assert_eq!(
            grpc_endpoint_uri("master.example.com:19333", Some(&tls)),
            "https://master.example.com:19333"
        );
    }

    #[test]
    fn test_load_outgoing_grpc_tls_requires_cert_key_and_ca() {
        let mut config = sample_config();
        config.grpc_cert_file = "/tmp/client.pem".to_string();
        assert!(load_outgoing_grpc_tls(&config).unwrap().is_none());
    }

    fn write_pem_files(dir: &tempfile::TempDir, config: &mut VolumeServerConfig) {
        let write = |name: &str, content: &str| {
            let path = dir.path().join(name);
            std::fs::write(&path, content).unwrap();
            path.to_str().unwrap().to_string()
        };
        config.grpc_cert_file = write("server.pem", "server-cert");
        config.grpc_key_file = write("server.key", "server-key");
        config.grpc_ca_file = write("ca.pem", "ca");
    }

    #[test]
    fn test_load_outgoing_grpc_tls_prefers_client_cert() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut config = sample_config();
        write_pem_files(&dir, &mut config);
        let client_cert = dir.path().join("client.pem");
        let client_key = dir.path().join("client.key");
        std::fs::write(&client_cert, "client-cert").unwrap();
        std::fs::write(&client_key, "client-key").unwrap();
        config.grpc_client_cert_file = client_cert.to_str().unwrap().to_string();
        config.grpc_client_key_file = client_key.to_str().unwrap().to_string();

        let tls = load_outgoing_grpc_tls(&config).unwrap().unwrap();
        assert_eq!(tls.cert_pem, "client-cert");
        assert_eq!(tls.key_pem, "client-key");
    }

    #[test]
    fn test_load_outgoing_grpc_tls_falls_back_to_server_cert() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut config = sample_config();
        write_pem_files(&dir, &mut config);

        let tls = load_outgoing_grpc_tls(&config).unwrap().unwrap();
        assert_eq!(tls.cert_pem, "server-cert");
        assert_eq!(tls.key_pem, "server-key");
    }

    #[test]
    fn test_build_grpc_endpoint_without_tls_uses_http_scheme() {
        let endpoint = build_grpc_endpoint("127.0.0.1:19333", None).unwrap();
        assert_eq!(endpoint.uri().scheme_str(), Some("http"));
    }

    #[test]
    fn test_parse_grpc_address_dotted_form() {
        use super::parse_grpc_address;
        assert_eq!(
            parse_grpc_address("127.0.0.1:8080.18080").unwrap(),
            "127.0.0.1:18080"
        );
    }

    #[test]
    fn test_parse_grpc_address_implicit_form_adds_10000() {
        use super::parse_grpc_address;
        assert_eq!(
            parse_grpc_address("127.0.0.1:8080").unwrap(),
            "127.0.0.1:18080"
        );
    }

    #[test]
    fn test_parse_grpc_address_rejects_non_numeric_http_port_in_dotted_form() {
        use super::parse_grpc_address;
        let err = parse_grpc_address("host:abc.18080").unwrap_err();
        assert!(err.contains("invalid http port"), "{}", err);
    }

    #[test]
    fn test_parse_grpc_address_rejects_non_numeric_grpc_port_in_dotted_form() {
        use super::parse_grpc_address;
        let err = parse_grpc_address("host:8080.xyz").unwrap_err();
        assert!(err.contains("invalid grpc port"), "{}", err);
    }

    #[test]
    fn test_parse_grpc_address_rejects_implicit_port_that_overflows() {
        use super::parse_grpc_address;
        let err = parse_grpc_address("127.0.0.1:60000").unwrap_err();
        assert!(err.contains("out of range"), "{}", err);
    }

    #[test]
    fn test_parse_grpc_address_rejects_input_without_colon() {
        use super::parse_grpc_address;
        let err = parse_grpc_address("hostname").unwrap_err();
        assert!(err.contains("cannot parse"), "{}", err);
    }

    #[test]
    fn test_parse_grpc_address_brackets_ipv6_literals() {
        use super::parse_grpc_address;
        // This used to come back as `::1:29333`, which is not a valid
        // authority: `build_grpc_endpoint` reads the last colon as the port
        // separator and rejects the rest.
        assert_eq!(parse_grpc_address("::1:19333").unwrap(), "[::1]:29333");
        assert_eq!(parse_grpc_address("::1:9333.19333").unwrap(), "[::1]:19333");
        // Already bracketed, so it is left alone.
        assert_eq!(parse_grpc_address("[::1]:9333").unwrap(), "[::1]:19333");
    }

    #[test]
    fn test_build_grpc_endpoint_accepts_an_ipv6_master_address() {
        use super::parse_grpc_address;
        let endpoint = build_grpc_endpoint(&parse_grpc_address("::1:9333").unwrap(), None).unwrap();
        assert_eq!(endpoint.uri().port_u16(), Some(19333));
    }
    /// A minimal HTTP/2 server that records the gRPC request headers it is
    /// sent and answers every call with a trailers-only `unimplemented`. It is
    /// enough to prove what a helper-built client puts on the wire, without
    /// standing up the whole `VolumeServer` service behind a tonic server.
    async fn serve_header_capture() -> (u16, Arc<Mutex<Option<String>>>) {
        use hyper::service::service_fn;
        use hyper_util::rt::{TokioExecutor, TokioIo};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&seen);

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let captured = Arc::clone(&captured);
                tokio::spawn(async move {
                    let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(
                            TokioIo::new(stream),
                            service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                                let captured = Arc::clone(&captured);
                                async move {
                                    let value = req
                                        .headers()
                                        .get("x-amz-request-id")
                                        .and_then(|v| v.to_str().ok())
                                        .map(str::to_string);
                                    *captured.lock().unwrap() = value;
                                    Ok::<_, std::convert::Infallible>(
                                        hyper::http::Response::builder()
                                            .status(200)
                                            .header("content-type", "application/grpc")
                                            .header("grpc-status", "12")
                                            .body(tonic::body::Body::empty())
                                            .unwrap(),
                                    )
                                }
                            }),
                        )
                        .await;
                });
            }
        });

        (port, seen)
    }

    #[tokio::test]
    async fn test_helper_built_client_sends_the_scoped_request_id() {
        let (port, seen) = serve_header_capture().await;

        let channel = connect_channel(
            &format!("127.0.0.1:{}", port),
            None,
            GrpcDialOptions::unary(),
        )
        .await
        .expect("dial the header-capturing server");

        let mut client = volume_server_client(channel);
        // The interceptor has a request id to forward only inside a scope, so
        // the call has to run inside one for this to test anything.
        let _ = scope_request_id("REQUEST-ID-ON-THE-WIRE".to_string(), async move {
            client
                .ping(volume_server_pb::PingRequest {
                    target: String::new(),
                    target_type: String::new(),
                })
                .await
        })
        .await;

        assert_eq!(
            seen.lock().unwrap().as_deref(),
            Some("REQUEST-ID-ON-THE-WIRE"),
            "a client built by volume_server_client must carry the outgoing request id"
        );
    }

    #[test]
    fn test_dial_presets_match_the_call_sites_they_replace() {
        assert_eq!(
            GrpcDialOptions::unary().connect_timeout,
            Duration::from_secs(5)
        );
        assert_eq!(
            GrpcDialOptions::unary().request_timeout,
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            GrpcDialOptions::long().connect_timeout,
            Duration::from_secs(5)
        );
        assert_eq!(
            GrpcDialOptions::long().request_timeout,
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            GrpcDialOptions::stream().connect_timeout,
            Duration::from_secs(5)
        );
        assert_eq!(
            GrpcDialOptions::stream().request_timeout,
            None,
            "a streaming dial must not put a per-request deadline on the channel"
        );
    }
}
