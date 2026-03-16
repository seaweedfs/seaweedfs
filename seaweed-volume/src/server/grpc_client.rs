use std::error::Error;
use std::fmt;
use std::time::Duration;

use hyper::http::Uri;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::config::VolumeServerConfig;

pub const GRPC_MAX_MESSAGE_SIZE: usize = 1 << 30;
const GRPC_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);
const GRPC_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);
const GRPC_INITIAL_WINDOW_SIZE: u32 = 16 * 1024 * 1024;

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
    if config.grpc_cert_file.is_empty()
        || config.grpc_key_file.is_empty()
        || config.grpc_ca_file.is_empty()
    {
        return Ok(None);
    }

    let cert_pem = std::fs::read_to_string(&config.grpc_cert_file).map_err(|e| {
        GrpcClientError(format!(
            "Failed to read outgoing gRPC cert '{}': {}",
            config.grpc_cert_file, e
        ))
    })?;
    let key_pem = std::fs::read_to_string(&config.grpc_key_file).map_err(|e| {
        GrpcClientError(format!(
            "Failed to read outgoing gRPC key '{}': {}",
            config.grpc_key_file, e
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

pub fn build_grpc_endpoint(
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

#[cfg(test)]
mod tests {
    use super::{build_grpc_endpoint, grpc_endpoint_uri, load_outgoing_grpc_tls};
    use crate::config::{NeedleMapKind, ReadMode, VolumeServerConfig};
    use crate::security::tls::TlsPolicy;

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

    #[test]
    fn test_build_grpc_endpoint_without_tls_uses_http_scheme() {
        let endpoint = build_grpc_endpoint("127.0.0.1:19333", None).unwrap();
        assert_eq!(endpoint.uri().scheme_str(), Some("http"));
    }
}
