use std::time::Duration;

/// How one worker process connects and how much work it will take on.
#[derive(Clone, Debug)]
pub struct WorkerOptions {
    /// Admin gRPC address, e.g. "localhost:23646".
    pub admin_address: String,
    pub worker_id: String,
    pub worker_version: String,
    /// Advertised address; empty when the worker takes no inbound connections.
    pub worker_address: String,
    pub heartbeat_interval: Duration,
    pub reconnect_delay: Duration,
    pub max_detection_concurrency: i32,
    pub max_execution_concurrency: i32,
    /// mTLS for the control stream, mirroring the Go worker's `[grpc.worker]`
    /// section of security.toml. None means plaintext, which is the default the
    /// Go worker also takes when no certificates are configured.
    pub tls: Option<TlsOptions>,
}

/// Certificates for the control stream. All three are required together: the
/// cluster's gRPC TLS is mutual, so a CA without a client identity gets refused
/// by admin rather than falling back to one-way TLS.
#[derive(Clone, Debug)]
pub struct TlsOptions {
    pub ca_path: String,
    pub client_cert_path: String,
    pub client_key_path: String,
    /// Name to verify the server certificate against, when the address a worker
    /// dials is not the name the certificate carries.
    pub server_name: Option<String>,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            admin_address: "localhost:23646".to_string(),
            worker_id: String::new(),
            worker_version: env!("CARGO_PKG_VERSION").to_string(),
            worker_address: String::new(),
            heartbeat_interval: Duration::from_secs(10),
            reconnect_delay: Duration::from_secs(5),
            max_detection_concurrency: 1,
            max_execution_concurrency: 1,
            tls: None,
        }
    }
}
