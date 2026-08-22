use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use seaweed_worker_core::{Metrics, Registry, TlsOptions, WorkerOptions};
use weed_lance_worker::handlers;
use weed_lance_worker::metrics::LanceMetrics;

/// Mirrors `weed worker`'s flags, because this is the same contract from another
/// language and an operator should not have to learn a second set of names.
#[derive(Parser, Debug)]
#[command(
    name = "weed-worker",
    about = "SeaweedFS maintenance worker"
)]
struct Args {
    /// Admin server gRPC address.
    #[arg(long, default_value = "localhost:23646", env = "WEED_ADMIN")]
    admin: String,

    /// Worker identity reported to admin.
    #[arg(long, default_value = "lance-worker", env = "WEED_WORKER_ID")]
    id: String,

    /// Lance namespace the worker lists tables from.
    #[arg(
        long,
        default_value = "http://localhost:9101",
        env = "WEED_LANCE_NAMESPACE"
    )]
    namespace: String,

    #[arg(long, default_value = "10", env = "WEED_HEARTBEAT_SECONDS")]
    heartbeat_seconds: u64,

    #[arg(long, default_value = "1")]
    max_concurrency: i32,

    /// Storage credentials to use where the namespace vends none. A gateway
    /// with STS configured vends its own and these are ignored.
    #[arg(long, env = "WEED_S3_ACCESS_KEY")]
    access_key: Option<String>,

    #[arg(long, env = "WEED_S3_SECRET_KEY")]
    secret_key: Option<String>,

    /// mTLS for the admin stream, the same certificates the Go worker reads
    /// from the [grpc.worker] section of security.toml. All three together, or
    /// none, in which case the stream is plaintext.
    #[arg(long, env = "WEED_GRPC_CA")]
    tls_ca: Option<String>,

    #[arg(long, env = "WEED_GRPC_CLIENT_CERT")]
    tls_cert: Option<String>,

    #[arg(long, env = "WEED_GRPC_CLIENT_KEY")]
    tls_key: Option<String>,

    /// Name to verify admin's certificate against, when it is not the address
    /// this worker dials.
    #[arg(long)]
    tls_server_name: Option<String>,

    /// Serve /health, /ready and /metrics on this port, the way
    /// `weed worker -metricsPort` does. Zero, the default, serves nothing.
    #[arg(long, default_value = "0", env = "WEED_METRICS_PORT")]
    metrics_port: u16,

    /// Address the metrics server binds. Loopback by default, since the
    /// endpoint is unauthenticated.
    #[arg(long, default_value = "127.0.0.1", env = "WEED_METRICS_IP")]
    metrics_ip: String,
}

impl Args {
    /// The TLS configuration, or an error when the three certificate paths do
    /// not arrive together: a CA on its own would silently give one-way TLS,
    /// which the cluster's mutual setup refuses anyway.
    fn tls(&self) -> Result<Option<TlsOptions>> {
        match (
            self.tls_ca.clone(),
            self.tls_cert.clone(),
            self.tls_key.clone(),
        ) {
            (None, None, None) => Ok(None),
            (Some(ca_path), Some(client_cert_path), Some(client_key_path)) => {
                Ok(Some(TlsOptions {
                    ca_path,
                    client_cert_path,
                    client_key_path,
                    server_name: self.tls_server_name.clone(),
                }))
            }
            _ => Err(anyhow::anyhow!(
                "--tls-ca, --tls-cert and --tls-key must be given together"
            )),
        }
    }
}

/// Builds the metrics bind address. Parsing the host separately is what makes an
/// IPv6 literal work: "::1" and 9327 joined with a colon is not an address, and
/// formatting them that way turns `--metrics-ip ::1` into a startup failure.
/// Brackets are accepted too, since that is how the same address is written in a
/// URL and an operator will reasonably try it.
fn metrics_address(ip: &str, port: u16) -> Result<SocketAddr> {
    let host = ip.trim().trim_start_matches('[').trim_end_matches(']');
    let parsed: IpAddr = host
        .parse()
        .with_context(|| format!("parse the metrics address {ip}"))?;
    Ok(SocketAddr::new(parsed, port))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let tls = args.tls()?;
    let options = WorkerOptions {
        admin_address: args.admin,
        worker_id: args.id,
        heartbeat_interval: Duration::from_secs(args.heartbeat_seconds),
        max_detection_concurrency: args.max_concurrency,
        max_execution_concurrency: args.max_concurrency,
        tls,
        ..Default::default()
    };

    let mut fallback = weed_lance_worker::dataset::FallbackOptions::new();
    if let (Some(access), Some(secret)) = (args.access_key, args.secret_key) {
        fallback.insert("aws_access_key_id".to_string(), access);
        fallback.insert("aws_secret_access_key".to_string(), secret);
    }

    let metrics = Metrics::new(&options.worker_id, &options.worker_version)?;
    let lance_metrics = LanceMetrics::new(&metrics)?;
    if args.metrics_port > 0 {
        seaweed_worker_core::metrics::spawn(
            metrics.clone(),
            metrics_address(&args.metrics_ip, args.metrics_port)?,
        );
    }

    let mut registry = Registry::new().with_preview(Arc::new(
        weed_lance_worker::preview::LancePreview::new(args.namespace.clone(), fallback.clone()),
    ));
    for handler in handlers(args.namespace, fallback, Some(lance_metrics)) {
        registry = registry.register(handler);
    }

    seaweed_worker_core::stream::run_with_metrics(options, registry, metrics).await
}

#[cfg(test)]
mod tests {
    use super::metrics_address;

    #[test]
    fn metrics_address_takes_ipv4_ipv6_and_brackets() {
        assert_eq!(
            metrics_address("127.0.0.1", 9327).unwrap().to_string(),
            "127.0.0.1:9327"
        );
        // Joining these with a colon gives "::1:9327", which is not an address.
        assert_eq!(
            metrics_address("::1", 9327).unwrap().to_string(),
            "[::1]:9327"
        );
        assert_eq!(
            metrics_address("[::1]", 9327).unwrap().to_string(),
            "[::1]:9327"
        );
        assert_eq!(
            metrics_address("0.0.0.0", 9327).unwrap().to_string(),
            "0.0.0.0:9327"
        );
    }

    #[test]
    fn metrics_address_rejects_a_hostname() {
        // Binding takes an address, not a name; saying so beats a confusing
        // failure inside the server.
        assert!(metrics_address("localhost", 9327).is_err());
    }
}
