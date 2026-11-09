use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use weed_lance_worker::cluster::Cluster;
use weed_vector_server::pb::vector::vector_service_server::VectorServiceServer;
use weed_vector_server::read_cache::ReadCache;
use weed_vector_server::security::Security;
use weed_vector_server::service::VectorServer;
use weed_vector_server::tls::{self, ServerTls};
use weed_vector_server::{
    master_client,
    metrics::{self, Metrics},
    observed::ObservedService,
};

/// The VectorService the S3 gateway fans vector calls out to.
#[derive(Parser)]
struct Args {
    /// gRPC listen address.
    #[arg(long, env = "WEED_VECTOR_LISTEN", default_value = "127.0.0.1:8090")]
    listen: String,
    /// Address to register when it differs from the listen address.
    #[arg(long, env = "WEED_VECTOR_ADVERTISE")]
    advertise: Option<String>,
    /// Comma-separated master addresses. The engine registers with them, so
    /// the gateway finds it, and asks them for the filers it keeps tables
    /// through.
    #[arg(long, alias = "master", env = "WEED_VECTOR_MASTERS")]
    masters: String,
    /// The filer group whose filers the engine keeps tables through.
    #[arg(
        long,
        alias = "filer.group",
        env = "WEED_VECTOR_FILER_GROUP",
        default_value = ""
    )]
    filer_group: String,
    /// Memory for caching reads of write-once table files, in MiB; 0 disables.
    #[arg(long, env = "WEED_VECTOR_READ_CACHE_MB", default_value_t = 512)]
    read_cache_mb: u64,
    /// Partitions each query probes in a table's vector index. Default: a
    /// tenth of the index's partitions, at least 8; more under a filter.
    #[arg(long, env = "WEED_VECTOR_NPROBES")]
    nprobes: Option<std::num::NonZeroUsize>,
    /// The cluster's security.toml: [grpc.vector] for this server,
    /// [grpc.client] for the masters and filers, https.client for volume
    /// servers under HTTPS, jwt.signing.read.key for signed volume reads,
    /// s3.sse.kek or s3.sse.key for SSE-S3 buckets. By default the file weed
    /// processes find in -config_dir, ., ~/.seaweedfs and /etc/seaweedfs.
    #[arg(long, env = "WEED_VECTOR_SECURITY_TOML")]
    security_toml: Option<PathBuf>,
    #[arg(
        long,
        alias = "metricsPort",
        env = "WEED_VECTOR_METRICS_PORT",
        default_value_t = 0
    )]
    metrics_port: u16,
    #[arg(
        long,
        alias = "metricsIp",
        env = "WEED_VECTOR_METRICS_IP",
        default_value = "127.0.0.1"
    )]
    metrics_ip: String,

    /// The directory weed's -config_dir names, searched for security.toml
    /// first.
    #[arg(long, alias = "config_dir", env = "WEED_VECTOR_CONFIG_DIR")]
    config_dir: Option<PathBuf>,
}

// weed tools take Go-style single-dash long flags; normalize_args converts
// any -flag to --flag — the same normalization weed-volume's config.rs does —
// so -master and --masters parse the same. The masters field carries the
// singular alias clap-side.
fn normalize_args(args: Vec<String>) -> Vec<String> {
    let mut args = args;
    let mut i = 1; // skip args[0] (binary name)
    while i < args.len() {
        let arg = &args[i];
        if arg == "--" {
            break;
        }
        if arg.starts_with("--") || !arg.starts_with('-') {
            i += 1;
            continue;
        }
        let without_dash = &arg[1..];
        if without_dash.len() > 1 && !without_dash.starts_with(|c: char| c.is_ascii_digit()) {
            args[i] = format!("-{}", arg);
        }
        i += 1;
    }
    args
}

/// A wildcard listen address is not reachable for peers; derive the outbound
/// IP toward the first master instead — the address the master sees anyway.
fn advertised_addr(listen: &str, masters: &[String], advertise: Option<&str>) -> String {
    if let Some(address) = advertise
        .map(str::trim)
        .filter(|address| !address.is_empty())
    {
        return address.to_string();
    }
    let Some((host, _)) = listen.rsplit_once(':') else {
        return listen.to_string();
    };
    if !matches!(host, "0.0.0.0" | "::" | "" | "[::]") {
        return listen.to_string();
    }
    let port = listen.rsplit_once(':').map(|(_, p)| p).unwrap_or_default();
    if let Some(master) = masters.first() {
        // Masters may arrive as host:http.grpc or scheme://host:http —
        // the UDP probe only needs a dialable host:port.
        let probe = master.rsplit_once("://").map(|(_, r)| r).unwrap_or(master);
        let probe = match probe.rsplit_once(':') {
            Some((h, p)) => match p.split_once('.') {
                Some((_, g)) => format!("{h}:{g}"),
                None => probe.to_string(),
            },
            None => probe.to_string(),
        };
        if let Ok(sock) = std::net::UdpSocket::bind((host, 0u16)) {
            if sock.connect(&probe).is_ok() {
                if let Ok(local) = sock.local_addr() {
                    return format!("{}:{}", local.ip(), port);
                }
            }
        }
    }
    listen.to_string()
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse_from(normalize_args(std::env::args().collect()));
    weed_lance_worker::tls::install_default_crypto_provider();
    let security = Security::load(args.security_toml.as_deref(), args.config_dir.as_deref())?;
    if let Some(path) = security.path() {
        tracing::info!("security settings from {}", path.display());
    }
    let server_tls = security
        .grpc_server_tls("grpc.vector")?
        .map(ServerTls::load)
        .transpose()?;
    // Bind before registering, so the master never lists an engine that is
    // about to exit because its port is taken.
    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("listen on {}", args.listen))?;
    let cluster = Cluster::new(&args.masters, &args.filer_group, &security)?;
    let reads = cluster.storage(&security).await?;
    let (health, health_service) = tonic_health::server::health_reporter();
    let metrics = Metrics::new()?.with_health(health);
    metrics.set_connected(false).await;
    let cache = (args.read_cache_mb > 0)
        .then(|| ReadCache::new(args.read_cache_mb << 20).with_metrics(metrics.clone()));
    let mut service = VectorServer::new()
        .with_nprobes(args.nprobes.map(std::num::NonZeroUsize::get))
        .with_storage(reads);
    if let Some(cache) = &cache {
        service = service.with_read_cache(cache.clone());
    }
    if args.metrics_port > 0 {
        let ip = args
            .metrics_ip
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()?;
        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::new(ip, args.metrics_port)).await?;
        let metrics = metrics.clone();
        tokio::spawn(async move {
            if let Err(error) = metrics::serve_listener(metrics, listener, cache).await {
                tracing::error!(%error, "vector metrics server stopped");
            }
        });
    }
    tracing::info!(
        listen = %args.listen,
        "weed-vector; serving gRPC {}, dialing masters {}",
        match &server_tls {
            Some(tls) if tls.files().allowed_common_names.is_empty()
                && tls.files().allowed_wildcard_domain.is_empty() =>
                "over TLS with client certificates required",
            Some(_) => "over TLS with allowed client certificates required",
            None => "in plaintext",
        },
        if cluster.master_tls.is_some() { "over TLS" } else { "in plaintext" },
    );
    // Registered only now, with storage connected and the listener bound,
    // so the gateway never finds an engine that cannot serve yet.
    let advertise = advertised_addr(&args.listen, &cluster.masters, args.advertise.as_deref());
    tokio::spawn(master_client::keep_connected(
        cluster.masters.clone(),
        advertise,
        args.filer_group.clone(),
        cluster.master_tls.clone(),
        metrics.clone(),
    ));
    let router = Server::builder()
        .add_service(health_service)
        // Match the gateway's 1 GiB cap — put batches over the tonic 4 MiB
        // default otherwise get rejected before they reach the handler.
        .add_service(
            VectorServiceServer::new(ObservedService {
                inner: service,
                metrics,
            })
            .max_decoding_message_size(1 << 30)
            .max_encoding_message_size(1 << 30),
        );
    match server_tls {
        Some(tls) => {
            tokio::spawn(tls.clone().refresh(tls::refresh_interval()));
            router.serve_with_incoming(tls.incoming(listener)).await?;
        }
        None => {
            router
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await?
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_advertise_uses_the_listen_address() {
        for value in [None, Some(""), Some("  ")] {
            assert_eq!(
                advertised_addr("127.0.0.1:8090", &[], value),
                "127.0.0.1:8090"
            );
        }
        assert_eq!(
            advertised_addr("127.0.0.1:8090", &[], Some("vector.example:8090")),
            "vector.example:8090"
        );
    }

    #[test]
    fn nprobes_must_be_positive() {
        let parse = |nprobes: &str| {
            Args::try_parse_from(["weed-vector", "--masters", "m:9333", "--nprobes", nprobes])
        };
        assert!(parse("0").is_err());
        let args = parse("12").unwrap();
        assert_eq!(args.nprobes.map(std::num::NonZeroUsize::get), Some(12));
    }

    #[test]
    fn the_catalog_flags_are_gone() {
        let parse = |extra: &[&str]| {
            let mut argv = vec!["weed-vector", "-master", "m:9333"];
            argv.extend_from_slice(extra);
            Args::try_parse_from(normalize_args(argv.into_iter().map(String::from).collect()))
        };
        assert_eq!(parse(&[]).unwrap().masters, "m:9333");
        // The gateway resolves every table it sends; the engine has no
        // catalog to name or authenticate to.
        assert!(parse(&["--namespace", "http://localhost:9101"]).is_err());
        assert!(parse(&["-filer-signing-key", "k"]).is_err());
    }

    #[test]
    fn the_masters_are_required() {
        assert!(Args::try_parse_from(["weed-vector"]).is_err());
    }
}
