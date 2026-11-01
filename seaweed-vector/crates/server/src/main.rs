use anyhow::Result;
use clap::Parser;
use tonic::transport::Server;
use weed_lance_worker::catalog::{NamespaceClient, SigningKey};
use weed_lance_worker::dataset::FallbackOptions;
use weed_vector_server::pb::vector::vector_service_server::VectorServiceServer;
use weed_vector_server::read_cache::ReadCache;
use weed_vector_server::service::VectorServer;

/// The VectorService the S3 gateway fans vector calls out to.
#[derive(Parser)]
struct Args {
    /// Lance Namespace base URL, e.g. http://localhost:8888/lance.
    #[arg(
        long,
        env = "WEED_VECTOR_NAMESPACE",
        default_value = "http://localhost:8888/lance"
    )]
    namespace: String,
    /// gRPC listen address.
    #[arg(long, env = "WEED_VECTOR_LISTEN", default_value = "127.0.0.1:8080")]
    listen: String,
    /// object_store option fallback for deployments that vend no credentials:
    /// key=value, repeatable.
    #[arg(long = "storage-option", alias = "storage.option", value_parser = parse_option)]
    storage_options: Vec<(String, String)>,
    /// Comma-separated master addresses to register this server with; the
    /// gateway discovers it via ListClusterNodes(client_type=vector).
    #[arg(long, alias = "master", env = "WEED_VECTOR_MASTERS")]
    masters: Option<String>,
    /// Memory for caching reads of write-once table files, in MiB; 0 disables.
    #[arg(long, env = "WEED_VECTOR_READ_CACHE_MB", default_value_t = 512)]
    read_cache_mb: u64,
    /// The `jwt.filer_signing.key` of the cluster. The Lance namespace vends
    /// credentials that write index data only to a caller holding it.
    #[arg(long, env = "WEED_FILER_SIGNING_KEY")]
    filer_signing_key: Option<String>,
    /// Lifetime of each token minted from the signing key, mirroring
    /// `jwt.filer_signing.expires_after_seconds`. Zero mints tokens with no
    /// expiry.
    #[arg(long, default_value = "0")]
    filer_signing_key_expires_after_seconds: u64,
}

fn parse_option(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| "expected key=value".to_string())
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
fn advertised_addr(listen: &str, masters: &[String]) -> String {
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
    if let Some(masters) = &args.masters {
        let masters: Vec<String> = masters.split(',').map(|s| s.trim().to_string()).collect();
        for master in &masters {
            weed_vector_server::master_client::grpc_addr(master)?;
        }
        let advertise = advertised_addr(&args.listen, &masters);
        tokio::spawn(weed_vector_server::master_client::keep_connected(
            masters, advertise,
        ));
    }
    let fallback: FallbackOptions = args.storage_options.into_iter().collect();
    let signing_key = SigningKey::new(
        args.filer_signing_key,
        args.filer_signing_key_expires_after_seconds,
    );
    let client = NamespaceClient::new(args.namespace.clone())
        .with_fallback(&fallback)
        .with_signing_key(signing_key.as_ref());
    let mut service = VectorServer::new(client, fallback);
    if args.read_cache_mb > 0 {
        service = service.with_read_cache(ReadCache::new(args.read_cache_mb << 20));
    }
    tracing::info!(namespace = %args.namespace, listen = %args.listen, "weed-vector");
    Server::builder()
        // Match the gateway's 1 GiB cap — put batches over the tonic 4 MiB
        // default otherwise get rejected before they reach the handler.
        .add_service(
            VectorServiceServer::new(service)
                .max_decoding_message_size(1 << 30)
                .max_encoding_message_size(1 << 30),
        )
        .serve(args.listen.parse()?)
        .await?;
    Ok(())
}
