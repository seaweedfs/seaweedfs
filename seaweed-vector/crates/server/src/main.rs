use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use weed_lance_worker::dataset::FallbackOptions;
use weed_lance_worker::direct_read::{Connection, DirectReads, FilerDialer};
use weed_vector_server::master_client;
use weed_vector_server::pb::vector::vector_service_server::VectorServiceServer;
use weed_vector_server::read_cache::ReadCache;
use weed_vector_server::security::Security;
use weed_vector_server::service::VectorServer;
use weed_vector_server::tls::{self, ServerTls};

/// The VectorService the S3 gateway fans vector calls out to.
#[derive(Parser)]
struct Args {
    /// gRPC listen address.
    #[arg(long, env = "WEED_VECTOR_LISTEN", default_value = "127.0.0.1:8090")]
    listen: String,
    /// object_store options for deployments whose gateway vends no
    /// credentials: key=value, repeatable. Credentials given here are used
    /// only with the aws_endpoint given here.
    #[arg(long = "storage-option", alias = "storage.option", value_parser = parse_option)]
    storage_options: Vec<(String, String)>,
    /// Comma-separated master addresses. The engine registers with them, so
    /// the gateway finds it, and asks them for the filers it reads through.
    #[arg(long, alias = "master", env = "WEED_VECTOR_MASTERS")]
    masters: String,
    /// The filer group whose filers the engine reads through.
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
    /// The cluster's security.toml, for gRPC TLS: this server's identity is
    /// [grpc.vector], the masters are dialed with [grpc.client]. By default
    /// the file weed processes find in -config_dir, ., ~/.seaweedfs and
    /// /etc/seaweedfs.
    #[arg(long, env = "WEED_VECTOR_SECURITY_TOML")]
    security_toml: Option<PathBuf>,
    /// The directory weed's -config_dir names, searched for security.toml
    /// first.
    #[arg(long, alias = "config_dir", env = "WEED_VECTOR_CONFIG_DIR")]
    config_dir: Option<PathBuf>,
    /// Ignored: the gateway resolves every table it sends. Still accepted so
    /// an existing command line starts.
    #[arg(long, hide = true)]
    namespace: Option<String>,
    /// Ignored, like --namespace.
    #[arg(long, hide = true)]
    filer_signing_key: Option<String>,
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
/// How often the masters are asked for the filers.
const FILER_REFRESH: Duration = Duration::from_secs(30);
/// How long startup waits for a filer to be registered.
const FILER_WAIT: Duration = Duration::from_secs(120);

/// The filers the masters list, waiting for the first to register.
async fn discover_filers(
    masters: &[String],
    filer_group: &str,
    tls: Option<&weed_vector_server::security::GrpcTls>,
) -> Result<Vec<String>> {
    let deadline = tokio::time::Instant::now() + FILER_WAIT;
    loop {
        match master_client::list_filers(masters, filer_group, tls).await {
            Ok(filers) if !filers.is_empty() => return Ok(filers),
            Ok(_) => tracing::warn!("no filer is registered with the masters yet"),
            Err(err) => tracing::warn!("listing filers: {err:#}"),
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("no filer registered with the masters within {FILER_WAIT:?}");
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn follow_filers(
    reads: DirectReads,
    masters: Vec<String>,
    filer_group: String,
    tls: Option<weed_vector_server::security::GrpcTls>,
) {
    loop {
        tokio::time::sleep(FILER_REFRESH).await;
        match master_client::list_filers(&masters, &filer_group, tls.as_ref()).await {
            Ok(filers) if !filers.is_empty() => {
                if let Err(err) = reads.set_filers(&filers) {
                    tracing::warn!("filers {}: {err:#}", filers.join(","));
                }
            }
            // Keep the last known filers while the masters list none.
            Ok(_) => tracing::warn!("the masters list no filer; keeping the known ones"),
            Err(err) => tracing::warn!("listing filers: {err:#}"),
        }
    }
}

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
    if args.namespace.is_some() || args.filer_signing_key.is_some() {
        tracing::warn!(
            "--namespace and --filer-signing-key are ignored: the gateway resolves every table it sends"
        );
    }
    weed_lance_worker::tls::install_default_crypto_provider();
    let security = Security::load(args.security_toml.as_deref(), args.config_dir.as_deref())?;
    if let Some(path) = security.path() {
        tracing::info!("security settings from {}", path.display());
    }
    let master_tls = security.grpc_client_tls("grpc.client", "grpc.master")?;
    let server_tls = security
        .grpc_server_tls("grpc.vector")?
        .map(ServerTls::load)
        .transpose()?;
    // Bind before registering, so the master never lists an engine that is
    // about to exit because its port is taken.
    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("listen on {}", args.listen))?;
    let masters: Vec<String> = args
        .masters
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    for master in &masters {
        master_client::grpc_addr(master)?;
    }
    // The filers are dialed as the masters are: grpc.client's credentials
    // and server allowlist, through the engine's own connector.
    let filer_dialer: Option<FilerDialer> = security
        .grpc_client_tls("grpc.client", "grpc.filer")?
        .map(|tls| {
            Arc::new(move |addr: &str| {
                let channel = tonic::transport::Endpoint::new(format!("http://{addr}"))?
                    .connect_timeout(Duration::from_secs(10))
                    .connect_with_connector_lazy(tls::connector(tls.clone()));
                Ok(sw_client::FilerClient::from_channel(addr, channel))
            }) as FilerDialer
        });
    let conn = Connection {
        filers: discover_filers(&masters, &args.filer_group, master_tls.as_ref()).await?,
        read_jwt_key: security.jwt_read_key(),
        sse: security.sse(),
        grpc_tls: None,
        https_tls: security.https_client_tls()?,
        filer_dialer,
    };
    let reads = DirectReads::connect(&conn).await.context("direct reads")?;
    tracing::info!(
        "reading tables through {reads:?}; filer gRPC {}, volume servers {}; SSE-S3 objects {}",
        if conn.filer_dialer.is_some() {
            "over TLS"
        } else {
            "plaintext"
        },
        if conn.https_tls.is_some() {
            "over HTTPS"
        } else {
            "over HTTP"
        },
        if reads.sse_s3_configured() {
            "are readable"
        } else {
            "need s3.sse.kek or s3.sse.key"
        },
    );
    tokio::spawn(follow_filers(
        reads.clone(),
        masters.clone(),
        args.filer_group.clone(),
        master_tls.clone(),
    ));
    let fallback: FallbackOptions = args.storage_options.into_iter().collect();
    let mut service = VectorServer::new(fallback)
        .with_nprobes(args.nprobes.map(std::num::NonZeroUsize::get))
        .with_direct_reads(reads);
    if args.read_cache_mb > 0 {
        service = service.with_read_cache(ReadCache::new(args.read_cache_mb << 20));
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
        if master_tls.is_some() { "over TLS" } else { "in plaintext" },
    );
    // Registered only now, with storage connected and the listener bound,
    // so the gateway never finds an engine that cannot serve yet.
    let advertise = advertised_addr(&args.listen, &masters);
    tokio::spawn(master_client::keep_connected(
        masters.clone(),
        advertise,
        master_tls.clone(),
    ));
    let router = Server::builder()
        // Match the gateway's 1 GiB cap — put batches over the tonic 4 MiB
        // default otherwise get rejected before they reach the handler.
        .add_service(
            VectorServiceServer::new(service)
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
    fn nprobes_must_be_positive() {
        let parse = |nprobes: &str| {
            Args::try_parse_from(["weed-vector", "--masters", "m:9333", "--nprobes", nprobes])
        };
        assert!(parse("0").is_err());
        let args = parse("12").unwrap();
        assert_eq!(args.nprobes.map(std::num::NonZeroUsize::get), Some(12));
    }

    #[test]
    fn an_earlier_command_line_still_parses() {
        let args = Args::try_parse_from(normalize_args(
            [
                "weed-vector",
                "-master",
                "m:9333",
                "--namespace",
                "http://localhost:9101",
                "-filer-signing-key",
                "k",
            ]
            .map(String::from)
            .to_vec(),
        ))
        .unwrap();
        assert!(args.namespace.is_some() && args.filer_signing_key.is_some());
    }

    #[test]
    fn the_masters_are_required() {
        assert!(Args::try_parse_from(["weed-vector"]).is_err());
    }
}
