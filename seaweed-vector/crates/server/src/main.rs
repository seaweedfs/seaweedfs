use anyhow::Result;
use clap::Parser;
use tonic::transport::Server;
use weed_lance_worker::catalog::NamespaceClient;
use weed_lance_worker::dataset::FallbackOptions;
use weed_vector_server::pb::vector::vector_service_server::VectorServiceServer;
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
    #[arg(long = "storage-option", value_parser = parse_option)]
    storage_options: Vec<(String, String)>,
    /// Comma-separated master addresses to register this server with; the
    /// gateway discovers it via ListClusterNodes(client_type=vector).
    #[arg(long, env = "WEED_VECTOR_MASTERS")]
    masters: Option<String>,
}

fn parse_option(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| "expected key=value".to_string())
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
        let probe = master
            .rsplit_once("://")
            .map(|(_, r)| r)
            .unwrap_or(master);
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
    let args = Args::parse();
    if let Some(masters) = &args.masters {
        let masters: Vec<String> = masters.split(',').map(|s| s.trim().to_string()).collect();
        let advertise = advertised_addr(&args.listen, &masters);
        tokio::spawn(weed_vector_server::master_client::keep_connected(
            masters,
            advertise,
        ));
    }
    let fallback: FallbackOptions = args.storage_options.into_iter().collect();
    let client = NamespaceClient::new(args.namespace.clone()).with_fallback(&fallback);
    let service = VectorServer::new(client, fallback);
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
