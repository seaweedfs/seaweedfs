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
}

fn parse_option(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| "expected key=value".to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let fallback: FallbackOptions = args.storage_options.into_iter().collect();
    let client = NamespaceClient::new(args.namespace.clone()).with_fallback(&fallback);
    let service = VectorServer::new(client, fallback);
    tracing::info!(namespace = %args.namespace, listen = %args.listen, "weed-vector-server");
    Server::builder()
        .add_service(VectorServiceServer::new(service))
        .serve(args.listen.parse()?)
        .await?;
    Ok(())
}
