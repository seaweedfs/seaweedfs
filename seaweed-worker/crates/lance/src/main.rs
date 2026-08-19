use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use seaweed_worker_core::{Registry, WorkerOptions};
use weed_lance_worker::handlers;

/// Mirrors `weed worker`'s flags, because this is the same contract from another
/// language and an operator should not have to learn a second set of names.
#[derive(Parser, Debug)]
#[command(
    name = "weed-lance-worker",
    about = "SeaweedFS maintenance worker for Lance tables"
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
    let options = WorkerOptions {
        admin_address: args.admin,
        worker_id: args.id,
        heartbeat_interval: Duration::from_secs(args.heartbeat_seconds),
        max_detection_concurrency: args.max_concurrency,
        max_execution_concurrency: args.max_concurrency,
        ..Default::default()
    };

    let mut fallback = weed_lance_worker::dataset::FallbackOptions::new();
    if let (Some(access), Some(secret)) = (args.access_key, args.secret_key) {
        fallback.insert("aws_access_key_id".to_string(), access);
        fallback.insert("aws_secret_access_key".to_string(), secret);
    }

    let mut registry = Registry::new();
    for handler in handlers(args.namespace, fallback) {
        registry = registry.register(handler);
    }

    seaweed_worker_core::run(options, registry).await
}
