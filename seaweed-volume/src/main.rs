use std::sync::{Arc, RwLock};

use tracing::{info, error};

use seaweed_volume::config::{self, VolumeServerConfig};
use seaweed_volume::metrics;
use seaweed_volume::security::{Guard, SigningKey};
use seaweed_volume::server::grpc_server::VolumeGrpcService;
use seaweed_volume::server::volume_server::VolumeServerState;
use seaweed_volume::storage::store::Store;
use seaweed_volume::storage::types::DiskType;
use seaweed_volume::pb::volume_server_pb::volume_server_server::VolumeServerServer;

fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = config::parse_cli();
    info!("SeaweedFS Volume Server (Rust) v{}", env!("CARGO_PKG_VERSION"));

    // Register Prometheus metrics
    metrics::register_metrics();

    // Build the tokio runtime and run the async entry point
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to build tokio runtime");

    if let Err(e) = rt.block_on(run(config)) {
        error!("Volume server failed: {}", e);
        std::process::exit(1);
    }
}

async fn run(config: VolumeServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the store
    let mut store = Store::new(config.index_type);
    store.ip = config.ip.clone();
    store.port = config.port;
    store.grpc_port = config.grpc_port;
    store.public_url = config.public_url.clone();
    store.data_center = config.data_center.clone();
    store.rack = config.rack.clone();

    // Add disk locations
    for (i, dir) in config.folders.iter().enumerate() {
        let idx_dir = if config.idx_folder.is_empty() {
            dir.as_str()
        } else {
            config.idx_folder.as_str()
        };
        let max_volumes = config.folder_max_limits[i];
        let disk_type = DiskType::from_string(&config.disk_types[i]);

        info!(
            "Adding storage location: {} (max_volumes={}, disk_type={:?})",
            dir, max_volumes, disk_type
        );
        store.add_location(dir, idx_dir, max_volumes, disk_type)
            .map_err(|e| format!("Failed to add storage location {}: {}", dir, e))?;
    }

    // Build shared state
    // TODO: Wire up JWT signing keys from config. Empty keys are acceptable for now
    // while the Rust volume server is still in development.
    let guard = Guard::new(
        &config.white_list,
        SigningKey(vec![]),
        0,
        SigningKey(vec![]),
        0,
    );
    let state = Arc::new(VolumeServerState {
        store: RwLock::new(store),
        guard,
        is_stopping: RwLock::new(false),
        maintenance: std::sync::atomic::AtomicBool::new(false),
        state_version: std::sync::atomic::AtomicU32::new(0),
    });

    // Build HTTP routers
    let admin_router = seaweed_volume::server::volume_server::build_admin_router(state.clone());
    let admin_addr = format!("{}:{}", config.bind_ip, config.port);

    let public_port = config.public_port;
    let needs_public = public_port != config.port;

    // Build gRPC service
    let grpc_service = VolumeGrpcService {
        state: state.clone(),
    };
    let grpc_addr = format!("{}:{}", config.bind_ip, config.grpc_port);

    info!("Starting HTTP server on {}", admin_addr);
    info!("Starting gRPC server on {}", grpc_addr);
    if needs_public {
        info!("Starting public HTTP server on {}:{}", config.bind_ip, public_port);
    }

    // Set up graceful shutdown via SIGINT/SIGTERM using broadcast channel
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    let state_shutdown = state.clone();
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to install SIGTERM handler");
            tokio::select! {
                _ = ctrl_c => { info!("Received SIGINT, shutting down..."); }
                _ = sigterm.recv() => { info!("Received SIGTERM, shutting down..."); }
            }
        }
        #[cfg(not(unix))]
        {
            ctrl_c.await.ok();
            info!("Received shutdown signal...");
        }
        *state_shutdown.is_stopping.write().unwrap() = true;
        let _ = shutdown_tx_clone.send(());
    });

    // Spawn all servers concurrently
    let admin_listener = tokio::net::TcpListener::bind(&admin_addr)
        .await
        .unwrap_or_else(|e| panic!("Failed to bind HTTP to {}: {}", admin_addr, e));
    info!("HTTP server listening on {}", admin_addr);

    let http_handle = {
        let mut shutdown_rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            if let Err(e) = axum::serve(admin_listener, admin_router)
                .with_graceful_shutdown(async move { let _ = shutdown_rx.recv().await; })
                .await
            {
                error!("HTTP server error: {}", e);
            }
        })
    };

    let grpc_handle = {
        let mut shutdown_rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let addr = grpc_addr.parse().expect("Invalid gRPC address");
            info!("gRPC server listening on {}", addr);
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(VolumeServerServer::new(grpc_service))
                .serve_with_shutdown(addr, async move { let _ = shutdown_rx.recv().await; })
                .await
            {
                error!("gRPC server error: {}", e);
            }
        })
    };

    let public_handle = if needs_public {
        let public_router = seaweed_volume::server::volume_server::build_public_router(state.clone());
        let public_addr = format!("{}:{}", config.bind_ip, public_port);
        let listener = tokio::net::TcpListener::bind(&public_addr)
            .await
            .unwrap_or_else(|e| panic!("Failed to bind public HTTP to {}: {}", public_addr, e));
        info!("Public HTTP server listening on {}", public_addr);
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, public_router)
                .with_graceful_shutdown(async move { let _ = shutdown_rx.recv().await; })
                .await
            {
                error!("Public HTTP server error: {}", e);
            }
        }))
    } else {
        None
    };

    // Wait for all servers
    let _ = http_handle.await;
    let _ = grpc_handle.await;
    if let Some(h) = public_handle {
        let _ = h.await;
    }

    info!("Volume server stopped.");
    Ok(())
}
