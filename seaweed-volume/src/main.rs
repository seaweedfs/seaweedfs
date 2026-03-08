use std::sync::{Arc, RwLock};

use tracing::{error, info};

use seaweed_volume::config::{self, VolumeServerConfig};
use seaweed_volume::metrics;
use seaweed_volume::pb::volume_server_pb::volume_server_server::VolumeServerServer;
use seaweed_volume::security::{Guard, SigningKey};
use seaweed_volume::server::debug::build_debug_router;
use seaweed_volume::server::grpc_server::VolumeGrpcService;
use seaweed_volume::server::volume_server::{
    build_metrics_router, RuntimeMetricsConfig, VolumeServerState,
};
use seaweed_volume::server::write_queue::WriteQueue;
use seaweed_volume::storage::store::Store;
use seaweed_volume::storage::types::DiskType;

use tokio_rustls::TlsAcceptor;

fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = config::parse_cli();
    info!(
        "SeaweedFS Volume Server (Rust) v{}",
        env!("CARGO_PKG_VERSION")
    );

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

/// Build a rustls ServerConfig from cert and key PEM files.
fn load_rustls_config(cert_path: &str, key_path: &str) -> rustls::ServerConfig {
    let cert_pem = std::fs::read(cert_path)
        .unwrap_or_else(|e| panic!("Failed to read TLS cert file '{}': {}", cert_path, e));
    let key_pem = std::fs::read(key_path)
        .unwrap_or_else(|e| panic!("Failed to read TLS key file '{}': {}", key_path, e));

    let certs = rustls_pemfile::certs(&mut &cert_pem[..])
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to parse TLS certificate PEM");
    let key = rustls_pemfile::private_key(&mut &key_pem[..])
        .expect("Failed to parse TLS private key PEM")
        .expect("No private key found in PEM file");

    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("Failed to build rustls ServerConfig")
}

async fn run(config: VolumeServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the store
    let mut store = Store::new(config.index_type);
    store.id = config.id.clone();
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
        let tags = config.folder_tags.get(i).cloned().unwrap_or_default();

        info!(
            "Adding storage location: {} (max_volumes={}, disk_type={:?})",
            dir, max_volumes, disk_type
        );
        let min_free_space = config.min_free_spaces[i].clone();
        store
            .add_location(dir, idx_dir, max_volumes, disk_type, min_free_space, tags)
            .map_err(|e| format!("Failed to add storage location {}: {}", dir, e))?;
    }

    // Build shared state
    let guard = Guard::new(
        &config.white_list,
        SigningKey(config.jwt_signing_key.clone()),
        config.jwt_signing_expires_seconds,
        SigningKey(config.jwt_read_signing_key.clone()),
        config.jwt_read_signing_expires_seconds,
    );
    let master_url = config.masters.first().cloned().unwrap_or_default();
    let self_url = format!("{}:{}", config.ip, config.port);

    let state = Arc::new(VolumeServerState {
        store: RwLock::new(store),
        guard,
        is_stopping: RwLock::new(false),
        maintenance: std::sync::atomic::AtomicBool::new(false),
        state_version: std::sync::atomic::AtomicU32::new(0),
        concurrent_upload_limit: config.concurrent_upload_limit,
        concurrent_download_limit: config.concurrent_download_limit,
        inflight_upload_data_timeout: config.inflight_upload_data_timeout,
        inflight_download_data_timeout: config.inflight_download_data_timeout,
        inflight_upload_bytes: std::sync::atomic::AtomicI64::new(0),
        inflight_download_bytes: std::sync::atomic::AtomicI64::new(0),
        upload_notify: tokio::sync::Notify::new(),
        download_notify: tokio::sync::Notify::new(),
        data_center: config.data_center.clone(),
        rack: config.rack.clone(),
        file_size_limit_bytes: config.file_size_limit_bytes,
        is_heartbeating: std::sync::atomic::AtomicBool::new(config.masters.is_empty()),
        has_master: !config.masters.is_empty(),
        pre_stop_seconds: config.pre_stop_seconds,
        volume_state_notify: tokio::sync::Notify::new(),
        write_queue: std::sync::OnceLock::new(),
        s3_tier_registry: std::sync::RwLock::new(
            seaweed_volume::remote_storage::s3_tier::S3TierRegistry::new(),
        ),
        read_mode: config.read_mode,
        master_url,
        self_url,
        http_client: reqwest::Client::new(),
        metrics_runtime: std::sync::RwLock::new(RuntimeMetricsConfig::default()),
        metrics_notify: tokio::sync::Notify::new(),
        has_slow_read: config.has_slow_read,
        read_buffer_size_bytes: (config.read_buffer_size_mb.max(1) as usize) * 1024 * 1024,
    });

    // Initialize the batched write queue if enabled
    if config.enable_write_queue {
        info!("Batched write queue enabled");
        let wq = WriteQueue::new(state.clone(), 128);
        let _ = state.write_queue.set(wq);
    }

    // Run initial disk space check
    {
        let store = state.store.read().unwrap();
        for loc in &store.locations {
            loc.check_disk_space();
        }
    }

    // Spawn background disk space monitor (checks every 60 seconds)
    {
        let monitor_state = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.tick().await; // skip the first immediate tick
            loop {
                interval.tick().await;
                let store = monitor_state.store.read().unwrap();
                for loc in &store.locations {
                    loc.check_disk_space();
                }
            }
        });
    }

    // Build HTTP routers
    let mut admin_router = seaweed_volume::server::volume_server::build_admin_router(state.clone());
    if config.pprof {
        admin_router = admin_router.merge(build_debug_router());
    }
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
        info!(
            "Starting public HTTP server on {}:{}",
            config.bind_ip, public_port
        );
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

        // Graceful drain: wait pre_stop_seconds before shutting down servers
        let pre_stop = state_shutdown.pre_stop_seconds;
        if pre_stop > 0 {
            info!("Pre-stop: waiting {} seconds before shutdown...", pre_stop);
            tokio::time::sleep(std::time::Duration::from_secs(pre_stop as u64)).await;
        }

        let _ = shutdown_tx_clone.send(());
    });

    // Build optional TLS acceptor for HTTPS
    let https_tls_acceptor =
        if !config.https_cert_file.is_empty() && !config.https_key_file.is_empty() {
            info!(
                "TLS enabled for HTTP server (cert={}, key={})",
                config.https_cert_file, config.https_key_file
            );
            let tls_config = load_rustls_config(&config.https_cert_file, &config.https_key_file);
            Some(TlsAcceptor::from(Arc::new(tls_config)))
        } else {
            None
        };

    // Spawn all servers concurrently
    let admin_listener = tokio::net::TcpListener::bind(&admin_addr)
        .await
        .unwrap_or_else(|e| panic!("Failed to bind HTTP to {}: {}", admin_addr, e));
    let scheme = if https_tls_acceptor.is_some() {
        "HTTPS"
    } else {
        "HTTP"
    };
    info!("{} server listening on {}", scheme, admin_addr);

    let http_handle = if let Some(tls_acceptor) = https_tls_acceptor.clone() {
        let mut shutdown_rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            serve_https(admin_listener, admin_router, tls_acceptor, async move {
                let _ = shutdown_rx.recv().await;
            })
            .await;
        })
    } else {
        let mut shutdown_rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            if let Err(e) = axum::serve(admin_listener, admin_router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.recv().await;
                })
                .await
            {
                error!("HTTP server error: {}", e);
            }
        })
    };

    let grpc_handle = {
        let grpc_cert_file = config.grpc_cert_file.clone();
        let grpc_key_file = config.grpc_key_file.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let addr = grpc_addr.parse().expect("Invalid gRPC address");
            let use_tls = !grpc_cert_file.is_empty() && !grpc_key_file.is_empty();
            if use_tls {
                info!("gRPC server listening on {} (TLS enabled)", addr);
                let cert = std::fs::read_to_string(&grpc_cert_file).unwrap_or_else(|e| {
                    panic!("Failed to read gRPC cert '{}': {}", grpc_cert_file, e)
                });
                let key = std::fs::read_to_string(&grpc_key_file).unwrap_or_else(|e| {
                    panic!("Failed to read gRPC key '{}': {}", grpc_key_file, e)
                });
                let identity = tonic::transport::Identity::from_pem(cert, key);
                let tls_config = tonic::transport::ServerTlsConfig::new().identity(identity);
                if let Err(e) = tonic::transport::Server::builder()
                    .tls_config(tls_config)
                    .expect("Failed to configure gRPC TLS")
                    .add_service(VolumeServerServer::new(grpc_service))
                    .serve_with_shutdown(addr, async move {
                        let _ = shutdown_rx.recv().await;
                    })
                    .await
                {
                    error!("gRPC server error: {}", e);
                }
            } else {
                info!("gRPC server listening on {}", addr);
                if let Err(e) = tonic::transport::Server::builder()
                    .add_service(VolumeServerServer::new(grpc_service))
                    .serve_with_shutdown(addr, async move {
                        let _ = shutdown_rx.recv().await;
                    })
                    .await
                {
                    error!("gRPC server error: {}", e);
                }
            }
        })
    };

    // Spawn heartbeat to master (if master addresses are configured)
    let heartbeat_handle = {
        let master_addrs = config.masters.clone();
        if !master_addrs.is_empty() {
            let hb_config = seaweed_volume::server::heartbeat::HeartbeatConfig {
                ip: config.ip.clone(),
                port: config.port,
                grpc_port: config.grpc_port,
                public_url: config.public_url.clone(),
                data_center: config.data_center.clone(),
                rack: config.rack.clone(),
                master_addresses: master_addrs.clone(),
                pulse_seconds: 5,
            };
            let hb_shutdown = shutdown_tx.subscribe();
            let hb_state = state.clone();
            info!("Will send heartbeats to master: {:?}", master_addrs);
            Some(tokio::spawn(async move {
                seaweed_volume::server::heartbeat::run_heartbeat_with_state(
                    hb_config,
                    hb_state,
                    hb_shutdown,
                )
                .await;
            }))
        } else {
            None
        }
    };

    let public_handle = if needs_public {
        let public_router =
            seaweed_volume::server::volume_server::build_public_router(state.clone());
        let public_addr = format!("{}:{}", config.bind_ip, public_port);
        let listener = tokio::net::TcpListener::bind(&public_addr)
            .await
            .unwrap_or_else(|e| panic!("Failed to bind public HTTP to {}: {}", public_addr, e));
        let pub_scheme = if https_tls_acceptor.is_some() {
            "HTTPS"
        } else {
            "HTTP"
        };
        info!("Public {} server listening on {}", pub_scheme, public_addr);
        if let Some(tls_acceptor) = https_tls_acceptor {
            let mut shutdown_rx = shutdown_tx.subscribe();
            Some(tokio::spawn(async move {
                serve_https(listener, public_router, tls_acceptor, async move {
                    let _ = shutdown_rx.recv().await;
                })
                .await;
            }))
        } else {
            let mut shutdown_rx = shutdown_tx.subscribe();
            Some(tokio::spawn(async move {
                if let Err(e) = axum::serve(listener, public_router)
                    .with_graceful_shutdown(async move {
                        let _ = shutdown_rx.recv().await;
                    })
                    .await
                {
                    error!("Public HTTP server error: {}", e);
                }
            }))
        }
    } else {
        None
    };

    let metrics_handle = if config.metrics_port > 0 {
        let metrics_router = build_metrics_router();
        let metrics_addr = format!("{}:{}", config.metrics_ip, config.metrics_port);
        info!("Metrics server listening on {}", metrics_addr);
        let listener = tokio::net::TcpListener::bind(&metrics_addr)
            .await
            .unwrap_or_else(|e| panic!("Failed to bind metrics HTTP to {}: {}", metrics_addr, e));
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, metrics_router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.recv().await;
                })
                .await
            {
                error!("Metrics HTTP server error: {}", e);
            }
        }))
    } else {
        None
    };

    let debug_handle = if config.debug {
        let debug_addr = format!("0.0.0.0:{}", config.debug_port);
        info!("Debug pprof server listening on {}", debug_addr);
        let listener = tokio::net::TcpListener::bind(&debug_addr)
            .await
            .unwrap_or_else(|e| panic!("Failed to bind debug HTTP to {}: {}", debug_addr, e));
        let debug_router = build_debug_router();
        let mut shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, debug_router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.recv().await;
                })
                .await
            {
                error!("Debug HTTP server error: {}", e);
            }
        }))
    } else {
        None
    };

    let metrics_push_handle = {
        let push_state = state.clone();
        let push_instance = format!("{}:{}", config.ip, config.port);
        let push_shutdown = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            run_metrics_push_loop(push_state, push_instance, push_shutdown).await;
        }))
    };

    // Wait for all servers
    let _ = http_handle.await;
    let _ = grpc_handle.await;
    if let Some(h) = public_handle {
        let _ = h.await;
    }
    if let Some(h) = metrics_handle {
        let _ = h.await;
    }
    if let Some(h) = debug_handle {
        let _ = h.await;
    }
    if let Some(h) = heartbeat_handle {
        let _ = h.await;
    }
    if let Some(h) = metrics_push_handle {
        let _ = h.await;
    }

    info!("Volume server stopped.");
    Ok(())
}

async fn run_metrics_push_loop(
    state: Arc<VolumeServerState>,
    instance: String,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    loop {
        let push_cfg = { state.metrics_runtime.read().unwrap().push_gateway.clone() };

        if push_cfg.address.is_empty() || push_cfg.interval_seconds == 0 {
            tokio::select! {
                _ = state.metrics_notify.notified() => continue,
                _ = shutdown_rx.recv() => return,
            }
        }

        if let Err(e) = metrics::push_metrics_once(
            &state.http_client,
            &push_cfg.address,
            "volumeServer",
            &instance,
        )
        .await
        {
            info!("could not push metrics to {}: {}", push_cfg.address, e);
        }

        let interval = std::time::Duration::from_secs(push_cfg.interval_seconds.max(1) as u64);
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = state.metrics_notify.notified() => {}
            _ = shutdown_rx.recv() => return,
        }
    }
}

/// Serve an axum Router over TLS using tokio-rustls.
/// Accepts TCP connections, performs TLS handshake, then serves HTTP over the encrypted stream.
async fn serve_https<F>(
    tcp_listener: tokio::net::TcpListener,
    app: axum::Router,
    tls_acceptor: TlsAcceptor,
    shutdown_signal: F,
) where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder as HttpBuilder;
    use hyper_util::service::TowerToHyperService;
    use tower::Service;

    let mut make_svc = app.into_make_service();

    tokio::pin!(shutdown_signal);

    loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                info!("HTTPS server shutting down");
                break;
            }
            result = tcp_listener.accept() => {
                match result {
                    Ok((tcp_stream, remote_addr)) => {
                        let tls_acceptor = tls_acceptor.clone();
                        let tower_svc = make_svc.call(remote_addr).await.expect("infallible");
                        let hyper_svc = TowerToHyperService::new(tower_svc);
                        tokio::spawn(async move {
                            match tls_acceptor.accept(tcp_stream).await {
                                Ok(tls_stream) => {
                                    let io = TokioIo::new(tls_stream);
                                    let builder = HttpBuilder::new(TokioExecutor::new());
                                    if let Err(e) = builder.serve_connection(io, hyper_svc).await {
                                        tracing::debug!("HTTPS connection error: {}", e);
                                    }
                                }
                                Err(e) => {
                                    tracing::debug!("TLS handshake failed: {}", e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept TCP connection: {}", e);
                    }
                }
            }
        }
    }
}
