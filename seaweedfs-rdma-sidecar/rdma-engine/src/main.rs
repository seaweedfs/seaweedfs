//! RDMA Engine Server
//!
//! High-performance RDMA engine server that communicates with the Go sidecar
//! via IPC and handles RDMA operations with zero-copy semantics.
//!
//! Usage:
//! ```bash
//! rdma-engine-server --device mlx5_0 --port 18515 --ipc-socket /tmp/rdma-engine.sock
//! ```

use clap::Parser;
use rdma_engine::{RdmaEngine, RdmaEngineConfig};
use std::path::PathBuf;
use tracing::{info, error};
use tracing_subscriber::{EnvFilter, fmt::layer, prelude::*};

#[derive(Parser)]
#[command(
    name = "rdma-engine-server",
    about = "High-performance RDMA engine for SeaweedFS",
    version = env!("CARGO_PKG_VERSION")
)]
struct Args {
    /// UCX device name preference (e.g., mlx5_0, or 'auto' for UCX auto-selection)
    #[arg(short, long, default_value = "auto")]
    device: String,
    
    /// RDMA port number
    #[arg(short, long, default_value_t = 18515)]
    port: u16,
    
    /// Maximum number of concurrent sessions
    #[arg(long, default_value_t = 1000)]
    max_sessions: usize,
    
    /// Session timeout in seconds
    #[arg(long, default_value_t = 300)]
    session_timeout: u64,
    
    /// Memory buffer size in bytes
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    buffer_size: usize,
    
    /// IPC socket path
    #[arg(long, default_value = "/tmp/rdma-engine.sock")]
    ipc_socket: PathBuf,
    
    /// Enable debug logging
    #[arg(long)]
    debug: bool,
    
    /// Configuration file path
    #[arg(short, long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    
    // Initialize tracing
    let filter = if args.debug {
        EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new("debug"))
            .unwrap()
    } else {
        EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new("info"))
            .unwrap()
    };
    
    tracing_subscriber::registry()
        .with(layer().with_target(false))
        .with(filter)
        .init();
    
    info!("🚀 Starting SeaweedFS UCX RDMA Engine Server");
    info!("   Version: {}", env!("CARGO_PKG_VERSION"));
    info!("   UCX Device Preference: {}", args.device);
    info!("   Port: {}", args.port);
    info!("   Max Sessions: {}", args.max_sessions);
    info!("   Buffer Size: {} bytes", args.buffer_size);
    info!("   IPC Socket: {}", args.ipc_socket.display());
    info!("   Debug Mode: {}", args.debug);
    
    // Load configuration
    let config = RdmaEngineConfig {
        device_name: args.device,
        port: args.port,
        max_sessions: args.max_sessions,
        session_timeout_secs: args.session_timeout,
        buffer_size: args.buffer_size,
        ipc_socket_path: args.ipc_socket.to_string_lossy().to_string(),
        debug: args.debug,
    };
    
    // Override with config file if provided
    if let Some(config_path) = args.config {
        info!("Loading configuration from: {}", config_path.display());
        // TODO: Implement configuration file loading
    }
    
    // Create and run RDMA engine
    let mut engine = match RdmaEngine::new(config).await {
        Ok(engine) => {
            info!("✅ RDMA engine initialized successfully");
            engine
        }
        Err(e) => {
            error!("❌ Failed to initialize RDMA engine: {}", e);
            return Err(e);
        }
    };
    
    // Set up signal handlers for graceful shutdown
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    
    // Run engine in background
    let engine_handle = tokio::spawn(async move {
        if let Err(e) = engine.run().await {
            error!("RDMA engine error: {}", e);
            return Err(e);
        }
        Ok(())
    });
    
    info!("🎯 RDMA engine is running and ready to accept connections");
    info!("   Send SIGTERM or SIGINT to shutdown gracefully");
    
    // Wait for shutdown signal
    tokio::select! {
        _ = sigterm.recv() => {
            info!("📡 Received SIGTERM, shutting down gracefully");
        }
        _ = sigint.recv() => {
            info!("📡 Received SIGINT (Ctrl+C), shutting down gracefully");
        }
        result = engine_handle => {
            match result {
                Ok(Ok(())) => info!("🏁 RDMA engine completed successfully"),
                Ok(Err(e)) => {
                    error!("❌ RDMA engine failed: {}", e);
                    return Err(e);
                }
                Err(e) => {
                    error!("❌ RDMA engine task panicked: {}", e);
                    return Err(anyhow::anyhow!("Engine task panicked: {}", e));
                }
            }
        }
    }
    
    info!("🛑 RDMA engine server shut down complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_args_parsing() {
        let args = Args::try_parse_from(&[
            "rdma-engine-server",
            "--device", "mlx5_0",
            "--port", "18515",
            "--debug"
        ]).unwrap();
        
        assert_eq!(args.device, "mlx5_0");
        assert_eq!(args.port, 18515);
        assert!(args.debug);
    }
}
