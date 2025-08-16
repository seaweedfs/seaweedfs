//! High-Performance RDMA Engine for SeaweedFS
//! 
//! This crate provides a high-performance RDMA (Remote Direct Memory Access) engine
//! designed to accelerate data transfer operations in SeaweedFS. It communicates with
//! the Go-based sidecar via IPC and handles the performance-critical RDMA operations.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────┐    IPC     ┌─────────────────────┐
//! │   Go Control Plane  │◄─────────►│  Rust Data Plane    │
//! │                     │  ~300ns    │                     │
//! │ • gRPC Server       │            │ • RDMA Operations   │
//! │ • Session Mgmt      │            │ • Memory Mgmt       │
//! │ • HTTP Fallback     │            │ • Hardware Access   │
//! │ • Error Handling    │            │ • Zero-Copy I/O     │
//! └─────────────────────┘            └─────────────────────┘
//! ```
//!
//! # Features
//!
//! - `mock-rdma` (default): Mock RDMA operations for testing and development
//! - `real-rdma`: Real RDMA hardware integration using rdma-core bindings

use std::sync::Arc;
use anyhow::Result;

pub mod ucx;
pub mod rdma;
pub mod ipc;
pub mod session;
pub mod memory;
pub mod error;

pub use error::{RdmaError, RdmaResult};

/// Configuration for the RDMA engine
#[derive(Debug, Clone)]
pub struct RdmaEngineConfig {
    /// RDMA device name (e.g., "mlx5_0")
    pub device_name: String,
    /// RDMA port number  
    pub port: u16,
    /// Maximum number of concurrent sessions
    pub max_sessions: usize,
    /// Session timeout in seconds
    pub session_timeout_secs: u64,
    /// Memory buffer size in bytes
    pub buffer_size: usize,
    /// IPC socket path
    pub ipc_socket_path: String,
    /// Enable debug logging
    pub debug: bool,
}

impl Default for RdmaEngineConfig {
    fn default() -> Self {
        Self {
            device_name: "mlx5_0".to_string(),
            port: 18515,
            max_sessions: 1000,
            session_timeout_secs: 300, // 5 minutes
            buffer_size: 1024 * 1024 * 1024, // 1GB
            ipc_socket_path: "/tmp/rdma-engine.sock".to_string(),
            debug: false,
        }
    }
}

/// Main RDMA engine instance
pub struct RdmaEngine {
    config: RdmaEngineConfig,
    rdma_context: Arc<rdma::RdmaContext>,
    session_manager: Arc<session::SessionManager>,
    ipc_server: Option<ipc::IpcServer>,
}

impl RdmaEngine {
    /// Create a new RDMA engine with the given configuration
    pub async fn new(config: RdmaEngineConfig) -> Result<Self> {
        tracing::info!("Initializing RDMA engine with config: {:?}", config);
        
        // Initialize RDMA context
        let rdma_context = Arc::new(rdma::RdmaContext::new(&config).await?);
        
        // Initialize session manager
        let session_manager = Arc::new(session::SessionManager::new(
            config.max_sessions,
            std::time::Duration::from_secs(config.session_timeout_secs),
        ));
        
        Ok(Self {
            config,
            rdma_context,
            session_manager,
            ipc_server: None,
        })
    }
    
    /// Start the RDMA engine server
    pub async fn run(&mut self) -> Result<()> {
        tracing::info!("Starting RDMA engine server on {}", self.config.ipc_socket_path);
        
        // Start IPC server
        let ipc_server = ipc::IpcServer::new(
            &self.config.ipc_socket_path,
            self.rdma_context.clone(),
            self.session_manager.clone(),
        ).await?;
        
        self.ipc_server = Some(ipc_server);
        
        // Start session cleanup task
        let session_manager = self.session_manager.clone();
        tokio::spawn(async move {
            session_manager.start_cleanup_task().await;
        });
        
        // Run IPC server
        if let Some(ref mut server) = self.ipc_server {
            server.run().await?;
        }
        
        Ok(())
    }
    
    /// Shutdown the RDMA engine
    pub async fn shutdown(&mut self) -> Result<()> {
        tracing::info!("Shutting down RDMA engine");
        
        if let Some(ref mut server) = self.ipc_server {
            server.shutdown().await?;
        }
        
        self.session_manager.shutdown().await;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_rdma_engine_creation() {
        let config = RdmaEngineConfig::default();
        let result = RdmaEngine::new(config).await;
        
        // Should succeed with mock RDMA
        assert!(result.is_ok());
    }
}
