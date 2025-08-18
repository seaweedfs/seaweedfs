//! IPC (Inter-Process Communication) module for communicating with Go sidecar
//!
//! This module handles high-performance IPC between the Rust RDMA engine and 
//! the Go control plane sidecar using Unix domain sockets and MessagePack serialization.

use crate::{RdmaError, RdmaResult, rdma::RdmaContext, session::SessionManager};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::{UnixListener, UnixStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tracing::{info, debug, error};
use uuid::Uuid;
use std::path::Path;

/// Atomic counter for generating unique work request IDs
/// This ensures no hash collisions that could cause incorrect completion handling
static NEXT_WR_ID: AtomicU64 = AtomicU64::new(1);

/// IPC message types between Go sidecar and Rust RDMA engine
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum IpcMessage {
    /// Request to start an RDMA read operation
    StartRead(StartReadRequest),
    /// Response with RDMA session information
    StartReadResponse(StartReadResponse),
    
    /// Request to complete an RDMA operation
    CompleteRead(CompleteReadRequest),
    /// Response confirming completion
    CompleteReadResponse(CompleteReadResponse),
    
    /// Request for engine capabilities
    GetCapabilities(GetCapabilitiesRequest),
    /// Response with engine capabilities
    GetCapabilitiesResponse(GetCapabilitiesResponse),
    
    /// Health check ping
    Ping(PingRequest),
    /// Ping response
    Pong(PongResponse),
    
    /// Error response
    Error(ErrorResponse),
}

/// Request to start RDMA read operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartReadRequest {
    /// Volume ID in SeaweedFS
    pub volume_id: u32,
    /// Needle ID in SeaweedFS
    pub needle_id: u64,
    /// Needle cookie for validation
    pub cookie: u32,
    /// File offset within the needle data
    pub offset: u64,
    /// Size to read (0 = entire needle)
    pub size: u64,
    /// Remote memory address from Go sidecar
    pub remote_addr: u64,
    /// Remote key for RDMA access
    pub remote_key: u32,
    /// Session timeout in seconds
    pub timeout_secs: u64,
    /// Authentication token (optional)
    pub auth_token: Option<String>,
}

/// Response with RDMA session details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartReadResponse {
    /// Unique session identifier
    pub session_id: String,
    /// Local buffer address for RDMA
    pub local_addr: u64,
    /// Local key for RDMA operations
    pub local_key: u32,
    /// Actual size that will be transferred
    pub transfer_size: u64,
    /// Expected CRC checksum
    pub expected_crc: u32,
    /// Session expiration timestamp (Unix nanoseconds)
    pub expires_at_ns: u64,
}

/// Request to complete RDMA operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteReadRequest {
    /// Session ID to complete
    pub session_id: String,
    /// Whether the operation was successful
    pub success: bool,
    /// Actual bytes transferred
    pub bytes_transferred: u64,
    /// Client-computed CRC (for verification)
    pub client_crc: Option<u32>,
    /// Error message if failed
    pub error_message: Option<String>,
}

/// Response confirming completion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteReadResponse {
    /// Whether completion was successful
    pub success: bool,
    /// Server-computed CRC for verification
    pub server_crc: Option<u32>,
    /// Any cleanup messages
    pub message: Option<String>,
}

/// Request for engine capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetCapabilitiesRequest {
    /// Client identifier
    pub client_id: Option<String>,
}

/// Response with engine capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetCapabilitiesResponse {
    /// RDMA device name
    pub device_name: String,
    /// RDMA device vendor ID
    pub vendor_id: u32,
    /// Maximum transfer size in bytes
    pub max_transfer_size: u64,
    /// Maximum concurrent sessions
    pub max_sessions: usize,
    /// Current active sessions
    pub active_sessions: usize,
    /// Device port GID
    pub port_gid: String,
    /// Device port LID
    pub port_lid: u16,
    /// Supported authentication methods
    pub supported_auth: Vec<String>,
    /// Engine version
    pub version: String,
    /// Whether real RDMA hardware is available
    pub real_rdma: bool,
}

/// Health check ping request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingRequest {
    /// Client timestamp (Unix nanoseconds)
    pub timestamp_ns: u64,
    /// Client identifier
    pub client_id: Option<String>,
}

/// Ping response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PongResponse {
    /// Original client timestamp
    pub client_timestamp_ns: u64,
    /// Server timestamp (Unix nanoseconds)
    pub server_timestamp_ns: u64,
    /// Round-trip time in nanoseconds (server perspective)
    pub server_rtt_ns: u64,
}

/// Error response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// Error code
    pub code: String,
    /// Human-readable error message
    pub message: String,
    /// Error category
    pub category: String,
    /// Whether the error is recoverable
    pub recoverable: bool,
}

impl From<&RdmaError> for ErrorResponse {
    fn from(error: &RdmaError) -> Self {
        Self {
            code: format!("{:?}", error),
            message: error.to_string(),
            category: error.category().to_string(),
            recoverable: error.is_recoverable(),
        }
    }
}

/// IPC server handling communication with Go sidecar
pub struct IpcServer {
    socket_path: String,
    listener: Option<UnixListener>,
    rdma_context: Arc<RdmaContext>,
    session_manager: Arc<SessionManager>,
    shutdown_flag: Arc<parking_lot::RwLock<bool>>,
}

impl IpcServer {
    /// Create new IPC server
    pub async fn new(
        socket_path: &str,
        rdma_context: Arc<RdmaContext>,
        session_manager: Arc<SessionManager>,
    ) -> RdmaResult<Self> {
        // Remove existing socket if it exists
        if Path::new(socket_path).exists() {
            std::fs::remove_file(socket_path)
                .map_err(|e| RdmaError::ipc_error(format!("Failed to remove existing socket: {}", e)))?;
        }
        
        Ok(Self {
            socket_path: socket_path.to_string(),
            listener: None,
            rdma_context,
            session_manager,
            shutdown_flag: Arc::new(parking_lot::RwLock::new(false)),
        })
    }
    
    /// Start the IPC server
    pub async fn run(&mut self) -> RdmaResult<()> {
        let listener = UnixListener::bind(&self.socket_path)
            .map_err(|e| RdmaError::ipc_error(format!("Failed to bind Unix socket: {}", e)))?;
        
        info!("🎯 IPC server listening on: {}", self.socket_path);
        self.listener = Some(listener);
        
        if let Some(ref listener) = self.listener {
            loop {
                // Check shutdown flag
                if *self.shutdown_flag.read() {
                    info!("IPC server shutting down");
                    break;
                }
                
                // Accept connection with timeout
                let accept_result = tokio::time::timeout(
                    tokio::time::Duration::from_millis(100),
                    listener.accept()
                ).await;
                
                match accept_result {
                    Ok(Ok((stream, addr))) => {
                        debug!("New IPC connection from: {:?}", addr);
                        
                        // Spawn handler for this connection
                        let rdma_context = self.rdma_context.clone();
                        let session_manager = self.session_manager.clone();
                        let shutdown_flag = self.shutdown_flag.clone();
                        
                        tokio::spawn(async move {
                            if let Err(e) = Self::handle_connection(stream, rdma_context, session_manager, shutdown_flag).await {
                                error!("IPC connection error: {}", e);
                            }
                        });
                    }
                    Ok(Err(e)) => {
                        error!("Failed to accept IPC connection: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                    Err(_) => {
                        // Timeout - continue loop to check shutdown flag
                        continue;
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Handle a single IPC connection
    async fn handle_connection(
        stream: UnixStream,
        rdma_context: Arc<RdmaContext>,
        session_manager: Arc<SessionManager>,
        shutdown_flag: Arc<parking_lot::RwLock<bool>>,
    ) -> RdmaResult<()> {
        let (reader_half, writer_half) = stream.into_split();
        let mut reader = BufReader::new(reader_half);
        let mut writer = BufWriter::new(writer_half);
        
        let mut buffer = Vec::with_capacity(4096);
        
        loop {
            // Check shutdown
            if *shutdown_flag.read() {
                break;
            }
            
            // Read message length (4 bytes)
            let mut len_bytes = [0u8; 4];
            match tokio::time::timeout(
                tokio::time::Duration::from_millis(100),
                reader.read_exact(&mut len_bytes)
            ).await {
                Ok(Ok(_)) => {},
                Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!("IPC connection closed by peer");
                    break;
                }
                Ok(Err(e)) => return Err(RdmaError::ipc_error(format!("Read error: {}", e))),
                Err(_) => continue, // Timeout, check shutdown flag
            }
            
            let msg_len = u32::from_le_bytes(len_bytes) as usize;
            if msg_len > 1024 * 1024 { // 1MB max message size
                return Err(RdmaError::ipc_error("Message too large"));
            }
            
            // Read message data
            buffer.clear();
            buffer.resize(msg_len, 0);
            reader.read_exact(&mut buffer).await
                .map_err(|e| RdmaError::ipc_error(format!("Failed to read message: {}", e)))?;
            
            // Deserialize message
            let request: IpcMessage = rmp_serde::from_slice(&buffer)
                .map_err(|e| RdmaError::SerializationError { reason: e.to_string() })?;
            
            debug!("Received IPC message: {:?}", request);
            
            // Process message
            let response = Self::process_message(
                request,
                &rdma_context,
                &session_manager,
            ).await;
            
            // Serialize response
            let response_data = rmp_serde::to_vec(&response)
                .map_err(|e| RdmaError::SerializationError { reason: e.to_string() })?;
            
            // Send response
            let response_len = (response_data.len() as u32).to_le_bytes();
            writer.write_all(&response_len).await
                .map_err(|e| RdmaError::ipc_error(format!("Failed to write response length: {}", e)))?;
            writer.write_all(&response_data).await
                .map_err(|e| RdmaError::ipc_error(format!("Failed to write response: {}", e)))?;
            writer.flush().await
                .map_err(|e| RdmaError::ipc_error(format!("Failed to flush response: {}", e)))?;
            
            debug!("Sent IPC response");
        }
        
        Ok(())
    }
    
    /// Process IPC message and generate response
    async fn process_message(
        message: IpcMessage,
        rdma_context: &Arc<RdmaContext>,
        session_manager: &Arc<SessionManager>,
    ) -> IpcMessage {
        match message {
            IpcMessage::Ping(req) => {
                let server_timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
                IpcMessage::Pong(PongResponse {
                    client_timestamp_ns: req.timestamp_ns,
                    server_timestamp_ns: server_timestamp,
                    server_rtt_ns: server_timestamp.saturating_sub(req.timestamp_ns),
                })
            }
            
            IpcMessage::GetCapabilities(_req) => {
                let device_info = rdma_context.device_info();
                let active_sessions = session_manager.active_session_count().await;
                
                IpcMessage::GetCapabilitiesResponse(GetCapabilitiesResponse {
                    device_name: device_info.name.clone(),
                    vendor_id: device_info.vendor_id,
                    max_transfer_size: device_info.max_mr_size,
                    max_sessions: session_manager.max_sessions(),
                    active_sessions,
                    port_gid: device_info.port_gid.clone(),
                    port_lid: device_info.port_lid,
                    supported_auth: vec!["none".to_string()],
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    real_rdma: cfg!(feature = "real-ucx"),
                })
            }
            
            IpcMessage::StartRead(req) => {
                match Self::handle_start_read(req, rdma_context, session_manager).await {
                    Ok(response) => IpcMessage::StartReadResponse(response),
                    Err(error) => IpcMessage::Error(ErrorResponse::from(&error)),
                }
            }
            
            IpcMessage::CompleteRead(req) => {
                match Self::handle_complete_read(req, session_manager).await {
                    Ok(response) => IpcMessage::CompleteReadResponse(response),
                    Err(error) => IpcMessage::Error(ErrorResponse::from(&error)),
                }
            }
            
            _ => IpcMessage::Error(ErrorResponse {
                code: "UNSUPPORTED_MESSAGE".to_string(),
                message: "Unsupported message type".to_string(),
                category: "request".to_string(),
                recoverable: true,
            }),
        }
    }
    
    /// Handle StartRead request
    async fn handle_start_read(
        req: StartReadRequest,
        rdma_context: &Arc<RdmaContext>,
        session_manager: &Arc<SessionManager>,
    ) -> RdmaResult<StartReadResponse> {
        info!("🚀 Starting RDMA read: volume={}, needle={}, size={}", 
              req.volume_id, req.needle_id, req.size);
        
        // Create session
        let session_id = Uuid::new_v4().to_string();
        let transfer_size = if req.size == 0 { 65536 } else { req.size }; // Default 64KB
        
        // Allocate local buffer
        let buffer = vec![0u8; transfer_size as usize];
        let local_addr = buffer.as_ptr() as u64;
        
        // Register memory for RDMA
        let memory_region = rdma_context.register_memory(local_addr, transfer_size as usize).await?;
        
        // Create and store session
        session_manager.create_session(
            session_id.clone(),
            req.volume_id,
            req.needle_id,
            req.remote_addr,
            req.remote_key,
            transfer_size,
            buffer,
            memory_region.clone(),
            chrono::Duration::seconds(req.timeout_secs as i64),
        ).await?;
        
        // Perform RDMA read with unique work request ID
        // Use atomic counter to avoid hash collisions that could cause incorrect completion handling
        let wr_id = NEXT_WR_ID.fetch_add(1, Ordering::Relaxed);
        rdma_context.post_read(
            local_addr,
            req.remote_addr,
            req.remote_key,
            transfer_size as usize,
            wr_id,
        ).await?;
        
        // Poll for completion
        let completions = rdma_context.poll_completion(1).await?;
        if completions.is_empty() {
            return Err(RdmaError::operation_failed("RDMA read", -1));
        }
        
        let completion = &completions[0];
        if completion.status != crate::rdma::CompletionStatus::Success {
            return Err(RdmaError::operation_failed("RDMA read", completion.status as i32));
        }
        
        info!("✅ RDMA read completed: {} bytes", completion.byte_len);
        
        let expires_at = chrono::Utc::now() + chrono::Duration::seconds(req.timeout_secs as i64);
        
        Ok(StartReadResponse {
            session_id,
            local_addr,
            local_key: memory_region.lkey,
            transfer_size,
            expected_crc: 0x12345678, // Mock CRC
            expires_at_ns: expires_at.timestamp_nanos_opt().unwrap_or(0) as u64,
        })
    }
    
    /// Handle CompleteRead request
    async fn handle_complete_read(
        req: CompleteReadRequest,
        session_manager: &Arc<SessionManager>,
    ) -> RdmaResult<CompleteReadResponse> {
        info!("🏁 Completing RDMA read session: {}", req.session_id);
        
        // Clean up session
        session_manager.remove_session(&req.session_id).await?;
        
        Ok(CompleteReadResponse {
            success: req.success,
            server_crc: Some(0x12345678), // Mock CRC
            message: Some("Session completed successfully".to_string()),
        })
    }
    
    /// Shutdown the IPC server
    pub async fn shutdown(&mut self) -> RdmaResult<()> {
        info!("Shutting down IPC server");
        *self.shutdown_flag.write() = true;
        
        // Remove socket file
        if Path::new(&self.socket_path).exists() {
            std::fs::remove_file(&self.socket_path)
                .map_err(|e| RdmaError::ipc_error(format!("Failed to remove socket file: {}", e)))?;
        }
        
        Ok(())
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_error_response_conversion() {
        let error = RdmaError::device_not_found("mlx5_0");
        let response = ErrorResponse::from(&error);
        
        assert!(response.message.contains("mlx5_0"));
        assert_eq!(response.category, "hardware");
        assert!(!response.recoverable);
    }
    
    #[test]
    fn test_message_serialization() {
        let request = IpcMessage::Ping(PingRequest {
            timestamp_ns: 12345,
            client_id: Some("test".to_string()),
        });
        
        let serialized = rmp_serde::to_vec(&request).unwrap();
        let deserialized: IpcMessage = rmp_serde::from_slice(&serialized).unwrap();
        
        match deserialized {
            IpcMessage::Ping(ping) => {
                assert_eq!(ping.timestamp_ns, 12345);
                assert_eq!(ping.client_id, Some("test".to_string()));
            }
            _ => panic!("Wrong message type"),
        }
    }
}
