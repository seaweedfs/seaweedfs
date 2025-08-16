//! Error types and handling for the RDMA engine

// use std::fmt;  // Unused for now
use thiserror::Error;

/// Result type alias for RDMA operations
pub type RdmaResult<T> = Result<T, RdmaError>;

/// Comprehensive error types for RDMA operations
#[derive(Error, Debug)]
pub enum RdmaError {
    /// RDMA device not found or unavailable
    #[error("RDMA device '{device}' not found or unavailable")]
    DeviceNotFound { device: String },
    
    /// Failed to initialize RDMA context
    #[error("Failed to initialize RDMA context: {reason}")]
    ContextInitFailed { reason: String },
    
    /// Failed to allocate protection domain
    #[error("Failed to allocate protection domain: {reason}")]
    PdAllocFailed { reason: String },
    
    /// Failed to create completion queue
    #[error("Failed to create completion queue: {reason}")]
    CqCreationFailed { reason: String },
    
    /// Failed to create queue pair
    #[error("Failed to create queue pair: {reason}")]
    QpCreationFailed { reason: String },
    
    /// Memory registration failed
    #[error("Memory registration failed: {reason}")]
    MemoryRegFailed { reason: String },
    
    /// RDMA operation failed
    #[error("RDMA operation failed: {operation}, status: {status}")]
    OperationFailed { operation: String, status: i32 },
    
    /// Session not found
    #[error("Session '{session_id}' not found")]
    SessionNotFound { session_id: String },
    
    /// Session expired
    #[error("Session '{session_id}' has expired")]
    SessionExpired { session_id: String },
    
    /// Too many active sessions
    #[error("Maximum number of sessions ({max_sessions}) exceeded")]
    TooManySessions { max_sessions: usize },
    
    /// IPC communication error
    #[error("IPC communication error: {reason}")]
    IpcError { reason: String },
    
    /// Serialization/deserialization error
    #[error("Serialization error: {reason}")]
    SerializationError { reason: String },
    
    /// Invalid request parameters
    #[error("Invalid request: {reason}")]
    InvalidRequest { reason: String },
    
    /// Insufficient buffer space
    #[error("Insufficient buffer space: requested {requested}, available {available}")]
    InsufficientBuffer { requested: usize, available: usize },
    
    /// Hardware not supported
    #[error("Hardware not supported: {reason}")]
    UnsupportedHardware { reason: String },
    
    /// System resource exhausted
    #[error("System resource exhausted: {resource}")]
    ResourceExhausted { resource: String },
    
    /// Permission denied
    #[error("Permission denied: {operation}")]
    PermissionDenied { operation: String },
    
    /// Network timeout
    #[error("Network timeout after {timeout_ms}ms")]
    NetworkTimeout { timeout_ms: u64 },
    
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    /// Generic error for unexpected conditions
    #[error("Internal error: {reason}")]
    Internal { reason: String },
}

impl RdmaError {
    /// Create a new DeviceNotFound error
    pub fn device_not_found(device: impl Into<String>) -> Self {
        Self::DeviceNotFound { device: device.into() }
    }
    
    /// Create a new ContextInitFailed error
    pub fn context_init_failed(reason: impl Into<String>) -> Self {
        Self::ContextInitFailed { reason: reason.into() }
    }
    
    /// Create a new MemoryRegFailed error
    pub fn memory_reg_failed(reason: impl Into<String>) -> Self {
        Self::MemoryRegFailed { reason: reason.into() }
    }
    
    /// Create a new OperationFailed error
    pub fn operation_failed(operation: impl Into<String>, status: i32) -> Self {
        Self::OperationFailed { 
            operation: operation.into(), 
            status 
        }
    }
    
    /// Create a new SessionNotFound error
    pub fn session_not_found(session_id: impl Into<String>) -> Self {
        Self::SessionNotFound { session_id: session_id.into() }
    }
    
    /// Create a new IpcError
    pub fn ipc_error(reason: impl Into<String>) -> Self {
        Self::IpcError { reason: reason.into() }
    }
    
    /// Create a new InvalidRequest error
    pub fn invalid_request(reason: impl Into<String>) -> Self {
        Self::InvalidRequest { reason: reason.into() }
    }
    
    /// Create a new Internal error
    pub fn internal(reason: impl Into<String>) -> Self {
        Self::Internal { reason: reason.into() }
    }
    
    /// Check if this error is recoverable
    pub fn is_recoverable(&self) -> bool {
        match self {
            // Network and temporary errors are recoverable
            Self::NetworkTimeout { .. } |
            Self::ResourceExhausted { .. } |
            Self::TooManySessions { .. } |
            Self::InsufficientBuffer { .. } => true,
            
            // Session errors are recoverable (can retry with new session)
            Self::SessionNotFound { .. } |
            Self::SessionExpired { .. } => true,
            
            // Hardware and system errors are generally not recoverable
            Self::DeviceNotFound { .. } |
            Self::ContextInitFailed { .. } |
            Self::UnsupportedHardware { .. } |
            Self::PermissionDenied { .. } => false,
            
            // IPC errors might be recoverable
            Self::IpcError { .. } |
            Self::SerializationError { .. } => true,
            
            // Invalid requests are not recoverable without fixing the request
            Self::InvalidRequest { .. } => false,
            
            // RDMA operation failures might be recoverable
            Self::OperationFailed { .. } => true,
            
            // Memory and resource allocation failures depend on the cause
            Self::PdAllocFailed { .. } |
            Self::CqCreationFailed { .. } |
            Self::QpCreationFailed { .. } |
            Self::MemoryRegFailed { .. } => false,
            
            // I/O errors might be recoverable
            Self::Io(_) => true,
            
            // Internal errors are generally not recoverable
            Self::Internal { .. } => false,
        }
    }
    
    /// Get error category for metrics and logging
    pub fn category(&self) -> &'static str {
        match self {
            Self::DeviceNotFound { .. } |
            Self::ContextInitFailed { .. } |
            Self::UnsupportedHardware { .. } => "hardware",
            
            Self::PdAllocFailed { .. } |
            Self::CqCreationFailed { .. } |
            Self::QpCreationFailed { .. } |
            Self::MemoryRegFailed { .. } => "resource",
            
            Self::OperationFailed { .. } => "rdma",
            
            Self::SessionNotFound { .. } |
            Self::SessionExpired { .. } |
            Self::TooManySessions { .. } => "session",
            
            Self::IpcError { .. } |
            Self::SerializationError { .. } => "ipc",
            
            Self::InvalidRequest { .. } => "request",
            
            Self::InsufficientBuffer { .. } |
            Self::ResourceExhausted { .. } => "capacity",
            
            Self::PermissionDenied { .. } => "security",
            
            Self::NetworkTimeout { .. } => "network",
            
            Self::Io(_) => "io",
            
            Self::Internal { .. } => "internal",
        }
    }
}

/// Convert from various RDMA library error codes
impl From<i32> for RdmaError {
    fn from(errno: i32) -> Self {
        match errno {
            libc::ENODEV => Self::DeviceNotFound { 
                device: "unknown".to_string() 
            },
            libc::ENOMEM => Self::ResourceExhausted { 
                resource: "memory".to_string() 
            },
            libc::EPERM | libc::EACCES => Self::PermissionDenied { 
                operation: "RDMA operation".to_string() 
            },
            libc::ETIMEDOUT => Self::NetworkTimeout { 
                timeout_ms: 5000 
            },
            libc::ENOSPC => Self::InsufficientBuffer { 
                requested: 0, 
                available: 0 
            },
            _ => Self::Internal { 
                reason: format!("System error: {}", errno) 
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_error_creation() {
        let err = RdmaError::device_not_found("mlx5_0");
        assert!(matches!(err, RdmaError::DeviceNotFound { .. }));
        assert_eq!(err.category(), "hardware");
        assert!(!err.is_recoverable());
    }
    
    #[test]
    fn test_error_recoverability() {
        assert!(RdmaError::NetworkTimeout { timeout_ms: 1000 }.is_recoverable());
        assert!(!RdmaError::DeviceNotFound { device: "test".to_string() }.is_recoverable());
        assert!(RdmaError::SessionExpired { session_id: "test".to_string() }.is_recoverable());
    }
    
    #[test]
    fn test_error_display() {
        let err = RdmaError::InvalidRequest { reason: "missing field".to_string() };
        assert!(err.to_string().contains("Invalid request"));
        assert!(err.to_string().contains("missing field"));
    }
}
