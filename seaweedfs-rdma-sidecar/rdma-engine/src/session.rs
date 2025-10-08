//! Session management for RDMA operations
//!
//! This module manages the lifecycle of RDMA sessions, including creation,
//! storage, expiration, and cleanup of resources.

use crate::{RdmaError, RdmaResult, rdma::MemoryRegion};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{Duration, Instant};
use tracing::{debug, info};
// use uuid::Uuid;  // Unused for now

/// RDMA session state
#[derive(Debug, Clone)]
pub struct RdmaSession {
    /// Unique session identifier
    pub id: String,
    /// SeaweedFS volume ID
    pub volume_id: u32,
    /// SeaweedFS needle ID  
    pub needle_id: u64,
    /// Remote memory address
    pub remote_addr: u64,
    /// Remote key for RDMA access
    pub remote_key: u32,
    /// Transfer size in bytes
    pub transfer_size: u64,
    /// Local data buffer
    pub buffer: Vec<u8>,
    /// RDMA memory region
    pub memory_region: MemoryRegion,
    /// Session creation time
    pub created_at: Instant,
    /// Session expiration time
    pub expires_at: Instant,
    /// Current session state
    pub state: SessionState,
    /// Operation statistics
    pub stats: SessionStats,
}

/// Session state enum
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SessionState {
    /// Session created but not yet active
    Created,
    /// RDMA operation in progress
    Active,
    /// Operation completed successfully
    Completed,
    /// Operation failed
    Failed,
    /// Session expired
    Expired,
    /// Session being cleaned up
    CleaningUp,
}

/// Session operation statistics
#[derive(Debug, Clone, Default)]
pub struct SessionStats {
    /// Number of RDMA operations performed
    pub operations_count: u64,
    /// Total bytes transferred
    pub bytes_transferred: u64,
    /// Time spent in RDMA operations (nanoseconds)
    pub rdma_time_ns: u64,
    /// Number of completion polling attempts
    pub poll_attempts: u64,
    /// Time of last operation
    pub last_operation_at: Option<Instant>,
}

impl RdmaSession {
    /// Create a new RDMA session
    pub fn new(
        id: String,
        volume_id: u32,
        needle_id: u64,
        remote_addr: u64,
        remote_key: u32,
        transfer_size: u64,
        buffer: Vec<u8>,
        memory_region: MemoryRegion,
        timeout: Duration,
    ) -> Self {
        let now = Instant::now();
        
        Self {
            id,
            volume_id,
            needle_id,
            remote_addr,
            remote_key,
            transfer_size,
            buffer,
            memory_region,
            created_at: now,
            expires_at: now + timeout,
            state: SessionState::Created,
            stats: SessionStats::default(),
        }
    }
    
    /// Check if session has expired
    pub fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }
    
    /// Get session age in seconds
    pub fn age_secs(&self) -> f64 {
        self.created_at.elapsed().as_secs_f64()
    }
    
    /// Get time until expiration in seconds
    pub fn time_to_expiration_secs(&self) -> f64 {
        if self.is_expired() {
            0.0
        } else {
            (self.expires_at - Instant::now()).as_secs_f64()
        }
    }
    
    /// Update session state
    pub fn set_state(&mut self, state: SessionState) {
        debug!("Session {} state: {:?} -> {:?}", self.id, self.state, state);
        self.state = state;
    }
    
    /// Record RDMA operation statistics
    pub fn record_operation(&mut self, bytes_transferred: u64, duration_ns: u64) {
        self.stats.operations_count += 1;
        self.stats.bytes_transferred += bytes_transferred;
        self.stats.rdma_time_ns += duration_ns;
        self.stats.last_operation_at = Some(Instant::now());
    }
    
    /// Get average operation latency in nanoseconds
    pub fn avg_operation_latency_ns(&self) -> u64 {
        if self.stats.operations_count > 0 {
            self.stats.rdma_time_ns / self.stats.operations_count
        } else {
            0
        }
    }
    
    /// Get throughput in bytes per second
    pub fn throughput_bps(&self) -> f64 {
        let age_secs = self.age_secs();
        if age_secs > 0.0 {
            self.stats.bytes_transferred as f64 / age_secs
        } else {
            0.0
        }
    }
}

/// Session manager for handling multiple concurrent RDMA sessions
pub struct SessionManager {
    /// Active sessions
    sessions: Arc<RwLock<HashMap<String, Arc<RwLock<RdmaSession>>>>>,
    /// Maximum number of concurrent sessions
    max_sessions: usize,
    /// Default session timeout
    #[allow(dead_code)]
    default_timeout: Duration,
    /// Cleanup task handle
    cleanup_task: RwLock<Option<tokio::task::JoinHandle<()>>>,
    /// Shutdown flag
    shutdown_flag: Arc<RwLock<bool>>,
    /// Statistics
    stats: Arc<RwLock<SessionManagerStats>>,
}

/// Session manager statistics
#[derive(Debug, Clone, Default)]
pub struct SessionManagerStats {
    /// Total sessions created
    pub total_sessions_created: u64,
    /// Total sessions completed
    pub total_sessions_completed: u64,
    /// Total sessions failed
    pub total_sessions_failed: u64,
    /// Total sessions expired
    pub total_sessions_expired: u64,
    /// Total bytes transferred across all sessions
    pub total_bytes_transferred: u64,
    /// Manager start time
    pub started_at: Option<Instant>,
}

impl SessionManager {
    /// Create new session manager
    pub fn new(max_sessions: usize, default_timeout: Duration) -> Self {
        info!("🎯 Session manager initialized: max_sessions={}, timeout={:?}", 
              max_sessions, default_timeout);
              
        let mut stats = SessionManagerStats::default();
        stats.started_at = Some(Instant::now());
        
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            max_sessions,
            default_timeout,
            cleanup_task: RwLock::new(None),
            shutdown_flag: Arc::new(RwLock::new(false)),
            stats: Arc::new(RwLock::new(stats)),
        }
    }
    
    /// Create a new RDMA session
    pub async fn create_session(
        &self,
        session_id: String,
        volume_id: u32,
        needle_id: u64,
        remote_addr: u64,
        remote_key: u32,
        transfer_size: u64,
        buffer: Vec<u8>,
        memory_region: MemoryRegion,
        timeout: chrono::Duration,
    ) -> RdmaResult<Arc<RwLock<RdmaSession>>> {
        // Check session limit
        {
            let sessions = self.sessions.read();
            if sessions.len() >= self.max_sessions {
                return Err(RdmaError::TooManySessions { 
                    max_sessions: self.max_sessions 
                });
            }
            
            // Check if session already exists
            if sessions.contains_key(&session_id) {
                return Err(RdmaError::invalid_request(
                    format!("Session {} already exists", session_id)
                ));
            }
        }
        
        let timeout_duration = Duration::from_millis(timeout.num_milliseconds().max(1) as u64);
        
        let session = Arc::new(RwLock::new(RdmaSession::new(
            session_id.clone(),
            volume_id,
            needle_id,
            remote_addr,
            remote_key,
            transfer_size,
            buffer,
            memory_region,
            timeout_duration,
        )));
        
        // Store session
        {
            let mut sessions = self.sessions.write();
            sessions.insert(session_id.clone(), session.clone());
        }
        
        // Update stats
        {
            let mut stats = self.stats.write();
            stats.total_sessions_created += 1;
        }
        
        info!("📦 Created session {}: volume={}, needle={}, size={}", 
              session_id, volume_id, needle_id, transfer_size);
        
        Ok(session)
    }
    
    /// Get session by ID
    pub async fn get_session(&self, session_id: &str) -> RdmaResult<Arc<RwLock<RdmaSession>>> {
        let sessions = self.sessions.read();
        match sessions.get(session_id) {
            Some(session) => {
                if session.read().is_expired() {
                    Err(RdmaError::SessionExpired { 
                        session_id: session_id.to_string() 
                    })
                } else {
                    Ok(session.clone())
                }
            }
            None => Err(RdmaError::SessionNotFound { 
                session_id: session_id.to_string() 
            }),
        }
    }
    
    /// Remove and cleanup session
    pub async fn remove_session(&self, session_id: &str) -> RdmaResult<()> {
        let session = {
            let mut sessions = self.sessions.write();
            sessions.remove(session_id)
        };
        
        if let Some(session) = session {
            let session_data = session.read();
            info!("🗑️ Removed session {}: stats={:?}", session_id, session_data.stats);
            
            // Update manager stats
            {
                let mut stats = self.stats.write();
                match session_data.state {
                    SessionState::Completed => stats.total_sessions_completed += 1,
                    SessionState::Failed => stats.total_sessions_failed += 1,
                    SessionState::Expired => stats.total_sessions_expired += 1,
                    _ => {}
                }
                stats.total_bytes_transferred += session_data.stats.bytes_transferred;
            }
            
            Ok(())
        } else {
            Err(RdmaError::SessionNotFound { 
                session_id: session_id.to_string() 
            })
        }
    }
    
    /// Get active session count
    pub async fn active_session_count(&self) -> usize {
        self.sessions.read().len()
    }
    
    /// Get maximum sessions allowed
    pub fn max_sessions(&self) -> usize {
        self.max_sessions
    }
    
    /// List active sessions
    pub async fn list_sessions(&self) -> Vec<String> {
        self.sessions.read().keys().cloned().collect()
    }
    
    /// Get session statistics
    pub async fn get_session_stats(&self, session_id: &str) -> RdmaResult<SessionStats> {
        let session = self.get_session(session_id).await?;
        let stats = {
            let session_data = session.read();
            session_data.stats.clone()
        };
        Ok(stats)
    }
    
    /// Get manager statistics
    pub fn get_manager_stats(&self) -> SessionManagerStats {
        self.stats.read().clone()
    }
    
    /// Start background cleanup task
    pub async fn start_cleanup_task(&self) {
        info!("📋 Session cleanup task initialized");
        
        let sessions = Arc::clone(&self.sessions);
        let shutdown_flag = Arc::clone(&self.shutdown_flag);
        let stats = Arc::clone(&self.stats);
        
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30)); // Check every 30 seconds
            
            loop {
                interval.tick().await;
                
                // Check shutdown flag
                if *shutdown_flag.read() {
                    debug!("🛑 Session cleanup task shutting down");
                    break;
                }
                
                let now = Instant::now();
                let mut expired_sessions = Vec::new();
                
                // Find expired sessions
                {
                    let sessions_guard = sessions.read();
                    for (session_id, session) in sessions_guard.iter() {
                        if now > session.read().expires_at {
                            expired_sessions.push(session_id.clone());
                        }
                    }
                }
                
                // Remove expired sessions
                if !expired_sessions.is_empty() {
                    let mut sessions_guard = sessions.write();
                    let mut stats_guard = stats.write();
                    
                    for session_id in expired_sessions {
                        if let Some(session) = sessions_guard.remove(&session_id) {
                            let session_data = session.read();
                            info!("🗑️  Cleaned up expired session: {} (volume={}, needle={})", 
                                 session_id, session_data.volume_id, session_data.needle_id);
                            stats_guard.total_sessions_expired += 1;
                        }
                    }
                    
                    debug!("📊 Active sessions: {}", sessions_guard.len());
                }
            }
        });
        
        *self.cleanup_task.write() = Some(task);
    }
    
    /// Shutdown session manager
    pub async fn shutdown(&self) {
        info!("🛑 Shutting down session manager");
        *self.shutdown_flag.write() = true;
        
        // Wait for cleanup task to finish
        if let Some(task) = self.cleanup_task.write().take() {
            let _ = task.await;
        }
        
        // Clean up all remaining sessions
        let session_ids: Vec<String> = {
            self.sessions.read().keys().cloned().collect()
        };
        
        for session_id in session_ids {
            let _ = self.remove_session(&session_id).await;
        }
        
        let final_stats = self.get_manager_stats();
        info!("📈 Final session manager stats: {:?}", final_stats);
    }
    
    /// Force cleanup of all sessions (for testing)
    #[cfg(test)]
    pub async fn cleanup_all_sessions(&self) {
        let session_ids: Vec<String> = {
            self.sessions.read().keys().cloned().collect()
        };
        
        for session_id in session_ids {
            let _ = self.remove_session(&session_id).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdma::MemoryRegion;
    
    #[tokio::test]
    async fn test_session_creation() {
        let manager = SessionManager::new(10, Duration::from_secs(60));
        
        let memory_region = MemoryRegion {
            addr: 0x1000,
            rkey: 0x12345678,
            lkey: 0x87654321,
            size: 4096,
            registered: true,
        };
        
        let session = manager.create_session(
            "test-session".to_string(),
            1,
            100,
            0x2000,
            0xabcd,
            4096,
            vec![0; 4096],
            memory_region,
            chrono::Duration::seconds(60),
        ).await.unwrap();
        
        let session_data = session.read();
        assert_eq!(session_data.id, "test-session");
        assert_eq!(session_data.volume_id, 1);
        assert_eq!(session_data.needle_id, 100);
        assert_eq!(session_data.state, SessionState::Created);
        assert!(!session_data.is_expired());
    }
    
    #[tokio::test]
    async fn test_session_expiration() {
        let manager = SessionManager::new(10, Duration::from_millis(10));
        
        let memory_region = MemoryRegion {
            addr: 0x1000,
            rkey: 0x12345678,
            lkey: 0x87654321,
            size: 4096,
            registered: true,
        };
        
        let _session = manager.create_session(
            "expire-test".to_string(),
            1,
            100,
            0x2000,
            0xabcd,
            4096,
            vec![0; 4096],
            memory_region,
            chrono::Duration::milliseconds(10),
        ).await.unwrap();
        
        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(20)).await;
        
        let result = manager.get_session("expire-test").await;
        assert!(matches!(result, Err(RdmaError::SessionExpired { .. })));
    }
    
    #[tokio::test]
    async fn test_session_limit() {
        let manager = SessionManager::new(2, Duration::from_secs(60));
        
        let memory_region = MemoryRegion {
            addr: 0x1000,
            rkey: 0x12345678,
            lkey: 0x87654321,
            size: 4096,
            registered: true,
        };
        
        // Create first session
        let _session1 = manager.create_session(
            "session1".to_string(),
            1, 100, 0x2000, 0xabcd, 4096,
            vec![0; 4096],
            memory_region.clone(),
            chrono::Duration::seconds(60),
        ).await.unwrap();
        
        // Create second session
        let _session2 = manager.create_session(
            "session2".to_string(),
            1, 101, 0x3000, 0xabcd, 4096,
            vec![0; 4096],
            memory_region.clone(),
            chrono::Duration::seconds(60),
        ).await.unwrap();
        
        // Third session should fail
        let result = manager.create_session(
            "session3".to_string(),
            1, 102, 0x4000, 0xabcd, 4096,
            vec![0; 4096],
            memory_region,
            chrono::Duration::seconds(60),
        ).await;
        
        assert!(matches!(result, Err(RdmaError::TooManySessions { .. })));
    }
    
    #[tokio::test]
    async fn test_session_stats() {
        let manager = SessionManager::new(10, Duration::from_secs(60));
        
        let memory_region = MemoryRegion {
            addr: 0x1000,
            rkey: 0x12345678,
            lkey: 0x87654321,
            size: 4096,
            registered: true,
        };
        
        let session = manager.create_session(
            "stats-test".to_string(),
            1, 100, 0x2000, 0xabcd, 4096,
            vec![0; 4096],
            memory_region,
            chrono::Duration::seconds(60),
        ).await.unwrap();
        
        // Simulate some operations - now using proper interior mutability
        {
            let mut session_data = session.write();
            session_data.record_operation(1024, 1000000); // 1KB in 1ms
            session_data.record_operation(2048, 2000000); // 2KB in 2ms
        }
        
        let stats = manager.get_session_stats("stats-test").await.unwrap();
        assert_eq!(stats.operations_count, 2);
        assert_eq!(stats.bytes_transferred, 3072);
        assert_eq!(stats.rdma_time_ns, 3000000);
    }
}
