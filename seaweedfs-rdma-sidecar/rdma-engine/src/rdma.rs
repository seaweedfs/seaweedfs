//! RDMA operations and context management
//! 
//! This module provides both mock and real RDMA implementations:
//! - Mock implementation for development and testing
//! - Real implementation using libibverbs for production

use crate::{RdmaResult, RdmaEngineConfig};
use tracing::{debug, warn, info};
use parking_lot::RwLock;

/// RDMA completion status
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompletionStatus {
    Success,
    LocalLengthError,
    LocalQpOperationError,
    LocalEecOperationError,
    LocalProtectionError,
    WrFlushError,
    MemoryWindowBindError,
    BadResponseError,
    LocalAccessError,
    RemoteInvalidRequestError,
    RemoteAccessError,
    RemoteOperationError,
    TransportRetryCounterExceeded,
    RnrRetryCounterExceeded,
    LocalRddViolationError,
    RemoteInvalidRdRequest,
    RemoteAbortedError,
    InvalidEecnError,
    InvalidEecStateError,
    FatalError,
    ResponseTimeoutError,
    GeneralError,
}

impl From<u32> for CompletionStatus {
    fn from(status: u32) -> Self {
        match status {
            0 => Self::Success,
            1 => Self::LocalLengthError,
            2 => Self::LocalQpOperationError,
            3 => Self::LocalEecOperationError,
            4 => Self::LocalProtectionError,
            5 => Self::WrFlushError,
            6 => Self::MemoryWindowBindError,
            7 => Self::BadResponseError,
            8 => Self::LocalAccessError,
            9 => Self::RemoteInvalidRequestError,
            10 => Self::RemoteAccessError,
            11 => Self::RemoteOperationError,
            12 => Self::TransportRetryCounterExceeded,
            13 => Self::RnrRetryCounterExceeded,
            14 => Self::LocalRddViolationError,
            15 => Self::RemoteInvalidRdRequest,
            16 => Self::RemoteAbortedError,
            17 => Self::InvalidEecnError,
            18 => Self::InvalidEecStateError,
            19 => Self::FatalError,
            20 => Self::ResponseTimeoutError,
            _ => Self::GeneralError,
        }
    }
}

/// RDMA operation types
#[derive(Debug, Clone, Copy)]
pub enum RdmaOp {
    Read,
    Write,
    Send,
    Receive,
    Atomic,
}

/// RDMA memory region information
#[derive(Debug, Clone)]
pub struct MemoryRegion {
    /// Local virtual address
    pub addr: u64,
    /// Remote key for RDMA operations
    pub rkey: u32,
    /// Local key for local operations
    pub lkey: u32,
    /// Size of the memory region
    pub size: usize,
    /// Whether the region is registered with RDMA hardware
    pub registered: bool,
}

/// RDMA work completion
#[derive(Debug)]
pub struct WorkCompletion {
    /// Work request ID
    pub wr_id: u64,
    /// Completion status
    pub status: CompletionStatus,
    /// Operation type
    pub opcode: RdmaOp,
    /// Number of bytes transferred
    pub byte_len: u32,
    /// Immediate data (if any)
    pub imm_data: Option<u32>,
}

/// RDMA context implementation (simplified enum approach)
#[derive(Debug)]
pub enum RdmaContextImpl {
    Mock(MockRdmaContext),
    // Ucx(UcxRdmaContext), // TODO: Add UCX implementation
}

/// RDMA device information
#[derive(Debug, Clone)]
pub struct RdmaDeviceInfo {
    pub name: String,
    pub vendor_id: u32,
    pub vendor_part_id: u32,
    pub hw_ver: u32,
    pub max_mr: u32,
    pub max_qp: u32,
    pub max_cq: u32,
    pub max_mr_size: u64,
    pub port_gid: String,
    pub port_lid: u16,
}

/// Main RDMA context
pub struct RdmaContext {
    inner: RdmaContextImpl,
    #[allow(dead_code)]
    config: RdmaEngineConfig,
}

impl RdmaContext {
    /// Create new RDMA context
    pub async fn new(config: &RdmaEngineConfig) -> RdmaResult<Self> {
        let inner = if cfg!(feature = "real-ucx") {
            RdmaContextImpl::Mock(MockRdmaContext::new(config).await?) // TODO: Use UCX when ready
        } else {
            RdmaContextImpl::Mock(MockRdmaContext::new(config).await?)
        };
        
        Ok(Self {
            inner,
            config: config.clone(),
        })
    }
    
    /// Register memory for RDMA operations
    pub async fn register_memory(&self, addr: u64, size: usize) -> RdmaResult<MemoryRegion> {
        match &self.inner {
            RdmaContextImpl::Mock(ctx) => ctx.register_memory(addr, size).await,
        }
    }
    
    /// Deregister memory region
    pub async fn deregister_memory(&self, region: &MemoryRegion) -> RdmaResult<()> {
        match &self.inner {
            RdmaContextImpl::Mock(ctx) => ctx.deregister_memory(region).await,
        }
    }
    
    /// Post RDMA read operation
    pub async fn post_read(&self, 
        local_addr: u64, 
        remote_addr: u64, 
        rkey: u32, 
        size: usize,
        wr_id: u64,
    ) -> RdmaResult<()> {
        match &self.inner {
            RdmaContextImpl::Mock(ctx) => ctx.post_read(local_addr, remote_addr, rkey, size, wr_id).await,
        }
    }
    
    /// Post RDMA write operation  
    pub async fn post_write(&self, 
        local_addr: u64, 
        remote_addr: u64, 
        rkey: u32, 
        size: usize,
        wr_id: u64,
    ) -> RdmaResult<()> {
        match &self.inner {
            RdmaContextImpl::Mock(ctx) => ctx.post_write(local_addr, remote_addr, rkey, size, wr_id).await,
        }
    }
    
    /// Poll for work completions
    pub async fn poll_completion(&self, max_completions: usize) -> RdmaResult<Vec<WorkCompletion>> {
        match &self.inner {
            RdmaContextImpl::Mock(ctx) => ctx.poll_completion(max_completions).await,
        }
    }
    
    /// Get device information
    pub fn device_info(&self) -> &RdmaDeviceInfo {
        match &self.inner {
            RdmaContextImpl::Mock(ctx) => ctx.device_info(),
        }
    }
}

/// Mock RDMA context for testing and development
#[derive(Debug)]
pub struct MockRdmaContext {
    device_info: RdmaDeviceInfo,
    registered_regions: RwLock<Vec<MemoryRegion>>,
    pending_operations: RwLock<Vec<WorkCompletion>>,
    #[allow(dead_code)]
    config: RdmaEngineConfig,
}

impl MockRdmaContext {
    pub async fn new(config: &RdmaEngineConfig) -> RdmaResult<Self> {
        warn!("🟡 Using MOCK RDMA implementation - for development only!");
        info!("   Device: {} (mock)", config.device_name);
        info!("   Port: {} (mock)", config.port);
        
        let device_info = RdmaDeviceInfo {
            name: config.device_name.clone(),
            vendor_id: 0x02c9, // Mellanox mock vendor ID
            vendor_part_id: 0x1017, // ConnectX-5 mock part ID
            hw_ver: 0,
            max_mr: 131072,
            max_qp: 262144,
            max_cq: 65536,
            max_mr_size: 1024 * 1024 * 1024 * 1024, // 1TB mock
            port_gid: "fe80:0000:0000:0000:0200:5eff:fe12:3456".to_string(),
            port_lid: 1,
        };
        
        Ok(Self {
            device_info,
            registered_regions: RwLock::new(Vec::new()),
            pending_operations: RwLock::new(Vec::new()),
            config: config.clone(),
        })
    }
}

impl MockRdmaContext {
    pub async fn register_memory(&self, addr: u64, size: usize) -> RdmaResult<MemoryRegion> {
        debug!("🟡 Mock: Registering memory region addr=0x{:x}, size={}", addr, size);
        
        // Simulate registration delay
        tokio::time::sleep(tokio::time::Duration::from_micros(10)).await;
        
        let region = MemoryRegion {
            addr,
            rkey: 0x12345678, // Mock remote key
            lkey: 0x87654321, // Mock local key
            size,
            registered: true,
        };
        
        self.registered_regions.write().push(region.clone());
        
        Ok(region)
    }
    
    pub async fn deregister_memory(&self, region: &MemoryRegion) -> RdmaResult<()> {
        debug!("🟡 Mock: Deregistering memory region rkey=0x{:x}", region.rkey);
        
        let mut regions = self.registered_regions.write();
        regions.retain(|r| r.rkey != region.rkey);
        
        Ok(())
    }
    
    pub async fn post_read(&self, 
        local_addr: u64, 
        remote_addr: u64, 
        rkey: u32, 
        size: usize,
        wr_id: u64,
    ) -> RdmaResult<()> {
        debug!("🟡 Mock: RDMA READ local=0x{:x}, remote=0x{:x}, rkey=0x{:x}, size={}", 
               local_addr, remote_addr, rkey, size);
        
        // Simulate RDMA read latency (much faster than real network, but realistic for mock)
        tokio::time::sleep(tokio::time::Duration::from_nanos(150)).await;
        
        // Mock data transfer - copy pattern data to local address
        let data_ptr = local_addr as *mut u8;
        unsafe {
            for i in 0..size {
                *data_ptr.add(i) = (i % 256) as u8; // Pattern: 0,1,2,...,255,0,1,2...
            }
        }
        
        // Create completion
        let completion = WorkCompletion {
            wr_id,
            status: CompletionStatus::Success,
            opcode: RdmaOp::Read,
            byte_len: size as u32,
            imm_data: None,
        };
        
        self.pending_operations.write().push(completion);
        
        Ok(())
    }
    
    pub async fn post_write(&self, 
        local_addr: u64, 
        remote_addr: u64, 
        rkey: u32, 
        size: usize,
        wr_id: u64,
    ) -> RdmaResult<()> {
        debug!("🟡 Mock: RDMA WRITE local=0x{:x}, remote=0x{:x}, rkey=0x{:x}, size={}", 
               local_addr, remote_addr, rkey, size);
        
        // Simulate RDMA write latency
        tokio::time::sleep(tokio::time::Duration::from_nanos(100)).await;
        
        // Create completion
        let completion = WorkCompletion {
            wr_id,
            status: CompletionStatus::Success,
            opcode: RdmaOp::Write,
            byte_len: size as u32,
            imm_data: None,
        };
        
        self.pending_operations.write().push(completion);
        
        Ok(())
    }
    
    pub async fn poll_completion(&self, max_completions: usize) -> RdmaResult<Vec<WorkCompletion>> {
        let mut operations = self.pending_operations.write();
        let available = operations.len().min(max_completions);
        let completions = operations.drain(..available).collect();
        
        Ok(completions)
    }
    
    pub fn device_info(&self) -> &RdmaDeviceInfo {
        &self.device_info
    }
}

/// Real RDMA context using libibverbs
#[cfg(feature = "real-ucx")]
pub struct RealRdmaContext {
    // Real implementation would contain:
    // ibv_context: *mut ibv_context,
    // ibv_pd: *mut ibv_pd,
    // ibv_cq: *mut ibv_cq,
    // ibv_qp: *mut ibv_qp,
    device_info: RdmaDeviceInfo,
    config: RdmaEngineConfig,
}

#[cfg(feature = "real-ucx")]
impl RealRdmaContext {
    pub async fn new(config: &RdmaEngineConfig) -> RdmaResult<Self> {
        info!("✅ Initializing REAL RDMA context for device: {}", config.device_name);
        
        // Real implementation would:
        // 1. Get device list with ibv_get_device_list()
        // 2. Find device by name
        // 3. Open device with ibv_open_device()
        // 4. Create protection domain with ibv_alloc_pd()
        // 5. Create completion queue with ibv_create_cq()
        // 6. Create queue pair with ibv_create_qp()
        // 7. Transition QP to RTS state
        
        todo!("Real RDMA implementation using libibverbs");
    }
}

#[cfg(feature = "real-ucx")]
#[async_trait::async_trait]
impl RdmaContextTrait for RealRdmaContext {
    async fn register_memory(&self, _addr: u64, _size: usize) -> RdmaResult<MemoryRegion> {
        // Real implementation would use ibv_reg_mr()
        todo!("Real memory registration")
    }
    
    async fn deregister_memory(&self, _region: &MemoryRegion) -> RdmaResult<()> {
        // Real implementation would use ibv_dereg_mr()
        todo!("Real memory deregistration")
    }
    
    async fn post_read(&self, 
        _local_addr: u64, 
        _remote_addr: u64, 
        _rkey: u32, 
        _size: usize,
        _wr_id: u64,
    ) -> RdmaResult<()> {
        // Real implementation would use ibv_post_send() with IBV_WR_RDMA_READ
        todo!("Real RDMA read")
    }
    
    async fn post_write(&self, 
        _local_addr: u64, 
        _remote_addr: u64, 
        _rkey: u32, 
        _size: usize,
        _wr_id: u64,
    ) -> RdmaResult<()> {
        // Real implementation would use ibv_post_send() with IBV_WR_RDMA_WRITE
        todo!("Real RDMA write")
    }
    
    async fn poll_completion(&self, _max_completions: usize) -> RdmaResult<Vec<WorkCompletion>> {
        // Real implementation would use ibv_poll_cq()
        todo!("Real completion polling")
    }
    
    fn device_info(&self) -> &RdmaDeviceInfo {
        &self.device_info
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_mock_rdma_context() {
        let config = RdmaEngineConfig::default();
        let ctx = RdmaContext::new(&config).await.unwrap();
        
        // Test device info
        let info = ctx.device_info();
        assert_eq!(info.name, "mlx5_0");
        assert!(info.max_mr > 0);
        
        // Test memory registration
        let addr = 0x7f000000u64;
        let size = 4096;
        let region = ctx.register_memory(addr, size).await.unwrap();
        assert_eq!(region.addr, addr);
        assert_eq!(region.size, size);
        assert!(region.registered);
        
        // Test RDMA read
        let local_buf = vec![0u8; 1024];
        let local_addr = local_buf.as_ptr() as u64;
        let result = ctx.post_read(local_addr, 0x8000000, region.rkey, 1024, 1).await;
        assert!(result.is_ok());
        
        // Test completion polling
        let completions = ctx.poll_completion(10).await.unwrap();
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].status, CompletionStatus::Success);
        
        // Test memory deregistration
        let result = ctx.deregister_memory(&region).await;
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_completion_status_conversion() {
        assert_eq!(CompletionStatus::from(0), CompletionStatus::Success);
        assert_eq!(CompletionStatus::from(1), CompletionStatus::LocalLengthError);
        assert_eq!(CompletionStatus::from(999), CompletionStatus::GeneralError);
    }
}
