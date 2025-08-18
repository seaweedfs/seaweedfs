//! Memory management for RDMA operations
//!
//! This module provides efficient memory allocation, registration, and management
//! for RDMA operations with zero-copy semantics and proper cleanup.

use crate::{RdmaError, RdmaResult};
use memmap2::MmapMut;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Memory pool for efficient buffer allocation
pub struct MemoryPool {
    /// Pre-allocated memory regions by size
    pools: RwLock<HashMap<usize, Vec<PooledBuffer>>>,
    /// Total allocated memory in bytes
    total_allocated: RwLock<usize>,
    /// Maximum pool size per buffer size
    max_pool_size: usize,
    /// Maximum total memory usage
    max_total_memory: usize,
    /// Statistics
    stats: RwLock<MemoryPoolStats>,
}

/// Statistics for memory pool
#[derive(Debug, Clone, Default)]
pub struct MemoryPoolStats {
    /// Total allocations requested
    pub total_allocations: u64,
    /// Total deallocations
    pub total_deallocations: u64,
    /// Cache hits (reused buffers)
    pub cache_hits: u64,
    /// Cache misses (new allocations)
    pub cache_misses: u64,
    /// Current active allocations
    pub active_allocations: usize,
    /// Peak memory usage in bytes
    pub peak_memory_usage: usize,
}

/// A pooled memory buffer
pub struct PooledBuffer {
    /// Raw buffer data
    data: Vec<u8>,
    /// Size of the buffer
    size: usize,
    /// Whether the buffer is currently in use
    in_use: bool,
    /// Creation timestamp
    created_at: std::time::Instant,
}

impl PooledBuffer {
    /// Create new pooled buffer
    fn new(size: usize) -> Self {
        Self {
            data: vec![0u8; size],
            size,
            in_use: false,
            created_at: std::time::Instant::now(),
        }
    }
    
    /// Get buffer data as slice
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
    
    /// Get buffer data as mutable slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
    
    /// Get buffer size
    pub fn size(&self) -> usize {
        self.size
    }
    
    /// Get buffer age
    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }
    
    /// Get raw pointer to buffer data
    pub fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }
    
    /// Get mutable raw pointer to buffer data
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }
}

impl MemoryPool {
    /// Create new memory pool
    pub fn new(max_pool_size: usize, max_total_memory: usize) -> Self {
        info!("🧠 Memory pool initialized: max_pool_size={}, max_total_memory={} bytes", 
              max_pool_size, max_total_memory);
        
        Self {
            pools: RwLock::new(HashMap::new()),
            total_allocated: RwLock::new(0),
            max_pool_size,
            max_total_memory,
            stats: RwLock::new(MemoryPoolStats::default()),
        }
    }
    
    /// Allocate buffer from pool
    pub fn allocate(&self, size: usize) -> RdmaResult<Arc<RwLock<PooledBuffer>>> {
        // Round up to next power of 2 for better pooling
        let pool_size = size.next_power_of_two();
        
        {
            let mut stats = self.stats.write();
            stats.total_allocations += 1;
        }
        
        // Try to get buffer from pool first
        {
            let mut pools = self.pools.write();
            if let Some(pool) = pools.get_mut(&pool_size) {
                // Find available buffer in pool
                for buffer in pool.iter_mut() {
                    if !buffer.in_use {
                        buffer.in_use = true;
                        
                        let mut stats = self.stats.write();
                        stats.cache_hits += 1;
                        stats.active_allocations += 1;
                        
                        debug!("📦 Reused buffer from pool: size={}", pool_size);
                        return Ok(Arc::new(RwLock::new(std::mem::replace(
                            buffer, 
                            PooledBuffer::new(0) // Placeholder
                        ))));
                    }
                }
            }
        }
        
        // No available buffer in pool, create new one
        let total_allocated = *self.total_allocated.read();
        if total_allocated + pool_size > self.max_total_memory {
            return Err(RdmaError::ResourceExhausted { 
                resource: "memory".to_string() 
            });
        }
        
        let mut buffer = PooledBuffer::new(pool_size);
        buffer.in_use = true;
        
        // Update allocation tracking
        let new_total = {
            let mut total = self.total_allocated.write();
            *total += pool_size;
            *total
        };
        
        {
            let mut stats = self.stats.write();
            stats.cache_misses += 1;
            stats.active_allocations += 1;
            if new_total > stats.peak_memory_usage {
                stats.peak_memory_usage = new_total;
            }
        }
        
        debug!("🆕 Allocated new buffer: size={}, total_allocated={}", 
               pool_size, new_total);
        
        Ok(Arc::new(RwLock::new(buffer)))
    }
    
    /// Return buffer to pool
    pub fn deallocate(&self, buffer: Arc<RwLock<PooledBuffer>>) -> RdmaResult<()> {
        let buffer_size = {
            let buf = buffer.read();
            buf.size()
        };
        
        {
            let mut stats = self.stats.write();
            stats.total_deallocations += 1;
            stats.active_allocations = stats.active_allocations.saturating_sub(1);
        }
        
        // Try to return buffer to pool
        {
            let mut pools = self.pools.write();
            let pool = pools.entry(buffer_size).or_insert_with(Vec::new);
            
            if pool.len() < self.max_pool_size {
                // Reset buffer state and return to pool
                if let Ok(buf) = Arc::try_unwrap(buffer) {
                    let mut buf = buf.into_inner();
                    buf.in_use = false;
                    buf.data.fill(0); // Clear data for security
                    pool.push(buf);
                    
                    debug!("♻️ Returned buffer to pool: size={}", buffer_size);
                    return Ok(());
                }
            }
        }
        
        // Pool is full or buffer is still referenced, just track deallocation
        {
            let mut total = self.total_allocated.write();
            *total = total.saturating_sub(buffer_size);
        }
        
        debug!("🗑️ Buffer deallocated (not pooled): size={}", buffer_size);
        Ok(())
    }
    
    /// Get memory pool statistics
    pub fn stats(&self) -> MemoryPoolStats {
        self.stats.read().clone()
    }
    
    /// Get current memory usage
    pub fn current_usage(&self) -> usize {
        *self.total_allocated.read()
    }
    
    /// Clean up old unused buffers from pools
    pub fn cleanup_old_buffers(&self, max_age: std::time::Duration) {
        let mut cleaned_count = 0;
        let mut cleaned_bytes = 0;
        
        {
            let mut pools = self.pools.write();
            for (size, pool) in pools.iter_mut() {
                pool.retain(|buffer| {
                    if buffer.age() > max_age && !buffer.in_use {
                        cleaned_count += 1;
                        cleaned_bytes += size;
                        false
                    } else {
                        true
                    }
                });
            }
        }
        
        if cleaned_count > 0 {
            {
                let mut total = self.total_allocated.write();
                *total = total.saturating_sub(cleaned_bytes);
            }
            
            info!("🧹 Cleaned up {} old buffers, freed {} bytes", 
                  cleaned_count, cleaned_bytes);
        }
    }
}

/// RDMA-specific memory manager
pub struct RdmaMemoryManager {
    /// General purpose memory pool
    pool: MemoryPool,
    /// Memory-mapped regions for large allocations
    mmapped_regions: RwLock<HashMap<u64, MmapRegion>>,
    /// HugePage allocations (if available)
    hugepage_regions: RwLock<HashMap<u64, HugePageRegion>>,
    /// Configuration
    config: MemoryConfig,
}

/// Memory configuration
#[derive(Debug, Clone)]
pub struct MemoryConfig {
    /// Use hugepages for large allocations
    pub use_hugepages: bool,
    /// Hugepage size in bytes
    pub hugepage_size: usize,
    /// Memory pool settings
    pub pool_max_size: usize,
    /// Maximum total memory usage
    pub max_total_memory: usize,
    /// Buffer cleanup interval
    pub cleanup_interval_secs: u64,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            use_hugepages: true,
            hugepage_size: 2 * 1024 * 1024, // 2MB
            pool_max_size: 1000,
            max_total_memory: 8 * 1024 * 1024 * 1024, // 8GB
            cleanup_interval_secs: 300, // 5 minutes
        }
    }
}

/// Memory-mapped region
#[allow(dead_code)]
struct MmapRegion {
    mmap: MmapMut,
    size: usize,
    created_at: std::time::Instant,
}

/// HugePage memory region
#[allow(dead_code)]
struct HugePageRegion {
    addr: *mut u8,
    size: usize,
    created_at: std::time::Instant,
}

unsafe impl Send for HugePageRegion {}
unsafe impl Sync for HugePageRegion {}

impl RdmaMemoryManager {
    /// Create new RDMA memory manager
    pub fn new(config: MemoryConfig) -> Self {
        let pool = MemoryPool::new(config.pool_max_size, config.max_total_memory);
        
        Self {
            pool,
            mmapped_regions: RwLock::new(HashMap::new()),
            hugepage_regions: RwLock::new(HashMap::new()),
            config,
        }
    }
    
    /// Allocate memory optimized for RDMA operations
    pub fn allocate_rdma_buffer(&self, size: usize) -> RdmaResult<RdmaBuffer> {
        if size >= self.config.hugepage_size && self.config.use_hugepages {
            self.allocate_hugepage_buffer(size)
        } else if size >= 64 * 1024 {  // Use mmap for large buffers
            self.allocate_mmap_buffer(size)
        } else {
            self.allocate_pool_buffer(size)
        }
    }
    
    /// Allocate buffer from memory pool
    fn allocate_pool_buffer(&self, size: usize) -> RdmaResult<RdmaBuffer> {
        let buffer = self.pool.allocate(size)?;
        Ok(RdmaBuffer::Pool { buffer, size })
    }
    
    /// Allocate memory-mapped buffer
    fn allocate_mmap_buffer(&self, size: usize) -> RdmaResult<RdmaBuffer> {
        let mmap = MmapMut::map_anon(size)
            .map_err(|e| RdmaError::memory_reg_failed(format!("mmap failed: {}", e)))?;
        
        let addr = mmap.as_ptr() as u64;
        let region = MmapRegion {
            mmap,
            size,
            created_at: std::time::Instant::now(),
        };
        
        {
            let mut regions = self.mmapped_regions.write();
            regions.insert(addr, region);
        }
        
        debug!("🗺️ Allocated mmap buffer: addr=0x{:x}, size={}", addr, size);
        Ok(RdmaBuffer::Mmap { addr, size })
    }
    
    /// Allocate hugepage buffer (Linux-specific)
    fn allocate_hugepage_buffer(&self, size: usize) -> RdmaResult<RdmaBuffer> {
        #[cfg(target_os = "linux")]
        {
            use nix::sys::mman::{mmap, MapFlags, ProtFlags};
            
            // Round up to hugepage boundary
            let aligned_size = (size + self.config.hugepage_size - 1) & !(self.config.hugepage_size - 1);
            
            let addr = unsafe {
                // For anonymous mapping, we can use -1 as the file descriptor
                use std::os::fd::BorrowedFd;
                let fake_fd = BorrowedFd::borrow_raw(-1); // Anonymous mapping uses -1
                
                mmap(
                    None, // ptr::null_mut() -> None
                    std::num::NonZero::new(aligned_size).unwrap(), // aligned_size -> NonZero<usize>
                    ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                    MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS | MapFlags::MAP_HUGETLB,
                    Some(&fake_fd), // Use borrowed FD for -1 wrapped in Some
                    0,
                )
            };
            
            match addr {
                Ok(addr) => {
                    let addr_u64 = addr as u64;
                    let region = HugePageRegion {
                        addr: addr as *mut u8,
                        size: aligned_size,
                        created_at: std::time::Instant::now(),
                    };
                    
                    {
                        let mut regions = self.hugepage_regions.write();
                        regions.insert(addr_u64, region);
                    }
                    
                    info!("🔥 Allocated hugepage buffer: addr=0x{:x}, size={}", addr_u64, aligned_size);
                    Ok(RdmaBuffer::HugePage { addr: addr_u64, size: aligned_size })
                }
                Err(_) => {
                    warn!("Failed to allocate hugepage buffer, falling back to mmap");
                    self.allocate_mmap_buffer(size)
                }
            }
        }
        
        #[cfg(not(target_os = "linux"))]
        {
            warn!("HugePages not supported on this platform, using mmap");
            self.allocate_mmap_buffer(size)
        }
    }
    
    /// Deallocate RDMA buffer
    pub fn deallocate_buffer(&self, buffer: RdmaBuffer) -> RdmaResult<()> {
        match buffer {
            RdmaBuffer::Pool { buffer, .. } => {
                self.pool.deallocate(buffer)
            }
            RdmaBuffer::Mmap { addr, .. } => {
                let mut regions = self.mmapped_regions.write();
                regions.remove(&addr);
                debug!("🗑️ Deallocated mmap buffer: addr=0x{:x}", addr);
                Ok(())
            }
            RdmaBuffer::HugePage { addr, size } => {
                {
                    let mut regions = self.hugepage_regions.write();
                    regions.remove(&addr);
                }
                
                #[cfg(target_os = "linux")]
                {
                    use nix::sys::mman::munmap;
                    unsafe {
                        let _ = munmap(addr as *mut std::ffi::c_void, size);
                    }
                }
                
                debug!("🗑️ Deallocated hugepage buffer: addr=0x{:x}, size={}", addr, size);
                Ok(())
            }
        }
    }
    
    /// Get memory manager statistics
    pub fn stats(&self) -> MemoryManagerStats {
        let pool_stats = self.pool.stats();
        let mmap_count = self.mmapped_regions.read().len();
        let hugepage_count = self.hugepage_regions.read().len();
        
        MemoryManagerStats {
            pool_stats,
            mmap_regions: mmap_count,
            hugepage_regions: hugepage_count,
            total_memory_usage: self.pool.current_usage(),
        }
    }
    
    /// Start background cleanup task
    pub async fn start_cleanup_task(&self) -> tokio::task::JoinHandle<()> {
        let pool = MemoryPool::new(self.config.pool_max_size, self.config.max_total_memory);
        let cleanup_interval = std::time::Duration::from_secs(self.config.cleanup_interval_secs);
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                tokio::time::Duration::from_secs(300) // 5 minutes
            );
            
            loop {
                interval.tick().await;
                pool.cleanup_old_buffers(cleanup_interval);
            }
        })
    }
}

/// RDMA buffer types
pub enum RdmaBuffer {
    /// Buffer from memory pool
    Pool {
        buffer: Arc<RwLock<PooledBuffer>>,
        size: usize,
    },
    /// Memory-mapped buffer
    Mmap {
        addr: u64,
        size: usize,
    },
    /// HugePage buffer
    HugePage {
        addr: u64,
        size: usize,
    },
}

impl RdmaBuffer {
    /// Get buffer address
    pub fn addr(&self) -> u64 {
        match self {
            Self::Pool { buffer, .. } => {
                buffer.read().as_ptr() as u64
            }
            Self::Mmap { addr, .. } => *addr,
            Self::HugePage { addr, .. } => *addr,
        }
    }
    
    /// Get buffer size
    pub fn size(&self) -> usize {
        match self {
            Self::Pool { size, .. } => *size,
            Self::Mmap { size, .. } => *size,
            Self::HugePage { size, .. } => *size,
        }
    }
    
    /// Get buffer as Vec (copy to avoid lifetime issues)
    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            Self::Pool { buffer, .. } => {
                buffer.read().as_slice().to_vec()
            }
            Self::Mmap { addr, size } => {
                unsafe { 
                    let slice = std::slice::from_raw_parts(*addr as *const u8, *size);
                    slice.to_vec()
                }
            }
            Self::HugePage { addr, size } => {
                unsafe { 
                    let slice = std::slice::from_raw_parts(*addr as *const u8, *size);
                    slice.to_vec()
                }
            }
        }
    }
    
    /// Get buffer type name
    pub fn buffer_type(&self) -> &'static str {
        match self {
            Self::Pool { .. } => "pool",
            Self::Mmap { .. } => "mmap",
            Self::HugePage { .. } => "hugepage",
        }
    }
}

/// Memory manager statistics
#[derive(Debug, Clone)]
pub struct MemoryManagerStats {
    /// Pool statistics
    pub pool_stats: MemoryPoolStats,
    /// Number of mmap regions
    pub mmap_regions: usize,
    /// Number of hugepage regions
    pub hugepage_regions: usize,
    /// Total memory usage in bytes
    pub total_memory_usage: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_memory_pool_allocation() {
        let pool = MemoryPool::new(10, 1024 * 1024);
        
        let buffer1 = pool.allocate(4096).unwrap();
        let buffer2 = pool.allocate(4096).unwrap();
        
        assert_eq!(buffer1.read().size(), 4096);
        assert_eq!(buffer2.read().size(), 4096);
        
        let stats = pool.stats();
        assert_eq!(stats.total_allocations, 2);
        assert_eq!(stats.cache_misses, 2);
    }
    
    #[test]
    fn test_memory_pool_reuse() {
        let pool = MemoryPool::new(10, 1024 * 1024);
        
        // Allocate and deallocate
        let buffer = pool.allocate(4096).unwrap();
        let size = buffer.read().size();
        pool.deallocate(buffer).unwrap();
        
        // Allocate again - should reuse
        let buffer2 = pool.allocate(4096).unwrap();
        assert_eq!(buffer2.read().size(), size);
        
        let stats = pool.stats();
        assert_eq!(stats.cache_hits, 1);
    }
    
    #[tokio::test]
    async fn test_rdma_memory_manager() {
        let config = MemoryConfig::default();
        let manager = RdmaMemoryManager::new(config);
        
        // Test small buffer (pool)
        let small_buffer = manager.allocate_rdma_buffer(1024).unwrap();
        assert_eq!(small_buffer.size(), 1024);
        assert_eq!(small_buffer.buffer_type(), "pool");
        
        // Test large buffer (mmap)
        let large_buffer = manager.allocate_rdma_buffer(128 * 1024).unwrap();
        assert_eq!(large_buffer.size(), 128 * 1024);
        assert_eq!(large_buffer.buffer_type(), "mmap");
        
        // Clean up
        manager.deallocate_buffer(small_buffer).unwrap();
        manager.deallocate_buffer(large_buffer).unwrap();
    }
}
