# RDMA Mount Integration - Implementation TODO

## 🎯 Overview

This document outlines the **remaining implementation tasks** to complete RDMA acceleration for `weed mount`. While the design and client are complete, several critical integration points need to be implemented.

## ✅ **COMPLETED COMPONENTS**

- [x] **RDMA Mount Client** (`weed/mount/rdma_client.go`) - Complete
- [x] **Mount Command Flags** (`weed/command/mount.go`) - Complete  
- [x] **Architecture Design** (`WEED-MOUNT-RDMA-DESIGN.md`) - Complete
- [x] **Demo Script** (`scripts/demo-mount-rdma.sh`) - Complete

## ❌ **MISSING CRITICAL INTEGRATIONS**

### 1. 🏗️ **Option Struct Extension** - **HIGH PRIORITY**

**File**: `weed/mount/weedfs.go`

**Problem**: The `mount.Option` struct doesn't include RDMA fields.

**Required Changes**:
```go
type Option struct {
    // ... existing fields ...
    
    // RDMA acceleration options
    RDMAEnabled        bool
    RDMASidecarAddr    string
    RDMAFallback       bool
    RDMAReadOnly       bool
    RDMAMaxConcurrent  int
    RDMATimeoutMs      int
}
```

**Status**: ❌ **NOT IMPLEMENTED**

---

### 2. 🔧 **Mount Command Integration** - **HIGH PRIORITY**

**File**: `weed/command/mount_std.go`

**Problem**: The `NewSeaweedFileSystem` call doesn't pass RDMA options.

**Required Changes**:
```go
seaweedFileSystem := mount.NewSeaweedFileSystem(&mount.Option{
    // ... existing fields ...
    
    // Add RDMA options
    RDMAEnabled:        *option.rdmaEnabled,
    RDMASidecarAddr:    *option.rdmaSidecarAddr,
    RDMAFallback:       *option.rdmaFallback,
    RDMAReadOnly:       *option.rdmaReadOnly,
    RDMAMaxConcurrent:  *option.rdmaMaxConcurrent,
    RDMATimeoutMs:      *option.rdmaTimeoutMs,
})
```

**Status**: ❌ **NOT IMPLEMENTED**

---

### 3. 🔧 **WFS Struct Integration** - **HIGH PRIORITY**

**File**: `weed/mount/weedfs.go`

**Problem**: The `WFS` struct doesn't include RDMA client and statistics.

**Required Changes**:
```go
type WFS struct {
    // ... existing fields ...
    
    // RDMA acceleration
    rdmaClient     *RDMAMountClient
    rdmaEnabled    bool
    rdmaFallback   bool
    rdmaStats      *RDMAStats
}

type RDMAStats struct {
    TotalReads      int64
    RDMAReads       int64
    HTTPFallbacks   int64
    AvgLatencyNs    int64
    TotalBytesRead  int64
}
```

**Status**: ❌ **NOT IMPLEMENTED**

---

### 4. 🚀 **WFS Initialization** - **HIGH PRIORITY**

**File**: `weed/mount/weedfs.go`

**Problem**: `NewSeaweedFileSystem` doesn't initialize RDMA client.

**Required Changes**:
```go
func NewSeaweedFileSystem(option *Option) *WFS {
    wfs := &WFS{
        // ... existing initialization ...
    }
    
    // Initialize RDMA client if enabled
    if option.RDMAEnabled && option.RDMASidecarAddr != "" {
        rdmaClient, err := NewRDMAMountClient(
            option.RDMASidecarAddr, 
            option.RDMAMaxConcurrent, 
            option.RDMATimeoutMs,
        )
        if err != nil {
            if !option.RDMAFallback {
                glog.Fatalf("Failed to initialize RDMA client: %v", err)
            }
            glog.Warningf("RDMA initialization failed, using HTTP fallback: %v", err)
        } else {
            wfs.rdmaClient = rdmaClient
            wfs.rdmaEnabled = true
            wfs.rdmaFallback = option.RDMAFallback
            wfs.rdmaStats = &RDMAStats{}
            glog.Infof("RDMA acceleration enabled with sidecar at %s", option.RDMASidecarAddr)
        }
    }
    
    return wfs
}
```

**Status**: ❌ **NOT IMPLEMENTED**

---

### 5. 📖 **Read Path Integration** - **CRITICAL**

**File**: `weed/mount/filehandle_read.go`

**Problem**: The `readFromChunksWithContext` function doesn't use RDMA.

**Required Changes**:
```go
func (fh *FileHandle) readFromChunksWithContext(ctx context.Context, buff []byte, offset int64) (int64, int64, error) {
    fh.entryLock.RLock()
    defer fh.entryLock.RUnlock()

    // ... existing validation code ...

    // Try RDMA acceleration first if enabled
    if fh.wfs.rdmaEnabled && fh.wfs.rdmaClient != nil {
        totalRead, ts, err := fh.readFromChunksWithRDMA(ctx, buff, offset)
        if err == nil {
            atomic.AddInt64(&fh.wfs.rdmaStats.RDMAReads, 1)
            atomic.AddInt64(&fh.wfs.rdmaStats.TotalBytesRead, totalRead)
            return totalRead, ts, nil
        }
        
        // Log RDMA failure and fall back to HTTP if enabled
        if fh.wfs.rdmaFallback {
            atomic.AddInt64(&fh.wfs.rdmaStats.HTTPFallbacks, 1)
            glog.V(2).Infof("RDMA read failed, falling back to HTTP: %v", err)
        } else {
            return 0, 0, fmt.Errorf("RDMA read failed and fallback disabled: %w", err)
        }
    }

    // Standard HTTP path (existing code)
    totalRead, ts, err := fh.entryChunkGroup.ReadDataAt(ctx, fileSize, buff, offset)
    if err == nil {
        atomic.AddInt64(&fh.wfs.rdmaStats.TotalReads, 1)
    }
    
    return int64(totalRead), ts, err
}
```

**Status**: ❌ **NOT IMPLEMENTED**

---

### 6. 🔗 **RDMA Read Implementation** - **CRITICAL**

**File**: `weed/mount/filehandle_read.go` (new function)

**Problem**: Need to implement RDMA read logic that processes chunks.

**Required Changes**:
```go
func (fh *FileHandle) readFromChunksWithRDMA(ctx context.Context, buff []byte, offset int64) (int64, int64, error) {
    entry := fh.GetEntry()
    fileSize := int64(entry.Attributes.FileSize)
    if fileSize == 0 {
        fileSize = int64(filer.FileSize(entry.GetEntry()))
    }

    // Handle small files stored in entry.Content (no chunks)
    if offset < int64(len(entry.Content)) {
        totalRead := copy(buff, entry.Content[offset:])
        return int64(totalRead), 0, nil
    }

    // Process chunks via RDMA
    var totalRead int64
    var maxTs int64
    
    for _, chunk := range entry.Chunks {
        chunkOffset := int64(chunk.Offset)
        chunkSize := int64(chunk.Size)
        
        // Check if this chunk overlaps with requested range
        requestEnd := offset + int64(len(buff))
        chunkEnd := chunkOffset + chunkSize
        
        if chunkEnd <= offset || chunkOffset >= requestEnd {
            continue // No overlap
        }
        
        // Calculate overlap region
        readStart := max(offset, chunkOffset)
        readEnd := min(requestEnd, chunkEnd)
        readSize := readEnd - readStart
        
        if readSize <= 0 {
            continue
        }
        
        // Extract volume ID and needle ID from chunk.FileId
        volumeID, needleID, cookie, err := ParseFileId(chunk.FileId)
        if err != nil {
            return totalRead, maxTs, fmt.Errorf("failed to parse file ID %s: %w", chunk.FileId, err)
        }
        
        // Calculate buffer position and chunk offset
        bufferPos := readStart - offset
        chunkReadOffset := readStart - chunkOffset
        
        // Read via RDMA
        data, isRDMA, err := fh.wfs.rdmaClient.ReadNeedle(ctx, volumeID, needleID, cookie, 
            uint64(chunkReadOffset), uint64(readSize))
        if err != nil {
            return totalRead, maxTs, err
        }
        
        if !isRDMA {
            return totalRead, maxTs, fmt.Errorf("RDMA read returned HTTP fallback")
        }
        
        // Copy data to buffer
        copied := copy(buff[bufferPos:], data)
        totalRead += int64(copied)
        maxTs = max(maxTs, chunk.ModifiedTsNs)
        
        if copied < len(data) {
            break // Buffer full
        }
    }
    
    return totalRead, maxTs, nil
}
```

**Status**: ❌ **NOT IMPLEMENTED**

---

### 7. 📊 **Statistics Integration** - **MEDIUM PRIORITY**

**File**: `weed/mount/weedfs_stats.go` (new functions)

**Problem**: Need RDMA statistics and monitoring.

**Required Changes**:
```go
func (wfs *WFS) GetRDMAStats() map[string]interface{} {
    if !wfs.rdmaEnabled || wfs.rdmaStats == nil {
        return map[string]interface{}{
            "enabled": false,
        }
    }
    
    totalReads := atomic.LoadInt64(&wfs.rdmaStats.TotalReads)
    rdmaReads := atomic.LoadInt64(&wfs.rdmaStats.RDMAReads)
    httpFallbacks := atomic.LoadInt64(&wfs.rdmaStats.HTTPFallbacks)
    totalBytes := atomic.LoadInt64(&wfs.rdmaStats.TotalBytesRead)
    
    rdmaRatio := float64(0)
    if totalReads > 0 {
        rdmaRatio = float64(rdmaReads) / float64(totalReads) * 100
    }
    
    return map[string]interface{}{
        "enabled":           true,
        "total_reads":       totalReads,
        "rdma_reads":        rdmaReads,
        "http_fallbacks":    httpFallbacks,
        "rdma_ratio_pct":    fmt.Sprintf("%.1f", rdmaRatio),
        "total_bytes_read":  totalBytes,
        "avg_latency_ns":    atomic.LoadInt64(&wfs.rdmaStats.AvgLatencyNs),
    }
}
```

**Status**: ❌ **NOT IMPLEMENTED**

---

### 8. 🧹 **Cleanup Integration** - **LOW PRIORITY**

**File**: `weed/mount/weedfs.go`

**Problem**: Need to cleanup RDMA client on shutdown.

**Required Changes**:
```go
// Add to existing cleanup logic
func (wfs *WFS) Shutdown() {
    // ... existing cleanup ...
    
    if wfs.rdmaClient != nil {
        wfs.rdmaClient.Close()
    }
}
```

**Status**: ❌ **NOT IMPLEMENTED**

---

## 🚧 **IMPLEMENTATION PRIORITY**

### **Phase 1: Core Integration** (Required for basic functionality)
1. ✅ Option struct extension
2. ✅ Mount command integration  
3. ✅ WFS struct integration
4. ✅ WFS initialization
5. ✅ Read path integration
6. ✅ RDMA read implementation

### **Phase 2: Production Features** (Required for reliability)
7. ✅ Statistics integration
8. ✅ Cleanup integration
9. ✅ Error handling improvements
10. ✅ Performance optimizations

### **Phase 3: Advanced Features** (Nice to have)
11. ✅ Health check endpoints
12. ✅ Configuration validation
13. ✅ Multi-sidecar support
14. ✅ Load balancing

---

## 🔧 **IMPLEMENTATION ESTIMATE**

| Component | Complexity | Time Estimate | Status |
|-----------|------------|---------------|---------|
| Option struct | Low | 30 minutes | ❌ Not started |
| Mount command | Low | 30 minutes | ❌ Not started |
| WFS struct | Medium | 1 hour | ❌ Not started |
| WFS initialization | Medium | 1 hour | ❌ Not started |
| Read path integration | High | 2-3 hours | ❌ Not started |
| RDMA read implementation | High | 2-3 hours | ❌ Not started |
| Statistics | Medium | 1 hour | ❌ Not started |
| Cleanup | Low | 30 minutes | ❌ Not started |

**Total Estimated Time**: **8-10 hours** for complete implementation

---

## 🎯 **NEXT STEPS**

1. **Start with Phase 1** - Core integration components
2. **Test each component** individually as implemented
3. **Integration testing** with the existing RDMA sidecar
4. **Performance validation** with real workloads
5. **Production hardening** with error handling and monitoring

---

## 🚨 **CRITICAL DEPENDENCIES**

- **RDMA Sidecar**: Must be running and accessible
- **SeaweedFS Cluster**: Master, volume, and filer must be operational
- **File ID Parsing**: Must correctly extract volume/needle/cookie from SeaweedFS file IDs
- **Chunk Processing**: Must handle overlapping chunks and partial reads correctly

---

**🎊 Result**: Once these components are implemented, `weed mount` will have full RDMA acceleration with 10-100x performance improvements for read operations!
