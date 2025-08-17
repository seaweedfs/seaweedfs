# SeaweedFS Mount RDMA Integration Design

This document outlines how to integrate RDMA acceleration with `weed mount` for ultra-fast file system reads.

## 🎯 Overview

The goal is to create a **read-optimized fast access mode** for `weed mount` that leverages RDMA acceleration for dramatic performance improvements while maintaining full compatibility with existing FUSE operations.

## 📊 Current Mount Architecture

### Read Path Analysis
```
FUSE Read Request
    ↓
WFS.Read() [weed/mount/weedfs_file_read.go]
    ↓
FileHandle.readFromChunksWithContext() [weed/mount/filehandle_read.go]
    ↓
ChunkGroup.ReadDataAt() [weed/filer/filechunk_group.go]
    ↓
FileChunkSection.readDataAt() [weed/filer/filechunk_section.go]
    ↓
ChunkReaderAt.ReadAtWithTime() [weed/filer/reader_at.go]
    ↓
HTTP GET to Volume Server (current bottleneck)
```

### Key Components
1. **FUSE Layer**: Handles OS file system calls
2. **FileHandle**: Manages file state and locking
3. **ChunkGroup**: Organizes file chunks by sections
4. **ChunkReaderAt**: Performs actual data retrieval from volume servers
5. **Volume Lookup**: Discovers volume server locations via filer

## 🚀 RDMA Integration Design

### Architecture Overview
```
┌─────────────────────────────────────────────────────────────┐
│                    FUSE Mount Point                         │
│                   /mnt/seaweedfs                            │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                WFS (FUSE Handler)                           │
│  • Read(), Write(), Lookup(), etc.                         │
│  • RDMA-aware file handle management                       │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│              RDMA-Enhanced FileHandle                       │
│  • Detects RDMA-capable volume servers                     │
│  • Routes reads via RDMA fast path                         │
│  • Falls back to HTTP when needed                          │
└─────────────────────┬───────────────────────────────────────┘
                      │
        ┌─────────────┴─────────────┐
        │                           │
        ▼                           ▼
┌─────────────────┐         ┌─────────────────┐
│   RDMA Client   │         │  HTTP Client    │
│  (Fast Path)    │         │  (Fallback)     │
│                 │         │                 │
│ • Direct needle │         │ • Standard HTTP │
│   access        │         │   requests      │
│ • Zero-copy     │         │ • Existing path │
│ • μs latency    │         │ • Full compat   │
└─────────┬───────┘         └─────────┬───────┘
          │                           │
          ▼                           ▼
┌─────────────────┐         ┌─────────────────┐
│ RDMA Sidecar    │         │ Volume Server   │
│ (per node)      │         │ (HTTP API)      │
└─────────────────┘         └─────────────────┘
```

## 🔧 Implementation Strategy

### Option 1: Enhanced Mount (Recommended)
Extend existing `weed mount` with RDMA capabilities:

```bash
# Standard mount (backward compatible)
weed mount -filer=localhost:8888 -dir=/mnt/seaweedfs

# RDMA-accelerated mount (new mode)
weed mount -filer=localhost:8888 -dir=/mnt/seaweedfs \
  -rdma.enabled=true \
  -rdma.sidecar=localhost:8081 \
  -rdma.fallback=true
```

### Option 2: Separate Fast Mount Command
Create a dedicated command for read-optimized access:

```bash
# New command for maximum performance
weed mount-fast -filer=localhost:8888 -dir=/mnt/seaweedfs-fast \
  -rdma.sidecar=localhost:8081 \
  -read-only=true \
  -cache.aggressive=true
```

## 📝 Code Changes Required

### 1. Mount Options Extension

**File**: `weed/command/mount.go`
```go
type MountOptions struct {
    // ... existing fields ...
    
    // RDMA acceleration options
    rdmaEnabled        *bool
    rdmaSidecarAddr    *string
    rdmaFallback       *bool
    rdmaReadOnly       *bool
    rdmaMaxConcurrent  *int
    rdmaTimeoutMs      *int
}

func init() {
    // ... existing flags ...
    
    mountOptions.rdmaEnabled = cmdMount.Flag.Bool("rdma.enabled", false, "enable RDMA acceleration for reads")
    mountOptions.rdmaSidecarAddr = cmdMount.Flag.String("rdma.sidecar", "", "RDMA sidecar address (e.g., localhost:8081)")
    mountOptions.rdmaFallback = cmdMount.Flag.Bool("rdma.fallback", true, "fallback to HTTP when RDMA fails")
    mountOptions.rdmaReadOnly = cmdMount.Flag.Bool("rdma.readOnly", false, "RDMA for reads only (writes use HTTP)")
    mountOptions.rdmaMaxConcurrent = cmdMount.Flag.Int("rdma.maxConcurrent", 64, "max concurrent RDMA operations")
    mountOptions.rdmaTimeoutMs = cmdMount.Flag.Int("rdma.timeoutMs", 5000, "RDMA operation timeout in milliseconds")
}
```

### 2. RDMA-Aware SeaweedFileSystem

**File**: `weed/mount/weedfs.go`
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

func NewSeaweedFileSystem(option *Option) *WFS {
    wfs := &WFS{
        // ... existing initialization ...
    }
    
    // Initialize RDMA client if enabled
    if option.RDMAEnabled && option.RDMASidecarAddr != "" {
        rdmaClient, err := NewRDMAMountClient(option.RDMASidecarAddr, option)
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

### 3. RDMA Mount Client

**File**: `weed/mount/rdma_client.go` (new)
```go
package mount

import (
    "context"
    "fmt"
    "net/http"
    "time"
    
    "github.com/seaweedfs/seaweedfs/weed/glog"
    "github.com/seaweedfs/seaweedfs/weed/util"
)

type RDMAMountClient struct {
    sidecarAddr    string
    httpClient     *http.Client
    maxConcurrent  int
    timeout        time.Duration
    semaphore      chan struct{}
}

type RDMAReadRequest struct {
    VolumeID    uint32 `json:"volume_id"`
    NeedleID    uint64 `json:"needle_id"`
    Cookie      uint32 `json:"cookie"`
    Offset      uint64 `json:"offset"`
    Size        uint64 `json:"size"`
}

type RDMAReadResponse struct {
    Success     bool   `json:"success"`
    Data        []byte `json:"data,omitempty"`
    IsRDMA      bool   `json:"is_rdma"`
    Source      string `json:"source"`
    Duration    string `json:"duration"`
    ErrorMsg    string `json:"error,omitempty"`
}

func NewRDMAMountClient(sidecarAddr string, option *Option) (*RDMAMountClient, error) {
    client := &RDMAMountClient{
        sidecarAddr:   sidecarAddr,
        maxConcurrent: option.RDMAMaxConcurrent,
        timeout:       time.Duration(option.RDMATimeoutMs) * time.Millisecond,
        httpClient: &http.Client{
            Timeout: time.Duration(option.RDMATimeoutMs) * time.Millisecond,
        },
        semaphore: make(chan struct{}, option.RDMAMaxConcurrent),
    }
    
    // Test connectivity
    if err := client.healthCheck(); err != nil {
        return nil, fmt.Errorf("RDMA sidecar health check failed: %w", err)
    }
    
    return client, nil
}

func (c *RDMAMountClient) healthCheck() error {
    ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
    defer cancel()
    
    req, err := http.NewRequestWithContext(ctx, "GET", 
        fmt.Sprintf("http://%s/health", c.sidecarAddr), nil)
    if err != nil {
        return err
    }
    
    resp, err := c.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        return fmt.Errorf("health check failed with status: %s", resp.Status)
    }
    
    return nil
}

func (c *RDMAMountClient) ReadNeedle(ctx context.Context, volumeID uint32, needleID uint64, cookie uint32, offset, size uint64) ([]byte, bool, error) {
    // Acquire semaphore for concurrency control
    select {
    case c.semaphore <- struct{}{}:
        defer func() { <-c.semaphore }()
    case <-ctx.Done():
        return nil, false, ctx.Err()
    }
    
    startTime := time.Now()
    
    // Prepare request
    reqURL := fmt.Sprintf("http://%s/read?volume=%d&needle=%d&cookie=%d&offset=%d&size=%d",
        c.sidecarAddr, volumeID, needleID, cookie, offset, size)
    
    req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
    if err != nil {
        return nil, false, err
    }
    
    // Execute request
    resp, err := c.httpClient.Do(req)
    if err != nil {
        return nil, false, err
    }
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        return nil, false, fmt.Errorf("RDMA read failed with status: %s", resp.Status)
    }
    
    // Read response data
    data := make([]byte, size)
    n, err := resp.Body.Read(data)
    if err != nil && err.Error() != "EOF" {
        return nil, false, err
    }
    
    duration := time.Since(startTime)
    glog.V(4).Infof("RDMA read completed: volume=%d, needle=%d, size=%d, duration=%v", 
        volumeID, needleID, size, duration)
    
    return data[:n], true, nil
}
```

### 4. Enhanced FileHandle with RDMA Support

**File**: `weed/mount/filehandle_read.go` (modified)
```go
func (fh *FileHandle) readFromChunksWithContext(ctx context.Context, buff []byte, offset int64) (int64, int64, error) {
    fh.entryLock.RLock()
    defer fh.entryLock.RUnlock()

    fileFullPath := fh.FullPath()
    entry := fh.GetEntry()

    // ... existing validation code ...

    // Try RDMA acceleration first if enabled
    if fh.wfs.rdmaEnabled && fh.wfs.rdmaClient != nil {
        totalRead, ts, err := fh.readFromChunksWithRDMA(ctx, buff, offset)
        if err == nil {
            atomic.AddInt64(&fh.wfs.rdmaStats.RDMAReads, 1)
            atomic.AddInt64(&fh.wfs.rdmaStats.TotalBytesRead, totalRead)
            glog.V(4).Infof("RDMA read success %s [%d,%d] %d", fileFullPath, offset, offset+totalRead, totalRead)
            return totalRead, ts, nil
        }
        
        // Log RDMA failure and fall back to HTTP if enabled
        if fh.wfs.rdmaFallback {
            atomic.AddInt64(&fh.wfs.rdmaStats.HTTPFallbacks, 1)
            glog.V(2).Infof("RDMA read failed for %s, falling back to HTTP: %v", fileFullPath, err)
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
        volumeID, needleID, cookie, err := parseFileId(chunk.FileId)
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

// Helper function to parse SeaweedFS file ID
func parseFileId(fileId string) (volumeID uint32, needleID uint64, cookie uint32, err error) {
    // Parse file ID format: "volumeId,needleIdCookie"
    // Example: "3,01637037d6"
    parts := strings.Split(fileId, ",")
    if len(parts) != 2 {
        return 0, 0, 0, fmt.Errorf("invalid file ID format: %s", fileId)
    }
    
    // Parse volume ID
    vol, err := strconv.ParseUint(parts[0], 10, 32)
    if err != nil {
        return 0, 0, 0, fmt.Errorf("invalid volume ID: %s", parts[0])
    }
    volumeID = uint32(vol)
    
    // Parse needle ID and cookie (combined hex string)
    if len(parts[1]) < 8 {
        return 0, 0, 0, fmt.Errorf("invalid needle ID format: %s", parts[1])
    }
    
    needleHex := parts[1][:len(parts[1])-8]  // All but last 8 hex chars
    cookieHex := parts[1][len(parts[1])-8:]  // Last 8 hex chars
    
    needle, err := strconv.ParseUint(needleHex, 16, 64)
    if err != nil {
        return 0, 0, 0, fmt.Errorf("invalid needle ID: %s", needleHex)
    }
    needleID = needle
    
    cook, err := strconv.ParseUint(cookieHex, 16, 32)
    if err != nil {
        return 0, 0, 0, fmt.Errorf("invalid cookie: %s", cookieHex)
    }
    cookie = uint32(cook)
    
    return volumeID, needleID, cookie, nil
}
```

### 5. RDMA Statistics and Monitoring

**File**: `weed/mount/weedfs_stats.go` (new)
```go
package mount

import (
    "encoding/json"
    "fmt"
    "sync/atomic"
    "time"
)

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

// Add HTTP endpoint for stats (optional)
func (wfs *WFS) ServeRDMAStats(w http.ResponseWriter, r *http.Request) {
    stats := wfs.GetRDMAStats()
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(stats)
}
```

## 🚀 Usage Examples

### Basic RDMA Mount
```bash
# Start RDMA sidecar
./bin/sidecar --port 8081 --enable-rdma --debug

# Mount with RDMA acceleration
weed mount \
  -filer=localhost:8888 \
  -dir=/mnt/seaweedfs \
  -rdma.enabled=true \
  -rdma.sidecar=localhost:8081 \
  -rdma.fallback=true
```

### Read-Only High-Performance Mount
```bash
# Read-only mount for maximum performance
weed mount \
  -filer=localhost:8888 \
  -dir=/mnt/seaweedfs-fast \
  -rdma.enabled=true \
  -rdma.sidecar=localhost:8081 \
  -rdma.readOnly=true \
  -rdma.maxConcurrent=128 \
  -cacheCapacityMB=1024 \
  -readOnly=true
```

### Docker Compose Integration
```yaml
services:
  seaweedfs-mount:
    image: chrislusf/seaweedfs:latest
    privileged: true
    volumes:
      - /mnt/seaweedfs:/mnt/seaweedfs:shared
    command: >
      mount
      -filer=seaweedfs-filer:8888
      -dir=/mnt/seaweedfs
      -rdma.enabled=true
      -rdma.sidecar=rdma-sidecar:8081
      -rdma.fallback=true
    depends_on:
      - seaweedfs-filer
      - rdma-sidecar
```

## 📊 Expected Performance Improvements

| Metric | HTTP (Current) | RDMA (Target) | Improvement |
|--------|----------------|---------------|-------------|
| **Read Latency** | 1-10ms | 10-100μs | **10-100x faster** |
| **Throughput** | 100MB/s | 1-10GB/s | **10-100x higher** |
| **CPU Usage** | High | Low | **50-80% reduction** |
| **Memory Copy** | Multiple | Zero-copy | **Eliminated** |
| **Concurrent Ops** | Limited | Massive | **10x more** |

## 🔍 Monitoring and Debugging

### RDMA Statistics
```bash
# Check RDMA stats via mount process
curl http://localhost:8081/stats

# Example output:
{
  "enabled": true,
  "total_reads": 10000,
  "rdma_reads": 9500,
  "http_fallbacks": 500,
  "rdma_ratio_pct": "95.0",
  "total_bytes_read": 1073741824,
  "avg_latency_ns": 50000
}
```

### Debug Logging
```bash
# Enable detailed RDMA logging
weed mount -filer=localhost:8888 -dir=/mnt/seaweedfs \
  -rdma.enabled=true -rdma.sidecar=localhost:8081 \
  -v=4  # Verbose logging
```

## 🛡️ Fallback and Error Handling

### Automatic Fallback Scenarios
1. **RDMA sidecar unavailable**: Falls back to HTTP immediately
2. **RDMA operation timeout**: Retries with HTTP
3. **RDMA device failure**: Switches to HTTP for subsequent reads
4. **Network partition**: Graceful degradation to HTTP

### Configuration Options
```bash
# Strict RDMA mode (no fallback)
-rdma.fallback=false

# Aggressive fallback (quick timeout)
-rdma.timeoutMs=1000

# Conservative fallback (longer timeout)
-rdma.timeoutMs=10000
```

## 🎯 Implementation Phases

### Phase 1: Basic Integration ✅
- [x] Design architecture
- [ ] Implement RDMAMountClient
- [ ] Add mount options
- [ ] Basic read path integration

### Phase 2: Performance Optimization
- [ ] Concurrent RDMA operations
- [ ] Connection pooling
- [ ] Memory optimization
- [ ] Caching integration

### Phase 3: Production Features
- [ ] Comprehensive error handling
- [ ] Monitoring and metrics
- [ ] Configuration validation
- [ ] Performance tuning

### Phase 4: Advanced Features
- [ ] Write acceleration (if needed)
- [ ] Multi-sidecar support
- [ ] Load balancing
- [ ] Automatic device discovery

## 🎊 Benefits Summary

### For Users
- **10-100x faster file reads** from mounted SeaweedFS
- **Zero application changes** - standard POSIX file operations
- **Automatic fallback** ensures reliability
- **Easy configuration** with simple command-line flags

### For Administrators
- **Dramatic performance gains** for read-heavy workloads
- **Reduced server load** due to RDMA efficiency
- **Better resource utilization** with zero-copy operations
- **Comprehensive monitoring** and statistics

### For Developers
- **Clean integration** with existing mount code
- **Modular design** allows easy testing and debugging
- **Backward compatibility** with all existing features
- **Extensible architecture** for future enhancements

---

**🚀 Result: SeaweedFS mount with microsecond read latencies and multi-gigabit throughput!**
