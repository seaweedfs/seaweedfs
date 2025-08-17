# SeaweedFS RDMA Mount Integration Testing

This document provides comprehensive instructions for testing SeaweedFS mount with RDMA acceleration using Docker Compose.

## 🎯 Overview

The RDMA mount testing environment provides:
- **Complete SeaweedFS cluster** (master, volume, filer)
- **RDMA acceleration stack** (Rust engine + Go sidecar)
- **FUSE mount with RDMA support**
- **Automated integration tests**
- **Performance benchmarking**
- **Comprehensive monitoring**

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Compose Environment               │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Master    │  │   Volume    │  │    Filer    │         │
│  │   :9333     │  │   :8080     │  │   :8888     │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐                          │
│  │ RDMA Engine │  │ RDMA Sidecar│                          │
│  │   (Rust)    │  │   :8081     │                          │
│  └─────────────┘  └─────────────┘                          │
│                                                             │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              SeaweedFS Mount                            │ │
│  │           /mnt/seaweedfs (FUSE)                         │ │
│  │         with RDMA Acceleration                          │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐                          │
│  │Integration  │  │Performance  │                          │
│  │   Tests     │  │   Tests     │                          │
│  └─────────────┘  └─────────────┘                          │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- At least 4GB RAM available for containers
- Linux or macOS (Windows with WSL2)

### 1. Start the Environment
```bash
./scripts/run-mount-rdma-tests.sh start
```

### 2. Check Status
```bash
./scripts/run-mount-rdma-tests.sh status
```

### 3. Run Integration Tests
```bash
./scripts/run-mount-rdma-tests.sh test
```

### 4. Run Performance Tests
```bash
./scripts/run-mount-rdma-tests.sh perf
```

### 5. Interactive Shell
```bash
./scripts/run-mount-rdma-tests.sh shell
```

### 6. Stop Environment
```bash
./scripts/run-mount-rdma-tests.sh stop
```

## 📋 Available Commands

| Command | Description |
|---------|-------------|
| `start` | Start all services |
| `stop` | Stop all services |
| `restart` | Restart all services |
| `status` | Show service status and health |
| `logs [service]` | Show logs for all or specific service |
| `test` | Run integration tests |
| `perf` | Run performance tests |
| `shell` | Open shell in mount container |
| `cleanup` | Full cleanup including volumes |
| `build` | Build Docker images |

## 🧪 Integration Tests

The integration test suite includes:

### Test Categories
1. **Mount Accessibility** - Verify mount point is accessible
2. **Basic File Operations** - Create, read, write, delete files
3. **Large File Operations** - Handle files > 10MB
4. **Directory Operations** - Create, list, delete directories
5. **Multiple Files** - Concurrent file operations
6. **RDMA Statistics** - Verify RDMA acceleration is working
7. **Performance Baseline** - Basic performance metrics

### Running Tests
```bash
# Run all integration tests
./scripts/run-mount-rdma-tests.sh test

# Check test results
ls -la test-results/
cat test-results/integration-test.log
```

### Test Results
- **Individual test results**: `test-results/*.result`
- **Overall result**: `test-results/overall.result`
- **Detailed log**: `test-results/integration-test.log`
- **Performance metrics**: `test-results/performance.txt`

## 🏁 Performance Tests

The performance test suite includes:

### Test Categories
1. **DD Tests** - Sequential read/write with various block sizes
2. **FIO Tests** - Advanced I/O patterns (sequential, random)
3. **Concurrent Access** - Multiple processes accessing files
4. **RDMA Statistics** - Before/after RDMA usage metrics

### Running Performance Tests
```bash
# Run all performance tests
./scripts/run-mount-rdma-tests.sh perf

# Check performance results
ls -la performance-results/
open performance-results/performance_report.html
```

### Performance Results
- **DD test results**: `performance-results/dd_*.json`
- **FIO test results**: `performance-results/fio_*.json`
- **Concurrent test**: `performance-results/concurrent_test.json`
- **HTML report**: `performance-results/performance_report.html`
- **Detailed log**: `performance-results/performance-test.log`

## 🔍 Monitoring and Debugging

### Service Health Checks
```bash
# Check all service status
./scripts/run-mount-rdma-tests.sh status

# Individual service health
curl http://localhost:9333/cluster/status  # Master
curl http://localhost:8080/status          # Volume
curl http://localhost:8888/                # Filer
curl http://localhost:8081/health          # RDMA Sidecar
```

### RDMA Statistics
```bash
# Get RDMA statistics
curl http://localhost:8081/stats | jq

# Example response:
{
  "rdma_enabled": true,
  "total_reads": 150,
  "rdma_reads": 142,
  "http_fallbacks": 8,
  "rdma_ratio_pct": "94.7",
  "total_bytes_read": 52428800,
  "avg_latency_ns": 45000
}
```

### Viewing Logs
```bash
# All service logs
./scripts/run-mount-rdma-tests.sh logs

# Specific service logs
./scripts/run-mount-rdma-tests.sh logs seaweedfs-mount
./scripts/run-mount-rdma-tests.sh logs rdma-sidecar
./scripts/run-mount-rdma-tests.sh logs seaweedfs-filer
```

### Interactive Debugging
```bash
# Open shell in mount container
./scripts/run-mount-rdma-tests.sh shell

# Inside the container:
ls -la /mnt/seaweedfs/
echo "test" > /mnt/seaweedfs/debug.txt
cat /mnt/seaweedfs/debug.txt
mountpoint /mnt/seaweedfs
```

## 📊 Expected Performance

### Mock RDMA Performance
With the mock RDMA implementation:
- **Latency**: ~4ms per operation
- **Throughput**: ~250 operations/second
- **RDMA Success Rate**: >95%

### Real RDMA Performance (with hardware)
With actual RDMA hardware:
- **Latency**: 10-100μs (40-400x improvement)
- **Throughput**: 1-10GB/s (40-400x improvement)
- **CPU Usage**: 50-80% reduction

## 🐛 Troubleshooting

### Common Issues

#### 1. Mount Not Working
```bash
# Check if FUSE is available
docker exec seaweedfs-rdma-mount-seaweedfs-mount-1 ls -la /dev/fuse

# Check mount process
docker exec seaweedfs-rdma-mount-seaweedfs-mount-1 ps aux | grep weed

# Check mount point
docker exec seaweedfs-rdma-mount-seaweedfs-mount-1 mountpoint /mnt/seaweedfs
```

#### 2. RDMA Not Working
```bash
# Check RDMA sidecar health
curl http://localhost:8081/health

# Check RDMA engine connection
curl http://localhost:8081/stats | jq '.rdma_enabled'

# Check socket connection
docker exec seaweedfs-rdma-mount-rdma-sidecar-1 ls -la /tmp/rdma/
```

#### 3. Services Not Starting
```bash
# Check service dependencies
./scripts/run-mount-rdma-tests.sh status

# Check individual service logs
./scripts/run-mount-rdma-tests.sh logs seaweedfs-master
./scripts/run-mount-rdma-tests.sh logs seaweedfs-volume
./scripts/run-mount-rdma-tests.sh logs seaweedfs-filer
```

#### 4. Permission Issues
```bash
# Check container privileges
docker inspect seaweedfs-rdma-mount-seaweedfs-mount-1 | jq '.[0].HostConfig.Privileged'

# Check FUSE permissions
docker exec seaweedfs-rdma-mount-seaweedfs-mount-1 cat /etc/fuse.conf
```

### Debug Mode
Enable debug mode for more verbose logging:
```bash
# Edit docker-compose.mount-rdma.yml
# Set DEBUG=true for relevant services
# Restart services
./scripts/run-mount-rdma-tests.sh restart
```

## 🔧 Configuration

### Environment Variables

#### SeaweedFS Mount
- `FILER_ADDR`: Filer address (default: seaweedfs-filer:8888)
- `MOUNT_POINT`: Mount point (default: /mnt/seaweedfs)
- `RDMA_ENABLED`: Enable RDMA (default: true)
- `RDMA_SIDECAR_ADDR`: RDMA sidecar address (default: rdma-sidecar:8081)
- `RDMA_FALLBACK`: Enable HTTP fallback (default: true)
- `RDMA_MAX_CONCURRENT`: Max concurrent RDMA ops (default: 64)
- `RDMA_TIMEOUT_MS`: RDMA timeout (default: 5000)
- `DEBUG`: Enable debug mode (default: false)

#### RDMA Sidecar
- `RDMA_SOCKET_PATH`: Socket path (default: /tmp/rdma/rdma-engine.sock)
- `VOLUME_SERVER_URL`: Volume server URL (default: http://seaweedfs-volume:8080)

#### RDMA Engine
- `RUST_LOG`: Rust log level (default: debug)
- `RDMA_DEVICE`: RDMA device (default: mock)
- `RDMA_PORT`: RDMA port (default: 1)
- `RDMA_GID_INDEX`: GID index (default: 0)

### Customization
To customize the setup:
1. Edit `docker-compose.mount-rdma.yml`
2. Modify environment variables
3. Adjust resource limits
4. Add custom volumes or networks
5. Restart services

## 📈 Performance Tuning

### For Better Performance
1. **Increase concurrent operations**:
   ```yaml
   environment:
     - RDMA_MAX_CONCURRENT=128
   ```

2. **Optimize timeouts**:
   ```yaml
   environment:
     - RDMA_TIMEOUT_MS=1000
   ```

3. **Use larger volumes**:
   ```yaml
   volumes:
     seaweedfs_mount:
       driver_opts:
         o: size=4g
   ```

4. **Enable real RDMA** (with hardware):
   ```yaml
   environment:
     - RDMA_DEVICE=mlx5_0
     - RDMA_PORT=1
   ```

## 🎯 Next Steps

### For Development
1. Implement missing RDMA integration points (see `IMPLEMENTATION-TODO.md`)
2. Add real RDMA hardware support
3. Optimize performance for production workloads
4. Add comprehensive monitoring and alerting

### For Production
1. Deploy on real RDMA-capable hardware
2. Configure proper security and authentication
3. Set up monitoring and logging infrastructure
4. Implement backup and disaster recovery

---

**🚀 Result**: Complete Docker Compose environment for testing SeaweedFS mount with RDMA acceleration, including automated testing and performance benchmarking!
