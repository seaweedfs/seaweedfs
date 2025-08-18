# 🐳 Docker Integration Testing Guide

This guide provides comprehensive Docker-based integration testing for the SeaweedFS RDMA sidecar system.

## 🏗️ Architecture

The Docker Compose setup includes:

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   SeaweedFS Master  │    │  SeaweedFS Volume   │    │    Rust RDMA       │
│     :9333          │◄──►│     :8080          │    │    Engine           │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
                                    │                           │
                                    ▼                           ▼
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   Go RDMA Sidecar  │◄──►│    Unix Socket      │◄──►│   Integration       │
│     :8081          │    │   /tmp/rdma.sock    │    │   Test Suite        │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
```

## 🚀 Quick Start

### 1. Start All Services

```bash
# Using the helper script (recommended)
./tests/docker-test-helper.sh start

# Or using docker-compose directly
docker-compose up -d
```

### 2. Run Integration Tests

```bash
# Run the complete test suite
./tests/docker-test-helper.sh test

# Or run tests manually
docker-compose run --rm integration-tests
```

### 3. Interactive Testing

```bash
# Open a shell in the test container
./tests/docker-test-helper.sh shell

# Inside the container, you can run:
./test-rdma ping
./test-rdma capabilities  
./test-rdma read --volume 1 --needle 12345 --size 1024
curl http://rdma-sidecar:8081/health
curl http://rdma-sidecar:8081/stats
```

## 📋 Test Helper Commands

The `docker-test-helper.sh` script provides convenient commands:

```bash
# Service Management
./tests/docker-test-helper.sh start      # Start all services
./tests/docker-test-helper.sh stop       # Stop all services  
./tests/docker-test-helper.sh clean      # Stop and clean volumes

# Testing
./tests/docker-test-helper.sh test       # Run integration tests
./tests/docker-test-helper.sh shell      # Interactive testing shell

# Monitoring
./tests/docker-test-helper.sh status     # Check service health
./tests/docker-test-helper.sh logs       # Show all logs
./tests/docker-test-helper.sh logs rdma-engine  # Show specific service logs
```

## 🧪 Test Coverage

The integration test suite covers:

### ✅ Core Components
- **SeaweedFS Master**: Cluster leadership and status
- **SeaweedFS Volume Server**: Volume operations and health
- **Rust RDMA Engine**: Socket communication and operations
- **Go RDMA Sidecar**: HTTP API and RDMA integration

### ✅ Integration Points
- **IPC Communication**: Unix socket + MessagePack protocol
- **RDMA Operations**: Ping, capabilities, read operations
- **HTTP API**: All sidecar endpoints and error handling
- **Fallback Logic**: RDMA → HTTP fallback behavior

### ✅ Performance Testing
- **Direct RDMA Benchmarks**: Engine-level performance
- **Sidecar Benchmarks**: End-to-end performance
- **Latency Measurements**: Operation timing validation
- **Throughput Testing**: Operations per second

## 🔧 Service Details

### SeaweedFS Master
- **Port**: 9333
- **Health Check**: `/cluster/status`
- **Data**: Persistent volume `master-data`

### SeaweedFS Volume Server
- **Port**: 8080  
- **Health Check**: `/status`
- **Data**: Persistent volume `volume-data`
- **Depends on**: SeaweedFS Master

### Rust RDMA Engine
- **Socket**: `/tmp/rdma-engine.sock`
- **Mode**: Mock RDMA (development)
- **Health Check**: Socket existence
- **Privileged**: Yes (for RDMA access)

### Go RDMA Sidecar
- **Port**: 8081
- **Health Check**: `/health`
- **API Endpoints**: `/stats`, `/read`, `/benchmark`
- **Depends on**: RDMA Engine, Volume Server

### Test Client
- **Purpose**: Integration testing and interactive debugging
- **Tools**: curl, jq, test-rdma binary
- **Environment**: All service URLs configured

## 📊 Expected Test Results

### ✅ Successful Output Example

```
===============================================
🚀 SEAWEEDFS RDMA INTEGRATION TEST SUITE  
===============================================

🔵 Waiting for SeaweedFS Master to be ready...
✅ SeaweedFS Master is ready
✅ SeaweedFS Master is leader and ready

🔵 Waiting for SeaweedFS Volume Server to be ready...
✅ SeaweedFS Volume Server is ready
Volume Server Version: 3.60

🔵 Checking RDMA engine socket...
✅ RDMA engine socket exists
🔵 Testing RDMA engine ping...
✅ RDMA engine ping successful

🔵 Waiting for RDMA Sidecar to be ready...
✅ RDMA Sidecar is ready
✅ RDMA Sidecar is healthy
RDMA Status: true

🔵 Testing needle read via sidecar...
✅ Sidecar needle read successful
⚠️  HTTP fallback used. Duration: 2.48ms

🔵 Running sidecar performance benchmark...
✅ Sidecar benchmark completed
Benchmark Results:
  RDMA Operations: 5
  HTTP Operations: 0  
  Average Latency: 2.479ms
  Operations/sec: 403.2

===============================================
🎉 ALL INTEGRATION TESTS COMPLETED!
===============================================
```

## 🐛 Troubleshooting

### Service Not Starting

```bash
# Check service logs
./tests/docker-test-helper.sh logs [service-name]

# Check container status
docker-compose ps

# Restart specific service
docker-compose restart [service-name]
```

### RDMA Engine Issues

```bash
# Check socket permissions
docker-compose exec rdma-engine ls -la /tmp/rdma/rdma-engine.sock

# Check RDMA engine logs
./tests/docker-test-helper.sh logs rdma-engine

# Test socket directly
docker-compose exec test-client ./test-rdma ping
```

### Sidecar Connection Issues  

```bash
# Test sidecar health directly
curl http://localhost:8081/health

# Check sidecar logs
./tests/docker-test-helper.sh logs rdma-sidecar

# Verify environment variables
docker-compose exec rdma-sidecar env | grep RDMA
```

### Volume Server Issues

```bash
# Check SeaweedFS status
curl http://localhost:9333/cluster/status
curl http://localhost:8080/status

# Check volume server logs  
./tests/docker-test-helper.sh logs seaweedfs-volume
```

## 🔍 Manual Testing Examples

### Test RDMA Engine Directly

```bash
# Enter test container
./tests/docker-test-helper.sh shell

# Test RDMA operations
./test-rdma ping --socket /tmp/rdma-engine.sock
./test-rdma capabilities --socket /tmp/rdma-engine.sock
./test-rdma read --socket /tmp/rdma-engine.sock --volume 1 --needle 12345
./test-rdma bench --socket /tmp/rdma-engine.sock --iterations 10
```

### Test Sidecar HTTP API

```bash
# Health and status
curl http://rdma-sidecar:8081/health | jq '.'
curl http://rdma-sidecar:8081/stats | jq '.'

# Needle operations
curl "http://rdma-sidecar:8081/read?volume=1&needle=12345&size=1024" | jq '.'

# Benchmarking
curl "http://rdma-sidecar:8081/benchmark?iterations=5&size=2048" | jq '.benchmark_results'
```

### Test SeaweedFS Integration

```bash
# Check cluster status
curl http://seaweedfs-master:9333/cluster/status | jq '.'

# Check volume status  
curl http://seaweedfs-volume:8080/status | jq '.'

# List volumes
curl http://seaweedfs-master:9333/vol/status | jq '.'
```

## 🚀 Production Deployment

This Docker setup can be adapted for production by:

1. **Replacing Mock RDMA**: Switch to `real-ucx` feature in Rust
2. **RDMA Hardware**: Add RDMA device mappings and capabilities
3. **Security**: Remove privileged mode, add proper user/group mapping  
4. **Scaling**: Use Docker Swarm or Kubernetes for orchestration
5. **Monitoring**: Add Prometheus metrics and Grafana dashboards
6. **Persistence**: Configure proper volume management

## 📚 Additional Resources

- [Main README](README.md) - Complete project overview
- [Docker Compose Reference](https://docs.docker.com/compose/)
- [SeaweedFS Documentation](https://github.com/seaweedfs/seaweedfs/wiki)
- [UCX Documentation](https://github.com/openucx/ucx)

---

**🐳 Happy Docker Testing!** 

For issues or questions, please check the logs first and refer to the troubleshooting section above.
