# 🚀 RDMA Hardware Simulation for SeaweedFS

This guide explains how to simulate RDMA hardware using software implementations for testing and development of the SeaweedFS RDMA integration without requiring physical RDMA hardware.

## 🎯 Overview

RDMA (Remote Direct Memory Access) simulation allows you to:
- **Test real RDMA code paths** instead of mock implementations
- **Validate UCX integration** with actual RDMA operations  
- **Develop without expensive hardware** using software implementations
- **CI/CD testing** in containerized environments

## 🔧 RDMA Simulation Technologies

### 1. **Soft-RoCE (RXE)** ⭐ **Recommended**
- **What**: Software implementation of RoCE (RDMA over Converged Ethernet)
- **Kernel Module**: `rdma_rxe` or `rxe_net`
- **Benefits**: Works on any Ethernet interface, most compatible
- **Performance**: Good for development, moderate performance

### 2. **SoftiWARP (siw)**
- **What**: Software iWARP implementation over TCP/IP
- **Kernel Module**: `siw`
- **Benefits**: Works over any IP network
- **Performance**: Lower than RoCE but very compatible

### 3. **UCX Mock Transports**
- **What**: UCX built-in TCP and shared memory transports
- **Benefits**: No kernel modules required
- **Limitations**: Not true RDMA, but useful for basic testing

## 🐳 Docker RDMA Simulation Setup

### Quick Start with Docker Compose

```bash
# 1. Build RDMA simulation environment
docker-compose -f docker-compose.rdma-sim.yml build

# 2. Start with RDMA simulation
docker-compose -f docker-compose.rdma-sim.yml up -d

# 3. Check RDMA status
docker-compose -f docker-compose.rdma-sim.yml exec rdma-simulation /opt/rdma-sim/test-rdma.sh

# 4. Run integration tests with real RDMA
docker-compose -f docker-compose.rdma-sim.yml run --rm integration-tests-rdma
```

### Manual Setup

```bash
# 1. Build simulation container
docker build -f docker/Dockerfile.rdma-simulation -t rdma-sim .

# 2. Run with required privileges
docker run -it --privileged \
  --cap-add=SYS_MODULE \
  -v /lib/modules:/lib/modules:ro \
  -v /sys:/sys \
  rdma-sim

# 3. Inside container - setup Soft-RoCE
sudo /opt/rdma-sim/setup-soft-roce.sh

# 4. Test RDMA functionality
/opt/rdma-sim/test-rdma.sh

# 5. Check UCX configuration
/opt/rdma-sim/ucx-info.sh
```

## 📋 Architecture: RDMA Simulation Integration

```
┌─────────────────────┐    ┌─────────────────────┐
│   SeaweedFS Master  │    │  SeaweedFS Volume   │
│     :9333          │◄──►│     :8080          │
└─────────────────────┘    └─────────────────────┘
                                    │
                                    ▼
┌─────────────────────┐    ┌─────────────────────┐
│   Go RDMA Sidecar  │◄──►│   Rust RDMA Engine  │
│     :8081          │    │  (Real UCX/RDMA)    │
└─────────────────────┘    └─────────────────────┘
         │                          │
         ▼                          ▼
┌─────────────────────┐    ┌─────────────────────┐
│  Integration Tests  │    │  RDMA Simulation    │
│                    │    │  (Soft-RoCE/RXE)    │
└─────────────────────┘    └─────────────────────┘
                                    │
                                    ▼
                           ┌─────────────────────┐
                           │   Virtual RDMA      │
                           │   Device (rxe0)     │
                           └─────────────────────┘
```

## 🔧 Configuration Options

### Environment Variables

```bash
# RDMA Transport Selection
export UCX_TLS=rc_verbs,ud_verbs,tcp,shm

# Network Device Configuration  
export UCX_NET_DEVICES=all
export UCX_IB_DEVICE_SPECS=rxe0:1

# Performance Tuning
export UCX_RNDV_SCHEME=put_zcopy
export UCX_RNDV_THRESH=8192

# Debugging
export UCX_LOG_LEVEL=info
export UCX_LOG_FILE=/tmp/ucx.log
```

### Docker Compose Profiles

```bash
# Basic testing (mock RDMA)
docker-compose up -d

# RDMA simulation testing
docker-compose -f docker-compose.rdma-sim.yml up -d

# Run integration tests
docker-compose -f docker-compose.rdma-sim.yml --profile testing up
```

## 🧪 Testing RDMA Simulation

### Verification Steps

```bash
# 1. Check RDMA devices
ls /sys/class/infiniband/

# 2. Test libibverbs
ibv_devinfo

# 3. Test UCX
ucx_info -d

# 4. Test SeaweedFS RDMA integration
curl http://localhost:8081/health
curl http://localhost:8081/stats
```

### Expected Output

```bash
# RDMA Devices
$ ls /sys/class/infiniband/
rxe0

# Device Info
$ ibv_devinfo
hca_id: rxe0
    transport:                  InfiniBand (0)
    fw_ver:                     0.0.0
    node_guid:                  xxxx:xxxx:xxxx:xxxx
    sys_image_guid:             xxxx:xxxx:xxxx:xxxx
    vendor_id:                  0x0000
    vendor_part_id:             0
    hw_ver:                     0x0
    board_id:                   RXE
        port:   1
            state:              PORT_ACTIVE (4)
            max_mtu:            4096 (5)
            active_mtu:         1024 (3)
            sm_lid:             0
            port_lid:           0
            port_lmc:           0x00
            link_layer:         Ethernet

# UCX Transports  
$ ucx_info -d
# UCX_TLS=rc_verbs,ud_verbs,tcp,shm
# Transport: rc_verbs  device: rxe0:1
# Transport: ud_verbs  device: rxe0:1
# Transport: tcp       device: eth0
# Transport: shm       device: memory
```

## 🚀 Performance Characteristics

### Soft-RoCE Performance

| Metric | Mock RDMA | Soft-RoCE | Hardware RDMA |
|--------|-----------|-----------|---------------|
| **Latency** | ~2ms | ~100μs | ~1μs |
| **Throughput** | ~100MB/s | ~1GB/s | ~100GB/s |
| **CPU Usage** | Low | Medium | Very Low |
| **Memory Copy** | Yes | Minimal | Zero-copy |

### Performance Tuning

```bash
# For maximum Soft-RoCE performance
export UCX_RNDV_SCHEME=put_zcopy
export UCX_RNDV_THRESH=1024
export UCX_TLS=rc_verbs
export UCX_IB_DEVICE_SPECS=rxe0:1

# For development/debugging  
export UCX_LOG_LEVEL=debug
export UCX_MEMTYPE_CACHE=y
```

## 🔍 Troubleshooting

### Common Issues

#### 1. **RDMA Devices Not Found**
```bash
# Check kernel module
sudo modprobe rdma_rxe
lsmod | grep rxe

# Setup device manually
sudo rxe_cfg add eth0
```

#### 2. **Permission Denied**
```bash
# Container needs privileges
docker run --privileged --cap-add=SYS_MODULE

# Or use systemd
sudo systemctl start rdma-rxe
```

#### 3. **UCX Transport Issues**
```bash
# Check available transports
ucx_info -d

# Force specific transport
export UCX_TLS=tcp  # Fallback to TCP

# Debug transport selection
export UCX_LOG_LEVEL=debug
```

#### 4. **Network Interface Issues**
```bash
# List interfaces
ip link show

# Use specific interface
sudo rxe_cfg add enp0s3  # Instead of eth0

# Check interface is up
sudo ip link set enp0s3 up
```

### Debugging Commands

```bash
# Container debugging
docker-compose -f docker-compose.rdma-sim.yml exec rdma-simulation bash
docker-compose logs rdma-simulation

# RDMA debugging
ibv_devinfo -v
ucx_info -c
rdma link show

# SeaweedFS RDMA debugging
curl -s http://localhost:8081/stats | jq '.rdma'
curl -s http://localhost:8081/rdma/capabilities | jq '.'
```

## 📊 Integration Testing

### Automated Tests

```bash
# Full integration test with RDMA simulation
./tests/docker-test-helper.sh test-rdma

# Specific RDMA tests
docker-compose -f docker-compose.rdma-sim.yml exec test-client \
  ./test-rdma capabilities

# Performance benchmarks
docker-compose -f docker-compose.rdma-sim.yml exec rdma-sidecar \
  curl "http://localhost:8081/benchmark?iterations=100&size=1024"
```

### Expected Results

```json
{
  "rdma": {
    "enabled": true,
    "connected": true,
    "real_rdma": true,
    "device_name": "rxe0",
    "transport": "rc_verbs",
    "max_transfer_size": 1073741824
  },
  "performance": {
    "operations_per_second": 50000,
    "average_latency": "100μs",
    "throughput": "500MB/s"
  }
}
```

## 🎯 Next Steps

### Development Workflow

1. **Start simulation**: `docker-compose -f docker-compose.rdma-sim.yml up -d`
2. **Test RDMA**: Verify real RDMA operations work
3. **Develop features**: Use real UCX/RDMA code paths  
4. **Run benchmarks**: Measure realistic performance
5. **Deploy to hardware**: Transition to real RDMA hardware

### Hardware Migration  

```bash
# Development (Simulation)
export UCX_TLS=rc_verbs,ud_verbs,tcp

# Production (Hardware) 
export UCX_TLS=rc_mlx5_dv,dc_mlx5_dv,ud_mlx5_dv
```

## 📚 Additional Resources

- [Soft-RoCE Documentation](https://github.com/SoftRoCE/rxe-dev)
- [UCX Documentation](https://github.com/openucx/ucx)
- [RDMA Programming Guide](https://www.rdmamojo.com/)
- [SeaweedFS RDMA Integration](README.md)

---

**🎉 With RDMA simulation, you can now test real RDMA operations without specialized hardware!**

This simulation environment provides a bridge between mock testing and production hardware, enabling realistic development and validation of the SeaweedFS RDMA integration.
