# Real RDMA Hardware Testing Guide

This guide shows how to test the SeaweedFS RDMA acceleration system with real RDMA hardware.

## 🎯 Overview

Our current system works with **mock RDMA** for development. To test with **real hardware**, you need:

1. **Native Linux environment** (not Docker Desktop on Mac)
2. **RDMA-capable hardware** OR software simulation (Soft-RoCE)
3. **Proper kernel modules** and drivers
4. **UCX libraries** with hardware support

## 🚀 Option 1: Soft-RoCE on Native Linux (Recommended for Testing)

### Prerequisites
- Ubuntu 20.04+ or similar Linux distribution
- Root/sudo access
- Network interface (eth0, ens33, etc.)

### Setup Steps

```bash
# 1. Install RDMA packages
sudo apt update
sudo apt install rdma-core libibverbs-dev librdmacm-dev
sudo apt install ibverbs-utils infiniband-diags
sudo apt install ucx-tools libucx-dev

# 2. Load Soft-RoCE kernel module
sudo modprobe rdma_rxe

# 3. Create RXE device over existing network interface
sudo rdma link add rxe0 type rxe netdev eth0
# Alternative method:
# echo eth0 | sudo tee /sys/class/infiniband/rxe/ports/add

# 4. Verify RDMA device is created
ibv_devinfo
rdma dev show

# Expected output:
# hca_id: rxe0
#     transport:                  InfiniBand (0)
#     fw_ver:                     0.0.0
#     node_guid:                  xxxx:xxxx:xxxx:xxxx
#     sys_image_guid:             xxxx:xxxx:xxxx:xxxx
#     vendor_id:                  0x0000
#     vendor_part_id:             0
#     hw_ver:                     0x0
#     phys_port_cnt:              1
```

### Testing Soft-RoCE

```bash
# Test basic RDMA functionality
ib_send_bw -d rxe0 &
ib_send_bw -d rxe0 localhost

# Test UCX with real device
ucx_info -d
ucx_perftest -t tag_lat -d rxe0
```

## 🏢 Option 2: Real RDMA Hardware

### Supported Hardware
- **Mellanox ConnectX-4/5/6/7** NICs (recommended)
- **Intel Omni-Path** adapters
- **Broadcom NetXtreme-E RoCE** NICs
- **Chelsio T6 iWARP** adapters

### Mellanox Setup (Most Common)

```bash
# 1. Download and install Mellanox OFED
wget https://www.mellanox.com/downloads/ofed/MLNX_OFED-5.8-1.1.2.1/MLNX_OFED_LINUX-5.8-1.1.2.1-ubuntu20.04-x86_64.tgz
tar -xzf MLNX_OFED_LINUX-*.tgz
cd MLNX_OFED_LINUX-*
sudo ./mlnxofedinstall --upstream-libs --dpdk

# 2. Restart and configure
sudo /etc/init.d/openibd restart
sudo ip link set dev ib0 up
sudo ip addr add 192.168.1.10/24 dev ib0

# 3. Test hardware
ibv_devinfo
ibstat
ib_send_bw  # bandwidth test
```

## ☁️ Option 3: Cloud RDMA Instances

### AWS EC2 with Enhanced Networking

```bash
# Launch RDMA-capable instance
aws ec2 run-instances \
  --image-id ami-0abcdef1234567890 \
  --instance-type c5n.large \
  --key-name my-key \
  --security-groups rdma-sg \
  --placement GroupName=my-cluster \
  --ena-support

# Enable SR-IOV
aws ec2 modify-instance-attribute \
  --instance-id i-1234567890abcdef0 \
  --ena-support
```

### Azure HPC Instances

```bash
# Create HPC cluster with InfiniBand
az vm create \
  --resource-group myResourceGroup \
  --name myHPCVM \
  --image OpenLogic:CentOS-HPC:7.7:latest \
  --size Standard_HB60rs \
  --enable-infiniband
```

## 🔧 Code Modifications for Real Hardware

### 1. Rust Engine Changes

Update `rdma-engine/Cargo.toml`:
```toml
[features]
default = ["real-ucx"]  # Change from "mock-ucx"
mock-ucx = []
real-ucx = []
```

Update `rdma-engine/src/rdma.rs`:
```rust
impl RdmaContext {
    pub fn new(config: RdmaEngineConfig) -> Result<Self, RdmaError> {
        let inner = if cfg!(feature = "real-ucx") {
            // Use real UCX context with hardware device
            RdmaContextImpl::Ucx(UcxRdmaContext::new(config)?)
        } else {
            RdmaContextImpl::Mock(MockRdmaContext::new(config))
        };
        
        Ok(RdmaContext { inner })
    }
}
```

### 2. UCX Configuration

Create `rdma-engine/src/ucx_real.rs`:
```rust
impl UcxRdmaContext {
    pub fn new(config: RdmaEngineConfig) -> Result<Self, RdmaError> {
        // Initialize UCX context with real device
        let ucx_config = UcxConfig::new()?;
        ucx_config.set("NET_DEVICES", "rxe0:1")?;  // Use real device
        ucx_config.set("TLS", "rc,ud,dc")?;        // Real transports
        
        let context = UcxContext::new(&ucx_config)?;
        let worker = context.create_worker()?;
        
        Ok(UcxRdmaContext {
            context,
            worker,
            config,
        })
    }
}
```

### 3. Docker Configuration for Real Hardware

Update `docker-compose.yml`:
```yaml
services:
  rdma-engine:
    build:
      context: .
      dockerfile: Dockerfile.rdma-engine
    privileged: true  # Required for RDMA access
    volumes:
      - /dev/infiniband:/dev/infiniband  # Mount RDMA devices
      - /sys/class/infiniband:/sys/class/infiniband:ro
    environment:
      - UCX_NET_DEVICES=rxe0:1
      - UCX_TLS=rc,ud,dc,shm,self
      - RUST_LOG=debug
    command: ["./rdma-engine-server", "--device", "rxe0", "--port", "1"]
```

### 4. Go Sidecar Configuration

Update configuration to detect real devices:
```go
type RDMAConfig struct {
    DeviceName    string `json:"device_name"`
    Port          int    `json:"port"`
    GID           string `json:"gid"`
    EnableRealRDMA bool  `json:"enable_real_rdma"`
}

func DetectRDMADevices() ([]string, error) {
    devices := []string{}
    
    // Check for InfiniBand devices
    files, err := filepath.Glob("/sys/class/infiniband/*")
    if err != nil {
        return devices, err
    }
    
    for _, file := range files {
        device := filepath.Base(file)
        devices = append(devices, device)
    }
    
    return devices, nil
}
```

## 🧪 Testing with Real Hardware

### 1. Build with Real UCX Support

```bash
# Build Rust engine with real UCX
cd rdma-engine
cargo build --release --features real-ucx

# Build Go components
cd ..
go build -o bin/sidecar ./cmd/sidecar
go build -o bin/test-rdma ./cmd/test-rdma
```

### 2. Test RDMA Device Detection

```bash
# Test device detection
./bin/test-rdma capabilities --device rxe0

# Expected output with real device:
# Device: rxe0
# Real RDMA: true
# Max Transfer Size: 1073741824
# Transport: RC, UD, DC
```

### 3. Performance Testing

```bash
# Run performance benchmark with real RDMA
./bin/test-rdma bench \
  --device rxe0 \
  --iterations 1000 \
  --size 4096 \
  --concurrent 10

# Expected improvements:
# Latency: ~1-10µs (vs 4ms mock)
# Throughput: 1-100GB/s (vs 250 ops/sec mock)
```

### 4. End-to-End Integration Test

```bash
# Start services with real RDMA
docker-compose -f docker-compose.real-rdma.yml up -d

# Run integration tests
make test-real-rdma

# Test SeaweedFS needle reads with real RDMA
curl "http://localhost:8081/read?volume=1&needle=12345&size=1MB"
```

## 📊 Expected Performance Improvements

| Metric | Mock RDMA | Soft-RoCE | Real Hardware |
|--------|-----------|-----------|---------------|
| Latency | ~4ms | ~100µs | ~1µs |
| Throughput | 250 ops/sec | ~1GB/s | ~100GB/s |
| CPU Usage | High | Medium | Low |
| Memory Copy | Yes | Minimal | Zero-copy |

## 🔍 Troubleshooting

### Common Issues

1. **No RDMA devices found**
   ```bash
   # Check kernel modules
   lsmod | grep rdma
   sudo modprobe rdma_rxe
   ```

2. **UCX initialization fails**
   ```bash
   # Check UCX configuration
   ucx_info -d
   export UCX_LOG_LEVEL=debug
   ```

3. **Permission denied**
   ```bash
   # Add user to rdma group
   sudo usermod -a -G rdma $USER
   # Or run with sudo for testing
   ```

4. **Docker container can't access devices**
   ```bash
   # Ensure privileged mode and device mounts
   docker run --privileged -v /dev/infiniband:/dev/infiniband ...
   ```

## 🎯 Next Steps

1. **Choose your testing environment** (Soft-RoCE recommended for initial testing)
2. **Set up RDMA devices** following the appropriate option above
3. **Modify the code** to enable real UCX support
4. **Build and test** with real RDMA devices
5. **Benchmark performance** and compare with mock results
6. **Deploy to production** with real RDMA hardware

## 📚 Additional Resources

- [UCX Documentation](https://openucx.readthedocs.io/)
- [RDMA Programming Guide](https://www.kernel.org/doc/Documentation/infiniband/user_verbs.txt)
- [Mellanox OFED Documentation](https://docs.mellanox.com/display/MLNXOFEDv581032)
- [Soft-RoCE Configuration Guide](https://github.com/SoftRoCE/rxe-dev/wiki/rxe-dev:-Home)

---

**🎊 With real RDMA hardware, you'll see dramatic performance improvements: microsecond latencies and multi-gigabit throughput!**
