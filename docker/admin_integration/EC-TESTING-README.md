# SeaweedFS EC Worker Testing Environment

This Docker Compose setup provides a comprehensive testing environment for SeaweedFS Erasure Coding (EC) workers using **official SeaweedFS commands**.

## 📂 Directory Structure

The testing environment is located in `docker/admin_integration/` and includes:

```
docker/admin_integration/
├── Makefile                     # Main management interface
├── docker-compose-ec-test.yml   # Docker compose configuration
├── EC-TESTING-README.md         # This documentation
└── run-ec-test.sh              # Quick start script
```

## 🏗️ Architecture

The testing environment uses **official SeaweedFS commands** and includes:

- **1 Master Server** (port 9333) - Coordinates the cluster with 50MB volume size limit
- **6 Volume Servers** (ports 8080-8085) - Distributed across 2 data centers and 3 racks for diversity  
- **1 Filer** (port 8888) - Provides file system interface
- **1 Admin Server** (port 23646) - Detects volumes needing EC and manages workers using official `admin` command
- **3 EC Workers** - Execute erasure coding tasks using official `worker` command with task-specific working directories
- **1 Load Generator** - Continuously writes and deletes files using SeaweedFS shell commands
- **1 Monitor** - Tracks cluster health and EC progress using shell scripts

## ✨ New Features

### **Task-Specific Working Directories**
Each worker now creates dedicated subdirectories for different task types:
- `/work/erasure_coding/` - For EC encoding tasks
- `/work/vacuum/` - For vacuum cleanup tasks  
- `/work/balance/` - For volume balancing tasks

This provides:
- **Organization**: Each task type gets isolated working space
- **Debugging**: Easy to find files/logs related to specific task types
- **Cleanup**: Can clean up task-specific artifacts easily
- **Concurrent Safety**: Different task types won't interfere with each other's files

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose installed
- GNU Make installed
- At least 4GB RAM available for containers
- Ports 8080-8085, 8888, 9333, 23646 available

### Start the Environment

```bash
# Navigate to the admin integration directory
cd docker/admin_integration/

# Show available commands
make help

# Start the complete testing environment
make start
```

The `make start` command will:
1. Start all services using official SeaweedFS images
2. Configure workers with task-specific working directories
3. Wait for services to be ready
4. Display monitoring URLs and run health checks

### Alternative Commands

```bash
# Quick start aliases
make up              # Same as 'make start'

# Development mode (higher load for faster testing)
make dev-start

# Build images without starting
make build
```

## 📋 Available Make Targets

Run `make help` to see all available targets:

### **🚀 Main Operations**
- `make start` - Start the complete EC testing environment
- `make stop` - Stop all services
- `make restart` - Restart all services
- `make clean` - Complete cleanup (containers, volumes, images)

### **📊 Monitoring & Status**
- `make health` - Check health of all services
- `make status` - Show status of all containers
- `make urls` - Display all monitoring URLs
- `make monitor` - Open monitor dashboard in browser
- `make monitor-status` - Show monitor status via API
- `make volume-status` - Show volume status from master
- `make admin-status` - Show admin server status
- `make cluster-status` - Show complete cluster status

### **📋 Logs Management**
- `make logs` - Show logs from all services
- `make logs-admin` - Show admin server logs
- `make logs-workers` - Show all worker logs
- `make logs-worker1/2/3` - Show specific worker logs
- `make logs-load` - Show load generator logs
- `make logs-monitor` - Show monitor logs
- `make backup-logs` - Backup all logs to files

### **⚖️ Scaling & Testing**
- `make scale-workers WORKERS=5` - Scale workers to 5 instances
- `make scale-load RATE=25` - Increase load generation rate
- `make test-ec` - Run focused EC test scenario

### **🔧 Development & Debug**
- `make shell-admin` - Open shell in admin container
- `make shell-worker1` - Open shell in worker container
- `make debug` - Show debug information
- `make troubleshoot` - Run troubleshooting checks

## 📊 Monitoring URLs

| Service | URL | Description |
|---------|-----|-------------|
| Master UI | http://localhost:9333 | Cluster status and topology |
| Filer | http://localhost:8888 | File operations |
| Admin Server | http://localhost:23646/ | Task management |
| Monitor | http://localhost:9999/status | Complete cluster monitoring |
| Volume Servers | http://localhost:8080-8085/status | Individual volume server stats |

Quick access: `make urls` or `make monitor`

## 🔄 How EC Testing Works

### 1. Continuous Load Generation
- **Write Rate**: 10 files/second (1-5MB each)
- **Delete Rate**: 2 files/second
- **Target**: Fill volumes to 50MB limit quickly

### 2. Volume Detection
- Admin server scans master every 30 seconds
- Identifies volumes >40MB (80% of 50MB limit)
- Queues EC tasks for eligible volumes

### 3. EC Worker Assignment
- **Worker 1**: EC specialist (max 2 concurrent tasks)
- **Worker 2**: EC + Vacuum hybrid (max 2 concurrent tasks)  
- **Worker 3**: EC + Vacuum hybrid (max 1 concurrent task)

### 4. Comprehensive EC Process
Each EC task follows 6 phases:
1. **Copy Volume Data** (5-15%) - Stream .dat/.idx files locally
2. **Mark Read-Only** (20-25%) - Ensure data consistency
3. **Local Encoding** (30-60%) - Create 14 shards (10+4 Reed-Solomon)
4. **Calculate Placement** (65-70%) - Smart rack-aware distribution
5. **Distribute Shards** (75-90%) - Upload to optimal servers
6. **Verify & Cleanup** (95-100%) - Validate and clean temporary files

### 5. Real-Time Monitoring
- Volume analysis and EC candidate detection
- Worker health and task progress
- No data loss verification
- Performance metrics

## 📋 Key Features Tested

### ✅ EC Implementation Features
- [x] Local volume data copying with progress tracking
- [x] Local Reed-Solomon encoding (10+4 shards)
- [x] Intelligent shard placement with rack awareness
- [x] Load balancing across available servers
- [x] Backup server selection for redundancy
- [x] Detailed step-by-step progress tracking
- [x] Comprehensive error handling and recovery

### ✅ Infrastructure Features
- [x] Multi-datacenter topology (dc1, dc2)
- [x] Rack diversity (rack1, rack2, rack3)
- [x] Volume size limits (50MB)
- [x] Worker capability matching
- [x] Health monitoring and alerting
- [x] Continuous workload simulation

## 🛠️ Common Usage Patterns

### Basic Testing Workflow
```bash
# Start environment
make start

# Watch progress
make monitor-status

# Check for EC candidates
make volume-status

# View worker activity
make logs-workers

# Stop when done
make stop
```

### High-Load Testing
```bash
# Start with higher load
make dev-start

# Scale up workers and load
make scale-workers WORKERS=5
make scale-load RATE=50

# Monitor intensive EC activity
make logs-admin
```

### Debugging Issues
```bash
# Check port conflicts and system state
make troubleshoot

# View specific service logs
make logs-admin
make logs-worker1

# Get shell access for debugging
make shell-admin
make shell-worker1

# Check detailed status
make debug
```

### Development Iteration
```bash
# Quick restart after code changes
make restart

# Rebuild and restart
make clean
make start

# Monitor specific components
make logs-monitor
```

## 📈 Expected Results

### Successful EC Testing Shows:
1. **Volume Growth**: Steady increase in volume sizes toward 50MB limit
2. **EC Detection**: Admin server identifies volumes >40MB for EC
3. **Task Assignment**: Workers receive and execute EC tasks
4. **Shard Distribution**: 14 shards distributed across 6 volume servers
5. **No Data Loss**: All files remain accessible during and after EC
6. **Performance**: EC tasks complete within estimated timeframes

### Sample Monitor Output:
```bash
# Check current status
make monitor-status

# Output example:
{
  "monitor": {
    "uptime": "15m30s",
    "master_addr": "master:9333",
    "admin_addr": "admin:9900"
  },
  "stats": {
    "VolumeCount": 12,
    "ECTasksDetected": 3,
    "WorkersActive": 3
  }
}
```

## 🔧 Configuration

### Environment Variables

You can customize the environment by setting variables:

```bash
# High load testing
WRITE_RATE=25 DELETE_RATE=5 make start

# Extended test duration
TEST_DURATION=7200 make start  # 2 hours
```

### Scaling Examples

```bash
# Scale workers
make scale-workers WORKERS=6

# Increase load generation
make scale-load RATE=30

# Combined scaling
make scale-workers WORKERS=4
make scale-load RATE=40
```

## 🧹 Cleanup Options

```bash
# Stop services only
make stop

# Remove containers but keep volumes
make down

# Remove data volumes only
make clean-volumes

# Remove built images only
make clean-images

# Complete cleanup (everything)
make clean
```

## 🐛 Troubleshooting

### Quick Diagnostics
```bash
# Run complete troubleshooting
make troubleshoot

# Check specific components
make health
make debug
make status
```

### Common Issues

**Services not starting:**
```bash
# Check port availability
make troubleshoot

# View startup logs
make logs-master
make logs-admin
```

**No EC tasks being created:**
```bash
# Check volume status
make volume-status

# Increase load to fill volumes faster
make scale-load RATE=30

# Check admin detection
make logs-admin
```

**Workers not responding:**
```bash
# Check worker registration
make admin-status

# View worker logs
make logs-workers

# Restart workers
make restart
```

### Performance Tuning

**For faster testing:**
```bash
make dev-start           # Higher default load
make scale-load RATE=50  # Very high load
```

**For stress testing:**
```bash
make scale-workers WORKERS=8
make scale-load RATE=100
```

## 📚 Technical Details

### Network Architecture
- Custom bridge network (172.20.0.0/16)
- Service discovery via container names
- Health checks for all services

### Storage Layout
- Each volume server: max 100 volumes
- Data centers: dc1, dc2
- Racks: rack1, rack2, rack3
- Volume limit: 50MB per volume

### EC Algorithm
- Reed-Solomon RS(10,4)
- 10 data shards + 4 parity shards
- Rack-aware distribution
- Backup server redundancy

### Make Integration
- Color-coded output for better readability
- Comprehensive help system (`make help`)
- Parallel execution support
- Error handling and cleanup
- Cross-platform compatibility

## 🎯 Quick Reference

```bash
# Essential commands
make help              # Show all available targets
make start             # Start complete environment
make health            # Check all services
make monitor           # Open dashboard
make logs-admin        # View admin activity
make clean             # Complete cleanup

# Monitoring
make volume-status     # Check for EC candidates  
make admin-status      # Check task queue
make monitor-status    # Full cluster status

# Scaling & Testing
make test-ec           # Run focused EC test
make scale-load RATE=X # Increase load
make troubleshoot      # Diagnose issues
```

This environment provides a realistic testing scenario for SeaweedFS EC workers with actual data operations, comprehensive monitoring, and easy management through Make targets. 