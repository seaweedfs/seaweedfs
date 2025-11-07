# FoundationDB Integration Testing

This directory contains integration tests and setup scripts for the FoundationDB filer store in SeaweedFS.

## Quick Start

```bash
# ✅ GUARANTEED TO WORK - Run reliable tests (no FoundationDB dependencies)
make test-reliable      # Validation + Mock tests

# Run individual test types
make test-mock          # Mock FoundationDB tests (always work)
go test -v ./validation_test.go  # Package structure validation

# 🐳 FULL INTEGRATION (requires Docker + FoundationDB dependencies)
make setup              # Start FoundationDB cluster + SeaweedFS
make test               # Run all integration tests
make test-simple        # Simple containerized test environment

# Clean up
make clean              # Clean main environment
make clean-simple       # Clean simple test environment

# 🍎 ARM64 / APPLE SILICON SUPPORT
make setup-arm64        # Native ARM64 FoundationDB (builds from source)
make setup-emulated     # x86 emulation (faster setup)
make test-arm64         # Test with ARM64 native
make test-emulated      # Test with x86 emulation
```

### Test Levels

1. **✅ Validation Tests** (`validation_test.go`) - Always work, no dependencies
2. **✅ Mock Tests** (`mock_integration_test.go`) - Test FoundationDB store logic with mocks
3. **⚠️  Real Integration Tests** (`foundationdb_*_test.go`) - Require actual FoundationDB cluster

### ARM64 / Apple Silicon Support

**🍎 For M1/M2/M3 Mac users:** FoundationDB's official Docker images are AMD64-only. We provide three solutions:

- **Native ARM64** (`make setup-arm64`) - Builds FoundationDB from source (10-15 min setup, best performance)
- **x86 Emulation** (`make setup-emulated`) - Uses Docker emulation (fast setup, slower runtime)  
- **Mock Testing** (`make test-mock`) - No FoundationDB needed (instant, tests logic only)

📖 **Detailed Guide:** See [README.ARM64.md](README.ARM64.md) for complete ARM64 documentation.

## Test Environment

The test environment includes:

- **3-node FoundationDB cluster** (fdb1, fdb2, fdb3) for realistic distributed testing
- **Database initialization service** (fdb-init) that configures the cluster
- **SeaweedFS service** configured to use the FoundationDB filer store
- **Automatic service orchestration** with proper startup dependencies

## Test Structure

### Integration Tests

#### `foundationdb_integration_test.go`
- Basic CRUD operations (Create, Read, Update, Delete)
- Directory operations and listing
- Transaction handling (begin, commit, rollback)
- Key-Value operations
- Large entry handling with compression
- Error scenarios and edge cases

#### `foundationdb_concurrent_test.go`
- Concurrent insert operations across multiple goroutines
- Concurrent read/write operations on shared files
- Concurrent transaction handling with conflict resolution
- Concurrent directory operations
- Concurrent key-value operations
- Stress testing under load

#### Unit Tests (`weed/filer/foundationdb/foundationdb_store_test.go`)
- Store initialization and configuration
- Key generation and directory prefixes
- Error handling and validation
- Performance benchmarks
- Configuration validation

## Configuration

### Environment Variables

The tests can be configured using environment variables:

```bash
export FDB_CLUSTER_FILE=/var/fdb/config/fdb.cluster
export WEED_FOUNDATIONDB_ENABLED=true
export WEED_FOUNDATIONDB_API_VERSION=740
export WEED_FOUNDATIONDB_TIMEOUT=10s
```

### Docker Compose Configuration

The `docker-compose.yml` sets up:

1. **FoundationDB Cluster**: 3 coordinating nodes with data distribution
2. **Database Configuration**: Single SSD storage class for testing
3. **SeaweedFS Integration**: Automatic filer store configuration
4. **Volume Persistence**: Data persists between container restarts

### Test Configuration Files

- `filer.toml`: FoundationDB filer store configuration
- `s3.json`: S3 API credentials for end-to-end testing
- `Makefile`: Test automation and environment management

## Test Commands

### Setup Commands

```bash
make setup              # Full environment setup
make dev-fdb           # Just FoundationDB cluster
make install-deps      # Check dependencies
make check-env         # Validate configuration
```

### Test Commands

```bash
make test              # All tests
make test-unit         # Go unit tests
make test-integration  # Integration tests
make test-e2e         # End-to-end S3 tests
make test-crud        # Basic CRUD operations
make test-concurrent  # Concurrency tests
make test-benchmark   # Performance benchmarks
```

### Debug Commands

```bash
make status           # Show service status
make logs             # Show all logs
make logs-fdb         # FoundationDB logs only
make logs-seaweedfs   # SeaweedFS logs only
make debug            # Debug information
```

### Cleanup Commands

```bash
make clean            # Stop services and cleanup
```

## Test Data

Tests use isolated directory prefixes to avoid conflicts:

- **Unit tests**: `seaweedfs_test`
- **Integration tests**: `seaweedfs_test`
- **Concurrent tests**: `seaweedfs_concurrent_test_<timestamp>`
- **E2E tests**: `seaweedfs` (default)

## Expected Test Results

### Performance Expectations

Based on FoundationDB characteristics:
- **Single operations**: < 10ms latency
- **Batch operations**: High throughput with transactions
- **Concurrent operations**: Linear scaling with multiple clients
- **Directory listings**: Efficient range scans

### Reliability Expectations

- **ACID compliance**: All operations are atomic and consistent
- **Fault tolerance**: Automatic recovery from node failures
- **Concurrency**: No data corruption under concurrent load
- **Durability**: Data persists across restarts

## Troubleshooting

### Common Issues

1. **FoundationDB Connection Errors**
   ```bash
   # Check cluster status
   make status
   
   # Verify cluster file
   docker-compose exec fdb-init cat /var/fdb/config/fdb.cluster
   ```

2. **Test Failures**
   ```bash
   # Check service logs
   make logs-fdb
   make logs-seaweedfs
   
   # Run with verbose output
   go test -v -tags foundationdb ./...
   ```

3. **Performance Issues**
   ```bash
   # Check cluster health
   docker-compose exec fdb-init fdbcli --exec 'status details'
   
   # Monitor resource usage
   docker stats
   ```

4. **Docker Issues**
   ```bash
   # Clean Docker state
   make clean
   docker system prune -f
   
   # Restart from scratch
   make setup
   ```

### Debug Mode

Enable verbose logging for detailed troubleshooting:

```bash
# SeaweedFS debug logs
WEED_FILER_OPTIONS_V=2 make test

# FoundationDB debug logs (in fdbcli)
configure new single ssd; status details
```

### Manual Testing

For manual verification:

```bash
# Start environment
make dev-fdb

# Connect to FoundationDB
docker-compose exec fdb-init fdbcli

# FDB commands:
# status                    - Show cluster status  
# getrange "" \xFF          - Show all keys
# getrange seaweedfs seaweedfs\xFF  - Show SeaweedFS keys
```

## CI Integration

For continuous integration:

```bash
# CI test suite
make ci-test    # Unit + integration tests
make ci-e2e     # Full end-to-end test suite
```

The tests are designed to be reliable in CI environments with:
- Automatic service startup and health checking
- Timeout handling for slow CI systems
- Proper cleanup and resource management
- Detailed error reporting and logs

## Performance Benchmarks

Run performance benchmarks:

```bash
make test-benchmark

# Sample expected results:
# BenchmarkFoundationDBStore_InsertEntry-8    1000    1.2ms per op
# BenchmarkFoundationDBStore_FindEntry-8      5000    0.5ms per op  
# BenchmarkFoundationDBStore_KvOperations-8   2000    0.8ms per op
```

## Contributing

When adding new tests:

1. Use the `//go:build foundationdb` build tag
2. Follow the existing test structure and naming
3. Include both success and error scenarios
4. Add appropriate cleanup and resource management
5. Update this README with new test descriptions
