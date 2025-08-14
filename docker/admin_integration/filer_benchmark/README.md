# Filer Benchmark Tool

A simple Go program to benchmark SeaweedFS filer performance and detect race conditions with concurrent file operations.

## Overview

This tool creates 300 (configurable) goroutines that concurrently:
1. Create empty files on the filer
2. Add multiple chunks to each file (with fake file IDs)
3. Verify the file was created successfully

This simulates the race condition scenario from [Issue #7062](https://github.com/seaweedfs/seaweedfs/issues/7062) where concurrent operations can lead to metadata inconsistencies.

## Usage

### Build and Run Directly
```bash
# Build the tool
go build -o bin/filer_benchmark ./cmd/filer_benchmark/

# Basic usage (single filer)
./bin/filer_benchmark -filers=localhost:8888

# Test with multiple filers
./bin/filer_benchmark -filers=localhost:8888,localhost:8889,localhost:8890

# High concurrency race condition test
./bin/filer_benchmark -goroutines=500 -loops=200 -verbose
```

### Using Helper Scripts
```bash
# Use the wrapper script with predefined configurations
./scripts/run_filer_benchmark.sh

# Run example test suite
./examples/run_filer_race_test.sh
```

## Configuration Options

| Flag | Default | Description |
|------|---------|-------------|
| `-filers` | `localhost:8888` | Comma-separated list of filer addresses |
| `-goroutines` | `300` | Number of concurrent goroutines |
| `-loops` | `100` | Number of operations per goroutine |
| `-chunkSize` | `1048576` | Chunk size in bytes (1MB) |
| `-chunksPerFile` | `5` | Number of chunks per file |
| `-testDir` | `/benchmark` | Test directory on filer |
| `-verbose` | `false` | Enable verbose error logging |

## Race Condition Detection

The tool detects race conditions by monitoring for these error patterns:
- `leveldb: closed` - Metadata cache closed during operation
- `transport is closing` - gRPC connection closed during operation  
- `connection refused` - Network connectivity issues
- `not found after creation` - File disappeared after being created

## Example Output

```
============================================================
FILER BENCHMARK RESULTS
============================================================
Configuration:
  Filers: localhost:8888,localhost:8889,localhost:8890
  Goroutines: 300
  Loops per goroutine: 100
  Chunks per file: 5
  Chunk size: 1048576 bytes

Results:
  Total operations attempted: 30000
  Files successfully created: 29850
  Total chunks added: 149250
  Errors: 150
  Race condition errors: 23
  Success rate: 99.50%

Performance:
  Total duration: 45.2s
  Operations/second: 663.72
  Files/second: 660.18
  Chunks/second: 3300.88

Race Condition Analysis:
  Race condition rate: 0.0767%
  Race conditions detected: 23
  🟡 MODERATE race condition rate
  Overall error rate: 0.50%
============================================================
```

## Test Scenarios

### 1. Basic Functionality Test
```bash
./bin/filer_benchmark -goroutines=20 -loops=10
```
Low concurrency test to verify basic functionality.

### 2. Race Condition Reproduction
```bash
./bin/filer_benchmark -goroutines=500 -loops=100 -verbose
```
High concurrency test designed to trigger race conditions.

### 3. Multi-Filer Load Test
```bash
./bin/filer_benchmark -filers=filer1:8888,filer2:8888,filer3:8888 -goroutines=300
```
Distribute load across multiple filers.

### 4. Small Files Benchmark
```bash
./bin/filer_benchmark -chunkSize=4096 -chunksPerFile=1 -goroutines=1000
```
Test with many small files to stress metadata operations.

## How It Simulates Race Conditions

1. **Concurrent Operations**: Multiple goroutines perform file operations simultaneously
2. **Random Timing**: Small random delays create timing variations
3. **Fake Chunks**: Uses file IDs without actual volume server data to focus on metadata operations
4. **Verification Step**: Attempts to read files immediately after creation to catch race conditions
5. **Multiple Filers**: Distributes load randomly across multiple filer instances

## Prerequisites

- SeaweedFS master server running
- SeaweedFS filer server(s) running  
- Go 1.19+ for building
- Network connectivity to filer endpoints

## Integration with Issue #7062

This tool reproduces the core problem from the original issue:
- **Concurrent file operations** (simulated by goroutines)
- **Metadata race conditions** (detected through error patterns)
- **Transport disconnections** (monitored in error analysis)
- **File inconsistencies** (caught by verification steps)

The key difference is this tool focuses on the filer metadata layer rather than the full CSI driver + mount stack, making it easier to isolate and debug the race condition.
