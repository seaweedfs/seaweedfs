# S3 Server-Side Encryption (SSE) Integration Tests

This directory contains comprehensive integration tests for SeaweedFS S3 API Server-Side Encryption functionality. These tests validate the complete end-to-end encryption/decryption pipeline from S3 API requests through filer metadata storage.

## Overview

The SSE integration tests cover three main encryption methods:

- **SSE-C (Customer-Provided Keys)**: Client provides encryption keys via request headers
- **SSE-KMS (Key Management Service)**: Server manages encryption keys through a KMS provider
- **SSE-S3 (Server-Managed Keys)**: Server automatically manages encryption keys

### 🆕 Real KMS Integration

The tests now include **real KMS integration** with OpenBao, providing:
- ✅ Actual encryption/decryption operations (not mock keys)
- ✅ Multiple KMS keys for different security levels
- ✅ Per-bucket KMS configuration testing
- ✅ Performance benchmarking with real KMS operations

See [README_KMS.md](README_KMS.md) for detailed KMS integration documentation.

## Why Integration Tests Matter

These integration tests were created to address a **critical gap in test coverage** that previously existed. While the SeaweedFS codebase had comprehensive unit tests for SSE components, it lacked integration tests that validated the complete request flow:

```
Client Request → S3 API → Filer Storage → Metadata Persistence → Retrieval → Decryption
```

### The Bug These Tests Would Have Caught

A critical bug was discovered where:
- ✅ S3 API correctly encrypted data and sent metadata headers to the filer
- ❌ **Filer did not process SSE metadata headers**, losing all encryption metadata
- ❌ Objects could be encrypted but **never decrypted** (metadata was lost)

**Unit tests passed** because they tested components in isolation, but the **integration was broken**. These integration tests specifically validate that:

1. Encryption metadata is correctly sent to the filer
2. Filer properly processes and stores the metadata
3. Objects can be successfully retrieved and decrypted
4. Copy operations preserve encryption metadata
5. Multipart uploads maintain encryption consistency

## Test Structure

### Core Integration Tests

#### Basic Functionality
- `TestSSECIntegrationBasic` - Basic SSE-C PUT/GET cycle
- `TestSSEKMSIntegrationBasic` - Basic SSE-KMS PUT/GET cycle

#### Data Size Validation
- `TestSSECIntegrationVariousDataSizes` - SSE-C with various data sizes (0B to 1MB)
- `TestSSEKMSIntegrationVariousDataSizes` - SSE-KMS with various data sizes

#### Object Copy Operations
- `TestSSECObjectCopyIntegration` - SSE-C object copying (key rotation, encryption changes)
- `TestSSEKMSObjectCopyIntegration` - SSE-KMS object copying

#### Multipart Uploads
- `TestSSEMultipartUploadIntegration` - SSE multipart uploads for large objects

#### Error Conditions
- `TestSSEErrorConditions` - Invalid keys, malformed requests, error handling

### Performance Tests
- `BenchmarkSSECThroughput` - SSE-C performance benchmarking
- `BenchmarkSSEKMSThroughput` - SSE-KMS performance benchmarking

## Running Tests

### Prerequisites

1. **Build SeaweedFS**: Ensure the `weed` binary is built and available in PATH
   ```bash
   cd /path/to/seaweedfs
   make
   ```

2. **Dependencies**: Tests use AWS SDK Go v2 and testify - these are handled by Go modules

### Quick Test

Run basic SSE integration tests:
```bash
make test-basic
```

### Comprehensive Testing

Run all SSE integration tests:
```bash
make test
```

### Specific Test Categories

```bash
make test-ssec      # SSE-C tests only
make test-ssekms    # SSE-KMS tests only  
make test-copy      # Copy operation tests
make test-multipart # Multipart upload tests
make test-errors    # Error condition tests
```

### Performance Testing

```bash
make benchmark      # Performance benchmarks
make perf          # Various data size performance tests
```

### KMS Integration Testing

```bash
make setup-openbao          # Set up OpenBao KMS
make test-with-kms          # Run all SSE tests with real KMS
make test-ssekms-integration # Run SSE-KMS with OpenBao only
make clean-kms             # Clean up KMS environment
```

### Development Testing

```bash
make manual-start   # Start SeaweedFS for manual testing
# ... run manual tests ...
make manual-stop    # Stop and cleanup
```

## Test Configuration

### Default Configuration

The tests use these default settings:
- **S3 Endpoint**: `http://127.0.0.1:8333`
- **Access Key**: `some_access_key1`
- **Secret Key**: `some_secret_key1`
- **Region**: `us-east-1`
- **Bucket Prefix**: `test-sse-`

### Custom Configuration

Override defaults via environment variables:
```bash
S3_PORT=8444 FILER_PORT=8889 make test
```

### Test Environment

Each test run:
1. Starts a complete SeaweedFS cluster (master, volume, filer, s3)
2. Configures KMS support for SSE-KMS tests
3. Creates temporary buckets with unique names
4. Runs tests with real HTTP requests
5. Cleans up all test artifacts

## Test Data Coverage

### Data Sizes Tested
- **0 bytes**: Empty files (edge case)
- **1 byte**: Minimal data
- **16 bytes**: Single AES block
- **31 bytes**: Just under two blocks
- **32 bytes**: Exactly two blocks
- **100 bytes**: Small file
- **1 KB**: Small text file
- **8 KB**: Medium file
- **64 KB**: Large file
- **1 MB**: Very large file

### Encryption Key Scenarios
- **SSE-C**: Random 256-bit keys, key rotation, wrong keys
- **SSE-KMS**: Various key IDs, encryption contexts, bucket keys
- **Copy Operations**: Same key, different keys, encryption transitions

## Critical Test Scenarios

### Metadata Persistence Validation

The integration tests specifically validate scenarios that would catch metadata storage bugs:

```go
// 1. Upload with SSE-C
client.PutObject(..., SSECustomerKey: key)  // ← Metadata sent to filer

// 2. Retrieve with SSE-C  
client.GetObject(..., SSECustomerKey: key)  // ← Metadata retrieved from filer

// 3. Verify decryption works
assert.Equal(originalData, decryptedData)    // ← Would fail if metadata lost
```

### Content-Length Validation

Tests verify that Content-Length headers are correct, which would catch bugs related to IV handling:

```go
assert.Equal(int64(originalSize), resp.ContentLength)  // ← Would catch IV-in-stream bugs
```

## Debugging

### View Logs
```bash
make debug-logs     # Show recent log entries
make debug-status   # Show process and port status
```

### Manual Testing
```bash
make manual-start   # Start SeaweedFS
# Test with S3 clients, curl, etc.
make manual-stop    # Cleanup
```

## Integration Test Benefits

These integration tests provide:

1. **End-to-End Validation**: Complete request pipeline testing
2. **Metadata Persistence**: Validates filer storage/retrieval of encryption metadata
3. **Real Network Communication**: Uses actual HTTP requests and responses
4. **Production-Like Environment**: Full SeaweedFS cluster with all components
5. **Regression Protection**: Prevents critical integration bugs
6. **Performance Baselines**: Benchmarking for performance monitoring

## Continuous Integration

For CI/CD pipelines, use:
```bash
make ci-test        # Quick tests suitable for CI
make stress         # Stress testing for stability validation
```

## Key Differences from Unit Tests

| Aspect | Unit Tests | Integration Tests |
|--------|------------|------------------|
| **Scope** | Individual functions | Complete request pipeline |
| **Dependencies** | Mocked/simulated | Real SeaweedFS cluster |
| **Network** | None | Real HTTP requests |
| **Storage** | In-memory | Real filer database |
| **Metadata** | Manual simulation | Actual storage/retrieval |
| **Speed** | Fast (milliseconds) | Slower (seconds) |
| **Coverage** | Component logic | System integration |

## Conclusion

These integration tests ensure that SeaweedFS SSE functionality works correctly in production-like environments. They complement the existing unit tests by validating that all components work together properly, providing confidence that encryption/decryption operations will succeed for real users.

**Most importantly**, these tests would have immediately caught the critical filer metadata storage bug that was previously undetected, demonstrating the crucial importance of integration testing for distributed systems.
