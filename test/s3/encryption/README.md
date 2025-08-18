# SSE-C (Server-Side Encryption with Customer-provided keys) Integration Tests

This directory contains comprehensive integration tests for SeaweedFS's SSE-C implementation.

## Overview

These tests verify that SeaweedFS correctly implements AWS S3-compatible SSE-C functionality:

- ✅ **Basic encryption/decryption** - Round-trip data integrity
- ✅ **Header validation** - Proper SSE-C header handling 
- ✅ **Error scenarios** - Wrong keys, missing keys, invalid headers
- ✅ **HEAD operations** - Metadata retrieval with SSE-C
- ✅ **Range requests** - Partial content with encryption
- ✅ **Large objects** - Performance with substantial data
- ✅ **Binary data** - Various data patterns and edge cases

## Test Structure

### Core Test Functions

- `TestSSECBasicEncryption` - Basic encrypt/decrypt workflow
- `TestSSECHeadObject` - HEAD operation with SSE-C headers
- `TestSSECWrongKey` - Security validation with incorrect keys
- `TestSSECMissingKey` - Error handling for missing encryption keys
- `TestSSECUnnecessaryKey` - Error handling when providing keys for unencrypted objects
- `TestSSECInvalidHeaders` - Validation of malformed SSE-C headers
- `TestSSECLargeObject` - Performance testing with 1MB objects
- `TestSSECPartialContent` - Range requests on encrypted objects
- `TestSSECRoundTripIntegrity` - Data integrity across various patterns

### Test Data Patterns

The tests cover various data types to ensure robust encryption:
- Text data (ASCII, UTF-8, special characters)
- Binary data (null bytes, 0xFF patterns)
- Large objects (1MB+ for performance testing)
- Empty objects (edge case handling)
- Random data patterns

## Running Tests

### Prerequisites

1. **Build SeaweedFS** from the root directory:
   ```bash
   make
   ```

2. **Ensure ports are available**:
   - S3 API: 8333 (configurable)
   - Filer: 8888
   - Master: 9333
   - Volume: 8080

### Quick Start

```bash
# Run all tests (includes setup and cleanup)
make test

# Manual setup for debugging
make setup

# Clean up after tests
make clean

# Run performance benchmarks
make benchmark
```

### Manual Testing

If you prefer to manage SeaweedFS manually:

```bash
# Start SeaweedFS (from project root)
./weed master &
./weed volume -mserver=localhost:9333 -dir=/tmp/vol1 &
./weed filer -master=localhost:9333 &
./weed s3 -filer=localhost:8888 -port=8333 &

# Run tests
cd test/s3/encryption
go test -v ./...
```

## Test Configuration

The tests use these SSE-C test vectors (matching s3tests):

```go
const (
    testSSEKey    = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs="
    testSSEKeyMD5 = "0d6ca09c746d82227bec70a6fb5aef1f"
)
```

Configuration can be modified in the test files or via environment variables.

## Expected Behavior

### Successful Operations
- PUT/GET with matching SSE-C headers ✅
- HEAD with correct SSE-C headers ✅ 
- Range requests on encrypted objects ✅
- Large object encryption/decryption ✅

### Expected Failures
- GET/HEAD without SSE-C headers on encrypted objects ❌
- GET/HEAD with wrong SSE-C key ❌
- PUT with invalid SSE-C headers (algorithm, key format, MD5) ❌
- GET/HEAD with SSE-C headers on unencrypted objects ❌

## Performance Expectations

The SSE-C implementation should have minimal performance impact:
- **Encryption overhead**: < 5% CPU increase
- **Memory usage**: Constant (streaming encryption)
- **Storage overhead**: +16 bytes per object (IV)
- **Throughput**: > 95% of unencrypted performance

## Debugging

### Common Issues

1. **Connection refused**: Ensure SeaweedFS is running
2. **Invalid credentials**: Check s3tests.conf configuration
3. **Port conflicts**: Verify ports 8333, 8888, 9333, 8080 are available
4. **Permission errors**: Ensure write access to test-volume-data/

### Logs

Test logs are available in:
- `master.log` - Master server logs
- `volume.log` - Volume server logs  
- `filer.log` - Filer server logs
- `s3.log` - S3 API server logs

### Verbose Testing

```bash
go test -v -timeout 60s ./... 
```

## Integration with s3tests

These tests complement the external s3tests suite by providing:
- SeaweedFS-specific test scenarios
- Performance benchmarking
- Easy local development workflow
- Detailed error case coverage

For comprehensive S3 compatibility testing, also run:
```bash
cd test/s3/compatibility
make test
```

## Contributing

When adding new SSE-C features:

1. Add corresponding unit tests in `weed/s3api/s3_sse_c_test.go`
2. Add integration tests here for end-to-end validation
3. Update this README with new test descriptions
4. Ensure all existing tests continue to pass

## Security Note

These tests use hardcoded keys for reproducibility. **Never use these keys in production!**

Production SSE-C keys should be:
- Randomly generated 256-bit AES keys
- Securely stored and managed by clients
- Rotated regularly per security policies
