# S3 SSE-C (Server-Side Encryption with Customer-provided Keys) Tests

This directory contains comprehensive integration tests for SeaweedFS's S3 SSE-C implementation, validating compatibility with AWS S3's SSE-C encryption standard.

## 🔐 What is SSE-C?

SSE-C (Server-Side Encryption with Customer-provided Keys) is an AWS S3 encryption method where:
- **Customer provides** the encryption key via HTTP headers
- **Server encrypts/decrypts** data using the provided key
- **Customer is responsible** for key management and storage
- **Keys are never stored** on the server side

## 📋 Test Coverage

### Core SSE-C Tests
- ✅ **Basic Encryption/Decryption** - PUT and GET with SSE-C headers
- ✅ **HEAD Object** - Metadata retrieval for encrypted objects  
- ✅ **Wrong Key Scenarios** - Error handling for incorrect keys
- ✅ **Missing Key Scenarios** - Error handling for missing keys
- ✅ **Large Object Support** - Streaming encryption for large files
- ✅ **Copy Operations** - Smart fast/slow path copy optimization
- ✅ **Round-trip Integrity** - Data consistency validation

### SSE-C Copy Operations
- ✅ **Same Key Copy** (Fast Path) - Direct chunk copy optimization
- ✅ **Different Key Copy** (Slow Path) - Decrypt and re-encrypt  
- ✅ **SSE-C to Unencrypted** - Remove encryption during copy
- ✅ **Unencrypted to SSE-C** - Add encryption during copy
- ✅ **Error Handling** - Invalid keys, missing keys, etc.

### Error Scenarios
- ❌ **Invalid Algorithm** - Non-AES256 algorithms
- ❌ **Invalid Key Format** - Malformed base64 keys
- ❌ **Key MD5 Mismatch** - Corrupted key validation
- ❌ **Missing Headers** - Incomplete SSE-C headers
- ❌ **Unnecessary Headers** - SSE-C headers for unencrypted objects

## 🚀 Quick Start

### Run All Tests with Server Management
```bash
make test-with-server
```

### Run Quick Core Tests Only  
```bash
make test-sse-c-quick
```

### Run Specific Test Pattern
```bash
make test-with-server TEST_PATTERN="TestSSECBasicEncryption|TestSSECCopyOperations"
```

## 📖 Available Commands

### Test Execution
```bash
make test-with-server          # Full test cycle with automatic server management
make test-sse-c               # Run all SSE-C tests (requires running server)
make test-sse-c-quick         # Run core tests only (faster)
make test-sse-c-comprehensive # Run all tests including edge cases
```

### Server Management
```bash
make start-server             # Start SeaweedFS server for testing
make stop-server              # Stop SeaweedFS server
make logs                     # View server logs
```

### Development & Debugging
```bash
make build-weed               # Build SeaweedFS binary
make check-deps               # Check dependencies
make benchmark                # Run performance benchmarks
make clean                    # Clean up all test artifacts
```

## ⚙️ Configuration

### Default Settings
```bash
S3_PORT=8333          # S3 API endpoint port
TEST_TIMEOUT=10m      # Maximum test execution time
TEST_PATTERN=TestSSEC # Test name pattern filter
```

### Environment Variables
```bash
# Override S3 port
S3_PORT=8334 make test-with-server

# Run specific test pattern
TEST_PATTERN="TestSSECCopy" make test-sse-c

# Increase timeout for slow systems
TEST_TIMEOUT=15m make test-with-server
```

## 🏗️ Server Configuration

The tests automatically start SeaweedFS with optimized settings:

- **S3 Port**: 8333 (configurable via `S3_PORT`)
- **Master Port**: 9333
- **Volume Port**: 8080  
- **Filer Port**: 8888
- **Data Directory**: `./test-volume-data` (auto-created)
- **Log File**: `weed-test.log`
- **Max Volume Size**: 100MB (for faster testing)
- **Max Volumes**: 100

## 📊 Test Results

### Successful Test Output
```
=== RUN   TestSSECBasicEncryption
--- PASS: TestSSECBasicEncryption (2.45s)
=== RUN   TestSSECHeadObject  
--- PASS: TestSSECHeadObject (0.12s)
=== RUN   TestSSECCopyOperations
--- PASS: TestSSECCopyOperations (1.88s)
PASS
✅ All SSE-C tests completed
```

### Performance Benchmarks
```bash
make benchmark
# BenchmarkSSECEncryption-8    1000   1234567 ns/op   1024 B/op   8 allocs/op
# BenchmarkSSECDecryption-8    1000   1234567 ns/op   1024 B/op   8 allocs/op
```

## 🔍 Debugging Test Failures

### View Server Logs
```bash
make logs
# Shows complete server output including errors
```

### Run Individual Tests
```bash
# Run only basic encryption test
go test -v -run TestSSECBasicEncryption .

# Run with detailed output
go test -v -run TestSSECBasicEncryption . -args -test.v
```

### Manual Server Testing
```bash
# Start server manually
make start-server

# Test with curl
curl -H "X-Amz-Server-Side-Encryption-Customer-Algorithm: AES256" \
     -H "X-Amz-Server-Side-Encryption-Customer-Key: $(echo -n 'my32charactersecretkey123456' | base64)" \
     -H "X-Amz-Server-Side-Encryption-Customer-Key-MD5: $(echo -n 'my32charactersecretkey123456' | md5sum | cut -d' ' -f1)" \
     -X PUT http://localhost:8333/test-bucket/encrypted-object \
     -d "test data"

# Stop server when done
make stop-server
```

## 🧪 Test Architecture

### Test Structure
```
test/s3/encryption/
├── s3_sse_c_test.go           # Core SSE-C functionality tests
├── s3_sse_c_copy_test.go      # Copy operation tests  
├── common_test.go             # Shared test utilities
├── go.mod                     # Go module dependencies
├── Makefile                   # Test automation
└── README.md                  # This documentation
```

### Key Test Functions
- `TestSSECBasicEncryption` - PUT/GET with encryption
- `TestSSECHeadObject` - HEAD requests for encrypted objects
- `TestSSECCopyOperations` - Copy with same/different keys
- `TestSSECWrongKey` - Error handling for incorrect keys
- `TestSSECLargeObject` - Large file encryption (100KB+)
- `TestSSECRoundTripIntegrity` - Data integrity validation

## ⚡ CI/CD Integration

These tests are automatically run in GitHub Actions:

### Python s3-tests Workflow
- **File**: `.github/workflows/s3tests-sse-c.yml`
- **Runs**: Basic object operations with SSE-C support
- **Coverage**: Core S3 compatibility validation

### Go Integration Tests Workflow  
- **File**: `.github/workflows/s3-go-tests.yml`
- **Jobs**: `s3-sse-c-tests`, `s3-sse-c-compatibility`
- **Matrix**: Quick and comprehensive test runs
- **Coverage**: Full SSE-C feature validation

### Test Triggers
- ✅ Pull Requests to `master` branch
- ✅ Pushes to `master` branch  
- ✅ Manual workflow dispatch

## 🎯 Compatibility

### AWS S3 Compatibility
- ✅ **Headers**: All standard SSE-C headers supported
- ✅ **Algorithms**: AES-256-CTR encryption 
- ✅ **Key Format**: Base64-encoded 32-byte keys
- ✅ **MD5 Validation**: Key integrity checking
- ✅ **Error Codes**: AWS-compatible error responses
- ✅ **Copy Operations**: Server-side copying with key changes

### SeaweedFS Features
- ✅ **Streaming**: Large file support without memory limits
- ✅ **Metadata**: Filer integration for encryption metadata
- ✅ **Performance**: Optimized fast-path copy operations
- ✅ **Chunking**: Works with SeaweedFS chunked storage

## 🔮 Future Enhancements

- **Range Requests** - Optimized partial content retrieval
- **Multipart Uploads** - Each part with separate IV
- **Performance Optimization** - Hardware acceleration support
- **Additional Algorithms** - Support for other encryption methods

## 📚 References

- [AWS S3 SSE-C Documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerProvidedKeys.html)
- [SeaweedFS S3 API Documentation](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API)
- [SSE-C Implementation Wiki](../../seaweedfs.wiki/Server-Side-Encryption-SSE-C.md)