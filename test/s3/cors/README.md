# CORS Integration Tests for SeaweedFS S3 API

This directory contains comprehensive integration tests for the CORS (Cross-Origin Resource Sharing) functionality in SeaweedFS S3 API.

## Overview

The CORS integration tests validate the complete CORS implementation including:
- CORS configuration management (PUT/GET/DELETE)
- CORS rule validation
- CORS middleware behavior
- Caching functionality
- Error handling
- Real-world CORS scenarios

## Prerequisites

1. **Go 1.19+**: For building SeaweedFS and running tests
2. **Network Access**: Tests use `localhost:8333` by default
3. **System Dependencies**: `curl` and `netstat` for health checks

## Quick Start

The tests now automatically start their own SeaweedFS server, so you don't need to manually start one.

### 1. Run All Tests with Managed Server

```bash
# Run all tests with automatic server management
make test-with-server

# Run core CORS tests only
make test-cors-quick

# Run comprehensive CORS tests
make test-cors-comprehensive
```

### 2. Manual Server Management

If you prefer to manage the server manually:

```bash
# Start server
make start-server

# Run tests (assuming server is running)
make test-cors-simple

# Stop server
make stop-server
```

### 3. Individual Test Categories

```bash
# Run specific test types
make test-basic-cors           # Basic CORS configuration
make test-preflight-cors       # Preflight OPTIONS requests
make test-actual-cors          # Actual CORS request handling
make test-origin-matching      # Origin matching logic
make test-header-matching      # Header matching logic
make test-method-matching      # Method matching logic
make test-multiple-rules       # Multiple CORS rules
make test-validation           # CORS validation
make test-caching              # CORS caching behavior
make test-error-handling       # Error handling
```

## Test Server Management

The tests use a comprehensive server management system similar to other SeaweedFS integration tests:

### Server Configuration

- **S3 Port**: 8333 (configurable via `S3_PORT`)
- **Master Port**: 9333
- **Volume Port**: 8080
- **Filer Port**: 8888
- **Metrics Port**: 9324
- **Data Directory**: `./test-volume-data` (auto-created)
- **Log File**: `weed-test.log`

### Server Lifecycle

1. **Build**: Automatically builds `../../../weed/weed_binary`
2. **Start**: Launches SeaweedFS with S3 API enabled
3. **Health Check**: Waits up to 90 seconds for server to be ready
4. **Test**: Runs the requested tests
5. **Stop**: Gracefully shuts down the server
6. **Cleanup**: Removes temporary files and data

### Available Commands

```bash
# Server management
make start-server              # Start SeaweedFS server
make stop-server               # Stop SeaweedFS server
make health-check              # Check server health
make logs                      # View server logs

# Test execution
make test-with-server          # Full test cycle with server management
make test-cors-simple          # Run tests without server management
make test-cors-quick           # Run core tests only
make test-cors-comprehensive   # Run all tests

# Development
make dev-start                 # Start server for development
make dev-test                  # Run development tests
make build-weed                # Build SeaweedFS binary
make check-deps                # Check dependencies

# Maintenance
make clean                     # Clean up all artifacts
make coverage                  # Generate coverage report
make fmt                       # Format code
make lint                      # Run linter
```

## Test Configuration

### Default Configuration

The tests use these default settings (configurable via environment variables):

```bash
WEED_BINARY=../../../weed/weed_binary
S3_PORT=8333
TEST_TIMEOUT=10m
TEST_PATTERN=TestCORS
```

### Configuration File

The `test_config.json` file contains S3 client configuration:

```json
{
  "endpoint": "http://localhost:8333",
  "access_key": "some_access_key1",
  "secret_key": "some_secret_key1",
  "region": "us-east-1",
  "bucket_prefix": "test-cors-",
  "use_ssl": false,
  "skip_verify_ssl": true
}
```

## Troubleshooting

### Compilation Issues

If you encounter compilation errors, the most common issues are:

1. **AWS SDK v2 Type Mismatches**: The `MaxAgeSeconds` field in `types.CORSRule` expects `int32`, not `*int32`. Use direct values like `3600` instead of `aws.Int32(3600)`.

2. **Field Name Issues**: The `GetBucketCorsOutput` type has a `CORSRules` field directly, not a `CORSConfiguration` field.

Example fix:
```go
// ❌ Incorrect
MaxAgeSeconds: aws.Int32(3600),
assert.Len(t, getResp.CORSConfiguration.CORSRules, 1)

// ✅ Correct
MaxAgeSeconds: 3600,
assert.Len(t, getResp.CORSRules, 1)
```

### Server Issues

1. **Server Won't Start**
   ```bash
   # Check for port conflicts
   netstat -tlnp | grep 8333
   
   # View server logs
   make logs
   
   # Force cleanup
   make clean
   ```

2. **Test Failures**
   ```bash
   # Run with server management
   make test-with-server
   
   # Run specific test
   make test-basic-cors
   
   # Check server health
   make health-check
   ```

3. **Connection Issues**
   ```bash
   # Verify server is running
   curl -s http://localhost:8333
   
   # Check server logs
   tail -f weed-test.log
   ```

### Performance Issues

If tests are slow or timing out:

```bash
# Increase timeout
export TEST_TIMEOUT=30m
make test-with-server

# Run quick tests only
make test-cors-quick

# Check server resources
make debug-status
```

## Test Coverage

### Core Functionality Tests

#### 1. CORS Configuration Management (`TestCORSConfigurationManagement`)
- PUT CORS configuration
- GET CORS configuration
- DELETE CORS configuration
- Configuration updates
- Error handling for non-existent configurations

#### 2. Multiple CORS Rules (`TestCORSMultipleRules`)
- Multiple rules in single configuration
- Rule precedence and ordering
- Complex rule combinations

#### 3. CORS Validation (`TestCORSValidation`)
- Invalid HTTP methods
- Empty origins validation
- Negative MaxAge validation
- Rule limit validation

#### 4. Wildcard Support (`TestCORSWithWildcards`)
- Wildcard origins (`*`, `https://*.example.com`)
- Wildcard headers (`*`)
- Wildcard expose headers

#### 5. Rule Limits (`TestCORSRuleLimit`)
- Maximum 100 rules per configuration
- Rule limit enforcement
- Large configuration handling

#### 6. Error Handling (`TestCORSErrorHandling`)
- Non-existent bucket operations
- Invalid configurations
- Malformed requests

### HTTP-Level Tests

#### 1. Preflight Requests (`TestCORSPreflightRequest`)
- OPTIONS request handling
- CORS headers in preflight responses
- Access-Control-Request-Method validation
- Access-Control-Request-Headers validation

#### 2. Actual Requests (`TestCORSActualRequest`)
- CORS headers in actual responses
- Origin validation for real requests
- Proper expose headers handling

#### 3. Origin Matching (`TestCORSOriginMatching`)
- Exact origin matching
- Wildcard origin matching (`*`)
- Subdomain wildcard matching (`https://*.example.com`)
- Non-matching origins (should be rejected)

#### 4. Header Matching (`TestCORSHeaderMatching`)
- Wildcard header matching (`*`)
- Specific header matching
- Case-insensitive matching
- Disallowed headers

#### 5. Method Matching (`TestCORSMethodMatching`)
- Allowed methods verification
- Disallowed methods rejection
- Method-specific CORS behavior

#### 6. Multiple Rules (`TestCORSMultipleRulesMatching`)
- Rule precedence and selection
- Multiple rules with different configurations
- Complex rule interactions

### Integration Tests

#### 1. Caching (`TestCORSCaching`)
- CORS configuration caching
- Cache invalidation
- Cache performance

#### 2. Object Operations (`TestCORSObjectOperations`)
- CORS with actual S3 operations
- PUT/GET/DELETE objects with CORS
- CORS headers in object responses

#### 3. Without Configuration (`TestCORSWithoutConfiguration`)
- Behavior when no CORS configuration exists
- Default CORS behavior
- Graceful degradation

## Development

### Running Tests During Development

```bash
# Start server for development
make dev-start

# Run quick test
make dev-test

# View logs in real-time
make logs
```

### Adding New Tests

1. Follow the existing naming convention (`TestCORSXxxYyy`)
2. Use the helper functions (`getS3Client`, `createTestBucket`, etc.)
3. Add cleanup with `defer cleanupTestBucket(t, client, bucketName)`
4. Include proper error checking with `require.NoError(t, err)`
5. Use assertions with `assert.Equal(t, expected, actual)`
6. Add the test to the appropriate Makefile target

### Code Quality

```bash
# Format code
make fmt

# Run linter
make lint

# Generate coverage report
make coverage
```

## Performance Notes

- Tests create and destroy buckets for each test case
- Large configuration tests may take several minutes
- Server startup typically takes 15-30 seconds
- Tests run in parallel where possible for efficiency

## Integration with SeaweedFS

These tests validate the CORS implementation in:
- `weed/s3api/cors/` - Core CORS package
- `weed/s3api/s3api_bucket_cors_handlers.go` - HTTP handlers
- `weed/s3api/s3api_server.go` - Router integration
- `weed/s3api/s3api_bucket_config.go` - Configuration management

The tests ensure AWS S3 API compatibility and proper CORS behavior across all supported scenarios. 