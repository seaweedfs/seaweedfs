# SeaweedFS S3 Object Retention Tests

This directory contains comprehensive tests for SeaweedFS S3 Object Retention functionality, including Object Lock, Legal Hold, and WORM (Write Once Read Many) capabilities.

## Overview

The test suite validates AWS S3-compatible object retention features including:

- **Object Retention**: GOVERNANCE and COMPLIANCE modes with retain-until-date
- **Legal Hold**: Independent protection that can be applied/removed
- **Object Lock Configuration**: Bucket-level default retention policies  
- **WORM Integration**: Compatibility with legacy WORM functionality
- **Version-specific Retention**: Different retention policies per object version
- **Enforcement**: Protection against deletion and overwriting

## Test Files

- `s3_retention_test.go` - Core retention functionality tests
- `s3_worm_integration_test.go` - WORM integration and advanced scenarios
- `test_config.json` - Test configuration (endpoints, credentials)
- `Makefile` - Comprehensive test automation
- `go.mod` - Go module dependencies

## Prerequisites

- Go 1.21 or later
- SeaweedFS binary built (`make build-weed`)
- AWS SDK Go v2
- Testify testing framework

## Quick Start

### 1. Build and Start Server
```bash
# Build SeaweedFS and start test server
make start-server
```

### 2. Run Tests
```bash
# Run core retention tests
make test-retention-quick

# Run all retention tests
make test-retention

# Run WORM integration tests  
make test-retention-worm

# Run all tests with managed server
make test-with-server
```

### 3. Cleanup
```bash
make clean
```

## Test Categories

### Core Retention Tests
- `TestBasicRetentionWorkflow` - Basic GOVERNANCE mode retention
- `TestRetentionModeCompliance` - COMPLIANCE mode (immutable)
- `TestLegalHoldWorkflow` - Legal hold on/off functionality
- `TestObjectLockConfiguration` - Bucket object lock settings

### Advanced Tests
- `TestRetentionWithVersions` - Version-specific retention policies
- `TestRetentionAndLegalHoldCombination` - Multiple protection types
- `TestExpiredRetention` - Post-expiration behavior
- `TestRetentionErrorCases` - Error handling and edge cases

### WORM Integration Tests
- `TestWORMRetentionIntegration` - New retention + legacy WORM
- `TestWORMLegacyCompatibility` - Backward compatibility
- `TestRetentionOverwriteProtection` - Prevent overwrites
- `TestRetentionBulkOperations` - Bulk delete with retention
- `TestRetentionWithMultipartUpload` - Multipart upload retention
- `TestRetentionExtendedAttributes` - Extended attribute storage
- `TestRetentionBucketDefaults` - Default retention application
- `TestRetentionConcurrentOperations` - Concurrent operation safety

## Individual Test Targets

Run specific test categories:

```bash
# Basic functionality
make test-basic-retention
make test-compliance-retention  
make test-legal-hold

# Advanced features
make test-retention-versions
make test-retention-combination
make test-expired-retention

# WORM integration
make test-worm-integration
make test-worm-legacy
make test-retention-bulk
```

## Configuration

### Server Configuration
The tests use these default settings:
- S3 Port: 8333
- Test timeout: 15 minutes
- Volume directory: `./test-volume-data`

### Test Configuration (`test_config.json`)
```json
{
  "endpoint": "http://localhost:8333",
  "access_key": "some_access_key1", 
  "secret_key": "some_secret_key1",
  "region": "us-east-1",
  "bucket_prefix": "test-retention-",
  "use_ssl": false,
  "skip_verify_ssl": true
}
```

## Expected Behavior

### GOVERNANCE Mode
- Objects protected until retain-until-date
- Can be bypassed with `x-amz-bypass-governance-retention` header
- Supports time extension (not reduction)

### COMPLIANCE Mode  
- Objects immutably protected until retain-until-date
- Cannot be bypassed or shortened
- Strictest protection level

### Legal Hold
- Independent ON/OFF protection
- Can coexist with retention policies
- Must be explicitly removed to allow deletion

### Version Support
- Each object version can have individual retention
- Applies to both versioned and non-versioned buckets
- Version-specific retention retrieval

## Development

### Running in Development Mode
```bash
# Start server for development
make dev-start

# Run quick test
make dev-test
```

### Code Quality
```bash
# Format code
make fmt

# Run linter
make lint

# Generate coverage report
make coverage
```

### Performance Testing
```bash
# Run benchmarks
make benchmark-retention
```

## Troubleshooting

### Server Won't Start
```bash
# Check if port is in use
netstat -tlnp | grep 8333

# View server logs
make logs

# Force cleanup
make clean
```

### Test Failures
```bash
# Run with verbose output
go test -v -timeout=15m .

# Run specific test
go test -v -run TestBasicRetentionWorkflow .

# Check server health
make health-check
```

### Dependencies
```bash
# Install/update dependencies
make install-deps

# Check dependency status
make check-deps
```

## Integration with SeaweedFS

These tests validate the retention implementation in:
- `weed/s3api/s3api_object_retention.go` - Core retention logic
- `weed/s3api/s3api_object_handlers_retention.go` - HTTP handlers
- `weed/s3api/s3_constants/extend_key.go` - Extended attribute keys
- `weed/s3api/s3err/s3api_errors.go` - Error definitions
- `weed/s3api/s3api_object_handlers_delete.go` - Deletion enforcement
- `weed/s3api/s3api_object_handlers_put.go` - Upload enforcement

## AWS CLI Compatibility

The retention implementation supports standard AWS CLI commands:

```bash
# Set object retention
aws s3api put-object-retention \
  --bucket mybucket \
  --key myobject \
  --retention Mode=GOVERNANCE,RetainUntilDate=2024-12-31T23:59:59Z

# Get object retention
aws s3api get-object-retention \
  --bucket mybucket \
  --key myobject

# Set legal hold
aws s3api put-object-legal-hold \
  --bucket mybucket \
  --key myobject \
  --legal-hold Status=ON

# Configure bucket object lock
aws s3api put-object-lock-configuration \
  --bucket mybucket \
  --object-lock-configuration ObjectLockEnabled=Enabled,Rule='{DefaultRetention={Mode=GOVERNANCE,Days=30}}'
```

## Contributing

When adding new retention tests:

1. Follow existing test patterns
2. Use descriptive test names
3. Include both positive and negative test cases
4. Test error conditions
5. Update this README with new test descriptions
6. Add appropriate Makefile targets for new test categories

## References

- [AWS S3 Object Lock Documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)
- [AWS S3 API Reference - Object Retention](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html)
- [SeaweedFS S3 API Documentation](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API) 