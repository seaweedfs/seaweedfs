# SeaweedFS S3 Copying Tests

This directory contains comprehensive Go tests for SeaweedFS S3 copying functionality, converted from the failing Python tests in the s3-tests repository.

## Overview

These tests verify that SeaweedFS correctly implements S3 operations, starting with basic put/get operations and progressing to advanced copy operations, including:
- **Basic S3 Operations**: Put/Get operations, bucket management, and metadata handling
- **Basic object copying**: within the same bucket
- **Cross-bucket copying**: across different buckets
- **Multipart copy operations**: for large files
- **Conditional copy operations**: ETag-based conditional copying
- **Metadata handling**: during copy operations
- **ACL handling**: during copy operations

## Test Coverage

### Basic S3 Operations (Run First)
- **TestBasicPutGet**: Tests fundamental S3 put/get operations with various object types
- **TestBasicBucketOperations**: Tests bucket creation, listing, and deletion
- **TestBasicLargeObject**: Tests handling of larger objects (up to 10MB)

### Basic Copy Operations
- **TestObjectCopySameBucket**: Tests copying objects within the same bucket
- **TestObjectCopyDiffBucket**: Tests copying objects to different buckets
- **TestObjectCopyCannedAcl**: Tests copying with ACL settings
- **TestObjectCopyRetainingMetadata**: Tests metadata preservation during copy

### Multipart Copy Operations
- **TestMultipartCopySmall**: Tests multipart copying of small files
- **TestMultipartCopyWithoutRange**: Tests multipart copying without range specification
- **TestMultipartCopySpecialNames**: Tests multipart copying with special character names
- **TestMultipartCopyMultipleSizes**: Tests multipart copying with various file sizes

### Conditional Copy Operations
- **TestCopyObjectIfMatchGood**: Tests copying with matching ETag condition
- **TestCopyObjectIfMatchFailed**: Tests copying with non-matching ETag condition (should fail)
- **TestCopyObjectIfNoneMatchFailed**: Tests copying with non-matching ETag condition (should succeed)
- **TestCopyObjectIfNoneMatchGood**: Tests copying with matching ETag condition (should fail)

## Requirements

1. **Go 1.19+**: Required for AWS SDK v2 and modern Go features
2. **SeaweedFS Binary**: Built from source (`../../../weed/weed`)
3. **Free Ports**: 8333 (S3), 8888 (Filer), 8080 (Volume), 9333 (Master)
4. **Dependencies**: Uses the main repository's go.mod with existing AWS SDK v2 and testify dependencies

## Quick Start

### 1. Build SeaweedFS
```bash
cd ../../../
make
```

### 2. Run Tests
```bash
# Run basic S3 operations first (recommended)
make test-basic

# Run all tests (starts with basic, then copy tests)
make test

# Run quick tests only
make test-quick

# Run multipart tests only
make test-multipart

# Run conditional tests only
make test-conditional
```

## Available Make Targets

### Basic Test Execution
- `make test-basic` - Run basic S3 put/get operations (recommended first)
- `make test` - Run all S3 tests (starts with basic, then copying)
- `make test-quick` - Run quick tests only (basic copying)
- `make test-full` - Run full test suite including large files
- `make test-multipart` - Run multipart copying tests only
- `make test-conditional` - Run conditional copying tests only

### Server Management
- `make start-seaweedfs` - Start SeaweedFS server for testing
- `make stop-seaweedfs` - Stop SeaweedFS server
- `make manual-start` - Start server for manual testing
- `make manual-stop` - Stop server and clean up

### Debugging
- `make debug-logs` - Show recent log entries from all services
- `make debug-status` - Show process and port status
- `make check-binary` - Verify SeaweedFS binary exists

### Performance Testing
- `make benchmark` - Run performance benchmarks
- `make stress` - Run stress tests with multiple iterations
- `make perf` - Run performance tests with large files

### Cleanup
- `make clean` - Clean up test artifacts and temporary files

## Configuration

The tests use the following default configuration:

```json
{
  "endpoint": "http://localhost:8333",
  "access_key": "some_access_key1",
  "secret_key": "some_secret_key1",
  "region": "us-east-1",
  "bucket_prefix": "test-copying-",
  "use_ssl": false,
  "skip_verify_ssl": true
}
```

You can modify these values in `test_config.json` or by setting environment variables:

```bash
export SEAWEEDFS_BINARY=/path/to/weed
export S3_PORT=8333
export FILER_PORT=8888
export VOLUME_PORT=8080
export MASTER_PORT=9333
export TEST_TIMEOUT=10m
export VOLUME_MAX_SIZE_MB=50
```

**Note**: The volume size limit is set to 50MB to ensure proper testing of volume boundaries and multipart operations.

## Test Details

### TestBasicPutGet
- Tests fundamental S3 put/get operations with various object types:
  - Simple text objects
  - Empty objects
  - Binary objects (1KB random data)
  - Objects with metadata and content-type
- Verifies ETag consistency between put and get operations
- Tests metadata preservation

### TestBasicBucketOperations
- Tests bucket creation and existence verification
- Tests object listing in buckets
- Tests object creation and listing with directory-like prefixes
- Tests bucket deletion and cleanup
- Verifies proper error handling for operations on non-existent buckets

### TestBasicLargeObject
- Tests handling of progressively larger objects:
  - 1KB, 10KB, 100KB, 1MB, 5MB, 10MB
- Verifies data integrity for large objects
- Tests memory handling and streaming for large files
- Ensures proper handling up to the 50MB volume limit

### TestObjectCopySameBucket
- Creates a bucket with a source object
- Copies the object to a different key within the same bucket
- Verifies the copied object has the same content

### TestObjectCopyDiffBucket
- Creates source and destination buckets
- Copies an object from source to destination bucket
- Verifies the copied object has the same content

### TestObjectCopyCannedAcl
- Tests copying with ACL settings (`public-read`)
- Tests metadata replacement during copy with ACL
- Verifies both basic copying and metadata handling

### TestObjectCopyRetainingMetadata
- Tests with different file sizes (3 bytes, 1MB)
- Verifies metadata and content-type preservation
- Checks that all metadata is correctly copied

### TestMultipartCopySmall
- Tests multipart copy with 1-byte files
- Uses range-based copying (`bytes=0-0`)
- Verifies multipart upload completion

### TestMultipartCopyWithoutRange
- Tests multipart copy without specifying range
- Should copy entire source object
- Verifies correct content length and data

### TestMultipartCopySpecialNames
- Tests with special character names: `" "`, `"_"`, `"__"`, `"?versionId"`
- Verifies proper URL encoding and handling
- Each special name is tested in isolation

### TestMultipartCopyMultipleSizes
- Tests with various copy sizes:
  - 5MB (single part)
  - 5MB + 100KB (multi-part)
  - 5MB + 600KB (multi-part)
  - 10MB + 100KB (multi-part)
  - 10MB + 600KB (multi-part)
  - 10MB (exact multi-part boundary)
- Uses 5MB part size for all copies
- Verifies data integrity across all sizes

### TestCopyObjectIfMatchGood
- Tests conditional copy with matching ETag
- Should succeed when ETag matches
- Verifies successful copy operation

### TestCopyObjectIfMatchFailed
- Tests conditional copy with non-matching ETag
- Should fail with precondition error
- Verifies proper error handling

### TestCopyObjectIfNoneMatchFailed
- Tests conditional copy with non-matching ETag for IfNoneMatch
- Should succeed when ETag doesn't match
- Verifies successful copy operation

### TestCopyObjectIfNoneMatchGood
- Tests conditional copy with matching ETag for IfNoneMatch
- Should fail with precondition error
- Verifies proper error handling

## Expected Behavior

These tests verify that SeaweedFS correctly implements:

1. **Basic S3 Operations**: Standard `PutObject`, `GetObject`, `ListBuckets`, `ListObjects` APIs
2. **Bucket Management**: Bucket creation, deletion, and listing
3. **Object Storage**: Binary and text data storage with metadata
4. **Large Object Handling**: Efficient storage and retrieval of large files
5. **Basic S3 Copy Operations**: Standard `CopyObject` API
6. **Multipart Copy Operations**: `UploadPartCopy` API with range support
7. **Conditional Operations**: ETag-based conditional copying
8. **Metadata Handling**: Proper metadata preservation and replacement
9. **ACL Handling**: Access control list management during copy
10. **Error Handling**: Proper error responses for invalid operations

## Troubleshooting

### Common Issues

1. **Port Already in Use**
   ```bash
   make stop-seaweedfs
   make clean
   ```

2. **SeaweedFS Binary Not Found**
   ```bash
   cd ../../../
   make
   ```

3. **Test Timeouts**
   ```bash
   export TEST_TIMEOUT=30m
   make test
   ```

4. **Permission Denied**
   ```bash
   sudo make clean
   ```

### Debug Information

```bash
# Check server status
make debug-status

# View recent logs
make debug-logs

# Manual server start for investigation
make manual-start
# ... perform manual testing ...
make manual-stop
```

### Log Locations

When running tests, logs are stored in:
- Master: `/tmp/seaweedfs-master.log`
- Volume: `/tmp/seaweedfs-volume.log`
- Filer: `/tmp/seaweedfs-filer.log`
- S3: `/tmp/seaweedfs-s3.log`

## Contributing

When adding new tests:

1. Follow the existing naming convention (`TestXxxYyy`)
2. Use the helper functions for common operations
3. Add cleanup with `defer deleteBucket(t, client, bucketName)`
4. Include error checking with `require.NoError(t, err)`
5. Use assertions with `assert.Equal(t, expected, actual)`
6. Add the test to the appropriate Make target

## Performance Notes

- **TestMultipartCopyMultipleSizes** is the most resource-intensive test
- Large file tests may take several minutes to complete
- Memory usage scales with file sizes being tested
- Network latency affects multipart copy performance

## Integration with CI/CD

For automated testing:

```bash
# Basic validation (recommended first)
make test-basic

# Quick validation
make ci-test

# Full validation
make test-full

# Performance validation
make perf
```

The tests are designed to be self-contained and can run in containerized environments. 