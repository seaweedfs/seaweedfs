# Filer Group S3 Tests

These tests verify that S3 bucket operations work correctly when a filer group is configured.

## Background

When SeaweedFS is configured with a filer group (via `-filer.group` option), collections are named with the filer group prefix:

```
Collection name = {filerGroup}_{bucketName}
```

For example, with filer group `mygroup` and bucket `mybucket`, the collection will be named `mygroup_mybucket`.

## Issue Being Tested

This test suite was created to verify the fix for a bug where:
- The admin UI was using just the bucket name when deleting collections
- This caused collection deletion to fail when a filer group was configured
- After bucket deletion via admin UI, the collection data would be orphaned

## Running the Tests

### Prerequisites

1. SeaweedFS servers must be running with a filer group configured
2. The S3 gateway must be accessible
3. Master server must be accessible for collection verification

### Quick Start

```bash
# Set environment variables
export FILER_GROUP=testgroup
export S3_ENDPOINT=http://localhost:8333
export MASTER_ADDRESS=localhost:9333

# Run tests
go test -v ./...
```

### Using the Makefile

```bash
# Start test servers with filer group configured
make start-servers FILER_GROUP=testgroup

# Run tests
make test

# Stop servers
make stop-servers

# Or run full test cycle
make full-test
```

### Configuration

Tests can be configured via:

1. Environment variables:
   - `FILER_GROUP`: The filer group name (required for tests to run)
   - `S3_ENDPOINT`: S3 API endpoint (default: http://localhost:8333)
   - `MASTER_ADDRESS`: Master server address (default: localhost:9333)

2. `test_config.json` file

## Test Cases

### TestFilerGroupCollectionNaming
Verifies that when a bucket is created and objects are uploaded:
1. The collection is created with the correct filer group prefix
2. Bucket deletion removes the correctly-named collection

### TestBucketDeletionWithFilerGroup
Specifically tests that bucket deletion via S3 API correctly deletes
the collection when filer group is configured.

### TestMultipleBucketsWithFilerGroup
Tests creating and deleting multiple buckets to ensure the filer group
prefix is correctly applied and removed for all buckets.

## Expected Behavior

With filer group `testgroup`:

| Bucket Name | Expected Collection Name |
|-------------|-------------------------|
| mybucket    | testgroup_mybucket      |
| test-123    | testgroup_test-123      |

Without filer group:

| Bucket Name | Expected Collection Name |
|-------------|-------------------------|
| mybucket    | mybucket                |
| test-123    | test-123                |

