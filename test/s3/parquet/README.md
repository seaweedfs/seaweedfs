# PyArrow Parquet S3 Compatibility Tests

This directory contains tests for PyArrow Parquet compatibility with SeaweedFS S3 API, including the implicit directory detection fix.

## Overview

**Status**: ✅ **All PyArrow methods work correctly with SeaweedFS**

SeaweedFS implements implicit directory detection to improve compatibility with s3fs and PyArrow. When PyArrow writes datasets using `write_dataset()`, it may create directory markers that can confuse s3fs. SeaweedFS now handles these correctly by returning 404 for HEAD requests on implicit directories (directories with children), forcing s3fs to use LIST-based discovery.

## Quick Start

### Running the Example Script

```bash
# Start SeaweedFS server
make start-seaweedfs-ci

# Run the example script
python3 example_pyarrow_native.py

# Or with uv (if available)
uv run example_pyarrow_native.py

# Stop the server when done
make stop-seaweedfs-safe
```

### Running Tests

```bash
# Setup Python environment
make setup-python

# Run all tests with server (small and large files)
make test-with-server

# Run quick tests with small files only (faster for development)
make test-quick

# Run implicit directory fix tests
make test-implicit-dir-with-server

# Run PyArrow native S3 filesystem tests
make test-native-s3-with-server

# Run SSE-S3 encryption tests
make test-sse-s3-compat

# Clean up
make clean
```

### Using PyArrow with SeaweedFS

#### Option 1: Using s3fs (recommended for compatibility)

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as pads
import s3fs

# Configure s3fs
fs = s3fs.S3FileSystem(
    key='your_access_key',
    secret='your_secret_key',
    endpoint_url='http://localhost:8333',
    use_ssl=False
)

# Write dataset (creates directory structure)
table = pa.table({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})
pads.write_dataset(table, 'bucket/dataset', filesystem=fs)

# Read dataset (all methods work!)
dataset = pads.dataset('bucket/dataset', filesystem=fs)  # ✅
table = pq.read_table('bucket/dataset', filesystem=fs)   # ✅
dataset = pq.ParquetDataset('bucket/dataset', filesystem=fs)  # ✅
```

#### Option 2: Using PyArrow's native S3 filesystem (pure PyArrow)

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as pads
import pyarrow.fs as pafs

# Configure PyArrow's native S3 filesystem
s3 = pafs.S3FileSystem(
    access_key='your_access_key',
    secret_key='your_secret_key',
    endpoint_override='localhost:8333',
    scheme='http',
    allow_bucket_creation=True,
    allow_bucket_deletion=True
)

# Write dataset
table = pa.table({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})
pads.write_dataset(table, 'bucket/dataset', filesystem=s3)

# Read dataset (all methods work!)
table = pq.read_table('bucket/dataset', filesystem=s3)  # ✅
dataset = pq.ParquetDataset('bucket/dataset', filesystem=s3)  # ✅
dataset = pads.dataset('bucket/dataset', filesystem=s3)  # ✅
```

## Test Files

### Main Test Suite
- **`s3_parquet_test.py`** - Comprehensive PyArrow test suite
  - Tests 2 write methods × 5 read methods × 2 dataset sizes = 20 combinations
  - Uses s3fs library for S3 operations
  - All tests pass with the implicit directory fix ✅

### PyArrow Native S3 Tests
- **`test_pyarrow_native_s3.py`** - PyArrow's native S3 filesystem tests
  - Tests PyArrow's built-in S3FileSystem (pyarrow.fs.S3FileSystem)
  - Pure PyArrow solution without s3fs dependency
  - Tests 3 read methods × 2 dataset sizes = 6 scenarios
  - All tests pass ✅

- **`test_sse_s3_compatibility.py`** - SSE-S3 encryption compatibility tests
  - Tests PyArrow native S3 with SSE-S3 server-side encryption
  - Tests 5 different file sizes (10 to 500,000 rows)
  - Verifies multipart upload encryption works correctly
  - All tests pass ✅

### Implicit Directory Tests
- **`test_implicit_directory_fix.py`** - Specific tests for the implicit directory fix
  - Tests HEAD request behavior
  - Tests s3fs directory detection
  - Tests PyArrow dataset reading
  - All 6 tests pass ✅

### Examples
- **`example_pyarrow_native.py`** - Simple standalone example
  - Demonstrates PyArrow's native S3 filesystem usage
  - Can be run with `uv run` or regular Python
  - Minimal dependencies (pyarrow, boto3)

### Configuration
- **`Makefile`** - Build and test automation
- **`requirements.txt`** - Python dependencies (pyarrow, s3fs, boto3)
- **`.gitignore`** - Ignore patterns for test artifacts

## Documentation

### Technical Documentation
- **`TEST_COVERAGE.md`** - Comprehensive test coverage documentation
  - Unit tests (Go): 17 test cases
  - Integration tests (Python): 6 test cases
  - End-to-end tests (Python): 20 test cases

- **`FINAL_ROOT_CAUSE_ANALYSIS.md`** - Deep technical analysis
  - Root cause of the s3fs compatibility issue
  - How the implicit directory fix works
  - Performance considerations

- **`MINIO_DIRECTORY_HANDLING.md`** - Comparison with MinIO
  - How MinIO handles directory markers
  - Differences in implementation approaches

## The Implicit Directory Fix

### Problem
When PyArrow writes datasets with `write_dataset()`, it may create 0-byte directory markers. s3fs's `info()` method calls HEAD on these paths, and if HEAD returns 200 with size=0, s3fs incorrectly reports them as files instead of directories. This causes PyArrow to fail with "Parquet file size is 0 bytes".

### Solution
SeaweedFS now returns 404 for HEAD requests on implicit directories (0-byte objects or directories with children, when requested without a trailing slash). This forces s3fs to fall back to LIST-based discovery, which correctly identifies directories by checking for children.

### Implementation
The fix is implemented in `weed/s3api/s3api_object_handlers.go`:
- `HeadObjectHandler` - Returns 404 for implicit directories
- `hasChildren` - Helper function to check if a path has children

See the source code for detailed inline documentation.

### Test Coverage
- **Unit tests** (Go): `weed/s3api/s3api_implicit_directory_test.go`
  - Run: `cd weed/s3api && go test -v -run TestImplicitDirectory`
  
- **Integration tests** (Python): `test_implicit_directory_fix.py`
  - Run: `cd test/s3/parquet && make test-implicit-dir-with-server`

- **End-to-end tests** (Python): `s3_parquet_test.py`
  - Run: `cd test/s3/parquet && make test-with-server`

## Makefile Targets

```bash
# Setup
make setup-python          # Create Python virtual environment and install dependencies
make build-weed           # Build SeaweedFS binary

# Testing
make test                 # Run full tests (assumes server is already running)
make test-with-server     # Run full PyArrow test suite with server (small + large files)
make test-quick           # Run quick tests with small files only (assumes server is running)
make test-implicit-dir-with-server  # Run implicit directory tests with server
make test-native-s3       # Run PyArrow native S3 tests (assumes server is running)
make test-native-s3-with-server  # Run PyArrow native S3 tests with server management
make test-sse-s3-compat   # Run comprehensive SSE-S3 encryption compatibility tests

# Server Management
make start-seaweedfs-ci   # Start SeaweedFS in background (CI mode)
make stop-seaweedfs-safe  # Stop SeaweedFS gracefully
make clean                # Clean up all test artifacts

# Development
make help                 # Show all available targets
```

## Continuous Integration

The tests are automatically run in GitHub Actions on every push/PR that affects S3 or filer code:

**Workflow**: `.github/workflows/s3-parquet-tests.yml`

**Test Matrix**:
- Python versions: 3.9, 3.11, 3.12
- PyArrow integration tests (s3fs): 20 test combinations
- PyArrow native S3 tests: 6 test scenarios ✅ **NEW**
- SSE-S3 encryption tests: 5 file sizes ✅ **NEW**
- Implicit directory fix tests: 6 test scenarios
- Go unit tests: 17 test cases

**Test Steps** (run for each Python version):
1. Build SeaweedFS
2. Run PyArrow Parquet integration tests (`make test-with-server`)
3. Run implicit directory fix tests (`make test-implicit-dir-with-server`)
4. Run PyArrow native S3 filesystem tests (`make test-native-s3-with-server`) ✅ **NEW**
5. Run SSE-S3 encryption compatibility tests (`make test-sse-s3-compat`) ✅ **NEW**
6. Run Go unit tests for implicit directory handling

**Triggers**:
- Push/PR to master (when `weed/s3api/**` or `weed/filer/**` changes)
- Manual trigger via GitHub UI (workflow_dispatch)

## Requirements

- Python 3.8+
- PyArrow 22.0.0+
- s3fs 2024.12.0+
- boto3 1.40.0+
- SeaweedFS (latest)

## AWS S3 Compatibility

The implicit directory fix makes SeaweedFS behavior more compatible with AWS S3:
- AWS S3 typically doesn't create directory markers for implicit directories
- HEAD on "dataset" (when only "dataset/file.txt" exists) returns 404 on AWS
- SeaweedFS now matches this behavior for implicit directories with children

## Edge Cases Handled

✅ **Implicit directories with children** → 404 (forces LIST-based discovery)  
✅ **Empty files (0-byte, no children)** → 200 (legitimate empty file)  
✅ **Empty directories (no children)** → 200 (legitimate empty directory)  
✅ **Explicit directory requests (trailing slash)** → 200 (normal directory behavior)  
✅ **Versioned buckets** → Skip implicit directory check (versioned semantics)  
✅ **Regular files** → 200 (normal file behavior)

## Performance

The implicit directory check adds minimal overhead:
- Only triggered for 0-byte objects or directories without trailing slash
- Cost: One LIST operation with Limit=1 (~1-5ms)
- No impact on regular file operations

## Contributing

When adding new tests:
1. Add test cases to the appropriate test file
2. Update TEST_COVERAGE.md
3. Run the full test suite to ensure no regressions
4. Update this README if adding new functionality

## References

- [PyArrow Documentation](https://arrow.apache.org/docs/python/parquet.html)
- [s3fs Documentation](https://s3fs.readthedocs.io/)
- [SeaweedFS S3 API](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API)
- [AWS S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/)

---

**Last Updated**: November 19, 2025  
**Status**: All tests passing ✅
