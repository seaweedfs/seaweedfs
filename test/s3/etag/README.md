# S3 ETag Format Integration Tests

This test suite verifies that SeaweedFS returns correct ETag formats for S3 operations, ensuring compatibility with AWS S3 SDKs.

## Background

**GitHub Issue #7768**: AWS S3 SDK for Java v2 was failing with `Invalid base 16 character: '-'` when performing `PutObject` on large files.

### Root Cause

SeaweedFS internally auto-chunks large files (>8MB) for efficient storage. Previously, when a regular `PutObject` request resulted in multiple internal chunks, SeaweedFS returned a composite ETag format (`<md5>-<count>`) instead of a pure MD5 hash.

### AWS S3 Specification

| Operation | ETag Format | Example |
|-----------|-------------|---------|
| PutObject (any size) | Pure MD5 hex (32 chars) | `d41d8cd98f00b204e9800998ecf8427e` |
| CompleteMultipartUpload | Composite (`<md5>-<partcount>`) | `d41d8cd98f00b204e9800998ecf8427e-3` |

AWS S3 SDK v2 for Java validates `PutObject` ETags as hexadecimal, which fails when the ETag contains a hyphen.

## Test Coverage

| Test | File Size | Purpose |
|------|-----------|---------|
| `TestPutObjectETagFormat_SmallFile` | 1KB | Verify single-chunk uploads return pure MD5 |
| `TestPutObjectETagFormat_LargeFile` | 10MB | **Critical**: Verify auto-chunked uploads return pure MD5 |
| `TestPutObjectETagFormat_ExtraLargeFile` | 25MB | Verify multi-chunk auto-chunked uploads return pure MD5 |
| `TestMultipartUploadETagFormat` | 15MB | Verify multipart uploads correctly return composite ETag |
| `TestPutObjectETagConsistency` | Various | Verify ETag consistency across PUT/HEAD/GET |
| `TestETagHexValidation` | 10MB | Simulate AWS SDK v2 hex validation |
| `TestMultipleLargeFileUploads` | 10MB x5 | Stress test multiple large uploads |

## Prerequisites

1. SeaweedFS running with S3 API enabled:
   ```bash
   weed server -s3
   ```

2. Go 1.21 or later

3. AWS SDK v2 for Go (installed via go modules)

## Running Tests

```bash
# Run all tests
make test

# Run only large file tests (the critical ones for issue #7768)
make test-large

# Run quick tests (small files only)
make test-quick

# Run with verbose output
make test-verbose
```

## Configuration

By default, tests connect to `http://127.0.0.1:8333`. To use a different endpoint:

```bash
S3_ENDPOINT=http://localhost:8333 make test
```

Or modify `defaultConfig` in `s3_etag_test.go`.

## SDK Compatibility

These tests use **AWS SDK v2 for Go**, which has the same ETag validation behavior as AWS SDK v2 for Java. The tests include:

- ETag format validation (pure MD5 vs composite)
- Hex decoding validation (simulates `Base16Codec.decode`)
- Content integrity verification

## Validated SDK Versions

| SDK | Version | Status |
|-----|---------|--------|
| AWS SDK v2 for Go | 1.20+ | ✅ Tested |
| AWS SDK v2 for Java | 2.20+ | ✅ Compatible (issue #7768 fixed) |
| AWS SDK v1 for Go | 1.x | ✅ Compatible (less strict validation) |
| AWS SDK v1 for Java | 1.x | ✅ Compatible (less strict validation) |

## Related

- [GitHub Issue #7768](https://github.com/seaweedfs/seaweedfs/issues/7768)
- [AWS S3 ETag Documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html)


