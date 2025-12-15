# SeaweedFS S3 Java SDK Compatibility Tests

This project contains Java-based integration tests for SeaweedFS S3 API compatibility.

## Overview

Tests are provided for both AWS SDK v1 and v2 to ensure compatibility with the various SDK versions commonly used in production.

## SDK Versions

| SDK | Version | Notes |
|-----|---------|-------|
| AWS SDK v1 for Java | 1.12.600 | Legacy SDK, less strict ETag validation |
| AWS SDK v2 for Java | 2.20.127 | Modern SDK with strict checksum validation |

## Running Tests

### Prerequisites

1. SeaweedFS running with S3 API enabled:
   ```bash
   weed server -s3
   ```

2. Java 18+ and Maven

### Run All Tests

```bash
mvn test
```

### Run Specific Tests

```bash
# Run only ETag validation tests (AWS SDK v2)
mvn test -Dtest=ETagValidationTest

# Run with custom endpoint
mvn test -Dtest=ETagValidationTest -DS3_ENDPOINT=http://localhost:8333
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_ENDPOINT` | `http://127.0.0.1:8333` | S3 API endpoint URL |
| `S3_ACCESS_KEY` | `some_access_key1` | Access key ID |
| `S3_SECRET_KEY` | `some_secret_key1` | Secret access key |
| `S3_REGION` | `us-east-1` | AWS region |

## Test Coverage

### ETagValidationTest (AWS SDK v2)

Tests for [GitHub Issue #7768](https://github.com/seaweedfs/seaweedfs/issues/7768) - ETag format validation.

| Test | Description |
|------|-------------|
| `testSmallFilePutObject` | Verify small files return pure MD5 ETag |
| `testLargeFilePutObject_Issue7768` | **Critical**: Verify large files (>8MB) return pure MD5 ETag |
| `testExtraLargeFilePutObject` | Verify very large files (>24MB) return pure MD5 ETag |
| `testMultipartUploadETag` | Verify multipart uploads return composite ETag |
| `testETagConsistency` | Verify ETag consistency across PUT/HEAD/GET |
| `testMultipleLargeFileUploads` | Stress test multiple large uploads |

### Background: Issue #7768

AWS SDK v2 for Java includes checksum validation that decodes the ETag as hexadecimal. When SeaweedFS returned composite ETags (`<md5>-<count>`) for regular `PutObject` with internally auto-chunked files, the SDK failed with:

```
java.lang.IllegalArgumentException: Invalid base 16 character: '-'
```

**Per AWS S3 specification:**
- `PutObject`: ETag is always a pure MD5 hex string (32 chars)
- `CompleteMultipartUpload`: ETag is composite format (`<md5>-<partcount>`)

The fix ensures SeaweedFS follows this specification.

## Project Structure

```
src/
├── main/java/com/seaweedfs/s3/
│   ├── PutObject.java           # Example PutObject with SDK v1
│   └── HighLevelMultipartUpload.java
└── test/java/com/seaweedfs/s3/
    ├── PutObjectTest.java       # Basic SDK v1 test
    └── ETagValidationTest.java  # Comprehensive SDK v2 ETag tests
```

## Validated SDK Versions

This Java test project validates:

- ✅ AWS SDK v2 for Java 2.20.127+
- ✅ AWS SDK v1 for Java 1.12.600+

Go SDK validation is performed by separate test suites:
- See [Go ETag Tests](/test/s3/etag/) for AWS SDK v2 for Go tests
- See [test/s3/SDK_COMPATIBILITY.md](/test/s3/SDK_COMPATIBILITY.md) for full SDK compatibility matrix

## Related

- [GitHub Issue #7768](https://github.com/seaweedfs/seaweedfs/issues/7768)
- [AWS S3 ETag Documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html)
- [Go ETag Tests](/test/s3/etag/)
- [SDK Compatibility Matrix](/test/s3/SDK_COMPATIBILITY.md)

