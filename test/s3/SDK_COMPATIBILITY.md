# S3 SDK Compatibility Testing

This document describes the SDK versions tested against SeaweedFS S3 API and known compatibility considerations.

## Validated SDK Versions

### Go SDKs

| SDK | Version | Test Location | Status |
|-----|---------|---------------|--------|
| AWS SDK v2 for Go | 1.20+ | `test/s3/etag/`, `test/s3/copying/` | ✅ Tested |
| AWS SDK v1 for Go | 1.x | `test/s3/basic/` | ✅ Tested |

### Java SDKs

| SDK | Version | Test Location | Status |
|-----|---------|---------------|--------|
| AWS SDK v2 for Java | 2.20.127+ | `other/java/s3copier/` | ✅ Tested |
| AWS SDK v1 for Java | 1.12.600+ | `other/java/s3copier/` | ✅ Tested |

### Python SDKs

| SDK | Version | Test Location | Status |
|-----|---------|---------------|--------|
| boto3 | 1.x | `test/s3/parquet/` | ✅ Tested |
| PyArrow S3 | 14+ | `test/s3/parquet/` | ✅ Tested |

## SDK-Specific Considerations

### AWS SDK v2 for Java - ETag Validation

**Issue**: [GitHub #7768](https://github.com/seaweedfs/seaweedfs/issues/7768)

AWS SDK v2 for Java includes strict ETag validation in `ChecksumsEnabledValidator.validatePutObjectChecksum`. It decodes the ETag as a hexadecimal MD5 hash using `Base16Codec.decode()`.

**Impact**: If the ETag contains non-hexadecimal characters (like `-` in composite format), the SDK fails with:
```text
java.lang.IllegalArgumentException: Invalid base 16 character: '-'
```

**Resolution**: SeaweedFS now correctly returns:
- **PutObject**: Pure MD5 hex ETag (32 characters) regardless of internal chunking
- **CompleteMultipartUpload**: Composite ETag (`<md5>-<partcount>`)

**Test Coverage**: `test/s3/etag/` and `other/java/s3copier/ETagValidationTest.java`

### AWS SDK v1 vs v2 Differences

| Feature | SDK v1 | SDK v2 |
|---------|--------|--------|
| ETag hex validation | No | Yes (strict) |
| Checksum validation | Basic | Enhanced |
| Async support | Limited | Full |
| Default retry behavior | Lenient | Stricter |

### Large File Handling

SeaweedFS auto-chunks files larger than **8MB** for efficient storage. This is transparent to clients, but previously affected ETag format. The current implementation ensures:

1. Regular `PutObject` (any size): Returns pure MD5 ETag
2. Multipart upload: Returns composite ETag per AWS S3 specification

## Test Categories by File Size

| Category | Size | Chunks | ETag Format |
|----------|------|--------|-------------|
| Small | < 256KB | 1 (inline) | Pure MD5 |
| Medium | 256KB - 8MB | 1 | Pure MD5 |
| Large | 8MB - 24MB | 2-3 | Pure MD5 |
| Extra Large | > 24MB | 4+ | Pure MD5 |
| Multipart | N/A | Per part | Composite |

## Running SDK Compatibility Tests

### Go Tests

```bash
# Run all ETag tests
cd test/s3/etag && make test

# Run large file tests only
cd test/s3/etag && make test-large
```

### Java Tests

```bash
# Run all Java SDK tests
cd other/java/s3copier && mvn test

# Run only ETag validation tests
cd other/java/s3copier && mvn test -Dtest=ETagValidationTest
```

### Python Tests

```bash
# Run PyArrow S3 tests
cd test/s3/parquet && make test
```

## Adding New SDK Tests

When adding tests for new SDKs, ensure:

1. **Large file tests (>8MB)**: Critical for verifying ETag format with auto-chunking
2. **Multipart upload tests**: Verify composite ETag format
3. **Checksum validation**: Test SDK-specific checksum validation if applicable
4. **Document SDK version**: Add to this compatibility matrix

## Known Issues and Workarounds

### Issue: Older SDK Versions

Some very old SDK versions (e.g., AWS SDK v1 for Java < 1.11.x) may have different behavior. Testing with the versions listed above is recommended.

### Issue: Custom Checksum Algorithms

AWS SDK v2 supports SHA-256 and CRC32 checksums in addition to MD5. SeaweedFS currently returns MD5-based ETags. For checksums other than MD5, use the `x-amz-checksum-*` headers.

## References

- [AWS S3 ETag Documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html)
- [AWS SDK v2 Migration Guide](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/migration.html)
- [GitHub Issue #7768](https://github.com/seaweedfs/seaweedfs/issues/7768)

