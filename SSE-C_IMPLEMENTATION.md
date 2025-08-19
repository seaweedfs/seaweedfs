# Server-Side Encryption with Customer-Provided Keys (SSE-C) Implementation

This document describes the implementation of SSE-C support in SeaweedFS, addressing the feature request from [GitHub Discussion #5361](https://github.com/seaweedfs/seaweedfs/discussions/5361).

## Overview

SSE-C allows clients to provide their own encryption keys for server-side encryption of objects stored in SeaweedFS. The server encrypts the data using the customer-provided AES-256 key but does not store the key itself - only an MD5 hash of the key for validation purposes.

## Implementation Details

### Architecture

The SSE-C implementation follows a transparent encryption/decryption pattern:

1. **Upload (PUT/POST)**: Data is encrypted with the customer key before being stored
2. **Download (GET/HEAD)**: Encrypted data is decrypted on-the-fly using the customer key
3. **Metadata Storage**: Only the encryption algorithm and key MD5 are stored as metadata

### Key Components

#### 1. Constants and Headers (`weed/s3api/s3_constants/header.go`)
- Added AWS-compatible SSE-C header constants
- Support for both regular and copy-source SSE-C headers

#### 2. Core SSE-C Logic (`weed/s3api/s3_sse_c.go`)
- **SSECustomerKey**: Structure to hold customer encryption key and metadata
- **SSECEncryptedReader**: Streaming encryption with AES-256-CTR mode
- **SSECDecryptedReader**: Streaming decryption with IV extraction
- **validateAndParseSSECHeaders**: Shared validation logic (DRY principle)
- **ParseSSECHeaders**: Parse regular SSE-C headers
- **ParseSSECCopySourceHeaders**: Parse copy-source SSE-C headers
- Header validation and parsing functions
- Metadata extraction and response handling

#### 3. Error Handling (`weed/s3api/s3err/s3api_errors.go`)
- New error codes for SSE-C validation failures
- AWS-compatible error messages and HTTP status codes

#### 4. S3 API Integration
- **PUT Object Handler**: Encrypts data streams transparently
- **GET Object Handler**: Decrypts data streams transparently
- **HEAD Object Handler**: Validates keys and returns appropriate headers
- **Metadata Storage**: Integrates with existing `SaveAmzMetaData` function

### Encryption Scheme

- **Algorithm**: AES-256-CTR (Counter mode)
- **Key Size**: 256 bits (32 bytes)
- **IV Generation**: Random 16-byte IV per object
- **Storage Format**: `[IV][EncryptedData]` where IV is prepended to encrypted content

### Metadata Storage

SSE-C metadata is stored in the filer's extended attributes:
```
x-amz-server-side-encryption-customer-algorithm: "AES256"
x-amz-server-side-encryption-customer-key-md5: "<md5-hash-of-key>"
```

## API Compatibility

### Required Headers for Encryption (PUT/POST)
```
x-amz-server-side-encryption-customer-algorithm: AES256
x-amz-server-side-encryption-customer-key: <base64-encoded-256-bit-key>
x-amz-server-side-encryption-customer-key-md5: <md5-hash-of-key>
```

### Required Headers for Decryption (GET/HEAD)
Same headers as encryption - the server validates the key MD5 matches.

### Copy Operations
Support for copy-source SSE-C headers:
```
x-amz-copy-source-server-side-encryption-customer-algorithm
x-amz-copy-source-server-side-encryption-customer-key  
x-amz-copy-source-server-side-encryption-customer-key-md5
```

## Error Handling

The implementation provides AWS-compatible error responses:

- **InvalidEncryptionAlgorithmError**: Non-AES256 algorithm specified
- **InvalidArgument**: Invalid key format, size, or MD5 mismatch
- **Missing customer key**: Object encrypted but no key provided
- **Unnecessary customer key**: Object not encrypted but key provided

## Security Considerations

1. **Key Management**: Customer keys are never stored - only MD5 hashes for validation
2. **IV Randomness**: Fresh random IV generated for each object
3. **Transparent Security**: Volume servers never see unencrypted data
4. **Key Validation**: Strict validation of key format, size, and MD5

## Testing

Comprehensive test suite covers:
- Header validation and parsing (regular and copy-source)
- Encryption/decryption round-trip
- Error condition handling  
- Metadata extraction
- Code reuse validation (DRY principle)
- AWS S3 compatibility

Run tests with:
```bash
go test -v ./weed/s3api

## Usage Example

### Upload with SSE-C
```bash
# Generate a 256-bit key
KEY=$(openssl rand -base64 32)
KEY_MD5=$(echo -n "$KEY" | base64 -d | openssl dgst -md5 -binary | base64)

# Upload object with SSE-C
curl -X PUT "http://localhost:8333/bucket/object" \
  -H "x-amz-server-side-encryption-customer-algorithm: AES256" \
  -H "x-amz-server-side-encryption-customer-key: $KEY" \
  -H "x-amz-server-side-encryption-customer-key-md5: $KEY_MD5" \
  --data-binary @file.txt
```

### Download with SSE-C
```bash
# Download object with SSE-C (same key required)
curl "http://localhost:8333/bucket/object" \
  -H "x-amz-server-side-encryption-customer-algorithm: AES256" \
  -H "x-amz-server-side-encryption-customer-key: $KEY" \
  -H "x-amz-server-side-encryption-customer-key-md5: $KEY_MD5"
```

## Integration Points

### Existing SeaweedFS Features
- **Filer Metadata**: Extends existing metadata storage
- **Volume Servers**: No changes required - store encrypted data transparently
- **S3 API**: Integrates seamlessly with existing handlers
- **Versioning**: Compatible with object versioning
- **Multipart Upload**: Ready for multipart upload integration

### Future Enhancements
- **SSE-S3**: Server-managed encryption keys
- **SSE-KMS**: External key management service integration
- **Performance Optimization**: Hardware acceleration for encryption
- **Compliance**: Enhanced audit logging for encrypted objects

## File Changes Summary

1. **`weed/s3api/s3_constants/header.go`** - Added SSE-C header constants
2. **`weed/s3api/s3_sse_c.go`** - Core SSE-C implementation (NEW)
3. **`weed/s3api/s3_sse_c_test.go`** - Comprehensive test suite (NEW)
4. **`weed/s3api/s3err/s3api_errors.go`** - Added SSE-C error codes
5. **`weed/s3api/s3api_object_handlers.go`** - GET/HEAD with SSE-C support
6. **`weed/s3api/s3api_object_handlers_put.go`** - PUT with SSE-C support
7. **`weed/server/filer_server_handlers_write_autochunk.go`** - Metadata storage

## Compliance

This implementation follows the [AWS S3 SSE-C specification](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html) for maximum compatibility with existing S3 clients and tools.

## Performance Impact

- **Encryption Overhead**: Minimal CPU impact with efficient AES-CTR streaming
- **Memory Usage**: Constant memory usage via streaming encryption/decryption
- **Storage Overhead**: 16 bytes per object for IV storage
- **Network**: No additional network overhead
