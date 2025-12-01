# TUS Protocol Integration Tests

This directory contains integration tests for the TUS (resumable upload) protocol support in SeaweedFS Filer.

## TUS Protocol Overview

TUS is an open protocol for resumable file uploads. It allows clients to upload files in chunks and resume uploads after network failures.

Key endpoints:
- `OPTIONS /tus/` - Server capability discovery
- `POST /tus/{path}` - Create new upload session
- `HEAD /tus/{upload-id}` - Get current upload offset
- `PATCH /tus/{upload-id}` - Upload data at offset
- `DELETE /tus/{upload-id}` - Cancel upload

## Prerequisites

1. Build the weed binary:
```bash
cd ../../weed
go build
```

2. The tests will automatically start required servers (master, volume, filer).

## Running Tests

### Run all TUS tests:
```bash
go test -v ./test/tus/...
```

### Run specific test:
```bash
go test -v ./test/tus -run TestTusBasicUpload
```

### Skip integration tests (short mode):
```bash
go test -v -short ./test/tus/...
```

## Test Coverage

The tests cover:
- Basic upload creation and completion
- Chunked/resumable uploads
- Upload offset tracking (HEAD requests)
- Upload cancellation (DELETE requests)
- Error handling (invalid offsets, missing uploads)
- Large file uploads with multiple chunks
- Concurrent uploads
- Metadata handling

## TUS Protocol Headers

Required headers for TUS requests:
- `Tus-Resumable: 1.0.0` - Protocol version
- `Upload-Length` - Total file size (on creation)
- `Upload-Offset` - Current byte offset (on PATCH)
- `Content-Type: application/offset+octet-stream` (on PATCH)

Optional headers:
- `Upload-Metadata` - Base64-encoded key-value pairs

