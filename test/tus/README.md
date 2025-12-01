# TUS Protocol Integration Tests

This directory contains integration tests for the TUS (resumable upload) protocol support in SeaweedFS Filer.

## Overview

TUS is an open protocol for resumable file uploads over HTTP. It allows clients to upload files in chunks and resume uploads after network failures or interruptions.

### Why TUS?

- **Resumable uploads**: Resume interrupted uploads without re-sending data
- **Chunked uploads**: Upload large files in smaller pieces
- **Simple protocol**: Standard HTTP methods with custom headers
- **Wide client support**: Libraries available for JavaScript, Python, Go, and more

## TUS Protocol Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `OPTIONS` | `/.tus/` | Server capability discovery |
| `POST` | `/.tus/{path}` | Create new upload session |
| `HEAD` | `/.tus/.uploads/{id}` | Get current upload offset |
| `PATCH` | `/.tus/.uploads/{id}` | Upload data at offset |
| `DELETE` | `/.tus/.uploads/{id}` | Cancel upload |

### TUS Headers

**Request Headers:**
- `Tus-Resumable: 1.0.0` - Protocol version (required)
- `Upload-Length` - Total file size in bytes (required on POST)
- `Upload-Offset` - Current byte offset (required on PATCH)
- `Upload-Metadata` - Base64-encoded key-value pairs (optional)
- `Content-Type: application/offset+octet-stream` (required on PATCH)

**Response Headers:**
- `Tus-Resumable` - Protocol version
- `Tus-Version` - Supported versions
- `Tus-Extension` - Supported extensions
- `Tus-Max-Size` - Maximum upload size
- `Upload-Offset` - Current byte offset
- `Location` - Upload URL (on POST)

## Enabling TUS

TUS protocol support is enabled by default at `/.tus` path. You can customize the path using the `-tusBasePath` flag:

```bash
# Start filer with default TUS path (/.tus)
weed filer -master=localhost:9333

# Use a custom path
weed filer -master=localhost:9333 -tusBasePath=uploads/tus

# Disable TUS by setting empty path
weed filer -master=localhost:9333 -tusBasePath=
```

## Test Structure

### Integration Tests

The tests cover:

1. **Basic Functionality**
   - `TestTusOptionsHandler` - Capability discovery
   - `TestTusBasicUpload` - Simple complete upload
   - `TestTusCreationWithUpload` - Creation-with-upload extension

2. **Chunked Uploads**
   - `TestTusChunkedUpload` - Upload in multiple chunks

3. **Resumable Uploads**
   - `TestTusHeadRequest` - Offset tracking
   - `TestTusResumeAfterInterruption` - Resume after failure

4. **Error Handling**
   - `TestTusInvalidOffset` - Offset mismatch (409 Conflict)
   - `TestTusUploadNotFound` - Missing upload (404 Not Found)
   - `TestTusDeleteUpload` - Upload cancellation

## Running Tests

### Prerequisites

1. **Build SeaweedFS**:
```bash
make build-weed
# or
cd ../../weed && go build -o weed
```

### Using Makefile

```bash
# Show available targets
make help

# Run all tests with automatic server management
make test-with-server

# Run all tests (requires running server)
make test

# Run specific test categories
make test-basic      # Basic upload tests
make test-chunked    # Chunked upload tests
make test-resume     # Resume/HEAD tests
make test-errors     # Error handling tests

# Manual testing
make manual-start    # Start SeaweedFS for manual testing
make manual-stop     # Stop and cleanup
```

### Using Go Test Directly

```bash
# Run all TUS tests
go test -v ./test/tus/...

# Run specific test
go test -v ./test/tus -run TestTusBasicUpload

# Skip integration tests (short mode)
go test -v -short ./test/tus/...
```

### Debug

```bash
# View server logs
make debug-logs

# Check process and port status
make debug-status
```

## Test Environment

Each test run:
1. Starts a SeaweedFS cluster (master, volume, filer)
2. Creates uploads using TUS protocol
3. Verifies files are stored correctly
4. Cleans up test data

### Default Ports

| Service | Port |
|---------|------|
| Master | 19333 |
| Volume | 18080 |
| Filer | 18888 |

### Configuration

Override defaults via environment or Makefile variables:
```bash
FILER_PORT=8889 MASTER_PORT=9334 make test
```

## Example Usage

### Create Upload

```bash
curl -X POST http://localhost:18888/.tus/mydir/file.txt \
  -H "Tus-Resumable: 1.0.0" \
  -H "Upload-Length: 1000" \
  -H "Upload-Metadata: filename dGVzdC50eHQ="
```

### Upload Data

```bash
curl -X PATCH http://localhost:18888/.tus/.uploads/{upload-id} \
  -H "Tus-Resumable: 1.0.0" \
  -H "Upload-Offset: 0" \
  -H "Content-Type: application/offset+octet-stream" \
  --data-binary @file.txt
```

### Check Offset

```bash
curl -I http://localhost:18888/.tus/.uploads/{upload-id} \
  -H "Tus-Resumable: 1.0.0"
```

### Cancel Upload

```bash
curl -X DELETE http://localhost:18888/.tus/.uploads/{upload-id} \
  -H "Tus-Resumable: 1.0.0"
```

## TUS Extensions Supported

- **creation**: Create new uploads with POST
- **creation-with-upload**: Send data in creation request
- **termination**: Cancel uploads with DELETE

## Architecture

```text
Client                         Filer                      Volume Servers
  |                              |                              |
  |-- POST /.tus/path/file.mp4 ->|                              |
  |                              |-- Create session dir ------->|
  |<-- 201 Location: /.../{id} --|                              |
  |                              |                              |
  |-- PATCH /.tus/.uploads/{id} >|                              |
  |   Upload-Offset: 0           |-- Assign volume ------------>|
  |   [chunk data]               |-- Upload chunk ------------->|
  |<-- 204 Upload-Offset: N -----|                              |
  |                              |                              |
  |   (network failure)          |                              |
  |                              |                              |
  |-- HEAD /.tus/.uploads/{id} ->|                              |
  |<-- Upload-Offset: N ---------|                              |
  |                              |                              |
  |-- PATCH (resume) ----------->|-- Upload remaining -------->|
  |<-- 204 (complete) -----------|-- Assemble final file ----->|
```

## Comparison with S3 Multipart

| Feature | TUS | S3 Multipart |
|---------|-----|--------------|
| Protocol | Custom HTTP headers | S3 API |
| Session Init | POST with Upload-Length | CreateMultipartUpload |
| Upload Data | PATCH with offset | UploadPart with partNumber |
| Resume | HEAD to get offset | ListParts |
| Complete | Automatic at final offset | CompleteMultipartUpload |
| Ordering | Sequential (offset-based) | Parallel (part numbers) |

## Related Resources

- [TUS Protocol Specification](https://tus.io/protocols/resumable-upload)
- [tus-js-client](https://github.com/tus/tus-js-client) - JavaScript client
- [go-tus](https://github.com/eventials/go-tus) - Go client
- [SeaweedFS S3 API](../../weed/s3api) - Alternative multipart upload
