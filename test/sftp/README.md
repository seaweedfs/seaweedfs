# SeaweedFS SFTP Integration Tests

This directory contains integration tests for the SeaweedFS SFTP server.

## Prerequisites

1. Build the SeaweedFS binary:
   ```bash
   cd ../../weed
   go build -o weed .
   ```

2. Ensure `ssh-keygen` is available (for generating test SSH host keys)

## Running Tests

### Run all tests
```bash
make test
```

### Run tests with verbose output
```bash
make test-verbose
```

### Run a specific test
```bash
go test -v -run TestHomeDirPathTranslation
```

### Skip long-running tests
```bash
go test -short ./...
```

## Test Structure

- `framework.go` - Test framework that starts SeaweedFS cluster with SFTP
- `basic_test.go` - Basic SFTP operation tests including:
  - HomeDir path translation (fixes issue #7470)
  - File upload/download
  - Directory operations
  - Large file handling
  - Edge cases

## Test Configuration

Tests use `testdata/userstore.json` which defines test users:

| Username | Password | HomeDir | Permissions |
|----------|----------|---------|-------------|
| admin | adminpassword | / | Full access |
| testuser | testuserpassword | /sftp/testuser | Full access to home |
| readonly | readonlypassword | /public | Read-only |

## Key Tests

### TestHomeDirPathTranslation

Tests the fix for [issue #7470](https://github.com/seaweedfs/seaweedfs/issues/7470) where
users with a non-root HomeDir (e.g., `/sftp/testuser`) could not upload files to `/`
because the path wasn't being translated to their home directory.

The test verifies:
- Uploading to `/` correctly maps to the user's HomeDir
- Creating directories at `/` works
- Listing `/` shows the user's home directory contents
- All path operations respect the HomeDir translation

## Debugging

To debug test failures:

1. Enable verbose output:
   ```bash
   go test -v -run TestName
   ```

2. Keep test artifacts (don't cleanup):
   ```go
   config := DefaultTestConfig()
   config.SkipCleanup = true
   ```

3. Enable debug logging:
   ```go
   config := DefaultTestConfig()
   config.EnableDebug = true
   ```


