# Remote Object Cache Integration Tests

This directory contains integration tests for the remote object caching feature with singleflight deduplication.

## Test Flow

Each test follows this pattern:
1. **Write to local** - Upload data to primary SeaweedFS (local storage)
2. **Uncache** - Push data to remote storage and remove local chunks
3. **Read** - Read data (triggers caching from remote back to local)

This tests the full remote caching workflow including singleflight deduplication.

## Architecture

```text
┌─────────────────────────────────────────────────────────────────┐
│                        Test Client                               │
│                                                                  │
│    1. PUT data to primary SeaweedFS                             │
│    2. remote.cache.uncache (push to remote, purge local)        │
│    3. GET data (triggers caching from remote)                   │
│    4. Verify singleflight deduplication                         │
└──────────────────────────────────┬──────────────────────────────┘
                                   │
                 ┌─────────────────┴─────────────────┐
                 ▼                                   ▼
┌────────────────────────────────────┐   ┌────────────────────────────────┐
│     Primary SeaweedFS              │   │     Remote SeaweedFS           │
│        (port 8333)                 │   │        (port 8334)             │
│                                    │   │                                │
│  - Being tested                    │   │  - Acts as "remote" S3         │
│  - Has remote storage mounted      │──▶│  - Receives uncached data      │
│  - Caches remote objects           │   │  - Serves data for caching     │
│  - Singleflight deduplication      │   │                                │
└────────────────────────────────────┘   └────────────────────────────────┘
```

## What's Being Tested

### Test Files and Coverage

| Test File | Commands Tested | Test Count | Description |
|-----------|----------------|------------|-------------|
| `remote_cache_test.go` | Basic caching | 5 tests | Original caching workflow and singleflight tests |
| `command_remote_configure_test.go` | `remote.configure` | 6 tests | Configuration management |
| `command_remote_mount_test.go` | `remote.mount`, `remote.unmount`, `remote.mount.buckets` | 10 tests | Mount operations |
| `command_remote_cache_test.go` | `remote.cache`, `remote.uncache` | 13 tests | Cache/uncache with filters |
| `command_remote_copy_local_test.go` | `remote.copy.local` | 12 tests | **NEW in PR #8033** - Local to remote copy |
| `command_remote_meta_sync_test.go` | `remote.meta.sync` | 8 tests | Metadata synchronization |
| `command_edge_cases_test.go` | All commands | 11 tests | Edge cases and stress tests |

**Total: 65 test cases covering 8 weed shell commands**

### Commands Tested

1. **`remote.configure`** - Configure remote storage backends
2. **`remote.mount`** - Mount remote storage to local directory
3. **`remote.unmount`** - Unmount remote storage
4. **`remote.mount.buckets`** - Mount all buckets from remote
5. **`remote.cache`** - Cache remote files locally
6. **`remote.uncache`** - Remove local cache, keep metadata
7. **`remote.copy.local`** - Copy local files to remote (**NEW in PR #8033**)
8. **`remote.meta.sync`** - Sync metadata from remote

### Test Coverage

**Basic Operations:**
- Basic caching workflow (Write → Uncache → Read)
- Singleflight deduplication (concurrent reads trigger ONE cache operation)
- Large object caching (5MB-100MB files)
- Range requests (partial reads)
- Not found handling

**File Filtering:**
- Include patterns (`*.pdf`, `*.txt`, etc.)
- Exclude patterns
- Size filters (`-minSize`, `-maxSize`)
- Age filters (`-minAge`, `-maxAge`)
- Combined filters

**Command Options:**
- Dry run mode (`-dryRun=true`)
- Concurrency settings (`-concurrent=N`)
- Force update (`-forceUpdate=true`)
- Non-empty directory mounting (`-nonempty=true`)

**Edge Cases:**
- Empty directories
- Nested directory hierarchies
- Special characters in filenames
- Very large files (100MB+)
- Many small files (100+)
- Rapid cache/uncache cycles
- Concurrent command execution
- Invalid paths
- Zero-byte files

## Running Tests

### Run All Tests
```bash
# Full automated workflow
make test-with-server

# Or manually
go test -v ./...
```

### Run Specific Test Files
```bash
# Test remote.configure command
go test -v -run TestRemoteConfigure

# Test remote.mount/unmount commands
go test -v -run TestRemoteMount
go test -v -run TestRemoteUnmount

# Test remote.cache/uncache commands  
go test -v -run TestRemoteCache
go test -v -run TestRemoteUncache

# Test remote.copy.local command (PR #8033)
go test -v -run TestRemoteCopyLocal

# Test remote.meta.sync command
go test -v -run TestRemoteMetaSync

# Test edge cases
go test -v -run TestEdgeCase
```

## Quick Start

### Run Full Test Suite (Recommended)

```bash
# Build SeaweedFS, start both servers, run tests, stop servers
make test-with-server
```

### Manual Steps

```bash
# 1. Build SeaweedFS binary
make build-weed

# 2. Start remote SeaweedFS (acts as "remote" storage)
make start-remote

# 3. Start primary SeaweedFS (the one being tested)
make start-primary

# 4. Configure remote storage mount
make setup-remote

# 5. Run tests
make test

# 6. Clean up
make clean
```

## Configuration

### Primary SeaweedFS (Being Tested)

| Service | Port |
|---------|------|
| S3 API | 8333 |
| Filer | 8888 |
| Master | 9333 |
| Volume | 8080 |

### Remote SeaweedFS (Remote Storage)

| Service | Port |
|---------|------|
| S3 API | 8334 |
| Filer | 8889 |
| Master | 9334 |
| Volume | 8081 |

## Makefile Targets

```bash
make help           # Show all available targets
make build-weed     # Build SeaweedFS binary
make start-remote   # Start remote SeaweedFS
make start-primary  # Start primary SeaweedFS
make setup-remote   # Configure remote storage mount
make test           # Run tests
make test-with-server  # Full automated test workflow
make logs           # Show server logs
make health         # Check server status
make clean          # Stop servers and clean up
```

## Test Details

### TestRemoteCacheBasic
Basic workflow test:
1. Write object to primary (local)
2. Uncache (push to remote, remove local chunks)
3. Read (triggers caching from remote)
4. Read again (from local cache - should be faster)

### TestRemoteCacheConcurrent
Singleflight deduplication test:
1. Write 1MB object
2. Uncache to remote
3. Launch 10 concurrent reads
4. All should succeed with correct data
5. Only ONE caching operation should run (singleflight)

### TestRemoteCacheLargeObject
Large file test (5MB) to verify chunked transfer works correctly.

### TestRemoteCacheRangeRequest
Tests HTTP range requests work correctly after caching.

### TestRemoteCacheNotFound
Tests proper error handling for non-existent objects.

## Troubleshooting

### View logs
```bash
make logs           # Show recent logs from both servers
make logs-primary   # Follow primary logs in real-time
make logs-remote    # Follow remote logs in real-time
```

### Check server health
```bash
make health
```

### Clean up and retry
```bash
make clean
make test-with-server
```
