# Remote Object Cache Integration Tests

This directory contains integration tests for the remote object caching feature with singleflight deduplication.

## Test Flow

Each test follows this pattern:
1. **Write to local** - Upload data to primary SeaweedFS (local storage)
2. **Uncache** - Push data to remote storage and remove local chunks
3. **Read** - Read data (triggers caching from remote back to local)

This tests the full remote caching workflow including singleflight deduplication.

## Architecture

```
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

1. **Basic Remote Caching**: Write → Uncache → Read workflow
2. **Singleflight Deduplication**: Concurrent reads only trigger ONE caching operation
3. **Large Object Caching**: 5MB files cache correctly
4. **Range Requests**: Partial reads work with cached objects
5. **Not Found Handling**: Proper error for non-existent objects

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
