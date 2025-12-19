# Metadata Subscribe Integration Tests

This directory contains integration tests for the SeaweedFS metadata subscription functionality.

## Tests

### TestMetadataSubscribeBasic
Tests basic metadata subscription functionality:
- Start a SeaweedFS cluster (master, volume, filer)
- Subscribe to metadata changes
- Upload files and verify events are received

### TestMetadataSubscribeSingleFilerNoStall
Regression test for [issue #4977](https://github.com/seaweedfs/seaweedfs/issues/4977):
- Tests that metadata subscription doesn't stall in single-filer setups
- Simulates high-load file uploads while a subscriber tries to keep up
- Verifies that events are received without significant stalling

The bug was that in single-filer setups, `SubscribeMetadata` would block indefinitely
on `MetaAggregator.MetaLogBuffer` which remains empty (no peers to aggregate from).
The fix ensures that when the buffer is empty, the subscription returns to read from
persisted logs on disk.

### TestMetadataSubscribeResumeFromDisk
Tests that subscription can resume from disk:
- Upload files before starting subscription
- Wait for logs to be flushed to disk
- Start subscription from the beginning
- Verify pre-uploaded files are received from disk

## Running Tests

```bash
# Run all tests (requires weed binary in PATH or built)
go test -v ./test/metadata_subscribe/...

# Skip integration tests
go test -short ./test/metadata_subscribe/...

# Run with increased timeout for slow systems
go test -v -timeout 5m ./test/metadata_subscribe/...
```

## Requirements

- `weed` binary must be available in PATH or in the parent directories
- Tests create temporary directories that are cleaned up after completion
- Tests use ports 9333 (master), 8080 (volume), 8888 (filer)


