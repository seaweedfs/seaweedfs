# Volume Server Integration Tests

This package contains integration tests for SeaweedFS volume server HTTP and gRPC APIs.

## Prerequisites

1. Build the `weed` binary:

```bash
cd /Users/chris/dev/seaweedfs2/weed
go build
```

2. Run tests from repo root:

```bash
go test ./test/volume_server/... -v
```

## Optional environment variables

- `WEED_BINARY`: explicit path to the `weed` executable.
- `VOLUME_SERVER_IT_KEEP_LOGS=1`: keep temporary test directories and process logs.

## Current scope (Phase 0)

- Shared cluster/framework utilities
- Matrix profile definitions
- Initial HTTP admin endpoint checks
- Initial gRPC state/status checks

More API coverage is tracked in `/Users/chris/dev/seaweedfs2/test/volume_server/DEV_PLAN.md`.
