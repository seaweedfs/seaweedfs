# S3 Lifecycle

SeaweedFS implements the S3 `PutBucketLifecycleConfiguration` API. Configured rules are evaluated and enforced by a worker that runs as a scheduled job and exits when each pass completes.

This page is the operator-facing entry point. Developers and architecture readers should see [`weed/s3api/s3lifecycle/DESIGN.md`](https://github.com/seaweedfs/seaweedfs/blob/master/weed/s3api/s3lifecycle/DESIGN.md).

## Supported rule shapes

| Action | Supported | Notes |
|---|---|---|
| `Expiration.Days` | Yes | Latest-version PUT clock. |
| `Expiration.Date` | Yes | Walker path; fires once date is reached. |
| `Expiration.ExpiredObjectDeleteMarker` | Yes | Walker path; sibling-aware. |
| `NoncurrentVersionExpiration.NoncurrentDays` | Yes | Clock starts at the demoting PUT, not the entry's own mtime. |
| `NoncurrentVersionExpiration.NewerNoncurrentVersions` | Yes | Walker path; version-list aware. |
| `AbortIncompleteMultipartUpload.DaysAfterInitiation` | Yes | |
| `Filter.Prefix` | Yes | |
| `Filter.Tag` | Yes | |
| `Filter.ObjectSizeGreaterThan` / `ObjectSizeLessThan` | Yes | |
| `Filter.And` (composite) | Yes | |
| `Transitions` (storage-class) | No | SeaweedFS doesn't model multiple storage classes; no-op. |

## What the worker does

1. Reads bucket lifecycle XML from each bucket's metadata.
2. Compiles rules into a per-shard partition (replay-eligible vs. walker-bound).
3. Subscribes to the filer meta-log (one stream covering all 16 shards in this worker process).
4. For each event whose ActionKind is replay-eligible (`ExpirationDays`, `NoncurrentDays`, `AbortMPU`), checks whether its DueTime has elapsed and dispatches `LifecycleDelete` if so.
5. For each walker-bound rule (`ExpirationDate`, `ExpiredObjectDeleteMarker`, `NewerNoncurrent`, or anything promoted to scan-only), iterates the bucket and evaluates each entry against current state.
6. Persists per-shard cursors so the next pass resumes where this one left off.

The worker exits when the pass is done. The admin scheduler invokes it on a daily cadence by default; operators can change that via the standard plugin scheduler config.

## Quick references

- **[Operator Guide](S3-Lifecycle-Operator-Guide)** — config knobs, defaults, when to change them.
- **[Monitoring](S3-Lifecycle-Monitoring)** — Prometheus metrics, heartbeat log line, what a healthy run looks like.
- **[Troubleshooting](S3-Lifecycle-Troubleshooting)** — stuck cursor, missing deletes, head-of-line blocking.
- **[Architecture](S3-Lifecycle-Architecture)** — high-level overview of the worker, engine, and dispatch path.
