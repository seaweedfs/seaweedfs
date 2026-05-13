# S3 Lifecycle

SeaweedFS implements the S3 `PutBucketLifecycleConfiguration` API. Configured rules are evaluated and enforced by a worker that runs as a scheduled job and exits when each pass completes.

This page is the operator-facing entry point. Developers and architecture readers should see [`weed/s3api/s3lifecycle/DESIGN.md`](https://github.com/seaweedfs/seaweedfs/blob/master/weed/s3api/s3lifecycle/DESIGN.md).

## Supported features

| Feature | Status | Notes |
|---|---|---|
| `Expiration.Days` | Yes | Latest-version PUT clock |
| `Expiration.Date` | Yes | Walker path; fires once date is reached |
| `Expiration.ExpiredObjectDeleteMarker` | Yes | Walker path; sibling-aware |
| `NoncurrentVersionExpiration.NoncurrentDays` | Yes | Clock starts at the demoting PUT, not the entry's own mtime |
| `NoncurrentVersionExpiration.NewerNoncurrentVersions` | Yes | Walker path; version-list aware |
| `AbortIncompleteMultipartUpload.DaysAfterInitiation` | Yes | |
| `Filter.Prefix` | Yes | |
| `Filter.Tag` | Yes | |
| `Filter.ObjectSizeGreaterThan` / `ObjectSizeLessThan` | Yes | |
| `Filter.And` (composite) | Yes | |
| `Transition` / `NoncurrentVersionTransition` | No | SeaweedFS doesn't model storage class tiers |

## API endpoints

```text
PUT    /{bucket}?lifecycle   # PutBucketLifecycleConfiguration
GET    /{bucket}?lifecycle   # GetBucketLifecycleConfiguration
DELETE /{bucket}?lifecycle   # DeleteBucketLifecycle
```

## Example: AWS CLI

```bash
# Set lifecycle configuration
aws s3api put-bucket-lifecycle-configuration \
  --endpoint-url http://localhost:8333 \
  --bucket my-bucket \
  --lifecycle-configuration '{
    "Rules": [
      {
        "ID": "expire-old",
        "Status": "Enabled",
        "Filter": { "Prefix": "" },
        "Expiration": { "Days": 90 },
        "NoncurrentVersionExpiration": { "NoncurrentDays": 30 },
        "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
      }
    ]
  }'

# Get lifecycle configuration
aws s3api get-bucket-lifecycle-configuration \
  --endpoint-url http://localhost:8333 \
  --bucket my-bucket

# Delete lifecycle configuration
aws s3api delete-bucket-lifecycle \
  --endpoint-url http://localhost:8333 \
  --bucket my-bucket
```

## Example: Terraform

```hcl
resource "aws_s3_bucket_lifecycle_configuration" "example" {
  bucket = aws_s3_bucket.example.id

  rule {
    id     = "expire-logs"
    status = "Enabled"

    filter {
      prefix = "logs/"
    }

    expiration {
      days = 30
    }

    noncurrent_version_expiration {
      noncurrent_days           = 7
      newer_noncurrent_versions = 2
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
```

## How it works

The lifecycle worker is a scheduled job (default daily). Each invocation:

1. Reads bucket lifecycle XML from each bucket's metadata.
2. Compiles rules into a per-shard partition (replay-eligible vs. walker-bound).
3. Subscribes to the filer meta-log — one stream covering all 16 shards in this worker process.
4. For replay-eligible actions (`ExpirationDays`, `NoncurrentDays`, `AbortMPU`), checks each event's DueTime and dispatches `LifecycleDelete` if elapsed.
5. For walker-bound rules (`ExpirationDate`, `ExpiredObjectDeleteMarker`, `NewerNoncurrent`, or anything promoted to scan-only), iterates the bucket and evaluates each entry against current state.
6. Persists per-shard cursors so the next pass resumes where this one left off.

The worker exits when the pass is done. The admin scheduler invokes it on a daily cadence by default; operators can change that via the standard plugin scheduler config.

## Versioning integration

Lifecycle rules interact with [S3 Object Versioning](S3-Object-Versioning):

- **`NoncurrentVersionExpiration`** only applies to versioned buckets. Non-current versions are deleted after `NoncurrentDays` days since they were superseded (the demoting PUT's TsNs, not the version's own mtime). `NewerNoncurrentVersions` retains the N newest non-current versions.
- **`ExpiredObjectDeleteMarker`** removes delete markers that are the sole remaining version of an object (no non-current versions behind them).
- **`Expiration.Days`** on a versioned bucket creates a delete marker when the current version expires; it does not permanently delete the object.

## Quick references

- **[Operator Guide](S3-Lifecycle-Operator-Guide)** — config knobs, defaults, when to change each
- **[Monitoring](S3-Lifecycle-Monitoring)** — Prometheus metrics, heartbeat log line, what a healthy run looks like
- **[Troubleshooting](S3-Lifecycle-Troubleshooting)** — stuck cursor, missing deletes, head-of-line blocking
- **[Architecture](S3-Lifecycle-Architecture)** — high-level overview of the worker, engine, and dispatch path
