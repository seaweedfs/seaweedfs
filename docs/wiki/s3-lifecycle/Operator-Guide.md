# S3 Lifecycle — Operator Guide

This page covers the admin and worker config knobs for the S3 lifecycle worker, plus when to change each one.

For monitoring guidance, see [S3-Lifecycle-Monitoring](S3-Lifecycle-Monitoring). For incident response, see [S3-Lifecycle-Troubleshooting](S3-Lifecycle-Troubleshooting).

## Configuration

All keys are set through the admin UI's plugin config for `s3_lifecycle`.

### Admin config

| Key | Type | Default | When to change |
|---|---|---|---|
| `cluster_deletes_per_second` | int64 | `0` (unlimited) | Set a positive value when lifecycle deletes are causing filer contention. Allocated evenly across active workers at job dispatch. |
| `cluster_deletes_burst` | int64 | `0` (= 2× rate) | Adjust if delete bursts overload the filer faster than the per-second rate allows. |
| `meta_log_retention_days` | int64 | `0` (treated as unbounded) | Stock SeaweedFS doesn't GC the meta-log, so the default is fine. Set positive if your deployment manually trims `/topics/.system/log` — then rules with TTL > retention will route through the walker. |
| `walker_interval_minutes` | int64 | `0` (fire every pass) | **Important.** See "Walker interval" below — most production deployments should set this to a positive value. |

### Worker config

| Key | Default | What |
|---|---|---|
| `max_runtime_minutes` | 60 | Wall-clock cap per `dailyrun.Run` invocation. The pass returns early if it hits this. |

### Detection schedule

The admin scheduler's `DetectionIntervalMinutes` for `s3_lifecycle` is `1440` by default — once per day. Each detection produces one execution. Change in the admin UI's runtime defaults for the job type.

## Walker interval

The walker is the part of the worker that lists bucket contents and evaluates them against rules. It fires:

- **Always**, on cold start (no persisted cursor) and on rule changes — these are bounded events.
- **Periodically**, in steady state, gated by `walker_interval_minutes`.

The throttle exists because walker cost is bucket size, not event rate. If the worker is scheduled at a tighter cadence than the desired walk frequency (CI, sub-hourly admin schedules, manual runs), the steady-state walker would crush the filer with a full subtree scan per invocation.

Recommended values by cluster size:

| Cluster | Recommended `walker_interval_minutes` |
|---|---|
| Small (≤1M objects, single bucket) | 60 |
| Medium (≤100M objects, mixed buckets) | 360 (6h) |
| Large (≥100M objects) | 1440 (24h) |
| Testing / CI | 0 (fire every pass) |

Setting `0` keeps the prior behavior — fire on every invocation. That's appropriate when the worker is scheduled at exactly the desired walk frequency (e.g., once per day) and the in-repo integration tests rely on it.

A negative value is rejected at worker start (loud error rather than silent fall-through to "walk every pass").

## Per-bucket lifecycle XML

Set via the standard S3 API:

```bash
aws --endpoint $S3_ENDPOINT s3api put-bucket-lifecycle-configuration \
  --bucket my-bucket \
  --lifecycle-configuration file://lifecycle.json
```

```jsonc
// lifecycle.json
{
  "Rules": [
    {
      "ID": "expire-logs-after-30d",
      "Status": "Enabled",
      "Filter": { "Prefix": "logs/" },
      "Expiration": { "Days": 30 }
    },
    {
      "ID": "abort-stuck-mpu",
      "Status": "Enabled",
      "Filter": {},
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
    },
    {
      "ID": "keep-3-versions",
      "Status": "Enabled",
      "Filter": { "Prefix": "versioned/" },
      "NoncurrentVersionExpiration": { "NewerNoncurrentVersions": 3 }
    }
  ]
}
```

The worker picks up rule changes on the next pass. Replay-eligible rule edits trigger a one-time recovery walk to catch already-due objects under the new rule.

## Verifying a rule is working

After applying a rule:

1. Wait one detection interval (default 24h) plus one walker interval.
2. Read `s3_lifecycle_dispatch_total{bucket="my-bucket"}` — the counter should advance.
3. Verify a target object is gone: `aws s3 head-object --bucket my-bucket --key <expected-expired>` should return 404.

For testing without waiting, the `weed shell` command supports manual invocation:

```text
weed shell -master <host:http_port.grpc_port>
> s3.lifecycle.run-shard -shards 0-15 -s3 <s3-host:port> -refresh 1s -runtime 30s
```

This is exactly what the CI integration suite uses. See [test/s3/lifecycle/](https://github.com/seaweedfs/seaweedfs/tree/master/test/s3/lifecycle) for examples.

## Rate limit allocation

The cluster delete cap is allocated per-worker at job dispatch:

```text
per_worker_rate = cluster_deletes_per_second / count(active_s3_lifecycle_workers)
```

Brief over/undershoot during worker join/leave is acceptable for a bulk workload. The allocation is recomputed each detection cycle, so adding workers smooths out within one day.

`cluster_deletes_burst` is divided the same way. `0` is treated as `2 × rate` (a token bucket reasonable default).

## Common change patterns

### Tightening a TTL (e.g., 60d → 30d)

The new rule hash differs from the persisted hash — next pass triggers a recovery walk over `RecoveryView`. Already-due objects under the new shorter TTL get caught. The cursor rewinds to `runNow - new_maxTTL`. No operator action needed beyond updating the XML.

### Adding a walker-only rule (`Expiration.Date`)

`RuleSetHash` and `PromotedHash` are unchanged (walker-only rules aren't in the replay hash). The replay cursor stays put. The walker reads the updated rule set on its next steady-state fire (or immediately, if invoked manually).

### Operator forgot to set `walker_interval_minutes`

Symptom: heartbeat log line shows `duration` ramping up across passes (each pass does a full subtree walk). Mitigation: set the throttle to a value matching your scheduling cadence.

### Filer is GC'ing meta-log (custom deployment)

Set `meta_log_retention_days` to the GC retention. Rules with TTL > retention will route through the walker (`PromotedHash` will be non-empty). The first run after this change triggers a recovery walk.
