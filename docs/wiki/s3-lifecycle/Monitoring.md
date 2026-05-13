# S3 Lifecycle — Monitoring

This page lists the Prometheus signals the worker exposes and how to read the heartbeat log line. For incident response, see [S3-Lifecycle-Troubleshooting](S3-Lifecycle-Troubleshooting).

## Prometheus metrics

All labels are in `weed/stats/metrics.go` under the `s3_lifecycle` subsystem.

### Per-shard gauges

| Metric | Labels | What |
|---|---|---|
| `s3_lifecycle_cursor_min_ts_ns` | `shard` | UnixNano of the last meta-log event whose matches all dispatched successfully on this shard |
| `s3_lifecycle_daily_run_last_walked_ns` | `shard` | UnixNano of the most recent successful walker fire |

Derived queries:

```promql
# Per-shard replay lag in seconds
(time() * 1e9 - s3_lifecycle_cursor_min_ts_ns) / 1e9

# Per-shard walker freshness in seconds
(time() * 1e9 - s3_lifecycle_daily_run_last_walked_ns) / 1e9

# Worst-shard lag across the cluster
max(time() * 1e9 - s3_lifecycle_cursor_min_ts_ns) / 1e9
```

Zero values mean "not started yet" — distinct from "0s caught up". The heartbeat line uses `cold` as the marker for that state.

### Counters

| Metric | Labels | What |
|---|---|---|
| `s3_lifecycle_dispatch_total` | `bucket`, `kind`, `outcome` | Per-bucket dispatch counter, partitioned by action kind and server outcome |
| `s3_lifecycle_daily_run_events_scanned_total` | `shard` | Meta-log events `drainShardEvents` processed |
| `s3_lifecycle_bootstrap_dispatch_total` | `bucket`, `kind` | Walker dispatch counter |
| `s3_lifecycle_metadata_only_total` | `bucket`, `rule_hash` | Successful deletes that took the metadata-only path |

`outcome` values: `DONE`, `NOOP_RESOLVED`, `SKIPPED_OBJECT_LOCK`, `RETRY_LATER`, `BLOCKED`, `LIFECYCLE_DELETE_OUTCOME_UNSPECIFIED`, `RPC_ERROR`. The first three are success outcomes that advance the cursor; the others halt the run.

### Histograms

| Metric | What |
|---|---|
| `s3_lifecycle_daily_run_shard_duration_seconds{shard}` | Wall-clock per shard pass. p95 climbing toward `max_runtime_minutes` means the shard is brushing its budget. |
| `s3_lifecycle_dispatch_limiter_wait_seconds` | Time spent waiting on the cluster rate limiter before issuing `LifecycleDelete`. Near-zero = cap not binding; long-tail at `1/rate` = cap is the active throttle. |

## Heartbeat log line

Emitted at the end of every `dailyrun.Run` invocation, at `glog.V(0)` (default verbosity):

```
daily_run: status=ok shards=16 errors=0 duration=7s cursor_lag_max=2m walked_max_age=3m
```

Tokens are space-separated `key=value` for grep / log-aggregator filtering. Stable across versions:

| Token | Meaning |
|---|---|
| `status=ok` or `status=error` | Whether any shard returned an error |
| `shards=N` | Number of shards processed this pass |
| `errors=N` | Per-shard error count |
| `duration=Ns` | Wall-clock for the whole pass |
| `cursor_lag_max=...` | Worst per-shard replay lag, or `cold` if no shard has a persisted cursor yet |
| `walked_max_age=...` | Worst per-shard walker age, or `cold` if no shard has walked yet |

A healthy production heartbeat looks like:

```
daily_run: status=ok shards=16 errors=0 duration=12.3s cursor_lag_max=45s walked_max_age=58m
```

Read it as: 16 shards finished cleanly in 12 seconds; the worst-case replay lag is 45 seconds behind real-time; the oldest walker fire on any shard is 58 minutes ago (so `walker_interval_minutes=60` is roughly honored).

## Anti-patterns to alert on

| Pattern | Meaning | What to do |
|---|---|---|
| `cursor_lag_max` grows unbounded | Stuck cursor; head-of-line blocking on some shard | See [Troubleshooting → Stuck cursor](S3-Lifecycle-Troubleshooting#stuck-cursor) |
| `walked_max_age` exceeds `walker_interval_minutes × 2` | Walker isn't firing as configured | Check `errors=N` in heartbeat and `s3_lifecycle_dispatch_total{outcome="RPC_ERROR"}` |
| `errors=16` (all shards) on every pass | Filer is unreachable or returning errors | Check filer health |
| `s3_lifecycle_dispatch_total{outcome="RETRY_LATER"}` rising fast | Server rate-limited or filer overloaded | Lower `cluster_deletes_per_second` or add capacity |
| `s3_lifecycle_dispatch_total{outcome="BLOCKED"}` non-zero | Programmatic event content error | Check worker logs for `FATAL_EVENT_ERROR` |
| `duration=Ns` ramping up across passes | Walker is firing too often | Set `walker_interval_minutes` |

## Suggested alerts

```yaml
- alert: S3LifecycleCursorLagHigh
  expr: max(time() * 1e9 - s3_lifecycle_cursor_min_ts_ns) / 1e9 > 3600
  for: 30m
  annotations:
    summary: "S3 lifecycle replay lag > 1h on shard {{ $labels.shard }}"
    runbook: https://github.com/seaweedfs/seaweedfs/wiki/S3-Lifecycle-Troubleshooting#stuck-cursor

- alert: S3LifecycleWalkerStuck
  expr: max(time() * 1e9 - s3_lifecycle_daily_run_last_walked_ns) / 1e9 > 86400
  for: 1h
  annotations:
    summary: "S3 lifecycle walker hasn't run in > 24h"
    runbook: https://github.com/seaweedfs/seaweedfs/wiki/S3-Lifecycle-Troubleshooting#walker-stuck

- alert: S3LifecycleDispatchFailures
  expr: |
    rate(s3_lifecycle_dispatch_total{outcome=~"RETRY_LATER|BLOCKED|RPC_ERROR"}[5m]) > 0.1
  for: 15m
  annotations:
    summary: "S3 lifecycle delete failure rate > 0.1/s"
```

Adjust thresholds to your cluster's normal levels — these are starting points.
