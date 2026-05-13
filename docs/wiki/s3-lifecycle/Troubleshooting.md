# S3 Lifecycle — Troubleshooting

Incident-response playbook for the S3 lifecycle worker. For monitoring background, see [S3-Lifecycle-Monitoring](S3-Lifecycle-Monitoring).

## Stuck cursor

**Symptom:** `s3_lifecycle_cursor_min_ts_ns{shard=N}` is not advancing. Heartbeat shows `cursor_lag_max` growing unbounded.

**Cause:** The cursor advance is gated on every match from the event dispatching successfully (`DONE`, `NOOP_RESOLVED`, or `SKIPPED_OBJECT_LOCK`). Any unresolved outcome (`RETRY_LATER`, `BLOCKED`, transport error after in-run retries) halts the run for that shard and persists the cursor at the last fully-processed event. Head-of-line blocking is intentional — it surfaces a real problem rather than silently retrying forever.

**Diagnostics:**

```promql
# Which shard is stuck?
time() * 1e9 - s3_lifecycle_cursor_min_ts_ns

# What outcomes are being returned?
sum by (outcome) (rate(s3_lifecycle_dispatch_total[5m]))
```

Look at worker log for the offending event. The dispatcher logs at `glog.V(1)`:

```
daily_run: RETRY_LATER on <bucket>/<key> EXPIRATION_DAYS
daily_run: BLOCKED on <bucket>/<key> NONCURRENT_DAYS
daily_run: transport error on <bucket>/<key> ABORT_MPU: <err>
```

**Mitigations:**

| Outcome | Root cause | Action |
|---|---|---|
| `RETRY_LATER` (high rate) | Filer or rate limiter is throttling | Lower `cluster_deletes_per_second` to give the filer headroom, or scale filer capacity |
| `BLOCKED FATAL_EVENT_ERROR` | A malformed event the server refuses to dispatch | Check log for the specific reason. May need a code fix; file an issue with the log line |
| `BLOCKED SKIPPED_OBJECT_LOCK` | Object is locked (legal hold, retention) | Wait for lock to expire, or remove lock manually. Cursor advances normally — this isn't stuck. |
| `RPC_ERROR` (sustained) | Transport / network issue | Check S3 server health and filer reachability |

The worker doesn't auto-skip past a stuck event. If you've verified the event is malformed and want to skip it, edit the cursor file directly (`/etc/s3/lifecycle/daily-cursors/shard-NN.json`), advancing `ts_ns` past the bad event's TsNs. Restart the worker.

## Walker stuck (no progress on walker-only rules)

**Symptom:** `s3_lifecycle_daily_run_last_walked_ns{shard=N}` is not advancing. Rules like `Expiration.Date`, `ExpiredObjectDeleteMarker`, `NewerNoncurrent` aren't firing on objects that should be due.

**Causes:**

1. `walker_interval_minutes` is too long for your invocation cadence. Worker runs once per day but interval is set to 48h.
2. Walker is hitting an error mid-walk (filer listing failure). Look for `recovery walk:` or `steady walk:` errors in the heartbeat's `errors=N` count.
3. The bucket has only walker-bound rules and the empty-replay branch's throttle hasn't elapsed.

**Diagnostics:**

```promql
# Walker age per shard
(time() * 1e9 - s3_lifecycle_daily_run_last_walked_ns) / 1e9
```

Check worker config: `walker_interval_minutes` should be ≤ the daily worker schedule interval.

**Mitigation:** lower `walker_interval_minutes`. Setting `0` temporarily forces every pass to walk.

## Test PUT a file with a 1-day rule, didn't expire

The S3 API rejects `Expiration.Days < 1`, so the smallest "expire after N days" you can configure is 1 day. The worker runs once per day by default. Object PUT + 1-day rule + waiting one day is the minimum scenario.

For testing, the in-repo integration suite uses a trick: backdate the entry's `Mtime` via `filer_pb.UpdateEntry` to 30+ days ago. See [test/s3/lifecycle/](https://github.com/seaweedfs/seaweedfs/tree/master/test/s3/lifecycle) for the pattern.

For ad-hoc verification, invoke the worker manually:

```
weed shell -master <addr>
> s3.lifecycle.run-shard -shards 0-15 -s3 <s3-host:port> -refresh 1s -runtime 30s
```

This runs the same code path as the scheduled worker, but driven from your shell rather than the admin scheduler.

## All shards report `errors=16` every pass

**Symptom:** Heartbeat consistently shows `status=error shards=16 errors=16 duration=Ns`.

**Common causes:**

1. **Filer unreachable.** Subscription fails on every shard. Check filer health and gRPC connectivity.
2. **passCtx timeout from `-refresh` loop.** If `-refresh` is less than the pass cap, the timeout fires before the drain completes. This is now treated as "clean end-of-pass" — it should not show as errors=N. If you see this on a build before #9481, upgrade.
3. **Bucket walker is timing out.** Big bucket, walker hits ctx deadline. Increase `max_runtime_minutes`.

## Some objects expired, others didn't (same rule)

**Symptom:** Two objects matching the same rule with the same age — one is deleted, the other isn't.

**Common causes:**

1. **The non-deleted object's mtime is wrong.** Check the entry's mtime — it might be more recent than you expect (e.g., a recent metadata update bumped it).
2. **The objects are on different shards** and one shard has a stuck cursor while the other doesn't. Check `s3_lifecycle_cursor_min_ts_ns` per shard.
3. **`Filter` doesn't match what you think.** A prefix-only filter requires the object key to start with that prefix; a tag filter requires the matching tag. Verify with `aws s3api head-object` (returns tags via `--query`).
4. **Object lock or retention.** The dispatcher returns `SKIPPED_OBJECT_LOCK` for protected objects. Check `s3_lifecycle_dispatch_total{outcome="SKIPPED_OBJECT_LOCK"}`.

## How to read the cursor files

Cursors live at `/etc/s3/lifecycle/daily-cursors/shard-NN.json` on the filer. Read with the filer's `read` API or `weed shell`:

```
weed shell -master <addr>
> fs.cat /etc/s3/lifecycle/daily-cursors/shard-00.json
```

Schema:

```json
{
  "version": 1,
  "shard_id": 0,
  "ts_ns": 1715600000000000000,
  "rule_set_hash": "<base64 32 bytes>",
  "promoted_hash": "<base64 32 bytes>",
  "last_walked_ns": 1715620000000000000
}
```

- `ts_ns == 0` means "no replay progress" — either cold start or a bucket whose rules are all walker-only.
- `last_walked_ns == 0` (or absent) means "never walked steady-state". Next pass will walk.

Manually editing the cursor is supported as an escape hatch but obviously breaks the invariant that "everything before persisted.TsNs has been processed under the same rules." Use sparingly.

## Resetting a shard

If a shard's cursor is corrupted or wedged in an unrecoverable state:

```
weed shell -master <addr>
> fs.rm /etc/s3/lifecycle/daily-cursors/shard-07.json
```

Next pass treats the shard as cold start: recovery walker fires over `RecoveryView(snap)`, then the cursor seeds at `runNow - maxTTL`. This re-replays a `maxTTL`-wide window of meta-log events. Identity-CAS on the server side makes redundant deletes no-ops, so re-replay is safe.

## Suspending the worker

The admin UI's plugin scheduler allows pausing the `s3_lifecycle` job type. The worker won't be invoked while paused; cursors are preserved, and on resume the next pass picks up at the persisted state.

For a single-bucket "stop deleting from this bucket" without pausing the worker:

```bash
aws --endpoint $S3_ENDPOINT s3api delete-bucket-lifecycle --bucket my-bucket
```

The worker reads the empty rule set on the next pass; `rsh == [32]byte{}` causes the empty-replay branch to run only the walker (which has nothing to walk for an empty rule set) and exit. No deletes.

## Reverting

If something goes very wrong, the streaming worker (the previous design) is no longer in the codebase. Revert is via downgrading the binary. The cursor format has a `version` field — versions `>1` would fail-loud on load by an older binary that only knows version `1`. Currently version `1` is the only version.

For escape-hatch operations (delete all cursors, suspend worker globally), prefer pausing via the admin UI over force-killing the worker process; the worker exits cleanly between passes.
