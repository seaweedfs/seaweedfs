# S3 Lifecycle (Design)

The S3 lifecycle worker replaces the streaming + heap design with a daily meta-log replay. The worker runs as a scheduled job: "start, do today's work, stop" — no long-running per-shard goroutines, no future-buffered match heap.

This document is the as-built reference. For operator-facing guides, see `docs/wiki/s3-lifecycle/`.

## Goal

For each bucket lifecycle rule with TTL `D` days, delete every object whose age exceeds `D`, exactly once per scheduled run, with the worker exiting when the pass completes. No future-buffered Matches in memory. Cluster-wide delete rate cap allocated per worker.

## Algorithm

One pass per scheduled invocation. The pass spawns one filer meta-log subscription, fans events out to per-shard processors by `ShardID(bucket, key)`, and drains each shard's events through the router + dispatcher:

```text
dailyrun.Run(ctx, cfg):
    runNow             = cfg.Now()                                  // frozen for the whole pass
    snap               = cfg.Engine.Snapshot()
    rsh                = engine.ReplayContentHash(snap)
    maxTTL             = engine.MaxEffectiveTTL(snap)

    if rsh != [32]byte{}:                                           // replay-eligible rules present
        globalStartTsNs = min over cfg.Shards of (persisted cursor or runNow - maxTTL)
        reader          = subscribeMeta(ShardPredicate ∈ cfg.Shards, StartTsNs = globalStartTsNs)
        fanOut(reader.Events → shardEvents[shardID]) until ev.TsNs > runNow

    spawn one goroutine per shard:
        runShard(ctx, cfg, snap, runNow, shardID, shardEvents[shardID])

    wait all
    teardown reader + fan-out

runShard(ctx, cfg, snap, runNow, shardID, events):
    persisted, found   = cfg.Persister.Load(shardID)
    retentionWindow    = cfg.RetentionWindow or maxTTL              // see "retention" below
    promoted           = engine.PromotedHash(snap, retentionWindow)

    if rsh == [32]byte{}:                                           // pure walker bucket
        if walkerDue and Walker:
            cfg.Walker(walkView)                                    // RulesForShard.walk
            lastWalkedNs = runNow.UnixNano()
        save cursor (TsNs=0, rsh, promoted, lastWalkedNs)
        return

    mustWalkRecovery   = found && (persisted.RuleSetHash != rsh || persisted.PromotedHash != promoted)
    mustWalkColdStart  = !found

    if mustWalkRecovery or mustWalkColdStart:
        cfg.Walker(engine.RecoveryView(snap))                       // every rule, force-active
        walkedThisPass = true
        lastWalkedNs   = runNow.UnixNano()
        if mustWalkRecovery:
            save cursor (TsNs = runNow - maxTTL, rsh, promoted, lastWalkedNs)  // rewind
            return

    if Walker and !walkedThisPass and walkerDue:
        cfg.Walker(walkView)                                        // steady-state walker
        lastWalkedNs = runNow.UnixNano()

    startTsNs = !found ? runNow - maxTTL : persisted.TsNs           // steady state honors cursor
    lastOK, _, drainErr = drainShardEvents(ctx, cfg, runNow, shardID, snap, startTsNs, events)
    save cursor (TsNs = lastOK, rsh, promoted, lastWalkedNs)
```

Walker invocations — three call sites, distinct conditions:

| Branch | View | Trigger | Throttle |
|---|---|---|---|
| Recovery | `engine.RecoveryView(snap)` | `mustWalkColdStart` or `mustWalkRecovery` | Unconditional |
| Steady state | `RulesForShard.walk` | `!walkedThisPass && walkerDue` | `WalkerInterval` |
| Empty replay | `RulesForShard.walk` | `rsh == [32]byte{}` and `walkerDue` | `WalkerInterval` |

`walkerDue` returns true when `WalkerInterval == 0`, or when no walk has happened yet (`LastWalkedNs == 0`), or when `runNow - LastWalkedNs >= WalkerInterval`. Within-pass double-fire suppression lives in `runShard`'s `walkedThisPass` flag, not in `walkerDue` — the recovery branch fires the walker with `RecoveryView` (a superset of every per-shard partition), so the steady-state branch must not re-walk in the same pass.

## Engine surface

```go
// In engine:
func (e *Engine) Snapshot() *Snapshot
func (s *Snapshot) RulesForShard(shardID int, retentionWindow time.Duration) (replay, walk *Snapshot)
func RecoveryView(s *Snapshot) *Snapshot
func ReplayContentHash(s *Snapshot) [32]byte
func PromotedHash(s *Snapshot, retentionWindow time.Duration) [32]byte
func MaxEffectiveTTL(s *Snapshot) time.Duration
```

`RulesForShard` and `RecoveryView` return new `*Snapshot` instances with cloned `*CompiledAction` objects and shared (by pointer) rule definitions. Fields that differ from the base:

- **`active`** — per clone, set per view.
- **`Mode`** — rewritten to `ModeEventDriven` on `replay` clones; preserved on `walk` and `recovery` clones. The rewrite is required because `router.Route` gates on `Mode == ModeEventDriven`, and today's compile preserves a persistent `prior.Mode = ModeScanOnly` that would otherwise lock a rule out of replay even after retention rehabilitates it.
- **Action-map membership** — `replay` contains only replay-eligible clones; `walk` contains only walker-bound clones; `recovery` contains every action.

Shared-by-pointer with the base: `Rule` definitions, predicate maps, `RuleHash` table.

## `router.Route` integration

`router.Route(ctx, snap, ev, now, lister)` iterates every action with `IsActive() == true` in the snapshot. Two snapshots can't disagree on activation if they share the same `*CompiledAction` pointers, which is why `RulesForShard` clones.

| View | Clone settings | Why |
|---|---|---|
| `replay` | `active = true`, `Mode = ModeEventDriven` | `router.Route` requires `ModeEventDriven`. Forced regardless of `prior.Mode`. |
| `walk` | `active = true`, `Mode` preserved | Walker accepts any non-`ModeDisabled` Mode. |
| `recovery` | `active = true`, `Mode` preserved | Walker iterates all action clones. |

## Subscription model

One filer `SubscribeMetadata` stream per `dailyrun.Run()` call, covering every shard in `cfg.Shards`. The `Reader` carries a `ShardPredicate func(int) bool` that accepts the shard set; a fan-out goroutine routes events to per-shard channels by `ev.ShardID`. This replaces the earlier model (16 separate per-shard subscriptions per pass).

`globalStartTsNs = min(per-shard cursor, runNow - maxTTL)`. Pre-loaded once at pass start so the subscription's `StartTsNs` covers every shard's needed range; per-shard drains then filter `ev.TsNs <= shard.startTsNs` locally.

Fan-out cancels the reader on the first `ev.TsNs > runNow` (meta-log events arrive in TsNs order; everything after is past the pass boundary). Per-shard channels are buffered to 256 events — large enough to absorb bursts without back-pressuring the fan-out.

## Action kinds and dispatch paths

Table uses the S3-spec rule names (what operators type in lifecycle XML). The corresponding engine constants in `weed/s3api/s3lifecycle/action_kind.go` are `ActionKindExpirationDays`, `ActionKindNoncurrentDays`, `ActionKindAbortMPU`, `ActionKindExpirationDate`, `ActionKindExpiredDeleteMarker`, `ActionKindNewerNoncurrent` — same one-to-one mapping, shorter spelling.

| ActionKind | Trigger | Due time | Path | Early-stop in replay? |
|---|---|---|---|---|
| `ExpirationDays` | Latest-version PUT | `ev.TsNs + r.ExpirationDays` | Replay | Yes |
| `NoncurrentDays` | Demotion (next PUT for same key) | `entry.NoncurrentSince + r.NoncurrentDays` | Replay | Yes |
| `AbortIncompleteMultipartUpload` | MPU init | `mpu_init.TsNs + r.AbortMPUDaysAfterInitiation` | Replay | Yes |
| `ExpirationDate` | Latest-version PUT, fires on `now >= r.ExpirationDate` | `r.ExpirationDate` (constant) | Walker | n/a |
| `ExpiredObjectDeleteMarker` | Delete marker with `NumVersions == 1` | "now if orphaned, else never" | Walker | n/a |
| `NewerNoncurrentVersions` | Version becomes noncurrent AND total noncurrents > `r.NewerNoncurrentVersions` | "now if over the cap, else never" | Walker | n/a |

`ExpiredObjectDeleteMarker` and `NewerNoncurrentVersions` are walker-only because their due-time depends on current sibling state, not on any event's TsNs. The `done` early-stop in replay can't engage — there's nothing event-time-monotonic to early-stop on.

## Cursor

Persisted per shard at `/etc/s3/lifecycle/daily-cursors/shard-NN.json`. Shape (`weed/s3api/s3lifecycle/dailyrun/cursor.go`):

```go
type Cursor struct {
    TsNs         int64       // last meta-log event whose matches all dispatched
    RuleSetHash  [32]byte    // ReplayContentHash of the rule set that wrote this
    PromotedHash [32]byte    // PromotedHash with retentionWindow at write time
    LastWalkedNs int64       // wall-clock of the last successful walker fire
}
```

`LastWalkedNs` is JSON-omitempty, so cursor files written before that field existed decode cleanly as zero (treated as "never walked steady-state" → next pass seeds the anchor).

Cursor save uses a fresh `context.Background()` with a 5s timeout because the steady-state drain exits via passCtx cancellation (the only way an idle subscription ends). Saving with the canceled passCtx would silently drop the cursor and the next pass would re-replay from the same floor.

In steady state the start position honors the cursor verbatim — the floor `runNow - maxTTL` is applied only on cold start (`!found`). The drain freezes the cursor at the last pre-skip event so pending matches with `DueTime == TsNs + maxTTL` stay in scope across passes; bumping forward in steady state would orphan exactly those events.

## Cursor hashes

The cursor stores two hashes that together detect every situation invalidating "everything before persisted.TsNs has been processed under the same rules":

**`RuleSetHash = engine.ReplayContentHash(snap)`** — content over the rule definitions (action kind, predicate, TTL value) of replay-eligible action kinds. Partition-independent.

**`PromotedHash = engine.PromotedHash(snap, retentionWindow)`** — hash of replay-eligible rules currently classified as `walk` due to `scan_only` promotion (their TTL exceeds `retentionWindow`).

Recovery triggers, complete list:

| Trigger | Detection | Why |
|---|---|---|
| Cold start | No persisted cursor | First run for this shard |
| Replay-rule edit | `RuleSetHash` mismatch | Replay-eligible rule content changed |
| Partition flip | `PromotedHash` mismatch | A replay-eligible rule moved between `replay` and `walk` |

Retention loss as a recovery trigger is a known gap: in stock SeaweedFS the filer's meta-log is effectively never GC'd (no on-disk retention policy for `/topics/.system/log`), so `cfg.RetentionWindow` defaults to `maxTTL` and PromotedHash stays empty. When operators add explicit meta-log retention, the cursor-vs-earliest-available check becomes load-bearing again.

## Walker throttle

`cfg.WalkerInterval` decouples the walker's cadence from `Run()` invocation cadence. The s3tests CI workflow invokes the worker every 2s; a single daily admin schedule invokes it once per day. The walker fire rate should be set by walk cost, not by the invocation interval — so:

- Steady-state and empty-replay walker fires gate on `walkerDue(persisted.LastWalkedNs, runNow, WalkerInterval)`.
- `0` keeps the prior "fire every pass" behavior (back-compat for tests).
- Production: pick the walk cost budget per shard per cluster. Small cluster: 1h. Large cluster: 6h+.
- Recovery walker fires (cold-start, hash mismatch) are unconditional — these are bounded events that must run once.

Walker fires update `Cursor.LastWalkedNs` so the next pass's throttle has a fresh anchor. The recovery walker also updates it, so a steady-state branch in the same pass doesn't double-walk over the same superset.

## Delete failure handling

Cursor advance is gated on success. The cursor only moves past events whose matches all returned `DONE`, `NOOP_RESOLVED`, or `SKIPPED_OBJECT_LOCK`. Any other outcome (`RETRY_LATER`, `BLOCKED`, transport error after in-run retries) halts the run and persists the cursor at the last fully-processed event.

- **Head-of-line blocking is intentional.** A transient filer error stalls today's pass; tomorrow's run resumes at the same cursor. Loud (operator sees stuck cursor in metrics) and idempotent (identity-CAS makes redundant deletes no-ops).
- **In-run retry with backoff** for transport errors only — default 3 attempts, exponential backoff capped at 5s. Server-side outcomes are not retried in-run.
- **No retry queue.** Removing the per-key freeze state was the whole point; adding it back would re-introduce the state machine the redesign replaces.

## Rate limiting

Cluster-wide deletes-per-second cap, set in admin config. The admin allocator:

1. Counts workers capable of `s3_lifecycle` from the registry.
2. Divides `cluster_deletes_per_second` by the count.
3. Writes the per-worker share into `ExecuteJobRequest.ClusterContext.Metadata["s3_lifecycle.deletes_per_second"]`.

The worker reads the share and constructs one `golang.org/x/time/rate.Limiter` shared across all shard goroutines. `dispatchWithRetry` calls `limiter.Wait(ctx)` before each `LifecycleDelete` RPC.

## Observability

Per-shard Prometheus gauges (`weed/stats/metrics.go`):

| Metric | What it tells you |
|---|---|
| `s3_lifecycle_cursor_min_ts_ns{shard}` | `now - this` is the per-shard replay lag |
| `s3_lifecycle_daily_run_last_walked_ns{shard}` | `now - this` is walker freshness; stuck = throttle misconfigured or walker failing |
| `s3_lifecycle_daily_run_shard_duration_seconds{shard}` | Wall-clock per shard pass |
| `s3_lifecycle_daily_run_events_scanned_total{shard}` | Counter of meta-log events drainShardEvents processed |
| `s3_lifecycle_dispatch_limiter_wait_seconds` | Per-dispatch wait time on the cluster rate limiter |
| `s3_lifecycle_dispatch_total{bucket,kind,outcome}` | Per-bucket dispatch counter |

Heartbeat log line, emitted once per `Run()`:

```text
daily_run: status=ok shards=16 errors=0 duration=7s cursor_lag_max=2h walked_max_age=3m
```

Tokens `status`, `shards`, `errors`, `duration` are stable for grep. `cursor_lag_max=cold` and `walked_max_age=cold` distinguish "not started yet" from "0s caught up".

## Data model

### `noncurrent_since` on version entries

A non-current version's TTL clock starts when the next version was written, not at its own mtime. The demoting PUT writes `NoncurrentSinceNs` on the demoted entry, set to the TsNs of the demoting meta-log event. Using the meta-log TsNs keeps `noncurrent_since` strictly monotonic in meta-log order across all replicas, immune to wall-clock skew.

The lifecycle evaluator uses `ev.TsNs` for current-version rules and `entry.NoncurrentSinceNs` for noncurrent rules — both monotonic in iteration order. Legacy entries with `NoncurrentSinceNs == 0` fall back to entry mtime.

`expected_mtime` passed to `LifecycleDelete` (for identity CAS) is always the entry's own mtime. CAS identity and TTL clock are separate concerns.

## Components

| Path | Role |
|---|---|
| `engine/` | Rule compilation, snapshot, partition views |
| `evaluate.go`, `due_at.go`, `rule_hash.go`, `tags.go` | Engine-side rule evaluation |
| `reader/` | Meta-log subscribe; one subscription per dailyrun.Run pass |
| `router/router.go` | Per-event rule evaluation |
| `bootstrap/walker.go` | Bucket walker with `RunForShard(view, shardID)` filter |
| `dispatcher/filer_persister.go` | Filer-backed cursor I/O |
| `dailyrun/run.go` | Main pass orchestrator: subscription, fan-out, per-shard `runShard` |
| `dailyrun/cursor.go` | Cursor type + filer JSON serializer |
| `dailyrun/walker_dispatcher.go` | Adapter from walker to `LifecycleDelete` RPC |

## Configuration

Admin config (`weed/worker/tasks/s3_lifecycle/`):

| Key | Type | Default | What |
|---|---|---|---|
| `cluster_deletes_per_second` | int64 | 0 (unlimited) | Cluster-wide ceiling on lifecycle delete RPCs/s. Allocated per worker. |
| `cluster_deletes_burst` | int64 | 0 (= 2× rate) | Token-bucket burst across cluster. |
| `meta_log_retention_days` | int64 | 0 (unbounded) | How far back the filer's meta-log can reach. Rules with TTL > retention promote to walker. |
| `walker_interval_minutes` | int64 | 0 (fire every pass) | Minimum time between steady-state walker fires per shard. Set positive when worker runs at tighter cadence than the desired walk frequency. |

Worker config:

| Key | Default | What |
|---|---|---|
| `max_runtime_minutes` | 60 | Wall-clock cap per `dailyrun.Run` call |

## Failure & recovery

- **Worker crashes mid-run.** Cursor advances only past successfully-deleted events. On restart, the next pass resumes at the same cursor and re-attempts. Identity-CAS makes redundant deletes no-ops.
- **Transient delete failure.** Pass halts at the failing event, cursor stays. Tomorrow's pass retries from the same point. Stuck cursor is visible in `s3_lifecycle_cursor_min_ts_ns`; operators see head-of-line blocking and address the root cause.
- **Identity drift** (object overwritten between event and delete). Handled by `LifecycleDelete` RPC's identity-CAS, which returns `NOOP_RESOLVED` for stale events. The algorithm dispatches optimistically and lets the server filter.
- **Cold start, rule edit, partition flip.** All route into the recovery branch. The walker over `engine.RecoveryView(snap)` catches already-due objects across the full rule set, then the cursor rewinds (rule edit) or stays at the cold-start floor.

## Implementation history

Phases were sequenced so each could ship independently. All five phases are merged; the codebase reflects the as-built model above.

| Phase | What | Tracking |
|---|---|---|
| 1 | `noncurrent_since` on version entries | `feat(s3): stamp noncurrent_since on versioned demotions` |
| 2 | Daily-replay worker (parallel to streaming, behind flag) | Initial dailyrun package |
| 3 | Cluster rate limit allocation | PR #9456 |
| 4 | Walker + recovery branch + engine surface | PR #9457, #9471 |
| 5 | Switch default, delete streaming | PR #9466 |
| 5a | Walker dispatch fix (ABORT_MPU + cursor saves + floor bug) | PR #9477 |
| 5b | Single meta-log subscription per pass | PR #9481 |
| 5c | Walker throttle via `WalkerInterval` | PR #9484, #9485 |
| 5d | Operator visibility gauges + heartbeat | PR #9486 |

## Future work

Tracked as optimizations rather than blockers:

1. **Long-lived subscription across passes.** Today the subscription is rebuilt per `Run()`. Keeping it alive across passes would eliminate the 7s ctx-timeout per pass and the start/teardown overhead. Requires per-shard pending heap (events whose `DueTime > runNow` would be parked in-memory instead of replayed) and a hot-swappable snapshot for mid-pass config changes. Multi-day refactor; current model works.
2. **Bucket-coordinated walker.** Phase 4 has each shard walk the full bucket and filter by `ShardID(bucket, key)` — simple but 16× the listing cost. A per-bucket coordinator (the worker owning shard 0 for that bucket lists once, routes matches to other shards) would cut listing cost. Worth doing if listing becomes the bottleneck for very large buckets.
3. **Per-bucket dispatch lag metric.** Currently only per-shard lag is exposed. Per-bucket would require a per-bucket cursor or a derived metric from `s3_lifecycle_dispatch_total{bucket,kind,outcome}`. Punted on cardinality concerns; revisit when an operator asks for it.
4. **Meta-log retention plumbing.** If the filer adds GC for `/topics/.system/log`, the `PromotedHash` partition flip needs to consume the filer's actual retention horizon (currently dormant because retention is effectively infinite).
