# S3 Lifecycle — Architecture

High-level overview of the lifecycle worker. For implementation detail, see [`weed/s3api/s3lifecycle/DESIGN.md`](https://github.com/seaweedfs/seaweedfs/blob/master/weed/s3api/s3lifecycle/DESIGN.md).

## At a glance

The lifecycle worker runs as a scheduled job. Each invocation:

```text
                ┌──────────────────────────────────────────┐
                │   dailyrun.Run (one filer subscription)  │
                │                                          │
   meta-log ──→ │   reader  ──→  fan-out  ──→  per-shard   │
                │                              channels    │
                │                                          │
                │   ┌──────────────────────────────────┐   │
                │   │   16 shard goroutines            │   │
                │   │   ┌──────────────────────────┐   │   │
                │   │   │  walker(view, shardID)?  │   │   │
                │   │   │  drainShardEvents        │   │   │
                │   │   │  saveCursorAndPublish    │   │   │
                │   │   └──────────────────────────┘   │   │
                │   └──────────────────────────────────┘   │
                │                                          │
                │   summary heartbeat + exit               │
                └──────────────────────────────────────────┘
```

One filer `SubscribeMetadata` stream covers every shard in this worker's set. A fan-out goroutine routes events to per-shard channels by `ev.ShardID = sha256(bucket || "/" || key) >> 252`. Each shard's goroutine independently runs the walker (when due), drains events, and persists its cursor.

Once every shard's goroutine returns, the worker tears down the subscription, emits a summary heartbeat, and exits.

## Per-shard state

Each shard owns a cursor file on the filer at `/etc/s3/lifecycle/daily-cursors/shard-NN.json`:

```text
TsNs          — last meta-log event for which all matches dispatched successfully
RuleSetHash   — ReplayContentHash of the rule set when this cursor was written
PromotedHash  — PromotedHash(retentionWindow) at write time
LastWalkedNs  — wall-clock of the last successful walker fire
```

The two hashes together detect every situation that invalidates the cursor: a replay-rule edit (`RuleSetHash` changes) or a partition flip (`PromotedHash` changes). On mismatch, the next pass triggers a recovery walk over `RecoveryView(snap)` to catch already-due objects across the full rule set, then rewinds the cursor.

## Replay vs walker

The lifecycle rule space splits two ways:

| Path | Action kinds | Why this path |
|---|---|---|
| Replay (meta-log) | `ExpirationDays`, `NoncurrentDays`, `AbortMPU` | DueTime is monotonic in event TsNs. The `done` early-stop works. |
| Walker (bucket list) | `ExpirationDate`, `ExpiredObjectDeleteMarker`, `NewerNoncurrent` | DueTime depends on current sibling/version state, not event age. |

The engine's `RulesForShard(shardID, retentionWindow)` returns two snapshot views (`replay`, `walk`); each is a clone of the base snapshot with the action map masked to the right partition. `router.Route` consumes the `replay` view per event; the walker consumes the `walk` view per bucket.

A rule promoted to scan-only because its TTL exceeds meta-log retention moves from `replay` to `walk` — visible via `PromotedHash`.

## Cadence layers

Three independent cadences shape worker behavior:

| Cadence | Set by | Default |
|---|---|---|
| Worker invocation | Admin scheduler `DetectionIntervalMinutes` | 1440 (daily) |
| Walker fire | `walker_interval_minutes` admin config | 0 (every invocation) |
| Cursor save | After each `runShard` | n/a |

The walker throttle decouples walker firing from invocation rate. CI invokes the worker every 2s; production invokes once per day. Both can use the same code with appropriate `walker_interval_minutes`.

## Failure model

- **Worker crash mid-run.** Cursor only advances past events whose matches all succeeded. On restart, the next pass resumes at the same cursor. Identity-CAS makes redundant deletes no-ops.
- **Transient delete failure.** Pass halts at the failing event, cursor persists. Next pass retries from there. Head-of-line blocking is intentional — surfaces real problems instead of silently retrying forever.
- **Rule edits.** Replay-rule edits trigger one-time recovery walk over `RecoveryView`. Walker-only rule edits don't change either hash; walker reads the new rules on its next steady-state fire.
- **Object overwritten between event and delete.** `LifecycleDelete` RPC's identity-CAS returns `NOOP_RESOLVED`; cursor advances normally.

## Rate limiting

Cluster-wide cap allocated per worker at job dispatch:

```text
per_worker_rate = cluster_deletes_per_second / count(active_s3_lifecycle_workers)
```

Each worker shares one `rate.Limiter` across all shard goroutines. `dispatchWithRetry` calls `limiter.Wait(ctx)` before each `LifecycleDelete` RPC.

## Components

| Path | Role |
|---|---|
| `engine/` | Rule compilation, partition views (`RulesForShard`, `RecoveryView`) |
| `evaluate.go` | Per-event rule evaluation (`EvaluateAction`) |
| `due_at.go` | Per-(rule, kind, info) due-time computation |
| `router/router.go` | Per-event match emission (calls engine.Action and EvaluateAction) |
| `reader/reader.go` | Meta-log subscribe with `ShardPredicate` |
| `bootstrap/walker.go` | Bucket-walker with `RunForShard` filter |
| `dailyrun/run.go` | Main orchestrator: subscription, fan-out, per-shard runShard |
| `dailyrun/cursor.go` | Cursor type + filer JSON serializer |
| `dailyrun/walker_dispatcher.go` | Walker-to-`LifecycleDelete` adapter |

## What it's not

- **Not a streaming dispatcher.** The earlier model kept a long-running goroutine per shard with an in-memory match heap. That code is gone. Worker is now "start, do today's work, stop."
- **Not event-time accurate.** Latency from PUT to delete is bounded by the worker invocation cadence plus the walker interval — typically up to 24h, not seconds.
- **Not a general-purpose scheduler.** The two action paths (replay, walker) are specific to lifecycle semantics. Don't add new event sources or actions without thinking through which path they belong on.

## Why this shape

Each design choice points back to a specific failure mode of the prior streaming worker:

| Choice | Replaces |
|---|---|
| Per-pass run + exit | Long-running goroutines with ticker drift, leak risk, restart pain |
| Cursor file per shard | Per-key freeze state, retry counters, in-memory heap on every restart |
| Identity-CAS at dispatch time | Pre-dispatch consistency checks at schedule time, racing object updates |
| Recovery branch over `RecoveryView` | Implicit "is this rule new" tracking with bookkeeping flags |
| Walker throttle independent of invocation | Walker hammering filer when test driver invokes every 2s |
| Single subscription per pass | 16x filer load with 16 per-shard subscriptions |

The result is a worker the operator can reason about by reading 2 metrics and a heartbeat line, with a state machine small enough to fit in one design doc.
