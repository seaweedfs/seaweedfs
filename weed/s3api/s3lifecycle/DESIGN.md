# S3 Lifecycle (Design)

Replaces the current streaming + heap design with a daily meta-log replay. The worker becomes "start, do today's work, stop" — matching the operator's mental model and removing the per-shard heap that grows O(events × TTL_days).

## Goal

For each bucket lifecycle rule with TTL `D` days, delete every object whose age exceeds `D`, exactly once per day, with the worker exiting when the pass completes. No future-buffered Matches in memory. No tick-driven dispatch loop. Cluster-wide delete rate cap allocated per worker.

## Algorithm

One pass per day, per shard:

```text
daily_run(shardID):
    persisted             = load_cursor(shardID)                // { TsNs, rule_set_hash, promoted_hash }
    earliest_available    = metalog.EarliestAvailableTsNs(shardID)
    retention_window      = duration(now - earliest_available)   // time.Duration; sole window input to engine
    snap                  = engine.CurrentSnapshot()             // base; per-action active/Mode reflect compile-time state, not partition
    replay, walk          = snap.RulesForShard(shardID, retention_window)
    if replay == nil and walk == nil: return                    // bucket has no rules
    rsh                   = engine.ReplayContentHash(snap)      // content over ExpirationDays/NoncurrentDays/AbortMPU rules; partition-independent
    promoted              = engine.PromotedHash(snap, retention_window)  // rules currently in walk due to scan_only

    needs_recovery_walk = (
        persisted is fresh                                      // cold start
        or persisted.rule_set_hash != rsh                       // replay-rule edit
        or persisted.promoted_hash != promoted                  // partition flip in either direction
        or (replay != nil and persisted.TsNs < earliest_available)  // retention loss; meaningless if pure walker bucket
    )

    if needs_recovery_walk:
        // Catch already-due objects across ALL rules — both walker-bound and
        // replay-bound. Replay-bound rules may have objects whose mtime is
        // older than (now - max_ttl) that the steady-state replay window cannot
        // reach. The recovery view force-activates every action so the walker
        // doesn't skip event-driven rules that the base snapshot has marked
        // inactive (e.g., pre-BootstrapComplete).
        walker.RunForShard(ctx, shardID, engine.RecoveryView(snap), limiter)
        if replay == nil:
            save_cursor(shardID, { TsNs: 0, rule_set_hash: rsh, promoted_hash: promoted })
        else:
            save_cursor(shardID, { TsNs: now - engine.MaxEffectiveTTL(replay), rule_set_hash: rsh, promoted_hash: promoted })
        return

    // --- Steady-state path. ----------------------------------------------
    if walk != nil:
        walker.RunForShard(ctx, shardID, walk, limiter)         // walker-bound actions only

    if replay == nil:
        save_cursor(shardID, { TsNs: 0, rule_set_hash: rsh, promoted_hash: promoted })
        return

    max_ttl  = engine.MaxEffectiveTTL(replay)
    cursor   = max(persisted.TsNs, now - max_ttl)
    done     = {r: false for r in replay.activeRules()}
    last_ok  = persisted.TsNs

    for ev in metalog.from(shardID, cursor):                    // ascending TsNs
        if all(done.values()): break
        halt    = false
        matches = router.Route(ctx, replay, ev, now, lister)    // existing signature; replay is *engine.Snapshot
        for m in matches:
            r = replay.lookup(m.RuleHash)
            if r == nil or done[r]: continue
            if m.DueTime > now:
                done[r] = true                                  // monotonic for replay kinds
                continue
            limiter.wait()
            outcome = dispatch_delete(m.Bucket, m.ObjectKey, m.VersionID,
                                      expected_mtime=m.Identity.mtime, ruleHash=m.RuleHash)
            if outcome is unresolved (RETRY_LATER, transport error after in-run retries, BLOCKED):
                halt = true; break
        if halt: break
        last_ok = ev.TsNs

    save_cursor(shardID, { TsNs: last_ok, rule_set_hash: rsh, promoted_hash: promoted })
```

Two distinct walker invocations, deliberately:

| Branch | Walker input | Why |
|---|---|---|
| Recovery (cold-start, replay-rule edit, retention loss, partition flip) | `engine.RecoveryView(snap)` — every action clone force-`markActive()`'d, `Mode` preserved | Catch already-due objects across all rules — including replay-bound rules whose subjects are older than `max_ttl` (replay scan window cannot reach), *and* event-driven rules the base snapshot may mark inactive (`compile.go:73-74` requires `BootstrapComplete`). |
| Steady-state | `walk` (walker-only actions) | Replay handles its own rules via the meta-log scan. Walker only runs for actions that have no replay path. |

`router.Route` and `walker.RunForShard` keep their existing `*engine.Snapshot` parameter; `replay`, `walk`, and `snap` are all `*Snapshot` values.

The single API surface is:

```go
// In engine:
func CurrentSnapshot() *Snapshot                                                  // base
func (s *Snapshot) RulesForShard(shardID int, retentionWindow time.Duration) (replay, walk *Snapshot)
func RecoveryView(s *Snapshot) *Snapshot                                          // every action force-activated; recovery walker only
func ReplayContentHash(s *Snapshot) [32]byte                                      // content over replay-eligible rules; partition-independent
func PromotedHash(s *Snapshot, retentionWindow time.Duration) [32]byte            // hash of replay-eligible rules currently in walk (scan_only)
func MaxEffectiveTTL(s *Snapshot) time.Duration                                   // max over active replay actions
```

`RulesForShard` and `RecoveryView` both return new `*Snapshot` instances with cloned `*CompiledAction` objects and shared (by pointer) rule definitions. The fields that differ from the base are:

- **`active`** (per clone) — set per view.
- **`Mode`** (per clone) — rewritten to `ModeEventDriven` on `replay` clones; preserved as-is on `walk` and `recovery` clones. The rewrite is required because `router.Route` gates on `Mode == ModeEventDriven` (`router.go:134, 253, 413, 479, 566, 623, 844`), and today's compile preserves a persistent `prior.Mode = ModeScanOnly` that would otherwise lock the action out of replay even after retention rehabilitates it.
- **Action-map membership** — `replay`'s map contains only replay-eligible clones; `walk`'s contains only walker-bound clones; `recovery`'s contains every action.

Per-view settings are summarized in the table under "`router.Route` integration."

`router.Route` is the existing entry point — it computes `ComputeDueAt`, applies predicate filters, expands one event into 0+ `Match`es (e.g., a demoting PUT produces one for the new version's lifecycle and another for the displaced version's `NoncurrentDays`). The replay path delegates due-time math to the engine so all replay-eligible action kinds (`ExpirationDays`, `NoncurrentDays`, `AbortMPU`) are handled uniformly.

### `router.Route` integration

Today's `router.Route(ctx, snap *engine.Snapshot, ev, now, lister)` iterates every action with `IsActive() == true` in the snapshot. `IsActive` is **instance-level state on `*CompiledAction`** (`engine/compile.go:73-85`, `ca.markActive()`), so two snapshots can't disagree on activation if they share the same `*CompiledAction` pointers.

**Approach: views are independent `*Snapshot` instances with cloned `CompiledAction` objects.**

Two gating fields are instance-level on `*CompiledAction` and must both match what the downstream call site expects:

- `active` (`engine/compile.go:73-85`) — gates routing and walker iteration.
- `Mode` — `router.Route` requires `Mode == engine.ModeEventDriven` at every emit site (`router.go:134, 253, 413, 479, 566, 623, 844`); the walker rejects only `Mode == engine.ModeDisabled` (`walker.go:147`). Today's compile preserves `prior.Mode`, so a rule previously promoted to `ModeScanOnly` will still be skipped by `router.Route` even if `active=true`.

`engine.Snapshot.RulesForShard(shardID, retentionWindow) (replay, walk *Snapshot)` returns two new `*Snapshot` instances. Each has its own `actions map[ActionKey]*CompiledAction` containing *clones* of the originals with **both `active` and `Mode` rewritten** per view. The partition predicate is "TTL ≤ retentionWindow" for replay membership; the engine does no clock arithmetic of its own — `retentionWindow` is the only window input it receives:

| View | Clone settings | Why |
|---|---|---|
| `replay` | `active = true`, `Mode = ModeEventDriven` | `router.Route` requires `ModeEventDriven`. Forced regardless of `prior.Mode`, so a rule previously stuck in `ModeScanOnly` is rehabilitated when retention now fits its TTL. |
| `walk` | `active = true`, `Mode` preserved from original (any non-`ModeDisabled` is accepted by the walker) | Walker doesn't gate on a specific Mode value, so no rewrite needed beyond ensuring `active`. |
| `recovery` (see below) | `active = true`, `Mode` preserved | Recovery walker iterates all action clones; preserving `Mode` lets each rule's evaluator do its normal predicate work. |

Shared-by-pointer with the base: `Rule` definitions, predicate maps, `RuleHash` table, and other immutable data — memory overhead is one `CompiledAction` struct per action per view (tens of bytes × dozens of actions, not megabytes).

Membership:

- `replay`: clones of `ExpirationDays`, `NoncurrentDays`, `AbortMPU` actions whose TTL ≤ `retentionWindow`. The Mode rewrite is what makes "rehabilitation" work — retention recovers, a previously `ModeScanOnly`-stuck rule clone now reads as `ModeEventDriven` in the replay view.
- `walk`: clones of `ExpirationDate`, `ExpiredDeleteMarker`, `NewerNoncurrent`, plus `scan_only`-promoted clones whose TTL exceeds `retentionWindow`.
- Either return value may be `nil` if its partition is empty.

The daily replay loop calls `router.Route(ctx, replay, ev, now, lister)`. Steady-state walker calls `walker.RunForShard(ctx, shardID, walk, limiter)`. Both call sites are drop-in — they see the correct, per-partition set with the correct `(active, Mode)` for their gating logic.

**Recovery view (separate).** The recovery branch needs the walker to see **all** replay-eligible + walker-eligible actions as active, including event-driven actions that the base snapshot may have marked inactive (`compile.go:73-74`: event-driven actions require `BootstrapComplete`). Add:

```go
func RecoveryView(s *Snapshot) *Snapshot
```

`RecoveryView` is a third `*Snapshot` instance whose action clones are all `markActive()`'d, with `Mode` preserved from the original. The walker accepts any non-`ModeDisabled` Mode, so no rewrite is needed. The recovery branch calls `walker.RunForShard(ctx, shardID, engine.RecoveryView(snap), limiter)`. Without this, recovery would silently skip any rule the base snapshot considers inactive — defeating the recovery branch's purpose of catching already-due objects across the full rule set.

`RecoveryView` does not participate in steady-state. It exists only for the recovery walker invocation.

**`scan_only` input path.** The retention boundary used for promotion is supplied explicitly via `retentionWindow time.Duration`, not derived inside the engine. `daily_run` computes it once at run start (`retention_window = duration(now - metalog.EarliestAvailableTsNs(shardID))`) and passes it to `RulesForShard` and `PromotedHash`. The promotion decision is therefore stable across one `daily_run` invocation — a rule that flips between `replay` and `walk` mid-run does not re-route; it gets its new bucket on the next day's `RulesForShard` call. `RecoveryView` takes only the base snapshot — it activates everything, partition-independent.

This approach keeps `router.Route` and `walker.Run` signatures unchanged. The trade-off is cloning `CompiledAction` per view (and rewriting `Mode` in the replay view); the alternative (adding a filter parameter to `Route` or restructuring `IsActive`/`Mode` to be view-scoped) would force every call site — including the streaming dispatcher during Phase 2/3 — to thread the filter, while clone-per-view localizes the partition decision to the engine. The Mode rewrite also bypasses today's persistent `prior.Mode = ModeScanOnly` lock-in, which is now obsoleted by the new partition logic.

Why correct:

- Meta-log is TsNs-ordered. For any rule `R`, `due_time` is also monotonic in TsNs (see "Due-time source"), so the first not-yet-due event for `R` proves every later event is also not-yet-due. Hence the `done` flag.
- Multiple rules fan out on the same scan: one meta-log read, N predicate checks per event.
- Identity drift (object overwritten between event and delete) is caught at the RPC by the existing `LifecycleDelete` identity-CAS, which returns `NOOP_RESOLVED` for stale events. The algorithm dispatches optimistically and lets the server filter.

### Action kinds and dispatch paths

The engine defines six action kinds (`weed/s3api/s3lifecycle/action_kind.go`). Each has a distinct trigger and due-time semantics, and they split into two dispatch paths. Replay path uses meta-log iteration with the `done` early-stop; walker path lists current bucket state.

| ActionKind | Trigger | Due time | Path | Early-stop? |
|---|---|---|---|---|
| `ExpirationDays` | Latest-version PUT | `ev.TsNs + r.ExpirationDays` | Replay | Yes — `due` strictly increasing in iteration order |
| `NoncurrentDays` | Demotion (next PUT for same key) | `entry.noncurrent_since + r.NoncurrentDays` | Replay | Yes — `noncurrent_since` is the demoting event's TsNs (see "`noncurrent_since`" below), monotonic |
| `AbortMPU` | MPU init | `mpu_init.TsNs + r.AbortMPUDaysAfterInitiation` | Replay | Yes — same monotonicity as `ExpirationDays` |
| `ExpirationDate` | Latest-version PUT, fired on `now >= r.ExpirationDate` | `r.ExpirationDate` (constant) | Walker | n/a — fixed wall-clock date, fires once per object |
| `ExpiredDeleteMarker` | Delete marker with `NumVersions == 1` (no remaining siblings) | "now if orphaned, else never" — depends on current bucket state, not event age | Walker | n/a — requires sibling lookup, not derivable from event TsNs |
| `NewerNoncurrent` | Version becomes noncurrent AND total noncurrents > `r.NewerNoncurrentVersions` | "now if over the count cap, else never" — count is current state | Walker | n/a — requires version-list lookup |

Why `ExpiredDeleteMarker` and `NewerNoncurrent` are walker-only:

Both rules' "due time" depends on the *current* state of sibling versions, not on any single event's timestamp. Modeling them as replay would force a sibling-list lookup on every meta-log event for the matching keys (today's `router.Route` does exactly this), and the `done` early-stop would never engage because the trigger condition isn't event-time-monotonic. They fit the walker far better: list each key's versions once per day, evaluate the rule against current state, emit deletes. Today's `bootstrap/walker.go` already handles them via `EvaluateAction`.

`expected_mtime` passed to `LifecycleDelete` is always the entry's original mtime (used for identity CAS), regardless of action kind. The TTL clock and the CAS identity are separate concerns.

### Rule changes

Rule changes invalidate the persisted cursor's assumption that "everything older has already been processed under the same rules." The algorithm tracks `rule_set_hash` alongside the cursor and forces a bootstrap walk on mismatch. Cases this covers:

- New rule added → walker discovers existing objects under the new rule.
- TTL increased (e.g. 30d → 60d) → walker discovers objects in `[now-60d, now-30d]` that were skipped while the old rule ran.
- Predicate widened (prefix/tag filter change) → walker re-evaluates everything.
- TTL decreased / predicate narrowed → walker still runs (harmless re-evaluation), but the next daily run picks up steady-state quickly.

Bootstrap walker on rule change is a one-shot full bucket list. Cost is proportional to bucket size, not history; acceptable as an infrequent event.

### Bootstrap retention window

The walker only deletes objects whose age already exceeds their TTL. Not-yet-due objects are intentionally left for the meta-log path. If the post-walker cursor were set to `now`, the next daily replay would scan from `now` forward and never see the PUT event for, say, a 20-day-old object under a 30-day rule — that event sits at `TsNs ≈ now - 20d`, before the cursor.

The cursor is therefore reset to `now - max_ttl` (excluding `ScanAtDate` rules from `max_ttl`, since those don't use the replay path). The clamp `max(persisted, now - max_ttl)` in the steady-state algorithm preserves the sliding window: each day the effective scan range is `[now - max_ttl, now]`, and not-yet-due events surface naturally as they age in.

Invariant this depends on: **meta-log retention ≥ max_ttl**. If retention is shorter, events for some still-pending objects age out before they become due. Detected at run start by comparing the meta-log's earliest available TsNs against `now - max_ttl`; if the gap is non-empty, the affected rules are flagged `scan_only` for that run and routed to the walker (see "Scan-at-date and scan-only paths").

### Replay vs walker: single source of truth

`Snapshot.RulesForShard(shardID, retentionWindow) (replay, walk *Snapshot)` is the **only** place that decides which path each compiled action takes during steady state. Both return values are new `*Snapshot` instances containing cloned `*CompiledAction` values with per-partition `active` flags (see "`router.Route` integration" for why cloning is required); either may be `nil` when its partition is empty. Every other component (`daily_run`, the walker invocation, the cursor's `RuleSetHash`, the `max_ttl` calculation) reads through these views. A given compiled action's clone has `active=true` in exactly one of `replay` or `walk`:

| Reason for walker | Engine signal |
|---|---|
| `ExpirationDate` (`ModeScanAtDate`) | `compile.go:73` sets `mode == ModeScanAtDate` |
| `ExpiredDeleteMarker` | Action kind requires sibling count |
| `NewerNoncurrent` | Action kind requires version count |
| `scan_only` (TTL > meta-log retention) | `event_log_horizon.go` check against earliest available TsNs at run start |

Everything else (`ExpirationDays`, `NoncurrentDays`, `AbortMPU` when TTL fits in retention) is on the replay list.

The `effective_ttl` per active replay action — used by `engine.MaxEffectiveTTL(replay)` — is:

- `ExpirationDays.ExpirationDays`
- `NoncurrentDays.NoncurrentDays`
- `AbortMPU.AbortMPUDaysAfterInitiation`

Walker actions don't appear as active in `replay`, so they naturally contribute nothing to `MaxEffectiveTTL` — the cursor sliding window only governs replay.

If `snap.replay` is empty (e.g., a bucket whose only rule is `ExpirationDate`), the cursor's `TsNs` is recorded as `0` (or any sentinel) and never read; only `RuleSetHash` matters, so a future addition of a replay rule is detected and triggers the rewind branch. The walker still runs every day for the walker rules.

### Cursor hashes: `RuleSetHash` and `PromotedHash`

The cursor stores two hashes. Together they detect every situation that invalidates the cursor's "everything before persisted.TsNs has been processed under the same rules" assumption.

**`RuleSetHash = engine.ReplayContentHash(snap)`** — content over the rule definitions (action kind, predicate, TTL value) of replay-eligible action kinds (`ExpirationDays`, `NoncurrentDays`, `AbortMPU`) in the base snapshot. Partition-independent.

**`PromotedHash = engine.PromotedHash(snap, retentionWindow)`** — hash of the set of replay-eligible rules that are currently classified as `walk` due to `scan_only` (their TTL exceeds `retentionWindow`). Changes when retention shifts move rules between partitions, even if XML rules and their content are unchanged.

Why two hashes: a replay-cursor rewind is needed when either the rule definitions change *or* when a rule transitions between partitions in either direction.

- Promotion (replay → walk, retention dropped): the promoted rule's meta-log events for in-flight objects are no longer reachable. Walker must take over for that rule. Cursor for other replay rules is fine, but the recovery branch's full-rule-set walk is still the safest catch-up — `PromotedHash` change triggers it.
- Demotion (walk → replay, retention recovered): the rule's events are now reachable again in meta-log. **But persisted.TsNs may be forward of where the rule's relevant events live**, because other replay rules kept the cursor advancing while this rule was in walk. The `persisted.TsNs < earliest_available` check does not catch this — that check fires only on retention regression. Demotion is detected by `PromotedHash` change.

Recovery triggers, complete list:

| Trigger | Detection | Why |
|---|---|---|
| Cold start | No persisted cursor | First run |
| Replay-rule edit | `RuleSetHash` mismatch | Content of replay-eligible rules changed |
| Retention loss | `replay != nil && persisted.TsNs < earliest_available` | Worker was down or retention shrank past cursor. Gated on `replay != nil` so the TsNs sentinel (`0`) used by pure walker buckets doesn't perpetually trigger recovery. |
| Partition flip | `PromotedHash` mismatch | A replay-eligible rule moved between `replay` and `walk` |

Walker-only rule edits (`ExpirationDate`, `ExpiredDeleteMarker`, `NewerNoncurrent`, or any rule with no replay-eligible action kind) do not change either hash. They are picked up by the steady-state `walk` invocation; replay cursor is untouched.

Practical consequences:

- Adding/removing/modifying a walker-only rule: both hashes unchanged, replay cursor untouched.
- Retention drops (replay rule promotes to walk): `RuleSetHash` unchanged, `PromotedHash` changes → recovery fires. Walker over base catches already-due objects; cursor rewinds for replay continuity.
- Retention recovers (rule demotes back to replay): `RuleSetHash` unchanged, `PromotedHash` changes → recovery fires. The demoted rule's events get a fresh scan window via the rewind.
- Editing a replay-eligible XML rule: `RuleSetHash` changes → recovery fires. Walker covers the full rule set; already-due objects under the new rule are caught.

### Shard-aware walker

Today's walker (`bootstrap/walker.go`) is bucket-wide: it lists every entry under a bucket and emits matches regardless of `ShardID`. Shards are derived as `sha256(bucket||"/"||key) >> 252` (`weed/s3api/s3lifecycle/shard.go:23`), so a single bucket's objects spread across all 16 shards.

Daily replay is per-shard, so a per-shard `daily_run(shardID)` invoking the bucket-wide walker would have all 16 shards re-walk every bucket — 16× the filer listing cost, 16× the duplicate delete RPCs (idempotent but wasteful and noisy in metrics).

Two paths are workable; the design picks the simpler one and notes the optimization:

1. **Shard-filtering walker (chosen).** Add `walker.RunForShard(shardID int, ...)`. The walker still does one full bucket listing, but for each entry it computes `ShardID(bucket, key)` and skips entries not in the target shard. Cost: still 1 listing per shard per bucket = 16× listing total. Simpler; no coordination across workers. Acceptable for buckets up to ~100M entries where listing is cheap relative to deletes.
2. **Bucket-coordinated walker (future).** A separate coordinator (e.g., the worker that owns shard 0 for that bucket) runs the walker once, routing emitted matches to each shard's local injection point. 1 listing per bucket total. Requires a coordination layer that today's plugin/worker model doesn't have. Tracked as an optimization, not blocking.

Path #1 is wire-compatible with today's walker — only the iteration filter changes. `RunForShard` is a thin wrapper that takes a shard predicate; the existing `Run(bucket)` becomes `RunForShard(shard, bucket)` with `shard == any`.

### Delete failure handling

Cursor advance is gated on success. The cursor only moves past events whose deletes returned `DONE`, `NOOP_RESOLVED`, or `SKIPPED_OBJECT_LOCK` (lock-protected, but rule semantics are satisfied). Any other outcome — `RETRY_LATER`, `BLOCKED`, or transport error after in-run retries — halts the run and persists the cursor at the last fully-processed event.

Semantics:

- **Head-of-line blocking is intentional.** A transient filer error stalls the rest of today's pass; tomorrow's run resumes at the same cursor and retries from the failing event. This is loud — operator sees a stuck cursor in metrics — and idempotent (identity-CAS makes any redundant delete a no-op).
- **In-run retry with backoff** for transport errors only (default 3 attempts, exponential backoff). Server-side outcomes (`RETRY_LATER`, `BLOCKED`) are not retried in-run; the server already classified them as "wait until next run."
- **No separate retry queue.** Removing the per-key freeze state was the whole point — adding it back would re-introduce the state machine this design replaces.

## Data model changes

### `noncurrent_since` on version entries

A non-current version's TTL clock starts when the next version was written, not at its own mtime. Today there's no field for that timestamp; the engine has to derive it by joining successive PUTs for the same key.

Change: at the S3 PUT handler that demotes a prior version (current → noncurrent), write `noncurrent_since` on the demoted entry **to the TsNs of the demoting event**, not local wall-clock. The demoting event is itself a meta-log PUT with a well-defined TsNs; using that value keeps `noncurrent_since` strictly monotonic in meta-log order across all replicas and immune to wall-clock skew between S3 handler nodes.

The lifecycle evaluator uses `ev.TsNs` for current-version rules and `entry.noncurrent_since` for noncurrent rules — both monotonic in the iteration order, so the algorithm's `done` flag still holds. `expected_mtime` (passed to `LifecycleDelete` for identity CAS) remains the entry's own mtime; CAS identity and TTL clock are kept separate.

### Cursor

Per-shard cursor is `{ TsNs int64, RuleSetHash [32]byte, PromotedHash [32]byte }`. No frozen-cursor set, no per-key freeze state — failed deletes halt the run and leave the cursor at the last successful event (see "Delete failure handling"). `RuleSetHash` and `PromotedHash` together gate recovery — mismatch on either triggers the recovery branch on the next run. See "Cursor hashes."

## Components

| Component | Status |
|---|---|
| `engine/` (rule compilation, snapshot) | Keep |
| `evaluate.go`, `due_at.go`, `rule_hash.go`, `tags.go` | Keep |
| `reader/` (meta-log subscribe) | Keep, used once per daily pass instead of continuously |
| `router/router.go` (rule evaluation per event) | Keep, callable in the per-event loop |
| `router/schedule.go` (heap) | **Remove** |
| `dispatcher/dispatcher.go` (tick, retries, freeze) | **Remove**; per-call `LifecycleDelete` moves into the daily loop |
| `dispatcher/pipeline.go` | **Replace** with `daily_run` per shard |
| `scheduler/scheduler.go` (long-running goroutines + refresh ticker) | **Replace** with one-shot per-shard fan-out |
| `bootstrap/walker.go` | Keep, with shard filter added (`RunForShard`). Two invocation modes: (a) **steady-state daily** against the `walk` view (`ExpirationDate`, `ExpiredDeleteMarker`, `NewerNoncurrent`, `scan_only` rules); (b) **recovery** against `engine.RecoveryView(snap)` (cold start, replay-rule edit, retention loss, partition flip). No longer feeds an in-memory schedule. |

## Rate limiting

Cluster-wide deletes-per-second cap, set as admin config. Admin computes per-worker share at job-dispatch time:

- Admin counts workers capable of `s3_lifecycle` from the registry, divides `cluster_deletes_per_second` by the count, writes the share into `ExecuteJobRequest.ClusterContext.Metadata["s3_lifecycle.deletes_per_second"]`.
- Worker reads the share and constructs one `golang.org/x/time/rate.Limiter` shared across all shard goroutines. `dispatch_delete` calls `limiter.Wait(ctx)` before each RPC.
- Brief over/undershoot during worker join/leave is acceptable for a bulk workload.

No proto changes: `ClusterContext.Metadata` already exists.

## Failure & recovery

- **Worker crashes mid-run.** Cursor advances only past successfully-deleted events (see "Delete failure handling"); on restart the next run resumes at the same cursor and re-attempts. Identity-CAS makes redundant deletes no-ops.
- **Transient delete failure.** Run halts at the failing event, cursor stays. Tomorrow's run retries from the same point. Stuck cursor is visible in metrics — operators see head-of-line blocking and address root cause.
Four triggers route into the same **recovery branch** (cold-start, replay-rule edit, retention loss, partition flip). All four run the walker against `engine.RecoveryView(snap)` (every action force-`markActive()`'d, regardless of compile-time `IsActive`/`BootstrapComplete`) to catch already-due objects across the full rule set, then rewind the cursor to `now - MaxEffectiveTTL(replay)` (or sentinel `0` if replay is empty). The merge is by design: distinguishing the four would require independent state that isn't worth the complexity, and the walker is idempotent.

- **Cold start (no persisted cursor).** Recovery branch.
- **Replay-rule edit** (new/removed/changed `ExpirationDays`, `NoncurrentDays`, or `AbortMPU` rule). Detected by `RuleSetHash` mismatch. Recovery branch.
- **Retention loss** (`replay != nil && persisted.TsNs < earliest_available`). Worker was down longer than meta-log retention, or retention itself shrunk. Gated on `replay != nil` so the sentinel `TsNs=0` used by pure walker buckets doesn't perpetually re-trigger recovery. Recovery branch.
- **Partition flip** (`PromotedHash` mismatch — a replay-eligible rule moved between `replay` and `walk` since last run). Covers both directions:
  - Promotion (replay → walk, retention dropped): walker takes over for that rule. Recovery fires to refresh state.
  - Demotion (walk → replay, retention recovered): persisted.TsNs is likely forward of the rule's relevant events (other replay rules kept advancing during the walk era). Recovery rewinds the cursor so the demoted rule's events get a fresh scan window.
- **Walker-only rule edit** (added/removed/modified `ExpirationDate`, `ExpiredDeleteMarker`, `NewerNoncurrent`, or any rule with no replay-eligible action kind). Not a recovery trigger — both hashes unchanged, cursor untouched. Walker re-reads `walk` on every steady-state run and applies the new rule automatically.
- **`ExpirationDate` rules.** Always routed to the walker — fixed wall-clock due time can't be derived from event TsNs, and objects created before the date may have aged out of meta-log. See "Replay vs walker."
- **Identity drift.** Handled by `LifecycleDelete` RPC as today. No worker-side state needed.

## Comparison

| | Streaming (today) | Daily replay (this design) |
|---|---|---|
| Worker lifetime | Long-running, capped by `MaxRuntime` | Starts, completes pass, exits |
| Memory | O(events with DueTime in `[now, now+TTL]`) | O(in-flight delete RPCs) |
| Disk reads | Continuous meta-log subscribe + periodic bootstrap | One meta-log scan per day, length ≤ max_ttl |
| Latency | DispatchTick (~1 min) | Up to 24h |
| Future-buffering | Yes, in heap | No |
| State persisted | Cursor + frozen set + heap (rebuilt from meta-log on restart) | Cursor only |
| Dispatch tick / checkpoint tick / refresh tick | Required | Removed |
| Bootstrap walker | Hot path (every run, feeds heap) | Daily for `walk` rules; recovery for full rule set |
| Failure recovery | Heap rebuild from meta-log replay | Cursor advance |

## Configuration changes

The admin and worker forms collapse significantly. New worker config:

```text
deletes_per_run_concurrency    // optional, in-pass parallelism (default 1)
max_runtime_minutes            // safety cap; default 1440 (24h)
```

Removed worker config (no longer meaningful):

```text
dispatch_tick_minutes
checkpoint_tick_seconds
refresh_interval_minutes
bootstrap_interval_minutes
```

New admin config (added to the existing form):

```text
cluster_deletes_per_second     // 0 = unlimited
cluster_deletes_burst          // 0 = 2× rps
```

Admin runtime stays at `DetectionIntervalMinutes=1440` (daily detection produces one execution per worker).

---

## Implementation phases

Sequenced so each phase compiles and ships independently. Phase 1 lands first because it's a prerequisite the algorithm relies on; phases 2–4 can be authored in parallel but should merge in order.

### Phase 1 — `noncurrent_since` on version entries

Smallest, isolated, prerequisite.

- Add `NoncurrentSinceNs int64` to the filer entry extended attributes (or as a top-level field if the proto allows non-breaking add).
- At the S3 PUT handler in `weed/s3api/s3api_object_handlers_put.go` (or whichever site demotes the prior current version), set `NoncurrentSinceNs` to the **TsNs of the demoting meta-log event**, not `time.Now().UnixNano()`. Read the value from the in-flight write's TsNs (the filer assigns this) — coordinate with the filer write API to make that value available to the demotion path. If the API can't surface it cleanly, fall back to a single `time.Now().UnixNano()` captured at the start of the demoting RPC, accepting that this introduces sub-millisecond skew relative to meta-log TsNs (still monotonic per node, weakly monotonic across nodes within clock-sync tolerance).
- Backfill: nil/zero on existing entries — the lifecycle evaluator treats `NoncurrentSinceNs == 0` as "fall back to entry mtime" so legacy data still expires, just with the old (slightly looser) semantics.
- Unit tests in the s3api package covering PUT-over-existing and PUT-with-versioning-suspended.
- Verify with a clock-skew test: two S3 nodes with ±5s clock drift produce a sequence of PUT/demotion events; assert `noncurrent_since` is non-decreasing in meta-log iteration order.

### Phase 2 — Daily-replay worker (parallel to today's, behind a flag)

Add the new code path without disturbing the existing one. Allows side-by-side smoke testing.

- New file `weed/s3api/s3lifecycle/dailyrun/dailyrun.go` with `func Run(ctx, shardID, ...) error` implementing the algorithm.
- Reuses `engine.Snapshot`, `evaluate.go`, `due_at.go`, the existing `reader.Reader`, and the existing `LifecycleDelete` RPC. No new RPCs.
- Per-shard cursor type `dailyrun.Cursor { TsNs int64; RuleSetHash [32]byte; PromotedHash [32]byte }`, JSON-persisted under the same filer path as today's cursor, keyed by `daily/` prefix to avoid collision. Cursor load returns `(cursor, isFresh bool)` so the algorithm can distinguish first-run from a rule-change rewrite. In Phase 2 (no walker plumbing), `PromotedHash` is always the empty hash; Phase 4 wires `engine.PromotedHash(snap, retentionWindow)` and the partition-flip recovery trigger.
- **Replay/walker partition**: full `RulesForShard` / `RecoveryView` plumbing lands in Phase 4. For Phase 2 alone, since walker rules are rejected (next bullet), Phase 2 is **replay-only**: pass the **base snapshot** directly to `router.Route` (no view, no clone, no Mode rewrite). This is acceptable for Phase 2 because the unsupported-rule check guarantees no walker-bound actions are present, and the absence of walker rules means there's no risk of router.Route emitting walker-bound matches. Mode rewrite is also unnecessary in Phase 2: any rule that compiled to `ModeScanOnly` (legacy persistent state) falls into the unsupported-rule path along with `scan_only`-promoted rules. Phase 4 then introduces clone-per-view (including Mode rewrite) and the recovery branch.
- **Refuse to run on unsupported rule sets.** Before invoking the replay loop, `dailyrun.Run` checks the compiled rules for any action of kind `ExpirationDate`, `ExpiredDeleteMarker`, `NewerNoncurrent`, or any rule promoted to `scan_only`. If any are present, the handler:
  - Logs at error level which rules are unsupported.
  - Returns a typed error from `Execute` so admin marks the run failed (visible in the activity log).
  - Does **not** advance the cursor.
  This makes flipping `algorithm: daily_replay` before Phase 4 a loud failure, not a silent dropped rule. Operators see the error and either revert to `streaming` or wait for Phase 4.
- **Rule-change handling**: compute `rsh = engine.ReplayContentHash(snap)` at run start (partition-independent — see "Cursor hashes"). If `rsh != cursor.RuleSetHash` and `replay` is non-`nil`, persist `{ TsNs: now - MaxEffectiveTTL(replay), RuleSetHash: rsh, PromotedHash: promoted }` and return. If `replay` is `nil`, persist `{ TsNs: 0, RuleSetHash: rsh, PromotedHash: promoted }` and return. In Phase 2, `promoted` is the empty hash (no walker plumbing); Phase 4 wires real values. All cursor writes carry all three fields so Phase 4 doesn't have to migrate existing Phase 2 cursors. Tested explicitly: add a replay-eligible rule mid-test, assert cursor TsNs rewinds to `now - max_ttl` and `RuleSetHash` updates.
- **Delete failure handling**: track `last_ok TsNs` as the algorithm progresses; only set `last_ok = ev.TsNs` after a successful or `NOOP_RESOLVED` outcome for *all* matches produced from that event. On any unresolved outcome (`RETRY_LATER`, `BLOCKED`, transport error after in-run retries), break the loop and persist `{ TsNs: last_ok, RuleSetHash: rsh, PromotedHash: promoted }`. In-run retry policy for transport errors: 3 attempts with exponential backoff capped at 5s.
- **Match expansion**: invoke `router.Route(ctx, snap, ev, now, lister)` per event, where `snap` is the **base snapshot** directly (Phase 2 is replay-only — see "Replay/walker partition" bullet above; the view-based call site `router.Route(ctx, replay, ev, ...)` lands in Phase 4 when the partition is wired). Handles `ExpirationDays`, `NoncurrentDays`, `AbortMPU` correctly out of the box (already in `due_at.go`).
- Rate limiter accepted as a parameter; constructed once per `Execute()` and shared across all shard goroutines.
- New admin config flag `algorithm: streaming | daily_replay` in `weed/worker/tasks/s3_lifecycle/handler.go`. When `daily_replay`, the handler invokes `dailyrun.Run` and exits. When `streaming` (default), today's code path runs unchanged.
- Unit tests:
  - Per-rule `done` flag correctness (single rule, multi-rule).
  - Multiple-match fan-out from one event (PUT-over-existing emits both `ExpirationDays` for the new version and `NoncurrentDays` for the displaced one).
  - Cursor advance halts at the first unresolved failure; subsequent run resumes there.
  - Identity-CAS skip path (event mtime stale → `NOOP_RESOLVED` still advances cursor).
  - Rule-set-hash mismatch rewinds cursor to `now - max_ttl` (Phase 4 adds the walker call on this path; Phase 2 just rewinds).
  - `replay` empty case: cursor persists `{ TsNs: 0, RuleSetHash: rsh, PromotedHash: <empty in Phase 2> }`, no meta-log read attempted.
  - `NoncurrentDays` rule uses `entry.noncurrent_since`, `ExpirationDays` rule uses `ev.TsNs`; both monotonic in iteration order.
  - `AbortMPU` test: MPU init event ages past `AbortMPUDaysAfterInitiation`, delete emitted.
  - **Unsupported-rule rejection**: bucket has an `ExpirationDate` (or `ExpiredDeleteMarker` or `NewerNoncurrent`) rule and the flag is set to `daily_replay`. Assert `Execute` returns the typed unsupported-rule error, the cursor is not advanced, and the streaming code path was not invoked. Repeat for a rule promoted to `scan_only`.

### Phase 3 — Cluster rate limit allocation

Admin-side share computation. Lands once Phase 2 has a working `Limiter` parameter to receive it.

- `weed/worker/tasks/s3_lifecycle/handler.go`: add `cluster_deletes_per_second`, `cluster_deletes_burst` to `AdminConfigForm.Sections["scope"]`.
- `weed/admin/plugin/plugin.go` near `RunDetectionRequest`/`ExecuteJobRequest` build sites: count capable workers, divide rps, write `s3_lifecycle.deletes_per_second` and `s3_lifecycle.deletes_burst` into `ClusterContext.Metadata`.
- Worker `Execute`: parse metadata keys, build `rate.Limiter`, pass to `dailyrun.Run`.
- New histogram `S3LifecycleDispatchLimiterWaitSeconds` for operator visibility.
- Tests: admin allocation math under join/leave, worker correctly slows under low share.

### Phase 4 — Shard-aware walker + scan-at-date / scan-only routing

Reposition the walker and add shard-filtering + per-rule routing.

- **Shard filter on the walker.** Add `walker.RunForShard(ctx context.Context, shardID int, view *engine.Snapshot, limiter *rate.Limiter) error`. `view` is one of `walk`/`replay`/`recovery` views — its action set already filters to the right partition (via per-clone `active` flags), so the walker iterates the bucket and evaluates each entry against `view` exactly as today's `Run` does, with the only added step being `if ShardID(bucket, entry.Name) != shardID { continue }`. The existing bucket-wide `Run` becomes `RunForShard` with `shardID = -1` (any) for backward compatibility, or is deleted outright in Phase 5.
- **Engine surface.** Four new exports on the engine package, all consumed by `daily_run`:
  - `func (s *Snapshot) RulesForShard(shardID int, retentionWindow time.Duration) (replay, walk *Snapshot)`. Returns two new `*Snapshot` instances with **cloned `*CompiledAction`** values; per-view `active` flags (and `Mode` rewrite on `replay`) determine partition membership. Shared-by-pointer with the base: `Rule` definitions, predicate maps, `RuleHash` table. Either return value may be `nil` when its partition is empty. The retention window is an explicit input — `daily_run` computes it once at run start as `duration(now - metalog.EarliestAvailableTsNs(shardID))` and passes it in. The engine does no clock arithmetic of its own. The partition is stable across one `daily_run` invocation. Membership:
    - **`replay`**: cloned `ExpirationDays`, `NoncurrentDays`, `AbortMPU` actions, `active=true`, where the action's TTL ≤ `retentionWindow`.
    - **`walk`**: cloned `ExpirationDate` (`ModeScanAtDate`), `ExpiredDeleteMarker`, `NewerNoncurrent` actions, plus any of the above promoted to `scan_only` because their TTL exceeds the retention boundary. All `active=true`.
    - A single XML rule that compiles to multiple action kinds (e.g., `ExpirationDays` + `NewerNoncurrent` on the same rule) is split: each compiled action lands in `replay` or `walk` independently. `ReplayContentHash` only hashes replay-eligible rule definitions from the base snapshot, so neither the `NewerNoncurrent` action nor any walker-only edit affects the hash.
  - `func RecoveryView(s *Snapshot) *Snapshot`. Returns a third `*Snapshot` instance with every action cloned and `markActive()`'d — regardless of `Mode` or prior `BootstrapComplete` state. The recovery walker uses this view to see all replay-eligible + walker-eligible rules. Without it, recovery would skip event-driven actions the base snapshot marks inactive (`compile.go:73-74`).
  - `func ReplayContentHash(s *Snapshot) [32]byte`. Stable across reorderings, partition-independent. Used as `RuleSetHash = ReplayContentHash(snap)`. Returns the empty hash when no replay-eligible rules exist. See "Cursor hashes."
  - `func PromotedHash(s *Snapshot, retentionWindow time.Duration) [32]byte`. Hash of the set of replay-eligible rules currently classified as `walk` due to `scan_only` (TTL > `retentionWindow`). Detects partition flips in either direction. Takes the same `retentionWindow` value as `RulesForShard` so the two cannot disagree about partition membership. See "Cursor hashes."
  - `func MaxEffectiveTTL(s *Snapshot) time.Duration`. Max over active replay actions; returns `0` for `nil` or empty replay (caller must already be in the empty-replay branch).
  - `func CurrentSnapshot() *Snapshot`. The base snapshot before partitioning. Recovery branch passes this to `RecoveryView`.
- **`dailyrun.Run` flow.** Cold-start, replay-rule edit (`RuleSetHash` mismatch), retention loss, and partition flip (`PromotedHash` mismatch) all route into the **recovery branch**: walker over `engine.RecoveryView(snap)` so already-due objects across all rules are caught (including event-driven actions the base snapshot may mark inactive), then cursor rewinds. Steady-state path runs walker over `walk` only, then replay over `replay`. Cursor stores `{ TsNs, RuleSetHash, PromotedHash }`; steady-state writes both hashes through unchanged.
- **Recovery branch.** Triggered by any of: no persisted cursor, `RuleSetHash` mismatch, `PromotedHash` mismatch, or `replay != nil && persisted.TsNs < earliest_available`. The retention-loss trigger uses the **absolute** `earliest_available` TsNs (the oldest available meta-log TsNs from `metalog.EarliestAvailableTsNs(shardID)`), not the `retentionWindow` duration that `RulesForShard`/`PromotedHash` consume — they're different shapes for different purposes: the retention check on the cursor compares two absolute timestamps, while the engine partition check compares a per-action TTL to a duration. Gated on `replay != nil` because pure walker buckets persist a sentinel `TsNs=0` that would otherwise re-fire recovery every run. Invokes `walker.RunForShard(ctx, shardID, engine.RecoveryView(snap), limiter)`. Persists cursor at:
  - `{ TsNs: now - engine.MaxEffectiveTTL(replay), RuleSetHash: rsh, PromotedHash: promoted }` if `replay` is non-`nil`.
  - `{ TsNs: 0, RuleSetHash: rsh, PromotedHash: promoted }` if `replay` is `nil` (pure walker bucket).
  `MaxEffectiveTTL` is undefined over an empty set, so the sentinel form is required when `replay` is `nil`. **Both hashes are written on every recovery persistence** — failing to update `PromotedHash` here would cause the partition-flip trigger to re-fire on every subsequent run. See "Bootstrap retention window."
- **Steady-state walker invocation.** Distinct from recovery: `walker.RunForShard(ctx, shardID, walk, limiter)`. `walk`'s cloned actions are masked to walker-bound kinds only, so the walker doesn't re-process replay rules that the meta-log scan already handles on the same run.
- Drop the bootstrap walker's call from `scheduler.go` (removed entirely in Phase 5).
- Tests:
  - Cold start with mixed walker-only + replay rules in same bucket: recovery branch fires once (walker over `engine.RecoveryView(snap)`, then cursor rewind). Next day's run is steady-state (walker over `walk` only, replay over `replay`).
  - `ExpirationDate` rule fires only on/after the date (walker path).
  - `ExpiredDeleteMarker` rule deletes orphaned markers (walker path, sibling-aware).
  - `NewerNoncurrent` count rule deletes oldest noncurrent versions beyond the cap (walker path, version-list-aware).
  - Bucket with only walker rules (e.g., pure `ExpirationDate`): `replay` is `nil`, cursor persisted with sentinel `TsNs=0`, no meta-log subscribe attempted. Critically — assert that across 5 consecutive daily runs, recovery fires exactly once (on the first run); subsequent runs see no recovery despite `persisted.TsNs (0) < earliest_available`, because the retention-loss check is gated on `replay != nil`.
  - Replay-rule edit triggers recovery: walker runs over `RecoveryView(snap)` (catches an object already 60d old under a brand-new 30d rule), then cursor rewinds. Object aged 20d at edit time is later deleted at age 30 via the replay path.
  - `RecoveryView` activates event-driven actions: simulate a base snapshot where an `ExpirationDays` action has `IsActive=false` (e.g., `BootstrapComplete=false`); assert the recovery walker still sees and evaluates the rule via the recovery view, while the steady-state walker over `walk` does not.
  - **Replay view rewrites Mode**: simulate a base snapshot with a `ModeScanOnly`-locked `ExpirationDays` action (carried over from `prior.Mode`); assert that `replay`'s clone of the action has `Mode == ModeEventDriven`, and that `router.Route(ctx, replay, ev, ...)` emits a Match for it. The test should fail if Mode rewrite is skipped — confirming the rehabilitation path is wired.
  - Cursor persistence after recovery: assert `PromotedHash` is written through on every recovery branch persistence; simulate a partition flip across two runs and verify the second run does not re-fire recovery because the prior `promoted_hash` was correctly stored.
  - Adding a walker-only rule to an existing replay-only bucket: `ReplayContentHash(snap)` is unchanged, replay cursor TsNs unchanged, walker picks up the new rule on its next steady-state pass.
  - `scan_only` promotion (retention drops, replay-eligible rule moves to `walk`): `ReplayContentHash` unchanged, **`PromotedHash` changes** → recovery branch fires. Walker over base catches in-flight objects under the promoted rule; cursor rewinds; `PromotedHash` is persisted.
  - Retention recovery (`scan_only` rule demotes back to `replay` while other replay rules' cursor has advanced past the rule's relevant events): `ReplayContentHash` unchanged, `persisted.TsNs > earliest_available` so retention-loss check does NOT fire, **`PromotedHash` changes** → recovery branch fires. Asserts the demoted rule's older events get processed (this is the test that would have failed before adding `PromotedHash`).
  - `PromotedHash` persistence: simulate a partition flip in either direction across two runs, assert cursor's `promoted_hash` field carries through and gates the next run's recovery decision correctly.
  - Shard filter: 2 shards × 1 bucket walker run leaves no double-deletes (assert via RPC counter).
  - `scan_only` promotion: rule with TTL > meta-log retention moves from `replay` to `walk` at run start.

### Phase 5 — Switch default and delete streaming path

Cut over and clean up. Should be a single PR that's mostly deletions.

- Default `algorithm: daily_replay` and remove the streaming alternative from the descriptor.
- Delete:
  - `weed/s3api/s3lifecycle/router/schedule.go` (+ tests)
  - `weed/s3api/s3lifecycle/dispatcher/dispatcher.go` (+ tests), `freeze`-related code, retry state
  - `weed/s3api/s3lifecycle/dispatcher/pipeline.go`
  - `weed/s3api/s3lifecycle/scheduler/scheduler.go` (+ tests), refresh ticker
  - Removed worker config fields and their `ParseConfig` handling
- Rewrite `weed/s3api/s3lifecycle/DESIGN.md` to remove "streaming" sections.
- One full end-to-end test on a bucket with 1M objects, mixed-TTL rules, with cluster rate cap engaged.

### Phase 6 — Observability polish

After cutover, before announcing.

- Daily-run summary log line: shard, rules-touched, events-scanned, deletes-issued, walker-fallback (y/n), duration.
- Prometheus: `S3LifecycleDailyRunDurationSeconds`, `S3LifecycleDailyRunEventsScanned`, `S3LifecycleDailyRunDeletesIssued`, all labelled by `shard_id` and `outcome`.
- Admin UI: a single "Last Run" card per bucket showing the above summary line. Removes the now-meaningless "Next Run / Detection Results" widgets we already hid for `s3_lifecycle`.

## Open questions

1. **In-pass concurrency.** Today's worker has 1–16 shard goroutines via `workers` config. Daily replay can either (a) keep that knob, fanning out shard scans across goroutines that share the limiter, or (b) drop it since the limiter is now the throughput governor. Lean toward (a) — shards are independent meta-log subscriptions and parallel reads cut wall-clock significantly.
2. **Cursor format compatibility.** Worth deciding whether the new cursor coexists at a separate filer path (clean cutover, no migration) or overwrites the old one (single source of truth, requires a one-time read-then-rewrite during cutover). Lean toward separate paths during phases 2–4, unified at Phase 5.
3. **Per-shard rule snapshots vs. global.** Engine snapshots are bucket-scoped today; Phase 4 adds `Snapshot.RulesForShard(shardID, retentionWindow) (replay, walk *Snapshot)`. Open: should `RulesForShard` also gate by which buckets a shard "owns" (every bucket is on every shard via `ShardID(bucket, key)`), or just return the global rule set partitioned by mode? Current lean: return global partition, since every shard worker runs against every bucket and the actual shard filter happens at the entry-iteration site (meta-log subscription and walker entry loop).
6. **Bucket-coordinated walker.** Phase 4 has each shard walk the full bucket and filter by `ShardID` — simple, but 16× the listing cost. A future optimization is a per-bucket coordinator (the worker owning shard 0 for that bucket lists once, routes matches to other shards via the existing injection point). Worth doing if listing cost becomes the bottleneck for very large buckets. Not blocking initial cutover.
4. **`noncurrent_since` source from filer write API.** Phase 1 prefers the demoting event's TsNs over wall-clock. Whether the existing filer write path surfaces that TsNs to the S3 handler without a proto change is a Phase 1 design decision; the fallback (handler-local `time.Now()`) is documented but loses cross-node monotonicity guarantees and should be a temporary measure.
5. **Retry budget for `RETRY_LATER`.** The current dispatcher tracks per-key retry counts; this design removes that state and lets head-of-line blocking surface the issue. Worth confirming that operators are OK with a stuck cursor as the failure signal vs. silent retry. If not, add a bounded retry counter on the cursor (one per stuck event) — small state, but inches back toward the model we're replacing.
