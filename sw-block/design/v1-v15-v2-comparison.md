# V1, V1.5, and V2 Comparison

Date: 2026-03-27

## Purpose

This document compares:

- `V1`: original replicated WAL shipping model
- `V1.5`: Phase 13 catch-up-first improvements on top of V1
- `V2`: explicit FSM / orchestrator / recoverability-driven design under `sw-block/`

It is a design comparison, not a marketing document.

## 1. One-line summary

- `V1` is simple but weak on short-gap recovery.
- `V1.5` materially improves recovery, but still relies on assumptions and incremental control-plane fixes.
- `V2` is structurally cleaner, more explicit, and easier to validate, but is not yet a production engine.

## 2. Steady-State Hot Path

In the healthy case, all three versions can look similar:

1. primary appends ordered WAL
2. primary ships entries to replicas
3. replicas apply in order
4. durability barrier determines when client-visible commit completes

### V1

- simplest replication path
- lagging replica typically degrades quickly
- little explicit recovery structure

### V1.5

- same basic hot path as V1
- WAL retention and reconnect/catch-up improve short outage handling
- extra logic exists, but much of it is off the hot path

### V2

- can keep a similar hot path if implemented carefully
- extra complexity is mainly in:
  - recovery planner
  - replica state machine
  - coordinator/orchestrator
  - recoverability checks

### Performance expectation

In a normal healthy cluster:

- `V2` should not be much heavier than `V1.5`
- most V2 complexity sits in failure/recovery/control paths
- there is no proof yet that V2 has better steady-state throughput or latency

## 3. Recovery Behavior

### V1

Recovery is weakly structured:

- lagging replica tends to degrade
- short outage often becomes rebuild or long degraded state
- little explicit catch-up boundary

### V1.5

Recovery is improved:

- short outage can recover by retained-WAL catch-up
- background reconnect closes the `sync_all` dead-loop
- catch-up-first is preferred before rebuild

But the model is still partly implicit:

- reconnect depends on endpoint stability unless control plane refreshes assignment
- recoverability boundary is not as explicit as V2
- tail-chasing and retention pressure still need policy care

### V2

Recovery is explicit by design:

- `InSync`
- `Lagging`
- `CatchingUp`
- `NeedsRebuild`
- `Rebuilding`

And explicit decisions exist for:

- catch-up vs rebuild
- stale-epoch rejection
- promotion candidate choice
- recoverable vs unrecoverable gap

## 4. Real V1.5 Lessons

The main V2 requirements come from real V1.5 behavior.

### 4.1 Changed-address restart

Observed in `CP13-8 T4b`:

- replica restarted
- endpoint changed
- primary shipper held stale address
- direct reconnect could not succeed until control plane refreshed assignment

V1.5 fix:

- saved address used only as hint
- heartbeat-reported address becomes source of truth
- master refreshes primary assignment

Lesson for V2:

- endpoint is not identity
- reassignment must be explicit

### 4.2 Reconnect race

Observed in Phase 13 review:

- barrier path and background reconnect path could both trigger reconnect

V1.5 fix:

- `reconnectMu` serializes reconnect / catch-up

Lesson for V2:

- one active recovery session per replica should be a protocol rule, not just a local mutex trick

### 4.3 Tail-chasing

Even with retained WAL:

- primary may write faster than a lagging replica can recover
- catch-up may not converge

Lesson for V2:

- explicit abort / `NeedsRebuild`
- do not pretend catch-up will always work

### 4.4 Control-plane recovery latency

V1.5 can be correct but still operationally slow if recovery waits on slower management cycles.

Lesson for V2:

- keep authority in coordinator
- but make recovery decisions explicit and fast when possible

## 5. V2 Structural Improvements

V2 is better primarily because it is easier to reason about and validate.

### 5.1 Better state model

Instead of implicit recovery behavior, V2 has:

- per-replica FSM
- volume/orchestrator model
- distributed simulator with scenario coverage

### 5.2 Better validation

V2 has:

- named scenario backlog
- protocol-state assertions
- randomized simulation
- V1/V1.5/V2 comparison tests

This is a major difference from V1/V1.5, where many fixes were discovered through implementation and hardware testing first.

### 5.3 Better correctness boundaries

V2 makes these explicit:

- recoverable gap vs rebuild
- stale traffic rejection
- promotion lineage safety
- reservation or payload availability transitions

## 6. Stability Comparison

### Current judgment

- `V1`: least stable under failure/recovery stress
- `V1.5`: meaningfully better and now functionally validated on real tests
- `V2`: best protocol structure and best simulator confidence

### Important limit

`V2` is not yet proven more stable in production because:

- it is not a production engine yet
- confidence comes from simulator/design work, not real block workload deployment

So the accurate statement is:

- `V2` is more stable **architecturally**
- `V1.5` is more stable **operationally today** because it is implemented and tested on real hardware

## 7. Performance Comparison

### What is likely true

`V2` should perform better than rebuild-heavy recovery approaches when:

- outage is short
- gap is recoverable
- catch-up avoids full rebuild

It should also behave better under:

- flapping replicas
- stale delayed messages
- mixed-state replica sets

### What is not yet proven

We do not yet know whether `V2` has:

- better steady-state throughput
- lower p99 latency
- lower CPU overhead
- lower memory overhead

than `V1.5`

That requires real implementation and benchmarking.

## 8. Smart WAL Fit

### Why Smart WAL is awkward in V1/V1.5

V1/V1.5 do not naturally model:

- payload classes
- recoverability reservations
- historical payload resolution
- explicit recoverable/unrecoverable transition

So Smart WAL would be harder to add cleanly there.

### Why Smart WAL fits V2 better

V2 already has the right conceptual slots:

- `RecoveryClass`
  - `WALInline`
  - `ExtentReferenced`
- recoverability planner
- catch-up vs rebuild decision point
- simulator for payload-availability transitions

### Important rule

Smart WAL must not mean:

- “read current extent for old LSN”

That is incorrect.

Historical correctness requires:

- WAL inline payload
- or pinned snapshot/versioned extent state
- not current live extent contents

## 9. What Is Proven Today

### Proven

- `V1.5` significantly improves V1 recovery behavior
- real `CP13-8` testing validated the V1.5 data path and `sync_all` behavior
- the V2 simulator covers:
  - stale traffic rejection
  - tail-chasing
  - flapping replicas
  - multi-promotion lineage
  - changed-address restart comparison
  - same-address transient outage comparison
  - Smart WAL availability transitions

### Not yet proven

- V2 production implementation quality
- V2 steady-state performance advantage
- V2 real hardware recovery performance

## 10. Bottom Line

If choosing based on current evidence:

- use `V1.5` as the production line today
- use `V2` as the better long-term architecture

If choosing based on protocol quality:

- `V2` is clearly better structured
- `V1.5` is still more ad hoc, even after successful fixes

If choosing based on current real-world proof:

- `V1.5` has the stronger operational evidence today
- `V2` has the stronger design and simulation evidence today
