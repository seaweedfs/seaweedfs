# V3 Phase 15 G9C — ReplicaReady Feed Continuity / Post-Close ACK Mini-Plan

**Date**: 2026-05-02
**Status**: A-slice landed (`1e36a50`)
**Code branch**: `p15-g9c/replica-ready-feed-continuity`
**Predecessors**: G9B replica join lifecycle close

---

## 1. Goal

Strengthen `replica_ready` for replicas that pass through recovery.

G9B made `replica_ready` a status vocabulary fact. G9C tightens the internal predicate so recovered replicas do not become ready just because a recovery session closed at a frontier.

For a recovered replica:

```text
replica_ready =
  recovery_window_closed
  AND live_feed_continuity_established
  AND durable_ack_observed_after_close
```

`recovery_window_closed` includes base completion for rebuild sessions. The receiver only emits `SessionClosedCompleted` after its base/WAL/barrier contract closes, so engine can treat successful rebuild close as `base_complete` without adding a second engine event in this slice.

---

## 2. Scope

### In scope

1. Engine predicate for recovered-replica readiness.
2. Component tests proving session close alone is not enough.
3. Durable ACK after close becomes the ready evidence for recovered replicas.
4. Keep no-session caught-up primary/fence behavior unchanged.

### Out of scope

1. Placement consuming `replica_ready`.
2. Sync ACK voter eligibility consuming `replica_ready`.
3. External diagnostics endpoint.
4. New base-complete wire event; receiver session close remains the base/WAL/barrier aggregate for this slice.

---

## 3. TDD

1. `TestG9C_RecoveredReplica_NotHealthyUntilPostCloseDurableAck`
   - catch-up/rebuild session closes successfully;
   - engine does not emit `PublishHealthy` yet;
   - projection remains transitional.
2. `TestG9C_RecoveredReplica_PostCloseDurableAckPublishesHealthy`
   - after the same close, a durable ack with `DurableLSN >= Recovery.R` arrives;
   - engine emits `PublishHealthy` and projection becomes healthy.
3. Regression: caught-up no-session fence path remains healthy after fence completion.

---

## 4. Non-Claims

1. `replica_ready` still does not drive placement membership.
2. `replica_ready` still does not drive sync ACK voter eligibility.
3. Full base-complete event decomposition is deferred; successful rebuild close is the aggregate proof for this slice.

---

## 5. A-Slice Evidence

Code: `seaweed_block@1e36a50` — `G9C: require post-close durable ack for recovered replica ready`

What landed:

1. `RecoveryTruth` now records `RecoveryWindowClosed`, `PostCloseDurableAckKnown`, and `PostCloseDurableAckR`.
2. `SessionClosedCompleted` for catch-up/rebuild closes the recovery window but holds `PublishHealthy`.
3. `DurableAckObserved` after the close, with `DurableLSN >= Recovery.R`, releases `PublishHealthy`.
4. `DeriveProjection` shows the held state as `ModeRecovering`, not degraded.
5. Adapter and host component tests were updated so recovery close and ready are separate facts.

Verification:

```powershell
go test ./core/engine ./core/adapter ./core/host/volume -count=1
go test ./core/authority ./core/engine ./core/adapter ./core/host/volume -count=1
```

Both passed on `p15-g9c/replica-ready-feed-continuity`.

---

## 6. Next Slice

G9C-B should move from component predicate to integration signal shape:

1. Ensure the real recovery feeder path emits a post-close durable/progress fact after the recovery window closes.
2. Add one L2 or subprocess test where a recovered replica remains `recovering` immediately after close, then becomes `ready` only after the real progress callback.
3. Keep placement and ACK voter eligibility out of this slice unless explicitly ratified.
