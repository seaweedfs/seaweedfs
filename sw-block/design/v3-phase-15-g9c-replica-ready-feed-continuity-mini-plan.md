# V3 Phase 15 G9C — ReplicaReady Feed Continuity / Post-Close ACK Mini-Plan

**Date**: 2026-05-02
**Status**: C-slice landed (`b90798d`)
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

## 6. B-Slice Evidence

Code: `seaweed_block@ca5d4f6` — `G9C: emit post-close durable ack from dual-lane recovery`

What landed:

1. Receiver `BarrierResp` now carries durable achieved LSN: `min(TryComplete achieved, Sync frontier)`.
2. Dual-lane `BlockExecutor` emits `OnDurableAck` after successful `OnSessionClose`.
3. The post-close ACK uses the receiver's durable BarrierResp witness, not the primary frontier intent.
4. `TestDualLane_BlockExecutor_StartRebuild` now pins event order: close first, post-close durable ACK second.

Verification:

```powershell
go test ./core/recovery/... ./core/transport/... ./core/engine ./core/adapter ./core/host/volume -count=1
go test ./core/authority ./core/recovery/... ./core/transport/... ./core/engine ./core/adapter ./core/host/volume -count=1
```

Both passed on `p15-g9c/replica-ready-feed-continuity`.

---

## 7. C-Slice Evidence

Code: `seaweed_block@b90798d` — `G9C: pin replica ready after post-close ack in dual-lane component path`

What landed:

1. Added `TestG9C_DualLaneRecoveredReplica_PublishesHealthyOnlyAfterPostCloseDurableAck`.
2. The test runs the real component path: engine -> adapter -> dual-lane `BlockExecutor` -> receiver -> close callback -> durable ack callback -> engine.
3. It pins the order: `SessionClosedCompleted` -> `publish_healthy_held` -> `DurableAckObserved` -> `post_close_durable_ack` -> `PublishHealthy`.
4. Final projection must be `ModeHealthy`, proving `replica_ready` comes only after the post-close durable ack.

Verification:

```powershell
go test ./core/replication/component -count=1
go test ./core/authority ./core/recovery/... ./core/transport/... ./core/engine ./core/adapter ./core/host/volume ./core/replication/component -count=1
```

Both passed on `p15-g9c/replica-ready-feed-continuity`.

---

## 8. Next Slice

G9C-D should move from component lifecycle evidence to host/daemon status evidence:

1. Prove the status endpoint reflects the same recovered-replica transition.
2. Prefer a narrow subprocess/L2 test if the harness can observe the intermediate `recovering` state without adding test-only production hooks.
3. Keep placement and ACK voter eligibility out of this slice unless explicitly ratified.
