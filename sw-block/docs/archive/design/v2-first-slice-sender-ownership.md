# V2 First Slice: Per-Replica Sender/Session Ownership

Date: 2026-03-27
Status: historical first-slice note
Depends-on: Q1 (recovery session), Q6 (orchestrator scope), Q7 (first slice)

## Problem

`SetReplicaAddrs()` replaces the entire `ShipperGroup` atomically. This causes:

1. **State loss on topology change.** All shippers are destroyed and recreated.
   Recovery state (`replicaFlushedLSN`, `lastContactTime`, catch-up progress) is lost.
   After a changed-address restart, the new shipper starts from scratch.

2. **No per-replica identity.** Shippers are identified by array index. The master
   cannot target a specific replica for rebuild/catch-up — it must re-issue the
   entire address set.

3. **Background reconnect races.** A reconnect cycle may be in progress when
   `SetReplicaAddrs` replaces the group. The in-progress reconnect's connection
   objects become orphaned.

## Design

### Per-replica sender identity

`ShipperGroup` changes from `[]*WALShipper` to `map[string]*WALShipper`, keyed by
the replica's canonical data address. Each shipper stores its own `ReplicaID`.

```go
type WALShipper struct {
    ReplicaID string // canonical data address — identity across reconnects
    // ... existing fields
}

type ShipperGroup struct {
    mu       sync.RWMutex
    shippers map[string]*WALShipper // keyed by ReplicaID
}
```

### ReconcileReplicas replaces SetReplicaAddrs

Instead of replacing the entire group, `ReconcileReplicas` diffs old vs new:

```
ReconcileReplicas(newAddrs []ReplicaAddr):
    for each existing shipper:
        if NOT in newAddrs → Stop and remove
    for each newAddr:
        if matching shipper exists → keep (preserve state)
        if no match → create new shipper
```

This preserves `replicaFlushedLSN`, `lastContactTime`, catch-up progress, and
background reconnect goroutines for replicas that stay in the set.

`SetReplicaAddrs` becomes a wrapper:
```go
func (v *BlockVol) SetReplicaAddrs(addrs []ReplicaAddr) {
    if v.shipperGroup == nil {
        v.shipperGroup = NewShipperGroup(nil)
    }
    v.shipperGroup.ReconcileReplicas(addrs, v.makeShipperFactory())
}
```

### Changed-address restart flow

1. Replica restarts on new port. Heartbeat reports new address.
2. Master detects endpoint change (address differs, same volume).
3. Master sends assignment update to primary with new replica address.
4. Primary's `ReconcileReplicas` receives `[oldAddr1, newAddr2]`.
5. Old shipper for the changed replica is stopped (old address gone from set).
6. New shipper created with new address — but this is a fresh shipper.
7. New shipper bootstraps: Disconnected → Connecting → CatchingUp → InSync.

The improvement over V1.5: the **other** replicas in the set are NOT disturbed.
Only the changed replica gets a fresh shipper. Recovery state for stable replicas
is preserved.

### Recovery session

Each WALShipper already contains the recovery state machine:
- `state` (Disconnected → Connecting → CatchingUp → InSync → Degraded → NeedsRebuild)
- `replicaFlushedLSN` (authoritative progress)
- `lastContactTime` (retention budget)
- `catchupFailures` (escalation counter)
- Background reconnect goroutine

No separate `RecoverySession` object is needed. The WALShipper IS the per-replica
recovery session. The state machine already tracks the session lifecycle.

What changes: the session is no longer destroyed on topology change (unless the
replica itself is removed from the set).

### Coordinator vs primary responsibilities

| Responsibility | Owner |
|---------------|-------|
| Endpoint truth (canonical address) | Coordinator (master) |
| Assignment updates (add/remove replicas) | Coordinator |
| Epoch authority | Coordinator |
| Session creation trigger | Coordinator (via assignment) |
| Session execution (reconnect, catch-up, barrier) | Primary (via WALShipper) |
| Timeout enforcement | Primary |
| Ordered receive/apply | Replica |
| Barrier ack | Replica |
| Heartbeat reporting | Replica |

### Migration from current code

| Current | V2 |
|---------|-----|
| `ShipperGroup.shippers []*WALShipper` | `ShipperGroup.shippers map[string]*WALShipper` |
| `SetReplicaAddrs()` creates all new | `ReconcileReplicas()` diffs and preserves |
| `StopAll()` in demote | `StopAll()` unchanged (stops all) |
| `ShipAll(entry)` iterates slice | `ShipAll(entry)` iterates map values |
| `BarrierAll(lsn)` parallel slice | `BarrierAll(lsn)` parallel map values |
| `MinReplicaFlushedLSN()` iterates slice | Same, iterates map values |
| `ShipperStates()` iterates slice | Same, iterates map values |
| No per-shipper identity | `WALShipper.ReplicaID` = canonical data addr |

### Files changed

| File | Change |
|------|--------|
| `wal_shipper.go` | Add `ReplicaID` field, pass in constructor |
| `shipper_group.go` | `map[string]*WALShipper`, `ReconcileReplicas`, update iterators |
| `blockvol.go` | `SetReplicaAddrs` calls `ReconcileReplicas`, shipper factory |
| `promotion.go` | No change (StopAll unchanged) |
| `dist_group_commit.go` | No change (uses ShipperGroup API) |
| `block_heartbeat.go` | No change (uses ShipperStates) |

### Acceptance bar

The following existing tests must continue to pass:
- All CP13-1 through CP13-7 protocol tests (sync_all_protocol_test.go)
- All adversarial tests (sync_all_adversarial_test.go)
- All baseline tests (sync_all_bug_test.go)
- All rebuild tests (rebuild_v1_test.go)

The following CP13-8 tests validate the V2 improvement:
- `TestCP13_SyncAll_ReplicaRestart_Rejoin` — changed-address recovery
- `TestAdversarial_ReconnectUsesHandshakeNotBootstrap` — V2 reconnect protocol
- `TestAdversarial_CatchupMultipleDisconnects` — state preservation across reconnects

New tests to add:
- `TestReconcileReplicas_PreservesExistingShipper` — stable replica keeps state
- `TestReconcileReplicas_RemovesStaleShipper` — removed replica stopped
- `TestReconcileReplicas_AddsNewShipper` — new replica bootstraps
- `TestReconcileReplicas_MixedUpdate` — one kept, one removed, one added

## Non-goals for this slice

- Smart WAL payload classes
- Recovery reservation protocol
- Full coordinator orchestration
- New transport layer
