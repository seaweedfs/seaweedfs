# CP13-7 Rebuild Fallback — Contract Review + Proof Package

Date: 2026-04-03
Code change: `sync_all_adversarial_test.go` + `sync_all_protocol_test.go` test rewrites

## NeedsRebuild Contract

### Entry

| Trigger | Source | Result |
|---------|--------|--------|
| Timeout budget exceeded | `EvaluateRetentionBudgets` (CP13-6) | `state.Store(NeedsRebuild)` |
| Max-bytes budget exceeded | `EvaluateRetentionBudgets` (CP13-6) | `state.Store(NeedsRebuild)` |
| Reconnect detects impossible progress | `reconnectWithHandshake` (CP13-5) | Returns `NeedsRebuild` |
| Reconnect detects gap beyond retained WAL | `reconnectWithHandshake` (CP13-5) | Returns `NeedsRebuild` |
| Catch-up failures exceed max retries | `doReconnectAndCatchUp` | `state.Store(NeedsRebuild)` |

### Blocking (fail-closed)

| Path | Behavior when NeedsRebuild |
|------|---------------------------|
| `Ship()` | Silently drops (state != InSync and != Disconnected) |
| `Barrier()` | Immediate `ErrReplicaDegraded` (default case in state switch) |
| `MinRecoverableFlushedLSN` | Excluded (NeedsRebuild shippers skipped) |
| `EvaluateRetentionBudgets` | Skipped (already escalated) |

### Visibility

| Surface | What it reports |
|---------|----------------|
| Heartbeat `ReplicaShipperStates` | `state: "needs_rebuild"` per-replica |
| `WALShipper.State()` | `ReplicaNeedsRebuild` (5) |

### Rebuild handoff

| Step | What happens |
|------|-------------|
| Master detects `NeedsRebuild` in heartbeat | Sends Rebuilding assignment to replica VS |
| Replica `HandleAssignment(RoleRebuilding)` | Starts rebuild from primary |
| `StartRebuild` completes | 3-phase copy (full extent + WAL catch-up) |
| Post-rebuild: `flushedLSN = checkpointLSN` | Not stale/zero — initialized from durable baseline |
| Master sends fresh Primary assignment | `SetReplicaAddrs` → fresh shipper → bootstrap → InSync |

### Abort

| Condition | Result |
|-----------|--------|
| Epoch changes during rebuild | `RebuildServer` rejects with `EPOCH_MISMATCH` |
| Rebuild copy fails | Error returned, role stays `RoleRebuilding` |

## Baseline Closures

| Test | Was | Now | What it proves |
|------|-----|-----|----------------|
| `TestAdversarial_NeedsRebuildBlocksAllPaths` | FAIL | PASS | NeedsRebuild blocks Ship (drops) + Barrier (rejects) + is sticky across retries |
| `TestReconnect_GapBeyondRetainedWal_NeedsRebuild` | PASS* | PASS | Unrecoverable gap → NeedsRebuild state assertion + SyncCache fails |

## Proof Promotion

### Primary proofs

| Test | What it proves for CP13-7 |
|------|--------------------------|
| `TestAdversarial_NeedsRebuildBlocksAllPaths` | 5 assertions: NeedsRebuild state, Ship drops, Barrier rejects, state sticky after barrier, second SyncCache still fails |
| `TestReconnect_GapBeyondRetainedWal_NeedsRebuild` | Unrecoverable gap → hard NeedsRebuild assertion + SyncCache failure |
| `TestHeartbeat_ReportsNeedsRebuild` | Heartbeat carries per-replica `needs_rebuild` state |
| `TestReplicaState_RebuildComplete_ReentersInSync` | Full rebuild cycle: NeedsRebuild → rebuild → fresh shipper → InSync |
| `TestRebuild_AbortOnEpochChange` | Epoch mismatch during rebuild → abort |
| `TestRebuild_PostRebuild_FlushedLSN_IsCheckpoint` | Post-rebuild `flushedLSN = checkpointLSN` (not stale/zero) |

## Updated Baseline Summary

| | PASS | FAIL | PASS* |
|---|---|---|---|
| CP13-1 (original) | 37 | 4 | 3 |
| After CP13-2..CP13-7 | **43** | **0** | **1** |

Remaining PASS*: `TestBug3_ReplicaAddr_MustBeIPPort_WildcardBind` (CP13-2 address witness — already upgraded to real proof in test but baseline doc still lists it as PASS*).

## What CP13-7 Does NOT Close

- Real-workload validation (CP13-8)
- Broad rollout or performance claims
- Mode normalization (CP13-9)
