# CP13-4 Replica State Machine / Barrier Eligibility — Contract Review + Proof Package

Date: 2026-04-03
Code change: one new test (`TestBarrier_NonEligibleStates_FailClosed`)

## Replica State Set

The replication path uses a bounded 6-state set (`wal_shipper.go:25-30`):

| State | Value | Meaning | Barrier eligible? |
|-------|-------|---------|-------------------|
| `Disconnected` | 0 | No session (initial state) | No — attempts bootstrap/reconnect, fails if no progress |
| `Connecting` | 1 | Socket open, handshake pending | No — immediate `ErrReplicaDegraded` |
| `CatchingUp` | 2 | Connected, replaying missed WAL | No — immediate `ErrReplicaDegraded` |
| `InSync` | 3 | Eligible for sync_all barriers | **Yes** — only state that proceeds to barrier request |
| `Degraded` | 4 | Transient failure, retry allowed | No — attempts reconnect, fails if reconnect fails |
| `NeedsRebuild` | 5 | WAL gap too large, rebuild required | No — immediate `ErrReplicaDegraded` |

## Barrier State Gate

`WALShipper.Barrier()` at `wal_shipper.go:160-182`:

```go
st := s.State()
switch st {
case ReplicaInSync:
    // proceed normally to barrier
case ReplicaDisconnected, ReplicaDegraded:
    // attempt reconnect; error if fails
default:
    // Connecting, CatchingUp, NeedsRebuild — reject immediately
    return ErrReplicaDegraded
}
```

**Only `InSync` enters the barrier request path.** All other states either fail immediately or attempt reconnect (which must succeed and transition to `InSync` before reaching the barrier).

## sync_all Gate

`dist_group_commit.go:59-66`: sync_all counts barrier failures. Any shipper that returns an error from `Barrier()` increments `failCount`. If `failCount > 0`, sync_all returns `ErrDurabilityBarrierFailed`.

Combined with the CP13-3 fix (FlushedLSN=0 rejected), the full chain is:
1. Only `InSync` shippers reach the barrier request
2. Only `BarrierOK` with `FlushedLSN > 0` counts as success
3. sync_all fails if any barrier fails

## Proof Promotion

### Primary proofs (directly verify state/eligibility contract)

| Test | What it proves for CP13-4 |
|------|--------------------------|
| `TestBarrier_NonEligibleStates_FailClosed` | Connecting, CatchingUp, NeedsRebuild all rejected immediately; Disconnected fails on dead address; only InSync enters barrier path |
| `TestBarrier_RejectsReplicaNotInSync` | SyncCache fails when replica is not InSync (end-to-end) |
| `TestBarrier_DuringCatchup_Rejected` | Barrier rejected while replica is CatchingUp |
| `TestDistSync_SyncAll_AllDegraded_Fails` | sync_all fails when all replicas degraded |
| `TestAdversarial_FreshShipperUsesBootstrapNotReconnect` | Fresh (Disconnected, no prior progress) shipper uses bootstrap path |

### Support evidence

| Test | What it supports |
|------|-----------------|
| `TestBarrier_EpochMismatchRejected` | Barrier rejects epoch mismatch — adjacent to eligibility |
| `TestBarrier_ReplicaSlowFsync_Timeout` | Barrier timeout — bounded failure, not silent success |

### Out of scope for CP13-4

| Test | Why |
|------|-----|
| `TestReconnect_*` | Reconnect protocol — CP13-5 |
| `TestWalRetention_*` | Retention — CP13-6 |
| `TestAdversarial_NeedsRebuildBlocksAllPaths` | Full NeedsRebuild lifecycle — CP13-5+CP13-7 |

## What CP13-4 Does NOT Close

- Reconnect/catch-up protocol (CP13-5)
- WAL retention policy (CP13-6)
- Rebuild fallback (CP13-7)
- The Disconnected/Degraded reconnect paths are tested for failure on dead addresses, but the actual reconnect protocol is CP13-5 scope
