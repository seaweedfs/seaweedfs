# CP13-5 Reconnect Handshake + WAL Catch-up — Contract Review + Proof Package

Date: 2026-04-03
Code change: `blockvol.go` SetReplicaAddrs + `shipper_group.go` AnyHasFlushedProgress

## Reconnect Decision Matrix

| Prior durable progress? | WAL covers gap? | Outcome |
|------------------------|----------------|---------|
| No (`hasFlushedProgress=false`) | N/A | Bootstrap: bare Ship + Barrier |
| Yes | Yes (gap within retained WAL) | Reconnect: ResumeShipReq handshake → catch-up replay → InSync |
| Yes | No (gap exceeds retained WAL) | Fail closed: NeedsRebuild (CP13-7 scope for full lifecycle) |

## What Changed

**Bug:** `SetReplicaAddrs` created fresh shippers with `hasFlushedProgress=false`, so after
disconnect + reconnect, the shipper used the bootstrap path instead of the reconnect handshake.
Bootstrap doesn't replay missed WAL entries, so the barrier waited forever for entries the
replica never received.

**Fix (`blockvol.go`):** `SetReplicaAddrs` now checks if the old shipper group had any
shipper with durable progress (`AnyHasFlushedProgress`). If so, new shippers are seeded
with `hasFlushedProgress=true`, routing them through the reconnect handshake + catch-up path.

**New helper (`shipper_group.go`):** `AnyHasFlushedProgress()` — returns true if any shipper
in the group has ever received a valid `FlushedLSN > 0` from a barrier response.

## Reconnect Path (production flow)

```
SetReplicaAddrs(new addresses after reconnect)
  ├─ old group had flushedProgress? → seed new shippers with hasFlushedProgress=true
  └─ new shipper created with WAL access

SyncCache → groupCommit.Submit → Barrier(lsnMax)
  ├─ state=Disconnected + hasFlushedProgress=true + wal != nil
  │   → doReconnectAndCatchUp()
  │     → reconnectWithHandshake()
  │       → TCP connect to new replica address
  │       → ResumeShipReq{Epoch, PrimaryHeadLSN, RetainStart}
  │       → replica responds with {Status, ReplicaFlushedLSN}
  │       → gap analysis: R (replica flushed) vs H (primary head) vs S (retain start)
  │         ├─ R >= H: already caught up → InSync
  │         ├─ R >= S: recoverable gap → CatchingUp → runCatchUp(R)
  │         └─ R < S: gap exceeds retention → NeedsRebuild
  │     → runCatchUp: stream WAL entries from R to H → replica applies
  │     → catch-up complete → InSync
  └─ barrier request proceeds (InSync)
```

## Baseline FAILs Now Closed

| Test | Was | Now | Why |
|------|-----|-----|-----|
| `TestAdversarial_ReconnectUsesHandshakeNotBootstrap` | FAIL | PASS | Seeded hasFlushedProgress → reconnect path used |
| `TestAdversarial_CatchupMultipleDisconnects` | FAIL | PASS | Repeated SetReplicaAddrs preserves progress seed |
| `TestAdversarial_CatchupDoesNotOverwriteNewerData` | FAIL | PASS | Catch-up now completes, safety invariant exercised |

## Baseline Tests Promoted to CP13-5 Proof

### Primary proofs

| Test | What it proves |
|------|---------------|
| `TestAdversarial_ReconnectUsesHandshakeNotBootstrap` | Degraded shipper with prior progress reconnects via handshake + catch-up |
| `TestAdversarial_CatchupMultipleDisconnects` | Repeated disconnect/reconnect cycles recover cleanly |
| `TestAdversarial_CatchupDoesNotOverwriteNewerData` | Catch-up replays missing entries without overwriting newer replica data |
| `TestReconnect_CatchupFromRetainedWal` | Retained-WAL gap replays and returns to InSync |
| `TestReconnect_EpochChangeDuringCatchup_Aborts` | Epoch change during catch-up aborts cleanly |
| `TestReconnect_CatchupTimeout_TransitionsDegraded` | Catch-up timeout → Degraded (bounded failure) |
| `TestAdversarial_FreshShipperUsesBootstrapNotReconnect` | Fresh shipper (no prior progress) uses bootstrap, not reconnect |

### Support evidence

| Test | What it supports |
|------|-----------------|
| `TestReconnect_GapBeyondRetainedWal_NeedsRebuild` | PASS* — asserts barrier failure on large gap, but full NeedsRebuild lifecycle is CP13-7 |

### Still FAIL (CP13-7 scope)

| Test | Why still fails |
|------|----------------|
| `TestAdversarial_NeedsRebuildBlocksAllPaths` | Full NeedsRebuild lifecycle — lease expiry + WAL overflow timing; CP13-7 scope |

## What CP13-5 Does NOT Close

- Replica-aware WAL retention policy (CP13-6)
- Full NeedsRebuild lifecycle / rebuild execution (CP13-7)
- The `TestAdversarial_NeedsRebuildBlocksAllPaths` failure is a CP13-7 gap, not CP13-5
