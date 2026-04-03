# CP13-3 Durable Progress Truth — Contract Review + Proof Package

Date: 2026-04-03
Commit: ac962fc83 → updated with legacy-response rejection fix

## Durable Progress Contract

### Definition

`replicaFlushedLSN` is the **sole authority** for replica durability in the sync_all path.

It means: the replica has called `fd.Sync()` (WAL fdatasync) for all entries through this LSN, and the barrier response carrying this value has reached the primary.

### What is NOT durable authority

| Variable | Location | Role | Why NOT authority |
|----------|----------|------|-------------------|
| `shippedLSN` | `wal_shipper.go:269` | Diagnostic | Tracks last LSN sent over TCP; receipt not confirmed |
| `receivedLSN` | `replica_apply.go:362` | Intermediate | Entry applied to WAL buffer; not yet fsynced |
| `sentLSN` / transport progress | shipper send loop | Diagnostic | TCP write completed; no durability guarantee |

### Where durable authority lives

| Component | File | How it works |
|-----------|------|-------------|
| Replica: barrier handler | `replica_barrier.go:53-110` | Waits for `receivedLSN >= req.LSN`, calls `fd.Sync()`, advances `flushedLSN` only after sync succeeds, returns `BarrierResponse{FlushedLSN: flushed}` |
| Shipper: barrier consumer | `wal_shipper.go:220-238` | Reads `resp.FlushedLSN`, updates `replicaFlushedLSN` via monotonic CAS (never decreases) |
| Shipper: explicit API | `wal_shipper.go:273-278` | `ReplicaFlushedLSN()` is the authoritative API; `ShippedLSN()` has explicit "NOT authoritative" comment at line 268 |
| Group commit: sync_all | `dist_group_commit.go:15-83` | `BarrierAll(lsnMax)` called in parallel with local WAL sync; sync_all fails if any barrier fails |
| SyncCache entry | `blockvol.go:774-782` | `groupCommit.Submit()` → distributed sync → barrier → durability |

### Durability proof chain

```
WriteLBA → appendWithRetry → WAL.Append + Ship (fire-and-forget)
    ↓
SyncCache → groupCommit.Submit → distributedSync:
    ├─ local: walSync (fd.Sync on primary WAL)
    └─ remote: group.BarrierAll(lsnMax)
              → shipper.Barrier(lsnMax)
                → WriteFrame(MsgBarrierReq) to replica
                → replica handleBarrier:
                    1. wait receivedLSN >= LSN
                    2. fd.Sync() — THIS IS THE DURABILITY EVENT
                    3. advance flushedLSN
                    4. return BarrierResponse{FlushedLSN}
                ← ReadFrame(MsgBarrierResp)
                ← update replicaFlushedLSN (monotonic CAS)
              → if BarrierOK: markInSync
    ↓
sync_all: ALL barriers must succeed → SyncCache returns nil
sync_all: ANY barrier fails → ErrDurabilityBarrierFailed
```

## Baseline Test Promotion

The following CP13-1 baseline PASS tests are promoted to CP13-3 proof:

### Primary proofs (directly verify the durable-progress contract)

| Test | What it proves for CP13-3 |
|------|--------------------------|
| `TestReplicaProgress_BarrierUsesFlushedLSN` | Barrier success is gated on `replicaFlushedLSN`, not `shippedLSN` |
| `TestReplicaProgress_FlushedLSNMonotonicWithinEpoch` | `replicaFlushedLSN` never decreases within an epoch |
| `TestReplica_FlushedLSN_OnlyAfterSync` | `flushedLSN` only advanced after `fd.Sync()` — not on entry receive |
| `TestReplica_FlushedLSN_NotOnReceive` | Receiving an entry does NOT advance `flushedLSN` — confirms receive != durable |
| `TestShipper_ReplicaFlushedLSN_UpdatedOnBarrier` | Shipper's tracked `replicaFlushedLSN` comes from barrier response, not from send |
| `TestShipper_ReplicaFlushedLSN_Monotonic` | Shipper's tracked progress is monotonic (CAS-only, never decreases) |
| `TestBarrierResp_FlushedLSN_Roundtrip` | Barrier response wire format correctly carries `flushedLSN` |
| `TestBarrierResp_BackwardCompat_1Byte` | Old 1-byte responses decode to `FlushedLSN=0` (wire compat) |
| `TestBarrier_LegacyResponseRejectedBySyncAll` | Legacy `BarrierOK` with `FlushedLSN=0` is rejected — no false durability authority |

### Support evidence (adjacent to the contract, not primary proof)

| Test | What it supports |
|------|-----------------|
| `TestBarrier_RejectsReplicaNotInSync` | Barrier rejects non-InSync replica — guards barrier correctness |
| `TestBarrier_EpochMismatchRejected` | Barrier rejects epoch mismatch — guards against stale durability claims |
| `TestBarrier_ReplicaSlowFsync_Timeout` | Barrier times out on slow fsync — bounded, not unbounded wait |
| `TestShipperGroup_MinReplicaFlushedLSN` | Group computes min flushedLSN across replicas — multi-replica support |
| `TestDistSync_SyncAll_NilGroup_Succeeds` | sync_all with no replicas = local-only (correct degenerate case) |
| `TestDistSync_SyncAll_AllDegraded_Fails` | sync_all fails when all replicas degraded — fail-closed |

### Out of scope for CP13-3

| Test | Why out of scope |
|------|-----------------|
| `TestBarrier_DuringCatchup_Rejected` | Barrier during CatchingUp — this is CP13-4 (state machine) |
| `TestReconnect_*` | Reconnect/catch-up — this is CP13-5 |
| `TestWalRetention_*` | WAL retention — this is CP13-6 |
| `TestAdversarial_NeedsRebuild*` | NeedsRebuild state — this is CP13-7 |

## Sender-Side Progress: Explicitly Diagnostic

Code evidence that `shippedLSN` / `sentLSN` are non-authoritative:

```go
// wal_shipper.go:266-271
// ShippedLSN returns the highest LSN sent to the replica.
// This is NOT authoritative for sync durability — use ReplicaFlushedLSN() instead.
func (s *WALShipper) ShippedLSN() uint64 {
    return s.shippedLSN.Load()
}
```

The comment at line 268 is explicit: sender-side progress is diagnostic only.

## Code Change

One targeted fix in `wal_shipper.go`: `BarrierOK` with `FlushedLSN == 0` now returns
an error instead of counting as successful sync_all durability. This closes the gap
where a legacy 1-byte barrier response could pass through as durable authority.

## What CP13-3 Does NOT Close

- Reconnect/catch-up protocol (CP13-5)
- WAL retention policy (CP13-6)
- Rebuild fallback (CP13-7)
- Replica state machine transitions beyond barrier eligibility (CP13-4)
