# CP13-6 Replica-Aware WAL Retention — Contract Review + Proof Package

Date: 2026-04-03
Code change: `shipper_group.go` EvaluateRetentionBudgets + `blockvol.go` caller updates + test fix

## Retention Contract

### Inputs

| Input | Source | How it's used |
|-------|--------|---------------|
| `replicaFlushedLSN` | Barrier response (CP13-3 authority) | Retention floor: WAL must keep entries from this LSN forward |
| `primaryHeadLSN` | `nextLSN.Load() - 1` | Lag calculation: head - replicaFlushed = entries the replica still needs |
| `lastContactTime` | Barrier/handshake success time | Timeout budget: how long since the replica was heard from |

### Decision matrix

| Condition | Action |
|-----------|--------|
| Recoverable replica needs WAL entries | Hold: flusher does not advance tail past `minRecoverableFlushedLSN` |
| Replica last contact exceeds `walRetentionTimeout` (5min) | Escalate to `NeedsRebuild`, release hold |
| Replica lag exceeds `walRetentionMaxBytes` (64MB default) | Escalate to `NeedsRebuild`, release hold |
| Replica in `NeedsRebuild` | Excluded from retention floor (`MinRecoverableFlushedLSN` skips it) |
| No recoverable replicas | No retention hold (flusher advances freely) |

### Code path

```
Flusher.FlushOnce()
  ├─ EvaluateRetentionBudgetsFn()  → shipper_group.EvaluateRetentionBudgets(timeout, maxBytes, primaryHead)
  │   ├─ for each recoverable shipper:
  │   │   ├─ timeout exceeded? → state.Store(NeedsRebuild)
  │   │   └─ lag * 4KB > maxBytes? → state.Store(NeedsRebuild)
  │   └─ NeedsRebuild shippers excluded from future floor computation
  ├─ RetentionFloorFn()  → shipper_group.MinRecoverableFlushedLSN()
  │   └─ returns min flushedLSN of non-NeedsRebuild shippers with prior progress
  └─ if maxLSN > floorLSN: hold WAL (don't advance tail)
     else: advance tail normally
```

## What Changed

**`shipper_group.go`:** `EvaluateRetentionBudgets` now takes `maxBytes` and `primaryHeadLSN` params.
Checks each recoverable replica: if `(primaryHeadLSN - replicaFlushedLSN) * 4KB > maxBytes`,
transitions to `NeedsRebuild`. This is a real state effect, not a log-only placeholder.

**`blockvol.go`:** Callers updated to pass `walRetentionMaxBytes` (64MB) and `v.nextLSN.Load()-1`.

**`sync_all_protocol_test.go`:** `TestWalRetention_MaxBytesTriggersNeedsRebuild` rewritten from
PASS* (log-only) to real PASS: asserts `s.State() == ReplicaNeedsRebuild` after max-bytes exceeded.

## Baseline PASS* Now Closed

| Test | Was | Now | Why |
|------|-----|-----|-----|
| `TestWalRetention_MaxBytesTriggersNeedsRebuild` | PASS* (logged "not implemented") | PASS (asserts NeedsRebuild) | Max-bytes budget triggers real state transition |

## Proof Promotion

### Primary proofs

| Test | What it proves |
|------|---------------|
| `TestWalRetention_RequiredReplicaBlocksReclaim` | Recoverable replica blocks WAL reclaim |
| `TestWalRetention_TimeoutTriggersNeedsRebuild` | Timeout budget → NeedsRebuild (releases hold) |
| `TestWalRetention_MaxBytesTriggersNeedsRebuild` | Max-bytes budget → NeedsRebuild (real state effect) |

## What CP13-6 Does NOT Close

- Full NeedsRebuild lifecycle / rebuild execution (CP13-7)
- `TestAdversarial_NeedsRebuildBlocksAllPaths` still FAIL (CP13-7)
