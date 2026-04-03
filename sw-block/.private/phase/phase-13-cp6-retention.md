# CP13-6 Replica-Aware WAL Retention — Contract Review + Proof Package

Date: 2026-04-03
Code change: `shipper_group.go` EvaluateRetentionBudgets (params struct + block-size-aware) + `blockvol.go` caller updates + 3 tests rewritten with hard assertions

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

**`shipper_group.go`:** `EvaluateRetentionBudgets` now takes `RetentionBudgetParams` struct
with `Timeout`, `MaxBytes`, `PrimaryHeadLSN`, and `BlockSize` (from volume config).
Max-bytes lag computed as `entryLag * BlockSize`, not hardcoded 4096.
Both timeout and max-bytes checks transition to `NeedsRebuild` with real state effects.

**`blockvol.go`:** Added `walRetentionMaxBytes` (64MB default). Callers pass `RetentionBudgetParams`
with actual `v.super.BlockSize`.

**`sync_all_protocol_test.go`:** All 3 retention tests rewritten with hard assertions (no log-only placeholders).

## Tests Upgraded

All 3 retention tests rewritten from placeholder/PASS* to hard-assertion proofs:

| Test | Was | Now | Hard assertion |
|------|-----|-----|----------------|
| `TestWalRetention_RequiredReplicaBlocksReclaim` | PASS (log-only, no assertion) | PASS (hard assert) | `checkpointLSN <= replicaFlushedLSN` — flusher did not advance past retention floor |
| `TestWalRetention_TimeoutTriggersNeedsRebuild` | PASS (log-only, no assertion) | PASS (hard assert) | `s.State() == NeedsRebuild` + `checkpointAfter > replicaFlushedLSN` (hold released) |
| `TestWalRetention_MaxBytesTriggersNeedsRebuild` | PASS* (logged "not implemented") | PASS (hard assert) | `s.State() == NeedsRebuild` after lag exceeds 8KB budget |

## Proof Promotion

### Primary proofs

| Test | What it proves |
|------|---------------|
| `TestWalRetention_RequiredReplicaBlocksReclaim` | Flusher checkpoint does not advance past `replicaFlushedLSN` while recoverable replica is behind |
| `TestWalRetention_TimeoutTriggersNeedsRebuild` | Timeout budget → `NeedsRebuild` (State assertion) + checkpoint advances past replicaFlushedLSN after flush (hold-release assertion) |
| `TestWalRetention_MaxBytesTriggersNeedsRebuild` | Max-bytes budget evaluation transitions shipper to `NeedsRebuild` (verified via `State()` assertion, uses actual `BlockSize` from volume config) |

## What CP13-6 Does NOT Close

- Full NeedsRebuild lifecycle / rebuild execution (CP13-7)
- `TestAdversarial_NeedsRebuildBlocksAllPaths` still FAIL (CP13-7)
