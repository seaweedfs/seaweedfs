# Phase 12 P4 — Rollout Gates

Date: 2026-04-02
Scope: bounded first-launch envelope for the accepted RF=2, sync_all chosen path.

This is a bounded first-launch envelope, not general readiness.

## Supported Launch Envelope

Only the transport/network combinations with measured baselines are included.

| Parameter | Value |
|-----------|-------|
| Topology | RF=2, sync_all |
| Transport + Network | NVMe-TCP @ 25Gbps RoCE (measured), iSCSI @ 25Gbps RoCE (measured), iSCSI @ 1Gbps (measured) |
| NOT included | NVMe-TCP @ 1Gbps (not measured) |
| Volume size | Up to 2GB (tested baseline) |
| Failover | Lease-based, bounded by TTL (30s default) |
| Recovery | Catch-up-first, rebuild fallback |
| Degraded mode | Documented -66% write penalty (sync_all RF=2, one replica dead) |

## Cleared Gates

| Gate | Evidence | Status | Notes |
|------|----------|--------|-------|
| G1 | P1 disturbance tests pass | Cleared | Restart/reconnect correctness under disturbance |
| G2 | P2 soak tests pass | Cleared | Repeated create/failover/recover cycles, no drift |
| G3 | P3 diagnosability tests pass | Cleared | Explicit bounded diagnosis surfaces for all symptom classes |
| G4 | P4 floor gates pass | Cleared | Explicit IOPS thresholds + P99 ceilings enforced per workload in code |
| G5 | P4 cost characterization bounded | Cleared | WAL 2x write amp, -56% replication tax documented |
| G6 | Production baseline exists | Cleared | baseline-roce-20260401.md: 28.4K write IOPS, 136.6K read IOPS |
| G8 | Floor gates are regression-safe | Cleared | Test fails if any workload drops below defined minimum IOPS or exceeds P99 ceiling |
| G7 | Blocker ledger finite | Cleared | 3 diagnosed (B1-B3) + 3 unresolved (U1-U3), all explicit |

## Remaining Blockers / Exclusions

| Exclusion | Why | Impact |
|-----------|-----|--------|
| E1 | Failover-under-load perf not measured | Cannot claim bounded perf during failover |
| E2 | Hours/days soak not run | Cannot claim long-run stability under sustained load |
| E3 | RF>2 not measured | Cannot claim perf floor for RF=3+ |
| E4 | Broad transport matrix not tested | Cannot claim parity across all kernel/NVMe/iSCSI versions |
| E5 | Degraded mode is severe (-66%) | sync_all RF=2 has sharp write cliff on replica death |
| E6 | V2 stale-epoch at orchestrator level (U1 from P3) | V1 guards suffice; V2 is secondary path |
| E7 | gRPC stream transport not exercised in unit tests (U3 from P3) | Blocks full integration test, not correctness |

## Reject Conditions

This launch envelope should be REJECTED if:

1. Any P1/P2/P3 test regresses (correctness/stability/diagnosability gate violated)
2. Production baseline numbers are not reproducible on the target hardware
3. Degraded mode behavior (-66% cliff) is not acceptable for the deployment scenario
4. The deployment requires RF>2, failover-under-load guarantees, or long soak proof
5. The deployment requires transport combinations not covered by the baseline

## What P4 does NOT claim

- This does not claim general production readiness.
- This does not claim readiness for any deployment outside the named launch envelope.
- This does not claim that the performance floor is optimal or final.
- This does not claim that the degraded-mode penalty is acceptable (deployment-specific decision).
- This does not claim hours/days stability under sustained load.
- This is a bounded first-launch gate, not a broad rollout approval.
