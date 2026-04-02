# Phase 12 P4 — Performance Floor Summary

Date: 2026-04-02
Scope: bounded performance floor for the accepted RF=2, sync_all chosen path.

## Workload Envelope

| Parameter | Value |
|-----------|-------|
| Topology | RF=2, sync_all |
| Operations | 4K random write, 4K random read, sequential write, sequential read |
| Runtime | Steady-state, no failover, no disturbance |
| Path | Accepted chosen path (same as P1/P2/P3) |

## Environment

### Unit Test Harness (engine-local)

| Parameter | Value |
|-----------|-------|
| Name | `TestP12P4_PerformanceFloor_Bounded` |
| Location | `weed/server/qa_block_perf_test.go` |
| Platform | Single-process, local disk |
| Volume | 64MB, 4K blocks, 16MB WAL |
| Writer | Single-threaded (worst-case for group commit) |
| Replication | Not exercised (engine-local only) |
| Measurement | Worst of 3 iterations (floor, not peak) |

### Production Baseline (cross-machine)

| Parameter | Value |
|-----------|-------|
| Name | `baseline-roce-20260401` |
| Location | `learn/projects/sw-block/test/results/baseline-roce-20260401.md` |
| Hardware | m01 (10.0.0.1) - M02 (10.0.0.3), 25Gbps RoCE |
| Protocol | NVMe-TCP |
| Volume | 2GB, RF=2, sync_all, cross-machine replication |
| Writer | fio, QD1-128, j=4 |

## Floor Table: Production (RF=2, sync_all, NVMe-TCP, 25Gbps RoCE)

These are measured floor values from the production baseline, not the unit test.

| Workload | Floor IOPS | Notes |
|----------|-----------|-------|
| 4K random write QD1 | 28,347 | Barrier round-trip limited (flat across QD) |
| 4K random write QD32 | 28,453 | Same barrier ceiling |
| 4K random read QD32 | 136,648 | No replication overhead |
| Mixed 70/30 QD32 | 28,423 | Write-side limited |

Latency: Write latency is bounded by sync_all barrier round-trip (~35us at QD1).
Read latency: sub-microsecond for cached, single-digit microseconds for extent.

## Floor Table: Engine-Local (unit test harness)

These values are measured by `TestP12P4_PerformanceFloor_Bounded` on the dev machine.
They characterize the engine I/O floor WITHOUT transport or replication.
Actual values vary by hardware; the test produces them on each run.

| Workload | Metric | Method | Gate |
|----------|--------|--------|------|
| 4K random write | Floor IOPS, Avg/P50/P99/Max latency | Worst of 3 iterations | >= 1,000 IOPS, P99 <= 100ms |
| 4K random read | Floor IOPS, Avg/P50/P99/Max latency | Worst of 3 iterations | >= 5,000 IOPS |
| 4K sequential write | Floor IOPS, Avg/P50/P99/Max latency | Worst of 3 iterations | >= 2,000 IOPS, P99 <= 100ms |
| 4K sequential read | Floor IOPS, Avg/P50/P99/Max latency | Worst of 3 iterations | >= 10,000 IOPS |

Gate thresholds are regression gates enforced in code (`perfFloorGates` in `qa_block_perf_test.go`).
Set at ~10% of measured values to tolerate slow CI/VM hardware while catching catastrophic regressions.

## Cost Summary

| Cost | Value | Source |
|------|-------|--------|
| WAL write amplification | 2x minimum | Engine design: each write → WAL + eventual extent flush |
| Replication tax (RF=2 sync_all vs RF=1) | -56% | baseline-roce-20260401.md (NVMe-TCP, 25Gbps RoCE) |
| Replication tax (RF=2 sync_all vs RF=1, iSCSI 1Gbps) | -56% | baseline-roce-20260401.md |
| Degraded mode penalty (sync_all RF=2, one replica dead) | -66% | baseline-roce-20260401.md (barrier timeout) |
| Group commit | 1 fdatasync per batch | Amortizes sync cost across concurrent writers |

## Acceptance Evidence

| Item | Evidence | Type |
|------|----------|------|
| Floor gates pass | `perfFloorGates` thresholds enforced per workload | Acceptance |
| Workload runs repeatably | `TestP12P4_PerformanceFloor_Bounded` passes | Acceptance |
| Cost statement is bounded | `TestP12P4_CostCharacterization_Bounded` passes | Acceptance |
| Production baseline exists | `baseline-roce-20260401.md` with measured values | Acceptance |
| Floor is worst-of-N, not peak | Test takes minimum IOPS across 3 iterations | Method |
| Regression-safe | Test fails if floor drops below gate (blocks rollout) | Acceptance |
| Replication tax documented | -56% from measured production baseline | Support telemetry |

## What P4 does NOT claim

- This is not a claim that the measured floor is "good enough" for any specific application.
- This does not claim readiness for failover-under-load scenarios.
- This does not claim readiness for hours/days soak under load.
- This does not claim readiness for RF>2 topologies.
- This does not claim readiness for all transport combinations (iSCSI + NVMe + kernel versions).
- This does not claim readiness for production rollout beyond the explicitly named launch envelope.
- Engine-local floor numbers are not production floor numbers.
- The replication tax is measured on one specific hardware configuration and may differ on other hardware.
