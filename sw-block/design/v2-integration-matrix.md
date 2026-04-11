# V2 Integration Matrix

Date: 2026-04-08
Status: active

## Purpose

This matrix maps real end-to-end integration scenarios across common and edge
product paths. It is the operational counterpart to `v2-validation-matrix.md`:

- **Validation matrix**: is this capability covered at all?
- **Integration matrix**: have we exercised enough real scenarios with real data,
  real topology, real failure, and real final-state validation?

## Cross-Link Rule

Use each integration row as the last step in a chain:

`protocol -> capability tier -> validation row -> integration scenario -> evidence`

The extra columns below make that chain explicit:

1. `Capability` points back to `v2-capability-map.md`
2. `Validation` points back to `v2-validation-matrix.md`
3. `Proof tier` states whether the row is currently covered by `component`,
   `integration`, or `hardware` evidence

## Inventory: Existing V1/V1.5 Integration Scenarios

The testrunner has 72 internal + 44 external scenarios. Below is the
categorized inventory with V2 reuse assessment.

### Category 1: Bootstrap / Smoke (reuse unchanged for V2)

| Scenario | File | What it tests | V2 reuse |
|---|---|---|---|
| `smoke-iscsi` | `scenarios/smoke-iscsi.yaml` | Basic iSCSI create + write + read | Reuse unchanged |
| `p0-validation` | `scenarios/internal/p0-validation.yaml` | Minimal cluster + volume lifecycle | Reuse unchanged |
| `recovery-bootstrap-closure` | `scenarios/internal/recovery-bootstrap-closure.yaml` | Stage 0: create → fence → publish_healthy | Reuse unchanged |
| `coord-smoke-iscsi` | `scenarios/internal/coord-smoke-iscsi.yaml` | Coordinator mode smoke | Reuse unchanged |

### Category 2: HA / Failover (reuse with V2 observation)

| Scenario | File | What it tests | V2 reuse |
|---|---|---|---|
| `recovery-baseline-failover` | `scenarios/internal/recovery-baseline-failover.yaml` | Auto-failover + data continuity | Reuse — add V2 projection checks |
| `ha-failover` | `scenarios/ha-failover.yaml` | HA failover basic | Reuse with adapter |
| `ha-full-lifecycle` | `scenarios/ha-full-lifecycle.yaml` | Create → write → failover → verify | Reuse with adapter |
| `ha-io-continuity` | `scenarios/ha-io-continuity.yaml` | I/O continuity through failover | Reuse unchanged |
| `ha-rebuild` | `scenarios/ha-rebuild.yaml` | Rebuild after failover | Reuse with V2 session |
| `ha-rf3-failover` | `scenarios/ha-rf3-failover.yaml` | RF3 failover | Reuse with adapter |
| `suite-ha-failover` | `scenarios/internal/suite-ha-failover.yaml` | HA suite | Reuse with adapter |
| `cp11b3-manual-promote` | `scenarios/cp11b3-manual-promote.yaml` | Manual promote + rejoin | Reuse with adapter |
| `cp11b3-auto-failover` | `scenarios/cp11b3-auto-failover.yaml` | Auto failover | Reuse unchanged |
| `ha-multi-client-failover` | `scenarios/internal/ha-multi-client-failover.yaml` | Multi-client during failover | Reuse unchanged |
| `ha-read-load-failover` | `scenarios/internal/ha-read-load-failover.yaml` | Read load during failover | Reuse unchanged |
| `ha-nvme-failover` | `scenarios/internal/ha-nvme-failover.yaml` | NVMe-oF failover | Reuse unchanged |

### Category 3: Rebuild / Recovery (key V2 upgrade area)

| Scenario | File | What it tests | V2 reuse |
|---|---|---|---|
| `ha-rebuild` | `scenarios/ha-rebuild.yaml` | V1 rebuild path | Replace with V2 session-controlled rebuild |
| `ha-failover-during-rebuild` | `scenarios/internal/ha-failover-during-rebuild.yaml` | Failover during active rebuild | Replace — V2 rebuild is different path |
| `ha-wal-pressure-failover` | `scenarios/internal/ha-wal-pressure-failover.yaml` | WAL pressure during failover | Reuse — now with CP13-6 disabled |
| `recovery-baseline-crash` | `scenarios/internal/recovery-baseline-crash.yaml` | Crash recovery | Reuse unchanged |
| `recovery-baseline-restart` | `scenarios/internal/recovery-baseline-restart.yaml` | Restart recovery | Reuse unchanged |
| `recovery-baseline-partition` | `scenarios/internal/recovery-baseline-partition.yaml` | Network partition recovery | Reuse with adapter |
| `robust-reconnect-catchup` | `scenarios/internal/robust-reconnect-catchup.yaml` | Reconnect + catch-up | Reuse with V2 session |
| `robust-gap-failover` | `scenarios/internal/robust-gap-failover.yaml` | Gap-based failover | Reuse with adapter |
| `robust-shipper-lifecycle` | `scenarios/internal/robust-shipper-lifecycle.yaml` | Shipper state machine | Replace with V2 session lifecycle |
| `robust-shipper-reconnect` | `scenarios/internal/robust-shipper-reconnect.yaml` | Shipper reconnect | Replace with V2 session |
| `robust-slow-replica` | `scenarios/internal/robust-slow-replica.yaml` | Slow replica handling | Reuse — CP13-6 disabled |
| `ec3-fast-reconnect-skips-failover` | `scenarios/internal/ec3-fast-reconnect-skips-failover.yaml` | Fast reconnect | Reuse unchanged |
| `ec5-wrong-primary-master-restart` | `scenarios/internal/ec5-wrong-primary-master-restart.yaml` | Wrong primary after master restart | Reuse unchanged |

### Category 4: Stability / Chaos (reuse unchanged)

| Scenario | File | What it tests | V2 reuse |
|---|---|---|---|
| `cp85-chaos-disk-full` | `scenarios/cp85-chaos-disk-full.yaml` | Disk full fault | Reuse unchanged |
| `cp85-chaos-partition` | `scenarios/cp85-chaos-partition.yaml` | Network partition chaos | Reuse unchanged |
| `cp85-chaos-primary-kill-loop` | `scenarios/cp85-chaos-primary-kill-loop.yaml` | Repeated primary kills | Reuse unchanged |
| `cp85-chaos-replica-kill-loop` | `scenarios/cp85-chaos-replica-kill-loop.yaml` | Repeated replica kills | Reuse unchanged |
| `cp85-role-flap` | `scenarios/cp85-role-flap.yaml` | Rapid role changes | Reuse with adapter |
| `cp85-session-storm` | `scenarios/cp85-session-storm.yaml` | Session storm | Reuse unchanged |
| `cp85-snapshot-stress` | `scenarios/cp85-snapshot-stress.yaml` | Snapshot stress | Reuse unchanged |
| `stable-degraded-mode` | `scenarios/internal/stable-degraded-mode.yaml` | Degraded mode stability | Reuse unchanged |
| `stable-degraded-best-effort` | `scenarios/internal/stable-degraded-best-effort.yaml` | Best-effort degraded | Reuse unchanged |
| `stable-degraded-sync-quorum` | `scenarios/internal/stable-degraded-sync-quorum.yaml` | Sync quorum degraded | Reuse unchanged |

### Category 5: Performance / Soak (reuse unchanged)

| Scenario | File | What it tests | V2 reuse |
|---|---|---|---|
| `cp85-perf-baseline` | `scenarios/cp85-perf-baseline.yaml` | Performance baseline | Reuse unchanged |
| `cp103-perf-baseline` | `scenarios/cp103-perf-baseline.yaml` | Phase 10 perf | Reuse unchanged |
| `cp84-soak-4h` | `scenarios/cp84-soak-4h.yaml` | 4-hour soak | Reuse unchanged |
| `cp85-soak-24h` | `scenarios/cp85-soak-24h.yaml` | 24-hour soak | Reuse unchanged |
| `cp103-soak-iscsi-1h` | `scenarios/internal/cp103-soak-iscsi-1h.yaml` | 1-hour iSCSI soak | Reuse unchanged |
| `cp103-soak-nvme-1h` | `scenarios/internal/cp103-soak-nvme-1h.yaml` | 1-hour NVMe soak | Reuse unchanged |
| `benchmark-pgbench` | `scenarios/internal/benchmark-pgbench.yaml` | pgbench workload | Reuse unchanged |

### Category 6: Snapshot / Expand / Operations (reuse unchanged)

| Scenario | File | What it tests | V2 reuse |
|---|---|---|---|
| `cp83-snapshot-expand` | `scenarios/cp83-snapshot-expand.yaml` | Snapshot + expand | Reuse unchanged |
| `cp85-expand-failover` | `scenarios/cp85-expand-failover.yaml` | Expand during failover | Reuse unchanged |
| `cp11a4-snapshot-export-import` | `scenarios/internal/cp11a4-snapshot-export-import.yaml` | Snapshot export/import | Reuse unchanged |
| `op-csi-lifecycle` | `scenarios/op-csi-lifecycle.yaml` | CSI full lifecycle | Reuse unchanged |
| `op-failure-injection` | `scenarios/op-failure-injection.yaml` | Operator failure injection | Reuse unchanged |
| `op-mini-soak` | `scenarios/op-mini-soak.yaml` | Mini soak test | Reuse unchanged |
| `lease-expiry-write-gate` | `scenarios/lease-expiry-write-gate.yaml` | Lease/write gate | Reuse unchanged |
| `consistency-epoch` | `scenarios/consistency-epoch.yaml` | Epoch consistency | Reuse unchanged |
| `consistency-lease` | `scenarios/consistency-lease.yaml` | Lease consistency | Reuse unchanged |

## V2 Integration Matrix

### Rebuild Integration

| ID | Stage | Capability | Validation | Proof tier | Scenario | Topology | Entry trigger | Workload | Failure | Expected path | Data validation | Status | File | Evidence |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `I-R1` | Rebuild | Tier 3 | `R1` | component | Fresh replica join | 2-node RF2 | Master assigns new replica | 50+ blocks pre-existing | None | syncAck → rebuild → converge | Block-by-block compare | Covered | `rebuild_matrix_gaps_test.go` | `TestRebuild_R1_SyncAckDrivenDecision` — TCP session control |
| `I-R2` | Rebuild | Tier 3 | `R2` | component | Large rebuild with live writes | 2-node RF2 | Manual rebuild trigger | 1GB + 5000 live writes | None | Two-line rebuild, flusher active | SHA-256 extent match | Covered | `rebuild_primary_initiated_test.go` | `TestRebuild_PrimaryInitiated_1GB_WithLiveWrites` |
| `I-R3` | Rebuild | Tier 3 | `R3` | component | Stale replica restart | 2-node RF2 | Replica restart with old data | 200+ blocks, WAL recycled | Replica crash + restart | syncAck → rebuild → converge | Block-by-block compare | Covered | `rebuild_matrix_gaps_test.go` | `TestRebuild_R3_StaleReplicaRestartBeyondWAL` |
| `I-R4` | Rebuild | Tier 3 | `R10`, `V8` | component | Failover rejoin | 2-node RF2 | Old primary restarts as replica | 200 initial + 500 post-failover | Primary kill | Role swap → rebuild → CRC match | SHA-256 extent match | Covered | `rebuild_failover_rejoin_test.go` | `TestRebuild_R10_FailoverRejoinRebuild` |
| `I-R5` | Rebuild | Tier 3 | `R5` | component | Mid-rebuild disconnect | 2-node RF2 | Connection drop at 50% | 100 blocks | TCP kill mid-transfer | Cancel → fresh rebuild → converge | Block-by-block compare | Covered | `rebuild_matrix_gaps_test.go` | `TestRebuild_R5_ConnectionDropMidBase` |
| `I-R6` | Rebuild | Tier 3 | `R11` | component | Divergent replica overwrite | 2-node RF2 | Replica has different data | 100 blocks divergent | None | Full overwrite → CRC match | SHA-256 extent match | Covered | `rebuild_r11_r12_test.go` | `TestRebuild_R11_DivergentReplicaFullOverwrite` |
| `I-R7` | Rebuild | Tier 3 | `R12` | component | Crash mid-rebuild restart | 2-node RF2 | Crash after 50% base + partial WAL | 50 blocks | Replica crash mid-session | Fresh session → converge | Block-by-block compare | Covered | `rebuild_r11_r12_test.go` | `TestRebuild_R12_CrashMidRebuild_FreshSessionConverges` |
| `I-R8` | Rebuild | Tier 3 | `R2`, `R10`, `V8` | hardware | Hardware rebuild via runner | 2-node m01/m02 | sw-test-runner suite | 1GB volume | Real hardware | Full rebuild → extent match | Extent compare on runner path | Needs V2 scenario | — | V1 scenario exists, needs V2 session control |

### Restore Integration

| ID | Stage | Capability | Validation | Proof tier | Scenario | Topology | Entry trigger | Workload | Failure | Expected path | Data validation | Status | File | Evidence |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `I-S1` | Restore | Tier 5 | `S5` | component | Snapshot-tail rebuild | 2-node RF2 | Snapshot at LSN N + WAL N+1..M | 100 base + 50 tail | None | Base + tail converge | Block-by-block compare | Covered | `restore_ready_test.go` | `TestRestore_S5_SnapshotTailRebuild` |
| `I-S2` | Restore | Tier 5 | `S7` | component | Crash between base and tail | 2-node RF2 | Crash after base, before tail complete | 40 blocks | Replica crash | Fresh rebuild → converge | Block-by-block compare | Covered | `restore_ready_test.go` | `TestRestore_S7_CrashBetweenBaseAndTail` |
| `I-S3` | Restore | Tier 5 | `S8` | component | Concurrent writes during restore | 2-node RF2 | Writes during snapshot base copy | 80 base + 30 live | None | Bitmap protects live writes | SHA-256 compare | Covered | `restore_ready_test.go` | `TestRestore_S8_SnapshotUnderConcurrentWrites` |
| `I-S4` | Restore | Tier 5 | `S1`-`S4` | hardware | Hardware snapshot export/import | 2-node m01/m02 | Runner scenario | Real volume | None | Export → import → verify | Extent compare | Covered (V1) | `cp11a4-snapshot-export-import.yaml` | V1 scenario |

### V2 Protocol Integration

| ID | Stage | Capability | Validation | Proof tier | Scenario | Topology | Entry trigger | Workload | Failure | Expected path | Data validation | Status | File | Evidence |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `I-V1` | V2 | Tier 2 | `V1` | hardware | Bootstrap to publish_healthy | 2-node m01/m02 | Create RF2 sync_all | 4K write + fsync | None | Bootstrap fence → publish_healthy | Mode = publish_healthy | Covered | `recovery-bootstrap-closure.yaml` | Stage 0 PASS |
| `I-V2` | V2 | Tier 2 | `V2` | hardware | Sustained write + barrier | 2-node m01/m02 | fio 10s + dd_write fsync | 10s randwrite + 2MB fsync | None | Barrier succeeds after bootstrap | dd checksum match | Covered | `recovery-baseline-failover.yaml` | Stage 1 32/33 |
| `I-V3` | V2 | Tier 3 | `V3` | hardware | Auto-failover | 2-node m01/m02 | Kill primary VS | Pre-write + failover | Primary kill | Master promotes replica | New primary serves I/O | Needs fix | `recovery-baseline-failover.yaml` | Stage 1 last failure: master didn't promote |
| `I-V4` | V2 | Tier 3 / Tier 6 | `V8` | integration | Failover rejoin full stack | 2-node m01/m02 | Old primary restarts | Post-failover writes | Primary kill + restart | Rejoin → rebuild → health surfaces correct | Projection + extent | Missing | — | Needs V8 integration scenario |
| `I-V5` | V2 | Tier 3 / Tier 8 | `V11` | integration | Long-haul write through recovery | 2-node m01/m02 | Sustained fio + fault mid-way | 30min+ workload | Replica kill + restart mid-run | Recovery → resume → final verify | SHA-256 extent match | Missing | — | V11 integration scenario |
| `I-V6` | V2 | Tier 6 | `V13` | integration | Observability coherence | 2-node m01/m02 | Recovery event | During rebuild/catchup | None | Logs + projection + debug agree | Surface comparison | Missing | — | V13 integration scenario |

### Chaos / Stability Integration (V1 reuse)

| ID | Stage | Capability | Validation | Proof tier | Scenario | Topology | Entry trigger | Workload | Failure | Expected path | Data validation | Status | File | Evidence |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `I-C1` | Chaos | Tier 8 | regression reuse | hardware | Repeated primary kills | 2-node m01/m02 | Kill loop | Sustained writes | 5+ kills | Each recovery converges | Data integrity | Covered (V1) | `cp85-chaos-primary-kill-loop.yaml` | V1 scenario |
| `I-C2` | Chaos | Tier 8 | regression reuse | hardware | Repeated replica kills | 2-node m01/m02 | Kill loop | Sustained writes | 5+ kills | Shipper degrades, recovers | Data integrity | Covered (V1) | `cp85-chaos-replica-kill-loop.yaml` | V1 scenario |
| `I-C3` | Chaos | Tier 8 | regression reuse | hardware | Network partition | 2-node m01/m02 | iptables partition | Sustained writes | 30s partition | Degraded → recover | Data integrity | Covered (V1) | `cp85-chaos-partition.yaml` | V1 scenario |
| `I-C4` | Chaos | Tier 8 | regression reuse | hardware | Disk full | 2-node m01/m02 | fallocate fill | Writes during fill | Disk full | Fail-closed, recover after space | No corruption | Covered (V1) | `cp85-chaos-disk-full.yaml` | V1 scenario |

### Performance Integration (V1 reuse)

| ID | Stage | Capability | Validation | Proof tier | Scenario | Topology | Entry trigger | Workload | Failure | Expected path | Data validation | Status | File | Evidence |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `I-P1` | Perf | Tier 8 | regression reuse | hardware | iSCSI baseline | 2-node m01/m02 | Benchmark run | fio sweep | None | IOPS/latency recorded | Baseline comparison | Covered (V1) | `cp85-perf-baseline.yaml` | V1 scenario |
| `I-P2` | Perf | Tier 8 | regression reuse | hardware | NVMe-oF baseline | 2-node m01/m02 | Benchmark run | fio sweep | None | IOPS/latency recorded | Baseline comparison | Covered (V1) | `cp103-perf-baseline.yaml` | V1 scenario |
| `I-P3` | Perf | Tier 8 | regression reuse | hardware | 4-hour soak | 2-node m01/m02 | Long-running write | 4h sustained | None | No degradation | Stable metrics | Covered (V1) | `cp84-soak-4h.yaml` | V1 scenario |

## Summary

| Stage | Total | Covered | Needs V2 scenario | Missing |
|---|---|---|---|---|
| Rebuild | 8 | 7 (component) | 1 (I-R8: hardware) | 0 |
| Restore | 4 | 4 | 0 | 0 |
| V2 Protocol | 6 | 2 (hardware) | 1 (I-V3: fix) | 3 (I-V4, I-V5, I-V6) |
| Chaos | 4 | 4 (V1 reuse) | 0 | 0 |
| Performance | 3 | 3 (V1 reuse) | 0 | 0 |
| **Total** | **25** | **20** | **2** | **3** |

## V1 Scenarios That Need V2 Adaptation

These V1 scenarios exist and work but need modification for V2:

1. `ha-rebuild.yaml` → replace V1 direct rebuild with V2 session-controlled path
2. `ha-failover-during-rebuild.yaml` → adapt for V2 rebuild session lifecycle
3. `robust-shipper-lifecycle.yaml` → replace with V2 session lifecycle test
4. `robust-shipper-reconnect.yaml` → replace with V2 sync/recovery path
5. `cp85-role-flap.yaml` → add V2 projection assertions

## Next Steps

1. Fix `I-V3` (auto-failover) — the Stage 1 last remaining failure
2. Create `I-V4` (failover rejoin full stack) — the V8 integration scenario
3. Adapt `ha-rebuild.yaml` for V2 session control
4. Run V1 chaos/stability scenarios on V2 binary to verify no regressions
