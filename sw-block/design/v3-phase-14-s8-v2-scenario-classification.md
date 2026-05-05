# V3 Phase 14 S8 — V2 Scenario Port Classification

Date: 2026-04-20
Status: draft (S8 scenario classification)
Purpose: classify every V2 testrunner scenario (`weed/storage/blockvol/testrunner/scenarios/`) as P14-runnable / P15-blocked / deferred, producing the table required by `v3-phase-14-s8-assignment.md` §3.C

## 1. Classification Vocabulary

| Class | Meaning | P14 S8 action |
|---|---|---|
| **RUNNABLE-P14** | Shape maps onto S4-S7 internal surfaces. Runnable with V3 binary + no external frontend. | Keep scenario YAML shape; adapt actions to V3 route; run as L3 classification only (S8 does not need to execute L3 itself). |
| **BLOCKED-FRONTEND** | Requires iSCSI / NVMe / CSI / real data path. P15 Frontend track gate. | Preserve scenario reference; hand off to P15 with required precondition. |
| **BLOCKED-OPS** | Requires operator CLI / HTTP / admin workflow. P15 Ops track gate. | Hand off to P15 Ops. |
| **BLOCKED-HA** | Requires multi-master / leader election / distributed authority. Outside P14/P15 bounded claim. | Mark as out-of-scope for both P14 and current P15 bounds. Document only. |
| **BLOCKED-PERF** | Benchmark / soak / perf-baseline; not a correctness gate. | Defer to release-hardening track; document only. |
| **PORT-MECHANISM** | Testrunner machinery itself (not a scenario): action vocabulary, artifact collection, scenario YAML shape. | Port machinery without V2 authority semantics (see `v3-phase-14-s6-s8-v2-port-plan.md` §5). |

## 2. Testrunner Machinery (port decision)

`weed/storage/blockvol/testrunner/` — YAML-driven runner with action vocabulary, artifact collection, report generation. V3 acceptance pack (P14 S8 carries scenario SHAPES forward; actual V3 testrunner integration is a P15 Cluster Validation track deliverable).

| Component | Classification | S8 action |
|---|---|---|
| `testrunner/*.go` (engine / parser / registry / reporter / metrics) | PORT-MECHANISM | Port to V3 when V3 has a stable CLI surface. S8 does not run the runner directly. |
| `testrunner/actions/*.go` (37 registered actions) | PORT-MECHANISM | Port per-action as V3 surfaces appear. S8 ports NONE directly (no V3 CLI surface exists yet outside `sparrow` hidden smoke flags). |
| `testrunner/scenarios/*.yaml` | scenario-by-scenario below | — |

**S8 decision:** do not port testrunner machinery into the V3 tree as part of S8. The scenario SHAPES below are what S8 classifies; the runner itself is a P15 Cluster Validation deliverable.

## 3. Public Scenarios Table

Path: `weed/storage/blockvol/testrunner/scenarios/public/`.

| # | Scenario | P14 route it touches | Class | Notes |
|---|---|---|---|---|
| 1 | `smoke-block-api.yaml` | block create / write / read / verify | **BLOCKED-FRONTEND** | Requires V3 block API surface + real data path. No V3 equivalent exists. P15 Frontend track. |
| 2 | `smoke-iscsi.yaml` | kernel iSCSI sanity | **BLOCKED-FRONTEND** | Requires iSCSI target + kernel initiator. P15 Frontend. |
| 3 | `smoke-kv.yaml` | KV layer smoke | **BLOCKED-FRONTEND** | KV path is out of P14 scope. P15 Frontend. |
| 4 | `e2e-block.yaml` / `e2e-block-auto.yaml` | end-to-end block I/O with V3 backend | **BLOCKED-FRONTEND** | Same preconditions as #1. |
| 5 | `e2e-kv.yaml` / `e2e-kv-auto.yaml` / `e2e-combined-auto.yaml` | KV + block combined | **BLOCKED-FRONTEND** | Same as #3. |
| 6 | `ha-restart-recovery.yaml` | restart-and-reload closed loop | **RUNNABLE-P14** | Shape maps directly onto S7 restart route. L1 / L2 subprocess smoke already proves the route in-process; scenario YAML would just drive the same route under scenario harness. Keep shape; port when V3 testrunner lands. Covered by Claim 3 / Claim 10 of the evidence matrix at L0/L1/L2. |
| 7 | `ha-failover.yaml` | failover under live I/O | **BLOCKED-FRONTEND** | Shape touches S4-S7 (observation → controller → reassign → adapter), but "under live I/O" requires the data path. Classification: control-plane portions are RUNNABLE-P14 (covered by Claim 14 at L1); I/O-continuity portions are BLOCKED-FRONTEND. Split at P15 port time. |
| 8 | `ha-full-lifecycle.yaml` | bind → heal → failover → rebuild → restart full cycle | **BLOCKED-FRONTEND** | Contains rebuild + I/O. Rebuild is P14-internal but has no V3 equivalent outside V2 bridge; I/O is frontend. Defer whole scenario to P15. |
| 9 | `ha-io-continuity.yaml` | zero data loss across failover | **BLOCKED-FRONTEND** | Entirely data-path. P15 Frontend. |
| 10 | `ha-rebuild.yaml` | full-extent rebuild via transport | **BLOCKED-FRONTEND** | Rebuild transport is V2-side; V3 adapter has no rebuild surface in current slice. Deferred. |
| 11 | `crash-recovery.yaml` | process kill + restart + verify | **RUNNABLE-P14** | Control-plane portion identical to #6. Data-verify portion is BLOCKED-FRONTEND. Split at port time. |
| 12 | `diag-restart-recovery.yaml` | diagnostics on restart | **RUNNABLE-P14** (control-plane subset) | S7 subprocess smoke emits structured JSON (`Bootstrap.ReloadedRecords`, `ReloadSkips`, no-backward-mint); maps onto this shape. Operator dashboards / full diag bundle is P15 Diagnostics. |
| 13 | `fault-partition.yaml` | network partition + recovery | **RUNNABLE-P14** (control-plane) | Control-plane partition → stale observation / convergence stuck is covered by Claims 8, 11 at L1. Real netem + data-path partition is BLOCKED-FRONTEND. |
| 14 | `fault-netem.yaml` | generic network fault injection | **BLOCKED-FRONTEND** | Needs data path under load. P15. |
| 15 | `fault-disk-full.yaml` | ENOSPC on primary | **BLOCKED-FRONTEND** | Needs write path. P15. |
| 16 | `consistency-epoch.yaml` | epoch monotonicity across failover | **RUNNABLE-P14** | Pure control-plane claim — already covered at L0 by `TestDurableAuthority_PublisherAdvancesFromReloaded` and at L1 by `TestS7_*` restart tests. L3 version would add cross-process multi-primary validation. |
| 17 | `consistency-lease.yaml` | lease-based guard | **BLOCKED-OPS** | Lease semantics are V2-specific; V3 uses publisher-owned Epoch. Would need a reshaped V3 scenario. Defer. |
| 18 | `lease-expiry-write-gate.yaml` | write blocked on lease expiry | **BLOCKED-FRONTEND** | Write path + lease. P15. |
| 19 | `lease-renewal-under-io.yaml` | lease renewal during I/O | **BLOCKED-FRONTEND** | Same. |
| 20 | `cp11b3-auto-failover.yaml` | automatic failover trigger | **RUNNABLE-P14** (control-plane) | Control-plane covered at L1 by `TestTopologyControllerToPublisher_E2E_MultiVolumePlacementAndFailover`. |
| 21 | `cp11b3-manual-promote.yaml` | operator-initiated promote | **BLOCKED-OPS** | Requires operator CLI + admin API. P15 Ops. Also — manual promote is V2 semantics; V3 does not have a direct equivalent by design (S6-S8 V2 port plan §2 rejects V2 `promote` ownership). |
| 22 | `cp11b3-fast-reconnect.yaml` | reconnect skips unnecessary failover | **RUNNABLE-P14** (control-plane) | S6 normal-lag handling covers this — `TestConvergence_NormalLag_OldObservationDoesNotSupersede`. Full scenario requires real reconnect transport = BLOCKED-FRONTEND for the transport layer, RUNNABLE-P14 for the control-plane. |

**Public scenarios summary:**
- RUNNABLE-P14: 6 scenarios (ha-restart-recovery, crash-recovery control plane, diag-restart-recovery, fault-partition control plane, consistency-epoch, cp11b3-auto-failover control plane) — all covered by Claims 3/10/11/12/14 at L0/L1/L2.
- BLOCKED-FRONTEND: 13 scenarios
- BLOCKED-OPS: 2 scenarios (lease-consistency, manual-promote)
- Mixed (split required at port time): ha-failover, cp11b3-fast-reconnect

## 4. Internal Scenarios Table (selected, by category)

Path: `weed/storage/blockvol/testrunner/scenarios/internal/`. 50+ files; classifying by category rather than per-file.

| Category | Example files | Class | Notes |
|---|---|---|---|
| Recovery baselines | `recovery-baseline-restart.yaml` / `recovery-baseline-failover.yaml` / `recovery-baseline-partition.yaml` / `recovery-baseline-rebuild.yaml` | RUNNABLE-P14 (control-plane) | Restart/failover/partition portions map onto S4-S7; rebuild portions need V3 rebuild surface (not in S8). |
| Coordination dev-cycle | `coord-dev-cycle.yaml` / `coord-ha-failover.yaml` / `coord-smoke-iscsi.yaml` | BLOCKED-FRONTEND | iSCSI / end-to-end workflows. |
| CP103 performance matrix | `cp103-*.yaml` | BLOCKED-PERF | Performance, not correctness. Defer to release hardening. |
| CP85 chaos | `cp85-chaos-partition.yaml` / `cp85-chaos-primary-kill-loop.yaml` / `cp85-chaos-replica-kill-loop.yaml` / `cp85-role-flap.yaml` / `cp85-session-storm.yaml` | Mixed | Control-plane portions RUNNABLE-P14 as property tests (kill-loop restart, role-flap convergence). I/O portions BLOCKED-FRONTEND. |
| CP85 metrics / observability | `cp85-metrics-verify.yaml` | BLOCKED-OPS | Operator metrics pipeline. P15 Diagnostics. |
| CP85 soak | `cp85-soak-24h.yaml` / `cp84-soak-4h.yaml` | BLOCKED-PERF | Long-running stability. Release hardening. |
| CP85 database / filesystem | `cp85-db-ext4-fsck.yaml` / `cp85-db-sqlite-crash.yaml` / `cp85-expand-failover.yaml` | BLOCKED-FRONTEND | Real filesystem + DB workload. |
| Snapshot | `cp11a4-snapshot-export-import.yaml` / `cp83-snapshot-expand.yaml` / `cp85-snapshot-stress.yaml` | BLOCKED-FRONTEND | Snapshot API is P15 Frontend / Ops. |
| EC / Erasure | `ec3-*.yaml` / `ec5-*.yaml` | BLOCKED-FRONTEND | Data path erasure. |
| HA extensions | `ha-failover-during-rebuild.yaml` / `ha-multi-client-failover.yaml` | BLOCKED-FRONTEND | Multi-client data path. |
| Benchmark | `benchmark-*.yaml` / `bench-validated.yaml` / `baseline-full-roce.yaml` / `fsync-only-test.yaml` | BLOCKED-PERF | Performance. |
| DM / stripe | `dm-stripe-two-server.yaml` | BLOCKED-FRONTEND | Device mapper. |
| Operator lifecycle | `op-upgrade-rollback.yaml` / `op-csi-lifecycle.yaml` / `op-failure-injection.yaml` | BLOCKED-OPS | P15 Ops + Migration. |
| Real-workload validation | `cp13-8-real-workload-validation.yaml` | BLOCKED-FRONTEND + BLOCKED-PERF | Full stack. |

**Internal scenarios summary:**
- Directly useful now as P14-internal control-plane L3 shape: ~8 scenarios (recovery-baseline × 3, cp85 chaos × 3, consistency-epoch at public, diag-restart-recovery at public). All already covered at L0/L1/L2 by the evidence matrix; L3 runnable is P15 Cluster Validation.
- BLOCKED-FRONTEND majority: ~30 scenarios tied to I/O / iSCSI / NVMe / snapshot / DB workloads.
- BLOCKED-OPS: ~8 scenarios tied to operator workflows.
- BLOCKED-PERF: ~10 scenarios tied to perf / soak.
- BLOCKED-HA: none of the current set explicitly requires multi-master, but `cp85-role-flap` and any future "multi-master" test would be BLOCKED-HA.

## 5. Port Shape (V3)

When V3 eventually ships a testrunner integration (P15 Cluster Validation), the port order should be:

1. **RUNNABLE-P14 scenarios first** — port scenario YAML shape (not V2 actions) against V3 `sparrow` binary + testrunner-wrapped smoke flags. Keep the structured-JSON stdout shape from S7 smoke; wrap it in testrunner action vocabulary.
2. **Mixed scenarios second** — split each into (control-plane sub-scenario, data-path sub-scenario). Port the control-plane half first.
3. **BLOCKED-FRONTEND last** — arrives when P15 Frontend ships iSCSI / NVMe / CSI.
4. **BLOCKED-OPS as needed** — arrives with P15 Ops admin surface.
5. **BLOCKED-PERF and BLOCKED-HA** — release-hardening and explicit non-goals respectively.

## 6. Closure

Per S8 assignment §3.C, this table is sufficient for S8. Actual scenario PORTING is P15 Cluster Validation work and is not an S8 deliverable. S8 produces the classification + the residual map; P15 executes.

Evidence that the RUNNABLE-P14 scenarios' underlying claims are proven today is in `v3-phase-14-s8-evidence-matrix.md` §3 — each RUNNABLE-P14 scenario above has at least one PROVEN row backing it at L0/L1/L2.
