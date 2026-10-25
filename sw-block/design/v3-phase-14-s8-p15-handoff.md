# V3 Phase 14 S8 — P15 Handoff

Date: 2026-04-20
Status: draft (S8 P15 handoff, round-2 aligned to canonical P15 plan)
Purpose: map every P14-residual product gap to the canonical P15 track in `v3-phase-15-product-plan.md`, so P15 execution starts from the correct ownership map

## 1. Handoff Contract

Per `v3-phase-14-s8-assignment.md` §3.F: every gap that P14 does NOT close must appear here with four columns — the gap, why it is not P14, which canonical P15 track owns it, and the minimum first proof gate.

**Canonical P15 track numbering** (from `v3-phase-15-product-plan.md` §4):

1. `T1` Frontend + Data Path Contract
2. `T2` CSI / External Lifecycle Surface
3. `T3` External Control API
4. `T4` Security And Auth Posture
5. `T5` Diagnostics And Explainability
6. `T6` Operator Workflow
7. `T7` V2/V3 Coexistence And Migration
8. `T8` Deployment / Upgrade / Release Hardening
9. `Final Gate` Cluster Validation Agent

S8 does NOT invent new tracks, renumber existing ones, or prescribe P15 implementation — only the acceptance gate each P15 track must pass for the handed-off gap.

## 2. Handoff Table

| # | Gap | Why not P14 | P15 owner track | Required first proof |
|---|---|---|---|---|
| 1 | **Frontend data path** (attach → real read → real write → stale-primary fence at frontend boundary) | P14 stops at adapter/engine projection; there is no user-data I/O path in P14, and `adapter.PublishHealthy` is a control-plane signal, not a data-path handshake. | **P15 T1** Frontend + Data Path | L2: V3 frontend backend attaches to an internal volume, writes data, reads it back, triggers P14 reassignment/failover, and verifies the stale primary can no longer serve writes. Covered by T1 §5 Acceptance. |
| 2 | **Subprocess heartbeat ingress** — the L2 subprocess smoke (`TestS7Process_RealSubprocessRestartSmoke`) drives mint via `StaticDirective`, not a real heartbeat wire. The full `heartbeat → observation → controller` path is L1-only in the subprocess. | Real heartbeat ingress is a frontend/transport surface. P14 has no intention of adding one. | **P15 T1** Frontend + Data Path (observation ingress is part of the frontend/transport contract) | L2: subprocess accepts heartbeats over a real transport; observation store mutates; controller sees the change; adapter advances — end-to-end in a real process. |
| 3 | **CSI driver lifecycle** (CreateVolume / DeleteVolume / ControllerPublish / NodeStage / NodePublish / NodeUnpublish / ControllerUnpublish) | P14 has no orchestrator-facing lifecycle surface; CSI sits above the P14 authority/adapter route. | **P15 T2** CSI / External Lifecycle Surface | L2: CSI sidecar ↔ sparrow process, full Create → Publish → use → Unpublish → Delete cycle via real CSI gRPC against V3 authority (T2 §5 Acceptance). |
| 4 | **External control API** (REST / gRPC verbs for list / inspect / create / delete / resize / pause / resume) | P14 exposes hidden test flags on `sparrow` only. An external API is a net-new surface. | **P15 T3** External Control API | L2: OpenAPI / gRPC verbs backed by V3 authority state, exercised by an external client that does NOT construct `AssignmentInfo` directly. |
| 5 | **Security / auth posture** (API authn/authz, CHAP for iSCSI, TLS/mTLS where applicable, audit trail) | P14 has no external authn surface. | **P15 T4** Security And Auth | L1+L2: CHAP positive+negative for any iSCSI target shipped; API authn positive+negative; tenant-scoped listing; audit log for mutating verbs. |
| 6 | **Operator diagnostics surface** (health dashboard, structured readout of `LastUnsupported` / `LastConvergenceStuck` / `ReloadSkips`, alerts) | P14 records internal evidence (per-volume maps + structured JSON pass-line in S7 subprocess smoke) but does NOT expose it to operators. | **P15 T5** Diagnostics And Explainability | L2: structured `/healthz` + `/metrics` surfacing P14 internal states; L3 `cp85-metrics-verify.yaml` scenario class reshaped for V3. |
| 7 | **Operator workflow** (planned failover, graceful drain, supervised reassign, supervised rebuild) | P14 rejects V2 `promote/demote/HandleAssignment`. Operator-initiated actions MUST route through the publisher mint path as ordinary `AssignmentAsk`s. A new workflow surface is required. | **P15 T6** Operator Workflow | L2: operator-triggered reassign lands in `TopologyController` as an `AssignmentAsk`; publisher mints new epoch; adapter converges. Manual promote is explicitly reshaped from V2 semantics. |
| 8 | **V2 ↔ V3 migration / coexistence** (online migrate a V2 volume to V3 authority; rollback; mixed V2+V3 cluster) | Migration is a product-level transition requiring both T1 and T6. P14 is V3-only. | **P15 T7** V2/V3 Coexistence And Migration | L3 + L4: `op-upgrade-rollback.yaml` scenario class on a mixed cluster; explicit no-split-brain check under migration. |
| 9 | **Deployment / hardening** (systemd / container packaging, lockfile+storedir pathing, crash policy, log rotation, store backup) | `sparrow` is currently a test binary with hidden smoke flags; no deployment story. | **P15 T8** Deployment / Upgrade / Release Hardening | L2: deployable package starts `sparrow` with the durable store correctly mounted; crash-restart cycle preserves the P14 restart truth. L4: release-hardening soak (`cp84-soak-4h.yaml`, `cp85-soak-24h.yaml`). |
| 10 | **Final cluster validation** (end-to-end: operator creates volume → data written → primary killed → failover → operator reads data back → no loss, on real hardware) | P14 covers the control plane only; full cluster validation needs T1 + T2 + T3 + T4 + T5 + T6 all landed. | **P15 Final Gate** Cluster Validation Agent | Composite L3/L4 pack: `smoke-block-api` + `ha-restart-recovery` + `ha-failover` + `ha-io-continuity` + `fault-partition` in one run on real hardware, with evidence artifacts in `learn/test/`. |
| 11 | **Adapter fence-watchdog quantitative timeout bound** (watchdog fires at exact fence deadline + structured watchdog event for fence lineage) | S7 L1 covers "Fence command reaches executor via bridge + withheld callback does not produce Healthy"; the quantitative bound is adapter-package-owned and depends on the currently-uncommitted fence-watchdog branch. S7 sketch §10.1 Path B forbids S7 from reconfiguring the fence path. | **P14 internal follow-up** (NOT a P15 track) | Single adapter-package test replacing the PARTIAL row for evidence-matrix Claim 15. Lands when the fence-watchdog branch commits. Not an S8 gate. |

## 3. Out-Of-Scope (NOT Handed Off)

These are explicit non-goals for both P14 and current P15 bounds — not failures, not deferrals, permanent exclusions.

| Non-goal | Reason |
|---|---|
| Multi-master / leader election / distributed authority store | `v3-phase-14-s8-final-bounded-close.md` §3.6 and `v3-phase-15-product-plan.md` §2 both reject. Single-active-master is the accepted topology. Any multi-master claim would require a separate phase with a distributed-authority institution. |
| V2 `HandleAssignment` / `promote` / `demote` authority-owning semantic | Rejected at S2, reaffirmed S3-S8. Not ported under any P15 track. Operator promote surface in T6 is reshaped, not ported. |
| V2 heartbeat-as-authority | Rejected at S4. Heartbeats are observation inputs only under any P14 or P15 surface. |

## 4. Handoff Integrity Check

The table splits into two distinct row classes, each with its own integrity rule:

**Rows 1-10 — P15 product-surface gaps.** Each of these must satisfy all four checks as of 2026-04-20:

1. **Gap is not proven at any P14 level.** Cross-reference `v3-phase-14-s8-evidence-matrix.md` §3 — none of rows 1-10 appear as PROVEN or PARTIAL rows of the internal matrix. These are genuinely product-surface gaps P14 never attempted.
2. **Gap is assigned to a canonical P15 track.** Numbering matches `v3-phase-15-product-plan.md` §4.
3. **First-proof gate is concrete**, not a hand-wave. Every row names a specific test layer and a specific scenario or surface shape.
4. **Gap does not silently depend on a P14 claim being wider than is actually proven.** Example: P15 T1 is allowed to depend on P14 Claim 10 (restart re-anchors via VolumeBridge, PROVEN at L1) but NOT on any data-path claim — P14 makes none.

**Row 11 — P14 internal follow-up (exception to rule #1).** Row 11 is NOT a P15 gap; it is a targeted P14 internal completion that lands when the fence-watchdog branch commits. Its integrity rules are narrower:

1. **Row 11 MUST correspond to an existing PARTIAL or subclaim-only row of the internal matrix** — the whole point of the row is to name the follow-up that closes that partial. In this case row 11 corresponds to evidence-matrix **Claim 15 PARTIAL** (fence quantitative timeout bound). This is the inverse of rule #1 above: row 11 is listed here precisely BECAUSE it appears as PARTIAL in the matrix.
2. **Row 11 MUST NOT be assigned to any P15 track** — it is explicitly labeled "P14 internal follow-up (NOT a P15 track)" in the gap table.
3. **Row 11's first-proof gate MUST be a single-package test that closes the matrix PARTIAL row** — not a multi-track composite.

Splitting the integrity check this way keeps rule #1 for P15 gaps strict (any cross-reference to PROVEN/PARTIAL in the matrix is a bug there) while making row 11's purpose explicit and auditable (every P14 internal follow-up row should have a matrix PARTIAL anchor).

## 5. Per-Track Dependency Summary

| P15 Track | Depends on P14 claims | Notes |
|---|---|---|
| T1 Frontend + Data Path | Claims 3, 6, 10, 14 (durable reload, controller-driven, restart re-anchor, placement/rebalance) | Core product dependency. |
| T2 CSI / Lifecycle | T1 + Claims 6, 10 | CSI needs a working data path. |
| T3 External API | Claims 6, 14 | API translates external verbs to `AssignmentAsk`s. |
| T4 Security | T1 + T3 | Authn/authz sits above the surfaces T1 and T3 expose. |
| T5 Diagnostics | Claims 2, 8, 11, 12 (supportability / stuck / unsupported evidence) | Surfaces what P14 already records internally. |
| T6 Operator Workflow | Claims 6, 9, 14 (controller-driven, supersede, placement) | Operator actions are `AssignmentAsk`s through the controller. |
| T7 Migration | T1 + T6 | Needs both frontend and operator surfaces. |
| T8 Deployment | Claim 3 (durable reload) + T1 | Depends on durable store shape and frontend. |
| Final Gate | All P14 claims + all P15 tracks | Composite. |

## 6. Closure

Every product gap that S8 identifies is listed here with a canonical P15 track or explicit P14 follow-up. Track numbering matches `v3-phase-15-product-plan.md`. P15 execution starts from this table without needing to re-derive the ownership map.
