# V3 Phase 14 S8 Closure

Date: 2026-04-20
Status: draft — pending 14A final pass + architect + tester sign-off
Purpose: the final P14 closure statement per `v3-phase-14-s8-final-bounded-close.md` §10

## 1. What P14 Now Owns

After S4-S7, P14 closes **one bounded single-active-master internal topology/control-plane truth loop** for the accepted topology:

1. **Observation institution (S4)**: heartbeats and inventory flow through `ObservationHost` into a synthesized `ClusterSnapshot` + `SupportabilityReport`. Observation never mints authority. Partial / conflicting / duplicate / unknown / expired inventory becomes explicit `VolumeUnsupportedEvidence`; pending state is distinct from unsupported; per-volume isolation holds.

2. **Durable authority institution (S5)**: current authority line is durable, single-owner, atomically written, and reloaded synchronously at publisher boot. One record per volume. Corrupt records per-volume-fail-closed and surface as structured `ReloadSkips`. Process lock is exclusive and idempotent. Store never mints authority (boundary-guard test).

3. **Convergence institution (S6)**: desired assignments have bounded fate — confirmed (cleared), publish-not-observed (stuck-evidence, no re-mint, no churn), or superseded (by newer publisher line or different decision). Passive retry only; no active re-drive. Normal observation lag does NOT supersede. Per-volume retry clock; no global state.

4. **Real-route restart closure (S7)**: the full `ObservationHost → TopologyController → Publisher(reloaded) → VolumeBridge → VolumeReplicaAdapter` composes correctly at restart. Restart recomputes desired from durable authority + fresh observation (no durable desired store). Old-slot per-replica state is NOT revived from the store (S5 one-record-per-volume rule); live-route old-slot delivery via bridge is rejected by the adapter's monotonic guard. Stale observation cannot move authority backward (4 simulation shapes). Unsupported topology after restart records evidence, not silent idle. L2 process smoke proves real-binary restart × 2 preserves durable truth with no backward mint, via pinned structured JSON.

5. **Accepted topology** (S8 §4): single active master; multiple volumes; three replica slots per volume on distinct servers; one primary + two candidates; publisher-owned `Epoch` / `EndpointVersion`; durable current line; passive convergence; real `VolumeBridge` into adapter.

## 2. What P14 Explicitly Does NOT Own

1. CSI lifecycle (Create/Delete/Publish/Node*).
2. iSCSI / NVMe-oF frontend data path.
3. External volume management API (REST / gRPC).
4. Security / auth (authn, CHAP, encryption).
5. Operator diagnostics surface (dashboards, alerts, `/metrics`).
6. Operator workflow (drain, planned failover, manual reassign, supervised rebuild).
7. V2 ↔ V3 migration and coexistence.
8. Deployment / packaging / hardening (systemd, container, backup of durable store).
9. Performance / soak / release-hardening claims.
10. Multi-master / leader election / distributed authority (explicit non-goal, not just deferred).
11. V2 `HandleAssignment` / `promote` / `demote` semantics (explicit non-goal).
12. Heartbeat-as-authority (explicit non-goal).

Every item 1–9 is mapped to a P15 track in `v3-phase-14-s8-p15-handoff.md` §2. Items 10–12 are permanent non-goals.

## 3. Evidence At L0 / L1 / L2 / L3

Full matrix: `v3-phase-14-s8-evidence-matrix.md` §3. Summary of coverage:

| Claim | L0 | L1 | L2 | L3 |
|---|---|---|---|---|
| 1. Observation system-fed | ✓ | ✓ | — (via L1) | L3 shape only |
| 2. Supportability explicit | ✓ | ✓ | — | L3 shape only |
| 3. Durable authority / reload | ✓ (13 tests) | ✓ | ✓ (subprocess) | L3 shape only |
| 4. No old-slot durable revival | ✓ | ✓ | ✓ | L3 shape only |
| 5. Live-route old-slot rejected | ✓ | ✓ | N/A (live-route) | N/A |
| 6. Controller-driven bind/reassign/refresh | ✓ (7 tests) | ✓ | ✓ (Bind only — Reassign / RefreshEndpoint are L1-only; subprocess does not ingest heartbeats) | L3 shape only |
| 7. Confirmation clears desired | ✓ | ✓ | N/A | L3 shape only |
| 8. Stuck evidence bounded / no churn | ✓ (5 tests) | ✓ | N/A | L3 shape only |
| 9. Supersede (2 modes) | ✓ (3 tests) | ✓ | N/A | L3 shape only |
| 10. Restart re-anchors via VolumeBridge | ✓ | ✓ | **subclaim only** — L2 proves durable `Publisher` reload (Claim 3); the bridge/adapter re-anchor is NOT at L2 because `sparrow` subprocess does not construct `ObservationHost` / controller / bridge / adapter | L3 shape only |
| 11. Stale observation cannot go backward | ✓ | ✓ (4 sub-cases) | — | L3 shape only |
| 12. Unsupported → evidence | ✓ | ✓ | **not at L2** — `ReloadSkips` is S5 durable-corruption evidence, not observation-layer unsupported-topology. Controller `LastUnsupported` is L1 | L3 shape only |
| 13. Per-volume isolation | ✓ (4 tests) | ✓ | N/A | L3 shape only |
| 14. Placement / rebalance | ✓ (5 tests) | ✓ | N/A | L3 shape only |
| 15. Fence route bounded | ✓ (adapter) | PARTIAL | N/A | L3 shape only |
| 16. Boundary guards | ✓ | N/A (structural) | N/A | N/A |

Sixteen claims; fifteen PROVEN at their target level; one (Claim 15) PARTIAL — adapter-package follow-up per §5 below. L3 is classification-only at P14 (`v3-phase-14-s8-v2-scenario-classification.md`) — L3 entries are scenario SHAPES, not executed evidence. Runnable L3 is P15 Final Gate (Cluster Validation Agent). Claims 6, 10, and 12 have NARROWED L2 cells where the subprocess binary cannot carry the full claim surface; each is handed off to the correct canonical P15 track.

## 4. Test Baseline (2026-04-20)

```
$ go test ./core/engine ./core/adapter ./core/authority ./cmd/sparrow -count=1
ok  github.com/seaweedfs/seaweed-block/core/engine     0.016s
ok  github.com/seaweedfs/seaweed-block/core/adapter    2.344s
ok  github.com/seaweedfs/seaweed-block/core/authority  4.569s
ok  github.com/seaweedfs/seaweed-block/cmd/sparrow     2.264s

$ go test ./... -count=1
(14 packages, all PASS)
```

193 Go tests across the P14 scope. L2 subprocess smoke (`TestS7Process_RealSubprocessRestartSmoke`) spawns a real `sparrow` binary twice on the same store directory and asserts the pinned JSON pass-line schema.

## 5. Residual Risks

| # | Risk | Scope / carry-forward |
|---|---|---|
| 1 | Windows `os.RemoveAll` on `t.TempDir()` can race with lock-file release during teardown. No correctness impact. | S7 sketch §8.5 — logged as `t.Logf`, not a failure. |
| 2 | Claim 15 PARTIAL: fence quantitative timeout-bound is adapter-package-owned, depends on the uncommitted fence-watchdog branch committing. | `v3-phase-14-s8-p15-handoff.md` item 11 — P14 internal follow-up, NOT P15. Single adapter test replaces PARTIAL status. |
| 3 | L2 subprocess drives mint via `StaticDirective`; it does NOT construct `ObservationHost` / controller / bridge / adapter. Full heartbeat-ingress and real-binary adapter route are L1-only in S8. | **P15 T1** Frontend + Data Path (handoff items #1 and #2). |

**No S8-blocking correctness residuals.** One adapter-owned quantitative fence proof (Claim 15) remains as P14-internal follow-up per row 11 of the handoff table — not a P15 track, not an S8 gate. Zero backward-authority-mint residuals. Zero silent-idle residuals.

## 6. P15 Track Owners

Handoff complete per `v3-phase-14-s8-p15-handoff.md`, aligned to canonical `v3-phase-15-product-plan.md` §4. Every P14 residual product gap has a named P15 track and a concrete first-proof gate:

- T1 Frontend + Data Path Contract (includes subprocess heartbeat ingress)
- T2 CSI / External Lifecycle Surface
- T3 External Control API
- T4 Security And Auth Posture
- T5 Diagnostics And Explainability
- T6 Operator Workflow
- T7 V2/V3 Coexistence And Migration
- T8 Deployment / Upgrade / Release Hardening
- Final Gate — Cluster Validation Agent

## 7. Final P14 Claim

> **P14 has closed one bounded single-active-master internal topology/control-plane truth loop for the accepted topology set.** Observation is system-fed; authority is durable, single-owner, and restart-recovered; convergence has bounded fate; the real `VolumeBridge → adapter` route re-anchors across restart without backward mint or old-slot revival.
>
> **P14 has NOT closed CSI, external API, frontend data path, migration, security, deployment, performance, or production readiness.** Those nine tracks are handed off to P15 with concrete first-proof gates. Multi-master, V2 `HandleAssignment/promote/demote` semantics, and heartbeat-as-authority are explicit non-goals — not deferrals.

## 8. Sign-off State

- **Architect** — pending review of this matrix + handoff.
- **Tester** — pending reproducibility check (commands in §4).
- **14A final targeted pass** — pending (scope in `v3-phase-14a-checklist.md`).

S8 cannot be accepted until all three sign off. After that, P14 is closed and the next active work is P15 T1 Frontend.
