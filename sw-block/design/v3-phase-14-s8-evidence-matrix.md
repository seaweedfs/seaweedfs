# V3 Phase 14 S8 Evidence Matrix

Date: 2026-04-20
Status: draft (S8 evidence consolidation)
Purpose: map every internal P14 claim from `v3-phase-14-s8-final-bounded-close.md` §5 to concrete tests and commands, classify L0/L1/L2/L3 coverage, and surface residual gaps for 14A / P15 handoff

## 1. How To Read This Matrix

Each row is one internal P14 claim. Columns:

- **L0 Unit** — package-local invariant / policy test that proves the claim inside one package.
- **L1 Component** — in-process multi-package route test (real `ObservationHost` + `TopologyController` + `Publisher` + `VolumeBridge` + `VolumeReplicaAdapter`, no shell, no kernel).
- **L2 Process** — real `sparrow` binary or in-process `Bootstrap()` against a real filesystem store.
- **L3 Scenario** — hardware / YAML-driven scenario. For P14 S8, L3 is **classification-only**: the rows below say "L3 shape only, not executed" where a V2 YAML scenario describes the same claim shape. L3 entries are NOT executed evidence; they are the scenario shapes that P15 Cluster Validation (Final Gate) will run. A claim with an L3 shape listed is NOT proven at L3 by S8.
- **Status** — `PROVEN` (explicit test); `PARTIAL` (route covered but some sub-claim deferred); `DEFERRED` (intentionally pushed to later slice / P15); `N/A` (not applicable at that level).
- **Residual** — what is NOT proven and where it is carried.

All cited L0/L1/L2 tests live in the committed tree; re-run with:

```
go test ./core/engine ./core/adapter ./core/authority ./cmd/sparrow -count=1
```

Baseline on 2026-04-20: engine 0.02s / adapter 2.3s / authority 4.6s / sparrow 2.3s — all green, 193 Go tests total across the S4-S7 scope.

## 2. Accepted Topology Claim Recap

Pinned by the S8 close doc §4. Reproduced here so every row in the matrix below is read against the correct bounded shape:

1. single active master
2. multiple volumes
3. three bounded replica slots per volume, distinct servers
4. one current authoritative primary, two failover/rebalance candidates
5. publisher-owned `Epoch` / `EndpointVersion`
6. durable current authority line, one record per volume
7. passive convergence (publish → observe → confirm / stuck / supersede)
8. real `VolumeBridge` delivery into adapter/engine

Anything outside this set is P15 or explicit non-goal.

## 3. Evidence Matrix

### Claim 1 — Observation is system-fed, not test-authored

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| `TestObservation_BuilderStampsAuthorityFromReader` / `TestObservation_SupportabilityDoesNotReadClusterSnapshotOutput` / `TestObservation_NeverMintsAuthority_BoundaryGuard` | `TestObservation_EndToEnd_SystemFedSnapshotReachesController` / `TestConvergenceRoute_ObservationFedFailover_ConfirmsAndClearsDesired` / `TestS7_ReloadedAuthority_ReanchorsAdapterViaVolumeBridge` | (covered at L1; `sparrow` run-path uses the same `ObservationHost` construction) | L3 shape only, not executed — `ha-restart-recovery.yaml` / `ha-failover.yaml` carry the L3 shape | PROVEN | none at L0/L1 |

### Claim 2 — Topology supportability is explicit and fail-closed (partial inventory / missing-server / conflict / duplicate / unknown-volume / expired)

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| `TestObservation_PartialInventory_PendingThenUnsupported` / `TestObservation_MissingServerObservation_NeverOrdinaryIneligible` / `TestObservation_ConflictingPrimaryClaim_Unsupported` / `..._NoWinnerEverChosen` / `TestObservation_DuplicateServerTopology_UnsupportedAndEvidenceRecorded` / `TestObservation_UnknownObservedVolume_ProducesUnsupportedEvidence` / `TestObservation_ExpiredFact_SemanticallyIneligible` / `TestObservation_ExpiredDoesNotReachController` / `TestObservation_FreshnessWindowHonored_NotTickMultiples` | `TestObservation_SupportabilityCollapsePropagatesToController` / `TestObservation_SupportabilityCollapseClearsControllerDesired` / `TestS7_Restart_UnsupportedTopologyRecordsEvidence` | N/A (covered by L0/L1) | L3 shape only, not executed — `fault-partition.yaml` is the L3 shape | PROVEN | none |

### Claim 3 — Durable authority is single-owner, atomic, and restart-recovered

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| `TestFileAuthorityStore_RoundTrip` / `TestFileAuthorityStore_LatestWinsPerVolumeID` / `TestFileAuthorityStore_EnvelopeShape` / `TestDurableAuthority_AtomicWrite_NoTornRecord` / `TestDurableAuthority_CorruptRecord_PerVolumeFailClosed` / `TestDurableAuthority_ProcessLock_ExclusiveOwner` / `TestDurableAuthority_ProcessLock_IdempotentRelease` / `TestDurableAuthority_ProcessLock_SurvivesStaleFileOnDisk` / `TestDurableAuthority_UnreadableIndex_BootFailsClosed` / `TestDurableAuthority_StoreNeverMintsAuthority_BoundaryGuard` / `TestDurableAuthority_StoreWriteCallerAllowlist_BoundaryGuard` / `TestEncodeVolumeIDForFilename_Injective` / `TestEncodeVolumeIDForFilename_NoFilesystemUnsafeOutput` | `TestDurableAuthority_ReloadReproducesCurrentLine` / `TestDurableAuthority_ControllerSeesReloadedLineAtBoot` / `TestDurableAuthority_StoreBackedPublisherRunsNormalRoute` / `TestDurableAuthority_PutFailureRollsBackInMemoryState` / `TestS7_ReloadedAuthority_ReanchorsAdapterViaVolumeBridge` | `TestBootstrap_FreshDir_AcquiresLockAndReloads` / `TestBootstrap_WithExistingRecord_ReloadsPublisherState` / `TestBootstrap_CorruptRecord_LoggedAtStartup` / `TestBootstrap_LockContention_RefusesSecondBootstrap` / `TestS7Process_BootstrapReloadRouteSmoke` / **subprocess**: `TestS7Process_RealSubprocessRestartSmoke` | L3 shape only, not executed — `ha-restart-recovery.yaml` / `diag-restart-recovery.yaml` | PROVEN | Windows `os.RemoveAll` may race with lock release at teardown — logged as residual per S7 sketch §8.5; does NOT affect correctness |

### Claim 4 — No old-slot durable revival after restart

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| `TestDurableAuthority_StalePreFailoverRecordRejectedOnReload` | `TestS7_Restart_OldSlotNotRevivedFromDurable` (explicitly asserts `LastAuthorityBasis("vr","r1")==false` and `("vr","r2")==Epoch=2` post-restart) | covered by S7 subprocess smoke plus in-process two-run `TestS7Process_RestartDoesNotRegressAuthorityLine` | L3 shape only, not executed | PROVEN | none — row 4a/4b split in S7 closes the original ambiguity |

### Claim 5 — Live-route monotonic guard rejects stale old-slot delivery (pre-restart)

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| adapter-internal: engine monotonic guards covered in `core/adapter/adapter_test.go` and `core/engine/*_test.go` | `TestS7_LiveRoute_OldSlotDeliveryRejectedByAdapter` (real `VolumeBridge`, in-memory publisher holds both per-replica keys, lower-epoch catch-up rejected) | N/A (live-route, pre-restart) | N/A | PROVEN | none |

### Claim 6 — Controller-driven bind / reassign / refresh over accepted topology (system-driven, not test-driven)

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| `TestTopologyController_InitialPlacementBalancesAcrossVolumes` / `..._InitialPlacementUsesEvidenceTieBreakOnEqualLoad` / `..._FailoverUsesHighestEvidenceCandidate` / `..._RebalanceMovesToLighterServer` / `..._RebalanceSkipsWhenLoadAlreadyWithinBound` / `..._ObservedAuthorityClearsPending` / `..._StalePreConfirmSnapshotDoesNotDuplicateQueuedMove` | Placement / failover: `TestTopologyControllerToPublisher_E2E_MultiVolumePlacementAndFailover` / `TestObservationHost_IdenticalSupportedSnapshots_NoDuplicateAsks` / `TestObservationHost_VolumeRecovers_ClearsStaleDesired`. Rebalance: `TestTopologyControllerToPublisher_E2E_Rebalance`. **Refresh: `TestConvergenceRoute_RefreshEndpoint_ConfirmsAndClearsDesired`** (host heartbeat changes slot DataAddr/CtrlAddr while publisher holds old addrs → controller emits `IntentRefreshEndpoint` → publisher bumps `EndpointVersion` on same `Epoch` → bridge delivers refreshed `AssignmentInfo` → confirmation clears desired). | `TestS7Process_RealSubprocessRestartSmoke` covers the Bind mint via the real binary (Reassign/Refresh are L1-only in `sparrow` today; real binary does not ingest heartbeats — see §5 L2 scope note). | L3 shape only, not executed | PROVEN | none |

### Claim 7 — Convergence: confirmation clears desired

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| `TestConvergence_Confirmation_ClearsDesired` / `TestConvergence_Confirmation_RequiresBothSources` / `TestConvergence_StaleObservation_NoConfirmation` / `TestConvergence_StuckThenConfirm_ClearsStuckEvidence` | `TestConvergenceRoute_ObservationFedFailover_ConfirmsAndClearsDesired` | N/A | L3 shape only, not executed | PROVEN | none |

### Claim 8 — Publish-not-observed stuck evidence is bounded and non-churning

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| `TestConvergence_PublishNotObserved_StuckAfterWindow` / `TestConvergence_Stuck_DoesNotReMintReassign` / `TestConvergence_Reassign_CalledAtMostOncePerDesired` / `TestConvergence_RetryStateIsPerVolume` / `TestConvergence_SupportedRecovery_ClearsPriorUnsupportedEvidence` | `TestConvergenceRoute_PublishNotObserved_StuckWithoutChurn` | N/A | L3 shape only, not executed | PROVEN | none |

### Claim 9 — Supersede by newer authority or different decision

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| `TestConvergence_Supersede_PublisherAdvancedToOtherReplica` / `TestConvergence_NormalLag_OldObservationDoesNotSupersede` / `TestConvergence_Supersede_DecisionTableProducesDifferentAsk` | `TestConvergenceRoute_Supersede_PublisherMovedElsewhere_DropsStaleDesired` | N/A | L3 shape only, not executed | PROVEN | Case 2 of the supersede rule was intentionally dropped (see S6 sketch §9) — that decision is tested indirectly by `TestConvergence_NormalLag_OldObservationDoesNotSupersede` which would otherwise fail |

### Claim 10 — Restart re-anchors authority via real `VolumeBridge` into the adapter/engine route

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| `TestDurableAuthority_PublisherAdvancesFromReloaded` | `TestS7_ReloadedAuthority_ReanchorsAdapterViaVolumeBridge` / `TestS7_Restart_RecomputesDesiredNotTransientState` (hard precondition prevents vacuous pass) | **Not at L2.** The `sparrow` subprocess smoke (`TestS7Process_RealSubprocessRestartSmoke`) reloads durable `Publisher` state only; it does NOT construct `ObservationHost`, `TopologyController`, `VolumeBridge`, or `VolumeReplicaAdapter`, so the bridge/adapter re-anchor cannot be counted at L2. L2 proves a strictly narrower subclaim (durable `Publisher` reload — Claim 3). | L3 shape only, not executed — `ha-restart-recovery.yaml` is the L3 shape | PROVEN at L1 (full route); L2 COVERS a subclaim only (durable reload, see Claim 3) | Subprocess heartbeat ingress + real-binary adapter route → **P15 T1** (handoff item #2) |

### Claim 11 — Stale observation cannot move authority backward

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| `TestConvergence_StaleObservation_NoConfirmation` | `TestS7_Restart_StaleObservationCannotMoveBackward` (4 sub-cases: stale-epoch / stale-replica / stale-endpointVersion / unassigned via `snapshottingReader` host-reader freeze) / `TestConvergenceRoute_PublishNotObserved_StuckWithoutChurn` (proves host-reader lag scenario) | covered at L1 | L3 shape only, not executed — `fault-partition.yaml` | PROVEN | stale-basis simulation is via host-reader freeze; heartbeat wire does not carry authority (documented in S7 sketch §9.1) |

### Claim 12 — Unsupported topology after restart records evidence, not silent idle

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| `TestConvergence_UnsupportedClearsDesiredWithEvidence` / `TestConvergence_PendingClearsDesired` | `TestS7_Restart_UnsupportedTopologyRecordsEvidence` | **Not at L2.** This claim is about the controller's `LastUnsupported(vid)` surface being populated from conflicting / incomplete observation; that requires `ObservationHost` + controller composition, which the subprocess does not construct. `Bootstrap.ReloadSkips` is a DIFFERENT surface (S5 durable-record corruption, see Claim 3 residual path) and is NOT observation-layer unsupported-topology evidence. | L3 shape only, not executed — `fault-partition.yaml` + `diag-restart-recovery.yaml` are the L3 shapes | PROVEN at L1 | Operator-visible surface over `LastUnsupported` / `LastConvergenceStuck` → **P15 T5** Diagnostics (handoff item #6) |

### Claim 13 — Per-volume isolation (one volume's failure does not block others)

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| `TestObservation_BadVolumeDoesNotBlockHealthyVolume_Isolation` / `TestObservation_VolumeUnsupportedEvidence_IsolatedPerVolume` / `TestConvergence_OneStuckVolumeDoesNotBlockOthers` / `TestTopologyController_UnsupportedVolumeDoesNotBlockOtherVolumes` | `TestS7_Restart_PerVolumeIsolation` | N/A | L3 shape only, not executed | PROVEN | none |

### Claim 14 — Placement / rebalance / failover within the accepted topology

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| `TestTopologyController_InitialPlacementBalancesAcrossVolumes` / `..._FailoverUsesHighestEvidenceCandidate` / `..._RebalanceMovesToLighterServer` / `..._RebalanceSkipsWhenLoadAlreadyWithinBound` / `..._OutOfTopologyAuthority_NoAskPlusEvidence` | `TestTopologyControllerToPublisher_E2E_MultiVolumePlacementAndFailover` / `TestTopologyControllerToPublisher_E2E_Rebalance` | N/A | L3 shape only, not executed — `ha-failover.yaml` carries the L3 shape | PROVEN | none |

### Claim 15 — Fence path routes through bridge + adapter without spurious Healthy

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| adapter fence-slot / monotonic-guard tests in `core/adapter/adapter_test.go` | `TestS7_Restart_FenceRouteBoundedWithoutCallback` (Fence command reaches executor via real bridge; withheld callback does not produce Healthy) | N/A | L3 shape only, not executed | PARTIAL | Full timeout-expiry quantitative bound (watchdog fires at exact deadline + structured event) is adapter-owned proof (sketch §10.1 Path B — S7 may not reconfigure the fence path). Carried to the adapter package's own tests; not an L1 S7 proof |

### Claim 16 — Boundary guards (no unauthorized `AssignmentInfo` minting, no observation-side asks, no store minting, no hidden adapter ingress)

| L0 | L1 | L2 | L3 | Status | Residual |
|---|---|---|---|---|---|
| `TestObservation_NeverMintsAuthority_BoundaryGuard` / `TestDurableAuthority_StoreNeverMintsAuthority_BoundaryGuard` / `TestDurableAuthority_StoreWriteCallerAllowlist_BoundaryGuard` / `TestNonForgeability_*` (AST-level guards in `authority_test.go`) | N/A (structural, not route) | N/A | N/A | PROVEN | none |

## 4. Gap Classification

Per S8 assignment §3.B, every missing proof must be classified. Scanning the matrix above:

### 4.1 P14 internal blockers
**None.** Every P14 internal claim has at least one PROVEN row.

### 4.2 P14 S8 test/harness gap
**Claim 15 (fence quantitative bound).** S7 deliberately capped at L1 route coverage. The full `fence timeout fires at exact deadline` proof lives in the adapter package and depends on the uncommitted fence-watchdog branch landing committed. After that commits, a single-file adapter test replaces the current PARTIAL status. Not an S8 gate.

### 4.3 P15 product-surface gaps (carried forward)
See `v3-phase-14-s8-p15-handoff.md` for the full mapping (aligned to canonical `v3-phase-15-product-plan.md` §4 track numbering). Cross-reference summary:

- Frontend data path + subprocess heartbeat ingress → **P15 T1** Frontend + Data Path
- CSI lifecycle → **P15 T2** CSI / External Lifecycle Surface
- External control API → **P15 T3** External Control API
- Security / auth → **P15 T4** Security And Auth Posture
- Operator diagnostics for `LastUnsupported` / `LastConvergenceStuck` / `ReloadSkips` → **P15 T5** Diagnostics
- Operator workflow (drain / supervised reassign / rebuild) → **P15 T6** Operator Workflow
- V2/V3 migration → **P15 T7** Migration
- Deployment / hardening → **P15 T8** Deployment / Release Hardening
- End-to-end cluster validation → **P15 Final Gate**

### 4.4 14A verification follow-ups
See `v3-phase-14a-checklist.md` for the sidecar list. The S8 matrix above is the input to 14A's final targeted review.

### 4.5 Explicit non-goals (do NOT carry)
- multi-master / leader election
- performance / soak claims at P14 close
- long-running stability under production load

## 5. L2 Process Smoke Decision

Per S8 assignment §3.D, S8 must answer: are current `sparrow` smoke surfaces sufficient?

**Answer: scoped YES — L2 proves durable-restart only; the full S4-S7 route remains L1-proven.** (Round-2 architect correction.)

### 5.1 What L2 proves today

| Required check | Covered by |
|---|---|
| real process starts | `TestS7Process_RealSubprocessRestartSmoke` spawns the real `sparrow` binary. |
| durable authority reloads | same test + component `TestS7Process_BootstrapReloadRouteSmoke`. |
| restart does not mint backward | subprocess asserts `backwardMint: false` in the pinned JSON pass-line; component `TestS7Process_RestartDoesNotRegressAuthorityLine` asserts reloaded `Epoch` equals pre-restart `Epoch`. |
| structured output can be consumed by tester | subprocess pass-line schema pinned in S7 sketch §8.4. |

### 5.2 What L2 deliberately does NOT prove

| Not covered at L2 | Where it IS proven | Carry-forward |
|---|---|---|
| "heartbeat wire → `ObservationHost` → `TopologyController`" in a real process. Subprocess drives mints via `StaticDirective`; there is no heartbeat ingress in `sparrow` today. | L1: `TestObservation_EndToEnd_SystemFedSnapshotReachesController`, `TestConvergenceRoute_ObservationFedFailover_ConfirmsAndClearsDesired`, `TestConvergenceRoute_RefreshEndpoint_ConfirmsAndClearsDesired`, `TestS7_ReloadedAuthority_ReanchorsAdapterViaVolumeBridge` (all run real `ObservationHost` → controller → publisher → bridge → adapter, in-process). | **P15 T1 Frontend + Data Path** (subprocess heartbeat ingress is part of the frontend/transport contract). Handoff item #2 in `v3-phase-14-s8-p15-handoff.md`. |
| "assignment reaches adapter via bridge IN THE REAL BINARY". The subprocess binary does not construct a VolumeReplicaAdapter — only the in-process component test does. | L1: `TestS7_ReloadedAuthority_ReanchorsAdapterViaVolumeBridge` composes the full stack in-process and waits for adapter `ModeHealthy`. | **P15 T1 Frontend + Data Path** — the first real-binary adapter ↔ frontend route is a T1 deliverable. |
| Multi-process mixed-route scenarios (separate authority + observing processes). | N/A at P14 | **P15 Final Gate** (Cluster Validation). |

### 5.3 Decision

**No new S8 smoke is added.** The scope split above is honest:

- L2 subprocess smoke correctly proves durable restart truth.
- The full S4-S7 composed route is proven at L1 — including the `RefreshEndpoint` path added in this pass.
- Subprocess heartbeat ingress and full real-binary adapter route are genuinely P15 T1 surfaces; inventing a hidden `--s8-route-smoke` heartbeat fixture would be a P15 surface in P14 clothing.

This is a downgrade from the round-1 matrix claim ("L2 covers the S4-S7 internal route"), which was an overclaim.

## 6. Baseline Command Result (2026-04-20)

```
$ go test ./core/engine ./core/adapter ./core/authority ./cmd/sparrow -count=1
ok    github.com/seaweedfs/seaweed-block/core/engine      0.016s
ok    github.com/seaweedfs/seaweed-block/core/adapter     2.344s
ok    github.com/seaweedfs/seaweed-block/core/authority   4.569s
ok    github.com/seaweedfs/seaweed-block/cmd/sparrow      2.264s

$ go test ./... -count=1
(14 packages, all PASS; runtime/schema have no tests)
```

Evidence is reproducible by any reviewer on Windows / Linux from the committed tree on `phase-14` at commit `a6421f9` or later.

## 7. Residual Risks

| # | Risk | Scope |
|---|------|-------|
| 1 | Windows `os.RemoveAll` on the lock file during `t.TempDir()` teardown races with lock release. Does NOT affect correctness — only teardown. | S7 sketch §8.5 residual; logged as `t.Logf` in `restart_route_test.go`. |
| 2 | Claim 15 partial: L1 covers "fence command dispatched via real bridge + withheld callback does not produce Healthy"; full timeout-expiry bound is adapter-package-owned. | Closes when adapter fence-watchdog branch commits. Not an S8 blocker. |
| 3 | L2 subprocess smoke drives the directive via `StaticDirective`, not real heartbeat ingress to `ObservationHost`. The full heartbeat-path is L1-proven; subprocess widening requires a P15 external surface. | P15 Frontend / Cluster Validation. |

**No S8-blocking correctness residuals.** One adapter-owned quantitative fence proof (Claim 15) remains as a P14-internal follow-up, landing when the fence-watchdog branch commits. No backward-authority-mint risk on restart. No unsupported-topology silent-idle path.

## 8. Closure Readiness

Against S8 acceptance gate (`v3-phase-14-s8-assignment.md` §6):

1. evidence matrix exists and is honest — **yes** (this document)
2. P14 internal blockers fixed or explicitly non-P14 — **yes** (§4.1 empty)
3. L0 + L1 tests pass — **yes** (baseline §6)
4. L2 process smoke present or absence classified — **yes** (§5; present)
5. L3 classified — **yes** (L3 shape only, not executed this slice; see `v3-phase-14-s8-v2-scenario-classification.md`)
6. 14A final pass has no open P14 safety blocker — **pending** (14A review on this matrix)
7. P15 handoff maps every product gap — **yes** (see `v3-phase-14-s8-p15-handoff.md`)
8. final closure statement does not claim production readiness — **yes** (see `v3-phase-14-s8-closure.md`, draft)

**S8 is evidence-complete pending 14A sign-off.** Awaiting architect + tester review.
