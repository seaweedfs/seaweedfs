# T4d QA Scenario Catalogue (component-scope)

**Date**: 2026-04-25
**Status**: ✅ ACTIVE — catalogue ready for conversion to test code as each T4d-N batch lands
**Owner**: QA
**Mirrors**: T4c QA Stage-1 (`core/replication/component/qa_t4c_scenarios_test.go`) — same framework + same authoring discipline
**Used by**: QA at each T4d-N batch close; converted to actual test code in `core/replication/component/qa_t4d_scenarios_test.go` (or split per batch)

---

## §1 Purpose

Component-scope QA scenarios provide a **second independent perspective** alongside sw's unit + L2 tests. Distinct discipline:

| Sw tests | QA component scenarios |
|---|---|
| Unit-level: tight scope, single-replica, mock peers | Cluster-level: `WithReplicas(N)`, real wire, real apply path |
| Test the implementation pieces | Test the invariants through the orchestration layer |
| Single substrate (or matrix at unit-scope) | `component.RunMatrix` — both substrates per scenario by default |
| Pre-built sw test setup helpers | Component framework primitives (`WithPrimaryStorageWrap`, `NewSeverDuringScanWrap`, `NewObservedScanWrap`, `WithLiveShip`, `PrimaryWriteViaBackend`) |

QA scenarios MUST NOT duplicate sw's tests at a different file location — they must exercise **different combinations** that surface integration-layer behavior sw's unit tests don't naturally cover.

---

## §2 Authoring discipline (per T4c QA Stage-1 precedent)

- Each scenario pins ≥1 invariant from catalogue §3.3
- `RunMatrix` when invariant applies to both substrates; `RunSubstrate(...,Smartwal/Walstore,...)` when substrate-mode specific
- Don't reach into framework internals — surface a primitive gap to sw if missing (T4c precedent: `WithPrimaryStorageWrap` etc.)
- Component-scope ONLY (not L2 subprocess, not L3 hardware) — those are sw + future m01 deliverables
- Scenarios stay ~30-50 lines each; bigger means the framework is missing a primitive

---

## §3 T4d-2 scenarios (apply gate)

T4d-2 is where most new invariants land. Sw's mini-plan §2.4 lists 14 unit-scope tests. QA scenarios add multi-replica + matrix + composition coverage.

### QA #1 — `TestT4d_QA_NoPerLBADataRegression_RetryAfterReplicaAdvanced`

**Pins**: `INV-REPL-NO-PER-LBA-DATA-REGRESSION` + `INV-REPL-RECOVERY-STALE-ENTRY-SKIP-PER-LBA` (composite — goal-level invariant via mechanism invariant under retry path)

**Distinct from sw**: sw's `TestComponent_Adversarial_StaleEntryDoesNotRegress` is single-replica + single-attempt. This composes multi-replica + sever-mid-stream + retry — verifies the §6.2 Option A safety claim from a higher orchestration angle than the G-1 §7 test pins.

**Substrate**: matrix

**Setup outline**:
```go
component.RunMatrix(t, func(t, c) {
    c.WithReplicas(2).
        WithPrimaryStorageWrap(component.NewSeverDuringScanWrap(5, nil)).
        Start()
    c.PrimaryWriteN(20)
    c.PrimarySync()
    // attempt #1 ships [R+1..5], severs
    r1 := c.CatchUpReplica(0); assert !r1.Success
    // primary writes more; replica 0's R has advanced via partial
    c.PrimaryWriteN(10)
    // attempt #2 retries with original Recovery.R+1; over-ships [R+1..5] before reaching new entries
    r2 := c.CatchUpReplica(0); assert r2.Success
    // assert byte-exact + no per-LBA regression at any LBA
    c.AssertReplicaConverged(0)
    c.AssertNoPerLBARegression(0) // new framework primitive needed
})
```

**Framework primitive needed**: `Cluster.AssertNoPerLBARegression(idx)` — surface to sw if not already in T4d-2 framework additions.

---

### QA #2 — `TestT4d_QA_LaneDiscipline_LiveAndRecoveryConcurrent_BothCorrect`

**Pins**: `INV-REPL-LANE-DERIVED-FROM-HANDLER-CONTEXT` (Q2) — composition test

**Distinct from sw**: sw's `TestApplyGate_LaneNeverDerivedFromWireField` is a fence test (asserts no wire byte). This exercises the actual concurrent-lane behavior — live ships + catch-up running simultaneously, verify each lane's apply path executes correctly without cross-contamination.

**Substrate**: matrix

**Setup outline**:
```go
component.RunMatrix(t, func(t, c) {
    c.WithReplicas(1).WithLiveShip().Start()
    // concurrent: live-ship some LBAs while catch-up is mid-stream
    c.PrimaryWriteViaBackendN(5) // live lane
    go c.CatchUpReplica(0)        // recovery lane
    c.PrimaryWriteViaBackendN(5) // more live during catch-up
    c.WaitForConverge(2 * time.Second)
    // assert: liveTouched contains live's LBAs; recoveryCovered contains catch-up's LBAs;
    // no overlap-routing errors (recovery LBA didn't go through live path or vice versa)
    c.AssertLaneIntegrity(0) // new framework primitive needed
})
```

**Framework primitive needed**: `Cluster.AssertLaneIntegrity(idx)` — verifies internal handler-routing correctness from the Cluster orchestration angle.

---

### QA #3 — `TestT4d_QA_CoverageAdvancesOnSkip_BarrierStillCompletes`

**Pins**: `INV-REPL-RECOVERY-COVERAGE-ADVANCES-ON-SKIP` + `INV-REPL-CATCHUP-COMPLETION-FROM-BARRIER-ACHIEVED-LSN` together (forward-carry under coverage path)

**Distinct from sw**: sw's `TestComponent_RecoveryStaleSkip_CoverageStillAdvances` tests single-LBA case. This tests the all-stale case: replica is FULLY caught up (no actual data writes needed), recovery stream ships everything, every entry stale-skips, barrier MUST still complete. Stresses the "skip data ≠ ignore frame" architectural decision.

**Substrate**: matrix

**Setup outline**:
```go
component.RunMatrix(t, func(t, c) {
    c.WithReplicas(1).Start()
    // pre-seed replica fully via ReplicaApply (matches primary)
    for i := 0; i < 10; i++ {
        data := makeData(i)
        lsn := c.PrimaryWrite(uint32(i), data)
        c.ReplicaApply(0, uint32(i), data, lsn)
    }
    c.PrimarySync()
    // catch-up will ship entries; replica will skip all (already applied at higher LSN)
    result := c.CatchUpReplica(0)
    assert result.Success                      // barrier completes
    assert result.AchievedLSN == 10            // achieved despite zero data writes
    c.AssertReplicaConverged(0)
})
```

---

### QA #4 — `TestT4d_QA_RecoveryRestart_SecondAttemptSeedsFromSubstrate`

**Pins**: `INV-REPL-RECOVERY-COVERAGE-RESTART-SAFE` (Option C hybrid)

**Distinct from sw**: sw's `TestApplyGate_RestartMidRecovery_SessionRestartReseeds` tests the gate's seed mechanism in isolation. This tests the cross-substrate Option C hybrid behavior at the orchestration layer: walstore + smartwal must seed from `AppliedLSNs()`; BlockStore variant must fall back to in-memory + log INFO once.

**Substrate**: matrix + dedicated BlockStore variant

**Setup outline (matrix walstore+smartwal)**:
```go
component.RunMatrix(t, func(t, c) {
    c.WithReplicas(1).Start()
    c.PrimaryWriteN(10)
    c.PrimarySync()
    c.CatchUpReplica(0)         // replica caught up via session #1
    c.RestartReplica(0)         // simulate restart; new framework primitive
    c.PrimaryWriteN(5)
    c.PrimarySync()
    c.CatchUpReplica(0)         // session #2 re-seeds appliedLSN from substrate
    c.AssertReplicaConverged(0)
    c.AssertSessionStartReseededFromSubstrate(0) // new framework primitive
})
```

Plus dedicated `TestT4d_QA_RecoveryRestart_BlockStoreFallback_LogsOnce` smartwal variant verifying the INFO log.

**Framework primitives needed**: `Cluster.RestartReplica(idx)` + `Cluster.AssertSessionStartReseededFromSubstrate(idx)`.

---

## §4 T4d-3 scenarios (R+1 + engine-driven recovery)

T4d-3 unblocks `INV-REPL-CATCHUP-WITHIN-RETENTION-001` (un-pinned at T4c). Sw's mini-plan §2.5 lists 11 R+1 + engine-wiring tests. QA adds matrix-multi-replica composition.

### QA #5 — `TestT4d_QA_CatchUpFromRPlus1_BandwidthBounded_MultiReplica`

**Pins**: `INV-REPL-CATCHUP-WITHIN-RETENTION-001` (T4c un-pin → T4d-3 PORTED) — at multi-replica scope

**Distinct from sw**: sw's `TestT4d3_CatchUp_NonEmptyReplica_ShortGap_BandwidthBounded_{Walstore,Smartwal}` is single-replica. This composes 2 replicas at different R values — primary's catch-up to each must scan from each replica's individual R+1, not a shared start.

**Substrate**: matrix walstore + smartwal (BlockStore variant separate per G-1 §9 caveat)

**Setup outline**:
```go
component.RunSubstrate(t, "walstore", component.Walstore, func(t, c) {
    c.WithReplicas(2).Start()
    c.PrimaryWriteN(50)
    c.PrimarySync()
    c.CatchUpReplica(0)  // replica 0 fully caught up: R=50
    c.PrimaryWriteN(20)  // primary advances to H=70
    c.PrimarySync()
    counter1 := new(atomic.Int32)
    counter0 := new(atomic.Int32)
    // engine emits StartCatchUp.FromLSN=51 for replica 0 (R+1=51); ships 20 entries
    // replica 1 still empty: FromLSN=1; ships ~70 entries
    c.CatchUpReplicaWithEmitCounter(0, counter0)  // new primitive
    c.CatchUpReplicaWithEmitCounter(1, counter1)
    assert counter0.Load() == 20
    assert counter1.Load() >= 70
})
```

**Framework primitive needed**: `Cluster.CatchUpReplicaWithEmitCounter(idx, *atomic.Int32)` — convenience wrapper around existing `NewObservedScanWrap` + `CatchUpReplica`.

---

### QA #6 — `TestT4d_QA_FromLSNFromEngineState_NotStaleProbeDirectly`

**Pins**: `INV-REPL-CATCHUP-FROMLSN-FROM-ENGINE-STATE-NOT-PROBE`

**Distinct from sw**: sw's `TestT4d3_CatchUp_Engine_RPlus1_FromOwnState_NotProbeDirectly` tests at engine unit scope. This drives the same scenario through the Cluster — two probes interleave; engine state must reflect the LATEST probe; emit must use updated state.

**Substrate**: matrix

**Setup outline**:
```go
component.RunMatrix(t, func(t, c) {
    c.WithReplicas(1).Start()
    c.PrimaryWriteN(50); c.PrimarySync()
    c.ProbeReplica(0)     // probe #1 reports R=0; engine state R=0
    c.CatchUpReplica(0)   // catches up; replica R now 50
    c.ProbeReplica(0)     // probe #2 reports R=50; engine state updated R=50
    c.PrimaryWriteN(20); c.PrimarySync()
    counter := new(atomic.Int32)
    c.CatchUpReplicaWithEmitCounter(0, counter)
    // engine emit uses current R=50 → FromLSN=51 → ships 20 entries
    // NOT stale probe #1's R=0 → FromLSN=1 → would ship 70
    assert counter.Load() == 20
})
```

---

### QA #7 — `TestT4d_QA_TransportImportFence_NoSubstrateInternals`

**Pins**: `INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY` (Q3) — compile-time fence

**Distinct from sw**: sw's `TestT4d3_Catchup_TransportNeverImportsSubstrateInternals` is the same fence; QA's variant is a parallel grep test that runs in CI alongside sw's, with QA-perspective error message ("transport package leaked substrate dependency — check core/transport imports against contract surface allow-list"). Belt-and-suspenders for an architecturally-critical invariant.

**Substrate**: N/A (linter-style; no Cluster setup needed)

**Setup outline**:
```go
func TestT4d_QA_TransportImportFence_NoSubstrateInternals(t *testing.T) {
    forbidden := []string{
        "github.com/seaweedfs/seaweed-block/core/storage/walstore",
        "github.com/seaweedfs/seaweed-block/core/storage/smartwal",
    }
    transportFiles, _ := filepath.Glob("../../transport/*.go")
    for _, file := range transportFiles {
        content, _ := os.ReadFile(file)
        for _, forbid := range forbidden {
            if strings.Contains(string(content), forbid) {
                t.Errorf("transport package leaked substrate dependency: %s imports %s\n"+
                    "  Q3 architect lock: transport may only depend on core/storage contract surface, not substrate internals.\n"+
                    "  See INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY",
                    file, forbid)
            }
        }
    }
}
```

---

## §5 T4d-4 scenarios (lifecycle + remaining T4c carries)

### QA #8 — `TestT4d_QA_LastSentMonotonic_AcrossRetries_FullForm`

**Pins**: `LastSentMonotonic_AcrossRetries` full cross-call form (deferred T4c Stage-2 scenario, now testable)

**Distinct from sw**: this IS the full form of T4c QA #6 (`TestT4c_QA_LastSentMonotonic_WithinCall`). T4c could only test within-call because `WithEngineDrivenRecovery` was a stub; T4d-3 binds it for real. T4d-4 lands the cross-call form QA-side.

**Substrate**: matrix

**Setup outline**:
```go
component.RunMatrix(t, func(t, c) {
    c.WithReplicas(1).
        WithEngineDrivenRecovery().                          // now real, not stub
        WithPrimaryStorageWrap(component.NewSeverDuringScanWrap(3, nil)).
        Start()
    c.PrimaryWriteN(20)
    c.PrimarySync()
    // engine drives multiple retry attempts (sever happens each call)
    // assert: lastSent across attempts is monotonic (each attempt's lastSent ≥ prior achieved)
    observed := c.WaitForEngineExhaustion(0, 10*time.Second)  // new primitive
    c.AssertLastSentMonotonicAcrossAttempts(observed)         // new primitive
})
```

**Framework primitives needed**: `Cluster.WaitForEngineExhaustion(idx, timeout)` + `Cluster.AssertLastSentMonotonicAcrossAttempts([]SessionResult)`.

---

### QA #9 — `TestT4d_QA_LifecycleStop_DuringActiveCatchUp_NoHandleLeak`

**Pins**: `INV-REPL-LIFECYCLE-HANDLE-BORROWED-001` — adversarial timing

**Distinct from sw**: sw's `TestReplicationVolume_Stop_DuringCatchUp_TerminatesCleanly` tests that Stop completes. This adds the BUG-005 non-repeat assertion: substrate handle was BORROWED (not owned) by ReplicationVolume; Stop MUST NOT close it; subsequent ProvOpen must reuse the handle without "already closed" errors.

**Substrate**: matrix

**Setup outline**:
```go
component.RunMatrix(t, func(t, c) {
    c.WithReplicas(1).Start()
    c.PrimaryWriteN(50)
    c.PrimarySync()
    done := make(chan struct{})
    go func() {
        c.CatchUpReplica(0)
        close(done)
    }()
    time.Sleep(10*time.Millisecond)  // let catch-up begin
    c.StopReplicationVolume(0)        // Stop mid-catchup
    <-done
    // assert substrate handle still open (borrowed-not-owned)
    c.AssertSubstrateHandleStillOpen(0)
    // assert can ProvOpen + reuse without error
    c.ReopenReplicationVolume(0)
    c.AssertReplicaConverged(0)
})
```

**Framework primitives needed**: `Cluster.StopReplicationVolume(idx)`, `Cluster.AssertSubstrateHandleStillOpen(idx)`, `Cluster.ReopenReplicationVolume(idx)`.

---

## §6 Cumulative scenario count

| Batch | QA scenarios | Sw tests in mini-plan | Total invariant pin coverage |
|---|---|---|---|
| T4d-2 | 4 (QA #1-#4) | 14 | All 6 T4d-2 invariants pinned by both sides |
| T4d-3 | 3 (QA #5-#7) | 11 | All 4 T4d-3 invariants pinned by both sides |
| T4d-4 | 2 (QA #8-#9) | 15 | T4c §I carries closed |
| **Total T4d QA** | **9 scenarios** | 40 sw tests | 19 forward-carry invariants verified by checklist |

T4c QA Stage-1 had 6 scenarios; T4d expands to 9 reflecting the bigger T4d scope.

---

## §7 Framework primitives requested from sw (forward-carry to T4d-2/T4d-3/T4d-4 framework PRs)

Per T4c precedent, QA surfaces missing primitives early so sw can land them in the same batch as the underlying production code:

| Primitive | Used by QA scenario | Lands in batch |
|---|---|---|
| `Cluster.AssertNoPerLBARegression(idx)` | QA #1 | T4d-2 (apply gate is the source of truth for per-LBA tracking) |
| `Cluster.AssertLaneIntegrity(idx)` | QA #2 | T4d-2 |
| `Cluster.RestartReplica(idx)` | QA #4 | T4d-2 |
| `Cluster.AssertSessionStartReseededFromSubstrate(idx)` | QA #4 | T4d-2 |
| `Cluster.CatchUpReplicaWithEmitCounter(idx, *atomic.Int32)` | QA #5, #6 | T4d-3 (convenience wrapper around `NewObservedScanWrap` + `CatchUpReplica`) |
| `Cluster.WaitForEngineExhaustion(idx, timeout)` | QA #8 | T4d-3 (alongside `WithEngineDrivenRecovery` real binding) |
| `Cluster.AssertLastSentMonotonicAcrossAttempts([]SessionResult)` | QA #8 | T4d-3 |
| `Cluster.StopReplicationVolume(idx)` | QA #9 | T4d-4 |
| `Cluster.AssertSubstrateHandleStillOpen(idx)` | QA #9 | T4d-4 |
| `Cluster.ReopenReplicationVolume(idx)` | QA #9 | T4d-4 |

Surfacing now (pre-batch) avoids the round-39/40 T4c experience where QA had to wait for sw to add `WithPrimaryStorageWrap` + `WithLiveShip` before authoring deferred scenarios.

---

## §8 Authoring sequence

QA scenarios convert from catalogue to actual test code per batch:

| When | Action |
|---|---|
| After T4d-2 lands | Author QA #1-#4 in `core/replication/component/qa_t4d2_scenarios_test.go` (or merge into single `qa_t4d_scenarios_test.go`) |
| After T4d-3 lands | Author QA #5-#7 |
| After T4d-4 lands | Author QA #8-#9 |
| At T4d batch close | Verify all 9 scenarios green at HEAD; cite in QA single-sign artifact |

If a scenario can't be authored due to missing framework primitive (per §7), surface to sw immediately — same discipline as T4c QA round-39 (`WithPrimaryStorageWrap` etc.).

---

## §9 Change log

| Date | Change | Author |
|---|---|---|
| 2026-04-25 | Initial v0.1 catalogue from T4d kickoff v0.3 + mini-plan v0.2 + G-1 v0.2. 9 component-scope scenarios across T4d-2 (4) / T4d-3 (3) / T4d-4 (2). Each pins specific invariant + setup outline + framework primitive requirements. 10 framework primitives surfaced for sw's batch PRs (avoids round-40-style "QA blocked on framework gap" deferral). Discipline mirrors T4c QA Stage-1 (`qa_t4c_scenarios_test.go`). Authoring sequence: convert catalogue → test code per batch as sw's production code lands. | QA |
