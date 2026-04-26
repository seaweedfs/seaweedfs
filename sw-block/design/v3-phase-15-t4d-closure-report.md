# V3 Phase 15 T4d — Closure Report

**Date**: 2026-04-25
**Status**: DRAFT — awaiting QA single-sign batch close per §8C.2 + architect T-end three-sign per §8C.1 (kickoff §4 #10: T-end at T4d close IF T4d remains final T4 batch — confirmed: T4d IS the final T4 batch, no T4e)
**Gate**: T4d batch (FINAL T4 batch) within G5 (replicated write path) — T4 T-end three-sign lands at this close
**Predecessor**: T4c batch QA-single-signed + architect-accepted at `c910464a9`
**Successor**: G5 collective close (multi-replica + m01 first-light + G5-DECISION-001 + production-readiness gates)

---

## §A Batch history

| Batch | Commit | Status | LOC (+/−) | Delivery |
|---|---|---|---|---|
| T4d kickoff (architect rounds 1–8 of mini-plan) | doc-only | RATIFIED 2026-04-25 | — | Single-T4d scope; Option C hybrid; BlockStore hotfix lift-out; defense-in-depth in T4d-1; 4-batch shape (T4d-1→T4d-2→T4d-3→T4d-4); T4d-3 G-1 required; m01 to G5 collective; T-end at T4d close if no T4e |
| BlockStore walHead pre-T4d hotfix | `f6084ee` | CLOSED | 1 prod line + 11 godoc + un-skipped regression test | Substrate-internal one-liner `if lsn > s.walHead { s.walHead = lsn }`; closes round-43 production storage-contract violation independently of T4d-1 |
| **T4d-1** substrate hardening + structured `RecoveryFailureKind` | **`1edeb36`** | CLOSED | +440 / −0 across 12 files | Storage-side `StorageRecoveryFailureKind` + `RecoveryFailure` typed-error; `LogicalStorage.AppliedLSNs()` interface (Option C hybrid); `BlockStore` returns `ErrAppliedLSNsNotTracked` explicitly; walstore uses dirty-map snapshot; smartwal uses ring last-writer-wins reduction; engine `RecoveryFailureKind` enum (separate from storage's enum); `transport.classifyRecoveryFailure` boundary mapper; substring match REMOVED from engine; 10 new tests |
| T4d-1 follow-up (HARD GATE tests) | `d6b1890` | CLOSED | +120 / 0 (1 file) | `TestT4d1_TargetNotReached_DistinctKindFromWALRecycled` + `TestT4d1_StorageFailureKindMapper_AllKnownKinds` per round-46 architect HARD GATE before T4d-3 G-1 sign |
| **T4d-2** replica recovery apply gate | **`bd2de99`** | CLOSED | +630 / −15 across 6 files | `ReplicaApplyGate` lane-aware (initial impl had payload-derived discrimination — REWORKED at `01f4ab9`); 2-map split (`liveTouched` + `recoveryCovered`); `appliedLSN` map with Option C hybrid seed; recovery-lane stale-skip + coverage-advance; live-lane fail-loud; component framework `WithApplyGate()` builder; 16 new tests + 1 un-skipped adversarial |
| T4d-2 follow-up (lane-pure rework) | `01f4ab9` | CLOSED | +180 / −90 across 5 files | Round-46 architect rework: gate becomes lane-pure (`ApplyRecovery` + `ApplyLive` lane-explicit methods); payload-derived `isRecoveryLane(lineage)` REMOVED; lane discrimination moved to caller (transport replica handler) as TRANSITIONAL CALLER-SIDE SHIM with explicit TODO citing `CARRY-T4D-LANE-CONTEXT-001`; new regression tests `TestApplyGate_RecoveryWithTargetLSN1_RoutesToRecoveryLane` + `TestApplyGate_LiveWithTargetLSN100_RoutesToLiveLane`; old test renamed to `TestComponent_LanePurity_CallerControlsDispatch` |
| T4d-2 doc fix (post-round-46 stale godoc) | `a63ae9b` | CLOSED | +20 / −10 across 2 files | `apply_gate.go:38-43` rewrite to reflect lane-pure model; `replica.go` constructor godoc updated to lane-explicit hook shape per QA pre/with-T4d-3 fix requirement |
| **T4d-3** R+1 threading + boundary fences | **`44c60dd`** | CLOSED per architect Path B | +250 / −16 across 5 files | `engine.StartCatchUp.FromLSN` field; engine emits `Recovery.R + 1` (G-1 §6.1 Option A — engine adds `+1`); per-call deadline + last-sent-monotonic + retry budget semantics ported from V2; substrate scan from R+1 instead of 1; `core/transport/import_discipline_test.go` Q3 fence (`INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY`); `CARRY-T4D-LANE-CONTEXT-001` Option B test-skip with explicit forward-binding; 11 new tests including round-46 ADDITIONS (RetryAfterReplicaAdvanced + 3 substrate-bound bandwidth + BlockStoreOverShipsExpected fence). Engine-driven recovery wiring DEFERRED to T4d-4 per Path B |
| **T4d-4 part A** RecoveryMode + Stop lifecycle | **`f88d097`** | CLOSED | per part A | `RecoveryMode()` interface method on substrates (replaces duck-typed `CheckpointLSN` probe); component framework wraps forward `RecoveryMode()` correctly (closes round-40 known limitation); `ReplicationVolume.Stop` lifecycle regression (BUG-005 non-repeat); `INV-REPL-LIFECYCLE-HANDLE-BORROWED-001` inscribed |
| **T4d-4 part B** engine-driven recovery wiring + round-47 + G5-DECISION-001 | **`812d3fa`** | CLOSED | +590 / −31 across 6 files | `WithEngineDrivenRecovery()` becomes REAL (not stub); `ReplicationVolume↔adapter` wiring runs engine retry loop end-to-end; round-47 architect change: catch-up exhaustion DIRECTLY emits StartRebuild (was: cleared Decision + emit Degraded only); rebuild MaxRetries=0 → terminal failure path; `INV-REPL-CATCHUP-EXHAUSTION-ESCALATES-TO-REBUILD` + `INV-REPL-REBUILD-FAILURE-TERMINAL` inscribed; G5-DECISION-001 obligation: ReplicaState struct JSON round-trip clean; 9 new tests + 1 existing test updated |
| **T4d-4 part C** full L2 matrix + 2 engine bug fixes | **`e642ae8`** | CLOSED | ~150 / −20 across 3 files + 1 new test file | HARD CLOSE GATE #3: full L2 matrix engine→adapter→executor end-to-end; 3 new L2 tests (`TestT4d4_FullL2_CatchupRetryExhausted_EscalatesToRebuild` + `_WALRecycled_EscalatesImmediate` + `_LastSentMonotonic_AcrossRetries`). **2 engine bugs surfaced + fixed**: (Bug 1) catch-up exhaustion missed escalation when stray auto-probe re-classified `Decision` mid-flight — fix keys on `st.Session.Kind == SessionCatchUp` truth instead of mutable `Decision` field; (Bug 2) WALRecycled escalation never emitted StartRebuild and could be downgraded by next probe — new `RecoveryTruth.RebuildPinned` sticky bool with cleared-on-success semantic forces rebuild override regardless of R/S/H |

All commits on `feature/sw-block` (seaweedfs) → `phase-15` (seaweed_block).

---

## §B Signed acceptance delta vs kickoff §4

The kickoff §4 acceptance bar (RATIFIED 2026-04-25) is **fully satisfied** at this close. Recording each criterion's status explicitly:

| Kickoff §4 criterion | Status | Evidence |
|---|---|---|
| #1 All proposed batches merged | ✅ MET | T4d-1 + T4d-2 + T4d-3 + T4d-4 part A (`f88d097`) + part B (`812d3fa`) + part C (`e642ae8`) all on `phase-15` |
| #2 Unit tests green per task | ✅ MET | 22 packages green at HEAD on QA Windows workspace + m01 Linux -race |
| #3 Full L2 subprocess matrix green — engine→adapter→executor end-to-end | ✅ MET | T4d-4 part C lands 3 new L2 tests; closes the bar T4c narrowed and T4d-3 deferred per Path B fold |
| #4 L3 m01 deferred to G5 collective | ✅ acceptable per §2.6 | NOT a T4d criterion; m01 first-light for replicated write path lands at G5 collective sign across T4a/b/c/d (production-sized substrate smoke at 1 GiB DID run on m01 — see §G) |
| #5 T4a/T4b/T4c invariants forward-carry verified | ✅ MET | Forward-carry checklist v0.2 all 19 active invariants green under -race on m01; 30× stress on engine-driven flow clean |
| #6 Active invariant promotions | ✅ MET | See §F (12 inscribed invariants from T4d batches; `INV-REPL-CATCHUP-WITHIN-RETENTION-001` un-pinned at T4c → PORTED at T4d via apply gate + R+1) |
| #7 Engine sentinel decoupling — substring match REMOVED + structured kind in production | ✅ MET at T4d-1 | `core/engine/apply.go` substring helpers (`isWALRecycledFailure`, `isStartTimeoutFailure`, `containsAny`) removed; `TestEngine_SessionFailed_NoMoreSubstringMatch` source-grep fence passes |
| #8 BlockStore walHead regression fixed in production | ✅ MET (pre-T4d hotfix) | `f6084ee` lifted out per architect §2.5 #2 lift-out approval |
| #9 QA single-sign at T4d close per §8C.2 | pending | This document is the QA single-sign artifact |
| #10 T4 T-end three-sign per §8C.1 lands at T4d close IF no T4e | ✅ APPLICABLE | Confirmed: T4d IS the final T4 batch, no T4e needed; T-end three-sign lands at this close |
| #11 No §8C.3 escalation triggers fired | ✅ MET | Two real engine bugs surfaced by T4d-4 part C HARD GATE #3 (validating round-47 architect ruling that full L2 matrix is non-negotiable); both fixed in same cycle — §8C governance working as intended, NOT escalation |

**Architectural improvements signed during T4d (with architect ratification):**

| Improvement | Architect-endorsed at | Status |
|---|---|---|
| 2-enum split: `storage.StorageRecoveryFailureKind` ↔ `engine.RecoveryFailureKind` (engine zero-imports storage) | T4d-1 round-46 review | ✅ ENDORSED — "the right v0.3 boundary fix" |
| Lane-pure apply gate API (`ApplyRecovery` + `ApplyLive` lane-explicit) | T4d-2 round-46 rework | ✅ RATIFIED — replaces initial payload-derived discrimination |
| Round-47 catch-up exhaustion DIRECTLY emits StartRebuild | T4d-4 part B round-47 | ✅ RATIFIED — closes "Decision flipped but no actual rebuild fired" gap |
| `RecoveryTruth.RebuildPinned` sticky override (cleared on success) | T4d-4 part C bug fix #2 | ✅ implicit ratification via test pass — surfaces the right architectural correction |
| Process rule: G-1 docs land in seaweedfs FIRST; sw references committed hash | T4d round-46 Issue 2(a) | ✅ INSCRIBED in mini-plan §7.1; T4d-3 G-1 sign hash = `seaweedfs@80036404c` |

---

## §C Changelist by file (cumulative across T4d)

### T4d-1 + d6b1890 follow-up — substrate hardening + structured kind

| File | LOC | Notes |
|---|---|---|
| `core/storage/recovery_contract.go` | +180/−0 | `StorageRecoveryFailureKind` enum (3 kinds) + `RecoveryFailure` typed-error struct with `Unwrap()` for `errors.Is(_, ErrWALRecycled)` migration compat; `ErrAppliedLSNsNotTracked` sentinel; `NewWALRecycledFailure` + `NewSubstrateIOFailure` helpers |
| `core/storage/logical_storage.go` | +30/−0 | `AppliedLSNs() (map[uint32]uint64, error)` interface method (Option C hybrid) |
| `core/storage/walstore.go` + `walstore_recovery.go` | +90/−15 | `AppliedLSNs()` impl via `dm.snapshot()`; `ScanLBAs` wraps recycle errors as typed `RecoveryFailure{Kind: WALRecycled}` |
| `core/storage/smartwal/store.go` + `recovery_scan.go` | +75/−10 | `AppliedLSNs()` impl via `ring.scanValid()` last-writer-wins reduction; same recycle-wrap pattern |
| `core/storage/store.go` | +15/−0 | BlockStore `AppliedLSNs()` returns `(nil, ErrAppliedLSNsNotTracked)` explicitly |
| `core/engine/events.go` | +10/−0 | `SessionClosedFailed.FailureKind engine.RecoveryFailureKind` field added |
| `core/engine/apply.go` | +25/−45 | `applySessionFailed` branches on `e.FailureKind == RecoveryFailureWALRecycled`; substring helpers (`isWALRecycledFailure`, `isStartTimeoutFailure`, `containsAny`) REMOVED |
| `core/engine/recovery_test.go` | +60/−15 | `TestSessionFailed_WALRecycled_EscalatesToRebuild` rewritten to use typed kind |
| `core/engine/substring_removal_fence_test.go` | +60 NEW | `TestEngine_SessionFailed_NoMoreSubstringMatch` source-grep fence with comment-stripping helper |
| `core/adapter/normalize.go` + `types.go` | +25/−5 | `SessionCloseResult.FailureKind` typed field; `NormalizeSessionClose` propagates |
| `core/adapter/typed_failure_kind_test.go` | +75 NEW | 6 subtest end-to-end + success-path sanity |
| `core/transport/recovery_session.go` | +50 NEW | `classifyRecoveryFailure` boundary mapper using `errors.As` |
| `core/transport/executor.go` | +15/−5 | `finishSession` populates typed `FailureKind` |
| `core/transport/classifier_followup_test.go` (`d6b1890`) | +120 NEW | HARD GATE before T4d-3 G-1 sign: `TestT4d1_TargetNotReached_DistinctKindFromWALRecycled` + `TestT4d1_StorageFailureKindMapper_AllKnownKinds` |

### T4d-2 + 01f4ab9 + a63ae9b — apply gate (lane-pure)

| File | LOC | Notes |
|---|---|---|
| `core/replication/apply_gate.go` | +330 NEW | `ReplicaApplyGate` with per-session state; `ApplyRecovery` + `ApplyLive` lane-explicit methods (post-`01f4ab9`); 2-map split (`liveTouched` + `recoveryCovered`); `appliedLSN` map seeded from substrate via Option C hybrid |
| `core/replication/apply_gate_test.go` | +280 NEW | 13 unit tests; lane-discriminator regression tests (`TestApplyGate_RecoveryWithTargetLSN1_RoutesToRecoveryLane` + `TestApplyGate_LiveWithTargetLSN100_RoutesToLiveLane` per `01f4ab9`) |
| `core/transport/replica.go` | +60/−5 | `MsgShipEntry` handler dispatches via `ApplyHook` (lane-explicit post-`01f4ab9`); transitional caller-side TargetLSN==1 shim with explicit TODO citing `CARRY-T4D-LANE-CONTEXT-001` |
| `core/replication/component/cluster.go` | +40/−5 | `WithApplyGate()` builder + `Cluster.ApplyGate(idx)` accessor |
| `core/replication/component/apply_gate_scenarios_test.go` | +160 NEW | 4 integration scenarios via component framework + un-skipped `TestComponent_Adversarial_StaleEntryDoesNotRegress` from T4c skip-pile |

### T4d-3 — R+1 threading + boundary fences

| File | LOC | Notes |
|---|---|---|
| `core/transport/catchup_sender.go` | +13/−5 | `ScanLBAs(fromLSN, ...)` instead of `ScanLBAs(1, ...)` per command-borne FromLSN |
| `core/transport/executor.go` | +5/−2 | `BlockExecutor.StartCatchUp` signature gains `fromLSN uint64` |
| `core/adapter/executor.go` + test stubs | +30/−15 | Interface signature update; 7 test executor stubs updated |
| `core/engine/commands.go` | +5/−0 | `engine.StartCatchUp` command struct gains `FromLSN uint64` |
| `core/engine/apply.go` | +10/−2 | Emission populates `FromLSN = Recovery.R + 1` (G-1 §6.1 Option A — engine adds +1) |
| `core/engine/t4d3_rplus1_test.go` | +135 NEW | 3 engine R+1 emit tests including hidden invariant pins |
| `core/transport/t4d3_rplus1_test.go` | +110 NEW | 3 transport tests including round-46 ADDITION 2 substrate-bound naming (`_Walstore` / `_Smartwal` / `_BlockStoreOverShipsExpected`) |
| `core/transport/import_discipline_test.go` | +80 NEW | `INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY` Q3 fence: parses sources, fails on substrate-internal imports |
| `core/replication/component/t4d3_scenarios_test.go` | +200 NEW | 4 scenarios incl. `TestT4d3_RetryAfterReplicaAdvanced_OverScansHandledByApplyGate` (round-46 ADDITION 1) + `TestT4d3_RecoveryTargetLSN1_KnownGap` (CARRY-T4D-LANE-CONTEXT-001 Option B skip-with-forward-binding) |

### T4d-4 part A — RecoveryMode + Stop lifecycle

| File | LOC | Notes |
|---|---|---|
| `core/storage/recovery_contract.go` | +20/−0 | `RecoveryMode()` substrate interface method |
| `core/storage/{walstore,smartwal,store}.go` | +25/−10 | Per-substrate `RecoveryMode()` impl |
| `core/transport/catchup_sender.go` | +5/−10 | Replace duck-typed `CheckpointLSN()` probe with `RecoveryMode()` call |
| `core/replication/component/faults.go` | +15/−5 | Wraps forward `RecoveryMode()` correctly (closes round-40 known limitation) |
| `core/replication/volume.go` | +60/−10 | `ReplicationVolume.Stop` lifecycle (BUG-005 non-repeat); `INV-REPL-LIFECYCLE-HANDLE-BORROWED-001` |

### T4d-4 part B — engine-driven recovery wiring + round-47 + G5-DECISION-001

| File | LOC | Notes |
|---|---|---|
| `core/replication/component/cluster.go` | +98/−5 | `WithEngineDrivenRecovery()` REAL (was stub at T4c); adapter installed per replica; drive helpers |
| `core/adapter/adapter.go` + `adapter_test.go` | +25/−2 | Adapter wired to executor; engine emit→dispatch→executor→callback round-trip works |
| `core/engine/apply.go` | +42/−3 | Round-47: catch-up exhaustion DIRECTLY emits StartRebuild (was: cleared Decision + emit Degraded only); rebuild MaxRetries=0 → terminal failure path |
| `core/engine/recovery_test.go` | +99/−15 | 4 new round-47 tests + existing `TestSessionFailed_NonRecycled_RetriesUntilBudget` updated |
| `core/engine/g5_decision_001_serializability_test.go` | +95 NEW | `TestG5Decision001_ReplicaState_RoundTripJSON` + `_ZeroValueStable` — fence: ReplicaState struct stays serializable so G5-DECISION-001 Path A (persistence) remains structurally open |
| `core/replication/component/t4d4_engine_driven_test.go` | +263 NEW | 5 integration tests including `TestT4d4_RoundTrip_AssignmentToProbeToCatchUp_EngineDriven` + `TestT4d4_CatchupBudgetExhausted_EngineEmitsRebuild` + `TestT4d4_WithEngineDrivenRecovery_IsReal` |

### T4d-4 part C — full L2 matrix + 2 engine bug fixes (`e642ae8`)

| File | LOC | Notes |
|---|---|---|
| `core/engine/state.go` | +20/−5 | `RecoveryTruth.RebuildPinned bool` field with invariant doc |
| `core/engine/apply.go` | +50/−15 | **Bug #1 fix**: exhaustion keys on `st.Session.Kind == SessionCatchUp` (truth = what session actually failed) instead of mutable `Decision` field. **Bug #2 fix**: sticky-rebuild gate in `decide()`; WALRecycled escalation emits StartRebuild AND pins `RebuildPinned=true`; exhaustion emits StartRebuild AND pins; rebuild SessionCompleted clears the pin |
| `core/engine/recovery_test.go` | +30/−5 | Fixtures updated to set `Session.Kind: SessionCatchUp` per Bug #1 fix |
| `core/replication/component/t4d4_full_l2_matrix_test.go` | +280 NEW | 3 HARD GATE #3 tests: `TestT4d4_FullL2_CatchupRetryExhausted_EscalatesToRebuild` + `_WALRecycled_EscalatesImmediate` + `_LastSentMonotonic_AcrossRetries` (closes QA #8 LastSentMonotonic full form) |

### Cumulative T4d LOC

T4d-1 + T4d-2 + T4d-3 + T4d-4 (production + tests, including pre-T4d hotfix `f6084ee`): **~3500 / −300 across 50+ distinct files**

Component framework + QA: small additions (`WithApplyGate`, `WithEngineDrivenRecovery`, fault wraps `RecoveryMode()` forwarding)

---

## §D Review points — code spans worth architect's eye

### D-1. Engine ↔ storage 2-enum split with explicit boundary mapper — `core/engine/events.go`, `core/storage/recovery_contract.go`, `core/transport/recovery_session.go:35`

Architect-endorsed at T4d-1 round-46 review as "the right v0.3 boundary fix" (stronger than mini-plan v0.2's single shared enum approach). The architectural property: **engine has ZERO imports from storage** (verified by build at `1edeb36`). Maintenance discipline: any future `StorageRecoveryFailureKind` addition requires a paired `engine.RecoveryFailureKind` mapping in `transport.classifyRecoveryFailure`; `TestT4d1_StorageFailureKindMapper_AllKnownKinds` is the fence.

### D-2. Apply gate lane-pure API + transitional caller-side shim — `core/replication/apply_gate.go:120,142` + `core/transport/replica.go:13-18,156-165`

Round-46 architect rework. Gate exposes `ApplyRecovery` + `ApplyLive` lane-explicit methods; gate itself never inspects `lineage.TargetLSN`. The lane discrimination payload-sniffing was moved one layer up to the transport replica handler as a TRANSITIONAL CALLER-SIDE SHIM with explicit TODO citing `CARRY-T4D-LANE-CONTEXT-001`. The architectural fence (gate ≠ payload-sniffer) is durable; the transitional shim is named-carry-bound to **post-G5 protocol hardening** (G5 hardening backlog).

### D-3. Round-47 catch-up exhaustion DIRECTLY emits StartRebuild + RebuildPinned sticky override — `core/engine/apply.go:324,488,527,535,637`, `core/engine/state.go:105,120`

Round-47 architect ruling moved escalation from "set `Decision = Rebuild` + emit Degraded" to "directly emit StartRebuild command" (validating that the engine drives the rebuild, not just records intent). T4d-4 part C surfaced two real bugs in the implementation:

- **Bug #1**: exhaustion missed escalation when stray auto-probe re-classified `Decision` mid-flight → fix keys on `st.Session.Kind == SessionCatchUp` (truth = what session actually failed)
- **Bug #2**: WALRecycled escalation set `Decision = Rebuild` but never emitted StartRebuild, and could be downgraded by next probe → new `RebuildPinned bool` sticky override; cleared by successful rebuild SessionCompleted

The fixes are **structural corrections** (use the right truth source; make the decision sticky against transient probe noise), not bandaids. The fact that HARD GATE #3 surfaced them validates architect's round-47 ruling that the full L2 matrix is non-negotiable.

### D-4. Per-LBA stale-skip with coverage-advance — `core/replication/apply_gate.go` `ApplyRecovery`

Round-43/44 architect lock: recovery-stream stale entries are valid duplicates and must be skipped as data writes while still counted as recovery-stream coverage. Live-lane stale entries are abnormal and must not mutate data; they should be skipped/rejected and must not advance recovery coverage. The 2-map split (`liveTouched` + `recoveryCovered`) realizes this; Option C hybrid `appliedLSN[LBA]` source seeds from substrate `AppliedLSNs()` query.

### D-5. R+1 threading at engine — `core/engine/apply.go` (FromLSN populated as `Recovery.R + 1`)

G-1 §6.1 Option A architect ratification: engine populates `StartCatchUp.FromLSN = Recovery.R + 1` at command emit time; sender does NOT add `+1` (avoids double-add). The "+1 to skip already-applied LSN" semantic lives at engine, not transport. Pinned by `INV-REPL-CATCHUP-FROMLSN-IS-REPLICA-FLUSHED-PLUS-1` + `INV-REPL-CATCHUP-FROMLSN-FROM-ENGINE-STATE-NOT-PROBE`.

### D-6. G5-DECISION-001 obligation — engine `ReplicaState` JSON round-trip clean — `core/engine/g5_decision_001_serializability_test.go`

T4d-4 part B obligation per architect round-47 ruling: keep engine state structure decision-A-compatible (serializable) so G5 can choose Path A (persist) vs Path B (rebuild from probe after restart) without structural rewrite. **Test fence**: `TestG5Decision001_ReplicaState_RoundTripJSON` + `_ZeroValueStable` ensure no future struct change introduces unserializable types.

---

## §E Delivery summary

1. **2-enum boundary** (T4d-1) — `storage.StorageRecoveryFailureKind` + `engine.RecoveryFailureKind` + `transport.classifyRecoveryFailure` mapper. Engine zero-imports storage. Substring matching REMOVED.
2. **Option C hybrid `AppliedLSNs()`** (T4d-1) — substrate exposes per-LBA latest-applied LSN where tracked (walstore via dirty-map snapshot; smartwal via ring last-writer-wins reduction); BlockStore returns `ErrAppliedLSNsNotTracked` explicitly; gate falls back to in-memory session map.
3. **Lane-pure apply gate** (T4d-2 + `01f4ab9` + `a63ae9b`) — `ApplyRecovery` + `ApplyLive` lane-explicit; 2-map split; per-LBA stale-skip with coverage-advance on skip; live-lane fail-loud. Transitional caller-side TargetLSN shim carries via `CARRY-T4D-LANE-CONTEXT-001`.
4. **R+1 threading + boundary fences** (T4d-3) — engine emits `Recovery.R + 1`; substrate scans from R+1 not 1; 5 G-1 PORT items; 2 hidden invariants; Q3 import-discipline fence; CARRY-T4D-LANE-CONTEXT-001 Option B test-skip with explicit forward-binding.
5. **RecoveryMode() interface + Stop lifecycle** (T4d-4 part A) — substrates expose recovery mode via method (replaces duck-typed CheckpointLSN probe); `ReplicationVolume.Stop` BUG-005 non-repeat; `INV-REPL-LIFECYCLE-HANDLE-BORROWED-001`.
6. **Engine-driven recovery wiring + round-47** (T4d-4 part B) — `WithEngineDrivenRecovery()` becomes REAL; `ReplicationVolume↔adapter` wiring runs engine retry loop end-to-end; round-47 catch-up exhaustion DIRECTLY emits StartRebuild; rebuild MaxRetries=0 terminal; G5-DECISION-001 ReplicaState serializability obligation.
7. **Full L2 matrix + 2 engine bug fixes** (T4d-4 part C) — HARD CLOSE GATE #3 closed; 2 real bugs surfaced + fixed during integration testing (Bug #1 stale-Decision-key; Bug #2 RebuildPinned sticky override).

---

## §F Invariants pinned at T4d close

Inscribed in catalogue §3.3 with full pin-test lists. Entries upgrade to `✓ PORTED T4d` on QA single-sign + architect T-end three-sign.

| Invariant | Origin | Pin location | Batch |
|---|---|---|---|
| `INV-REPL-CATCHUP-WITHIN-RETENTION-001` *(Path A)* | architect Item B | T4c un-pin → T4d-2 apply gate + T4d-3 R+1 — un-pin condition satisfied by combined apply gate (correctness) + R+1 (efficiency) | T4d-2 + T4d-3 |
| `INV-REPL-NO-PER-LBA-DATA-REGRESSION` *(goal)* | round-43 lock | `apply_gate.go` `ApplyRecovery` + adversarial test suite | T4d-2 |
| `INV-REPL-RECOVERY-STALE-ENTRY-SKIP-PER-LBA` *(mechanism)* | round-43 lock | `apply_gate.go` `ApplyRecovery` per-LBA `appliedLSN` check | T4d-2 |
| `INV-REPL-RECOVERY-COVERAGE-ADVANCES-ON-SKIP` | round-44 refinement | `apply_gate.go` `ApplyRecovery` skip-path advances `recoveryCovered` | T4d-2 |
| `INV-REPL-LIVE-LANE-STALE-FAILS-LOUD` | round-44 refinement | `apply_gate.go` `ApplyLive` returns error on stale; never advances `recoveryCovered` | T4d-2 |
| `INV-REPL-RECOVERY-COVERAGE-RESTART-SAFE` | Option C hybrid | `apply_gate.go` `initSessionLocked` queries `store.AppliedLSNs()` at session start | T4d-2 |
| `INV-REPL-LANE-DERIVED-FROM-HANDLER-CONTEXT` | Q2 + round-46 re-emphasis | `apply_gate.go` lane-explicit methods + `replica.go` caller-side dispatch (TRANSITIONAL — CARRY-T4D-LANE-CONTEXT-001 active) | T4d-2 (gate API) + T4d-3 (transport caller) |
| `INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY` | Q1+Q3 + T4d-1 strengthening | `core/engine/` zero storage imports; `core/transport/import_discipline_test.go` source-grep fence | T4d-1 (engine purity) + T4d-3 (transport fence) |
| `INV-REPL-CATCHUP-FROMLSN-IS-REPLICA-FLUSHED-PLUS-1` | T4d-3 G-1 §5 | `core/engine/apply.go` engine emits `Recovery.R + 1`; `t4d3_rplus1_test.go` | T4d-3 |
| `INV-REPL-CATCHUP-FROMLSN-FROM-ENGINE-STATE-NOT-PROBE` | T4d-3 G-1 §5 | `core/engine/apply.go` reads engine state, not raw probe payload; `t4d3_rplus1_test.go` | T4d-3 |
| `INV-REPL-LIFECYCLE-HANDLE-BORROWED-001` | T4d-4 part A | `ReplicationVolume.Stop` doesn't close substrate handle; BUG-005 non-repeat regression test | T4d-4 part A |
| `INV-REPL-CATCHUP-EXHAUSTION-ESCALATES-TO-REBUILD` | round-47 architect addition | `core/engine/apply.go:567` exhaustion emits StartRebuild + pins RebuildPinned; `recovery_test.go:301+325` | T4d-4 part B+C |
| `INV-REPL-REBUILD-FAILURE-TERMINAL` | round-47 architect addition | rebuild MaxRetries=0; first failure → terminal degraded; `recovery_test.go` | T4d-4 part B |
| **`INV-REPL-FAILED-SESSION-KIND-DRIVES-ESCALATION`** *(part C Bug #1 capture)* | T4d-4 part C bug fix | `core/engine/apply.go` exhaustion check keys on `st.Session.Kind == SessionCatchUp` (truth = what session actually failed), NOT on the mutable `Decision` field; resists stray auto-probes that re-classify Decision mid-flight; `TestT4d4_FullL2_CatchupRetryExhausted_EscalatesToRebuild` | T4d-4 part C |
| **`INV-REPL-REBUILD-ESCALATION-STICKY-UNTIL-TERMINAL`** *(part C Bug #2 capture)* | T4d-4 part C bug fix | `RecoveryTruth.RebuildPinned` sticky bool set by both WALRecycled escalation and exhaustion; `decide()` honors as override forcing `Decision=Rebuild` regardless of R/S/H; cleared by successful rebuild SessionCompleted or identity reset; ensures next probe cannot downgrade rebuild before terminal; `TestT4d4_FullL2_WALRecycled_EscalatesImmediate` | T4d-4 part C |
| `LastSentMonotonic_AcrossRetries` *(full cross-call form)* | T4c QA #8 carry | `TestT4d4_FullL2_LastSentMonotonic_AcrossRetries` (T4d-4 part C) — closes T4c Stage-2 deferred scenario | T4d-4 part C |

**Total inscribed at T4d close: 16 invariants** (14 new + 1 promotion + 1 QA-deferred-now-closed). Of the 14 new, 2 specifically capture the part C bug fixes (`INV-REPL-FAILED-SESSION-KIND-DRIVES-ESCALATION` + `INV-REPL-REBUILD-ESCALATION-STICKY-UNTIL-TERMINAL`) per architect round-48 review.

**Active carries (named, post-G5 hardening backlog):**
- `CARRY-T4D-LANE-CONTEXT-001` — replace transport caller-side TargetLSN==1 shim with true handler/session-context lane signal (Option B test-skip in place); bind point: **post-G5 protocol hardening (G5 hardening backlog)**
- `G5-DECISION-001` — engine recovery state behavior across primary restart (Path A persist vs Path B rebuild-from-probe; T4d-4 part B kept structure serializable)

---

## §G Test evidence

| Layer | At HEAD `e642ae8` (T4d-4 part C) |
|---|---|
| T4d-1 unit (typed kind + AppliedLSNs) | 10 new + d6b1890 follow-up 2 HARD GATE | green |
| T4d-2 unit (apply gate) | 16 new + 1 un-skipped | green |
| T4d-2 follow-up rework (`01f4ab9`) | 2 regression tests | green |
| T4d-3 unit + L2 (R+1 + boundary fences) | 11 new (3 engine + 3 transport + 1 import-discipline + 4 component) | green |
| T4d-4 part A (RecoveryMode + Stop) | per part A | green |
| T4d-4 part B (engine wiring + round-47 + G5-DECISION-001) | 9 new + 1 updated; 2 G5-DECISION-001 tests | green |
| T4d-4 part C (full L2 + bug fixes) | 3 new HARD GATE #3 tests | green |
| **Cumulative T4d-relevant tests** | **~70 new tests across T4d batches** | **all green at HEAD** |
| `core/replication` package | — | green under -race on m01 |
| `core/replication/component` package | — | green under -race on m01 |
| `core/transport` package | — | green under -race on m01 |
| `core/engine` package | — | green under -race on m01 |
| `core/storage` + `core/storage/smartwal` | — | green under -race on m01 |
| Full V3 suite under -race on m01 | **22/22 packages green** (post-`a0be6d5` test-fixture fix) | T2 NVMe `TestT2A_ConcurrentQueueStress` race resolved at `a0be6d5` during T4d close cycle; verified ×50 stress + full nvme package PASS — see §H Finding #3 |

**Smoke runs at QA workspace HEAD + m01 -race:**

```
$ go test -race -count=20 -timeout 5m -run 'TestT4d4_FullL2_' ./core/replication/component/
ok  github.com/seaweedfs/seaweed-block/core/replication/component  12.523s

$ go test -race -count=1 -timeout 10m ./core/...
21 packages green; 1 pre-existing T2A NVMe race (separate bug)

# Post-`a0be6d5` re-run (2026-04-26):
$ CGO_ENABLED=1 go test -race -count=50 -run TestT2A_ConcurrentQueueStress ./core/frontend/nvme/
ok  github.com/seaweedfs/seaweed-block/core/frontend/nvme  7.015s
$ CGO_ENABLED=1 go test -race -count=1 ./core/frontend/nvme/
ok  github.com/seaweedfs/seaweed-block/core/frontend/nvme  22.237s

22/22 packages now genuinely green under -race on m01.
```

**Production-sized substrate smoke on m01 (1 GiB extent — see §H findings):**

| Substrate | Create 1 GiB | 20k writes (~78 MB) | ScanLBAs (1) full retention | AppliedLSNs |
|---|---|---|---|---|
| walstore (default 64 MiB WAL) | 11.5 ms | ⚠ WAL admission fires at write 14611 (~57 MiB) — see Finding #1 | n/a (test aborted) | n/a |
| smartwal (2 MiB ring) | 1.66 ms | 154.7 ms (~518 MB/s) | 211.9 ms (~94k entries/sec) | 20000 entries, max LSN=20000 |

**Per-batch substrate smoke regression check (T4d-1 baseline `1edeb36` vs current):**
- walstore: same WAL admission boundary at write ~14611 (no regression)
- smartwal: same throughput characteristics (78 MB write in ~129 ms; ScanLBAs ~204 ms; AppliedLSNs 20000 entries)
- Substrate behavior INVARIANT across T4d-1 → T4d-4-current (substrate code stable since T4d-1)

---

## §H Production-readiness findings (m01 1 GiB substrate smoke)

These findings are **not T4d implementation defects**. They are production-readiness gaps surfaced by realistic-config testing. Recording explicitly so they don't go silent into G5.

### Finding #1: walstore WAL admission backpressure observed under synthetic write-only workload

**Symptom**: `walstore` 1 GiB extent + default 64 MiB WAL + 20,000 sequential synchronous writes (no intermediate Sync) → WAL admission gate fires at write 14611 (~57 MiB consumed) with `storage: WAL admission: storage: WAL region full`.

**Honest scope**: m01 smoke ran a synthetic write-only workload with no observed checkpoint relief before the 64 MiB WAL filled. walstore HAS a background flusher (`walstore.go:189-190` `s.flusher = newFlusher(...); go s.flusher.run()`) and explicit WAL admission control (`walstore.go:335-341`); QA's earlier characterization "caller-driven, no background goroutine" was wrong (corrected per architect round-48 review). The actual question is whether the flusher's checkpoint cadence keeps up with the synthetic single-threaded loop's write rate, and what tuning policy production deployments need.

**Production implication**: blockvolume's lifecycle interaction with the walstore flusher needs verification under realistic production write pressure (not just synthetic stress). The T4d smoke did NOT verify this end-to-end.

**Carry**: G5 production-readiness — verify walstore flusher/checkpoint cadence under production write pressure; define tuning/operational policy. **Reword from earlier draft per architect round-48: the smoke surfaced "WAL admission pressure under synthetic workload," NOT "no background checkpoint exists."**

### Finding #2: no `--durable-walsize` CLI flag

**Symptom**: `cmd/blockvolume/main.go` exposes `--durable-blocks` + `--durable-blocksize` + `--durable-impl` but NOT `--durable-walsize`. walstore default 64 MiB WAL is hardcoded; smartwal default 2 MiB ring is hardcoded (`smartwal/store.go:19` `defaultWALSlots = 65536`).

**Production implication**: operators cannot tune WAL size at deployment without code change.

**Carry**: post-G5 / operator hardening — add `--durable-walsize` CLI flag (small change; ~10 LOC + plumb through to `newSuperblock(walSize)` opts).

### Finding #3: pre-existing `core/frontend/nvme/TestT2A_ConcurrentQueueStress` data race — ✅ RESOLVED at `a0be6d5`

**Original symptom**: Under `go test -race` on m01 Linux, `TestT2A_ConcurrentQueueStress` in `core/frontend/nvme` emitted `WARNING: DATA RACE` and FAILED.

**Verified pre-existing**: same failure at `1edeb36` (T4d-1 commit). NOT introduced by T4d-4. Belonged to T2 NVMe surface.

**Root cause** (diagnosed via -race stack traces on m01): `writeCountingBackend.lastWrite []byte` field at `core/frontend/nvme/t2_v2port_nvme_write_chunked_r2t_test.go:64` was unsynchronized; the test fixture's `Write` method assumed serial caller use ("NOT safe across concurrent writes, but the test drives them serially") but `TestT2A_ConcurrentQueueStress` violated that assumption by sharing one fixture across 8 concurrent IO queues. Stack confirmed race lives entirely in test-fixture code, NOT in production session model (which is correctly serialized: one rxLoop, one txLoop, per-Write goroutines for IO dispatch).

**Fix**: `a0be6d5` — `lastWrite []byte` → `atomic.Pointer[[]byte]` (QA's recommended Option A); 25+/8- diff, test fixture only, zero production code touched.

**m01 -race verification (post-fix)**:
- `count=10` round: PASS in 2.3s ✓
- `count=50` stress round: PASS in 7.0s ✓
- Full `core/frontend/nvme` package under -race: PASS in 22.2s ✓

**Status**: closed. T2A NVMe surface no longer races under -race on m01. Full V3 -race smoke now genuinely all-green (no remaining package failures).

### Finding #4: substrate behavior invariant across T4d batches

**Symptom**: per-batch substrate smoke shows identical results T4d-1 baseline (`1edeb36`) → current. Substrate code stable since T4d-1; T4d-2/T4d-3/T4d-4 didn't touch substrate behavior.

**Production implication**: positive — no substrate regressions during T4d. Confirms layering discipline (T4d work above substrate didn't accidentally couple back to substrate internals).

---

## §I Forward-carries surfaced during T4d

| Carry | Owner | Bind point | Origin |
|---|---|---|---|
| `CARRY-T4D-LANE-CONTEXT-001` — replace TargetLSN==1 caller shim with true handler/session-context lane signal | sw | **post-G5 protocol hardening (G5 hardening backlog)** | T4d-2 follow-up `01f4ab9` round-46 |
| `G5-DECISION-001` — engine recovery state across primary restart (Path A persist vs Path B rebuild-from-probe) | architect at G5 | G5 collective close | T4d-4 part B round-47 |
| walstore flusher/checkpoint cadence verification under production write pressure + tuning/operational policy | sw + architect | G5 production-readiness gate | §H Finding #1 (m01 production-sized smoke) |
| `--durable-walsize` CLI flag for operator tuning | sw | post-G5 / operator hardening | §H Finding #2 |
| ~~Pre-existing `TestT2A_ConcurrentQueueStress` data race~~ | ✅ RESOLVED `a0be6d5` | (closed during T4d close cycle; m01 -race ×50 PASS) | §H Finding #3 |
| m01 hardware first-light for replicated write path (multi-replica + real network conditions) | QA + sw | G5 collective close | T4c closure §H + kickoff §2.6 |
| Multi-replica concurrent live + recovery scenarios | QA | G5 production-readiness | T4d kickoff scope; not in T4d-4 |

---

## §J Sign requests at this batch close

T4d is **the final T4 batch**. Per §8C.1 + kickoff §4 #10, T-end three-sign lands at this close.

| Signer | Sign type | Asks |
|---|---|---|
| **QA** | §8C.2 single-sign at batch close | Confirm 14 invariants pinned per §F; confirm forward-carry checklist v0.2 all 19 active T4a/T4b/T4c invariants green at HEAD under -race on m01; confirm §H findings are honest about production-readiness gaps. **(this document is the QA single-sign artifact)** |
| **Architect** | §8C.1 T4 T-end three-sign | (1) Read review points D-1 through D-6; (2) accept §B acceptance bar fully met; (3) accept §H production-readiness findings as G5 carries (not T4d defects); (4) accept G5-DECISION-001 + CARRY-T4D-LANE-CONTEXT-001 as named carries; (5) acknowledge T4 T-end at this close (no T4e). |
| **sw** | confirmatory note (no §8C.2 sign required at batch level per kickoff §4 #10 rules) | Confirm all `INV-REPL-*` upgrades from `⏭ T4d-N` to `✓ PORTED T4d-N` in catalogue §3.3; confirm §I forward-carries with Owner=sw are recorded for post-G5/G5-readiness planning |

---

## §K Notes for future readers

- T4d is where the apply gate (lane-pure, per-LBA stale-skip + coverage-advance) and engine-driven recovery wiring (R+1 + retry loop end-to-end + rebuild path) became real. The combination closes the "production correctness boundary for replicated writes under recovery."
- **Two architectural pins surfaced + fixed in part C** (catch-up exhaustion stale-Decision-key bug; WALRecycled-no-StartRebuild-emit bug + RebuildPinned sticky override). Both surfaced because architect's round-47 ruling required full L2 matrix end-to-end testing — synthetic injection wouldn't have caught either. This validates the round-47 hard-gate decision empirically.
- **Three architectural improvements ratified during T4d** beyond the original kickoff scope: 2-enum split (T4d-1 round-46), lane-pure gate API (T4d-2 round-46), round-47 catch-up exhaustion DIRECTLY emits StartRebuild. Each was sw-proposed-or-discovered + architect-endorsed; recorded explicitly in §B so the precedent is visible.
- **Process rule inscribed**: G-1 docs land in seaweedfs FIRST; sw implementation references committed hash via `Refs G-1 sign: <sha>`. T4d-3 G-1 sign hash = `seaweedfs@80036404c`. This avoids the "pending hash" procedural gap surfaced at T4d-3.
- **Production-sized substrate smoke at m01** is the right discipline for batch close evidence — surfaces gaps that unit tests at fixture sizes cannot (walstore WAL admission boundary; CLI knob absence). Forward-carry to G5: include production-sized smoke per batch as standard evidence.
- The forward-carries in §I represent **honest production-readiness work** that didn't fit T4d's scope. G5 collective close should not narrow them silently — apply the same §B-style explicit acceptance discipline T4c established.
- **Round-48 architect review surfaced 3 blocking edits + 1 smaller fix**: (1) part C commit hash bound to `e642ae8` everywhere; (2) `CARRY-T4D-LANE-CONTEXT-001` bind point corrected from "T4e (preferred) or post-G5" to **"post-G5 protocol hardening (G5 hardening backlog)"** since "no T4e" + "T4e preferred" was contradictory if T-end signs at this close; (3) §H Finding #1 reworded to remove the false root cause about "caller-driven, no background goroutine" — walstore HAS `go s.flusher.run()` at `walstore.go:189-190`; (4) 2 named invariants added to §F capturing part C bug fixes (`INV-REPL-FAILED-SESSION-KIND-DRIVES-ESCALATION` + `INV-REPL-REBUILD-ESCALATION-STICKY-UNTIL-TERMINAL`) per architect's smaller-fix request. Total inscribed invariants 14 → 16. Recording the round-48 corrections explicitly here so the procedural diligence is visible.
