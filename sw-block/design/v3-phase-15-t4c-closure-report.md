# V3 Phase 15 T4c — Closure Report

**Date**: 2026-04-25
**Status**: DRAFT — awaiting QA single-sign batch close per §8C.2 + architect acceptance of scope delta (§B). T4 T-end three-sign per §8C.1 lands later, after T4d.
**Gate**: T4c batch (mid-T4) within G5 (replicated write path)
**Predecessor**: T4b QA-single-signed at batch close (durability closure)
**Successor**: T4d (next G5 batch — to be scoped by architect)

---

## §A Batch history

| Batch | Commit | Status | LOC (+/−) | Delivery |
|---|---|---|---|---|
| T4c kickoff (architect rounds 1–25) | doc-only | CLOSED | — | Item B (Path A) + Item C.3 (symmetric probe pair) signed; `v3-phase-15-t4c-recovery-protocol-design.md` rounds 26–34 |
| T4c-pre-A POC (smartwal) | `66495f9` | CLOSED | — | smartwal recovery scan capability validation |
| T4c-pre-A POC (walstore) | `c1584c6` | CLOSED | — | walstore wal_replay tier-1 mode validation (round 34) |
| T4c-pre-B | `1d76e65` | CLOSED | — | Unified `StartRecovery` command + per-kind `RecoveryRuntimePolicy` |
| **T4c-1** wire + validation | **`4dfe582`** | CLOSED | +887 / −54 across 18 files | Probe symmetric pair + `validateProbeLineage` discipline + transient probe sessionID (Option D) |
| **T4c-2** muscle + state machine | **`ff731dd`** | CLOSED | +1044 / −215 across 14 files | Catch-up sender muscle port + ScanLBAs seam + peer state machine + per-kind retry policy |
| **T4c-3** engine retry + muscle-level L2 | **`eb278ed`** | CLOSED | +516 / −16 across 5 files | `applySessionFailed` budget + sentinel escalation + **muscle-level L2** (4×2 scenarios; engine→adapter→executor end-to-end **not** integrated, see §B delta #3) |
| Component framework | `f94578f` (sw) | CLOSED | +788 across 4 files | Cluster harness + 7 demo scenarios + authoring guide |
| Component framework gap closures | `b1ee20b` (sw) | CLOSED | +678 / −7 across 4 files | `WithPrimaryStorageWrap`, `WithLiveShip`, fault wraps; `WithEngineDrivenRecovery` API-stub for T4d |
| **T4c QA Stage-1** | **`3926160`** | CLOSED | +174 / −11 (1 file) | 6 component scenarios / 11 matrix rows pinning 5 invariants |

All commits on `feature/sw-block` (seaweedfs) → `phase-15` (seaweed_block).

---

## §B Signed acceptance delta vs mini-plan §4

The mini-plan acceptance bar (`v3-phase-15-t4c-mini-plan.md` §4, signed at round 22) is partially deferred at this close. Each delta is recorded here as an explicit governance change, **not** a silent re-interpretation of the bar. Architect acceptance signature on this section is what authorizes the deltas.

| Mini-plan §4 criterion | Status at this close | Delta + carry |
|---|---|---|
| #1 All 3 batches merged | ✅ MET | T4c-1 `4dfe582` + T4c-2 `ff731dd` + T4c-3 `eb278ed` on `phase-15` |
| #2 Unit tests green per task | ✅ MET (with platform caveat) | QA workspace green at HEAD `3926160`; architect's Windows reproduction reports cleanup-only `TempDir RemoveAll Access denied` failures (assertions complete, file handles held on test exit). Tracked as cleanup-race forward-carry to next batch (§I); not a code blocker for T4c. See §G. |
| #3 **L2 subprocess matrix green: 5 scenarios × 2 substrates** | ⚠ **PARTIALLY MET — DELTA** | `core/replication/integration_t4c_test.go` delivers **4 scenarios × 2 substrates** at muscle-level scope. The file explicitly disclaims engine-driven end-to-end: *"Engine-driven recovery (decide → StartCatchUp emission → adapter dispatch → executor) is exercised at unit scope ... Full engine→adapter→executor→replica integration would require new ReplicationVolume↔adapter wiring."* The original mini-plan L2 sense (full engine→adapter→executor end-to-end through subprocess matrix) **is not met**; what is delivered is "muscle-level L2 + component evidence." Carry: full L2 binds at T4d alongside `WithEngineDrivenRecovery`. |
| #4 **L3 m01 integration scenario green** | ❌ **NOT MET — DELTA** | T4c is not validated on m01 hardware. Carry: m01 first-light for replicated write path binds at G5 batch close (collective sign across T4a/b/c/d). Rationale §H: the residual cross-call scenario depends on the same engine wiring deferred in #3, so m01 today would produce an artifact whose central assertion can't fire. |
| #5 T4a + T4b invariants forward-carry verified | ✅ MET (with platform caveat) | QA workspace: all `core/replication` package tests green at HEAD; no T4a/T4b pin regressions. Architect Windows reproduction has the same cleanup-only failures as #2 — tracked to next batch, not a regression. |
| #6 New T4c invariants queue ACTIVE | ✅ MET | See §F. Mini-plan listed 5 candidates; T4c yielded 8 inscribed (over-yield from G-1 §5) |
| #7 QA single-sign at T4c close per §8C.2 | pending | This document is the QA single-sign artifact |
| #8 No §8C.3 escalation triggers fired | ✅ MET | Two pre-merge findings (probe non-mutation, sessionID collision) fixed/reframed in same cycle — §K describes both as governance-as-intended, not §8C.3 escalations |

**Additional deltas surfaced post-mini-plan, not §4 rows but signed here:**

| Item | Status | Carry |
|---|---|---|
| Engine ↔ storage decoupling via stable substring `"WAL recycled"` (review point D-1) | ⚠ **TEMPORARY** | Acceptable as T4c decoupling. **Bound to T4d or G5 final sign**: replace with structured `RecoveryFailureKind` enum (or equivalent typed signal) before G5 final sign. QA scenario #2 (`RetryBudget_RecycledFailureSentinelStable`) is the drift fence in the interim. |
| **Catch-up `fromLSN` hardcoded to `1` instead of replica's `R+1`** (`core/transport/catchup_sender.go:131`; TODO at lines 125–129) | 🚫 **PRODUCT-SEMANTIC GAP — T4d BLOCKER** | The current sender always scans from LSN=1, not from the replica's flushed-LSN frontier. Failure mode: when checkpoint has advanced past 1 but the replica's R+1 is still within retention (the canonical "short disconnect within retention" case), `ScanLBAs(1)` returns `ErrWALRecycled` and the engine spuriously escalates to Rebuild — false-rebuilds the exact case Path A is supposed to close. **Consequence: `INV-REPL-CATCHUP-WITHIN-RETENTION-001` is not actually pinned by T4c**; it is downgraded to T4d carry (see §F + §I). The L2 `ShortDisconnect_DeltaOnly` test (architect-flagged) uses an empty replica, so it proves "ship full retained window to empty replica," not "reconnect within retention catches up via delta." Required for un-blocking: thread `replicaFlushedLSN` (R) through `StartCatchUp` (signature change + adapter wire + engine command schema bump) AND add a non-empty-replica short-gap test. Naturally lands alongside `WithEngineDrivenRecovery` in T4d. |

**What architect acceptance of this section signs**:
1. Criterion #3 sense narrowed from "L2 subprocess matrix" to "muscle-level L2 + component evidence"
2. Criterion #4 deferred from T4c-blocking to G5-blocking
3. Sentinel substring matching accepted as temporary, bound for replacement before G5 final sign
4. **`INV-REPL-CATCHUP-WITHIN-RETENTION-001` downgraded from active to T4d carry** — the `fromLSN` threading gap is a product-semantic gap, not a governance delta; the invariant is not pinned by T4c and is removed from §F (active) into §I (T4d blocker)
5. T4c batch close proceeds on QA single-sign per §8C.2 with the above four deltas explicitly recorded

---

## §C Changelist by file

### T4c-1 `4dfe582` — wire + validation

| File | LOC | Notes |
|---|---|---|
| `core/transport/protocol.go` | +123/−10 | `ProbeReq` 32B body + `ProbeResponse` 56B body; strict decode mirroring BarrierResp |
| `core/transport/replica.go` | +67 | `validateProbeLineage` (gate-without-advance) **paired with** existing `acceptMutationLineage` (gate-with-advance) |
| `core/transport/executor.go` | +95/−5 | Probe session minting; transient sessionID handling |
| `core/transport/probe_wire_test.go` | +206 NEW | 12 wire decode tests |
| `core/transport/probe_validation_test.go` | +226 NEW | 7 validation tests including non-mutation regression |
| `core/adapter/adapter.go` | +15/−4 | `prepareQueuedCommands` mints transient probe sessionID (Option D — engine stays pure) |
| `core/adapter/probe_session_test.go` | +118 NEW | 2 transient sessionID tests |
| `core/transport/failclosed_test.go` | +27/−18 | Stub-executor signature update for new sessionID param |
| 5 stub-executor callers (authority, host, ops, transport tests) | small | sessionID param threaded |

### T4c-2 `ff731dd` — muscle + state machine + storage seam

| File | LOC | Notes |
|---|---|---|
| `core/storage/recovery_contract.go` | +84 NEW | **Unified contract surface**: `RecoveryEntry` + `ErrWALRecycled` + `RecoveryMode` |
| `core/storage/logical_storage.go` | +27 | `ScanLBAs(fromLSN, fn) error` interface seam |
| `core/storage/store.go` | +78 | `BlockStore` adapter implements ScanLBAs |
| `core/storage/{smartwal,walstore}/*recovery*.go` | renamed + adapted | POC code promoted to production; smartwal emits `state_convergence`, walstore emits `wal_replay` |
| `core/transport/catchup_sender.go` | +153/−31 | **Muscle port**: `AllBlocks()` walk → `ScanLBAs(fromLSN, fn)` callback. Per-call deadline scope; lastSent monotonic; barrier-achieved-LSN gate |
| `core/transport/catchup_sender_test.go` | +265 NEW | 8 sender tests (6 G-1 invariants + 2 mode-label) |
| `core/replication/peer.go` | +108 | `ReplicaCatchingUp` + `ReplicaNeedsRebuild` states; `replicaStateTransitionAllowed` table |
| `core/replication/peer_state_machine_test.go` | +85 NEW | 4 state transition tests |
| `core/engine/apply.go` | +48 | `RecoveryRuntimePolicy.MaxRetries` per-kind defaults (3/0/1) |
| `core/engine/commands.go` | +17 | StartRecovery wiring extension |
| `core/engine/recovery_test.go` | +82 | 3 engine pin tests |

### T4c-3 `eb278ed` — engine retry + muscle-level L2

| File | LOC | Notes |
|---|---|---|
| `core/engine/apply.go` | +108/−5 | `applySessionFailed` retry budget + `isWALRecycledFailure` sentinel match (temporary; see §B delta + review point D-1) |
| `core/engine/state.go` | +20/−3 | Per-replica retry counter state |
| `core/engine/recovery_test.go` | +90 | Retry-loop unit tests including budget exhaustion |
| `core/replication/integration_t4c_test.go` | +301 NEW | **Muscle-level L2** matrix: 4 scenarios × 2 substrates (ShortDisconnect_DeltaOnly, GapExceedsRetention_EscalatesToNeedsRebuild, Probe_HappyPath_AcrossBothSubstrates, Catchup_RecoveryModeLabelSurfaced). The file's own header disclaims engine→adapter→executor end-to-end; that integration is a T4d carry (see §B delta #3). |
| `core/transport/catchup_sender.go` | +13/−5 | Wire glue for retry |

### Component framework (sw)

`f94578f` — `cluster.go` (+499) `cluster_test.go` (+198) `doc.go` (+52) `matrix.go` (+39)
`b1ee20b` — `cluster.go` (+187/−7), `faults.go` (+146 NEW), `faults_test.go` (+151 NEW), first-pass `qa_t4c_scenarios_test.go` (+194 NEW, round-39 trio)

### T4c QA Stage-1 `3926160`

`core/replication/component/qa_t4c_scenarios_test.go` (+174/−11) — adds round-40 trio (BarrierAchievedLSN, DeadlinePerCallScope, LastSentMonotonic_WithinCall) on top of round-39 trio bundled with `b1ee20b`.

### Cumulative T4c LOC

T4c-1 + T4c-2 + T4c-3 (production + tests): **+2447 / −285** across 37 distinct files.
Component framework + QA: **+1640 / −18**.

---

## §D Review points — code spans worth architect's eye

These are the spots where T4c made an architectural choice that doesn't live cleanly in a design memo. Reading the code is the fastest way to confirm or push back.

### D-1. Engine ↔ storage decoupling via sentinel **text**, not import — `core/engine/apply.go:573-595`

```go
// isWALRecycledFailure detects the substrate's ErrWALRecycled sentinel
// without taking a package dependency on storage. The
// `errors.Is`-friendly form is `... WAL recycled past requested LSN`;
// we match the "WAL recycled" infix to absorb both the wrap-formatted
// "smartwal: WAL slot recycled..." and the canonical text
// (`storage.ErrWALRecycled`) without taking a package dependency on
// storage from the engine.
```

The engine catches `ErrWALRecycled` and escalates `recovery.Decision = Rebuild` by **substring matching** the FailReason text — not by importing `storage.ErrWALRecycled` and using `errors.Is`. The reason: keeping `core/engine` storage-agnostic. The cost: the substring `"WAL recycled"` is now a **stable cross-package contract**; a rename in either place silently breaks escalation.

QA scenario #2 (`RetryBudget_RecycledFailureSentinelStable`) is the drift fence in the interim.

**Status: TEMPORARY (signed §B).** Stable-text-as-API is **not** the long-term engine↔storage contract. Bound for replacement by a structured `RecoveryFailureKind` enum (or equivalent typed signal) **before G5 final sign**. Replacement timing: T4d (preferred — alongside `WithEngineDrivenRecovery` wiring) or G5 final sign (latest).

### D-2. Probe non-mutation discipline — `core/transport/replica.go:238-285`

`validateProbeLineage` (line 238) and `acceptMutationLineage` (line 262) sit side-by-side. Both gate incoming RecoveryLineage; only one advances `r.activeLineage`. The split was discovered at PR review during T4c-1 — using `acceptMutation` for probes broke C5 calibration because the probe's monotonic sessionID raced ahead of an in-flight rebuild's lower sessionID, then rebuild frames at the lower sessionID got rejected as stale.

This generalizes: **any future non-mutating frame consumer** (status query, health check, observability probe) must use `validateProbeLineage` semantics. Inscribed as `INV-REPL-PROBE-NON-MUTATING-VALIDATION`. Architect should read both functions back-to-back to confirm the discipline reads correctly and the comment block at the call site is sufficient guidance for future authors.

### D-3. Catch-up sender muscle port + barrier-achieved-LSN gate — `core/transport/catchup_sender.go:79+` (`doCatchUp`)

The muscle port from V2's `AllBlocks()` walk to V3's `ScanLBAs(fromLSN, fn)` callback. Three architecturally interesting points inside this function:

- **Per-call deadline scoping** — deadline set at function entry, restored via `defer` at exit. The pin (G-1 §5) is that the deadline must NOT spill across calls into the wire's persistent state. QA scenario #5 (DeadlinePerCallScope_NoSpilling) exercises two back-to-back live-ship batches; in-wire form pinned, cross-orchestration form is a forward-carry to T4d.

- **`lastSent` monotonic update** — only advances within the scan loop, never resets. Pin (G-1 §5).

- **Barrier-achieved-LSN completion gate** — the session closes Success=true ONLY when `AchievedLSN == TargetLSN` after barrier ack. Partial progress (sender died mid-scan) closes Success=false with AchievedLSN reflecting actual progress. This guards against a silent "successful close at LSN=5" leaving the replica permanently behind. G-1 §4.2 binding `INV-REPL-CATCHUP-COMPLETION-FROM-BARRIER-ACHIEVED-LSN` — QA scenario #4 pins this.

LOC ratio V3/V2 ≈ 1.8× (153 added vs. ~85 in V2). PRESERVE concern N/A (heuristic flags **simplification** drift, not expansion); the +68 is callback boilerplate + per-error-class wrapping + recovery_mode label probe.

### D-4. Storage recovery contract — `core/storage/recovery_contract.go` (NEW, 84 lines)

The unified type surface:
- `RecoveryEntry` — what flows through the ScanLBAs callback
- `ErrWALRecycled` — the cross-package sentinel (see D-1)
- `RecoveryMode` — `wal_replay` (walstore, V2-faithful per-LSN) vs `state_convergence` (smartwal, per-LBA dedup)

Same `MsgShipEntry` wire on both substrates; the per-substrate scan emits its own dedup semantics under the same envelope. Architect should confirm the type shapes are stable across both substrates and don't leak substrate-specific fields.

### D-5. Peer state machine — `core/replication/peer.go:333+` (`replicaStateTransitionAllowed`)

Table-driven gate with new states `ReplicaCatchingUp` and `ReplicaNeedsRebuild`. Notable: **`NeedsRebuild` is terminal in T4c** (no transition out). T4d will add the rebuild-completion edge that exits NeedsRebuild back into a syncing state. Architect should read the transition table and confirm:
- All inbound edges to `NeedsRebuild` are correct (catch-up exhaustion, sentinel escalation)
- The terminal-in-T4c semantic is intentional and documented for the T4d author

### D-6. Engine retry-loop entry — `core/engine/apply.go:464+` (`applySessionFailed`)

The engine's response to a `SessionClosedFailed` event:
1. Increment per-replica retry counter
2. If `isWALRecycledFailure(FailReason)` → escalate `Decision=Rebuild` (see D-1)
3. Else if counter ≥ `MaxRetries` for this content kind → escalate
4. Else → emit a fresh `StartCatchUp` command with same target

The interlock with D-1 (sentinel detection) and D-3 (sender close semantics) is what makes catch-up self-driving inside the engine. Architect should read this with D-1 to confirm the matched-text infix actually catches both wrap formats (`storage: WAL recycled past requested LSN` AND `smartwal: WAL slot recycled past requested LSN`).

---

## §E Delivery summary

1. **Symmetric probe pair wire** (T4c-1) — `ProbeReq` 32B / `ProbeResponse` 56B with strict decode; transient probe sessionID minted by adapter (Option D — engine stays pure).
2. **Probe non-mutating validation** (T4c-1) — see D-2. Inscribed as `INV-REPL-PROBE-NON-MUTATING-VALIDATION`.
3. **Catch-up muscle port** (T4c-2) — see D-3. V2 muscle preserved per §0; V3 container is callback-driven.
4. **Storage interface seam** (T4c-2) — see D-4. `LogicalStorage.ScanLBAs` is the per-substrate scan; same wire, distinct dedup semantics.
5. **Peer state machine** (T4c-2) — see D-5. `NeedsRebuild` terminal in T4c.
6. **Engine retry loop** (T4c-3) — see D-1 + D-6. Per-kind `MaxRetries` (3 / 0 / 1).
7. **Component test framework** (sw `f94578f` + `b1ee20b`) — Cluster harness, 4 fault wraps (recycled, sever, observed, plus stub for engine-driven recovery).
8. **QA Stage-1 evidence** — 6 scenarios pinning 5 invariants. Stage-2 m01 deferred to T4d (justified §H).

---

## §F Invariants pinned at T4c close

Inscribed in catalogue §3.3 with full pin-test lists. Entries upgrade to `✓ PORTED T4c` on QA single-sign + architect acceptance of §B deltas (NOT T-end three-sign — that lands after T4d).

`INV-REPL-CATCHUP-WITHIN-RETENTION-001` *(Path A)* — **NOT pinned at T4c close.** Architect-flagged product-semantic gap (§B): catch-up sender hardcodes `ScanLBAs(1)` instead of replica's R+1, so the "delta within retention" claim is unproven and a checkpoint-past-1 + replica-within-retention case will spuriously escalate to Rebuild. Downgraded to T4d carry (§I).

| Invariant | Origin | Pin location |
|---|---|---|
| `INV-REPL-PROBE-NON-MUTATING-VALIDATION` | T4c-1 round-37 (sw-discovered) | `core/transport/replica.go:238` + T4c-1 unit tests + QA #1 |
| `INV-REPL-CATCHUP-LASTSENT-MONOTONIC` *(within-call)* | G-1 §5 | T4c-2 unit + QA #6 |
| `INV-REPL-CATCHUP-DEADLINE-PER-CALL-SCOPE` | G-1 §5 | T4c-2 unit + QA #5 (in-wire form) |
| `INV-REPL-CATCHUP-COMPLETION-FROM-BARRIER-ACHIEVED-LSN` | G-1 §4.2 binding | T4c-2 unit + QA #4 |
| `INV-REPL-CATCHUP-CALLBACK-RETURN-NIL-CONTINUES` | G-1 §5 | T4c-2 unit |
| `INV-REPL-RECOVERY-MODE-OBSERVABLE` | memo §5.1 + §13.0a | T4c-2 unit + QA #3 |
| Engine retry-budget escalation (no formal INV id yet) | T4c-3 forward-carry | `core/engine/apply.go:464` + T4c-3 L2 + QA #2 (sentinel drift fence) |

`INV-REPL-CATCHUP-FULL-TRANSFER-001` (Path B) remains queued; not active under architect's Path A signature.

**Active count at T4c close: 6 invariants** (down from the 8 originally drafted: WITHIN-RETENTION-001 downgraded to T4d carry per §B; DONE-MARKER-EMITTED removed because V2's `MsgCatchupDone` is collapsed into barrier-as-terminator per G-1 §4.2 — `core/transport/catchup_sender.go:48,187` — so the completion signal IS `COMPLETION-FROM-BARRIER-ACHIEVED-LSN`, not a separate marker). Mini-plan §4 #6 listed 5 candidate invariants; T4c yields 6 inscribed (still over-yield).

---

## §G Test evidence

| Layer | Tests added at T4c | At HEAD `3926160` |
|---|---|---|
| T4c-1 unit (`probe_wire_test.go` + `probe_validation_test.go` + `probe_session_test.go`) | 12 + 7 + 2 = 21 new + 1 rewritten | green |
| T4c-2 unit (`catchup_sender_test.go` + `peer_state_machine_test.go` + `recovery_test.go`) | 8 + 4 + 3 = 15 new | green |
| T4c-3 unit + muscle-level L2 (`recovery_test.go` + `integration_t4c_test.go`) | per `eb278ed`; **muscle-level only** (4×2), not full engine→adapter→executor — see §B delta #3 | green |
| Component framework demos (sw rounds 39–40) | 7 + 5 = 12 | green |
| **QA Stage-1** (`qa_t4c_scenarios_test.go`) | **6 scenarios / 11 matrix rows** | **all green** |
| `core/replication` package | — | green |
| `core/replication/component` package | — | green |

Cross-substrate (smartwal + walstore) coverage on QA: every scenario except #2 (`RetryBudget_RecycledFailureSentinelStable`, smartwal-only by design — wrap pattern doesn't need substrate variation for the sentinel-text pin) runs the matrix.

**Naming caveat (architect-flagged):** the test `TestT4c3_Catchup_ShortDisconnect_DeltaOnly` actually exercises **ship-full-retained-window-to-empty-replica**, not "short disconnect delta-only catch-up" — because the executor hardcodes `ScanLBAs(1)` (see §B product-semantic gap row). The test is correct as written for the muscle-level path it exercises; the **name** overclaims and will be renamed or annotated when WITHIN-RETENTION-001 properly pins at T4d.

**Smoke run at QA workspace HEAD `3926160`:**
```
$ go test -count=1 ./core/replication/component -run TestComponent_HappyCatchUp -v
=== RUN   TestComponent_HappyCatchUp/smartwal
=== RUN   TestComponent_HappyCatchUp/walstore
--- PASS: TestComponent_HappyCatchUp (0.04s)
ok  github.com/seaweedfs/seaweed-block/core/replication/component 0.056s

$ go test -count=1 ./core/replication/...
ok  github.com/seaweedfs/seaweed-block/core/replication           0.629s
ok  github.com/seaweedfs/seaweed-block/core/replication/component 0.665s
```

**Reproducibility note (Windows):** architect's review reproduction reported `TempDir RemoveAll Access denied` on `.smartwal` / `.walstore` files in his workspace; QA's Windows workspace passes cleanly. The assertions complete (test logic green) but file handles can remain held by goroutines on test exit on some Windows configurations. **This is a real reproducibility gap for the closure claim.** Carry: investigate whether substrate Close() is racing test cleanup; either fix the close ordering or document the platform-specific cleanup gotcha. Ownership: sw (next batch). Until investigated, "green at HEAD" should be read as "green at QA workspace; varies on Windows by environment."

---

## §H Stage-2 m01 hardware — honest deferral to T4d

Stage-2 m01 originally scoped three scenarios needing real-network timing. After sw closed framework gaps in `b1ee20b`, two landed cleanly at component scope:

- `BarrierAchievedLSN_PartialProgress` → QA #4 (component, both substrates)
- `DeadlinePerCallScope_NoSpilling` → QA #5 (component, both substrates, reframed to in-wire form after sessionID-collision finding — see §I forward-carry)

The single residual is `LastSentMonotonic_AcrossRetries` (full **cross-call** form). This requires the engine retry loop to drive multi-call catch-up with advancing `fromLSN` between calls. Today the framework's `CatchUpReplica` restarts at `fromLSN=1` every call because the engine retry loop is not wired through the adapter to the executor — that wiring is `WithEngineDrivenRecovery`, an API-stub today (`b1ee20b`), with binding scheduled for T4d.

**Driving this scenario on m01 hardware does not bypass the dependency**: the engine loop still doesn't advance `fromLSN` cross-call until T4d. Hardware timing changes nothing about which Go method drives the next call. Drafting an m01 script today would produce an artifact whose central assertion can't fire until T4d wiring lands; the honest scope is to carry this scenario forward to T4d's batch close.

What we do not lose by deferring: within-call lastSent monotonicity is pinned (QA #6); substrate emit-order discipline is pinned (T4c-2 unit + QA #6 observed-wrap).

What we explicitly do not claim: **T4c is not validated on m01 hardware.** First-light hardware exposure for the replicated write path remains a future deliverable; component-scope evidence is sufficient to close T4c against its own invariants but does not substitute for hardware-path confidence at G5 batch close.

---

## §I Forward-carries surfaced during T4c

| Carry | Owner | Bind point | Origin |
|---|---|---|---|
| `WithEngineDrivenRecovery` real wiring (engine retry-loop end-to-end through `ReplicationVolume`) | sw | T4d | Framework gap-closure round (`b1ee20b`) |
| Cross-orchestration sessionID coordination — live-ship and explicit catch-up coexisting on the same wire share `sessionID=1` and reject each other as stale | sw | T4d (alongside engine recovery) | Found by QA at round 40 authoring time when original `DeadlinePerCallScope` design (live-ship → catch-up → live-ship) failed with "reject stale ship session=1"; reframed scenario landed in-wire form |
| `LastSentMonotonic_AcrossRetries` full form scenario | QA | T4d batch close | Engine-loop dependency, see §H |
| **`INV-REPL-CATCHUP-WITHIN-RETENTION-001` (Path A "within-retention catches up via delta") — pin properly** | sw + QA | **T4d (BLOCKER for T4d close)** | Architect-flagged at T4c review (PM/lease review). Required: thread `replicaFlushedLSN` (R) through `StartCatchUp` (signature + adapter wire + engine command schema bump) so executor scans from R+1, not from 1; then add a non-empty-replica short-gap test where checkpoint has advanced past 1 and replica's R is within retention, asserting catch-up succeeds with delta-only ship (no false rebuild escalation). Naturally co-lands with `WithEngineDrivenRecovery`. Until pinned, the L2 `ShortDisconnect_DeltaOnly` test name is misleading (it tests "ship full retained window to empty replica"); QA will rename or annotate at T4d. |
| Substrate `RecoveryMode()` method — so wraps can forward the label without depending on duck-typed `CheckpointLSN` | sw | T4d (or any batch where wrap + accurate label both needed) | Documented limitation in `faults.go`; surfaced by sw in `b1ee20b` |
| m01 hardware first-light for replicated write path | QA + sw | G5 batch close (collective sign across T4a/b/c/d once all batches green) | T4c close-path direction from architect |
| Engine sentinel decoupling — replace substring `"WAL recycled"` match with structured `RecoveryFailureKind` enum (or equivalent typed signal) | sw | **T4d (preferred) or G5 final sign (latest)** | Signed §B as TEMPORARY; review point D-1 |
| Windows TempDir cleanup race — `.smartwal` / `.walstore` files held on test exit on some Windows configurations (architect's review reproduction failed where QA's passes) | sw | next batch | Surfaced at T4c review; tests themselves green (assertions complete); cleanup-only race. Investigate substrate Close() ordering vs `t.TempDir` RemoveAll. Until fixed, "green at HEAD" varies by Windows environment. |

---

## §J Sign requests at this batch close

T4c is a **mid-T4 batch close**, not a T-end sign. Per §8C.2 + mini-plan §4 #7, the close requires QA single-sign. The §B scope deltas additionally require architect acceptance signature. T4 T-end three-sign per §8C.1 lands later, after T4d.

| Signer | Sign type | Asks |
|---|---|---|
| **QA** | §8C.2 single-sign at batch close | confirm 6/6 component scenarios green at HEAD `3926160`; confirm muscle-level L2 (4×2) green at HEAD; confirm §B delta entries are honest about what is and is not delivered. **(this document is the QA single-sign artifact)** |
| **Architect** | acceptance of §B scope deltas | (1) accept criterion #3 narrowing (mini-plan "L2 subprocess matrix" → "muscle-level L2 + component evidence"); (2) accept criterion #4 deferral (L3 m01 → G5 batch close); (3) accept temporary substring sentinel matching with binding to T4d/G5; (4) read review points D-1 through D-6 and confirm or push back. **NOT a §8C.1 T-end three-sign — that lands after T4d.** |
| **sw** | confirmatory note (no sign required at batch close per §8C.2) | confirm `INV-REPL-CATCHUP-*` upgrades from `⊙ T4c` to `✓ PORTED T4c` in catalogue §3.3 are accurate; confirm §I forward-carries with Owner=sw are recorded for T4d planning |

---

## §K Notes for future readers

- T4c is the first batch where QA authored at component scope rather than waiting for L2 / L3. The component framework (sw rounds 39–40) is the new authoring surface; it parallels (does not replace) the L2 subprocess matrix and the L3 hardware harness.
- The §0 PRESERVE discipline held: catch-up sender is a faithful muscle port from V2 with V3 callback structure adapted to `ScanLBAs`. LOC ratio V3/V2 ≈ 1.8× explained entirely by callback boilerplate + per-error-class wrapping; PRESERVE concern N/A (the heuristic flags simplification drift, not expansion).
- The `recovery_mode` label is now part of the closed surface. Operators reading `recovery_mode=wal_replay` see V2-faithful per-LSN replay; `recovery_mode=state_convergence` sees per-LBA dedup. Same wire, distinct semantics, both observable.
- The two architectural pins discovered at PR/authoring time (probe non-mutation in T4c-1, sessionID collision at QA round 40) both surfaced before merge and got fixed/reframed in the same cycle. This is the §8C governance working as intended — pre-code G-1 reads + post-code review surfaces the kind of finding that doesn't live in a design memo.
