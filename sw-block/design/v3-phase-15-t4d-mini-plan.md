# V3 Phase 15 — T4d (Apply Gate + Engine-Driven Recovery + Substrate Hardening + Lifecycle) Mini-Plan

**Date**: 2026-04-25 (v0.3 — addresses v0.2 engine/storage dependency blocker)
**Status**: DRAFT v0.3 — re-submitted for QA + architect sign after v0.2 review (engine failure kind is engine-owned; no engine→storage dependency)
**Owner**: sw (implementation); QA (acceptance)
**Authority sources**:
- `v3-phase-15-t4d-kickoff.md` v0.3 (architect-ratified 2026-04-25 by pingqiu) — §1–8 ratified, §9 ratified Q1/Q2/Q3
- `v3-phase-15-t4c-closure-report.md` §I forward-carries
- Round-43 architect lock (replica recovery apply gate, lane-aware, per-LBA)
- Round-44 architect refinement (2-map split: liveTouched + recoveryCovered)
- §9 architect decisions: Q1 engine-owned retry; Q2 lane implicit from handler context; Q3 transport↔storage contract-only
**Discipline**: §8B.4 mid-T batch; §8C.2 QA single-sign per batch close
**Timeline**: ~3-4 weeks (T4d-2 apply gate is the largest batch; T4d-3 G-1 read serializes its code start)
**Predecessors**: T4c batch closed (`c910464a9`); BlockStore walHead pre-T4d hotfix landed (`f6084ee`); kickoff §1–8 + §9 RATIFIED

---

## §0 Why this is 4 batches, not 3 — and where V2 stops, V3 starts

T4d kickoff §3 ratified a 4-batch shape (was 3 in kickoff v0.1; v0.2 added substrate hardening as a separate batch after round-43 surfaced the BlockStore + walstore + smartwal vulnerability). Architect rationale (kickoff §3 sign):

> "This ordering is correct: remove brittle contracts first, then add the correctness gate, then optimize/wire end-to-end."

**Why 4 batches, ordered exactly this way:**

| New | Replaces / extends | Architect's reasoning |
|---|---|---|
| **T4d-1** Substrate hardening + structured failure kind | (new — round-43 surfaced; T4c closure §I structured-kind binding lands here) | "Lowest-risk decoupled work; closes the BlockStore production vulnerability quickly even if architect chooses bundle-not-hotfix; structured RecoveryFailureKind is a contract change other batches depend on." |
| **T4d-2** Replica recovery apply gate | (new — round-43 lock + round-44 refinement) | "The apply gate is the correctness boundary. R+1 is bandwidth optimization built ON the apply gate's correctness guarantee. Building R+1 first would mean shipping unsafe recovery in production for the duration of T4d-2 if T4d-3 lands first." |
| **T4d-3** R+1 threading + engine-driven recovery wiring | (T4c §I row 1 + row 3 carries; round-43 downgraded R+1 from correctness-blocker to bandwidth-optimization) | "T4d-3 needs G-1 because it touches the V2 catch-up muscle boundary and `runCatchUpTo(replicaFlushedLSN, targetLSN)` semantics." |
| **T4d-4** Lifecycle + integration | (BUG-005 lifecycle from sketch §4 row T4d + T4c §I rows 2/5/6/7) | Bundle the remaining T4c carries with the lifecycle item; integration matrix becomes meaningfully end-to-end once T4d-3 wiring is in. |

**§9 walshipper boundary — PORT vs V3-native (architect-ratified at kickoff Q1/Q2/Q3):**

T4d-3 is **NOT a wholesale V2 `WALShipper` port**. Per kickoff §9.2:

| Area | Classification | T4d-3 treatment |
|---|---|---|
| `runCatchUpTo(R, target)` start boundary | **PORT from V2** | Thread replica R through `StartCatchUp` → executor scans `R+1..target` |
| Per-call catch-up deadline | **PORT from V2** | Keep deadline scope discipline; do NOT leak to later live ships |
| Last-sent / achieved-LSN monotonicity | **PORT from V2** | "Advance only after successful frame write"; final completion via `BarrierResponse.AchievedLSN` |
| Catch-up retry budget | **SEMANTIC PORT, V3 location** | Preserve V2 `maxCatchupRetries=3` behavior via `RecoveryRuntimePolicy`; **retry bookkeeping lives in engine** (Q1) |
| WAL-recycled escalation | **SEMANTIC PORT, V3 location** | Preserve "retention miss → rebuild"; T4d-1 replaces string match with structured `RecoveryFailureKind` |
| Live ship during recovery | **V3 generalization** | V2 allowed live + recovery concurrent; V3 enforces via lineage + lane-aware apply gate |
| Unified catch-up/rebuild/LBA-rebuild protocol | **V3-native** | `StartRecovery` + `RecoveryContentKind` + runtime policy; no V2 1:1 |
| Replica stale-entry skip + coverage accounting | **V3-native** | T4d-2 apply gate; transport calls the gate; transport never queries substrate per-LBA freshness |
| Engine-driven recovery retry loop | **V3-native** (Q1) | Engine emits fresh recovery commands per policy; executor stays byte-movement-only |
| Package-boundary discipline (transport↔storage contract-only; no `core/recovery` move in T4d) | **V3-native** (Q3 / kickoff §9.5) | Inscribed via `INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY`; pinned at T4d-3. Package move deferred to post-G5/T4e per kickoff §9.3 #3 |

Architect-binding: kickoff §9.5 instructs the T4d-3 G-1 V2 read to explicitly separate PORT items (5 listed) from V3-native items (5 listed). G-1 deliverable.

---

## §1 Scope

T4d delivers **the production correctness boundary for replicated writes under recovery**: a lane-aware replica apply gate that prevents per-LBA data regression, structured failure semantics that decouple engine from substrate string-matching, V2-faithful catch-up start-boundary porting (R+1 threading), end-to-end engine-driven recovery wiring, and the lifecycle close that closes BUG-005 non-repeat.

### §1.1 Semantic whole

After T4d:

- **Recovery is correct under partial-failure interrupted replay**: replica apply gate enforces per-LBA stale-skip + coverage accounting; data does not regress on any over-scan / pinLSN-below-replicaR / repeated-window / live-races-recovery scenario
- **Recovery is bandwidth-efficient**: catch-up scans from `R+1`, not genesis (V2-faithful start boundary)
- **Recovery is fully engine-driven**: `ReplicationVolume↔adapter` wired; engine retry loop runs end-to-end; `WithEngineDrivenRecovery` framework primitive becomes real; full L2 matrix gains true engine-level coverage (T4c was muscle-level only)
- **Recovery failure semantics are structured**: `RecoveryFailureKind` enum replaces substring `"WAL recycled"` match; engine switches on typed kind
- **Substrates are stale-safe in depth**: BlockStore walHead guard already landed (`f6084ee`); walstore + smartwal substrate-level stale-protection where practical
- **Lifecycle is regression-safe**: `ReplicationVolume.Stop` + cross-orchestration sessionID coordination + Windows TempDir cleanup race fixed; BUG-005 non-repeat sealed

### §1.2 What's NOT in T4d (explicit deferrals per kickoff §2.6 + §9.3)

- **m01 first-light for replicated write path** — G5 collective close
- **Package move `core/transport` → `core/recovery`** — post-G5/T4e refactor (§9.3 #3)
- **Lane byte on `MsgShipEntry` wire** — Q2 ratified: lane is implicit from handler context; do NOT add wire field in T4d (§9.3 #2)
- **Substrate-native per-LBA applied-LSN exposure for ALL substrates** — Option C hybrid means add where practical; remaining substrates use in-memory session map seeded by §2.5 #1 architect ratification

### §1.3 Lane discipline (Q2 architect-ratified, kickoff §9.3 #2)

`MsgShipEntry` does NOT carry a lane byte in T4d. Lane is derived from the receiving handler / session context:

| Handler | Lane | Apply path |
|---|---|---|
| Live-ship handler (steady-state Ship) | live | Existing lineage/session/live-order rules; stale → reject + diagnostic; do NOT advance `recoveryCovered` |
| Catch-up / rebuild handler | recovery | Apply gate: `if entry.LSN <= appliedLSN[LBA] → skip data + recoveryCovered[LBA]=true; else apply + appliedLSN[LBA]=entry.LSN` |

`INV-REPL-LANE-DERIVED-FROM-HANDLER-CONTEXT` pins this at T4d-2 / T4d-3.

### §1.4 Location

- Production code → `seaweed_block` repo, branch `phase-15`
- Reference docs → `seaweedfs` repo, `sw-block/design/`
- Tests → `seaweed_block` alongside code; component scenarios in `core/replication/component/`
- L3 m01 — out of T4d (G5 collective)

---

## §2 Tasks (4 batches; ordered; each self-contained, reviewable, mergeable)

### T4d-1 — Substrate hardening + structured recovery failure kind end-to-end (~170 production + ~100 tests; v0.2/v0.3 revised upward from v0.1 to absorb adapter + engine event-struct contract additions)

**Where**:
- `core/storage/recovery_contract.go` — new substrate-facing `StorageRecoveryFailureKind` enum + `RecoveryFailure` typed-error struct + wrap helpers for `ScanLBAs` errors
- `core/storage/walstore_recovery.go` — wrap `ErrWALRecycled` returns with `RecoveryFailure{Kind: WALRecycled, ...}`; add per-LBA stale-skip in `ApplyEntry` body where practical (defense-in-depth, kickoff §2.5 #3 caveat)
- `core/storage/smartwal/recovery_scan.go` — same wrap + same defense-in-depth caveat
- `core/storage/store.go` — BlockStore.ApplyEntry already gated by hotfix `f6084ee`; verify + reference here
- **`core/engine/events.go`** — define engine-owned `RecoveryFailureKind` enum and add `FailureKind RecoveryFailureKind` field to `SessionClosedFailed`; engine MUST NOT import `core/storage`
- **`core/adapter/types.go`** — add `FailureKind engine.RecoveryFailureKind` field to `SessionCloseResult` (architect HIGH v0.1 #1 + v0.2 dependency fix: end-to-end typed kind, NOT hidden in FailReason text)
- `core/engine/apply.go` — `applySessionFailed` switches on `e.FailureKind == RecoveryFailureWALRecycled` (engine-owned typed branch); `containsAny`/substring match REMOVED. `Reason` field stays as diagnostic text only
- `core/transport/catchup_sender.go` — extracts substrate-returned `storage.RecoveryFailure.Kind` via `errors.As`, maps it to `engine.RecoveryFailureKind`, and populates `SessionCloseResult.FailureKind`
- `core/replication/volume.go` (and any close-callback wiring) — propagates `FailureKind` through the close path; doesn't re-derive from text
- Adapter normalization (`OnSessionClose` → engine event): copies `FailureKind` from `SessionCloseResult` to `SessionClosedFailed`
- Existing engine test `TestSessionFailed_WALRecycled_EscalatesToRebuild` — rewritten to construct event with `FailureKind: RecoveryFailureWALRecycled` and assert engine branches on the typed field

**Why** (kickoff §2.4 / §3 / §4 #7): T4c accepted substring matching as TEMPORARY; the binding was "T4d (preferred) or G5 final sign (latest)." Engine→storage decoupling is a contract obligation. Plus substrate hardening per kickoff §2.3 closes the round-43-surfaced defense-in-depth gap that the apply gate (T4d-2) covers as primary.

**Typed contract end-to-end (architect HIGH v0.1 #1 + v0.2 dependency fix)** — failure kind is a typed field on every plumbing hop, NOT parsed from text. Storage may classify substrate errors, but engine owns the decision vocabulary it consumes:

```go
// core/storage/recovery_contract.go
type StorageRecoveryFailureKind int

const (
    StorageRecoveryFailureUnknown StorageRecoveryFailureKind = iota
    StorageRecoveryFailureWALRecycled             // tier-class change; adapter maps to engine rebuild escalation
    StorageRecoveryFailureSubstrate               // substrate IO error; retryable unless caller says otherwise
)

type RecoveryFailure struct {
    Kind   StorageRecoveryFailureKind
    Cause  error  // wrapped underlying error for diagnostics
    Detail string // free-form additional context
}

func (f *RecoveryFailure) Error() string { ... }
func (f *RecoveryFailure) Unwrap() error { return f.Cause }
// errors.Is / errors.As let transport identify the storage-side kind.

// core/engine/events.go
type RecoveryFailureKind int

const (
    RecoveryFailureUnknown RecoveryFailureKind = iota
    RecoveryFailureWALRecycled       // tier-class change; escalate to rebuild
    RecoveryFailureTransport         // stream/connection error; retryable
    RecoveryFailureSubstrate         // substrate IO error; retryable
    RecoveryFailureTargetNotReached  // catch-up didn't reach targetLSN; retryable
    RecoveryFailureSessionInvalidated
)

// core/adapter/types.go (modify existing SessionCloseResult)
type SessionCloseResult struct {
    ReplicaID    string
    SessionID    uint64
    Success      bool
    AchievedLSN  uint64
    FailureKind  engine.RecoveryFailureKind  // NEW v0.3; engine-owned, no engine→storage dependency
    FailReason   string                      // DIAGNOSTIC TEXT ONLY — engine MUST NOT parse
}

// core/engine/events.go (modify existing SessionClosedFailed)
type SessionClosedFailed struct {
    ReplicaID    string
    SessionID    uint64
    FailureKind  RecoveryFailureKind  // engine-owned; engine does not import storage
    Reason       string               // DIAGNOSTIC TEXT ONLY — engine MUST NOT parse
}
```

Engine `applySessionFailed` branches on `e.FailureKind`:

```go
// core/engine/apply.go (replace existing substring-match block)
if e.FailureKind == RecoveryFailureWALRecycled {
    st.Recovery.Decision = DecisionRebuild
    st.Recovery.DecisionReason = "wal_recycled"
    st.Recovery.Attempts = 0
    // ... existing recycle escalation path
}
// substring `containsAny(reason, ["WAL recycled", ...])` match REMOVED
```

**Acceptance fence**: `TestEngine_SessionFailed_NoMoreSubstringMatch` greps `core/engine/apply.go` for `"WAL recycled"` substring usage and fails if present. The substring helper `containsAny` may stay if other engine code uses it; if not, removed too.

**Cross-package contract note (v0.3 dependency fix)**: `core/engine` MUST NOT import `core/storage`. Storage owns substrate-side error classification; transport/adapter maps that classification into `engine.RecoveryFailureKind`. `core/adapter` already imports `core/engine`, so `adapter.SessionCloseResult.FailureKind engine.RecoveryFailureKind` does not create a new package direction. This keeps the dependency line: storage error → transport mapping → adapter close result → engine event.

**Substrate defense-in-depth (architect §2.5 #3 caveat)**:

| Substrate | Action | Practical-scope decision |
|---|---|---|
| BlockStore | Already done — `f6084ee` walHead gate | No further action |
| walstore | Add per-LBA stale-skip in `ApplyEntry` body BEFORE `dm.put` (read existing `dm.get(lba)`, compare LSN, skip if `incoming <= existing`) | Sw judgment at impl time per §2.5 #3 caveat. If `dm.get` exposes per-LBA LSN cleanly (it does — the dirty entry carries `lsn`), include. Else: documented carry to T4e. |
| smartwal | Add per-LBA stale-skip in `ApplyEntry` body BEFORE `writeAt` extent overwrite. smartwal's per-LBA LSN tracking lives in the ring; need a fast-path read or session map | Sw judgment per §2.5 #3 caveat. Likely needs a small `lastAppliedLSN[lba]` in-memory map maintained by `ApplyEntry` itself. Inclusion contingent on not turning T4d-1 into a substrate refactor (kickoff §2.5 #3 verbatim). |

**Tests** (10 required):

- `TestStorageRecoveryFailureKind_Walstore_RecycleWrappedAsKind` — walstore `ScanLBAs` ErrWALRecycled wraps as storage-side `StorageRecoveryFailureWALRecycled` kind
- `TestStorageRecoveryFailureKind_Smartwal_RecycleWrappedAsKind` — smartwal same
- `TestTransportRecoveryFailureKind_MapsStorageRecycleToEngineKind` — transport maps `StorageRecoveryFailureWALRecycled` to `engine.RecoveryFailureWALRecycled`
- `TestTransportRecoveryFailureKind_TransportError_WrappedAsEngineKind` — non-recycle stream error surfaces as `engine.RecoveryFailureTransport`
- `TestEngine_SessionFailed_StructuredKind_RecycleEscalatesToRebuild` — engine maps typed `FailureKind` field, not substring (replaces existing `TestSessionFailed_WALRecycled_EscalatesToRebuild`)
- `TestEngine_SessionFailed_StructuredKind_NonRecycleRetries` — engine retry path uses `FailureKind` match
- `TestEngine_SessionFailed_NoMoreSubstringMatch` — fence: greps `core/engine/apply.go` for `"WAL recycled"` substring; fails if present (architect HIGH v0.1 #1 — engine MUST NOT parse text)
- `TestAdapter_SessionCloseResult_TypedKindEndToEnd` — close result built with `FailureKind` set; adapter normalization to engine event preserves the typed field
- `TestEngine_SessionClosedFailed_TypedKindFieldShape` — event struct compile fence: `FailureKind` field present + non-zero kind value triggers correct engine branch without importing storage
- `TestStorageWalstore_ApplyEntry_StaleLSN_SubstrateSkips` (if walstore stale-skip included)
- `TestStorageSmartwal_ApplyEntry_StaleLSN_SubstrateSkips` (if smartwal stale-skip included)

**Inscribed invariants**:
- Update `INV-REPL-CATCHUP-RECYCLE-ESCALATES` (catalogue §3.3) — pinning method changes from substring to structured kind

**Preserve**:
- All T4c invariants forward-carry green (per kickoff §4 #5)
- `applySessionFailed` non-recycle behavior (retry-loop, start-timeout exception) unchanged

**No G-1 required** (architect kickoff §3 sign #4): "T4d-1 is V3-native + small substrate edits; no V2 muscle source."

---

### T4d-2 — Replica recovery apply gate (lane-aware, per-LBA, 2-map split) (~250 production + ~250 tests)

**Where**:
- `core/replication/apply_gate.go` — new file: `ReplicaApplyGate` component with `ApplyRecovery(...)` and `ApplyLive(...)` entry methods; per-session state (`liveTouched`, `recoveryCovered`, `appliedLSN`)
- `core/replication/apply_gate_session.go` — per-session state machine (initialized at session start, cleared at session close); seeds `appliedLSN` from substrate query path (Option C hybrid per kickoff §2.5 #1)
- `core/transport/replica.go` — `MsgShipEntry` handler dispatches to gate via lane discriminator (handler context, NOT wire byte per Q2)
- Adapter wiring (whichever entry registers session start with the gate)
- Existing skipped adversarial tests in `core/replication/component/stale_entry_adversarial_test.go` — un-skip + extend per round-44 (coverage-advance + live-fails-loud)
- New component scenarios per round-44 spec
- Substrate query path for Option C hybrid: small additive method on `LogicalStorage` returning per-LBA latest-applied-LSN if known (fallback: 0 = "ask the gate's session map")

**Why** (kickoff §2.2 round-43 lock + round-44 refinement): primary correctness boundary. Without this, walstore's per-LSN scan + replica's blind apply produces per-LBA data regression on partial-failure interrupted replay.

**Architect-locked spec (round-43 verbatim)**:

> Recovery-stream stale entries are valid duplicates and must be skipped as data writes while still counted as recovery-stream coverage. Live-lane stale entries are abnormal and must not mutate data; they should be skipped/rejected under lineage/order diagnostics and must not advance recovery coverage.

**Gate behavior** (lane-aware):

```
ApplyRecovery(replicaID, sessionID, entry):
    require: session exists + matches lineage
    if entry.LSN <= appliedLSN[entry.LBA]:
        recoveryCovered[entry.LBA] = true
        return  // skip data; coverage advances
    substrate.ApplyEntry(entry.LBA, entry.Data, entry.LSN)
    appliedLSN[entry.LBA] = entry.LSN
    recoveryCovered[entry.LBA] = true
    return

ApplyLive(replicaID, sessionID, entry):
    require: session exists + matches lineage
    if entry.LSN <= appliedLSN[entry.LBA]:
        log diagnostic "live stale" + reject  // round-44 fail-loud
        return error
    substrate.ApplyEntry(entry.LBA, entry.Data, entry.LSN)
    appliedLSN[entry.LBA] = entry.LSN
    liveTouched[entry.LBA] = true
    // do NOT touch recoveryCovered
    return
```

**`appliedLSN` source — Option C hybrid (architect §2.5 #1 ratified)**:

At session start, gate queries substrate for per-LBA latest applied LSN:

```go
type LogicalStorage interface {
    // ... existing methods ...
    // T4d-2 addition (Option C hybrid):
    // Returns per-LBA latest-applied LSN where the substrate tracks
    // it. Implementations that don't track this MUST return
    // (nil, ErrAppliedLSNsNotTracked). The gate then falls back to
    // in-memory session-only tracking, seeded from live + recovery
    // applies during the session.
    AppliedLSNs() (map[uint32]uint64, error)
}

// New sentinel in core/storage/recovery_contract.go
var ErrAppliedLSNsNotTracked = errors.New("storage: substrate does not track per-LBA applied LSN")
```

**Per-substrate implementation (architect MED v0.1 #2 fix — every impl explicit, none "not implemented"):**

| Substrate | `AppliedLSNs()` impl | Source |
|---|---|---|
| **walstore** | Iterate `dm.snapshot()`; for each entry build `map[uint32]uint64{lba: entry.lsn}` | dirty map already carries per-LBA LSN |
| **smartwal** | `ring.scanValid()` then per-LBA last-writer-wins reduction (mirrors smartwal's existing recovery semantic) | Already implemented for `ScanLBAs`; reuse |
| **BlockStore** | `func (s *BlockStore) AppliedLSNs() (map[uint32]uint64, error) { return nil, ErrAppliedLSNsNotTracked }` — explicit not-tracked return; ensures interface compatibility | In-memory; no per-LBA LSN tracking |

Gate behavior on `ErrAppliedLSNsNotTracked`: log INFO once at session start, fall back to session-only `appliedLSN` map, seed from live + recovery applies as they arrive. Restart-safety degrades for unsupported substrates (BlockStore is calibration-only, so this is acceptable).

**Tests** (~14 required):

Adversarial set (un-skipped + extended from T4c skip-pile):
- `TestComponent_Adversarial_StaleEntryDoesNotRegress` — un-skipped; same LBA old/new + sever before new → no regression (architect round-41 set)
- `TestComponent_RepeatedRecoveryWindow_ByteAndLSNStable` — repeated window → byte state + per-LBA LSN stable
- `TestComponent_LiveRacesRecoveryOld_LiveWins` — live write at fresher LSN beats recovery's stale entry
- `TestComponent_RecoveryStaleSkip_CoverageStillAdvances` — round-44 #1: data skipped but `recoveryCovered[LBA]` updates
- `TestComponent_LiveLaneStaleEntry_FailsLoud` — round-44 #2: live lane stale → reject + diagnostic, do NOT advance recoveryCovered

Lane-discrimination set:
- `TestApplyGate_RecoveryHandler_DispatchesToRecoveryPath` — handler-context routing
- `TestApplyGate_LiveHandler_DispatchesToLivePath` — handler-context routing
- `TestApplyGate_LaneNeverDerivedFromWireField` — fence: no lane byte on MsgShipEntry; lane comes from handler context only

Option C hybrid set:
- `TestApplyGate_SessionStart_SeedsFromSubstrateAppliedLSN_Walstore` — walstore exposes; gate seeds correctly
- `TestApplyGate_SessionStart_SeedsFromSubstrateAppliedLSN_Smartwal` — smartwal exposes; gate seeds correctly
- `TestApplyGate_SessionStart_BlockStoreFallback_InMemoryOnly` — BlockStore returns ErrNotTracked; gate falls back

Restart-safety set (kickoff §4 #6 promotion):
- `TestApplyGate_RestartMidRecovery_SessionRestartReseeds` — kill session mid-recovery; new session re-queries substrate; appliedLSN consistent with substrate truth (pins `INV-REPL-RECOVERY-COVERAGE-RESTART-SAFE`)

Coverage-completion set:
- `TestApplyGate_BarrierCompletion_BarrierAuthoritativeWithCoverageContribution` — barrier ack is the authoritative completion truth (T4c-2 `INV-REPL-CATCHUP-COMPLETION-FROM-BARRIER-ACHIEVED-LSN` preserved); the gate's `recoveryCovered` map contributes to per-LBA coverage accounting that feeds barrier judgment, but does NOT replace the barrier as completion source. (Rename per QA v0.1 review issue #1; original name `TestApplyGate_BarrierAchievedLSN_DerivesFromCoverageNotData` could misread as conflicting with T4c-2 invariant.)

**Inscribed invariants** (catalogue §3.3 — already pre-inscribed by QA per round-44 prep work; T4d-2 promotes to PORTED):
- `INV-REPL-NO-PER-LBA-DATA-REGRESSION` (goal-level, round-43)
- `INV-REPL-RECOVERY-STALE-ENTRY-SKIP-PER-LBA` (mechanism, round-43)
- `INV-REPL-RECOVERY-COVERAGE-ADVANCES-ON-SKIP` (round-44)
- `INV-REPL-LIVE-LANE-STALE-FAILS-LOUD` (round-44)
- `INV-REPL-RECOVERY-COVERAGE-RESTART-SAFE` (Option C makes pinnable)
- `INV-REPL-LANE-DERIVED-FROM-HANDLER-CONTEXT` (Q2)

**Preserve**:
- T4c apply path for non-stale entries unchanged (gate forwards to substrate `ApplyEntry`)
- Existing T4c bitmap/coverage logic IF any (none — T4d-2 introduces it)
- `INV-REPL-CATCHUP-COMPLETION-FROM-BARRIER-ACHIEVED-LSN` (T4c-2): completion still derives from barrier ack; gate accounting feeds barrier judgment, doesn't replace it

**No G-1 required** (architect kickoff §3 sign #4): "T4d-2 does not need G-1; its spec is the round-43/44 V3-native apply-gate rule." Round-43 + round-44 architect text IS the spec.

**Estimated LOC**: ~250 production (~150 gate + state + ~50 substrate AppliedLSNs impls + ~50 wiring) + ~250 tests (high test-to-production ratio because the gate is the correctness boundary; test density matters).

---

### T4d-3 — R+1 threading + engine-driven recovery wiring — **G-1 PRE-CODE REQUIRED** (~250 production + ~150 tests)

**Where**:
- `core/transport/catchup_sender.go` — `StartCatchUp` body: `ScanLBAs(R+1, ...)` instead of `ScanLBAs(1, ...)`
- `core/transport/executor.go` — `BlockExecutor.StartCatchUp` signature gains `fromLSN uint64` parameter
- `core/adapter/executor.go` — `CommandExecutor.StartCatchUp` interface signature update; all test executor stubs updated
- `core/engine/commands.go` — `engine.StartCatchUp` command struct gains `FromLSN uint64`
- `core/engine/apply.go` — emission populates `FromLSN` from `Recovery.R`; retry-loop re-emit also populates
- `core/replication/volume.go` — wire `ReplicationVolume↔adapter` so engine retry loop runs end-to-end
- `core/replication/component/cluster.go` — `WithEngineDrivenRecovery()` becomes real (was stub at T4c)
- L2 integration matrix — gain true engine-level coverage

**Why** (kickoff §2.4 + §3 + §9.5):
- R+1 threading: kickoff §2.4 + T4c §I row 3 — bandwidth optimization (post-round-43; was correctness-blocker pre-round-43, now the apply gate handles correctness regardless of where primary scans from). Pins `INV-REPL-CATCHUP-WITHIN-RETENTION-001` un-pin from T4c.
- Engine-driven recovery wiring: kickoff §2.4 + T4c §I row 1 — closes the integration-scope gap left at T4c (engine retry loop unit-tested only).
- Q1 architect lock: retry loop is engine-owned, not transport. T4d-3 does NOT add a self-driven retry loop in `catchup_sender.go`.

**§9.5 architect-binding split** (T4d-3 G-1 must explicitly separate):

PORT (5 items from V2):
1. `runCatchUpTo(replicaFlushedLSN, targetLSN)` start-boundary semantics
2. Per-call deadline scope (do not leak to live ships)
3. Last-sent monotonicity ("advance only after successful frame write")
4. Retry count semantics (`maxCatchupRetries=3` envelope; bookkeeping in engine per Q1)
5. Retention-miss escalation ("WAL recycled" → rebuild; via T4d-1 structured kind)

V3-native (5 items, do not look for V2 source):
1. Engine-owned retry loop (Q1 — engine decides emit/escalate; transport stays byte-movement-only)
2. Unified recovery command model (`StartRecovery` / `RecoveryContentKind`)
3. Lane-aware apply gate integration (T4d-3 calls into T4d-2 gate; transport never queries substrate per-LBA freshness per Q3)
4. `BarrierResponse.AchievedLSN` as completion truth (T4c-2 `INV-REPL-CATCHUP-COMPLETION-FROM-BARRIER-ACHIEVED-LSN`)
5. Package-boundary discipline (transport↔storage contract-only per `INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY`)

**G-1 V2 read deliverable** — `sw-block/design/v3-phase-15-t4d-3-g1-v2-read.md` (sw produces; QA reviews + signs before T4d-3 code starts).

Deliverable structure:
1. V2 source verbatim — `weed/storage/blockvol/wal_shipper.go:845` `runCatchUpTo` body + calling context
2. Per-V2-concern → V3-location map (5 PORT items + their V3 placement)
3. V3-native items list (5 items, named explicitly so reviewer doesn't expect V2 source)
4. Hidden-invariant audit (anticipated 2-3 invariants)
5. Per-substrate behavior under R+1 threading (smartwal R+1, walstore R+1, BlockStore R+1)
6. LOC heuristic + V3 vs V2 expansion budget (per `feedback_g1_pre_code_review.md` LOC asymmetry note)
7. Test parity matrix (V2 mirror + V3-only)
8. Open placement decisions (if any)
9. QA sign block

**Tests** (~10 required):

R+1 threading set:
- `TestT4d3_CatchUp_ScansFromReplicaR_NotGenesis` — replica at R=50, primary at H=100; sender ScanLBAs called with `fromLSN=51`, NOT `fromLSN=1`
- `TestT4d3_CatchUp_NonEmptyReplica_ShortGap_BandwidthBounded` — pins `INV-REPL-CATCHUP-WITHIN-RETENTION-001` un-pin; over-scan reduction observable via emit count (use `NewObservedScanWrap` from component framework)
- `TestT4d3_CatchUp_StartCatchUpSignature_FromLSNRequired` — sender rejects calls without explicit `fromLSN`

Engine-driven recovery set:
- `TestT4d3_EngineRetryLoop_E2E_RetryUntilBudget` — engine retry loop end-to-end through real ReplicationVolume; budget exhaustion → escalate observable
- `TestT4d3_EngineRetryLoop_E2E_RecycleEscalatesImmediate` — ErrWALRecycled bypasses retry, escalates to rebuild via real path
- `TestComponent_WithEngineDrivenRecovery_NotStubAnymore` — framework primitive emits real engine commands

Lane discipline (transport side):
- `TestT4d3_Catchup_TransportNeverCallsSubstratePerLBA` — `catchup_sender` does not import substrate-internal packages; pins `INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY`

L2 matrix expansion:
- 3 existing T4c L2 scenarios (ShortDisconnect, GapExceedsRetention, RecoveryModeLabelSurfaced) re-pass with engine-driven flow

**Inscribed invariants**:
- `INV-REPL-CATCHUP-WITHIN-RETENTION-001` un-pinned at T4c → PORTED at T4d-3
- `INV-REPL-LANE-DERIVED-FROM-HANDLER-CONTEXT` (Q2) — receiver-side: catch-up handler routes to recovery lane
- `INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY` (Q3) — sender-side: no substrate-internal imports

**Preserve**:
- T4c `ScanLBAs(1, ...)` wasn't wrong, was wasteful; new `ScanLBAs(R+1, ...)` is V2-faithful
- T4c-2's 6 catch-up invariants (callback-return-nil-continues, lastSent-monotonic, deadline-per-call-scope, target-not-reached-vs-recycled-distinguished, recycle-escalates, completion-from-barrier-achievedLSN) all forward-carry
- T4c-pre-B `StartRecoverySession` dispatch shell unchanged (T4d-3 changes `StartCatchUp` legacy path; the unified path stays)

**G-1 required** per kickoff §3 sign #3.

---

**T4d-3 closure scope (architect-bound 2026-04-25 round-46+ Path B):**

T4d-3 closes as **R+1 bounded catch-up PORT + boundary fences**, NOT full engine-driven recovery. Architect explicit: "do not repeat T4c's narrowing silently" — this is a **scope delta**, not a soft carry.

**What T4d-3 (`44c60dd`) DELIVERED:**
- 5 G-1 PORT items (R+1 start boundary + per-call deadline + lastSent-monotonic + retry budget semantics + retention-miss escalation)
- 2 hidden invariants pinned (`INV-REPL-CATCHUP-FROMLSN-IS-REPLICA-FLUSHED-PLUS-1` + `INV-REPL-CATCHUP-FROMLSN-FROM-ENGINE-STATE-NOT-PROBE`)
- Boundary fence: `INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY` (Q3) via import-discipline test
- CARRY-T4D-LANE-CONTEXT-001 Option B (skip-test with forward-binding)
- 11 new tests per G-1 §7

**What T4d-3 DEFERRED to T4d-4 (mandatory, see T4d-4 expansion below):**
- `ReplicationVolume↔adapter` wiring so engine retry loop runs end-to-end
- `WithEngineDrivenRecovery()` framework primitive becomes real (currently stub)
- Full L2 subprocess matrix gain engine→adapter→executor end-to-end coverage

**Tests below labeled "Engine-driven recovery set"** (`TestT4d3_EngineRetryLoop_E2E_*` + `TestComponent_WithEngineDrivenRecovery_NotStubAnymore`) **DID NOT land in T4d-3** — they land in T4d-4 per Path B fold.

**T4d cannot close** until `WithEngineDrivenRecovery()` is real and QA #8 (`LastSentMonotonic_AcrossRetries_FullForm`) can run — see updated T4d-4 acceptance criteria.

**Estimated LOC**: ~250 production (signature change ripples through 5 files + engine wiring ~100 + framework primitive ~50) + ~150 tests.

---

### T4d-4 — Lifecycle + integration + remaining T4c carries + **engine-driven recovery wiring (Path B fold)** (~300 production + ~350 tests; **REVISED v0.4** from ~150/~200)

**Scope expansion (architect-bound 2026-04-25 round-46+ Path B):** T4d-3's mini-plan §2.3 listed engine-driven recovery wiring (`ReplicationVolume↔adapter` + `WithEngineDrivenRecovery()` real binding + full L2 matrix). T4d-3 (`44c60dd`) deferred this work; architect ratified Path B = fold into T4d-4 as **HARD CLOSE GATE** (not soft carry). T4d cannot close until this wiring is real and QA #8 can run.

**Where (revised v0.4 — adds engine-driven wiring rows):**

**Where**:

*Original T4d-4 scope (lifecycle + integration + T4c carries):*
- `core/replication/volume.go` — `ReplicationVolume.Stop` lifecycle regression (BUG-005 non-repeat per `v3-phase-15-t4d-bug005-non-repeat.md`)
- `core/replication/peer.go` — cross-orchestration sessionID coordination (T4c §I row 5: live-ship + explicit catch-up share `sessionID=1`)
- `core/storage/recovery_contract.go` — substrate `RecoveryMode()` method (T4c §I row 6 — wraps forward label without duck-typed `CheckpointLSN`)
- `core/replication/component/faults.go` — wraps forward `RecoveryMode()` correctly (closes the round-40 known limitation)
- `core/transport/catchup_sender.go` — replace duck-typed `CheckpointLSN()` probe with `RecoveryMode()` call
- `core/replication/component/` — Windows TempDir cleanup race investigation/fix (T4c §I row 7); rename or split `TestT4c3_Catchup_ShortDisconnect_DeltaOnly` per kickoff §5 trailing chore
- QA `LastSentMonotonic_AcrossRetries` cross-call form (T4c §I row 2)

***Path B fold from T4d-3 (engine-driven recovery wiring — MANDATORY HARD CLOSE GATE):***
- **`core/replication/volume.go`** — wire `ReplicationVolume↔adapter` so engine retry loop runs end-to-end (T4d-3 deferred per Path B; mini-plan v0.2 §2.3 row)
- **`core/replication/component/cluster.go`** — `WithEngineDrivenRecovery()` becomes real (was stub at T4c; T4d-3 deferred per Path B)
- **Full L2 integration matrix** — engine→adapter→executor end-to-end coverage (closes mini-plan §4 acceptance criterion #3 that T4c narrowed and T4d-3 deferred)

***Round-47 architect addition: rebuild path engine-driven end-to-end (MANDATORY):***
- `core/engine/apply.go` — extend `applySessionFailed`'s rebuild escalation to actually emit a rebuild command (not just transition `Decision = Rebuild` state); engine MUST own rebuild emit per Q1 architect lock
- `core/engine/events.go` — `RebuildSessionClosed{Success,Fail}` event (or equivalent) so rebuild outcomes are observable to engine state
- `core/replication/volume.go` (or appropriate dispatch site) — wire rebuild commands through ReplicationVolume↔adapter the same way catch-up commands do
- `core/engine/apply.go` — apply rebuild-session-closed events; define rebuild failure terminal (retry budget OR immediate PublishDegraded — architect impl-time call)
- L2 integration test: real catch-up → budget-exhausted → engine emits rebuild → rebuild executes and completes (or fails terminally) → outcome observable in engine state. **Test must observe an actual rebuild session occurring**, not just `Decision = Rebuild` state transition.

**Why** (kickoff §2.1 + §2.4 + Path B fold): close BUG-005 non-repeat (the original sketch T4d scope item); land all T4c §I forward-carries; round out integration matrix at L2 end-to-end; **complete the engine-driven recovery wiring deferred from T4d-3 (Path B fold)**.

**Tests** (~15 required):

Lifecycle:
- `TestReplicationVolume_Stop_LifecycleRegression_BUG005NonRepeat` — pin per `v3-phase-15-t4d-bug005-non-repeat.md` (pins `INV-REPL-LIFECYCLE-HANDLE-BORROWED-001`)
- `TestReplicationVolume_Stop_DuringCatchUp_TerminatesCleanly`
- `TestReplicationVolume_Stop_DuringRebuild_TerminatesCleanly`
- `TestReplicationVolume_Stop_Idempotent`

Cross-orchestration sessionID:
- `TestPeer_SessionIDCoordination_LiveShipAndExplicitCatchUp_NoCollision`

`RecoveryMode()` method:
- `TestStorageRecoveryMode_Walstore_ReportsWALReplay` — walstore exposes mode via method (not duck-typed)
- `TestStorageRecoveryMode_Smartwal_ReportsStateConvergence`
- `TestStorageRecoveryMode_BlockStore_ReportsStateConvergence`
- `TestComponent_AssertSawRecoveryMode_UnderWrap_StillWorks` — closes the round-40 wrap-vs-CheckpointLSN limitation

Integration matrix (L2 end-to-end — Path B fold from T4d-3 — MANDATORY for T4d close):
- `TestT4d4_Integration_FullRecoveryFlow_E2E_BothSubstrates` — engine→adapter→executor→replica end-to-end
- `TestT4d4_Integration_RecoverFromMidStreamFailure_RetryAndEscalate` — partial failure → retry → escalate full chain
- `TestT4d3_EngineRetryLoop_E2E_RetryUntilBudget` (relocated from T4d-3 per Path B fold) — engine retry loop end-to-end through real ReplicationVolume; budget exhaustion → escalate observable
- `TestT4d3_EngineRetryLoop_E2E_RecycleEscalatesImmediate` (relocated from T4d-3 per Path B fold) — ErrWALRecycled bypasses retry, escalates to rebuild via real path
- `TestComponent_WithEngineDrivenRecovery_NotStubAnymore` (relocated from T4d-3 per Path B fold) — framework primitive emits real engine commands
- `TestComponent_LastSentMonotonic_AcrossRetries_FullForm` — QA Stage-1 deferred scenario (QA #8); REQUIRES `WithEngineDrivenRecovery()` real binding to be runnable; T4d cannot close until this test runs green

Rebuild path engine-driven (round-47 architect addition — MANDATORY for T4d close):
- `TestT4d4_Integration_CatchupBudgetExhausted_EngineEmitsRebuild` — catch-up retry budget exhausted → engine emits actual rebuild command (not just state transition)
- `TestT4d4_Integration_RebuildSession_CompletesAndReportsToEngine` — rebuild session runs end-to-end; completion event reaches engine state
- `TestT4d4_Integration_RebuildFailure_TerminalDefined` — rebuild failure path; verifies terminal is either retry-budget OR PublishDegraded per architect impl-time call (test asserts whichever was chosen)
- `TestT4d4_Integration_CatchupToRebuildEscalation_RealPath` — full path: catch-up failure → escalation → rebuild session → outcome observable. **Test observes actual rebuild session occurring**, not just `Decision = Rebuild` state.

Cleanups:
- Windows TempDir cleanup race fix verification
- `TestT4c3_Catchup_ShortDisconnect_DeltaOnly` rename or split per kickoff §5 chore

**Inscribed invariants**:
- `INV-REPL-LIFECYCLE-HANDLE-BORROWED-001` (newly inscribed at T4d-4 per kickoff §4 #6)
- `LastSentMonotonic_AcrossRetries` full form pin

**Preserve**:
- All prior T4d-1 / T4d-2 / T4d-3 invariants forward-carry
- All T4a/T4b/T4c invariants forward-carry per kickoff §4 #5

**No G-1 required** (port + tests, no new V2 muscle source).

**Estimated LOC (revised v0.4)**: ~300 production + ~350 tests (was ~150/~200; expanded by Path B fold ~150 prod / ~150 tests for `ReplicationVolume↔adapter` wiring + `WithEngineDrivenRecovery` real binding + L2 matrix scenarios).

**HARD CLOSE GATE for T4d (architect-bound 2026-04-25 round-46+ Path B; round-47 rebuild expansion):**
T4d cannot close until ALL of:
1. `WithEngineDrivenRecovery()` is REAL (not stub) — `core/replication/component/cluster.go`
2. `ReplicationVolume↔adapter` wiring runs engine retry loop end-to-end
3. **Full L2 matrix engine→adapter→executor→replica end-to-end** including:
   - catch-up retry-until-budget end-to-end
   - WAL recycled → rebuild escalation real path (NOT unit-scope only)
   - `TestComponent_LastSentMonotonic_AcrossRetries_FullForm` (QA #8) runs and passes
   - Mini-plan §4 acceptance criterion #3 satisfied — same bar T4c narrowed and T4d-3 deferred per Path B
4. **Rebuild path engine-driven end-to-end (round-47 architect addition; cannot be cathup-only)** — minimum:
   - catch-up failure / budget exhausted → engine emits rebuild command (not just sets `Decision = Rebuild` state)
   - rebuild session completion / failure → returns to engine via observable event (`RebuildSessionClosed{Success/Fail}` or equivalent)
   - rebuild failure terminal defined explicitly: either rebuild has its own retry budget OR `PublishDegraded` immediately on first rebuild failure (architect call at impl time)
   - L2 covers the **real path** catch-up → rebuild escalation (test must observe an actual rebuild session occurring, not just `Decision = Rebuild` state transition)
5. All other T4d-4 acceptance items (lifecycle, T4c carries, etc.) green

**Explicitly NOT in T4d-4 hard gate (E1 — architect round-47 ruling):**
Engine state persistence across primary restart is a **G5 decision gate**, not a T4d-4 implementation gate. T4d-4 doesn't need to implement persistence; G5 must explicitly decide "persist Recovery state" vs "rebuild state from probe after restart" as a named decision (see §G5-DECISION-001 below).

---

## §3 Predicates (must be true before T4d-1 starts)

| Predicate | Source |
|---|---|
| T4c batch closed | `c910464a9` (architect 2026-04-25) |
| Round-43 architect lock | kickoff §2.2 |
| Round-44 architect refinement | kickoff §2.2 |
| Kickoff §1–8 RATIFIED | architect sign 2026-04-25 |
| Kickoff §9 (walshipper Q1/Q2/Q3) RATIFIED | architect sign 2026-04-25 |
| BlockStore walHead pre-T4d hotfix landed | `f6084ee` |
| All T4a + T4b + T4c invariants forward-carry green | last full-suite run on `f6084ee` |
| QA sign on this mini-plan | _________ pending |

---

## §4 Acceptance (per kickoff §4 ratified bar)

T4d closes when ALL of:

1. ✅ All 4 batches (T4d-1, T4d-2, T4d-3, T4d-4) merged to `phase-15`
2. ✅ Unit tests green per task (with Windows cleanup race fixed or platform-noted)
3. ✅ Full L2 subprocess matrix green — engine→adapter→executor end-to-end (the bar T4c narrowed)
4. ⏸ L3 m01 — deferred to G5 collective per kickoff §2.6 (NOT a T4d criterion)
5. ✅ T4a/T4b/T4c invariants forward-carry verified (no regression)
6. ✅ Active invariant promotions per kickoff §4 #6 (9 invariants — see kickoff for full list)
7. ✅ Engine sentinel decoupling: substring match REMOVED from `core/engine/apply.go`; structured kind in production at T4d-1
8. ✅ BlockStore walHead regression fixed in production — DONE (`f6084ee`)
9. ✅ QA single-sign at T4d close per §8C.2
10. ✅ T4 T-end three-sign per §8C.1 lands at T4d close IF no T4e is created (kickoff §4 #10)
11. ✅ No §8C.3 escalation triggers fired

---

## §5 Non-claims (explicit deferrals within T4d)

- **No m01 first-light** — G5 collective close (kickoff §2.6)
- **No `core/recovery` package move** — post-G5/T4e (kickoff §9.3 #3)
- **No lane byte on `MsgShipEntry` wire** — Q2 ratified, lane is implicit from handler context (kickoff §9.3 #2)
- **No wholesale V2 `WALShipper` port** — T4d-3 ports the bounded catch-up muscle ONLY; engine-owned recovery orchestration stays V3-native (kickoff §9.2 + §0 above)
- **No substrate-native per-LBA applied LSN exposure for ALL substrates** — Option C hybrid: add where practical (walstore, smartwal); BlockStore falls back to in-memory session map (kickoff §2.5 #1)
- **No retry loop in transport** — Q1: engine-owned (kickoff §9.3 #1)
- **No engine recovery state persistence implementation** (round-47 architect ruling) — engine `Recovery{Attempts, TargetLSN, ActiveSessionID}` survives only as long as the primary process. Across primary restart, all in-flight Recovery state is lost; replicas re-probe and engine rebuilds state from observed facts. **G5-DECISION-001** below makes this explicit.

### G5-DECISION-001 — engine recovery state behavior across primary restart (named decision record)

**Status**: ⏳ DECISION REQUIRED at G5 collective close (architect-bound 2026-04-25 round-47).

**Question**: when the primary process restarts mid-recovery (panic, deploy, container restart, OS kill), what happens to in-flight Recovery state (`Attempts` counter, `TargetLSN`, `ActiveSessionID`, retry budget consumed)?

**Two paths** (G5 must explicitly choose one):
- **A — persist Recovery state**: engine state durably stored (somewhere TBD); restart resumes from persisted state. Pros: retry budget honored across restart; in-flight session can resume. Cons: substrate work; restart-during-persist atomicity; new failure mode if persistence fails.
- **B — rebuild state from probe after restart**: engine starts cold; probes replicas; reconstructs `Recovery.R` from probe responses; resets `Attempts` to 0. Pros: simpler; no new substrate. Cons: retry budget effectively unbounded (each restart resets); operator can't tell "this replica has failed 9 times" if interspersed with restarts.

**Why this is a G5 decision, not a T4d-4 implementation**:
- T4d-4 hard gate is "engine-driven recovery wiring works in steady-state." Restart semantics is orthogonal.
- Path A vs B has architectural weight (persistence = new substrate concern; both have observable behavior differences operators must understand).
- Choosing requires production-context input (operations team's tolerance for retry-budget reset on restart, persistence cost vs simplicity).
- G5 close demands the decision recorded; the **implementation** of the chosen path may live in G5 or post-G5.

**T4d-4 obligation**: ensure engine state structure is **decision-A-compatible** (i.e., serializable; no embedded non-serializable types). This keeps either path open. Architect call at T4d-4 close if decision A would require structural changes that should be pre-positioned now.

**Owner of the decision**: architect at G5 collective close.

**Risk if silent**: G5 closes assuming "recovery works in production" without anyone having decided what restart does. First production primary restart surprises operators; depending on path, retry budget is either nonsense (B + frequent restarts → infinite retries) or fails loudly with no fallback (A + persistence layer not chosen).

---

## §6 Invariants to preserve (forward-carry baseline)

Per kickoff §4 #5. Named invariants from T4a/T4b/T4c that MUST stay green:

| Source | Invariant | T4d regression risk | Protection |
|---|---|---|---|
| T4a-2 | `INV-REPL-SHIP-TRANSPORT-MUSCLE-001` | Low — Ship not touched | T4a-2 suite via G-3 |
| T4a-4 | `INV-REPL-LSN-ORDER-FANOUT-001` | **Medium — T4d-3 changes catch-up scan boundary** | Forward-carry test in T4d-3; lock scope at PR |
| T4a-4 | `INV-REPL-PEER-REBUILD-ON-AUTHORITY-CHANGE` | Low | T4a-4 test |
| T4a-3 | `INV-REPL-FANOUT-001` | Low | T4a-6 BasicEndToEnd |
| T4b-1 | `INV-REPL-LINEAGE-BORNE-ON-BARRIER-ACK` | Low — wire untouched | T4b-1 + T4b-6 wire fence |
| T4b-3 | `INV-REPL-BARRIER-FAILURE-DEGRADES-PEER` | Low | T4b-3 + T4b-4 peer-state pin |
| T4b-4 | `INV-REPL-LOCAL-FSYNC-GATES-QUORUM` | Low | T4b-4 |
| T4b-1 | `INV-REPL-NO-ZERO-LINEAGE-FIELDS` | Low — no new wire surfaces in T4d | T4b-1 fence still applies |
| T4c-1 | `INV-REPL-LINEAGE-BORNE-ON-PROBE-PAIR` | Low — probe wire untouched | T4c-1 fence |
| T4c-1 | `INV-REPL-PROBE-NON-MUTATING-VALIDATION` | Low — probe handler untouched | T4c-1 test |
| T4c-2 | `INV-REPL-CATCHUP-CALLBACK-RETURN-NIL-CONTINUES` | **Medium — T4d-3 changes scan boundary** | Forward-carry test in T4d-3 |
| T4c-2 | `INV-REPL-CATCHUP-LASTSENT-MONOTONIC` | **Medium — T4d-3 R+1 changes start LSN** | Forward-carry test in T4d-3 |
| T4c-2 | `INV-REPL-CATCHUP-DEADLINE-PER-CALL-SCOPE` | Low — deadline discipline preserved | T4c-2 test |
| T4c-2 | `INV-REPL-CATCHUP-TARGET-NOT-REACHED-VS-RECYCLED-DISTINGUISHED` | **Medium — T4d-1 structured kind changes detection** | T4d-1 test must distinguish the two via typed kind, not substring |
| T4c-2 | `INV-REPL-CATCHUP-RECYCLE-ESCALATES` | **High — pinning method changes from substring to structured kind** | T4d-1 + T4d-2 + T4d-3 chain |
| T4c-2 | `INV-REPL-CATCHUP-COMPLETION-FROM-BARRIER-ACHIEVED-LSN` | Low — completion semantics unchanged | T4c-2 test |
| T4c-2 | `INV-REPL-PEER-STATE-CATCHINGUP` | Low | T4c-2 test |
| T4c-3 | `INV-REPL-RECOVERY-MODE-OBSERVABLE` | **Medium — T4d-4 replaces duck-typed probe with method** | T4d-4 test |
| T4c retry pin | retry-budget-per-content-kind defaults | Low | T4c-3 unit test |

---

## §7 Review gates

### §7.1 Pre-merge gates (mandatory; PR blocked until satisfied)

**Gate G-1 — V2 diff-footprint review on muscle-path tasks.**

Applies to **T4d-3 ONLY** in T4d. NOT required for:
- T4d-1: V3-native + small substrate edits (no V2 muscle source)
- T4d-2: V3-native; round-43 + round-44 architect text IS the spec
- T4d-4: port + tests

T4d-3 G-1 deliverable per kickoff §3 sign #3 + §9.5 split: sw produces `sw-block/design/v3-phase-15-t4d-3-g1-v2-read.md` separating PORT items (5) from V3-native items (5). QA signs pre-code per round-11 / round-15 / round-24 / T4c-2 G-1 discipline. Sw does not write T4d-3 production code until QA signs the G-1.

**Procedural binding** (QA v0.1 review NOTE 2): every T4d-3 PR commit message MUST reference the QA-signed G-1 doc by commit hash (the sw commit that landed the QA-sign edits). Format: `Refs G-1 sign: <sha>`. This makes the procedural chain auditable and prevents post-hoc claims of "I didn't see the G-1 finding."

**Process rule inscribed (architect-bound 2026-04-25 round-46+ Issue 2(a) ratification):**
G-1 sign docs land in `seaweedfs/sw-block/design/` FIRST; sw implementation in `seaweed_block` references the committed G-1 hash via `Refs G-1 sign: <sha>` per the binding above. Avoids the T4d-3 procedural lesson where `44c60dd` referenced "pending hash" because the G-1 doc was still untracked at commit time. T4d-3 G-1 doc landed at `seaweedfs@80036404c` (2026-04-25); future references to T4d-3 G-1 use this sha.

**Gate G-2 — Three-line godoc contract on every new public method.**

Applies to every new exported symbol in T4d-1 / T4d-2 / T4d-3 / T4d-4:
- `// Called by:`
- `// Owns:`
- `// Borrows:`

**Gate G-3 — Cumulative suite stays green.**

Every T4d-N commit: full `go test -count=1 ./...` green. Particular attention:
- `core/storage/` — substrate hardening + AppliedLSNs() new method
- `core/replication/` — apply gate + lifecycle changes
- `core/transport/` — catchup_sender signature change
- `core/engine/` — substring match removal + retry-loop end-to-end wiring
- `core/replication/component/` — adversarial tests un-skip + framework wraps update

### §7.2 Risks + procedural mitigations

| Risk | Mitigation | §8C.3 trigger? |
|---|---|---|
| T4d-2 apply gate misses lane discrimination edge case (recovery enters via wrong handler) | `INV-REPL-LANE-DERIVED-FROM-HANDLER-CONTEXT` test + dedicated lane-routing scenarios | trigger #1 if cross-lane leak post-merge |
| T4d-2 Option C hybrid `appliedLSN` seed differs from substrate truth on race | Restart-safety test + substrate-query semantics fence | trigger #1 if seed-vs-truth divergence |
| T4d-3 R+1 threading regresses T4c L2 matrix | Forward-carry tests for `INV-REPL-CATCHUP-CALLBACK-RETURN-NIL-CONTINUES` + `INV-REPL-CATCHUP-LASTSENT-MONOTONIC` under R+1 | trigger #1 if any T4c-2 invariant fails post-merge |
| T4d-3 G-1 mis-classifies V3-native item as PORT (or vice versa) | QA G-1 review uses §9.5 split as checklist | catch at G-1 sign |
| T4d-1 substring removal misses a call site | `TestEngine_SessionFailed_NoMoreSubstringMatch` fence + grep at PR | trigger #1 if substring sneaks back |
| T4d-4 lifecycle regression mis-handles pending sessions | BUG-005 non-repeat tests; per-state cleanup pins | trigger #1 if BUG-005 reproducible |
| Substrate defense-in-depth (T4d-1) turns into refactor | §2.5 #3 caveat: "if practical" — sw judgment + documented carry to T4e if needed | acceptable carry |

---

## §8 Sign table (mid-T batch, §8C.2)

| Role | Signer | Date | Decision |
|---|---|---|---|
| QA Owner | Claude (QA agent) | _________ | ⏸ pending T4d close |

Per §8C.2: no architect / PM sign at batch level. Architect + PM sign at T-end via closure report (kickoff §4 #10 / criterion #10) — lands at T4d close IF T4d remains the final T4 batch.

---

## §9 Change log

| Date | Change | Author |
|---|---|---|
| 2026-04-25 | Initial T4d mini-plan v0.1 drafted from kickoff v0.3 (§1–8 + §9 RATIFIED 2026-04-25). 4 batches per kickoff §3 sign: T4d-1 substrate hardening + structured failure kind (~150 LOC + ~80 tests, no G-1) → T4d-2 replica recovery apply gate (~250 LOC + ~250 tests, no G-1, V3-native spec from round-43/44) → T4d-3 R+1 + engine-driven recovery wiring (~250 LOC + ~150 tests, **G-1 required** per §9.5 split) → T4d-4 lifecycle + integration (~150 LOC + ~200 tests, no G-1). Incorporates §9 walshipper architecture: T4d-3 is bounded V2 muscle PORT (5 items) + V3-native items (5 items) per architect §9.5 binding; T4d-3 is NOT a wholesale `WALShipper` port. Q1 engine-owned retry, Q2 lane implicit from handler context, Q3 transport↔storage contract-only inscribed via `INV-REPL-LANE-DERIVED-FROM-HANDLER-CONTEXT` + `INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY`. BlockStore walHead pre-T4d hotfix already landed (`f6084ee`); §4 #8 acceptance criterion shows DONE. Predicates: T4c closed + round-43/44 + kickoff §1–9 RATIFIED + hotfix landed. T-end three-sign at T4d close conditional on no T4e per kickoff §4 #10. Total ~800 LOC production + ~680 LOC tests across 4 batches. | sw |
| 2026-04-25 (v0.2) | Architect v0.1 review absorbed. **HIGH BLOCKER FIX**: structured failure kind now end-to-end typed contract — `RecoveryFailureKind` enum + `RecoveryFailure` typed-error in storage; new `FailureKind storage.RecoveryFailureKind` field on **`adapter.SessionCloseResult`** AND **`engine.SessionClosedFailed`**; engine `applySessionFailed` branches on `e.FailureKind`, NOT on `containsAny(reason, ...)` substring; `Reason`/`FailReason` strings explicitly downgraded to "DIAGNOSTIC TEXT ONLY — engine MUST NOT parse"; new fence test `TestEngine_SessionFailed_NoMoreSubstringMatch` greps source for `"WAL recycled"` substring. Cross-package import note: typed-kind field doesn't widen dependency surface beyond existing storage-recovery-contract import (T4c-2 already established). **MED FIX**: every `LogicalStorage` impl now has explicit `AppliedLSNs()` body — walstore via `dm.snapshot()`, smartwal via `ring.scanValid()` reduction, BlockStore explicitly returns `(nil, ErrAppliedLSNsNotTracked)` (new sentinel); gate falls back to session-only map on the sentinel. **LOW FIX**: §0 PORT/V3-native table gains 5th V3-native row (package-boundary discipline / `INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY`) per kickoff §9.5. **QA test rename**: `TestApplyGate_BarrierAchievedLSN_DerivesFromCoverageNotData` → `TestApplyGate_BarrierCompletion_BarrierAuthoritativeWithCoverageContribution` (avoids misread as conflicting with T4c-2 `INV-REPL-CATCHUP-COMPLETION-FROM-BARRIER-ACHIEVED-LSN`). **QA NOTE 2 procedural absorbed**: §7.1 G-1 fence now binds T4d-3 PR commits to reference signed G-1 by hash (`Refs G-1 sign: <sha>`). T4d-1 test count grows from 8 to 10 (adds typed-kind end-to-end + event-shape fence). T4d-1 LOC est revised slightly upward (~170 prod + ~100 tests) due to adapter + engine event-struct contract additions. | sw |
| 2026-04-25 (v0.3) | Architect v0.2 dependency blocker absorbed. v0.2's typed-kind direction was correct but put `storage.RecoveryFailureKind` directly on engine events, which would create a new `core/engine → core/storage` dependency. v0.3 fixes the boundary: storage owns substrate-side `StorageRecoveryFailureKind` + `RecoveryFailure`; transport extracts that with `errors.As` and maps it to **engine-owned** `engine.RecoveryFailureKind`; `adapter.SessionCloseResult.FailureKind` uses `engine.RecoveryFailureKind`; `engine.SessionClosedFailed.FailureKind` uses the local engine type. `core/engine` MUST NOT import `core/storage`; `Reason`/`FailReason` remain diagnostic-only text. Test list label corrected to 10 required and mapping tests now explicitly cover storage-kind → engine-kind translation. | architect |
| 2026-04-25 (v0.4) | Architect Path B ratification absorbed (round-46+). T4d-3 `44c60dd` deferred engine-driven recovery wiring (`ReplicationVolume↔adapter` + `WithEngineDrivenRecovery()` real binding + full L2 matrix); architect chose Path B = fold into T4d-4 as **HARD CLOSE GATE** (not soft carry). T4d-3 §2.3 closure scope clarified as "R+1 bounded catch-up PORT + boundary fences" (5 G-1 PORT items + 2 hidden invariants + Q3 fence + CARRY-T4D-LANE-CONTEXT-001 Option B); engine-driven set explicitly relocated to T4d-4. T4d-4 §2.4 scope expanded with Path B fold rows (3 production sites + 5 tests relocated/added); LOC est revised ~150/~200 → ~300/~350. T4d HARD CLOSE GATE inscribed: cannot close until `WithEngineDrivenRecovery()` is REAL (not stub), `ReplicationVolume↔adapter` wiring runs end-to-end, QA #8 (`TestComponent_LastSentMonotonic_AcrossRetries_FullForm`) passes, and acceptance criterion #3 (full L2 matrix engine→adapter→executor end-to-end) satisfied — same bar T4c narrowed and T4d-3 deferred per Path B. **Process rule** (Issue 2(a) ratification): G-1 docs land in `seaweedfs` FIRST; sw implementation references committed hash via `Refs G-1 sign: <sha>`. T4d-3 G-1 doc landed at `seaweedfs@80036404c` (this revision). Architect explicitly rejected Path C (defer to G5) — "G5 should not inherit the same full-L2 gap again." | architect |
| 2026-04-25 (v0.5) | Architect round-47 ruling absorbed (post-T4d-3 production-readiness scope conversation). **T4d-4 HARD CLOSE GATE expanded with rebuild path (4th item)**: catch-up engine-driven wiring alone is insufficient for T4d close; rebuild path must also be engine-driven end-to-end. Minimum: catch-up failure/budget exhausted → engine emits actual rebuild command (not just `Decision = Rebuild` state); rebuild session completion/failure → returns to engine; rebuild failure terminal explicitly defined; L2 covers real path (test observes actual rebuild session, not just state transition). 4 new tests added in §2.4 Tests block. **Engine recovery state persistence (E1)**: architect explicitly EXCLUDED from T4d-4 implementation gate; instead inscribed as **G5-DECISION-001** named decision record in §5. G5 must choose Path A (persist) vs Path B (rebuild from probe after restart) explicitly; T4d-4 obligation is to keep engine state structure decision-A-compatible (serializable) so either path stays open. **Lane handler-context (CARRY-T4D-LANE-CONTEXT-001)** correction: NOT promoted to G5 hard blocker (it's already explicitly carried via Option B test-skip with non-claim); stays at T4e/post-G5 per existing inscription. Scope map clarified: T4d-4 = catch-up + rebuild engine-driven wiring; G5 = multi-replica mixed states + m01 + G5-DECISION-001 + minimal metrics/backpressure assessment; post-G5/T4e = lane fix + snapshot catch-up + protocol negotiation + auth/encryption. | architect |
