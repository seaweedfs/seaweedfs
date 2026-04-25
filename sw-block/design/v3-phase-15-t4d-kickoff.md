# T4d — Replica Recovery Apply Gate + Engine-Driven Recovery + Substrate Hardening — Kick-Off

**Date**: 2026-04-25 (v0.3 — architect-ratified)
**Status**: ✅ **RATIFIED** — architect sign by pingqiu 2026-04-25. Sw cleared to (a) land BlockStore walHead pre-T4d hotfix immediately, (b) produce T4d mini-plan + T4d-3 G-1 V2 read.
**Predicates met**:
- T4c batch closed (`c910464a9` closure report; architect acceptance signed 2026-04-25)
- Round-43 architect lock on stale-entry fix shape (replica recovery apply gate, lane-aware, per-LBA)
- Round-44 architect refinement (2-map split: `liveTouched` + `recoveryCovered`)
- Architect ratification of this kickoff: scope, batch shape, acceptance bar, G-1 requirements, decisions all locked
**Reference**: `v3-phase-15-t4c-closure-report.md` §I forward-carries; sketch `v3-phase-15-t4-sketch.md` §4 T4d row; `v3-phase-15-t4d-bug005-non-repeat.md` for original lifecycle scope; round-43 + ratification architect text inline at §2.2 + §8

**Revision history:**
- **v0.1** (2026-04-25 morning) — first draft based on T4c closure §I carries (R+1 threading + engine-driven recovery + structured RecoveryFailureKind + lifecycle)
- **v0.2** (2026-04-25 mid-day) — folds round-43 + round-44 locks: replica recovery apply gate added as primary T4d work; R+1 threading downgrades from correctness-blocker to bandwidth-optimization; BlockStore walHead one-liner + substrate defense-in-depth added; batch shape expands 3 → 4
- **v0.3** (2026-04-25 afternoon, this revision) — architect ratification folded: §2.5 open decisions resolved, §6 open issues closed, §4 criterion #10 wording tweak, status moves DRAFT → RATIFIED, sw cleared to proceed per §7

---

## §1 Why this kickoff is not just "the optional lifecycle batch"

The original T4 sketch (round 21) marked T4d **"optional"** with a single scope item: `ReplicationVolume.Stop` lifecycle regression for BUG-005 non-repeat. That scope alone would have been ~1 batch / ~1 task.

T4c batch close (closure §B + §I) added three product-semantic blockers carried forward instead of fixed at T4c. **Round-43 architect lock added a fourth blocker** that is bigger than the other three combined: a per-LBA data-regression vulnerability surfaced during the pinLSN < replicaLSN design conversation. Architect's verbatim:

> T4d stale-entry safety belongs at the replica recovery apply gate, with substrate fixes as defense-in-depth. The invariant is about **no per-LBA data regression**, not just frontier monotonicity.

| T4d blocker | Why it landed at T4d (not T4c) | Source |
|---|---|---|
| **Replica recovery apply gate** — lane-aware (recovery vs live) per-LBA stale-skip with coverage-advance on skip | Production vulnerability surfaced post-T4c during pinLSN design discussion; architect locked fix shape at round-43; no substrate currently implements per-LBA stale-skip | Round-43 (this revision) |
| **R+1 catch-up threading** — `StartCatchUp` signature + adapter wire so executor scans from replica's flushed LSN, not from `1` | T4c sender hardcodes `ScanLBAs(1)`. **Status change post round-43**: was correctness-blocker; now bandwidth-optimization (apply gate handles correctness regardless of what primary ships). Still belongs in T4d, but acceptance criterion shifts from correctness-pin to performance-evidence | T4c closure §B delta #4 / §I row 3, revised by round-43 |
| **Full engine→adapter→executor recovery wiring** (`WithEngineDrivenRecovery` real binding) | T4c L2 matrix is muscle-level only; full engine-loop end-to-end requires `ReplicationVolume↔adapter` plumbing not present at T4c | T4c closure §B delta #3 / §I row 1 |
| **Structured `RecoveryFailureKind`** replacing substring `"WAL recycled"` match | T4c accepted substring matching as TEMPORARY; architect-signed binding to "T4d (preferred) or G5 final sign (latest)" | T4c closure §B additional delta / §I row 4 |

Plus the original lifecycle item (still in scope) and substrate hardening (NEW post round-43).

**T4d is no longer optional, no longer small, and now contains a new component (the replica recovery apply gate) that did not exist in any prior batch.**

---

## §2 Scope expansion vs sketch — architect ratification asks

### §2.1 Original sketch T4d scope (still in)

| Item | Source | Owner |
|---|---|---|
| `ReplicationVolume.Stop` lifecycle regression — `INV-REPL-LIFECYCLE-HANDLE-BORROWED-001` | sketch §4 row T4d + `v3-phase-15-t4d-bug005-non-repeat.md` | sw |

### §2.2 Round-43 locked scope — primary T4d work (NEW since v0.1)

**Architect rule (round-43 verbatim, locked):**

> Recovery-stream stale entries are valid duplicates and must be skipped as data writes while still counted as recovery-stream coverage. Live-lane stale entries are abnormal and must not mutate data; they should be skipped/rejected under lineage/order diagnostics and must not advance recovery coverage.

**Components to build:**

| Component | Description | Owner |
|---|---|---|
| **Replica recovery apply gate** (NEW file in `core/replication/`) | Lane-aware apply policy. Recovery lane: `if entry.LSN <= appliedLSN[LBA] → skip data + recoveryCovered[LBA]=true; else apply + appliedLSN[LBA]=entry.LSN`. Live lane: existing lineage/session/live-order rules; stale → reject + diagnostic, **do NOT** advance recoveryCovered | sw |
| **`liveTouched[LBA]` + `recoveryCovered[LBA]` 2-map split** | Two distinct session-state maps per architect's round-44 precision: `liveTouched` = live lane wrote (recovery must yield); `recoveryCovered` = recovery stream processed this LBA/entry (drives barrier-completion accounting). Same physical LBA can appear in both | sw |
| **`appliedLSN[LBA]` source** | Per-LBA applied-LSN map. Bootstrap: from substrate per-LBA-LSN read API (must add) OR maintained in replica recovery state seeded from recovery/live applies during the session. Architect noted: "for full pre-existing state correctness, storage eventually needs to expose per-LBA applied LSN metadata." Sw architect-decision needed on Option A (substrate-native) vs Option B (in-memory session) vs Option C (hybrid) — see §2.5 | sw + architect |
| **Wire `replica.go` MsgShipEntry handler through the gate** | Existing handler bypasses any gate and calls substrate `ApplyEntry` directly; needs lane discrimination + gate dispatch | sw |
| **Adversarial test suite** (currently skipped with TODO per sw round-43 status) | Un-skip + extend per round-44 lane refinements: same-LBA old/new with sever-before-new (no regression), repeated recovery window (idempotency), live-races-recovery-old (live wins via per-LBA LSN), replica-restart-mid-recovery (Option-A discriminator), recovery-stale-still-advances-coverage (round-44 specific), live-stale-fails-loud (round-44 specific) | sw + QA |

### §2.3 Substrate hardening (NEW post round-43)

| Substrate | Status today | Required at T4d |
|---|---|---|
| **BlockStore** (`core/storage/store.go:264`) | `s.walHead = lsn` **unconditional** — regresses walHead on older-LSN apply. Production bug today. | **One-liner gate**: `if lsn > s.walHead { s.walHead = lsn }`. **Candidate for pre-T4d hotfix-PR** rather than bundled in T4d-1 — architect call (see §2.5) |
| **smartwal** (`core/storage/smartwal/store.go:343`) | walHead guarded; per-LBA data overwrites unconditionally via `writeAt(lba, lsn, data, flagWrite)` | Defense-in-depth: per-LBA stale-skip in `ApplyEntry` body. Lower priority — apply gate at replica is the primary correctness boundary |
| **walstore** (`core/storage/walstore.go:516`) | walHead guarded; `dm.put(lba, walRelOff, lsn, ...)` overwrites dirty-map blindly + WAL appended *before* any guard could skip → stale apply produces permanent WAL bloat | Defense-in-depth: per-LBA stale-skip + dirty-map conditional put. Lower priority for correctness, but has WAL-bloat impact unrelated to correctness |

### §2.4 Carry-forward from T4c §I (unchanged from v0.1)

| Item | Source | Owner |
|---|---|---|
| `LastSentMonotonic_AcrossRetries` full cross-call scenario | T4c §I row 2 | QA |
| Cross-orchestration sessionID coordination — live-ship + explicit catch-up share `sessionID=1` | T4c §I row 5 | sw → QA scenario |
| Substrate `RecoveryMode()` method (wraps forward label without duck-typed `CheckpointLSN`) | T4c §I row 6 | sw |
| Windows TempDir cleanup race | T4c §I row 7 | sw |

### §2.5 Architect decisions (RATIFIED v0.3)

1. **`appliedLSN[LBA]` source** — ✅ **Option C (hybrid) RATIFIED.** T4d-2 adds a substrate query path where available and seeds an in-memory session map from it, then updates the map during live/recovery applies. Architect rationale: "Option B alone is not restart-safe enough. Option A everywhere is too much substrate churn for this batch. Option C is the right production path, with Option A as the long-term substrate target."
2. **BlockStore walHead one-liner — pre-T4d hotfix vs T4d-1 bundle?** ✅ **Pre-T4d hotfix RATIFIED.** Sw cleared to land single-commit PR immediately, including the un-skipped regression test in the same PR. Architect rationale: "It is a confirmed storage contract violation, one-line fix, and low conflict risk."
3. **Substrate defense-in-depth (smartwal + walstore stale-skip)** — ✅ **Include in T4d-1 RATIFIED, with practical-scope caveat.** Architect text: "The replica apply gate is the primary correctness boundary, but `ApplyEntry` remains a dangerous public-ish substrate method. Add stale-protection to `walstore` and `smartwal` if it can be done without turning T4d-1 into a substrate refactor. If a substrate lacks enough per-LBA metadata, document the limitation and keep the central gate as authoritative." Sw judgment call at T4d-1 implementation time on per-substrate inclusion vs documented carry.

### §2.6 Explicitly NOT in T4d (deferred to G5 collective close)

| Item | Bind point | Rationale |
|---|---|---|
| m01 hardware first-light for replicated write path | G5 batch close (collective sign across T4a/b/c/d) | Per T4c closure §H — m01 first-light naturally lands when ALL T4 batches are green together |

**Architect decisions for §2 (RATIFIED v0.3):**
1. ✅ **Single T4d, not split.** Architect text: "The apply gate, structured failure kind, R+1 threading, engine-driven recovery, and lifecycle close are all part of making G5's replicated write path coherent. Splitting would leave T4d with half a recovery contract and push the real closure out again."
2. ✅ §2.5 resolved (see above)
3. ✅ m01 stays at G5 collective per §2.6 — "Keep it out of T4d's per-batch acceptance unless T4d naturally produces it."

---

## §3 Proposed batch shape (REVISED v0.2 — 4 batches, was 3 in v0.1)

| Batch | Scope | LOC est | G-1 required? | Status change |
|---|---|---|---|---|
| **T4d-1** Substrate hardening + structured failure kind | BlockStore walHead one-liner (or skip if architect lifts to pre-T4d hotfix) + walstore + smartwal stale-skip (defense-in-depth) + new `RecoveryFailureKind` enum in `core/storage/recovery_contract.go`; `LogicalStorage.ScanLBAs` wraps errors as typed kind; engine `applySessionFailed` switches on kind instead of substring; QA scenario #2 updated to assert typed kind | ~150 production + ~80 tests | NO (V3-native + small substrate edits) | NEW |
| **T4d-2** Replica recovery apply gate (lane-aware) | New file in `core/replication/` (apply gate component); `liveTouched` + `recoveryCovered` 2-map split; `appliedLSN[LBA]` source per §2.5 architect resolution; wire `replica.go` MsgShipEntry handler through gate (lane discrimination); un-skip + extend the 5 existing adversarial tests + add round-44 coverage-advance + live-fails-loud tests | ~250 production + ~250 tests | NO (V3-native — no V2 source for lane-aware split-bitmap design; round-43 + round-44 architect text IS the spec) | NEW (was R+1+engine bundled at v0.1's T4d-2) |
| **T4d-3** R+1 threading + engine-driven recovery wiring | `StartCatchUp(replicaID, sessionID, epoch, endpointVersion, **fromLSN**, targetLSN)` signature change; adapter command schema bump; `ReplicationVolume↔adapter` wiring binds engine retry loop end-to-end; `WithEngineDrivenRecovery` framework primitive becomes real; full L2 matrix (currently muscle-level only) gains engine→adapter→executor end-to-end coverage | ~250 production + ~150 tests | **YES** — wire change touches T4a-3 + T4b-3 + T4c-2 muscle code; G-1 V2 read on V2's `runCatchUpTo(replicaFlushedLSN, targetLSN)` (line 845) is the muscle-source reference | unchanged from v0.1 (was T4d-2) |
| **T4d-4** Lifecycle + integration | `ReplicationVolume.Stop` lifecycle regression (BUG-005 non-repeat); cross-orchestration sessionID coordination; substrate `RecoveryMode()` method; full integration matrix at L2 end-to-end (now possible with T4d-3 wiring); QA `LastSentMonotonic_AcrossRetries` cross-call form lands here; Windows TempDir cleanup race investigation | ~150 production + ~200 tests | NO (port + tests) | unchanged from v0.1 (was T4d-3) |

**Why T4d-1 (substrate + structured kind) goes first:** lowest-risk decoupled work; closes the BlockStore production vulnerability quickly even if architect chooses bundle-not-hotfix; structured RecoveryFailureKind is a contract change other batches depend on.

**Why T4d-2 (apply gate) before T4d-3 (R+1 + engine wiring):** the apply gate is the correctness boundary. R+1 is bandwidth optimization built ON the apply gate's correctness guarantee. Building R+1 first would mean shipping unsafe recovery in production for the duration of T4d-2 if T4d-3 lands first.

**Architect decisions for §3 (RATIFIED v0.3):**
1. ✅ **4-batch shape RATIFIED.** Architect text: "This ordering is correct: remove brittle contracts first, then add the correctness gate, then optimize/wire end-to-end."
2. ✅ Order RATIFIED: T4d-1 substrate+kind → T4d-2 apply gate → T4d-3 R+1+engine wiring → T4d-4 lifecycle+integration
3. ✅ T4d-3 G-1 RATIFIED. Architect text: "T4d-3 needs G-1 because it touches the V2 catch-up muscle boundary and `runCatchUpTo(replicaFlushedLSN, targetLSN)` semantics."
4. ✅ T4d-2 NO G-1 RATIFIED. Architect text: "T4d-2 does not need G-1; its spec is the round-43/44 V3-native apply-gate rule."

---

## §4 Acceptance bar — proposal (REVISED v0.2 — adds round-43-derived criteria)

T4d closes when ALL of (proposed; awaiting ratification):

1. ✅ All proposed batches merged to `phase-15`
2. ✅ Unit tests green per task (with Windows cleanup race fixed or platform-noted)
3. ✅ **Full L2 subprocess matrix green** — engine→adapter→executor end-to-end (the bar T4c narrowed). Includes the new R+1 short-gap test required for `INV-REPL-CATCHUP-WITHIN-RETENTION-001` un-pin
4. ⏸ L3 m01 — **deferred to G5 collective** per §2.6 (not a T4d criterion)
5. ✅ T4a/T4b/T4c invariants forward-carry verified (no regression)
6. ✅ **Active invariant promotions (REVISED v0.2):**
   - `INV-REPL-CATCHUP-WITHIN-RETENTION-001` — un-pinned at T4c — promotes to PORTED via T4d-3 R+1 + T4d-2 apply gate
   - `INV-REPL-LIFECYCLE-HANDLE-BORROWED-001` — newly inscribed at T4d-4
   - **NEW: `INV-REPL-NO-PER-LBA-DATA-REGRESSION`** (goal-level, round-43 lock) — pinned at T4d-2
   - **NEW: `INV-REPL-RECOVERY-STALE-ENTRY-SKIP-PER-LBA`** (mechanism, round-43 lock) — pinned at T4d-2
   - **NEW: `INV-REPL-RECOVERY-COVERAGE-ADVANCES-ON-SKIP`** (round-44 refinement) — pinned at T4d-2
   - **NEW: `INV-REPL-LIVE-LANE-STALE-FAILS-LOUD`** (round-44 refinement) — pinned at T4d-2
   - **NEW: `INV-REPL-RECOVERY-COVERAGE-RESTART-SAFE`** (Option-A discriminator if architect picks A in §2.5; documented but not pinned if Option B/C) — conditionally pinned at T4d-2
   - `LastSentMonotonic_AcrossRetries` full form — pinned at T4d-4
7. ✅ Engine sentinel decoupling: substring match REMOVED from `core/engine/apply.go`; structured kind in production at T4d-1
8. ✅ **NEW: BlockStore walHead regression fixed in production** (whether via pre-T4d hotfix or T4d-1 bundle per §2.5 #2)
9. ✅ QA single-sign at T4d close per §8C.2
10. ✅ **T4 T-end three-sign per §8C.1 lands at T4d close IF no T4e is created** (architect-tweaked wording v0.3). If T4d splits or defers a G5-critical item, T-end sign moves to the actual final batch.
11. ✅ No §8C.3 escalation triggers fired

**Architect decisions for §4 (RATIFIED v0.3):**
1. ✅ Acceptance bar accepted with the criterion #10 wording tweak above.
2. ✅ m01 first-light stays G5 collective (§4 #4 unchanged); architect text: "It is required for G5 final confidence, not for every sub-batch."

---

## §5 Forward-carry hand-off from T4c + round-43

Per T4c closure §I + round-43 lock, T4d inherits these explicit obligations:

| Obligation | Source | T4d batch |
|---|---|---|
| Replica recovery apply gate (lane-aware, per-LBA stale-skip + coverage advance) | Round-43 lock | **T4d-2 (primary work)** |
| 2-map split: `liveTouched` + `recoveryCovered` | Round-44 refinement | T4d-2 |
| `appliedLSN[LBA]` source resolution | Round-43 + §2.5 #1 | T4d-2 (architect-decision-gated) |
| BlockStore walHead one-liner | Round-43 substrate triage | T4d-1 (or pre-T4d hotfix per §2.5 #2) |
| walstore + smartwal substrate stale-skip (defense-in-depth) | Round-43 substrate triage | T4d-1 (architect-decision-gated per §2.5 #3) |
| R+1 threading + non-empty-replica short-gap test | T4c §I (status changed: was blocker, now optimization) | T4d-3 |
| `WithEngineDrivenRecovery` real wiring | T4c §I | T4d-3 |
| Structured `RecoveryFailureKind` | T4c §I | T4d-1 |
| Cross-orchestration sessionID coordination | T4c §I | T4d-4 |
| `LastSentMonotonic_AcrossRetries` full form | T4c §I | T4d-4 |
| Substrate `RecoveryMode()` method | T4c §I | T4d-4 |
| Windows TempDir cleanup race | T4c §I | T4d-4 (or earlier opportunistically) |

T4c rename / annotation chore: `TestT4c3_Catchup_ShortDisconnect_DeltaOnly` rename or annotate at T4d-3 (the test name overclaims today; once R+1 lands, either rename to reflect the now-correct delta semantic or split into "ship-from-genesis" + "delta-from-R+1" tests). Architect-flagged at T4c review (PM/lease round-1 finding).

---

## §6 Open issues — all resolved at v0.3 ratification

| Issue | Resolution |
|---|---|
| ~~§2.5 #1 `appliedLSN[LBA]` source~~ | ✅ Option C hybrid (architect-ratified) |
| ~~§2.5 #2 BlockStore walHead lift-out~~ | ✅ Pre-T4d hotfix approved (architect-ratified) |
| ~~§2.5 #3 Substrate defense-in-depth scope~~ | ✅ Include in T4d-1 with practical-scope caveat (architect-ratified) |
| ~~§2 single-T4d vs T4d/T4e split~~ | ✅ Single T4d (architect-ratified) |
| ~~§3 batch shape + ordering~~ | ✅ 4-batch shape + ordering ratified (architect-ratified) |
| ~~§4 T-end three-sign timing~~ | ✅ Lands at T4d close IF no T4e (architect-ratified, criterion #10 wording tweaked) |
| ~~§4.2 L3-script question (sw-flagged at round-43)~~ | ✅ Resolved by T4c batch close `c910464a9`; not a T4d blocker |

**No outstanding architect-line decisions.** Sw cleared per §7.

---

## §7 Status — RATIFIED; sw cleared to proceed

**Sw clearances (effective immediately on ratification 2026-04-25):**

| Clearance | Action |
|---|---|
| **Pre-T4d hotfix** | Land BlockStore walHead one-liner as single-commit PR. Include un-skipped `TestComponent_Adversarial_BlockStoreApplyEntryRegressesWalHead` in same PR as regression fence. Title suggestion: "BlockStore: gate walHead update on lsn > walHead (round-43 substrate hardening, defense-in-depth)" |
| **T4d mini-plan** | Produce `v3-phase-15-t4d-mini-plan.md` per the 4-batch shape ratified at §3 |
| **T4d-3 G-1 V2 read** | Produce `v3-phase-15-t4d-3-g1-v2-read.md` for the StartCatchUp signature change + R+1 threading (V2 muscle source: `wal_shipper.go:845` `runCatchUpTo`) |
| **T4d-2 spec** | Round-43 + round-44 architect text IS the spec; no G-1 needed. Sw can begin design as soon as mini-plan ratifies |
| **Forward-carry verification list** | 6 active T4c invariants + 1 T4c retry-budget pin must continue green under T4d wire/signature/apply-gate changes |

**QA review obligations (in order):**
1. Review T4d mini-plan for batch decomposition + acceptance discipline alignment with §3 + §4
2. Review T4d-3 G-1 V2 read using same discipline as T4c-2 G-1 (signed-options + invariant audit)
3. Review each batch's PR per §8C.2 batch-close criteria
4. Author T4d-2 + T4d-3 + T4d-4 QA scenarios at component scope (mirroring T4c QA Stage-1)

**Cadence (architect-ratified ordering):**
- (now) sw lands BlockStore hotfix
- (next) sw produces mini-plan + T4d-3 G-1; QA reviews both
- T4d-1 substrate hardening + structured kind (decoupled lowest-risk)
- T4d-2 apply gate (V3-native, parallel-eligible with T4d-3 once both specs ratified)
- T4d-3 R+1 + engine-driven recovery (G-1-gated)
- T4d-4 lifecycle + integration + remaining QA carries

**T-end horizon:** if T4d remains the final T4 batch (no T4e splits during execution), §8C.1 T-end three-sign lands at T4d close per criterion #10. Otherwise T-end moves to whichever batch becomes final.

---

## §8 Architect ratification record

**Decisions (verbatim from ratification message, 2026-04-25):**

1. **Scope stays one T4d, not T4d/T4e.** "The apply gate, structured failure kind, R+1 threading, engine-driven recovery, and lifecycle close are all part of making G5's replicated write path coherent. Splitting would leave T4d with half a recovery contract and push the real closure out again."
2. **`appliedLSN[LBA]` source: choose Option C hybrid.** "T4d should add a substrate query path where available and seed an in-memory session map from it, then update the map during live/recovery applies. Option B alone is not restart-safe enough. Option A everywhere is too much substrate churn for this batch. Option C is the right production path, with Option A as the long-term substrate target."
3. **BlockStore walHead one-liner: approved as pre-T4d hotfix.** "Lift it out. It is a confirmed storage contract violation, one-line fix, and low conflict risk. Include the un-skipped regression test in the same PR."
4. **Substrate defense-in-depth: include in T4d-1.** "The replica apply gate is the primary correctness boundary, but `ApplyEntry` remains a dangerous public-ish substrate method. Add stale-protection to `walstore` and `smartwal` if it can be done without turning T4d-1 into a substrate refactor. If a substrate lacks enough per-LBA metadata, document the limitation and keep the central gate as authoritative."
5. **Batch shape: approve the 4-batch plan and order.** "T4d-1 substrate hardening + structured kind → T4d-2 apply gate → T4d-3 R+1 + engine-driven recovery → T4d-4 lifecycle + integration. This ordering is correct: remove brittle contracts first, then add the correctness gate, then optimize/wire end-to-end."
6. **G-1 requirements: approve as written.** "T4d-3 needs G-1 because it touches the V2 catch-up muscle boundary and `runCatchUpTo(replicaFlushedLSN, targetLSN)` semantics. T4d-2 does not need G-1; its spec is the round-43/44 V3-native apply-gate rule."
7. **Acceptance bar: approve with one wording tweak.** "T4 T-end three-sign should happen **at T4d closure only if T4d remains the final T4 batch**. If T4d splits or defers a G5-critical item, T-end sign moves to the actual final batch. So criterion #10 should say 'T4 T-end three-sign lands at T4d close if no T4e is created.'" — applied to §4 #10 in v0.3
8. **m01 first-light remains G5 collective.** "Keep it out of T4d's per-batch acceptance unless T4d naturally produces it. It is required for G5 final confidence, not for every sub-batch."

**Sign:**

> `Architect | pingqiu | 2026-04-25 | ✓ RATIFIED — T4d v0.2 scope accepted as one batch series; Option C for appliedLSN source; BlockStore walHead hotfix may land pre-T4d; substrate defense-in-depth included where practical; 4-batch order approved; T4d-3 G-1 required; T4d-2 no G-1; T-end three-sign at T4d close if T4d remains final T4 batch.`
