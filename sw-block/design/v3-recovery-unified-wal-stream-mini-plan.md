# Recovery — Unified WAL stream / cursor-rewind mini-plan (v0.2)

**Status**: mini-plan v0.2 — round 1 architect ratification 2026-04-29 logged in §10. All Q10-Q15 ratified-with-defaults. One §2.3 comment fix (Q13 alignment) + cross-reference to kickoff v0.3 §5.1 row split. Stage 2 of governance sequence (`feedback_t4d_governance_sequence.md`) closed; **G-1 stage may begin** per architect.

**Branch target**: `g7-redo/unified-wal-mini-plan` (this branch carries the doc; implementation branch is a sibling once mini-plan ratifies).

**Stack base**: `g7-redo/unified-wal-kickoff` (kickoff v0.2 with round 1 ratification logged).

**Trunk base** (after kickoff merges): `phase-15` tip with the train + kickoff doc landed.

**Scope contract**: this doc spells out the per-file diff. The implementation branch executes it. Anything not pinned here is out-of-scope for the implementation PR; surfaces back as a Round 2 question.

---

## 1. What this stage owns vs. what stays for G-1 / code

| Stage | Owns | Defers |
|---|---|---|
| Kickoff (v0.2, ratified) | Framing, naming, INV pin, scope boundary, Q1-Q9 defaults, receiver discipline table | File-level diff, test catalog |
| **Mini-plan (this doc)** | **File-level diff plan, test catalog, LOC budget, G-1 reading list, sequencing** | Actual code, V2 read execution |
| G-1 | V2 read of the gate-deletion site + receiver discipline (per `feedback_g1_pre_code_review.md`) | Actual code |
| Code | Implementation; tests; CI green | — |

Mini-plan is **diff specifications** — what changes per file, what tests get written, in what order. It is NOT a code review against pseudocode.

---

## 2. File-by-file diff plan

### 2.1 `core/recovery/sender.go` — gate deletion + rewind-once pump

**Current**: 470 lines. Five-step `Run()` that streamBacklog → wait closeCh → drainAndSeal → TryAdvanceToSteadyLive → barrier.

**Diff**:

| Action | Target | Notes |
|---|---|---|
| **DELETE** | Lines 60-69 (`queueMu`, `liveQueue`, `sealed` fields) | Buffer + atomic-seal disappear |
| **DELETE** | Lines 117-134 (`Close` method + closeOnce) | Caller-side "no more live writes" signal disappears (replaced by idle-window) |
| **DELETE** | Lines 136-156 (`PushLiveWrite` API) | Live writes flow through substrate's growing tail, NOT through this API |
| **DELETE** | Lines 244-258 (Step 5 in `Run()`: `<-closeCh` / `<-ctx.Done()` wait) | Replaced by idle-window exit in pump loop |
| **DELETE** | Lines 260-279 (Step 6 + 7: `drainAndSeal` + `TryAdvanceToSteadyLive`) | Both gone |
| **DELETE** | Lines 384-428 (`streamBacklog` method) | Replaced by `streamUntilHead` |
| **DELETE** | Lines 432-470 (`drainAndSeal` method) | Goes with the queue |
| **DELETE** | Lines 203-209 (defer that sets `sealed=true`) | No `sealed` to set |
| **DELETE** | Field `closeCh chan struct{}` (~line 70 area) + init in `NewSender` | Caller signal removed |
| **MODIFY** | `Run()` step ordering (lines ~211-300) | New flow: SessionStart → streamBase → BaseDone → streamUntilHead → BarrierReq → BarrierResp → CanEmitSessionComplete |
| **ADD** | New method `streamUntilHead()` | Replaces `streamBacklog()`. ~80 LOC. Body sketched in §2.1.1 |
| **ADD** | Field `idleWindow time.Duration` to `Sender` struct | Default `100ms` (mitigates H1; configurable via constructor) |

**LOC delta**: ~-120 deleted, ~+80 added → net **~-40 LOC** in sender.go.

#### 2.1.1 `streamUntilHead()` body sketch

```go
// streamUntilHead pumps WAL entries from the cursor (initialized to
// fromLSN at session start — the one rewind) forward until the cursor
// has caught up with primary's head AND no new appends arrive within
// idleWindow. The Kind byte flips from Backlog to SessionLive once
// (head - cursor < kindFlipEpsilon); never flips back.
//
// Exit conditions:
//   1. ScanLBAs returns substrate failure → wrap as FailureSubstrate.
//   2. ScanLBAs returns ErrWALRecycled (typed) → wrap as FailureWALRecycled.
//   3. Frame write fails → wrap as FailureWire.
//   4. cursor == head AND idleWindow elapsed → return nil (ready to barrier).
//   5. ctx cancelled → wrap as FailureCancelled.
//
// idleWindow is configurable; default 100ms. Tighter values barrier
// prematurely under sustained writes; longer values stall otherwise-
// caught-up sessions. Pinned by TestSender_IdleWindow_NotPrematureBarrier.
func (s *Sender) streamUntilHead(ctx context.Context) error {
    cursor := s.fromLSN  // the one rewind happens at NewSender; this is the start point
    kind := WALKindBacklog
    const kindFlipEpsilon uint64 = 8

    for {
        scanErr := s.primaryStore.ScanLBAs(cursor, func(entry storage.RecoveryEntry) error {
            // Frame write
            if err := s.writeFrame(frameWALEntry,
                encodeWALEntry(kind, entry.LBA, entry.LSN, entry.Data)); err != nil {
                return err
            }
            cursor = entry.LSN
            // Kind flip: monotonic, one-way
            if kind == WALKindBacklog {
                _, _, head := s.primaryStore.Boundaries()
                if head-cursor < kindFlipEpsilon {
                    kind = WALKindSessionLive
                }
            }
            return nil
        })

        // Classify substrate vs wire failure
        if scanErr != nil {
            // ... FailureSubstrate or FailureWALRecycled or FailureWire
            return ...
        }

        // Scan exhausted at this snapshot. Decide: loop again or barrier?
        _, _, head := s.primaryStore.Boundaries()
        if cursor == head {
            // Truly caught up at this moment; verify idle window passes.
            select {
            case <-time.After(s.idleWindow):
                _, _, headAgain := s.primaryStore.Boundaries()
                if cursor == headAgain {
                    return nil  // exit pump; ready to barrier
                }
                // head moved during idle window; loop again
            case <-ctx.Done():
                return newFailure(FailureCancelled, PhaseBacklog, ctx.Err())
            }
        }
        // cursor < head: more entries appeared mid-scan. Loop immediately.
    }
}
```

Bikeshed: method name. `streamUntilHead`, `pumpToHead`, `walPumpLoop` — no preference; mini-plan defaults to `streamUntilHead` for symmetry with deleted `streamBacklog`. Implementation PR can rename if reviewer prefers.

### 2.2 `core/recovery/sender_test.go` — test catalog churn

**DELETE** (tests that rely on the gate API):

| Test | Rationale |
|---|---|
| `TestSender_PushLiveWrite_BeforeDrain_AccumulatesQueue` | API gone |
| `TestSender_PushLiveWrite_AfterSeal_Errors` | API gone |
| `TestSender_DrainAndSeal_FlushesQueueInLSNOrder` | drainAndSeal gone |
| `TestSender_Close_TriggersSealAndDrain` | Close gone |
| `TestSender_TryAdvanceToSteadyLive_FiresAtTransition` | Step gone |

(Exact test names per the current sender_test.go — implementation PR audits in case names differ.)

**ADD** (new tests pinning §3.2 #3 behavior):

| Test | Pins |
|---|---|
| `TestSender_RewindOnce_CursorMonotonicForward` | Sender's cursor starts at fromLSN; primary writes during pump; cursor follows monotonically; barrier hits expected `AchievedLSN`. Uses memorywal substrate with concurrent goroutine writing to primary. |
| `TestSender_KindByte_FlipsOnceAtCatchUp` | Frame sequence shows monotonic Backlog→SessionLive transition; never flips back even if primary bursts mid-stream. Uses memorywal + frame capture in test wire. |
| `TestSender_IdleWindow_NotPrematureBarrier` | Head briefly catches cursor mid-burst; pump waits idle window; primary writes again before window elapses; cursor resumes; barrier eventually fires only at true exhaustion. Default `idleWindow=100ms`; test uses tight `5ms` for speed. |
| `TestSender_StreamUntilHead_WALRecycledMidScan` | Primary's substrate trims past cursor mid-pump; sender returns `FailureWALRecycled` — pinning H5-class boundary. |
| `TestSender_StreamUntilHead_SubstrateIOError` | Substrate returns generic IO error mid-scan; sender wraps as `FailureSubstrate`. |
| `TestSender_StreamUntilHead_CtxCancelDuringIdleWindow` | ctx cancelled while pump sleeps in idle-window `time.After`; returns `FailureCancelled`. |

**KEEP** (unchanged — must continue passing byte-identical):

`TestE2E_RebuildHappyPath`, `TestE2E_RebuildWithLiveWritesDuringSession`, `TestRebuildSession_*`, all `TestCoordinator_*` except those touching `TryAdvanceToSteadyLive` (see §2.4).

**LOC delta**: -100 (deletes) +200 (adds) → net **+100 LOC** in sender_test.go.

### 2.3 `core/recovery/receiver.go` — monotonic-frame audit + new branches

**Current**: receiver decodes inbound frames, calls `r.session.ApplyWALEntry(kind, lba, data, lsn)` without checking if `lsn` is monotonic with respect to prior applied LSN. RebuildSession is intentionally permissive (`rebuild_session.go:99-102` notes the layer-1 stance).

**Per kickoff v0.2 §5 (architect round 2)**: receiver MUST enforce gap + backward checks at the wire boundary, not at substrate.

**Diff**:

| Action | Target | Notes |
|---|---|---|
| **ADD** | Field `appliedLSN uint64` to `Receiver` struct | Tracks highest LSN successfully passed to `session.ApplyWALEntry` |
| **MODIFY** | `Receiver.Run()` `case frameWALEntry` (~line 130 area) | Insert monotonic check BEFORE calling `session.ApplyWALEntry` |
| **ADD** | Helper method `checkMonotonic(lsn uint64) error` | Returns typed `*Failure` per §5 table |

The check:

```go
// Per kickoff v0.3 §5.1: receiver enforces wire-level monotonic LSN.
// Four cases (row split landed in kickoff v0.3 round 2):
//   - lsn == applied + 1: normal (the only legal forward step).
//   - lsn >  applied + 1: GAP — FailureContract; not silent skip.
//                         Only legal forward step is +1; sender MUST
//                         emit contiguous LSN. WAL-recycled gaps go
//                         through FailureWALRecycled at sender side.
//   - lsn == applied:     EXACT-DUPLICATE on the wire — FailureProtocol.
//                         TCP delivers in-order; recovery sender writes
//                         each LSN exactly once. NOT to be confused with
//                         §5.2 substrate-level per-LBA arbitration
//                         (INV-DUAL-LANE-WAL-WINS-BASE) — that's a
//                         different layer.
//   - lsn <  applied:     BACKWARD — FailureProtocol. Only legitimate
//                         "rewind" is session-level cursor:=pinLSN at
//                         a NEW SessionStart, never in-stream.
//
// Initialization at SessionStart (per Q13 ratified default):
// `r.appliedLSN := fromLSN` so the first frame's expected LSN is
// `fromLSN + 1` — sender's first emit after rewind. The watermark
// reading: "fromLSN is the already-installed-on-replica boundary".
func (r *Receiver) checkMonotonic(lsn uint64) error {
    expected := r.appliedLSN + 1
    if lsn == expected {
        return nil
    }
    if lsn > expected {
        return newFailure(FailureContract, PhaseRecvDispatch,
            fmt.Errorf("WAL gap: got LSN=%d, expected %d (applied=%d)",
                lsn, expected, r.appliedLSN))
    }
    // lsn <= applied: covers both exact-duplicate (lsn == applied)
    // and in-stream backward (lsn < applied). Both are FailureProtocol
    // per kickoff v0.3 §5.1; only the diagnostic message differs.
    if lsn == r.appliedLSN {
        return newFailure(FailureProtocol, PhaseRecvDispatch,
            fmt.Errorf("WAL exact-duplicate: got LSN=%d == applied=%d "+
                "(sender re-emit; never legitimate on the wire)",
                lsn, r.appliedLSN))
    }
    return newFailure(FailureProtocol, PhaseRecvDispatch,
        fmt.Errorf("WAL backward: got LSN=%d < applied=%d",
            lsn, r.appliedLSN))
}
```

After successful `ApplyWALEntry`, update `r.appliedLSN = lsn`.

**LOC delta**: ~+30 LOC in receiver.go.

### 2.4 `core/recovery/peer_ship_coordinator.go` — Phase enum collapse (Q3 ratified)

**Current**: 3-value `PeerShipPhase` enum (`Idle`, `DrainingHistorical`, `SteadyLiveAllowed`). `TryAdvanceToSteadyLive` is the explicit transition; `RouteLocalWrite` returns `RouteSessionLane` for both `DrainingHistorical` and `SteadyLiveAllowed`.

**Q3 ratified default**: collapse to `{Idle, Active}`. `TryAdvanceToSteadyLive` deletes; `RouteLocalWrite` returns `RouteSessionLane` for any non-Idle phase (which is exactly today's behavior — see existing test `TestCoordinator_RouteLocalWrite` line 114 in `peer_ship_coordinator_test.go`: phase=DrainingHistorical → SessionLane; phase=SteadyLiveAllowed → still SessionLane).

**Diff**:

| Action | Target | Notes |
|---|---|---|
| **MODIFY** | Enum constants (~lines 40-42) | `PhaseIdle`, `PhaseActive` (was: `PhaseIdle`, `PhaseDrainingHistorical`, `PhaseSteadyLiveAllowed`) |
| **MODIFY** | `String()` method (~lines 45-55) | Two cases instead of three |
| **DELETE** | `TryAdvanceToSteadyLive` method | Q3 ratified |
| **MODIFY** | `MarkBaseDone` | Stays — still useful for Layer-1 `TryComplete` conjunction. Does NOT advance phase. |
| **MODIFY** | `RecordShipped` (existing, advances `BacklogDrained`) | Stays — `BacklogDrained` flag survives because Layer-1 `TryComplete` still uses it. Phase is separate concept. |
| **MODIFY** | `RouteLocalWrite` (~line 200 area) | Two-case switch: `Idle` → `RouteSteadyLive`; otherwise → `RouteSessionLane` |
| **MODIFY** | All callers of `PhaseDrainingHistorical` / `PhaseSteadyLiveAllowed` | Replace with `PhaseActive` |
| **MODIFY** | `peer_ship_coordinator_test.go` | Update `TestCoordinator_PhaseTransitionRequiresDrainAndBaseDone` semantics: CHK-PHASE-NEVER-STEADY-BEFORE-DRAIN re-anchors per kickoff §6. May rename test. |

Subtle: `BacklogDrained` and `baseDone` flags are NOT removed — they're still input to Layer-1 `TryComplete` (`INV-SESSION-COMPLETE-ON-CONJUNCTION-LAYER1`). Only the **explicit phase transition** based on them disappears. The flags become diagnostic + gate inputs for `CanEmitSessionComplete`.

Updated CHK semantics (per kickoff §6):
- `CHK-PHASE-NEVER-STEADY-BEFORE-DRAIN` re-anchors as: "session active = SessionLane until EndSession" (no `Steady*` substate in active session).
- `CHK-NO-FAKE-LIVE-DURING-BACKLOG` re-anchors as: same — single rule.

**LOC delta**: ~-30 LOC in peer_ship_coordinator.go (deletion-heavy).

### 2.5 `core/transport/rebuild_sender.go` — workaround removal

**Current** (`rebuild_sender.go:138-167`): `FinishLiveWrites` called immediately after `StartRebuildSession` to match legacy "no live writes during rebuild" semantic. Comment explicitly says this is the §3.2 #3 workaround.

**Diff**:

| Action | Target | Notes |
|---|---|---|
| **DELETE** | Lines 138-144 (workaround comment) | No longer needed; can write a one-line "see kickoff" pointer if reviewer prefers |
| **DELETE** | Lines 164-167 (`FinishLiveWrites` call + race-handling) | Lifted: live writes during rebuild are now legal |

**LOC delta**: ~-15 LOC in rebuild_sender.go.

### 2.6 Files NOT touched

- `core/recovery/wire.go` — wire format is invariant per kickoff §4. Zero changes.
- `core/storage/memorywal/store.go`, `core/storage/walstore.go` — substrate untouched.
- `cmd/blockvolume/main.go`, `cmd/blockmaster/...` — wiring layer untouched.
- `core/replication/component/cluster.go` — no changes.

If the implementation PR touches any file outside this section without an explicit Round 2 question, that's a scope violation and the reviewer asks why.

---

## 3. New invariant pin — `INV-WAL-CURSOR-MONOTONIC-FROM-PINLSN`

**Where it lands** in `v3-recovery-inv-test-map.md`:

```
| `INV-WAL-CURSOR-MONOTONIC-FROM-PINLSN` | Sender's WAL pump rewinds
  cursor to fromLSN once at session start; cursor advances
  monotonically per ScanLBAs callback; never decreases. Receiver
  enforces matching wire-level monotonic discipline (gap = Contract;
  backward = Protocol). | Sender side: `TestSender_RewindOnce_
  CursorMonotonicForward`.<br>Receiver side: `TestReceiver_
  RejectsBackwardLSN_InSession` + `TestReceiver_RejectsGap_InSession`
  (both new in implementation PR). | ✅ pinned |
```

Goes in Layer 1 section (between INV-DUAL-LANE-WAL-WINS-BASE and INV-SESSION-COMPLETE-ON-CONJUNCTION-LAYER1) since it concerns receiver-side mechanism.

---

## 4. Test catalog summary

| Action | Sender | Receiver | Coord | Total |
|---|---|---|---|---|
| DELETE | 5 | 0 | 1 (PhaseTransition rename only) | 5-6 |
| ADD | 6 | 2 | 0 | 8 |
| KEEP | ~all others | all | most | — |

Net test LOC: **+~250** across recovery package.

Hardware canonical (`v3-recovery-dual-lane-canonical.yaml`) — **unchanged**. Kickoff §4 wire invariance + §6 INV preservation guarantees this. Mini-plan adds NO new canonical phase; existing phases re-run identically.

---

## 5. G-1 audit scope (V2 read)

Per `feedback_g1_pre_code_review.md`: muscle-path deletions still get a V2 read. The G-1 stage executes against the implementation branch BEFORE merge.

**G-1 reading list** (compact):

1. **V2 reference**: `weed/storage/blockvol/wal_shipper.go` and surrounding (the V2 wal_shipper that V3 ports). G-1 audits whether the v3 sender's deletion of `liveQueue + drainAndSeal` regresses any V2 invariant. Specifically — does V2 ever buffer live writes outside the WAL? If yes, document why V3's "no buffer" is correct (substrate carries it). If no, V2 alignment confirmed.

2. **V2 receiver discipline**: V2's wal-apply path enforces monotonic LSN how? G-1 confirms V3's `Receiver.checkMonotonic` matches or exceeds V2's strictness. (Memo `feedback_recovery_stale_entry_skip.md` — duplicate-stale-skip is per-LBA, not LSN-monotonic. Different axis.)

3. **Phase enum collapse vs V2**: V2 had no formal phase enum at this layer. G-1 confirms collapse doesn't lose a V2-tested invariant. Suspected: clean — V2's "session active or not" was implicit; V3's `{Idle, Active}` is a faithful port.

G-1 produces a `proceed-with-minor-patch` or `revise-mini-plan` verdict. If `revise-mini-plan`, this doc gets a v0.2 round.

---

## 6. LOC budget (refined)

| File | Delete | Add | Net |
|---|---|---|---|
| `core/recovery/sender.go` | ~120 | ~80 | **-40** |
| `core/recovery/sender_test.go` | ~100 | ~200 | **+100** |
| `core/recovery/receiver.go` | 0 | ~30 | **+30** |
| `core/recovery/receiver_test.go` | 0 | ~80 | **+80** |
| `core/recovery/peer_ship_coordinator.go` | ~40 | ~10 | **-30** |
| `core/recovery/peer_ship_coordinator_test.go` | ~20 | ~10 | **-10** |
| `core/transport/rebuild_sender.go` | ~15 | 0 | **-15** |
| `v3-recovery-inv-test-map.md` | 0 | ~5 | **+5** |
| **TOTAL** | **~295** | **~415** | **+120** (production: -55; tests: +175) |

Net production code: **-55 LOC** (deletion-heavy as predicted in kickoff). Test surface grows because the new invariants need explicit pins.

---

## 7. Risk mitigations spelled out (kickoff §8 references)

| Hazard | Kickoff ref | Mini-plan mitigation |
|---|---|---|
| H1 — `idleWindow` value | §8 | Default `100ms`; constructor parameter for tests + future tuning. Pinned by `TestSender_IdleWindow_NotPrematureBarrier`. |
| H2 — `ε` for kind-flip | §8 | Constant `kindFlipEpsilon = 8` LSNs per Q1. Inline-constant; not configurable in this milestone. Re-tunable in Option B if observability demands. |
| H3 — `coord.Phase` collapse | §8 | §2.4 spells out the exact diff. Existing tests audit. |
| H4 — Hardware canonical timeout | §8 | No expected change to canonical YAML. If `observe_recycle_gate_active` phase times out, mini-plan v0.2 round bumps timeout from `30s` → `60s`. |
| H5 — Substrate `ScanLBAs` perf | §8 | Mini-plan does NOT add streaming-API change. If implementation PR profiles show >50ms per scan iteration on production substrate, that's a new question (Q-perf in Round 2). |
| H6 — Receiver discipline behavior change | §8 | The new `checkMonotonic` is **defense-in-depth** for current production senders (which do not send backward in-stream). If a production sender turns out to backward-send under some race, that's a real bug surfaced; do NOT silently skip — fail-loud with FailureProtocol per kickoff §5. |

---

## 8. Sequencing (single PR vs split)

**Default**: single implementation PR off the (post-kickoff-merge) trunk tip. Reasons:
- File count is small (5 production files + 4 test files).
- Net production diff is deletion-heavy; reviews scale well.
- Splitting into "delete gate" + "add receiver discipline" creates an intermediate state where sender no longer buffers but receiver doesn't yet enforce monotonic — a moment when the discipline contract is asymmetric. Avoid.

**If reviewer prefers split**: cut `core/recovery/receiver.go` + `receiver_test.go` into a separate prefix PR (#NN-1), gate the sender PR (#NN-2) on it. Asymmetric-state risk is small in practice (no production sender backward-sends today), but reviewer is the judge.

**Branch base**: implementation branch off post-merged trunk (after both kickoff and mini-plan land in `phase-15`).

---

## 9. Open questions for architect at this stage

| Q | Topic | Default |
|---|-------|---------|
| **Q10** | Method name `streamUntilHead` vs alternatives (`pumpToHead`, `walPumpLoop`)? | `streamUntilHead` for symmetry with deleted `streamBacklog`; reviewer can rename. |
| **Q11** | `idleWindow` default value `100ms` — too tight, too loose, or fine? | `100ms` is the default; pinned by test using tighter `5ms` for speed. Configurable knob exists. |
| **Q12** | `kindFlipEpsilon = 8` ratified as Q1 — keep as inline constant, or expose as field for tests? | Inline constant in this milestone; expose if Option B observability demands. |
| **Q13** | Receiver's `appliedLSN` field name + initial value (0 vs fromLSN-1) — `lsn == applied+1` rule means initial `applied = fromLSN` so first frame's `lsn = fromLSN+1`. Clear? | Initial `applied = fromLSN` (since `fromLSN` itself is the pinLSN already-applied watermark). |
| **Q14** | Single PR or split (gate-deletion + receiver-discipline)? | Single per §8. |
| **Q15** | After implementation merges, is the unified-wal-kickoff doc itself archived (moved to `sw-block/design/archive/`) or kept in `sw-block/design/`? | Kept; doc is the design source-of-truth for the mechanism, not a one-shot kickoff transcript. |

Q10-Q15 are mini-plan-stage refinements. If architect ratifies-with-defaults, the implementation PR proceeds.

---

## 10. Resolution log

### Round 1 — 2026-04-29 (architect ratification)

**Q10-Q15 ratified-with-defaults**: all six refinement questions accepted at their default positions. No mini-plan content change for Q10-Q15 themselves.

**§2.3 comment fix (the one real edit in v0.2)**: v0.1 `checkMonotonic` comment block said "First WAL entry of session: applied is 0" — this contradicted Q13's ratified default `appliedLSN := fromLSN`. The two cannot coexist except in the trivial case `fromLSN == 0`. v0.2 corrects the comment to describe the watermark form ("`r.appliedLSN := fromLSN` so the first frame's expected LSN is `fromLSN + 1`"). Cross-references the kickoff v0.3 §5.1 row split.

**§5.1 row split adoption**: kickoff v0.3 round 2 split `lsn ≤ applied` into two distinct rows (`lsn == applied` exact-duplicate; `lsn < applied` backward). v0.2 `checkMonotonic` body updated to the four-case shape; both exact-duplicate and backward map to `FailureProtocol` (different diagnostic messages, same failure class), so the engine-side branching does not change. No additional implementation cost.

**Implementation may proceed to G-1**. Per architect Round 1: mini-plan v0.2 closes the design surface; G-1 stage opens next on the implementation branch with the §5 V2 reading list as the audit anchor.

### Round 2 — (pending; only if G-1 surfaces a `revise-mini-plan` verdict)

Empty unless G-1 finds a V2-alignment regression that requires diff revision.
