# Recovery — Unified WAL stream / cursor-rewind kickoff (v0.3)

**Status**: kickoff v0.3 — round 2 erratum 2026-04-29 logged in §11. Round 1 ratified all Q1-Q7 with defaults (+Q8 follow-on, +Q9 receiver discipline). Round 2 corrects two read-time contradictions surfaced by architect re-read after mini-plan v0.1; **no invariant or scope changes**, documentation-only. Code-go-ahead **still gated** on mini-plan v0.2 ratification per governance sequence (`feedback_t4d_governance_sequence.md`).

**Branch target**: `g7-redo/unified-wal-kickoff` (this branch carries the doc; implementation branch is a sibling once mini-plan ratifies).

**Trunk base**: `phase-15` tip `afec29d` — train (#11/#13/#14/#15/#16) is in trunk; kickoff sits on the merged dual-lane recovery foundation rather than a moving base.

**Ledger pin (forward)**: this work closes `INV-WAL-CURSOR-MONOTONIC-FROM-PINLSN` (Q2 ratified — short name; "monotonic-from-pinLSN" says the load-bearing thing without inviting "tracked window" overhead).

**Working name**: "Unified WAL stream / cursor-rewind to pinLSN" (Q4 ratified). Spec text "§3.2 #3 single-queue real-time interleave" stays in citations to other docs.

---

## 1. Why this exists — the framing flip

The spec calls this `§3.2 #3 single-queue real-time interleave`. "Interleave" reads as "two streams mixed", and the POC literally implements that: a backlog stream meets a live-write queue at a `drainAndSeal` step. Architect 2026-04-29 framing flip — quoted verbatim, design-quotable:

> 单队列交织 ≠ 两段物理 phase。

The follow-on architect clarification (round 1 ratification) sharpens the working description without leaning on "sliding window" abstraction:

> WAL pump 在开始 rewind 到 pinLSN，再向前走到 head——这就够描述 §3.2 #3 相对 POC 的差别。

So: **a single session, single WAL queue. The only special operation is `cursor := pinLSN` once at session start. After that, the cursor is monotonically increasing — same shape as steady-state shipping.** No "sliding window" abstraction; no tracked bandwidth; just one rewind and a forward walk.

The work this milestone delivers is therefore:

1. Add the **rewind-to-pinLSN** at session start (where today the recovery sender opens a separate scan path).
2. **Delete** the two-phase gate (`liveQueue` / `sealed` / `PushLiveWrite` / `drainAndSeal` / explicit `TryAdvanceToSteadyLive`).
3. Replace with a single forward loop. Optionally tag frames with a `Kind=Backlog→SessionLive` byte that flips once when the cursor catches `head` (observability only — see §4).

---

## 2. Current shape — the gate that's being deleted

Trunk tip `afec29d` carries the train, including `core/recovery/sender.go`. The two-phase gate is concentrated in five concrete artifacts:

| Artifact | Role | Disposition |
|---|---|---|
| `Sender.liveQueue []walItem` field (~line 65) | Separate buffer for live writes pushed during backlog phase | **DELETE** |
| `Sender.sealed bool` flag + `Sender.queueMu sync.Mutex` | Atomic-seal protection for the live queue | **DELETE** (no buffer = no seal needed) |
| `Sender.PushLiveWrite(lba, lsn, data) error` API (~line 146) | Caller-side push-into-queue model | **DELETE** (writes arrive naturally via substrate's growing tail) |
| `Sender.Run()` step 5 — `<-closeCh` wait (~line 254) | Caller signals "no more live writes" before barrier | **REPLACE** with "cursor reached head + idle window elapsed" |
| `Sender.drainAndSeal()` method (~line 432) + `coord.TryAdvanceToSteadyLive()` step (~line 273) | Drains buffered live writes; flips coordinator phase | **DELETE** (transition becomes implicit: kind-byte flip when `head - cursor < ε`) |

POC author's own admission at sender.go line 141-145:

> Live writes are buffered until the backlog drain phase finishes, then flushed in LSN order before barrier. **POC simplification — in production the sender would interleave them with backlog by LSN order in real time.**

The wiring layer (`core/transport/rebuild_sender.go:138-144`) explicitly notes the workaround is current-binary scope; the `FinishLiveWrites` early-call at line 164-167 is the production-side workaround that becomes obsolete once §3.2 #3 lands.

The gate is architecturally wrong but **operationally harmless today** — `liveQueue` is empty at `drainAndSeal` time because `FinishLiveWrites` is called before any live write can land. Lifting the restriction (= live writes during rebuild become legal) requires deletion of the gate.

---

## 3. Target shape — one cursor, one rewind, monotonic forward

### 3.1 Sender lifecycle (after the deletion)

Sender's WAL-lane work becomes:

```
1. cursor := pinLSN                       // the one rewind
2. ship base lane (extent dump, parallel)
3. send frameBaseDone
4. loop:
     scan ScanLBAs(cursor, fn) where
       fn(entry):
         emit frameWALEntry{Kind=kind, LBA, LSN, data}
         cursor = entry.LSN
         // Kind-flip rule (§4)
         _, _, head := primaryStore.Boundaries()
         if kind == Backlog && head - cursor < ε:
           kind = SessionLive
   until cursorAtHead(cursor) AND idleWindowElapsed()
5. send frameBarrierReq
6. read frameBarrierResp; verify AchievedLSN ≥ targetLSN
```

`cursor` is just `uint64`. It starts at `pinLSN` (the one and only rewind), increases monotonically per `ScanLBAs` callback, never decreases. There is no separate "window" or "bandwidth" or "queue" — just an integer that advances.

**Naming note**: `pinLSN` (this doc's design term) and `fromLSN` (sender code's existing field name carrying the same value into the wire as `frameSessionStart.FromLSN`) refer to the same number within a session — the session's already-installed-on-replica watermark, the lower bound of the rewind. Implementation uses `fromLSN`; design text uses `pinLSN` for emphasis on the architect framing. Mini-plan Q13's `appliedLSN := fromLSN` initializer is consistent with this identity.

### 3.2 idleWindow guard (not a convergence proof)

The pump exits when (a) `cursor == head` at the moment the scan exhausts AND (b) an idle window passes without new appends. This is a **don't-barrier-mid-burst** guard. It is **not** a convergence proof — only barrier-ack with `AchievedLSN ≥ targetLSN` proves convergence (`INV-LIVE-CAUGHT-UP-IFF-FRONTIER-AT-BARRIER`). If the pump exits prematurely (rare: idle window elapses but a write lands during barrier round-trip), the barrier returns `AchievedLSN < targetLSN` and the session fails as `FailureContract` per existing taxonomy.

### 3.3 Why this isn't a "sliding window" abstraction

The v0.1 of this kickoff used "sliding window over `[pinLSN, head)`" as the framing. Architect ratification 2026-04-29 round 1 sharpened: the v0.1 phrase invites readers to think there's window-state to maintain (bandwidth, width, slide rules). There isn't. There's one `uint64` that goes up, plus an idle-window check at exit. Sharper text adopted: **rewind-once + monotonic forward**.

(For contributors who want the geometric metaphor as a private mental model, "cursor walks `[pinLSN, head)` while the right edge moves" is fine — but should not appear in code comments or public design text, where the literal "rewind once, increment, exit when stuck" is shorter.)

---

## 4. Wire format invariance

**The wire is byte-identical from the replica's perspective.** Headline architectural claim:

| Frame type | Today | After §3.2 #3 |
|---|---|---|
| `frameSessionStart` (1) | `[8 SessionID][8 FromLSN][8 TargetLSN][4 NumBlocks]` | **unchanged** |
| `frameBaseBlock` (2) | `[4 LBA][block]` | **unchanged** |
| `frameBaseDone` (3) | empty | **unchanged** |
| `frameWALEntry` (4) | `[1 Kind][4 LBA][8 LSN][block]` | **unchanged framing**; **transition rule** for `Kind` byte changes (see 4.1) |
| `frameBarrierReq` (5) | empty | **unchanged** |
| `frameBarrierResp` (6) | `[8 AchievedLSN]` | **unchanged** |
| `frameBaseBatchAck` (7) | `[8 SessionID][8 AcknowledgedLSN][4 BaseLBAUpper]` | **unchanged** |

Receiver-side **wire decode/encode** (`frameWALEntry` layout, `WALEntryKind` byte values, `decodeWALEntry`) requires **zero changes**. Receiver-side **apply path** gets a NEW monotonic-discipline check (§5) — non-zero LOC. The two are different layers: the wire is invariant; the validation around `session.ApplyWALEntry` gets a new branch. The hardware canonical (`v3-recovery-dual-lane-canonical.yaml`) re-runs unmodified — only the kind-byte distribution in any frame-capture artifact (`dual_lane.pcap` or equivalent) changes.

### 4.1 Kind byte — the only behavioral change on the wire

Today: kind transitions at `drainAndSeal` boundary (explicit step in sender's state machine).

After: kind transitions when `head - cursor < ε`. Once flipped to `SessionLive`, never flips back.

**Q1 ratified default**: `ε = 8 LSNs`. Constant; observability-only (does not affect any correctness invariant).

---

## 5. Receiver frame discipline (monotonic LSN contract)

Per architect 2026-04-29 round 2, the receiver's monotonic-LSN expectation needs explicit handling rules. The sender's deletion work does NOT loosen the receiver contract; it tightens it (no more `drainAndSeal` boundary that hides ordering glitches).

### 5.1 Three frame-arrival cases

For receiver state `applied = highest LSN durably applied so far`, define `next.LSN` as the LSN on the next inbound `frameWALEntry`:

| Case | Action | Failure class | Notes |
|---|---|---|---|
| `next.LSN == applied + 1` | **Apply normally** | — | Steady-state path; the only legal forward step |
| `next.LSN > applied + 1` (gap) | **Reject — fail-loud** | `FailureContract` | Hole in LSN sequence; primary did NOT send a contiguous stream. NOT a silent skip. Not allowed even in recover. WAL-recycled gaps go through `FailureWALRecycled` + new lineage, not via this path. |
| `next.LSN == applied` (exact duplicate on the wire) | **Reject — fail-loud** | `FailureProtocol` | Sender does not retransmit the same LSN within a session. TCP delivers in-order; recovery sender writes each LSN exactly once; this case shouldn't happen and indicates a real sender bug if it does. NOT to be confused with §5.2 duplicate-LSN claim semantics — those are about base-vs-WAL arbitration on the receiver, not wire retransmits. |
| `next.LSN < applied` (in-stream backward) | **Reject — fail-loud** | `FailureProtocol` | Backward LSN in same session/same lineage = protocol violation. The ONLY legitimate "rewind" is session-level (`cursor := pinLSN` at SessionStart, observable as the new session's `frameSessionStart`). NOT a normal recover branch. |

### 5.2 Duplicate-LSN claim semantics — different layer than §5.1

The "skip data, update bitmap" pattern is a **per-LBA arbitration** concern at the substrate / bitmap layer (`INV-DUAL-LANE-WAL-WINS-BASE`), NOT a wire-frame ordering concern. Concretely:

> The base lane and the WAL lane both touch the same LBA. The WAL lane writes LSN=N for LBA L; later, the base lane delivers L's snapshot bytes (which were captured at LSN ≤ N at session start). Without arbitration the base lane would clobber the WAL-won bytes. The bitmap records `MarkApplied(L)` when the WAL lane wins, and base-lane handlers skip `L` thereafter. This is per-LBA last-writer-wins by LSN, not a wire-frame gate.

This **does not contradict §5.1**. §5.1 is wire-level (frame ordering on a single TCP session); §5.2 is substrate-level (per-LBA arbitration across two lanes). The two layers do not see each other's "duplicates":
- Same-LSN frame on the wire: §5.1, Protocol error (sender bug; never legitimate).
- Same-LBA write at lower LSN at the substrate after a higher-LSN write already applied: §5.2, no-op + bitmap-claim already in place.

Mini-plan §2.3's `checkMonotonic` implements §5.1 only. §5.2 is existing receiver-side bitmap behavior; this milestone does not change it.

### 5.3 The only legitimate "rewind" in recover

`cursor := pinLSN` happens **once**, at session start, on the **sender** side. The replica observes a `frameSessionStart` followed by frames whose LSNs start at `pinLSN + 1` and increase monotonically. The replica never observes "LSN goes down" within one session.

If a session ends and a new session begins (new `frameSessionStart` with new `SessionID`), the new session's `FromLSN` may be lower than the prior session's last applied LSN — but that's a **new session boundary**, not an in-stream rewind. The receiver's `applied` cursor resets per session (or is tracked per-lineage); existing receiver code already handles this.

### 5.4 Implementation note

`core/recovery/receiver.go` currently enforces some-but-not-all of these rules. Mini-plan stage audits the receiver's frame-handling for explicit gap/backward/duplicate branches. **Only NEW assertion in mini-plan: explicit FailureProtocol on `next.LSN ≤ applied` if not already pinned.** No code change to add a "skip data + bitmap update for backward frames" path — that was conflated logic, now explicitly disallowed.

---

## 6. Invariants preserved (no regression)

The deletion work must preserve every INV currently pinned. Audit:

| INV | How current shape pins it | How target shape pins it |
|---|---|---|
| `INV-PIN-EXISTS-ONLY-DURING-SESSION` | `coord.StartSession`/`EndSession` bracket | unchanged — pump runs inside same bracket |
| `INV-PIN-ADVANCES-ONLY-ON-REPLICA-ACK` | `BaseBatchAck` (frame 7) → `coord.SetPinFloor` | unchanged — reader-loop and ack handling untouched |
| `INV-PIN-COMPATIBLE-WITH-RETENTION` | `coord.SetPinFloor` rejects `floor < primaryS` | unchanged |
| `INV-RECYCLE-GATED-BY-MIN-ACTIVE-PIN` | walstore + memorywal recycle gates consult `MinPinAcrossActiveSessions` | unchanged |
| `INV-SESSION-COMPLETE-CLOSURE` | barrier-ack with `AchievedLSN ≥ targetLSN` + layer-1 `TryComplete` | unchanged |
| `INV-LIVE-CAUGHT-UP-IFF-FRONTIER-AT-BARRIER` | only barrier proves convergence | **strengthened** — pump's idle-window is explicitly NOT a convergence proof; only barrier counts (§3.2) |
| `INV-DUAL-LANE-WAL-WINS-BASE` | bitmap arbitration receiver-side | unchanged — duplicate-LSN claim path (§5.2) honors this |
| `INV-SESSION-COMPLETE-ON-CONJUNCTION-LAYER1` | `TryComplete` requires `baseDone ∧ walApplied ≥ targetLSN` | unchanged |
| `INV-BITMAP-NO-INDEPENDENT-LOCK` | bitmap serialized via session mutex | unchanged |
| `INV-SINGLE-FLIGHT-PER-REPLICA` | coord rejects concurrent sessions | unchanged |
| `INV-SESSION-TEARDOWN-IS-EXPLICIT` | `coord.EndSession` runs in defer | unchanged |
| `INV-PIN-STABLE-WITHIN-SESSION` | monotonic SetPinFloor | unchanged |
| `INV-WAL-CURSOR-MONOTONIC-FROM-PINLSN` (NEW) | n/a (added by this milestone) | sender pump enforces monotonic increase from pinLSN; receiver enforces monotonic-applied (§5) |
| `CHK-PHASE-NEVER-STEADY-BEFORE-DRAIN` | `TryAdvanceToSteadyLive` requires `BacklogDrained ∧ baseDone` | **affected** — Q3 ratified default: collapse `Phase` enum to `{Idle, Active}`. Phase semantic recasts to "active session ⇒ ack-driven pin advancement; routing decisions move into kind-byte flip rule" |
| `CHK-BARRIER-BEFORE-CLOSE` | no `SessionClosedCompleted` without barrier | unchanged |
| `CHK-NO-FAKE-LIVE-DURING-BACKLOG` | RouteLocalWrite returns SessionLane during DrainingHistorical | **affected** — Q3 default: replaced by "session active = SessionLane until EndSession", since there is no "during backlog" sub-state once the gate is deleted |

Q3 ratified default: collapse `Phase` enum. CHK-PHASE-NEVER-STEADY-BEFORE-DRAIN and CHK-NO-FAKE-LIVE-DURING-BACKLOG re-anchor to the new {Idle, Active} semantic — mini-plan owns the diff.

---

## 7. Implementation plan (high-level — mini-plan owns the diff)

### 7.1 Files touched

- `core/recovery/sender.go` — DELETE the gate; ADD `streamUntilHead()` (or equivalent — name is bikeshed). Net: **~-40 LOC** (delete ~120, add ~80).
- `core/recovery/sender_test.go` — DELETE PushLiveWrite-path tests; ADD monotonic-cursor + rewind-once + idle-window tests.
- `core/recovery/peer_ship_coordinator.go` — Q3 default: collapse `Phase` enum to `{Idle, Active}`, drop `TryAdvanceToSteadyLive`. Update `RouteLocalWrite` accordingly.
- `core/transport/rebuild_sender.go` — DELETE the `FinishLiveWrites` early-call workaround at line 164-167.
- `core/recovery/wire.go` — **no changes** (wire format invariant; only kind-flip rule is sender-internal).
- `core/recovery/receiver.go` — verify monotonic-LSN discipline (§5); add explicit `FailureProtocol` branch for `next.LSN ≤ applied` if not already present.

### 7.2 Test surface

Existing tests must continue to pass byte-identical: `TestE2E_RebuildHappyPath`, `TestE2E_RebuildWithLiveWritesDuringSession`, `TestRebuildSession_*`, `TestCoordinator_*`.

New tests:
- `TestSender_RewindOnce_CursorMonotonicForward` — primary writes during pump; cursor follows; barrier hits expected `AchievedLSN`.
- `TestSender_KindByte_FlipsOnceAtCatchUp` — sequence shows monotonic Backlog→SessionLive transition; never flips back.
- `TestSender_IdleWindow_NotPrematureBarrier` — head briefly catches cursor mid-burst; pump waits idle window; primary writes again; cursor resumes; barrier eventually fires only at true exhaustion.
- `TestReceiver_RejectsBackwardLSN_InSession` — synthesized inbound frame with `LSN ≤ applied` produces typed `FailureProtocol`; receiver does NOT silently skip or rewind.
- `TestReceiver_RejectsGap_InSession` — synthesized inbound frame with `LSN > applied + 1` produces typed `FailureContract`.

### 7.3 LOC / complexity estimate

- Production: net **~-40 LOC** (deletion-heavy).
- Tests: **+~150 LOC** (new pump tests; old PushLiveWrite tests deleted; new receiver-discipline tests).
- Dependent file count: **5** (sender, sender_test, coord, rebuild_sender, receiver).

---

## 8. Risks & open hazards

| # | Hazard | Mitigation |
|---|--------|-----------|
| H1 | `idleWindow` value is a tuning knob — too small barriers prematurely under sustained-write workload; too large stalls otherwise-complete sessions | Mini-plan: configurable; default `100ms`; pin `TestSender_IdleWindow_NotPrematureBarrier` |
| H2 | `ε` for kind-flip — observability assertions counting Backlog vs SessionLive frames could regress | Q1 ratified `ε = 8`; observability contract states kind transition is **monotonic** but exact split-point is not pinned |
| H3 | `coord.Phase` semantic collapse — Q3 ratified | Mini-plan owns the enum change; integration tests audit |
| H4 | Hardware canonical's `observe_recycle_gate_active` phase — pump runs longer (serves live writes); checkpoint clamping window extends | Mini-plan reviews canonical YAML; minor edit if needed |
| H5 | Substrate `ScanLBAs` called repeatedly — performance under heavy load | Mini-plan benchmarks; may require ScanLBAs streaming/cursor API extension |
| H6 (NEW) | Receiver monotonic-frame discipline — if existing receiver doesn't ALREADY enforce backward-frame rejection, it's a behavioral change observable to senders that incorrectly retransmit | Mini-plan audits `receiver.go`; if missing, the new branch becomes a **defense-in-depth** addition, not an observable change for current production senders (which never send backward in-stream) |

---

## 9. Open questions for architect (round 1 status)

| Q | Topic | Default | Round 1 |
|---|-------|---------|---------|
| **Q1** | `ε` for kind-flip | constant `ε=8` LSNs | **RATIFIED** |
| **Q2** | Invariant name | `INV-WAL-CURSOR-MONOTONIC-FROM-PINLSN` | **RATIFIED** (short form per architect; see §11) |
| **Q3** | Coordinator `Phase` enum after `TryAdvanceToSteadyLive` removal | collapse to `{Idle, Active}` | **RATIFIED** |
| **Q4** | Milestone working name | "Unified WAL stream / cursor-rewind to pinLSN" | **RATIFIED** |
| **Q5** | Implementation governance | standard (mini-plan → architect → G-1 → code) | **RATIFIED** |
| **Q6** | Mini-plan scope | include `coord.Phase` redesign | **RATIFIED** |
| **Q7** | Hardware canonical impact | minimal (only timeout tuning) | **RATIFIED** |
| **Q8** (NEW) | Merge with production WalShipper (one cursor mechanism for steady-state + recover) | **follow-on milestone B** — NOT this PR | **RATIFIED** as follow-on |
| **Q9** (NEW) | Receiver monotonic-frame discipline (§5) | architect-provided table; pin via 2 new tests | **RATIFIED** (architect provided the discipline; mini-plan implements) |

All Q1-Q9 ratified or ratified-with-defaults. Mini-plan stage may surface more questions; new questions get appended (Q10+) at that stage.

---

## 10. Out of scope (explicit)

- **Production WalShipper merge** (Q8, follow-on milestone B). This kickoff explicitly does NOT collapse the steady-state (push) and recovery (pull) shipping paths into one cursor mechanism. That is a separate kickoff/PR.
- **Option B retry policy** (per-failure-kind budget + lossless mapping + observability): separate kickoff.
- **Backoff / wall-clock retry cap**: separate milestone from Option B.
- **Multi-volume / multi-replica (RF>2) interleave stress**: hardware canonical covers RF2; RF3 is a follow-on.
- **Substrate `ScanLBAs` streaming API**: H5 mitigation may surface a need; mini-plan flags it. Not gating §3.2 #3.
- **Substrate per-entry LSN assumption**: this milestone assumes the WAL substrate (`memorywal.Store`, `walstore.WALStore`) emits each `RecoveryEntry` with its **write-time LSN** — every callback in `ScanLBAs` carries a strictly-increasing `entry.LSN`. Substrates that synthesize a scan-time LSN (e.g., the in-memory `BlockStore` discussed in earlier POC reviews) are out of scope. The recovery-execution path uses MemoryWAL or WalStore; BlockStore is not on this milestone's substrate list.
- **Removing legacy mode entirely**: post-default-flip + ≥1 release cycle (per Open Question 4 of `v3-recovery-wiring-plan.md`). Independent of §3.2 #3.
- **Metric/log surface for kind transition**: nice-to-have for Option B.

---

## 11. Resolution log

### Round 1 — 2026-04-29 (architect ratification)

**Naming change**: v0.1 used "sliding-window" framing throughout. Architect: "不必死守『滑动窗』隐喻；rewind-once + 单调 cursor 就够". Adopted **rewind-to-pinLSN + monotonic forward** as the standard text. "Sliding window" appears in §3.3 only as an explicit private-mental-model note, not in code/design.

**Q1-Q7 ratified-with-defaults**: defaults stand.

**Q8 added**: "merge recovery sender's WAL pump with production WalShipper into one cursor mechanism" — architect ratified as **follow-on milestone B**, NOT this kickoff/PR. Default rationale: the gate-deletion in §2 is the focused work; full shipper unification is a deeper architectural change that would inflate scope and slow §3.2 #3 closure.

**Q9 added**: receiver monotonic-frame discipline (§5). Architect provided the gap/backward/duplicate handling table directly; mini-plan implements as audit + missing branches.

**INV name (Q2)**: ratified as `INV-WAL-CURSOR-MONOTONIC-FROM-PINLSN` — shorter than the v0.1 `INV-WAL-STREAM-IS-ONE-CURSOR-OVER-RETAINED-WINDOW`. Architect: "比 LONG retained-window 更贴代码".

**Branch cleanup**: architect green-lit deletion of merged-content branches (`g7-redo/pin-floor`, `wiring`, `recycle-pin-hookup`, `retry-fix-pin-under-retention`). Convention: feat branches deleted post-merge. Tags in `archive/g7-phase15-attempt-*` are intentional bisect ground; do NOT touch.

**Governance (Q5)**: standard — mini-plan → architect → G-1 → code. Round 1 closes the kickoff stage; mini-plan stage opens next.

### Round 2 — 2026-04-29 (kickoff erratum aligned with mini-plan v0.1)

Architect re-read after mini-plan v0.1. Two contradictions / easy-to-misread surfaces flagged in v0.2 + corrected here as v0.3:

**(A) "Receiver code requires zero changes" was overclaim**. §4 paragraph said receiver.go gets zero changes; §7.1 (and mini-plan §2.3) said receiver gets a new `checkMonotonic` branch. Resolved by distinguishing layers: **wire decode/encode** unchanged (frame layout, kind values, decodeWALEntry); **apply path** gets the new monotonic discipline check (non-zero LOC). §4 paragraph rewritten to make this explicit.

**(B) `lsn == applied` was implicit in `≤ applied`** but ambiguous against §5.2 duplicate-LSN claim semantics. v0.2 §5.1 lumped `≤ applied` under one "backward → Protocol" row; reader could read this as conflicting with §5.2 "duplicates are legitimate at-least-once". Resolved by splitting the row in §5.1: `== applied` (exact wire-level duplicate) is `FailureProtocol` separately from `< applied` (in-stream backward) which is also `FailureProtocol`. §5.2 rewritten to clarify it's substrate-level per-LBA arbitration, NOT wire-frame gating, and explicitly does NOT contradict §5.1. Mini-plan §2.3 `checkMonotonic` implements §5.1 only.

**Naming consistency**: explicit note added to §3.1 — `pinLSN` (design text) ≡ `fromLSN` (code field). Aligns with mini-plan Q13's `appliedLSN := fromLSN` initializer.

**Substrate LSN assumption**: §10 OOS adds explicit note — this milestone assumes per-entry write-time LSN from MemoryWAL/WALStore; BlockStore-style synthetic scan-time LSN is out of scope.

**dual_lane.pcap reference**: softened to "frame-capture artifact (e.g. `dual_lane.pcap`)" since the canonical may produce equivalent capture under a different filename.

No invariant changes. No question changes. No code changes; this is documentation-only erratum. Mini-plan v0.2 (in companion branch) carries the matching `appliedLSN` initializer fix.
