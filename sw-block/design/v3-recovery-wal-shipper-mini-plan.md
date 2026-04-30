# WalShipper implementation mini-plan (**PR bridge**)

**Status**: Draft — executable sequence from **`v3-recovery-wal-shipper-spec.md`** to **`seaweed_block`** concrete diffs.  
**Supersedes narrative scope of**: **`v3-recovery-unified-wal-stream-mini-plan.md` §3.2 (#3 unified stream implementation)** — that track treated recovery `sender` as the Wal scheduler; **non-compliant** with consensus **§6** + wal-shipper-spec **INV-NO-DOUBLE-LIVE**. Architect may add formal **SUPERSEDED** banners on kickoff/unified docs; **this file does not block on that.**

**Normative**: **`v3-recovery-wal-shipper-spec.md`** (**§2–§§7**) + **`v3-recovery-algorithm-consensus.md`** **§I / §II §6**.

**Anti‑archive branch**: **`g7-redo/unified-wal-impl`** — **do not PR** as WalShipper truth; cherry-pick only **tests/fixtures** if still valid after refactor audit.

---

## 0. Outcome (**definition of done**)

1. **One Wal emit decision stream per `(volume identity, replicaID)`** on Primary — **cursor + shipMu + spec §4**.  
2. **Steady path** (`ReplicaPeer` / **`BlockExecutor.Ship`**) and **recover Wal path** (**dual‑lane frames**) **both** funnel through **that** scheduler — **`INV-NO-DOUBLE-LIVE`**.  
3. **`core/recovery/sender.go`** (**`Sender`**) is **thin**: session bracket, **base lane** (`streamBase`/barrier taxonomy), **`Run` choreography** — **no** independent `cursor` / `streamUntilHead` competing with **`Ship()`**.  
4. **`R1` double‑check** and **`R2` lag hook** present per **spec §§5–6**.  
5. **`§9` tests** mapped below — **`CHK-WALSHIPPER-SINGLE-CURSOR` / `NO-GAP-R1` proven at unit/concurrency scope** — *not* as the sole reliance on flaky multi‑second integration (architect constraint).

---

## 1. Where code lives (**seaweed_block** anchors)

| Component | Primary file(s) (**today**) | Target responsibility |
|-----------|----------------------------|------------------------|
| **Stateful WalShipper** | *new* **`core/transport/wal_shipper.go`** | Holds **`cursor`**, **`shipMu`**, session **`fromLSN`**, **`NotifyAppend`** / **`DrainBacklog`**, **`OnShipTimer`**. Emits via injected **`EmitFunc`** (mock in P0) |
| **Steady live ship** | **`core/transport/ship_sender.go`** — **`BlockExecutor.Ship`**| **Delegates emit** to per‑replica **WalShipper** (same mutex family as append path — **INV-SINGLE**) — **never** standalone encode+write concurrent with backlog pump |
| **Executor registry** | **`core/transport/executor.go`** **`sessions`** map | Registers **`WalShipper` per replica** keyed **stable ID** (**volume + ReplicaID**) — satisfies **INV-SINGLE** dedup (**spec §2 forbids** dual goroutines advancing cursor) |
| **Recovery orchestration** | **`core/recovery/sender.go`**| Drops independent Wal pump → **`WalShipper.DrainBacklog(fromLSN, ctx)`** (or **`BeginRecoverSession`** + **cursor reset**) — skeleton P0; wired P2 |
| **Local Wal append ingress (**steady**)** | **Chain below** terminates in **`core/transport/ship_sender.go`::`BlockExecutor.Ship`** (**P1**)| **Recommended hook**: **`Ship` delegates** to **`WalShipper.NotifyAppend`** (or **`EmitAccordingTo§4`**). **`ReplicationVolume.OnLocalWrite` / `ReplicaPeer.ShipEntry` signatures unchanged** |

### 1.1 Steady-path write fan‑out (**`seaweed_block`**, verified)

**Call chain (4 hops, **P0** leaves this chain untouched):**

```
core/replication/volume.go:421     ReplicationVolume.OnLocalWrite(...)
   → iterates v.peers, peer.ShipEntry(...) per replica

core/replication/peer.go:459       ReplicaPeer.ShipEntry(...)
   → state gate, builds peer lineage

core/replication/peer.go:480       p.executor.Ship(p.target.ReplicaID, peerLineage, lba, lsn, data)

core/transport/ship_sender.go:58   BlockExecutor.Ship(...)
   → session lookup, lazy-dial, WriteMsg(MsgShipEntry / SWRP)
```

**P1 delegation rule**: add **`walShippers map[string]*WalShipper`** keyed by **`ReplicaID`** (or richer key if volumes share executor — reviewer confirms). **`BlockExecutor.Ship(...)` → `WalShipper` emits** via injected **`EmitFunc`** encoding **`MsgShipEntry`** — single live wire path (**INV‑NO‑DOUBLE‑LIVE** prelude).

**P0 note**: **`WalShipper`** type lives in **`wal_shipper.go`** with mocks only — **does not alter** this chain until **P1**.

*§2 design choice (**`shipMu`** vs actor) applies where **`WalShipper`** merges **`NotifyAppend`** / timer / drain.*

---

## 2. Architectural choice (**serialize append vs ship**)

Pick **before coding** (mini‑plan reviewer sign‑off):

- **Choice A (`shipMu`)** — **Broad `shipMu`**: append callback that notifies shipper acquires **`shipMu`** after `lsn` fixed, bumps **`head`**, runs **§4 emit loop** excerpt. *Simplest for R1 double‑check.*

- **Choice B (actor queue)** — Single goroutine consumes `{Append, ShipOpportunity}` messages; **no `shipMu`** but **FIFO** proves **INV-SINGLE**. *Harder glue to existing `Ship` callers.*

Default recommendation: **A** for **minimal moving parts** in first merge.

---

## 3. Phased rollout (**merged PR chunks**)

| Phase | Content | Receipt |
|-------|---------|---------|
| **P0** | Introduce **`WalShipper`** type + **`NewWalShipperForReplica(...)`**; **pure unit tests**: **INV-MONOTONIC-CURSOR**, **INV-SUBSET** (fake `head`/`emit` mocks) | Green package tests without executor |
| **P1** | **`BlockExecutor`** registry: create/destroy **`WalShipper`** with **`StartRebuild`/`EndSession`/steady attach**; **`Ship`** delegates **`Emit`** | Existing **`ship_sender_test` / replicated path** unchanged or extended |
| **P2** | **Drain API** wired from **`recovery.Sender`**; **delete**/disable **`recovery/sender`** duplicate pump | **`e2e_test` / stub** revived only if semantics unchanged |
| **P3** | **§5 R1** **`AssertCaughtUp...`** + **§6 `lag`/SignalSaturation** ( thresholds **from config/constants** ) | **Test‑R1-Flip-NoGap**: **stress** goroutine concurrent append (**unit**, `-race`) |
| **P4** | Cleanup dead code (**unified-wal-impl** ports), **`docs` supersede headers** (#architect tooling) | Consensus **G0** checklist ticked |

Phases **P0–P1** safe to merge before full recovery deletes if **INV-NO-DOUBLE-LIVE** holds by construction (**only WalShipper calls wire write**).

---

## 4. Spec ↔ invariant ↔ test (**§9 wal-shipper-spec expanded**)

Wal-shipper-spec **§9** names coarse tests — here map **package + style**:

| Spec ref | Minimal test (**unit first**) |
|----------|-------------------------------|
| **INV-SINGLE** | **`-race`**: concurrent **`NotifyAppend`** + **`Drain`** from N goroutines — **≤1 emitting** (`atomic` counter on `EmitFunc`) |
| **INV-MONOTONIC-CURSOR** | Table: never regress **`cursor`** without `ResetSession` |
| **INV-SUBSET** | Emit mock asserts **`pin < lsn ≤ head`** for every **`Emit`** |
| **INV-NO-GAP-R1** (**§5**) | Scripted: `cursor == head` at **`H0`**, interleave **`head++`**, assert **`Emit(H0+1)`** occurs |
| **INV-NO-DOUBLE-LIVE** (**CHK surrogate**) | **Two entrypoints**: call **`WalShipper`** + legacy **`Ship` mock** — after P1, **`Ship` must not bypass** (**single emit counter**) |
| **§6 `lag`/R2** | Fake clock or small threshold: **`SignalSaturation` called exactly once**, session hook receives reason |

**Integration**: **smoke** only (daemon up, no assertion of **`CHK-*` solely from E2E**).

---

## 5. **PR reviewer checklist**

- [ ] **No duplicate `Emit` routes** — grep **`frameWALEntry`/`MsgShipEntry` write paths** reachable from **`recovery/Sender`** *and* **`Ship`** without **`WalShipper`**.  
- [ ] **`R1`** procedure identifiable in code (**double read `head`** or equivalent proof in PR description).  
- [ ] **`g7-redo/unified-wal-impl`** **NOT** landed as authoritative Wal scheduling.  
- [ ] Consensus **`G1`**: **INV-NO-DOUBLE-LIVE** exercised **deterministically** (unit/table).  

---

## 6. Dependencies & parallelism

| Item | Blocking? |
|------|-----------|
| Architect **SUPERSEDED** on unified kickoff | **No** — optional doc hygiene same or follow PR |
| **`v3-storage-logical-pin-gate.md`** (smartwal **`RecycleFloorGate`**) | **Orthogonal** — WalShipper refactor should not tighten substrate in same mega-PR |

---

## 7. Estimate (**order of magnitude**, revisable)

| Slice | Rough LOC |
|-------|-----------|
| **`wal_shipper.go` (+ tests P0)** | +250 prod + 300 test |
| **`ship_sender.go`/executor refactor** | +150 / −200 |
| **`recovery/sender.go` thin** | −200 / +80 |
| **R1/R2 + race tests** | +200 |

---

## 8. Revision

| Date | Change |
|------|--------|
| 2026-04-29 | v0.1 — Bridges **wal-shipper-spec** ↔ **`seaweed_block`**; supersede unified §3.2 #3; phased PR |
| 2026-04-29 | v0.2 — **§1.1** steady-path **four-hop fan-out** + **`Ship` delegation** (**P1**); **`DrainBacklog`** naming |
| 2026-04-29 | v0.3 — **§10 P2d decision request** appended; P2c split into **slice A / B-1 / B-2** (all merged into `g7-redo/wal-shipper-impl`) |
| 2026-04-30 | v0.4 — **P2d ratified + implemented** (`cb8ff1c`): per-connection format dispatch (steady=`MsgShipEntry`, dual-lane=`frameWALEntry`); resident WalShipper with EmitProfile + EmitKind; RecoverySink wired into `startRebuildDualLane` |
| 2026-04-30 | v0.5 — **§11 C1–C3 hardening sequence** appended after architect review of `cb8ff1c` exposed three correctness gaps vs §6.8 checklist (writer race, missing RecordShipped, no timer drain, sequential BASE/WAL) |
| 2026-04-30 | v0.6 — **Dual-mode **`ModeBacklog`/ModeRealtime`** + consensus **`§13` E‑WALSHIPPER‑DUAL‑MODE** (**T4a carve-out**) — rewrote **§11.2**, **§11.3** `#3/#4/#9`, §11.4 anchors, §11.5 (**StrictRealtimeOrdering**, TOCTOU); **§11.6 migrations** (**replicaID**, rebuild-on-gap); CHK/timer scope = **Backlog** |

---

## 9. Answer card (**for implementer FAQ**)

| Question | Answer |
|----------|--------|
| **(a) mini-plan before code?** | **Yes (`this file`) —** spec lacks file/layout; avoids second wrong placement |
| **(b) wait for superseded banner?** | **No blocker** |
| **(c) skip mini-plan?** | **Not advised** — already slipped once (**recovery.sender==shipper**) |

---

## 10. **P2d decision request** (architect-gated)

**Status**: **P2c is closed** (slice A / B-1 / B-2 merged into `g7-redo/wal-shipper-impl`). Bridging **`senderBacklogSink`** owns the live-write buffer + `flushAndSeal` under `sinkMu`; `Sender.Run` barriers as soon as `sink.DrainBacklog` returns; `Close`/`closeCh`/`liveQueue`/`drainAndSeal` are deleted from `Sender`. Atomic-seal contract preserved (capture-vs-reject migrates from `queueMu` → `sinkMu`).

**P2d cannot start** until the architect picks one canonical dual-lane wire body format and one applier owner. The bridging sink stays in place until then; once a real `transport.WalShipper` adapter replaces it, the buffer + flushAndSeal scaffolding here goes too.

### 10.1 The decision (three-axis, narrowly scoped)

| Axis | Options | Blast radius |
|------|---------|--------------|
| **Body format on the dual-lane port** | (A) **`MsgShipEntry`** payload (`EncodeShipEntry` from transport) — unify on legacy steady-ship encoding | recovery/`frameWALEntry` decode path on receiver must **delegate to** transport apply path *or* re-encode kind tagging into metadata; WAL-replay tooling that assumes backlog payload shape must align |
| | (B) **`frameWALEntry`** payloads (`encodeWALEntry` from recovery) — teach `WalShipper.EmitFunc` to encode this | `BlockExecutor` / `WalShipper` `EmitFunc` must encode `frameWALEntry`, not `MsgShipEntry`; existing `MsgShipEntry`-only parsers need explicit **lane vs message-type dispatch** |
| | (C) **Documented third** (e.g. envelope byte that disambiguates) | Both decoders gain a tag check; cleanest if neither current shape is right; highest design cost |
| **Single applier owner** | `recovery.Receiver` *or* `transport` replica handler | Whichever is canonical owns the `apply(lba, lsn, data)` path; the other becomes a thin demux/forward |
| **Replay source of truth** | Which encoding the on-disk WAL playback decoder reads | Determines whether existing on-disk WAL frames (V2-faithful) need a one-shot rewrite or a dual-decoder migration |

### 10.2 What B-2 already buys us

- Single live path: `Sender.PushLiveWrite` → `s.sink.NotifyAppend` is the only entry point.  
- Sink interface stable: `StartSession / DrainBacklog / EndSession / NotifyAppend` — a real `transport.WalShipper` adapter satisfies this by duck typing without touching `recovery`.  
- Architect P1 review rules 1+2 (emit context **before** `StartSession`; restore steady lineage **after** `EndSession`) are caller obligations that hold whether the sink is `senderBacklogSink` or the real adapter.  
- Atomic-seal contract is owned by the sink, not `Sender` — when the bridging sink is replaced, the contract migrates with it.

### 10.3 Pre-decision deliverables (parallel-safe)

These do **not** require the format choice and can land while waiting on the decision:

- **Adapter scaffolding**: `transport`-side struct that wraps `WalShipperFor(replicaID)` + lineage management; satisfies `recovery.WalShipperSink` by duck typing. Wire format remains TBD; tests use a recording `EmitFunc`.  
- **Integration tests for architect rules 1+2**: assertions that the calling layer sets `(conn, lineage)` before `StartSession` and restores steady lineage after `EndSession`. Test the bridging sink today; carry over to the real adapter unchanged.  
- **V2 port discipline note**: dual-lane v3 convergence first; V2 wire-compat is gated separately per `feedback_porting_discipline.md` (don't silently collapse incidental invariants).

### 10.4 Ask, in one paragraph

> P2c/B sink path is unified; bridging `senderBacklogSink` still duplicates `frameWALEntry` encoding because P2d is open. Going to a real `transport.WalShipper` sink requires **one** of: (A) unify dual-lane on `MsgShipEntry` payload + receiver dispatch, (B) teach `WalShipper.Emit` backlog path to encode `frameWALEntry`, or (C) a documented third (e.g. envelope byte). Plus: which package owns the **single applier** (`recovery.Receiver` vs `transport` replica handler), and which encoding is the **replay source of truth** for on-disk WAL. Pre-decision adapter scaffolding + rules-1+2 integration tests can land in parallel.

**§10.5 P2d ratification + implementation** (architect 2026-04-30, `cb8ff1c`):

Choice — **per-connection format dispatch**, not single-format unification. Dual-lane port → `frameWALEntry` (`recovery.WriteWALEntryFrame`). Legacy port → `MsgShipEntry`. Single resident WalShipper per (volume, replicaID); `walShipperEntry.emitProfile` selects encoder + `EmitKind` (Backlog/Live) tags WAL kind. `RecoverySink` swaps profile via `updateWalShipperEmitContext` at session brackets; nil-conn emit silently drops (matches Idle semantics). Replay source of truth + applier owner unchanged from existing design.

---

## 11. C1–C3 hardening (post-`cb8ff1c`, post-§6.8 architect review)

§6.8 / consensus v3.8 review of `cb8ff1c` exposed three correctness gaps vs the checklist's nine MUSTs. Three commits, in order:

### §11.1 Three identified gaps

| # | §6.8 ref | Gap |
|---|---------|-----|
| **G-WRITE-RACE** | (1) SINGLE-SERIALIZER (mechanical) | `Sender.writeFrame` (`writerMu`) and `EmitFunc.conn.Write` (no mutex) race on same dual-lane conn; can interleave header/payload of two frames |
| **G-RECORDSHIPPED** | accounting | WalShipper-routed emits don't call `coord.RecordShipped`; `shipCursor` stays at `fromLSN` for whole session |
| **G-TIMER** | (4) TIMER + **(9) PRIORITY** (consensus v3.8) | **Originally**: no periodic `emit-from-cursor`; Realtime **`NotifyAppend`** could skip debt. **Resolved in C2 + clarified by dual-mode (**`§13` E‑WALSHIPPER‑DUAL‑MODE**)** — **idle timer MUST** **`ModeBacklog` only**; Realtime ships **without** substrate drain per **T4a** (**§11.2a**). |
| **G-PARALLEL** | (6) BASE ∥ WAL overlap | `Sender.Run` runs `streamBase` fully before `sink.DrainBacklog`. Strictly serial. P6 / `G3` requires wall-clock overlap |

### §11.2 Commit sequence

**C1 — concurrency safety** (`writeMu` shared + post-emit hook for `RecordShipped`):

- `walShipperEntry` adds `writeMu sync.Mutex`. `EmitFunc` acquires it during `conn.Write`.
- `recovery.WalShipperSink` gains optional duck-type sub-interfaces:
  - `WriteMu() *sync.Mutex` — Sender uses if present (for shared serialization)
  - `SetPostEmitHook(func(lsn uint64))` — Sender installs the `coord.RecordShipped(replicaID, lsn)` callback at session start
- `transport.RecoverySink` implements both. Steady (Ship) path keeps own mutex (no contention).
- Tests: `-race` on concurrent Sender.writeFrame + EmitFunc; assert `coord.PinFloor` advances during dual-lane session.

**C2 — `send(∅, debt)` timer + **`ModeBacklog`** priority** (**consensus** **§6.3 / §6.8 #3,#4,#9**, scoped **`v3-recovery-algorithm-consensus.md` §13 — E‑WALSHIPPER‑DUAL‑MODE**):

- **`ModeBacklog`**: `WalShipper` internal `timerLoop` (`IdleSleep`) + **`nudgeCh`**. **`drainOpportunity`**: under **`shipMu`**, **`mode`, `cursor`, `head`** read atomically (**TOCTOU fix** `377bcb0`); if **`cursor < head`**, one **`ScanLBAs(cursor, …)`** cycle (`emit-from-cursor`), then **`cursor++`**; **`assertCaughtUpAndEnableTailShipLocked`** unchanged.
- **`ModeRealtime`** (`NotifyAppend`): **does not** run substrate-scan ship on **`NotifyAppend` hot path** (**T4a / no‑replay`). **`lsn <= cursor`** ⇒ idempotent **`nil`**; else **`emit(EmitKindLive, …, data)`**, **`cursor = lsn`** (optimized steady tail — caller bytes canonical). **`cursor < head`** in Realtime ⇒ **BUG / caller contract violation** ⇒ engine **`MUST`** drive **`ModeBacklog`** **`DrainBacklog`** **or rebuild** (**§11.6**); **never** silently “repair” via Realtime scan.
- **`StrictRealtimeOrdering`**: **`wal_shipper`** config — **default log-warn** on dense **`lsn ≠ cursor + 1`**; **strict / error-return** optional **production** opt‑in (**pre-condition §11.6**).
- Signature **`NotifyAppend(lba, lsn, data)`**: **`data`** used **Realtime** optimized path **only**; **`ModeBacklog`** drain **`MUST`** read substrate (**one tape** authoritative bytes).
- **`DisableTimerDrain`**: production **`MUST false`** (test-only; see §11.5).
- Tests: **`TestC2_TimerDrainsIdle`** (**CHK-WALSHIPPER-TIMER‑DRAIN**, **`ModeBacklog`** only per **§13**); **`TestC2_PriorityOldFirst`** (§6.8 #3 / #9, Backlog); **`TestC2_NoGapDenseLSNEdge`** (**regression**) — dense **single-unsent‑LSN** debt ships only via Backlog/timer (bans naive **`lsn > cursor + 1`** debt predicate).

**§11.2a — Dual-mode normative contract (consensus ⇄ mini-plan)**

| Mode | Scheduling |
|------|------------|
| **`ModeBacklog`** | **`send(incoming, debt)`**, **`send(∅, debt)`**: idle timer **`MUST`** (**§13**). Oldest‑first (**§6.8 #3**) via **`ScanLBAs`**. Scope of **`CHK-WALSHIPPER-TIMER‑DRAIN`**. |
| **`ModeRealtime`** | **Append‑driven only** — **literal §6.3(B)** idle timer **`MUST NOT`** apply. **Hot path `MUST NOT` substrate‑replay** for shipped WAL bytes (**§13**). |

**C3 — BASE ∥ WAL parallel in `Sender.Run`** (§6.8 #6 / P6 / G3):

- `Sender.Run` spawns two goroutines via errgroup:
  - Base goroutine: `streamBase` → `BaseDone` frame
  - WAL goroutine: `sink.StartSession` → `sink.DrainBacklog`
- Both write through shared `writeMu` (C1's mutex). Mutex-bounded interleaving = correct frame integrity; wall-clock overlap from CPU/IO of substrate reads + small base block writes.
- Barrier writes after both goroutines return (errgroup.Wait); on first error, ctx cancel propagates.
- Test: receiver observes interleaved `frameBaseBlock` and `frameWALEntry` frames (not strictly all-base-then-all-WAL).

**Note on physical reality**: shared `writeMu` means base waits on slow WAL writes (and vice versa). Logical overlap, mutex-bounded interleaving — not zero-blocking parallelism. Hard SLO on base under bursty WAL would need separate credit channel (out of scope for §6.8 #6 strict reading).

### §11.3 Compliance receipt — landed commits on `g7-redo/wal-shipper-impl`

| §6.8 # | Requirement | Implementation | Commit |
|---|---|---|---|
| 1 | SINGLE-SERIALIZER | `walShipperEntry.writeMu` + `WalShipper.shipMu`; lock hierarchy `shipMu → writeMu` documented | C1 `294d4bf` |
| 2 | ONE TAPE | substrate canonical, single cursor, single shipMu | (P0/P1, retained) |
| 3 | PRIORITY | **`ModeBacklog`** only (**§13**): drain emits LSN-ascending from substrate scan | C2 `53c292f` |
| 4 | TIMER | **`ModeBacklog`**: `timerLoop` + **`drainOpportunity`** — idle drain **`MUST NOT`** be suppressed while **`cursor<head`** (**§13**) | C2 `53c292f` |
| 5 | FRAMING ≠ SECOND TAPE | `EmitProfile` (Steady / DualLane) selects encoder per conn | P2d `cb8ff1c` |
| 6 | BASE ∥ WAL | `Sender.Run` two-goroutine errgroup + shared `writeMu` | C3 `40f2935` |
| 7 | R1 | `assertCaughtUpAndEnableTailShipLocked` under shipMu + double-check | (P0, retained) |
| 8 | R2 | `OnSaturation` single-shot per session | (P0, retained) |
| 9 | `send(·,·)` | **`ModeBacklog`**: **`cursor<head`** + priority + **`send(∅, debt)`** (no banned **`lsn > cursor+1`** predicate). **`ModeRealtime`**: **`NotifyAppend`** tail (**T4a**); **`StrictRealtimeOrdering`** + TOCTOU (`377bcb0`) | C2 `53c292f` |

### §11.4 Test anchors (CHK + regression)

| Test | §6.8 # | Repo path |
|---|---|---|
| `TestC1_WriteMu_SharedAcrossSenderAndWalShipper` | #1 wiring | `core/transport/c1_writemu_recordshipped_test.go` |
| `TestC1_WriteMu_NoInterleave` | #1 race | (same file) |
| `TestC1_PostEmitHook_AdvancesShipCursor` | accounting (RecordShipped) | (same file) |
| `TestC2_TimerDrainsIdle` | #4 (CHK-WALSHIPPER-TIMER-DRAIN) | `core/transport/c2_timer_drain_test.go` |
| `TestC2_PriorityOldFirst` | #3 / #9 | (same file) |
| `TestC2_NoGapDenseLSNEdge` | #9 / **Backlog** — **regression anchor**: bans **`lsn > cursor + 1`** as sole debt detector; asserts single‑LSN debt via Backlog/timer | (same file) |
| **`StrictRealtimeOrdering`** (ordering guard) | **`§13`** safety switch (`wal_shipper` — default warn / strict opt‑in; see tests in `wal_shipper`/`NotifyAppend`) | **`377bcb0`** |
| `TestC3_BaseWalParallel_FramesInterleave` | #6 (CHK-BASE-WAL-OVERLAP via interleaved frame indices) | `core/recovery/c3_base_wal_parallel_test.go` |

### §11.5 Invariants documented in code (architect-review-derived)

1. **`DisableTimerDrain` test-only invariant** (`wal_shipper.go` config doc): production MUST leave false; tests using true MUST NOT linger in `cursor < head` without manual drain source. Today's only users: R2 saturation tests, scope-bounded.
2. **Lock hierarchy** (`walShipperEntry.writeMu` doc): `shipMu` always outermost; `writeMu` acquired under it. Reversal risks deadlock.
3. **`RecordShipped` coverage audit** (PR description): four production emit paths verified — bridging streamBacklog/flushAndSeal call inline; WalShipper-routed Backlog drain + Realtime post-R1 fire via C1 postEmit hook. Steady-state `Ship()` without session correctly skips (no session = no shipCursor to advance).
4. **`StrictRealtimeOrdering`** (**`wal_shipper`**) — default **warn** on **`lsn ≠ cursor + 1`** (dense WAL); strict opt‑in (**§13**). **Production `strict=true`**: **pre‑condition §11.6** — engine **must** rebuild / re-anchor on gap first.
5. **`drainOpportunity` atomic read** (**`377bcb0`)**: **`mode` + `cursor` + `head`** under **`shipMu`** **before** cross-checking backlog work — **fixes TOCTOU** vs **`head`/cursor** snapshot.

### §11.6 Open items · production migrations (hardware + ops)

| Item | Action |
|------|--------|
| **§IV T2 (`targetLSN` vs moving `head` / barrier)** | Architect-open at consensus **§IV**; hardware run validates **receiver `achievedLSN` ⇄ coordinator `targetLSN`** at C3 barrier. |
| **§IV T7 (R2 sustained pressure)** | Single-shot **`OnSaturation`** is structural only; throttle / escalation remains **engine** work. |
| **Engine rebuild-on-gap** (before `StrictRealtimeOrdering=true`) | If **Ship→WalShipper** sees Realtime **`NotifyAppend`** violations (out‑of‑order / **`lsn≠cursor+1`** under strict policy, or **`cursor<head`** stuck in wrong mode): engine **MUST** drive **rebuild** or session reopen to **re‑anchor **`cursor`** (§13)** before tightening production ordering. |
| **startRebuildDualLane `replicaID` consistency** | **Caller obligation**: **RecoverySink** / WalShipper key (**engine **`replicaID`**)** **MUST** match **`bridge.coord`** / dual‑lane **`dl.ReplicaID`** used for **`RecordShipped`** and cursor polls. Drift surfaced during C2 debugging — **bridge‑side cleanup** (same ID end‑to‑end). |
| **PR template guard** | For PRs touching **`WalShipper.NotifyAppend`**: (**a**) **Ban `lsn > cursor + 1` as sole debt test** — use **`cursor < head`** (**Backlog**); (**b**) **Realtime never substrate-scan for ship** (**T4a**, **§13**). |

