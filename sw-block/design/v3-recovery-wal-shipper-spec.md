# WalShipper — algorithm spec (**implementable**)

**Status**: Normative leaf spec (**Primary-side**, per-peer Wal replication scheduling). Extends **`v3-recovery-algorithm-consensus.md` §6** without contradicting **§I P1–P7**.

**Audience**: Engineers implementing or reviewing `WalShipper` / catch-up sender / dual-lane Wal path.

**Goal**: Write **this** algorithm once — reviewers match code to headings **§2–§7**; no parallel “mystery shipper” in `core/recovery/sender` unless architect supersedes this doc.

**Consensus checklist (**`seaweedfs`** / **`seaweed_block`** handoff**)**: **`v3-recovery-algorithm-consensus.md` §6.8** — **nine MUST** bullets (**item 9** = **`send` strategy**); **implementations SHOULD map each item to tests** (below §9). **Scope**: §6.8 **(3)(4)(9)** **as literal idle / debt-priority** apply **`ModeBacklog`**; **`ModeRealtime`** obey **`§13` — E‑WALSHIPPER‑DUAL‑MODE** (`**v3-recovery-wal-shipper-mini-plan.md` §11.2a**).

---

## 1. Scope

| In scope | Out of scope (link elsewhere) |
|----------|--------------------------------|
| **One ordered Wal tape** on Primary: `head`, per-peer **`cursor[P]`**, **emit** order to **P** | **Extent/base bulk** — `v3-recovery-algorithm-consensus.md` §5; bytes `v3-recovery-pin-floor-wire.md` |
| **§7.3 — sender recover emit** (**normative = consensus **`Drive`**) · **§7.4** — illustrative **recover receive** rewind + monotone + **`bitmap`** pseudocode (**consensus §II §7 routing**) | **§7.4** **`ApplyWalWithWalClaimBitmap` / **`ApplyBaseUnderBitmapGate` **→** **`v3-recovery-algorithm-consensus.md` §6.10** pseudocode (**`INV‑RECV‑*`**). Receiver detail — **`seaweed_block`** `core/recovery` + tests; **`checkMonotonic`** |
| **Single serializer** for “next LSN sent to P” (**P2**) | Receiver **`checkMonotonic`**, **`bitmap`** — consensus §7; replica apply |
| **R1** transition (caught-up vs append race) | **Engine** retry / escalate after fail-close — executor / engine |
| **R2** saturation observability hooks | **`MinPin`/recycle** full story — coordinator + substrate |

---

## 2. State (**per replica `P`**)

Stored in **one owning struct** (“`WalShipper`” here); names are logical.

| Symbol | Meaning |
|--------|---------|
| **`head`** | Next LSN the Primary Wal will assign on append (**exclusive upper bound** of assigned LSNs: valid entries satisfy `pin < lsn ≤ head` per product pinning, see **§3**). **Single source** — bumps only inside **Wal append** path owned by same package or **serialized** through **§6.1**. |
| **`cursor`** | Next LSN **to send** to peer **P** (inclusive lower bound for unsent prefix). **`cursor ∈ [fromLSN, head]`** once session valid. (**Shorthand**: `cursor` means `cursor[P]`.) |
| **`fromLSN` / `pin`** | Session lower bound from engine/coordinator (**P7**). **`cursor` initialized ≤ `fromLSN` semantics** as product defines (usually `cursor := fromLSN` before first emit). |
| **`shipMu`** | **One mutex** (or FIFO channel) guarding **§4–§6** — **`head`/`cursor` decision + emit claim** (**R1**, **§6.1 consensus**). |
| **`lastEmit`** | Optional diagnostic: last LSN **handed to transport** for P (for metrics). |

**Vocabulary (consensus **§6.2–6.3**)** — **normative mapping**:

| Term | Meaning |
|------|---------|
| **`debt` non-empty** | **`cursor < head`** — **oldest unsent** is always **`cursor`** on the **single** Primary Wal order. |
| **`debt` empty** | **`cursor == head`** — **no unsent prefix**; next emit is **tail** / **R1**-safe new assign. |
| **`incoming`** | **New tail** Wal LSN made visible to the serializer **this `ShipOpportunity`** (typically from **append**), **not** allowed to **skip ahead** of **`cursor..head−1`**. |
| **`send(incoming, debt)`** | **Strategy choice**: if **`debt` empty** → **emit `incoming`** (steady **debt-free** path); if **`debt` non-empty** → **emit `cursor` first**, **`incoming` waits** on the same tape. |
| **`send(∅, debt)`** | **Timer / auxiliary loop** opportunity: **no append paired to this tick** (**`∅`**) but **still drain** **`cursor < head`** — consensus **§6.3(B)** / checklist **§6.8 item 4**. |

**Policy note (informative)**: **Upper layers** open recover **policy debt** via **`fromLSN`** / extent-anchored admission (**P7**). **Closing** that hole is **gap → 0** on **`cursor→head`**, not “freeze **`head`’s coordinate**” — **consensus §6.2**, **`T2`**.

**Forbidden**: second concurrent “recover pump” and “steady pump” **both** calling `Emit` for the same **`P`** without **`shipMu`** — **violates INV-SINGLE**.

---

## 3. Invariants (**must hold between observable steps**)

**INV-SINGLE** — At most **one** execution context at a time runs **`PlanNextEmit` → `EmitToTransport(P, lsn)`** for a given **`P`**.

**INV-MONOTONIC-CURSOR** — **`cursor` never decreases** unless session is torn down / re-seeded (**P7** engine reset).

**INV-SUBSET** — **Every emitted LSN** satisfies **`pin < lsn ≤ head`** (or product’s closed/open interval choice, but **consistent** with receiver `expected`).

**INV-NO-GAP-R1** — No LSN **`k`** with **`cursor ≤ k ≤ head`** is **skipped** across the **catch-up → tail** seam: **§5**.

**INV-NO-DOUBLE-LIVE** — No duplicate emit of same logical **`lsn`** on two live outbound paths for **P** — matches **CHK-WALSHIPPER-SINGLE-CURSOR**.

---

## 4. Core priority rule (**default scheduling**)

On each **`ShipOpportunity`** (defined as: **after Wal append**, or **timer tick**, or **transport ready** callback — all must **serialize** through **`shipMu`**):

### 4.1 Strategy summary (**must match consensus §6.3**)

- **`send(incoming, debt)`**:
  - **`debt` empty** (`cursor == head`): **emit `incoming`** (new tail) when this opportunity includes one — **debt-free steady send**.
  - **`debt` non-empty** (`cursor < head`): **emit `cursor`** (oldest unsent). **`incoming` does not overtake** — it **extends** the tape behind the unsent prefix.
- **`send(∅, debt)`**: **Timer-only** (or loop) opportunity — even if **no `incoming`** this tick, if **`cursor < head`**, **PlanNextEmit** follows **(1)** below until budget / blocked.

### 4.2 PlanNextEmit steps

1. If **`cursor < head`**: **PlanNextEmit** returns **`cursor`** (the **oldest unsent** LSN). After successful handoff to transport (or committed send slot), **`cursor ← cursor + 1`** (or next present LSN if sparse — product must define; **binary Wal is dense LSN**).
2. Else **`cursor == head`**: **no debt**. If this opportunity was triggered by **a new append** that just set **`head = h`**: **PlanNextEmit** returns **`h`** (the new tail) **once** (steady **append → ship one**).
3. **Timer-only** opportunity with **`cursor < head`**: same as (1) — **`send(∅, debt)`**; **drain without requiring new appends** (consensus §6.3(B)).

**Wrong model (non‑compliant)**: alternating “K live + M backlog” as **two mailboxes**. **Right model**: single tape, **`cursor` priority** before tail within one serializer.

---

## 5. **R1** — Caught-up / transition (**mutex + double-check**)

**Problem**: Between “observed **`cursor == head`**” and “declare tail-push only”, **`head` grows** (`head = h+1`); tail-push path never sees **`h+1`** → **gap**.

**Procedure `AssertCaughtUpAndEnableTailShip`** (runs **inside `shipMu`**):

1. Read **`h ← head`**.
2. If **`cursor < h`**, return **`Backlogged`** (caller runs §4 loop).
3. **Double-check**: read **`h2 ← head`** again under same lock. If **`h2 ≠ h`** or **`cursor < h2`**, go to step 1 or §4 as appropriate.
4. Only now may implementation set internal flag **`tailPushEligible := true`** (if it uses one) — **optional**; many implementations **need no flag** if **§4** always applies: when **`cursor == head`** and append occurs, next **`ShipOpportunity`** ships **exactly** the new tail.

**Append path** (**Wal append**) **MUST**:

- Bump **`head`** **only** while holding **`shipMu`**, **or**
- Bump **`head`** then **immediately** call **`ShipOpportunity`** under **`shipMu`**, **or**
- Use a **single threaded** Wal append + shipper goroutine communicating by ordered messages.

Any **ordering** works iff **INV-NO-GAP-R1** is provable.

---

## 6. Saturation (**R2**) — bounded progress (**not algorithm beauty**)

**Observation** (**outside hot path mutex if needed**):

- **`lag := head − cursor`** (dense LSN **or product-defined synthetic distance**).

**Threshold policy** (**product**, consensus §6.6): if **`lag`**, **time‑above‑T**, or **pin pressure** satisfies fail condition:

- **`SignalSaturation`** → executor ends session **`FailureTimeout`/product kind** **and/or** triggers **Wal admission throttle** on Primary — **never** silently infinite spin.

Timer **priority drain** (**§4**) remains required for **liveness** but **does not** disprove **R2** math.

---

## 7. Pseudocode (**reference shapes**)

### 7.1 Primary **`WalShipper`** core loop (**steady + recover**)

```text
// One goroutine OR all entrypoints take shipMu.

function OnWalAppend(assigns_new_lsn):
    lock(shipMu)
    head++

    EmitAccordingTo§4UntilBudgetOrBlocked()  // drains cursor..head-1 before new tail rules

    // Typically: while cursor < head: emit(cursor); cursor++
    // then if new tail == head prev+1 logic: emit that tail once

    unlock(shipMu)

function OnShipTimer():
    lock(shipMu)
    EmitAccordingTo§4UntilBudgetOrBlocked()
    unlock(shipMu)

function EmitAccordingTo§4UntilBudgetOrBlocked():
    while transport_has_credit() and cursor < head:
        emit(cursor)
        cursor++
    while transport_has_credit() and cursor == head and pending_tail_ship_from_append:
        emit(head); clear pending_tail_flag  // depends on refactor; simplest: append path calls Emit once after advancing head while cursor tracked
```

Implementations **flatten** this sketch; **§3–§5** remain the semantic contract.

### 7.2 **Recover-visible high layer** (**session** + **`backlog` pointer**)

Pairs with **`v3-recovery-algorithm-consensus.md` §6.9** (**`NEGATIVE‑EQUITY`**, **`HOPE‑SHIPPER‑MONOTONIC`**). Recover adds **no** parallel “Wal ownership” beside:

| Artifact | Meaning |
|----------|---------|
| **Recover session envelope** | **`sessionID`, epoch/ev, lineage`** (may carry optional **`targetLSN=Y`** as **frozen lineage text** — **not** a backlog pointer — **never** WAL segmentation rhetoric). |
| **Backlog pointer** | **`cursor`** on the **same** Primary tape — **`cursor < head` ⇔ debt** (**§6.3** **`send(incoming, debt)`** / **`send(∅, debt)`**). |

```text
type RecoverSessionView = record
    session_id : uint64
    lineage    : opaque   // echoed on bearer + barrier; includes engine-authored fromLSN/target text
end

// cursor + head owned ONLY by WalShipper (§2). Executor/coordinator publishes RecoverSessionView + brackets.
WalShipper.OnRecoverSessionBracket(StartSession(fromLSN), …, EndSession)
```

### 7.3 **Primary recover — monotone emit**

**Normative**: **`v3-recovery-algorithm-consensus.md` §6.3 — reference pseudocode `Drive(input)`** — single **`fifo`**, **`cursor < head ⇒ ReadAtLSN(cursor)` before tail**, **`lsn > cursor` ⇒ `CursorGap`** (**no realtime scan‑fill**). **`(a)`** state, **`(b)`** **`shipMu`** atomic boundary, **`(c)`** **`INV‑WIRE‑WAL‑LSN‑MONOTONIC`**.

Optional **`ScanLBAs(...,≤Y]`** emits **Backlog‑class** frames yet **still** advances **`cursor`** only **under **`shipMu`** — **INV-SINGLE** / **INV-MONOTONIC-CURSOR** unchanged.

**Informative**: one lock hold often **loops** **`Drive(∅ | fifo.peek())`** until transport budget clears or **blocked** — **observable semantics remain** **`Drive`** **per step** per **§6.3**.

### 7.4 Replica recover session (**monotone receive**, **one rewind**, **`bitmap` overlap**)

**“One rewind”**: not an arbitrary backward LSN cascade — the session **resets** ingest expectation to **`fromLSN + 1`** (dense default; **`CHK-RECOVER-REWIND-ONCE`** in **consensus §V §12**), validates the **first lawful** recover-WAL frame, then **`monotonic_armed`** — only **strict monotone** (`checkMonotonic`) thereafter.

**Normative ingest**: exactly **one** recover-WAL monotonic frontier after contractual **rewind**; **Wal-vs-base overlap** gated by **`bitmap`** (**§I P5** — operationalized **`v3-recovery-algorithm-consensus.md` §7 routing table**, **`checkMonotonic`**); substrate **Wal-vs-Wal LWW** after ingest order holds.

```text
type RecoverRcvrSession = record
    open                : bool
    fromLSN_exclusive   : uint64
    expect_next_LSN     : uint64   // rewound expectation baseline
    monotonic_armed     : bool     // after first lawful recover-WAL ingest
end

procedure OpenRecoverRcvr(s: RecoverRcvrSession*, fromLSN, lineage_blob):
    require not s.open
    s.open := true
    s.fromLSN_exclusive := fromLSN

    // INFORMATIVE (CHK-RECOVER-REWIND-ONCE): first post-open WAL expects fromLSN+1 on dense products
    s.expect_next_LSN := fromLSN + 1
    s.monotonic_armed := false
    BITMAP_scope_for_session(lineage_blob)

procedure OnRecoverWalTuple(s: RecoverRcvrSession*, frame_kind /* Backlog | SessionLive | … */, lsn, lba, data):
    require s.open

    if not s.monotonic_armed:
        REWIND_VALIDATE(lsn, s.expect_next_LSN, s.fromLSN_exclusive)
        // after success, ingest at lsn establishes monotonic stream
        s.monotonic_armed := true
    else:
        FAIL_IF_NOT checkMonotonic(lsn, s.expect_next_LSN)   // gaps / backwards / unlawful dup vs policy

    ApplyWalWithWalClaimBitmap(s, lsn, lba, data)

    s.expect_next_LSN := AdvanceDenseOrProductRule(lsn)


procedure OnRecoverBaseBlock(s: RecoverRcvrSession*, lba, bytes):
    require s.open
    ApplyBaseUnderBitmapGate(s, lba, bytes)                  // overlaps with Wal claims (**P5**); does NOT bypass Wal LSN frontier


procedure CloseRecoverRcvr(s: RecoverRcvrSession*):
    s.open := false
```

```text
// Receiver-hard invariant (consensus §6.9): all recover WAL frames obey ONE global LSN order to this session —
// bearer “kind” reshapes encoding only — never splits into independent tapes.
```

**`bitmap` arbitration + races** — **`INV‑RECV‑BITMAP‑CORE`** / **`INV‑RECV‑WAL‑NAIVE`**: normative **`ApplyWAL` / `ApplyBASE`** (**`appendWAL` vs `writeExtentDirect`**) — **`v3-recovery-algorithm-consensus.md` §6.10**. This §7.4 sketch **routes** only; **`ApplyWalWithWalClaimBitmap`** **`MUST`** use **Wal path**; **`ApplyBaseUnderBitmapGate`** **`MUST`** **not** smuggle base through **Wal** without **§I** sign-off.

---

## 8. Interface boundaries (**who owns what**)

| Concern | Owner |
|---------|-------|
| **`fromLSN` / pin / SessionID** | Engine + **`PeerShipCoordinator`** |
| **`head` mutation** | Primary Wal append path (**substrate**/BlockVol — **serialized** vs shipper §5) |
| **`cursor` mutation** | **WalShipper** only (per **P**) |
| **`EmitToTransport`** | WalShipper calls into transport (SWRP / `frameWALEntry` / dual-lane) — **one caller** per **P** |

---

## 9. Tests (**minimum**)

| ID | Intent |
|----|--------|
| **CHK-WALSHIPPER-SINGLE-CURSOR** | consensus **§V** / **§6.8 item 1** — no duplicate live paths |
| **CHK-WALSHIPPER-TIMER-DRAIN** | consensus **§V / §6.8 item 4** |
| **Test-R1-Flip-NoGap** | After synthetic `Scan`/drain at `H0`, concurrent append `H0+1` before unlock — **receiver** must see **`H0+1`** (integration) |
| **Test-Priority-OldFirst** | With `cursor < head` and new append, **first emit** is **`cursor`**, not new tail — **`send(incoming, debt)`** with **`debt` non-empty** |
| **Test-Timer-Drains-Idle** | No append; timer moves **`cursor`** toward **`head`** — **`send(∅, debt)`** |

---

## 10. Revision

| Date | Change |
|------|--------|
| 2026-04-29 | Initial **WalShipper** implementable spec (priority queue on one tape, **R1** double-check, **R2** hook, boundaries). |
| **2026-04-30** | Pointer to consensus **§6.8** implementer checklist; **§V** **`CHK-WALSHIPPER-TIMER-DRAIN`** cross-ref in **§9**. |
| **2026-04-30** | **§2** vocabulary **`incoming` / `debt` / `send(·,·)`**; **§4.1–4.2** strategy split; tests cross-ref **§6.3**; checklist **nine** items (**§6.8(9)**). |
| **2026-04-30** | **§1** intro + **consensus §6.3(B)** cross-ref — **`ModeBacklog` vs `ModeRealtime`** (**§13** E‑WALSHIPPER‑DUAL‑MODE); mini-plan **§11.2a** bridge |
| **2026-04-30** | **§7** split: **7.1–7.4** — recover **session/backlog pointer**, **monotone send (`send(∅,·)` / `send(incoming,·)`)**, receiver **rewind + monotone + `bitmap`** pseudocode |
| **2026-04-30** | **§7.4** foot — **`bitmap`/`CAS`** cross-ref **consensus §6.10** (`INV‑RECV-*`) |
| **2026-04-27** | **§7.3** — primary sender recover emit: normative **`Drive(input)` → `v3-recovery-algorithm-consensus.md` §6.3** (removed duplicate **`ShipOpportunity_RecoverMerged`** skeleton); **§7.4** note → consensus **§6.10** **`ApplyWAL`/`ApplyBASE`** pseudocode |
| **2026-04-27** | **§7.4** note — **`appendWAL` vs `writeExtentDirect`** (**consensus §6.10** substrate split) |

---

## 11. Document map

| Doc | Role |
|-----|------|
| **`v3-recovery-algorithm-consensus.md` §6, §6.3 **`Drive`**, §6.8–§6.9, §II §7** | **Axioms** + **`Drive`/`Apply*`** pseudocode (**`INV‑WIRE‑*`**, **`INV‑RECV‑*`**) + **implementer checklist** + **`targetLSN`** process + **replica routing / `checkMonotonic`** |
| **This** | **Algorithm + §7 pseudocode** (**Primary shipper** + **recover receive sketch**) |
| **`v3-recovery-wal-shipper-mini-plan.md`** | **Phased PR / file anchoring (**`seaweed_block`**) **↔** §INV tests** |
| `v3-recovery-wiring-plan.md` | Bearer / port |
| `v3-recovery-unified-wal-stream-*.md` | Historical kickoff — **align or supersede** with this spec |
