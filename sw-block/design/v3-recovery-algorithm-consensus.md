# V3 / G7-recovery — algorithm consensus (normative)

**Status**: Normative — **single source of truth** for recover *algorithm* (**第一性原理** + **可追溯推论**).

**Audience**: Architects, protocol authors, implementers, QA.

**How to read this document**

| Layer | Sections | Stability |
|-------|----------|-----------|
| **I — Recover 算法基础 / 第一性原理** | **§I — P1–P7** | **Maximal stability.** Change requires architect + explicit revision log entry. Implementations **must** preserve every principle or document a **controlled exception**. |
| **II — Architecture derived from §I** | **§II — §4–§6** | **Stable**, but refactorable if wording improves without violating **§I**. |
| **III — Coordination / wire / taxonomy** | **§III — §8–§10** | **Operational detail.** Child docs (**§V §15**) deepen bytes and file paths — **cannot contradict §I–§II**. |
| **IV — 未定细节 · 已知风险登记** | **§IV** | **Explicitly unfinished.** Skipping **§IV** risks **logical bugs at protocol→code handoff**. Each row should close into **§III** or a leaf spec + test. |

**Rule**: If a **detail is not pinned** anywhere in this doc suite, reviewers **still** MUST check it against **§I (P1–P7)** before merge.

---

## I. Recover algorithm foundation & first principles

These are **not** preferences — they are the **minimal joint axioms**. Everything in **§II+** serves them.

### 1. Boundary & truth (**P1**)

- **Recover is a bounded session**, not ambient state (`StartSession … EndSession` / fail).
- **Authoritative convergence** toward **healthy replicated state** is proven only via **explicit barrier handshake** whose **`AchievedLSN`** and receiver conjuncts match the **engine-authored cut semantics** for **that session** (**§6.9**, **§IV `T2`**) together with **`baseDone` / layer-1 conjunct as defined by product**. **Inferring “done” from traffic silence, Kind ratios, or sender idle windows is forbidden.**

### 2. Exactly one causal WAL frontier per peer-session (**P2**)

- For a given replica during a recover session there is **exactly one** logical sequence of WAL application that must not be forked (`LSN`-monotonic ingest where required).
- **Double-ship**: the Primary **must not** deliver the **same logical WAL mutation twice** down **two uncoordinated live paths**. Resolving ambiguity by “substrate might dedupe” is **non-compliant** (#2 is preventive, not rehabilitative).

### 3. Homogeneous WAL (**P3**)

- Any path that carries **(LSN, LBA, data)** to the Replica is **semantically WAL replication** regardless of framing (`MsgShipEntry` vs `frameWALEntry`). **Rebuild does not invent a second Wal tape** — only **scheduling** (**which LSN ships next** off the **same ordered log**) and **framing** differ.

### 4. Separation of transports vs separation of truths (**P4**)

- **Multiple sockets / envelopes** ARE allowed (**legacy vs dual-lane** today); **forked causal ownership of WAL shipping** is NOT.
- The **minimal strict “dual line”** is the **extent/base bulk channel** (**no authoring LSN** on wire). WAL toward a replica is **one shipper decision stream** (**cursor + pin**, **§6**) — **not** a second phantom **`core/recovery`** WAL pump.

### 5. Replica is the semantic hinge (**P5**)

- **Steady WAL apply** → substrate path (LSN contiguous where enforced).
- **Recover WAL apply** → same substrate **plus** WAL-claims visible to base (`bitmap` / `MarkApplied`).
- **Recover base bytes** → `bitmap` arbitration **against** WAL-claims (**base cannot argue with LWW across LSN** alone).
- **Substrate cross-LSN LWW resolves WAL-vs-WAL** (including hypothetical duplicate ingress); **bitmap resolves WAL-vs-base**.

### 6. Pin costs the Primary (**P6**)

- **Recycle / retention MUST respect** **`MinPinAcrossActiveSessions`** and **honest `BaseBatchAck → SetPinFloor`**. Designing for **parallel base ∥ WAL drain** reduces wall-clock pin exposure (**performance / ops invariant**, correctness rests on **§I P1–P5**).
- **Substrate recycle** (`S` advancement, ring reclaim) **consult** **`RecycleFloorSource`** — see **`v3-storage-logical-pin-gate.md`** (**WALStore** / **memorywal** gated; **smartwal** MUST close before dual-lane is default-eligible).

### 7. Engine owns trigger numbers (**P7**)

- **`fromLSN` / pin lower bound** (**`pinLSN` when product distinguishes them**): **frozen or engine-authored** at session admission; transport **silently overwriting** (`fromLSN := 0` when engine chose `R+1`) violates **parity** with decision logic.
- **`targetLSN` (when carried on wire / coordinator)**: names **only** the **historical WAL job upper bound `Y`** per **§6.9** — **not** the Primary tape’s physical end (**`head` keeps moving**). Implementations MUST NOT treat “`shipCursor` reached `targetLSN`” as equivalent to **`cursor == head`** or **steady healthy** unless **§6.9 Phase C** is already satisfied under the closed **§IV `T2`** semantics.

---

## II. Derived architecture (**from §I**)

### 4. Architectural picture (**two lines, homogeneous WAL**)

#### 4.1 Steady state

```
Primary ──[ WAL exchange · steady path ]──► Replica ──► substrate ApplyEntry(...)
```

Conceptually **steady path** — **append → ship** on **legacy bearer** (**today**: SWRP / `MsgShipEntry`).

#### 4.2 Recover session (**superposition**)

| Line | Payload | Consensus |
|------|---------|-----------|
| **WAL** | **(LSN, LBA, data)** tuples | **One `WalShipper` / peer: **pin + `cursor` + `send(incoming, debt)` / `send(∅, debt)` + timer** (**§6**). **Dual-lane `frameWALEntry` is still P3 homogeneous WAL.** |
| **Recover / extent** | Snapshot blocks | **Dedicated bulk channel** (**P4** strict second line). |

**Anti-pattern (**P2/P4**)**: duplicated **recovery-only WAL pumps** hiding in **`core/recovery/sender`** without **unified shipper cursor** — forks ownership and restores **double-ship** risk. Archive such branches unless architect re-opens.

### 5. Recover / extent lane (**minimal**)

Purpose: converge faster than pure WAL for cold images.  
Uses **`frameBaseBlock`**, **`BaseDone`**, **`BaseBatchAck` → `pin_floor`** (bytes: **`v3-recovery-pin-floor-wire.md`**).  
**Bitmap + `ApplyBaseBlock`** encodes **P5** for base.

### 6. WalShipper (**single owning entity**) — cursor + pin, not a heavy “mode taxonomy” (**primary implementation locus**)

**Implementable procedure** (pseudo-code, INV names, **`shipMu`/R1 double-check`): **`v3-recovery-wal-shipper-spec.md`**. Implementations **must** match that doc or obtain **architect exception** logged in consensus **§13**.

Implementations **MAY** avoid a visible **`Realtime | Backlog` enum** provided the **behavior below** holds. Naming **“modes”** in prose is optional; **state** is **`cursor[replica]`**, **`pin` / `fromLSN`**, and **`head`** on the **one Primary Wal order**.

#### 6.1 Session injection (**recover manager**)

On recover **Start** for peer **P**, the **manager/coordinator** sets **`pin`** / **`fromLSN`** (lower replay bound) and initializes **`cursor[P] := fromLSN`** (or product-defined watermark). **Engine owns numbers** (**P7**).

#### 6.2 Debt / backlog (**same tape**)

**Operational debt** for peer **P** (what the serializer **must** drain) means **`cursor[P] < head`** — **unsent Wal prefix** on the **single Primary log**. This is **not** a second queue of “different Wal”; it is **`head − cursor`** on **one LSN order**.

**Policy debt (recovery)**: Upper layers **open** operational debt by admitting a recover session with **`fromLSN` / conservative start** anchored to an **extent-persisted** (or otherwise **truthful**) lower bound (**P7**). **Closing recovery** drives this **gap → 0** — i.e. the algorithm **closes the prefix gap**, not “catch up to **`head`’s instantaneous coordinate**”, which moves every append (**moving target**; **§IV `T2`** refines barrier cuts).

**Steady (no reopened policy hole)**: After **`cursor == head`** for the session’s purpose, **each new Wal append** triggers **normal ship** but **does not open a new conservative debt class** — only **bounded implementation lag** (buffers, batches) remains. **Semantic debt reopening** requires a **new** session / coordinator decision (extent re-anchor, re-seed, failure path), not “every append adds debt.”

#### 6.3 Emit priority & **strategy abstraction** (**per append + timer**)

Implementations **must** obey the **scheduler choices** below. Prose **`send(...)`** names **WalShipper’s decision** (“what goes out next **on the Wal tape for P**”), **not** a second protocol frame type unless product maps it to bytes.

**(A) `send(incoming, debt)` — tail visible this opportunity**

- **`debt` empty (`cursor == head`)**: **emit `incoming`** (the **new tail** Wal assigned on append, or the next unsent tail under **R1**). **Steady**: **one append → one forward emit** when transport allows — **debt-free send**.
- **`debt` non-empty (`cursor < head`)**: **emit the oldest unsent** (`cursor`) first; the **new tail `incoming` does not jump ahead** — it **extends** the same tape and **waits** behind **`cursor..head−1`**. **Right model**: **priority dequeue on one LSN order**; **wrong model**: “K live + M backlog” as **two mailboxes**. **`ModeBacklog`**: **`MUST`** obey this literally. **`ModeRealtime`**: gap repair via substrate scan is **forbidden** on the **`NotifyAppend` hot path** — see **§13**.

**(B) `send(∅, debt)` — timer / auxiliary loop (**no newly paired append**)**

**MUST** exist: **periodic `ShipOpportunity`** (timer or shipper-internal loop **equivalent**) that runs **under the same serializer** and attempts **`emit-from-cursor`** when **`cursor < head`**, so debt **cannot** depend solely on new appends (**Primary idle starvation forbidden**). **Interpretation**: **Wal-side payload paired to *this tick’s append* may be absent** (**`∅`**) yet **debt drain still runs**. **Literal requirement applies in `ModeBacklog`**; **`ModeRealtime`** **carve-out** — **consensus §13 E‑WALSHIPPER‑DUAL‑MODE**.

**P2**: **Exactly one serializer** chooses **the next LSN emitted to P** — **no parallel** “steady ship” and “recover pump” **both** advancing unsynchronized cursors for the same peer.

**Formal recovery arc (informative)**: Session proceeds **with-debt sends** (`send(incoming, debt)` while **`cursor < head`**, plus **`send(∅, debt)`**) **until operational debt clears** (**`cursor == head`** under **§6.4** consistency), then **without-debt sends** for steady tail (**`send(incoming, ∅)`**).

**Dual-mode carve-out (**`WalShipper`**)**: **`ModeBacklog`** vs **`ModeRealtime`** (**steady / no substrate replay**) is **architect‑approved** under **`§13 — E‑WALSHIPPER‑DUAL‑MODE`**. **`§6.3**(A)(B)** and **§6.8**(3)(4)(9) remain **literally normative in Backlog**; Realtime semantics **do not** replace **P2 / §6.8(1)/(5) / §6.4**.

##### Reference pseudocode — **`Drive(input)`** (**single serializer**, **`HOPE‑SHIPPER‑MONOTONIC`**)

**Purpose**: every reviewer instantiates the **same** (**a**) state, (**b**) atomic boundaries, (**c**) **`INV`** links — not a second informal compilation.

**Dense‑LSN product**: sparse / non‑contiguous LSNs **`MUST`** replace literal **`+1`** step and dense **`ReadAtLSN`** with **`NextPresentLSN` / product scan — **architect leaf**. **Substrate** **`ReadAtLSN(k)`** is **semantic** only — **batch prefetch** **MAY** hide behind it (**mini-plan §12.4 #1**).

**§6.1 / §V (`CHK‑RECOVER‑REWIND‑ONCE`) alignment**: **`cursor`** here is **`next LSN to emit`** (**same convention as **`v3-recovery-wal-shipper-spec.md` §7.1** `while cursor < head`**). **`§6.1 **`cursor[P] := fromLSN`** **and** engine **`fromLSN`** **MUST** be paired so **first lawful ship `lsn`**, **`ReadAtLSN(cursor)`** (debt path), and receiver **`expect_next_LSN`** after rewind (**spec §7.4**) **agree** — **exclusive‑floor **`fromLSN`** ⇒ commonly **`cursor := fromLSN+1`** for first frame **`lsn = fromLSN+1`**; **inclusive / product variants** **`MUST`** document the mapping **without** drifting **`Drive`** cases.

```text
# —— (a) STATE (per peer P, under one serializer lock) ——
state:
    cursor : LSN                    # next LSN to emit (unsent prefix starts at cursor; §6.2 debt = cursor < head)
    fifo   : queue<Append>          # single mailbox — NO separate backlog/live queues
    head() : LSN                    # consistent read: exclusive next-assign slot (dense: new append takes lsn==head, then head++)

# —— (b) ATOMIC BOUNDARY — entire body runs under shipMu / single-writer serializer (P2, §6.4) ——
# Event sources:
#   NotifyAppend(...) → fifo.push(Append); Drive(peek)
#   timer / ShipOpportunity      → Drive(∅)

def Drive(input):   # input ∈ { ∅, Append{lba,lsn,data} }

    # CASE A — debt: next emit exists before tail (cursor < head).
    # FIFO: pending Append WAITS (§6.3(A) “incoming does not overtake debt”).
    if cursor < head():
        e := substrate.ReadAtLSN(cursor)          # MUST be the record at exactly this LSN (dense)
        emit(e.lba, e.lsn, e.data)
        cursor := cursor + 1                      # exactly one LSN advanced
        return                                    # input (if any) stays in fifo

    # CASE B — no debt: cursor == head. Tail path (steady / post-catch-up).
    if input is Append{lba, lsn, data}:
        if lsn == cursor:                          # dense: next assigned tail
            emit(lba, lsn, data)
            cursor := cursor + 1
            fifo.pop()
            return
        if lsn < cursor:
            fifo.pop()                              # idempotent retry / duplicate notify
            return
        # lsn > cursor — gap / contract violation. NO silent substrate scan to fill (§13 T4a spirit).
        raise CursorGap{cursor, lsn}              # engine MUST rebuild-on-gap / re-anchor (NEGATIVE‑EQUITY ban on “heal by scan”)

    # CASE C — ∅, cursor == head: nothing to ship
    return
```

**Derived invariant (sender → wire)** — **`INV‑WIRE‑WAL‑LSN‑MONOTONIC`**: **successful** **`emit(...)`** sequence in a session yields **strictly increasing `lsn`** with **no gaps** (dense) — **precondition** for **`INV‑RECV‑WAL‑NAIVE`** (**§6.10**).

#### 6.4 Flip / “mode transition” atomicity = **Primary Wal + ship serialization** (**R1**)

The historical **gap-at-flip** bug is **not** a magical “mode bit” — it is **check-then-act** between **“`cursor` caught `head`”** and **a concurrent append** extending **`head`**.

**Normative**: **Cursor catch-up decisions** and **append admission** that affect **`head` / `cursor`** **MUST** be reconciled under **one lock or equivalent serial point**, or use a **double-check** after claiming “caught up” so **no LSN is neither backlog-emitted nor append-emitted**. **“Backlog empty” is `cursor == head` under that serializer’s consistent read.**

#### 6.5 Worst-case bandwidth (**1:1** ship ≤ append)

If **link capacity ≈ append rate**, the shipper **MAY** apply **only scheduling**, not magic:

| Policy | Meaning |
|--------|---------|
| **Drain-only** | **Emit only from `cursor`** until budget; stall or slow append (**admission** product choice). |
| **Lockstep** | **Each append admits at most one forward ship op** pacing **`head−cursor`**. |
| **Append-off, drain-only** | **Temporary freeze** frontend Wal append; **emit until** **debt / `cursor<head` predicate** clears — **protects pin/retention** at latency cost. |

**None** of these create a **second Wal**; they reshape **who runs when inside the same entity**.

#### 6.6 Saturation (**R2**) — **normative fail-close**

If **sustained append rate > sustainable ship rate** for the session, **`head−cursor` diverges** without bound — **timer + priority alone cannot fix math**.

**Normative**: implementation **MUST** surface **fail-close** (end session, engine retry/escalate) **and/or** **admission throttle** after **product thresholds** — **unbounded wait is non-compliant**. **Ops “enough bandwidth”** is **necessary not sufficient**.

#### 6.7 Replica

**One rewind** at session open (**`expected = fromLSN+1` first frame**) then **same monotonic `checkMonotonic`** on recover Wal ingress (**§7**).

**Note**: **`§IV T1–T3, T7`** refine edge cases; **§6.4–§6.6** already narrow them.

#### 6.8 WalShipper scheduling — **implementer checklist** (**SW / code review**)

**Audience**: `seaweedfs` docs + **`seaweed_block`** reviewers — verifies implementation against **§6** without re-deriving prose. Violating any **MUST** is **non-compliant** unless logged in **§13** as an architect-approved exception.

1. **SINGLE-SERIALIZER (`P2`)** — Exactly **one** WalShipper (or mechanically equivalent owning struct) **per peer `P`** runs **`cursor` / emit decisions**. **MUST NOT** attach a second live WAL pump (`core/recovery` backlog scan vs steady **`Ship`** both advancing **`cursor`** for **`P`** without **one** `shipMu` / equivalent).

2. **ONE TAPE** — **`cursor < head`** means unsent prefix on the **same** Primary WAL order; **MUST NOT** model as unrelated “steady mailbox vs recover mailbox”.

3. **PRIORITY (`§6.3`)** — When **`cursor < head`**, **MUST** ship **from `cursor` forward** until policy stops the round (**budget**, **`ctx`**, transport back-pressure) **before** serving **pure new tail** driven only by arrivals that skipped backlog.

4. **TIMER — SELF-DRIVING BACKLOG DRAIN (`§6.3` MUST)** — **MUST** implement a **periodic `ShipOpportunity`** (timer or shipper-internal loop **equivalent**) that attempts **`emit-from-cursor`** so backlog **cannot** depend solely on new appends (**Primary idle starvation forbidden**).

5. **FRAMING ≠ SECOND TAPE (`P3–P4`)** — **`MsgShipEntry`** (legacy bearer) vs **`frameWALEntry`** (dual-lane bearer) is **encode/profile + conn** selection at emit time; **does not** authorize a **forked causal owner** or second **`cursor`**.

6. **`BASE ∥ WAL` WALL-CLOCK OVERLAP (`P6`, binding `G3`)** — **Extent/base bulk** (**§5**) **SHOULD overlap** WAL drain where product permits (**reduces wall-clock pin exposure**). **Does not** add a second WAL serializer — bulk channel is **not** an LSN tape (**§II 4**).

7. **R1 — flip atomicity (`§6.4`)** — “Caught up → tail” decisions **under** the **same serializer lock** **with double-check** vs **`head`** after observing **`cursor == head`**.

8. **R2 — saturation (`§6.6`)** — **`head − cursor` diverges**: **fail-close** **and/or** admission throttle; **never** unbounded silent wait.

9. **`send` STRATEGY (`§6.3`)** — **`debt` non-empty ⇒ oldest (`cursor`) first**; **`debt` empty ⇒ emit tail (`incoming`)**; **timer / loop ⇒ `send(∅, debt)` drain** (**no append required**).

**Dual-mode scope (`§13`)**: Checklist items **(3)(4)(9)** (debt-before-tail priority, periodic idle drain tied to **`send(∅, debt)`**) are **`MUST`** in **`ModeBacklog`**; **`ModeRealtime`** obeys **`§13 — E‑WALSHIPPER‑DUAL‑MODE`**.

**Still open**: flip hysteresis (**§IV `T1`**), **`targetLSN`** vs **`head` at barrier** (**`T2`** — **process refinement in §6.9** must close into an explicit phased rule before code claims “healthy”), bearer policy while backlog exists (**`T3`**). Closing those rows **updates** checklist edge semantics but **does not** relax **(1)**–**(4)** **in **`ModeBacklog`**, **`§13`**, or **(9)** as read **`ModeBacklog`**.

### 6.9 `targetLSN` — **when to use what words** (**job-bound slice** vs **single-tape ship**)

Normative **`WalShipper`** truth is always **§6.2–6.4**: **one ordered tape**, **`cursor` vs `head`**, **`send(incoming, debt)` / `send(∅, debt)`**, **single serializer** (**P2**). **Recover does not contradict steady Wal**: it temporarily adds **lanes** (**base**, dual‑lane framing), not a **second causal ship owner**.

#### **Receiver-hard invariant — `Y` does not cut the WAL into two tapes**

**Normative**: A numeric **`targetLSN = Y` MUST NOT mean** “Primary WAL becomes **two independent ordered logs** (**≤Y** vs **>Y**).” The replica consumes **one** recover-WAL apply story: **§7 `checkMonotonic` / session state** advances on **every** **`(LSN, LBA, data)`** in **strict LSN order** relevant to **`StartSession..EndSession`**, regardless of wire **frame kind** (`WALKindBacklog` vs `WALKindSessionLive` …) or **whether** Primary used **`ScanLBAs`** vs **`NotifyAppend`** to **choose** emission order.

Permitted meaning of **`Y`** (**only**): a **sender-side scheduling / enumerator bound** (**how** Primary drains its **single** tape into frames) plus a **frozen lineage field for barrier/certification**. **Forbidden**: any design where the receiver treats **≤Y** and **>Y** as **separate ingest frontiers**, **restart rewind semantics**, or **reorder-permitted halves** — that **engineering shape is incompatible** (**applies-before / applies-after fights**, **duplicate or stale LSN**, **barrier nonsense**).

**Informative**: Framing prefixes “historical blob then tail blob” **on the wire** is allowed **only if** arrival + apply preserves **single global LSN monotonicity** as seen by **`RebuildSession`** — i.e. it is **one tape carried in two envelopes**, **not two truths**.

#### **Product veto — interpreting `target` as WAL “segmentation” is negative equity**

If **`targetLSN=Y`** appears in **architecture narrative** as **segmenting WAL** — i.e. “first segment done, mission accomplished” — without **WalShipper `cursor→head`** closure, architects classify that pattern as **`NEGATIVE‑EQUITY`**: recurring sessions, stalled adoption, confusion at **receiver** (**§6.9 invariant**) and **`MinPin`/retention** pressure. **`Y` MAY exist as lineage text** (**§IV `T2`**) but MUST NOT substitute for **tape monotonicity**.

#### **Positive model (`HOPE‑SHIPPER‑MONOTONIC`) — the only durable fix**

Implementations converge by keeping **WalShipper** as the **sole** causal advance of **unsent LSN prefix** (**P2**): **`cursor` moves forward monotonically** on **Primary’s one order** (**§6**), never “two halves decide independently.” Recover **inherits** steady Wal **strategy abstraction** (**§6.3**) — **explicitly**:

| Ship opportunity kind | **`send` form** | Role when **`cursor < head` (backlog / debt)** |
|-----------------------|----------------|-----------------------------------------------|
| **No freshly paired append on this tick** | **`send(∅, debt)`** | **Periodic / timer‑driven** **`emit-from-cursor`** — backlog **cannot** rely only on arrivals (**Primary‑idle starvation forbidden** — **§6.8 item 4** **`ModeBacklog`**). |
| **Append visible / tail visible** | **`send(incoming, debt)`** | **Oldest‑unsent first** whenever debt non‑empty (**§6.3**(A)); **`incoming` extends** tape; **never** mailbox split. |

**Normative shorthand**: backlog’s “extra oxygen” is **empty‑input advancement** (**`send(∅, debt)`**); **choose‑among‑eligible** advancement when **`incoming` exists** is **`send(incoming, debt)`** — **same serializer**, **monotone `cursor`**. **Realtime carve‑outs** (**§13**) do **not** fork this story into **Wal segmentation**.

Some implementations (**today `seaweed_block`**) also carry **`targetLSN = Y`** on **lineage / coordinator**. **Mandatory reading**: **`Y` is not “the WAL tape ends here.”** **`head` may exceed `Y` at any instant** during the same session — new appends **extend** backlog as **`cursor < head`** on the **same** tape (**§6.2 operational debt**). Therefore:

| Phrase | Meaning |
|--------|---------|
| **Stream Wal ship** (**primary**) | **`WalShipper` decisions** advancing **`cursor` toward live `head`** under **§6.3 / §13** — **continuous**, append‑fed and timer‑fed in **ModeBacklog** as applicable. |
| **Historical WAL job bound at `Y` (sender-only helper)** | **Primary-only**: a **bounded substrate read** that emits **backlog-class** frames for **`(fromLSN, Y]`** (e.g. **`ScanLBAs`** stops at **`LSN > Y`**). This is **one emission tactic** on the **same** tape — **not** “first tape then second tape” at the receiver (**see invariant above**). |
| **`targetLSN = Y`** | **Lineage + coordinator**: freezes **`Y`** for **barrier text** and **sender bookkeeping**; **does not** fork replica apply order. |

#### Recover **process** (how a `Y`-bearing session reaches caught up and stays healthy)

Architects + implementers **MUST** document which phases their code performs; mismatches (**stopping after step 3 alone**) are **§IV `T2` bugs**. **Informative canonical skeleton**:

1. **Admit**: Engine publishes **`fromLSN`** (often **≈ rebuild semantic lower bound**) and **`Y`** (**`targetLSN`**) unchanged to **receiver lineage** (**§I P7**). **Coordinator** **`StartSession`** records both; **`pin`/recycle** obey **§I P6** / pin wire doc.
2. **Base / extent lane** (**if used**): **GLOBAL block sweep** (**§5**) — dense or sparse **per product** — subject to **`bitmap`** arbitration (**P5**). **Independent of LSN**, except synthetic frontiers pinned by session contract.
3. **Sender: bounded read for backlog-class frames ending at `Y`**: Emit **`(LSN,LBA,*)`** with **`fromLSN < LSN ≤ Y`** through recover bearer (**does not authorize a second replica WAL frontier** — **§6.9 invariant**).
4. **Same serializer: tape tail (**`LSN > Y`**) into frames** (**Phase C**) via **WalShipper** only — **`NotifyAppend`**, **SessionLive**/seal, **`DrainBacklog`**, **ModeRealtime** per **§13**; obeys **`cursor→head`**. **`ScanLBAs` stopping at `Y` does not** permit **`cursor < head`** on Primary to stall without **WalShipper** drain (**§6.2**).
5. **Barrier**: Receiver answers **`AchievedLSN`** consistent with **`Y` + Phase C semantics** (**§IV `T2`** must pin the exact predicate: e.g. **conjunct **`walApplied ≥ Y`** ∧ tail policy ∧ `baseDone`**). **§I P1** completion uses this handshake, **not** “silence after step 3 alone”.
6. **End session → steady**: **`EndSession`**, steady **`Ship`/`NotifyAppend`** resumes; **`cursor == head`** in steady means **implementation‑lag‑only**, not **semantic policy debt reopened** (**§6.2** last paragraph). **Healthy** ⇒ engine **probe/decision** predicates satisfied (**control plane**, not **`Sender.Run` idle** alone).

#### Anti‑patterns (**normative DON’Ts**)

- **Treat “historical WAL job finished ( enumerator past `Y` / coordinator saw through `Y` )” as “WAL recover done”** while **`head > cursor`** on Primary — **violates §6.2**.
- **`targetLSN=Y` marketed as WAL segmentation semantics** (**“Tape A then Tape B”**) — **`NEGATIVE‑EQUITY`** (**§6.9 Product veto**) — violates **receiver invariant**.
- **`targetLSN` as UX throttle** forcing **cheap barrier** followed by **new session churn** whenever **`head` moves**, without **explicit product policy** — **risk per execution institution § Risk envelope**.
- **`targetLSN` as silent substitute** for **`fromLSN` / rewind** (**P7 parity** violation).

Implementations aiming to **omit `Y`** on wire (**pure stream recover**) MUST still obey **§I P1** barrier conjunct with an **alternative engine‑authored cut** — **rename only**, not weaker proof.

### 6.10 **Receiver recover invariants** — **`bitmap` core** (**P5**) · **WAL naive** · **race‑free arbitration**

**§I P5** already requires **`bitmap`** visibility for **Wal‑claims** vs **base**. **Normative clarification**: the **`bitmap` is not an optional “parallelism tax”** detachable from recover — it is the **receiver’s single arbitration surface** for **“has a Wal touched this LBA in this session scope?”** regardless of whether base is **dense**, **sparse**, **striped workers**, or **wall‑clock overlapped** with Wal (**§6.8 item 6 / G3**). **Dropping `bitmap` in favor of naive base-only apply** is a **different product mode** (typically **serial BASE → Wal**) and **MUST** be labeled as such (**mini-plan §12 candidate “A‑serial”**).

| ID | Invariant (**receiver recover path**) |
|----|----------------------------------------|
| **`INV‑RECV‑BITMAP‑CORE`** | **`bitmap`** is the **sole** gate that lets **recover base** mutate an LBA after **Wal** may have asserted **Wal‑truth** on that LBA. **Base apply** **`MUST`** follow a **single critical pattern**: **observe / claim “not Wal‑protected” and write base bytes in one race‑free atomic step** — e.g. **`CAS`/compare‑and‑set** style **`0→1`** on **`bitmap[lba]`** (or **`TryClaimBase(lba)`** with equivalent semantics); **winner** performs **`ApplyBaseBlock`** and sets **`claimed`**; **loser** **skips** base write. **Wal apply** **`MUST NOT`** rely on a **read‑then‑later‑write** of **`bitmap`** that can interleave with base (**§6.10 anti‑pattern** below). |
| **`INV‑RECV‑WAL‑NAIVE`** | **Recover Wal tuples** (**`frameWALEntry`**) **`MUST`** apply with **substrate LWW** (**write payload**) **without** consulting **`bitmap`** before write **for stale‑Wal micro‑comparison** — **Wal vs Wal ordering** is **`LSN`** monotonic on the **single ingest frontier** (**§6.9 receiver-hard**, **`checkMonotonic`**, precondition **`INV‑WIRE‑WAL‑LSN‑MONOTONIC`** **§6.3** pseudocode). After successful Wal substrate write, **`MUST`** **set **`bitmap`/Wal‑claim** for **`lba`** so **later base** loses **`CAS`**. Ordering inside the implementation **`MUST`** be **Wal data durable before observable claim** (or use one **`per‑LBA` / `shipMu`‑compatible** serialization that merges data+claim — product picks; **wrong interleavings** ⇒ **NEGATIVE‑EQUITY‑class bugs**). |

##### Reference pseudocode — **`ApplyWAL` / `ApplyBASE`** (**per‑LBA atomic envelope`)

**Naming**: **`store.lock(lba)`** = **one race‑free critical section** **`MUST`** cover **`data write` ∪ `bitmap` transition** together — equivalent **`bitmap.CAS`** patterns — **informative** notes below.

```text
# —— (a) STATE ——
state:
    bitmap : array[NumLBAs] of bit, init 0      # CORE — INV-RECV-BITMAP-CORE
    store  : substrate                          # exposes per-LBA lock OR global writer (see equivalence note)

# —— WAL — precondition: ingest order ≡ strict LSN increase (INV-WIRE-WAL-LSN-MONOTONIC §6.3) ——
def ApplyWAL(lba, lsn, data):
    with store.lock(lba):                       # atomic envelope (INV-RECV-BITMAP-CORE + WAL-NAIVE)
        store.write(lba, data)                  # naive LWW vs prior Wal bytes — no bitmap pre-check
        bitmap[lba] := 1                        # claim set BEFORE releasing lock

# —— BASE — may race WAL on same lba under BASE ∥ WAL (§6.8 #6) ——
def ApplyBASE(lba, data):
    with store.lock(lba):                       # SAME lock family as ApplyWAL
        if bitmap[lba] == 0:
            store.write(lba, data)
            bitmap[lba] := 1
        # else: Wal already authoritative for this session scope — stale base snapshot SKIP

# ANTI-PATTERN (banned — same prose as §6.10 DON’T below):
#   if bitmap[lba] == 0:
#       with store.lock(lba): store.write(lba, data)
#       bitmap[lba] := 1
```

**Informative equivalence**: if substrate serializes **all** **`Apply*`** via **one** goroutine, lock may degrade to **global queue** + **`bitmap` atomic** — **`CAS`**‑only formulations **remain valid** (**§9** tests **may** simulate either).

**Informative (**substrate **`Write` arity**)**: **`ApplyBASE`** payloads **typically lack author Wal **`LSN`****; **synthetic frontier** for extent layer **MAY** use **session **`targetLSN` / lineage anchor** per **`RebuildSession` / §5** — **architect** pins if **`Write(lba, data, effectiveLSN)`** becomes required.

**Anti-pattern (normative DON’T)** — **`read bit; compute elsewhere; later base write`** without **`CAS`/lock** vs concurrent **`Wal`** — permits **Wal data then base overwrite** (**lost update**).

**Session scope**: recover **`bitmap`** state **`SHOULD`** be **scoped to `StartSession…EndSession`** (or **`RebuildSession`** lifetime) **unless** substrate definition extends Wal‑claims — **explicit** in leaf spec (**architect**) so **`streamBase`/parallel base** races cannot cross **stale boundaries**.

---

### 7. Replica routing table (**P5 operationalized**)

| Ingress | Shape | Action |
|---------|-------|--------|
| Steady `MsgShipEntry` | WAL tuple | Substrate apply; **LWW** across LSN |
| Recover `frameWALEntry` | WAL tuple | Substrate apply **+** **`bitmap` WAL-claim** |
| Recover `frameBaseBlock` | Bytes | **`bitmap` gate** → **`ApplyBaseBlock` / synthetic frontier** |

**`checkMonotonic`**: protocol defense on **recover WAL** stream (**gap / backward / dup / +1**). **Bitmap / base race atomicity** (**`INV‑RECV-*`**) — **`§6.10`**.

---

## III. Coordination, wiring, failure surface

### 8. Coordinator & pin

**`PeerShipCoordinator`**: single routing truth **per volume** (`StartSession`/`EndSession`, phase).  
**Pin / recycle**: **`v3-recovery-pin-floor-wire.md`**, **`MinPinAcrossActiveSessions`**.

### 9. Wiring & transports

**Legacy + `port+1` dual-lane** — **`v3-recovery-wiring-plan.md`**.  
Bearer differs; **§7 semantic is P3**.

### 10. Failure taxonomy pointer

**`Failure*` matrix / engine bypass** — **`core/recovery/failure.go`** + transport classification. **Option B** (retry budget) orthogonal.

---

## IV. 未定细节 · 协议→执行 风险登记 (**explicit TBD**)

**Policy**: every row is **allowed to stay open** until a **leaf spec or code contract** closes it. **Closing** = promote into **§II–§III** or a linked doc + **INV/test pin**.

| ID | Open question | Why it can bite at code time |
|----|----------------|------------------------------|
| **T1** | **“Caught up” / flip policy** (**refines §6.4**): strict `cursor == head` vs hysteresis **`head − cursor < ε`** vs **time-based idle**; **product** semantics when **flip races append** — serializer already required | Premature declaring **normal** ⇒ **gap** if **outside §6.4**; late flip ⇒ long session / pin |
| **T2** | **Close into §6.9 + explicit barrier predicate** — **`Y` never forks receiver WAL ingest** (**one LSN-monotonic stream**); **`targetLSN = Y`** is **sender enumerator / lineage freeze** only; **tail** **`LSN > Y`** ships via **WalShipper** same session (**Phase C**); **Barrier `AchievedLSN`** predicate agreed with replica (**exact tail clause** vs **moving `head`**) (**§IV T2 residual**). | Premature success, duplicate/stale apply, or **ordering chaos** if **≤Y vs >Y** modeled as **split tapes** |
| **T3** | **Bearer choice while `cursor < head`**: **steady bearer silent vs dual-lane only** (**§II–§III** closes); **either way** obey **§6 P2 single serializer** | **P2** if two pumps both emit unconsumed prefix |
| **T4** | **Duplicate** `lsn == applied` on recover path: **hard error** vs **idempotent no-op** | Retransmit policy vs **checkMonotonic** |
| **T5** | **Sparse base** vs dense full-LBA — contract with **bitmap** density | Performance / correctness on huge volumes |
| **T6** | **Multi-replica RF>2**: independent **`cursor[P]` per peer**, not “modes” per replica | Out of **current** binary topology scope — RF>2 convergence **needs** explicit coordinator story + tests |
| **T7** | **Saturation policy detail** (**§6.6**): threshold timing, hysteresis vs flapping sessions, UX/ops signaling | Fail-close/throttle correctness under bursty workloads |
| **T8** | **Replica/session liveness** (**R3**): deadline, eviction, **no-progress** watchdog; **`PinUnderRetention` storms** (**R4**) — **pin_floor** vs **`S`** vs ack cadence; **distinct timers** for stall vs **Primary-restart / fencing** (**R5** — see **`v3-recovery-execution-institution.md`**) | Wrong coupling ⇒ **silent hang** or **spurious recycle** |

**Residual engineering catalog (**does not overturn **§I**; closes in leaf specs**)**: **dual-lane ingress accounting → one RebuildSession** (**R6**); **substrate Scan contract vs pin** (**R7**); **misconnect / cross-volume tests** (**R8**). **Architect direction**: protocol-first pinning before large code divergence.

### IV.1 Cross-cutting **progress envelope** (not recover-only)

Steady **replicate** and **recover** share the same physical bottlenecks: **Wal `head` growth**, **ship `cursor`**, **extent/base bulk**, **receiver apply**, **`SetPinFloor` / `MinPin` / retention `S`**. **§6.6** and **§IV T7–T8** apply whenever **sustained ingress exceeds sustainable drain** — including **non-session** operation. **P5 `bitmap`** gives **WAL-vs-base** correctness at the replica; it does **not** guarantee **throughput parity** or **pin velocity**.

**Design obligation**: product SHOULD define a single **progress envelope** (metering, throttle, watchdog, degraded / fail semantics) for **steady + recover**, then specialize budgets per phase — see **`v3-recovery-execution-institution.md` § Risk envelope**.

**Operational analogy** (informative): distributed block systems expose **admission**, **recovery vs foreground limits**, **degraded replicas** rather than indefinite internal queue growth; our failure modes should remain **explicit** at the engine/ops boundary.

---

## V. Binding gaps · QA checks · history · index

### 11. Binding gaps (until closed)

| ID | Statement |
|----|-----------|
| **G0** | **`WalShipper` unified loop** (**§6**, checklist **§6.8**) in **production** — **cursor + serializer**, not orphaned sender pumps |
| **G1** | **P2** proven by tests (**CHK-WALSHIPPER-SINGLE-CURSOR** or superseding suite) |
| **G2** | **P7** — dual-lane obeys engine **`fromLSN`** |
| **G3** | **P6** — base ∥ WAL overlap (**perf**) |

### 12. QA checks

| ID | Predicate |
|----|-----------|
| **CHK-WALSHIPPER-SINGLE-CURSOR** | **No duplicate logical `lsn` to same peer** on two live outbound paths (**§6.3**) |
| **CHK-WALSHIPPER-TIMER-DRAIN** | **`ModeBacklog` only** ( **`§13` — E‑WALSHIPPER‑DUAL‑MODE** ): no sustained **`cursor ≪ head`** while **Primary append‑idle** without timer‑driven **`emit-from-cursor`** attempts (**§6.8 item 4**); aligns **`v3-recovery-wal-shipper-spec.md` §9** `Test-Timer-Drains-Idle` |
| **CHK-BARRIER-BEFORE-CLOSE** | **P1** |
| **CHK-RECOVER-REWIND-ONCE** | First post-open recover-WAL expects **`fromLSN+1`** |

### 13. Architect-approved exceptions (**binding**)

Controlled relaxations of **§6** prose **without** weakening **§I**. Each row **requires** engineer + architect sign‑off **before merge** unless **already Approved** herein.

#### E‑WALSHIPPER‑DUAL‑MODE — ModeRealtime vs ModeBacklog

| Field | Statement |
|-------|-----------|
| **ID** | **E‑WALSHIPPER‑DUAL‑MODE** |
| **Status** | **Approved** (**2026-04-30**). |
| **Motivation (**T4a / fresh receiver**)**: Substrate‑scan WAL ship during **steady Realtime** **`NotifyAppend`** can replay **dead‑window** bytes and violate **no‑replay** session invariants asserted by tests/product. |
| **`ModeBacklog`** (recover / catch‑up Wal path on WalShipper) | **`§6.3**(A)** `send(incoming, debt)`, **`§6.3**(B)** `send(∅, debt)` / periodic **`ShipOpportunity`**, and **§6.8**(3)(4)(9) **`MUST`** hold **in full**. Timer **`MUST`** attempt **`emit-from-cursor`** when **`cursor < head`** even if Primary append‑idle (**§6.8 item 4**). **Priority (= oldest unsent)** **`MUST`** use substrate **`ScanLBAs`** (or equivalent single‑tape authoritative read). |
| **`ModeRealtime`** (steady per‑append **`NotifyAppend`**) | **Literal §6.3(B)** “idle timer **`MUST`”** **`MUST NOT`** apply — ship is **`NotifyAppend`**‑driven. **Hot path **`MUST NOT`** substrate‑replay** for ship byte selection (**caller `data`** is canonical on optimized tail emit). **`send(incoming, debt)` debt‑priority **`MUST NOT`** be re‑interpreted as “Realtime must drain before tail”**: gap **`cursor < head`** **`MUST`** be corrected only via **`ModeBacklog`** + **`DrainBacklog`** / coordinator **re‑anchor**, not Realtime substrate scan. **`CHK‑WALSHIPPER‑TIMER‑DRAIN`** applies to **Backlog** only (see **§12** predicate). |
| **Safety switch** | Implementations **SHOULD** expose **`StrictRealtimeOrdering`**: **log‑warn** on dense WAL when **`NotifyAppend`** **`lsn`** **≠** **`expected_next_tail`** **by default** — **canonical** (**§6.3 **`Drive`**, **`cursor` = next emit**): **`lsn == cursor`** on the debt‑free path; **`lastEmitted`‑style** internal fields **`MUST`** compare **`lsn == lastEmitted + 1`** **equivalently** (**single contract** across shipper code, **`Drive`** prose, and tests). **Strict / error path** optional **production** opt‑in. **Hard opt‑in** (`strict=true`): engine **SHOULD** require prior proof of **ordering discipline** (**rebuild‑on‑gap**, fresh peer policy). Operational detail **`v3-recovery-wal-shipper-mini-plan.md` §11.6**. |
| **Not relaxed** | **P2** single serializer (**§6.8(1)**); **§6.4** (**R1**); **§6.8(5)** framing≠second tape; **§6.6** (**R2**) observability hooks; **§I P1–P7**. |

**Doc bridge**: **`v3-recovery-wal-shipper-mini-plan.md`** **§11.2 dual‑mode contract** ⇄ this exception.

---

### 14. Revision

| Date | Change |
|------|--------|
| 2026-04-27 | v1 stub |
| 2026-04-29 | v2 WalShipper / receiver |
| 2026-04-29 | **v3.1**: **How to read** table ↔ **§I P1–P7** numbering; **§IV** TBD cross-refs |
| 2026-04-29 | **v3.2**: **Wal shipper as one tape + `cursor`/pin/timer** (**§6**); **flip = Wal+ship atomicity (**R1**)**; worst-case **1:1** scheduling table; saturation **§6.6 (**R2**)**; **§IV** T7–T8 + R-catalog; QA **CHK-WALSHIPPER-SINGLE-CURSOR** |
| 2026-04-29 | **v3.3**: **§IV.1 progress envelope** (steady+recover); **T8** disambiguates R3/R4 vs **R5 restart**; **execution institution** risk ledger **R1–R6**, spec-before-delete‑bad‑code ordering, substrate admission note (**WALStore** vs **smartwal**) |
| 2026-04-29 | **v3.4**: **§6** → **`v3-recovery-wal-shipper-spec.md`** (**implementable** WalShipper; unified wal stream docs must align) |
| 2026-04-29 | **v3.6**: **`v3-recovery-wal-shipper-mini-plan.md`** (**spec→code bridge**); **doc map** |
| **2026-04-30** | **v3.7**: **`§6.8` implementer checklist** (single serializer, priority, timer drain, framing≠tape, **`G3` pointer**); **§V** **`CHK-WALSHIPPER-TIMER-DRAIN`** |
| **2026-04-30** | **v3.8**: **§6.2–6.3** — **policy vs operational debt**, **moving-target / gap→0**, **`send(incoming, debt)` / `send(∅, debt)`** strategy abstraction + **steady debt-free sends**; **§6.8** item **(9)**; **`T2`** cross-ref |
| **2026-04-30** | **v3.9**: **§13** (**E‑WALSHIPPER‑DUAL‑MODE**); **`§14`/`§15` renumber**; **§6.3** carve‑out pointer; **`CHK‑WALSHIPPER‑TIMER‑DRAIN`** scoped per **§13** |
| **2026-04-30** | **v3.10**: **§6.9** — **`targetLSN=Y`** as **historical WAL job bound** (`(fromLSN,Y]`) vs **WalShipper `cursor→head` stream**; phased **recover process**; **§I P7** / **P1** / **§IV T2** aligned (**`Y` is not tape end**) |
| **2026-04-30** | **v3.11**: **§6.9** — **receiver-hard invariant**: **`Y` MUST NOT denote two WAL tapes** / **dual apply frontiers**; **one LSN order** (**frame kind ≠ second truth**) |
| **2026-04-30** | **v3.12**: **§6.9** — **`NEGATIVE‑EQUITY`** if **`target`** read as WAL **segmentation**; **`HOPE‑SHIPPER‑MONOTONIC`**: backlog = **`send(∅, debt)`** + **`send(incoming, debt)`** (**§6.3**) |
| **2026-04-30** | **v3.13**: **§6.10** — **`INV‑RECV‑BITMAP‑CORE`** (**`CAS`‑style base**); **`INV‑RECV‑WAL‑NAIVE`**; **`bitmap`** = **core P5 arbitration** (not optional “parallel bit” alone); session scope |
| **2026-04-27** | **v3.14**: **§6.3** — normative **`Drive(input)`** pseudocode (**state / atomic boundary / `INV‑WIRE‑WAL‑LSN‑MONOTONIC`**); **§6.10** — **`ApplyWAL`/`ApplyBASE`** pseudocode (**`INV‑RECV‑*`**); **§6.1/§V** cursor↔rewind alignment note; **§13** **`StrictRealtimeOrdering`** keyed to **`Drive`** **`cursor`** convention |

### 15. Document map

| Doc | Role |
|-----|------|
| **This** | **Foundation + index**; **`§6.8`** checklist; **`§13`**; **`§6.3`/`§6.9–§6.10`** — **`Drive`/`Apply*`** pseudocode, **`INV‑WIRE‑WAL‑LSN‑MONOTONIC`**, **`NEGATIVE‑EQUITY`**, **`HOPE‑SHIPPER‑MONOTONIC`**, **`INV‑RECV‑BITMAP‑CORE`** / **`INV‑RECV‑WAL‑NAIVE`** |
| `v3-recovery-pin-floor-wire.md` | Pin bytes |
| `v3-recovery-wal-shipper-mini-plan.md` | **Implementation bridge** (**seaweed_block** phased PR ↔ spec §INV) |
| `v3-recovery-wal-shipper-spec.md` | **WalShipper algorithm** (priority, **R1**, **INV-***) |
| `v3-recovery-unified-wal-stream-*.md` | **Align to WalShipper spec** or mark superseded |
| `v3-recovery-wiring-plan.md` | Ports / flags |
| `v3-recovery-inv-test-map.md` | INV ↔ tests |
| `v3-recovery-execution-institution.md` | Lifecycle + **risk envelope** / spec→delete-code ordering + **entity layering & assembly contract** (SW: where **attempt** / muscles may attach) + **§ Management plane vs data-plane recover** (**single index** for probe / quorum / RF>2 admission — **no parallel recovery hub**) |
| `v3-recovery-live-line-backlog-spec.md` | Retired stub → **here** |
| `v3-storage-logical-pin-gate.md` | **LogicalStorage substrates + `RecycleFloorGate` / pin** |
