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
- **Authoritative convergence** toward **healthy replicated state** is proven only via **explicit barrier handshake** **`AchievedLSN ≥ engineered target`** together with **`baseDone` / layer-1 conjunct as defined by product**. **Inferring “done” from traffic silence, Kind ratios, or sender idle windows is forbidden.**

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

- **`fromLSN` / `pinLSN` / `targetLSN`** are **frozen or engine-authored** facts at session admission; transport **silently overwriting** (`fromLSN := 0` when engine chose `R+1`) violates **parity** with decision logic.

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

**Still open**: flip hysteresis (**§IV `T1`**), **`targetLSN`** vs **`head` at barrier** (**`T2`**), bearer policy while backlog exists (**`T3`**). Closing those rows **updates** checklist edge semantics but **does not** relax **(1)**–**(4)** **in **`ModeBacklog`**, **`§13`**, or **(9)** as read **`ModeBacklog`**.

### 7. Replica routing table (**P5 operationalized**)

| Ingress | Shape | Action |
|---------|-------|--------|
| Steady `MsgShipEntry` | WAL tuple | Substrate apply; **LWW** across LSN |
| Recover `frameWALEntry` | WAL tuple | Substrate apply **+** **`bitmap` WAL-claim** |
| Recover `frameBaseBlock` | Bytes | **`bitmap` gate** → **`ApplyBaseBlock` / synthetic frontier** |

**`checkMonotonic`**: protocol defense on **recover WAL** stream (**gap / backward / dup / +1**).

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
| **T2** | **`targetLSN` vs moving `head`**: if **`head > target`** at session start, does backlog drain stop at **target** for **barrier** only, or always emit to **head**? **Related (§6.2)**: **catch-up closes `cursor→head` gap** (operational debt), not “match **`head`’s value at an instant**”; **barrier / pin** still need an **engine-chosen cut** agreed with receiver. | **Barrier false negatives** or **double application** if sender/receiver disagree |
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
| **Safety switch** | Implementations **SHOULD** expose **`StrictRealtimeOrdering`**: **log‑warn** on **`lsn ≠ cursor + 1`** (dense WAL) **by default**; **strict / error path** optional **production** opt‑in. **Hard opt‑in** (`strict=true`): engine **SHOULD** require prior proof of **ordering discipline** (**rebuild‑on‑gap**, fresh peer policy). Operational detail **`v3-recovery-wal-shipper-mini-plan.md` §11.6**. |
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

### 15. Document map

| Doc | Role |
|-----|------|
| **This** | **Foundation + index**; **`§6.8`** implementer checklist; **`§13`** architect-approved exceptions (**E‑WALSHIPPER‑DUAL‑MODE**) |
| `v3-recovery-pin-floor-wire.md` | Pin bytes |
| `v3-recovery-wal-shipper-mini-plan.md` | **Implementation bridge** (**seaweed_block** phased PR ↔ spec §INV) |
| `v3-recovery-wal-shipper-spec.md` | **WalShipper algorithm** (priority, **R1**, **INV-***) |
| `v3-recovery-unified-wal-stream-*.md` | **Align to WalShipper spec** or mark superseded |
| `v3-recovery-wiring-plan.md` | Ports / flags |
| `v3-recovery-inv-test-map.md` | INV ↔ tests |
| `v3-recovery-execution-institution.md` | Lifecycle + **risk envelope** / spec→delete-code ordering |
| `v3-recovery-live-line-backlog-spec.md` | Retired stub → **here** |
| `v3-storage-logical-pin-gate.md` | **LogicalStorage substrates + `RecycleFloorGate` / pin** |
