# V3 Phase 15 — G7 (Rebuild / Replica Re-creation) Mini-Plan

**Date**: 2026-04-27 (v0.1 — kickoff draft for architect ratification)
**Status**: **DRAFT** — §1–§6 await architect + QA review per `v3-batch-process.md`
**Repo**: `seaweed_block` (V3) — code; `seaweedfs` — design docs / mini-plans
**Owner**: sw (mini-plan + code + tests); QA (harness + m01 verification)
**Process**: `v3-batch-process.md` compressed flow (one mini-plan, one PR per repo, one §close)
**Predecessors**:
- G6 closed at `seaweedfs@4a876a9cd` — retention-aware catch-up + WALRecycled → rebuild **dispatch** + operator retention knob + m01 single-run evidence.
- T4d-4 muscle (engine-driven recovery, rebuild executor, per-peer adapter wiring) — already landed; G6 proved **escalation into** rebuild; G7 closes **rebuild path behavior** end-to-end under expanded scenarios.

**QA kickoff brief** (2026-04-27): incorporated below as candidate AC + harness notes; **architect must bind** open questions in §1.A.

---

## §1 Scope

> **Scope-rule (unchanged from G5-5C / G6)**: *master owns identity / topology; primary + engine own data recovery; the protocol aligns the two via `(PeerSetGeneration, epoch, EndpointVersion)` fences.* G7 stays on the primary/engine/replica data path — **no master protocol expansion** unless a discovery forces it (default: no).

G6 proved: **Decision=Rebuild + RebuildPinned + StartRebuild dispatch**, per-peer executor wiring, **rebuild session start** log marker, **byte-equal after rebuild** on hardware (recycle-escalation path), and operator-visible WAL retention. **V2 remote rebuild** (Phase 20 note) shows large rebuild can be fast on RoCE — informational for G7, not a new SLO.

G7 closes **rebuild / replica re-creation** as a **product-credible** path: not only “dispatch happened,” but **correctness and safety under join, failure, concurrency, and stale WAL** where those scenarios are in scope.

### What G7 inherits (evidence — not re-proven unless §1.A says otherwise)

| Component | Evidence |
|---|---|
| Engine Rebuild decision + `StartRebuild` dispatch | G5-5C #4 + G6 engine table-driven tests |
| Adapter → rebuild executor | G5-5C Batch #7 |
| Executor: `executor: rebuild start replica=<id> sessionID=<n> epoch=<n> EV=<n> targetLSN=<n>` (hardware-visible `log.Printf` literal; G6 pinned at `seaweed_block@85475cd` `core/transport/rebuild_sender.go:41`) | G6 + `core/transport/rebuild_sender.go` (NOT the internal RecoveryLog event name `exec_rebuild_started` — that's process-local Orchestrator.Log; see §harness-notes) |
| Hardware: dispatch + byte-equal post-rebuild | G6 #5 (2026-04-28 m01 run) |
| V2 remote rebuild wall-clock (1 GB ~2 s) | Phase 20 / `phase20_rebuild_verified.md` (informational) |
| `--wal-retention-lsns` | G6 #2–#5 |

**sw default stance**: G7 is **verification-bias** — expect **§1.H → PROCEED-verify-only** unless audit finds a real hole. Code changes should be **tests + harness + log/obs clarity**, not new recovery primitives.

### What G7 delivers (candidate scenarios — numbering from QA brief)

| # | Scenario | In scope for v0.1? |
|---|---|---|
| 1 | WALRecycled → rebuild → byte-equal | **Fold G6 §2 #5 evidence** unless architect binds **fresh re-run** (see §1.A). |
| 2 | New empty replica joins running cluster → full rebuild → byte-equal | **Likely headline AC** — architect binds 2-node vs 3-replica topology (§1.A). |
| 3 | Rebuild mid-flight primary kill → next primary continues / fails-cleanly | **Candidate** — architect binds in vs defer to G8 (failover). |
| 4 | Rebuild during concurrent writes (live ship + recovery lane) | **Candidate** — architect binds G7 vs G8. |
| 5 | Stale-but-not-empty WAL on replica → rebuild path correctness | **Candidate** — depends on harness + audit. |
| 6 | Large-volume rebuild timing (no SLO) | **Informational only** — **SLO lives at G21** (see §1.A). |

### Architecture touchpoints

- `v3-architecture.md §7` Recovery Architecture — rebuild session, executor, completion → engine truth
- `v3-runtime-state-machines-overview.md` — probe loop vs recovery sessions (G7 scenarios must not confuse “peer healthy” with “rebuild complete”)
- `v3-phase-15-mvp-scope-gates.md` — G7 gate definition vs G8 failover

### What G7 does NOT deliver (explicit non-claims unless architect expands §1)

- **Performance SLO** — wall-clock record only; G21 owns SLOs.
- **Placement / multi-volume / RF>2 product semantics** — unless explicitly pulled in for scenario #2 topology.
- **Metrics dashboard** — G5-3.
- **Failover election / leader continuity** as primary scope — if #3 is out, pointer to G8.

### Files (preliminary — finalized at §1.H code-start)

Audit-first. Likely touch: `core/engine/*`, `core/replication/*`, `core/host/volume/*`, `cmd/blockvolume/*`, `scripts/*` / QA harness on `V:\share\g5-test\`, new `g7_*_test.go` as needed. **Exact list after §1.H.**

### Architecture truth-domain check (`v3-architecture.md §4`)

| Truth domain | This batch (G7) |
|---|---|
| Master / control plane | **Default: no change** (same fence as G6 §2 — diff inspection). |
| Primary / replication / engine | **Read-heavy + tests**; possible small observability/logging patches for harness pins. |
| Replica | **Sessions + apply path** — verify-only unless audit finds bugs. |

---

## §1.A Architect bindings (OPEN — ratify before code-start)

| Question | Options | **Decision** |
|---|---|---|
| **Q1** — Topology for empty-replica-join (#2): 2-node/2-role vs third replica / 3-process? | (a) stay m01 **2-node 2-replica** with “join empty peer” simulation; (b) **3-replica** topology requires extra config | ⏳ **TBD architect** |
| **Q2** — Headline AC: is #2alone the “hero,” or parity among #2+#3+#4+#5? | (a) #2 headline; (b) **multi-axis** gate | ⏳ **TBD architect** |
| **Q3** — Deadline budget for rebuild **completion** (not just dispatch): align with G6 §2 #5 **30 s** dispatch window; separate **completion** bound vs best-effort + byte-equal-only? | (a) dispatch + completion **both** bounded in §2; (b) dispatch bounded; completion = best-effort + byte-equal | ⏳ **TBD architect** |
| **Fold G6 #5 into G7 #1** vs **fresh re-run** | (a) **reference G6 log + commit** in §close; (b) **re-run** on G7 branch for cleanliness | ⏳ **TBD architect** — sw default **(a)** to avoid duplicate hardware |
| **Rebuild perf (#6)** | (a) **G7 informational** (log wall-clock); (b) defer entirely | ⏳ **TBD architect** — sw default **(a)** if large volume available |
| **Concurrent rebuild + writes (#4)** | (a) **G7 in scope**; (b) **defer to G8** | ⏳ **TBD architect** |
| **Primary-kill during rebuild (#3)** | (a) **G7 in scope**; (b) **defer to G8** | ⏳ **TBD architect** |

### §1.H code-start audit (before production code)

Same discipline as G5-5C / G6: sw runs a **read-only audit** of rebuild executor completion path, engine `SessionCompleted` / rebuild truth, adapter release, and **harness-visible log markers**. Verdict: **verify-only** / **minor-patch** / **halt → evolution batch**.

**Default expectation**: **verify-only** — T4d-4 + G6 already exercised the stack; G7 adds **scenario coverage** + INVs.

---

## §2 Acceptance criteria (v0.1 draft — tie to §1.A ratification)

| # | Criterion | Verifier |
|---|---|---|
| 1 | **§1.H audit** published with verdict. | sw audit commit |
| 2 | **Empty-replica join → full rebuild → byte-equal** (topology per Q1). | m01 + harness |
| 3 | **G7 #1 (recycle → rebuild path)** — either **(a)** cite G6 §close evidence + same commit pins, or **(b)** fresh hardware re-run per §1.A. | §close evidence table |
| 4 | **Primary-kill mid-rebuild** — only if §1.A keeps in scope; else **explicit NON-CLAIM** with pointer to G8. | scenario / deferred |
| 5 | **Concurrent writes during rebuild** — only if §1.A keeps in scope; else deferred. | scenario / deferred |
| 6 | **Stale WAL → rebuild correctness** — if in scope; else deferred. | engine/harness tests + optional m01 |
| 7 | **Harness: `wait_until_rebuild_complete`** — polls primary.log for `executor: rebuild complete, sent <n> blocks (targetLSN=<n>)` (hardware-visible literal from `core/transport/rebuild_sender.go:120`), filtered by the same `sessionID=<n>` as the prior START dispatch. Pair with **byte-equal** + `wait_until_peer_healthy` per `INV-G6-HARNESS-DATA-AND-STATE-CONVERGENCE`. See §harness-notes for full marker pinning + the v0.1 correction note. | QA |
| 8 | **No master diff** — same fence style as G6 (`core/host/master/` etc. **zero** unless architect explicitly frees). | `git diff --stat` |
| 9 | **Large rebuild timing** (#6): if in scope, **record** duration in §close — **no SLO assertion**. | QA log excerpt |

Architect may **renumber**, **split**, or **drop** rows when ratifying §1–§6.

---

## §3 Invariants to inscribe at close (proposed)

| INV ID | Claim (draft) | When inscribed |
|---|---|---|
| `INV-G7-EMPTY-REPLICA-JOIN-DISPATCHES-REBUILD` | Adding / rejoining an empty replica on a running volume triggers rebuild execution and convergence to consistent state. | If #2 in scope |
| `INV-G7-REBUILD-PRESERVES-CONCURRENT-WRITES` | Live writes during rebuild cannot corrupt replica state vs primary; post-rebuild byte-equal holds. | If #5 in scope |
| `INV-G7-REBUILD-IDEMPOTENT-ON-PRIMARY-RESTART` | Primary restart during rebuild does not wedge volume permanently; system reaches defined safe/failed state per spec. | If #4 in scope |
| _(additional)_ | Replica stale-not-empty WAL → rebuild correctness | If #6 in scope |

Exact wording **PR-atomic** with code + `v3-invariant-ledger.md` at §close.

---

## §4 G-1 V2 read

**Default N/A** — G7 exercises V3 engine + host path already PORTed. If §1.H surfaces a V2 contract question, sw adds a **short G-1 addendum** in-doc per `v3-batch-process.md §6.1`.

---

## §5 Forward-carry consumed (from G6 §close)

| Carry | Disposition in G7 |
|---|---|
| Rebuild path semantics “G7 territory” (G6 non-claim) | **Primary G7 scope** |
| `INV-G6-HARNESS-DATA-AND-STATE-CONVERGENCE` | **Reuse** — chained steps still need **data + state** convergence, now add **rebuild complete** pin |
| β/γ replica-aware retention (`INV-G6-RETENTION-POLICY-REPLICA-AWARE` reserved) | **Still out of G7** unless architect merges batches |
| G5-2 / G5-6 durability, G5-3 metrics | **Unchanged** — not G7 unless pulled in |

---

## §6 Risks + mitigations

| Risk | Mitigation |
|---|---|
| G7 duplicates G6 hardware time | §1.A default **fold G6 #5** by reference. |
| Ambiguous "rebuild done" vs "peer healthy" | Harness uses **both** the hardware-visible `executor: rebuild complete, sent <n> blocks (targetLSN=<n>)` log marker **and** `wait_until_peer_healthy` (peer-state transition) + `wait_until_byte_equal` per G6 lesson + `INV-G6-HARNESS-DATA-AND-STATE-CONVERGENCE`. (The internal RecoveryLog `exec_rebuild_completed` event name is NOT the harness pin — see §harness-notes v0.1 correction.) |
| Scope creep (#3–#5 pull G8 work) | §1.A **must** place #3–#5 in G7 vs G8. |
| Perf confusion with G21 | §2 #9 is **record-only**; no SLO. |

---

## §7 Sign table (v0.1 — fill as work lands)

| Item | Owner | When | State |
|---|---|---|---|
| §1.A bindings (Q1–Q3 + scenario placement) | architect | Before code-start | ⏳ |
| §1–§6 ratification of v0.1 | architect + QA review | Before audit | ⏳ |
| §1.H audit | sw | After ratification | ⏳ |
| Code + tests + harness | sw + QA | After audit | ⏳ |
| m01 runs | QA | After harness | ⏳ |
| §close + ledger + architect sign | sw → architect | Per `v3-batch-process.md §5` | ⏳ |

---

## §harness-notes (sw — for QA pre-work without architect blockers)

**v0.1 correction 2026-04-28**: the original v0.1 draft of this section pointed at `exec_rebuild_started` / `exec_rebuild_completed` as the harness markers. Those are **RecoveryLog event names** (internal Orchestrator.Log, NOT visible in primary.log on hardware). QA's pre-work survey confirmed via source grep that the hardware-visible literal strings are produced by `log.Printf` in `core/transport/rebuild_sender.go`. Those are the strings the harness must scrape. Corrected pins below.

**Hardware-visible rebuild markers** — produced by `log.Printf` in `core/transport/rebuild_sender.go` and routed to the daemon's stdout/stderr stream (which the iterate harness captures into `${REMOTE_RUN_DIR}/logs/primary.log`):

| Marker | Source | Literal string |
|---|---|---|
| **START** (G6 #1, pinned) | `core/transport/rebuild_sender.go:41` (added at `seaweed_block@85475cd`) | `executor: rebuild start replica=<id> sessionID=<n> epoch=<n> EV=<n> targetLSN=<n>` |
| **COMPLETE** (pre-existing) | `core/transport/rebuild_sender.go:120` (T4d-4 part B / earlier) | `executor: rebuild complete, sent <n> blocks (targetLSN=<n>)` |

Both are deterministic, sessionID-correlatable, and already proven on m01 (G6 hardware run scraped the START marker successfully via `wait_until_rebuild_dispatched`).

**RecoveryLog event names** (`exec_rebuild_started`, `exec_rebuild_completed`) DO exist in the engine's structured RecoveryLog (`Orchestrator.Log` ring buffer), but that buffer is process-internal — operators / harnesses don't see it without an additional `/recovery-log` HTTP surface (G5-3 forward-carry territory; not in G7 scope).

**Recommended helper shape**: `wait_until_rebuild_complete <replica_id> <session_id> <deadline_s>` = poll primary.log for the COMPLETE literal above, **filtered to the matching `sessionID=<n>` from the prior START dispatch** so chained scenarios don't false-positive on a stale rebuild. Pair with existing `wait_until_byte_equal` + `wait_until_peer_healthy` per `INV-G6-HARNESS-DATA-AND-STATE-CONVERGENCE`.

**QA pre-work helper at `V:\share\g5-test\scenarios\g7-helpers.sh`** is already written against these literal strings — corrected §harness-notes is the source-of-truth pin.

---

## Architect ratification block (empty until signed)

| Binding | Decision |
|---|---|
| _(pending)_ | |

---

## §close

**Not yet drafted** — appended after m01 GREEN per `v3-batch-process.md`.
