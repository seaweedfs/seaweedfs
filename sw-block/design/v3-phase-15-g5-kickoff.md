# G5 — Replicated Write Path Collective Close — Kick-Off (PROPOSAL)

**Date**: 2026-04-26 (v0.1 — QA-authored proposal post-T4 close)
**Status**: ⏸ DRAFT — awaiting architect ratification on §2 scope + §3 batch shape + §4 acceptance bar + §5 G5-DECISION-001 path choice
**Predicates met**: T4 batch series closed (T4a + T4b + T4c + T4d batches all signed; T4 T-end three-sign at T4d closure report `seaweedfs@2ee12b2c1`)
**Reference**: `v3-phase-15-t4d-closure-report.md` §I forward-carries; `v3-phase-15-t4d-mini-plan.md` §5 G5-DECISION-001 named decision record; `v3-phase-15-t4c-closure-report.md` §H m01 deferral

---

## §1 What G5 closes

G5 is the **collective close of the replicated write path** across all T4 batches (T4a + T4b + T4c + T4d). Per the T4d kickoff §2.6 ratification, several production-readiness items were intentionally deferred from per-batch closes to G5 collective sign:

- m01 hardware first-light for replicated write path (deferred from every T4 batch)
- Multi-replica concurrent live + recovery scenarios
- `G5-DECISION-001` resolution (engine recovery state across primary restart)
- walstore flusher cadence verification + tuning policy
- Minimal metrics/backpressure assessment

G5 closes when these collectively pass + the architect signs off on production-readiness for the replicated write path.

**G5 is NOT**: a re-test of T4 batches; a scope expansion adding new features; a substitute for post-G5 hardening backlog work.

---

## §2 Scope (per T4d closure §I + closure §H findings) — architect ratification ask

### §2.1 Inherited from T4d closure (G5 collective items)

| Item | Source | Owner |
|---|---|---|
| **m01 hardware first-light for replicated write path** | T4c §H + T4d kickoff §2.6 | QA + sw |
| **Multi-replica concurrent live + recovery scenarios** | T4d kickoff scope; production-readiness gap | QA |
| **`G5-DECISION-001` resolution** (Path A persist vs Path B rebuild-from-probe) | T4d-4 part B round-47 | architect |
| **walstore flusher/checkpoint cadence verification under production write pressure + tuning/operational policy** | T4d closure §H Finding #1 (round-48 reworded) | sw + architect |
| **Minimal metrics/backpressure assessment** | T4d kickoff scope (production-readiness items) | sw |

### §2.2 Explicitly NOT in G5 (post-G5 hardening backlog)

| Item | Source |
|---|---|
| `CARRY-T4D-LANE-CONTEXT-001` — replace transport caller-side TargetLSN==1 shim with true handler/session-context lane signal | T4d-2 follow-up; bound to post-G5 hardening backlog at T4d round-48 |
| `--durable-walsize` CLI flag for operator tuning | T4d closure §H Finding #2 |
| Snapshot-based catch-up for far-behind replicas | not yet in any batch |
| Wire protocol versioning evolution (SWRP+v2) | not yet in any batch |
| Auth / encryption / mTLS for replication wire | not yet in any batch |

**Rationale**: G5 closes the **replicated write path correctness + production confidence** at MVP scope. Items above are hardening / operator-tuning / future-feature work. Mixing them into G5 would conflate "is V3 P15 replication correct + production-confident" with "is V3 P15 hardened for all production scenarios" — different gates.

**Architect decision asks for §2:**
1. Accept the §2.1 / §2.2 boundary as correct G5 scope?
2. Confirm any §2.2 item should promote to §2.1 (G5 must-have)?
3. Confirm m01 first-light scope at G5 — what specific scenarios?

---

## §3 Proposed batch shape — architect ratification ask

QA proposal for G5 batch decomposition (smaller than T4 batches since G5 is integration / verification heavy, not new-feature heavy):

| Batch | Scope | LOC est | Owner |
|---|---|---|---|
| **G5-1** Multi-replica concurrent live + recovery scenarios at component scope | Author the QA scenarios catalogued at T4d (#1–#9) as actual test code; add multi-replica RF=3 mixed-state scenarios; component framework primitives needed (`AssertNoPerLBARegression`, `AssertLaneIntegrity`, `RestartReplica`, etc. from T4d QA scenario catalogue §7) | ~50 prod (framework primitives) + ~600 tests (9 scenarios + RF=3 mixed-state + new framework primitives) | QA + sw (framework only) |
| **G5-2** walstore flusher cadence verification + tuning policy | Production write pressure smoke (sustained 10+ minute write load); identify flusher cadence under pressure; document operational tuning policy (knobs to expose? defaults? what operator should monitor?) | ~80 prod (instrumentation if needed) + ~150 tests (sustained-write + observability) | sw + architect |
| **G5-3** Minimal metrics/backpressure assessment | Document current observability surface (logs, status endpoint, etc.); identify minimal Prometheus-style metrics needed for production (catch-up progress, retry counters, error rates per kind, session duration); decide what's G5 must-have vs post-G5 | ~50 prod (metrics) + ~50 tests (assertion fences) | sw + architect |
| **G5-4** m01 hardware first-light + L3 integration | Author `iterate-m01-replicated-write.sh` script (mirroring `iterate-m01-nvme.sh` from T2); drive real 2-node primary↔replica via blockmaster + assignment; iptables-based mid-stream disconnect; verify catch-up + rebuild paths under real network | ~200 (bash script + Go test driver) | QA + sw |
| **G5-5** G5-DECISION-001 resolution + closure | Architect resolves Path A (persist Recovery state) vs Path B (rebuild from probe after restart); IF Path A: persistence implementation + tests; IF Path B: documentation of retry-budget-resets-on-restart semantic + operator awareness; G5 closure report drafted | varies by path | architect + sw + QA |

**Architect decision asks for §3:**
1. Confirm 5-batch shape vs alternatives (3-batch dense / 7-batch with mixed-state and rebuild-edge as separate)?
2. Confirm batch ordering — component-multi-replica → walstore-cadence → metrics → m01 → G5-DECISION-001 close?
3. Confirm G5-5 ordering — does G5-DECISION-001 resolution happen at the START of G5 (architect call upfront) or at G5 CLOSE (after evidence from G5-1/2/3/4)?

QA recommendation: G5-5 at close, because evidence from G5-1/2/3/4 (especially walstore cadence + production write pressure findings) helps inform Path A vs Path B decision.

---

## §4 Acceptance bar — proposal

G5 closes when ALL of (proposed; awaiting ratification):

1. ✅ All 5 G5 batches merged to `phase-15`
2. ✅ Unit tests + L2 integration tests green at HEAD
3. ✅ **m01 hardware L3 first-light green** — replicated write path under real network conditions; multi-replica + disconnect + reconnect + byte-exact convergence
4. ✅ **Multi-replica RF=3 mixed-state scenarios green** — A live + B catch-up + C rebuild concurrently; engine drives all without cross-lane contamination
5. ✅ All 19 active T4a/T4b/T4c invariants + 16 T4d invariants forward-carry verified (no regression under any G5 batch)
6. ✅ **`G5-DECISION-001` explicitly resolved** — architect signs Path A or Path B; if Path A, persistence implementation lands; if Path B, documentation + operator-awareness
7. ✅ **walstore flusher cadence verified under production write pressure** — tuning policy documented; if knobs need exposure, decision recorded
8. ✅ **Minimal metrics defined** — Prometheus-style metric names + cardinalities documented; G5 must-have subset implemented
9. ✅ G5 collective sign — architect + QA + PM (per §8C.1 G5-end three-sign)

**Architect decision asks for §4:**
1. Accept criterion #3 m01 scope — what specific scenarios are required for sign? (suggest: `ShortDisconnect_DeltaCatchUp` + `LongDisconnect_RebuildEscalation` + `MidStreamDisconnect_Recovery` + `SteadyStateLiveShip_Throughput` × {smartwal, walstore} = 8 scenarios)
2. Accept criterion #4 multi-replica scope — RF=3 minimum, or RF=5 stretch?
3. Accept criterion #6 G5-DECISION-001 binding — is "documented + operator-aware" sufficient for Path B, or does Path B also require a runtime test asserting retry-budget-reset behavior?

---

## §5 `G5-DECISION-001` — open architect decision (must resolve before G5 close)

**Question** (per T4d-4 part B round-47): when the primary process restarts mid-recovery, what happens to in-flight Recovery state (`Attempts` counter, `TargetLSN`, `ActiveSessionID`, retry budget consumed)?

**Two paths** (G5 must explicitly choose one):

| Path | Behavior | Pros | Cons | Implementation cost |
|---|---|---|---|---|
| **A — persist Recovery state** | Engine state durably stored (substrate? separate file? blockmaster?); restart resumes from persisted state | Retry budget honored across restart; in-flight session can resume; operator gets accurate "this replica failed N times" view | Substrate work; restart-during-persist atomicity; new failure mode if persistence fails; storage layer to be picked | Substantial — needs persistence layer choice + atomicity guarantees + tests |
| **B — rebuild state from probe after restart** | Engine starts cold; probes replicas; reconstructs `Recovery.R` from probe responses; resets `Attempts` to 0 | Simpler; no new substrate; structurally easy (T4d-4 part B already kept ReplicaState serializable via G5-DECISION-001 obligation) | Retry budget effectively unbounded under frequent restarts; operator confusion: "this replica has been retried N times" view is reset every restart | Minimal — already structurally compatible at T4d-4 part B; documentation + operator-awareness only |

**T4d-4 obligation (already satisfied)**: engine `ReplicaState` struct is JSON round-trip clean per `TestG5Decision001_ReplicaState_RoundTripJSON` — Path A remains structurally open without rewrite.

**QA recommendation: Path B for V3 P15 MVP.** Rationale:
- T4d-4 part B already structurally enables Path B (no implementation needed)
- Production primary restarts should be rare events (not frequent enough to make retry-budget reset a serious concern)
- Path A's persistence work (substrate choice + atomicity) is genuinely substantial and risks delaying G5
- Operator awareness via documentation + runtime metric exposure (G5-3) gives sufficient visibility
- If production usage proves Path B insufficient, Path A is a backwards-compatible upgrade later

**Architect call needed.** Either path is acceptable; the cost/benefit favors B for MVP scope.

---

## §6 Forward-carry from T4 close

Per T4d closure §I (post-T4 active carries):

| Carry | Owner | G5 status |
|---|---|---|
| `CARRY-T4D-LANE-CONTEXT-001` — true handler/session-context lane signal | sw | NOT in G5; post-G5 hardening backlog (per T4d round-48 architect ruling) |
| `G5-DECISION-001` — engine recovery state across primary restart | architect at G5 | IN G5 (G5-5 batch resolves it) |
| walstore flusher cadence verification + tuning policy | sw + architect | IN G5 (G5-2 batch) |
| `--durable-walsize` CLI flag | sw | NOT in G5; post-G5 / operator hardening |
| m01 hardware first-light for replicated write path | QA + sw | IN G5 (G5-4 batch) |
| Multi-replica concurrent live + recovery scenarios | QA | IN G5 (G5-1 batch) |

---

## §7 Status — awaiting architect ratification on §2 + §3 + §4 + §5

**No G5 code work begins until:**
- §2 scope ratified
- §3 batch shape + ordering ratified
- §4 acceptance bar ratified
- §5 G5-DECISION-001 path choice deferred to G5-5 close OR resolved upfront (architect call)

**Once ratified, sw + QA produce:**
1. `v3-phase-15-g5-mini-plan.md` — task list per ratified batch shape
2. QA scenario catalogue v0.2 — adds RF=3 multi-replica scenarios on top of T4d catalogue's 9 scenarios
3. `iterate-m01-replicated-write.sh` script draft (G5-4)
4. Forward-carry verification list — 35 active T4a/T4b/T4c/T4d invariants must continue green under all G5 batches

QA reviews mini-plan + scenario catalogue + m01 script; sw reviews framework-primitive PRs; architect resolves G5-DECISION-001 + walstore tuning policy at G5-5 close.

---

## §8 Open issues blocking ratification

1. **Architect resolution of §2** — accept §2.1/§2.2 scope boundary
2. **Architect resolution of §3** — 5-batch shape + ordering (especially G5-5 timing)
3. **Architect resolution of §4** — m01 scope, multi-replica RF level, G5-DECISION-001 binding
4. **Architect resolution of §5 (or defer to G5-5 close)** — Path A vs Path B; QA recommends Path B for MVP
5. **Architect call on T4 T-end three-sign** — closure report at `seaweedfs@2ee12b2c1` is QA-signed; awaiting architect + PM signs to formally close T4

**Resolved during v0.1 drafting** (no longer blocking):
- ~~T4d batch close itself~~ — closure report committed at `seaweedfs@2ee12b2c1`; round-48 + round-49 corrections incorporated
- ~~T2A NVMe race~~ — resolved at `seaweed_block@a0be6d5`; m01 -race ×50 PASS

---

## §9 Change log

| Date | Change | Author |
|---|---|---|
| 2026-04-26 | Initial G5 kickoff PROPOSAL v0.1 drafted from T4d closure §I forward-carries + closure §H findings + T4d-4 part B G5-DECISION-001. 5-batch shape proposed (G5-1 multi-replica scenarios; G5-2 walstore cadence; G5-3 metrics; G5-4 m01 first-light; G5-5 G5-DECISION-001 close + closure report). QA recommendations: Path B for G5-DECISION-001 (cost/benefit favors B for MVP); G5-5 timing at close (so G5-1/2/3/4 evidence informs decision); §2.2 explicit non-claims to prevent G5 scope creep. Awaiting architect ratification on §2 + §3 + §4 + §5. | QA |
