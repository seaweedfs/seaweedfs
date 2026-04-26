# T4d Forward-Carry Verification Checklist (T4a + T4b + T4c → T4d)

**Date**: 2026-04-26 (v0.3 — T4d batch closed; all per-batch focus rows resolved)
**Status**: ✅ COMPLETE — T4d batch close evidence; per-batch verification done; G5 collective close inherits this checklist as baseline
**Owner**: QA
**Used by**: T4d batch close (this revision); G5 collective close uses this checklist as the regression-baseline list

**Revision history:**
- **v0.1** (2026-04-25 morning) — initial draft from T4c closure §I (6 T4c + 2 T4b + 1 T4a invariants)
- **v0.2** (2026-04-25 afternoon) — aligned with mini-plan v0.2 §6 superset; adds 3 T4a + 4 T4b + 4 T4c invariants I'd missed; corrects "Engine retry-budget escalation (no formal INV id)" to its actual catalogue name `INV-REPL-CATCHUP-RECYCLE-ESCALATES`; risk grades match mini-plan §6 column
- **v0.3** (2026-04-26) — T4d batch close: all per-batch focus rows resolved; m01 -race verified across all T4d batches incl. T2A NVMe race fix at `a0be6d5`; checklist transitions from "active gating" to "G5-baseline" status

---

## Purpose

Per kickoff §4 acceptance criterion #5: *"T4a/T4b/T4c invariants forward-carry verified (no regression)."*

T4d touches three high-risk surfaces: **substrate `ApplyEntry`** (T4d-1), **replica apply gate above the substrate** (T4d-2), and **`StartCatchUp` signature + engine retry wire** (T4d-3). Plus lifecycle close (T4d-4). Each could regress an active T4a/T4b/T4c invariant if the T4d change accidentally bypasses an existing pin point. This checklist is the gate that catches it.

Risk grading mirrors mini-plan v0.2 §6 column: **High** = T4d directly changes the pinning method; **Medium** = T4d changes adjacent code that touches the invariant's enforcement path; **Low** = T4d code does not touch this invariant's enforcement (smoke-test only).

---

## Active T4a invariants (forward-carry)

| Invariant | Pin location at T4a | T4d regression risk | Verification |
|---|---|---|---|
| `INV-REPL-SHIP-TRANSPORT-MUSCLE-001` | T4a-2 Ship transport muscle | **Low** — Ship code path not touched | Smoke via T4a-2 suite under G-3 |
| `INV-REPL-FANOUT-001` | T4a-3 ReplicaPeer fan-out | **Low** | Smoke via T4a-6 BasicEndToEnd |
| `INV-REPL-LSN-ORDER-FANOUT-001` | T4a-4 ReplicationVolume Option-X LSN-order fan-out | **Medium — T4d-3 changes catch-up scan boundary; engine-driven recovery wires through ReplicationVolume** | Re-run T4a-4 fan-out tests; assert LSN-order fan-out across replicas preserved when engine drives recovery (no out-of-order under retry) |
| `INV-REPL-PEER-REBUILD-ON-AUTHORITY-CHANGE` | T4a-4 peer rebuild on authority change | **Low** | T4a-4 test |

---

## Active T4b invariants (forward-carry)

| Invariant | Pin location at T4b | T4d regression risk | Verification |
|---|---|---|---|
| `INV-REPL-LINEAGE-BORNE-ON-BARRIER-ACK` | T4b-1 BarrierResponse 32B lineage echo | **Low — wire untouched** | T4b-1 + T4b-6 wire fence; assert BarrierResponse still carries echoed RecoveryLineage on R+1-aware path |
| `INV-REPL-NO-ZERO-LINEAGE-FIELDS` | T4b-1 strict-decode no-zero | **Low — no new wire surfaces in T4d** | T4b-1 fence still applies |
| `INV-REPL-BARRIER-FAILURE-DEGRADES-PEER` | T4b-3 peer-state on barrier failure | **Low** | T4b-3 + T4b-4 peer-state pin |
| `INV-REPL-LOCAL-FSYNC-GATES-QUORUM` | T4b-4 DurabilityCoordinator | **Low** | T4b-4 |
| `INV-REPL-DURABILITY-COORDINATOR-OWNS-SYNC` | T4b-4 + T4b-5 `ReplicationVolume.Sync` | **Medium — T4d-4 lifecycle close interacts with coordinator state** | Re-run T4b-4 + T4b-5 durability tests during T4d-4 lifecycle work; assert coordinator state cleanup doesn't bypass durability invariants |

---

## Active T4c invariants (forward-carry)

| Invariant | Pin location at T4c | T4d regression risk | Verification |
|---|---|---|---|
| `INV-REPL-LINEAGE-BORNE-ON-PROBE-PAIR` | T4c-1 ProbeReq + ProbeResponse symmetric pair | **Low — probe wire untouched** | T4c-1 fence |
| `INV-REPL-PROBE-NON-MUTATING-VALIDATION` | `core/transport/replica.go:238` `validateProbeLineage` + T4c-1 unit + QA #1 | **Low — probe handler untouched** | T4c-1 test; assert `validateProbeLineage` still called for `MsgProbeReq` (NOT replaced by `acceptMutationLineage`) |
| `INV-REPL-CATCHUP-CALLBACK-RETURN-NIL-CONTINUES` | T4c-2 per-entry target cap | **Medium — T4d-2 apply gate inserted between scan callback and substrate; T4d-3 changes scan boundary** | Forward-carry test in T4d-2 (apply gate forwards nil correctly) + T4d-3 (R+1 path); assert returning nil continues scan |
| `INV-REPL-CATCHUP-LASTSENT-MONOTONIC` | T4c-2 `doCatchUp` lastSent loop + T4c-2 unit + QA #6 | **Medium — T4d-3 R+1 changes start LSN** | Forward-carry test in T4d-3; assert `lastSent` advances only-forward across the new R+1 + retry-loop path |
| `INV-REPL-CATCHUP-DEADLINE-PER-CALL-SCOPE` | T4c-2 per-call deadline `defer SetDeadline(time.Time{})` + T4c-2 unit + QA #5 | **Low — deadline discipline preserved** | T4c-2 test + QA #5 DeadlinePerCallScope_NoSpilling; assert each engine-driven retry sets + clears its own deadline |
| `INV-REPL-CATCHUP-COMPLETION-FROM-BARRIER-ACHIEVED-LSN` | T4c-2 barrier-as-terminator + T4c-2 unit + QA #4 | **Low — completion semantics unchanged** (barrier remains authoritative; T4d-2 coverage map contributes to barrier judgment, doesn't replace) | T4c-2 test + QA #4 BarrierAchievedLSN_PartialProgress + new T4d-2 `TestApplyGate_BarrierCompletion_BarrierAuthoritativeWithCoverageContribution` |
| `INV-REPL-CATCHUP-TARGET-NOT-REACHED-VS-RECYCLED-DISTINGUISHED` | T4c-2 distinguishing failure modes | **Medium — T4d-1 structured-kind switch changes detection** | T4d-1 test must distinguish the two via typed kind (`RecoveryFailureKind`), not substring |
| `INV-REPL-CATCHUP-RECYCLE-ESCALATES` *(corrected from v0.1 "Engine retry-budget escalation; no formal INV id")* | T4c-2 recycle escalation + T4c-3 L2 + QA #2 | **High — T4d-1 changes pinning method from substring match to structured `FailureKind` field** | T4d-1 test chain: substrate wraps as `RecoveryFailure{Kind: WALRecycled}` → sender extracts via `errors.As` → engine branches on typed `e.FailureKind` → escalates `Decision = Rebuild`. Substring `"WAL recycled"` MUST be removed from `core/engine/apply.go` per criterion #7. Fence test `TestEngine_SessionFailed_NoMoreSubstringMatch` greps source. |
| `INV-REPL-PEER-STATE-CATCHINGUP` | T4c-2 peer state machine | **Low** | T4c-2 test |
| `INV-REPL-RECOVERY-MODE-OBSERVABLE` | T4c-2 mode label emit + T4c-2 unit + QA #3 | **Medium — T4d-4 replaces duck-typed `CheckpointLSN` probe with `RecoveryMode()` method** | T4d-4 test + QA #3 ModeLabelObservability; assert success log line still carries `recovery_mode=wal_replay` or `recovery_mode=state_convergence` per substrate after method replacement; closes round-40 wrap-vs-CheckpointLSN limitation |
| Engine retry-budget per-content-kind defaults *(no formal INV id; T4c-3 forward-carry)* | T4c-3 `RecoveryRuntimePolicy.MaxRetries` (3 / 0 / 1) | **Low** | T4c-3 unit test |

---

## Verification protocol (per T4d batch PR)

Each T4d batch PR must run this checklist as part of QA review:

1. **Smoke-test the full active invariant suite**:
   ```bash
   go test -count=1 ./core/replication/... ./core/transport/... ./core/engine/... ./core/storage/...
   ```
   All packages must remain green.

2. **Spot-test the High + Medium rows above** for THIS batch's regression-risk column — re-run the specific named tests, not just package-level smoke. Catch silent semantic drift that compiles + passes generic tests but breaks the specific pin.

3. **Catalogue cross-check**: confirm no `✓ PORTED T4a/T4b/T4c` row in catalogue §3.3 quietly downgrades to `⊙` or `⏭` in the diff. If a T4d change requires downgrade, that's a §B-style scope-delta requiring architect acceptance, not a silent change.

4. **Document deltas**: any forward-carry that fails or weakens MUST appear in T4d batch's PR description as an explicit delta entry (mirroring T4c closure §B's discipline).

---

## Per-batch focus (high+medium risk only — efficient PR review)

**T4d-1 (substrate hardening + structured kind)** — verify these specifically:
- High: `INV-REPL-CATCHUP-RECYCLE-ESCALATES` (substring → typed kind switch) — ✅ landed `1edeb36` with `TestEngine_SessionFailed_NoMoreSubstringMatch` fence + comment-stripping helper
- Medium: `INV-REPL-CATCHUP-TARGET-NOT-REACHED-VS-RECYCLED-DISTINGUISHED` (typed kind detection)
   - **REQUIRED FOLLOW-UP before T4d-3 G-1 sign** (architect-bound 2026-04-25):
   - `TestT4d1_TargetNotReached_DistinctKindFromWALRecycled` — asserts `classifyRecoveryFailure(...)` maps target-not-reached error to `engine.RecoveryFailureTargetNotReached` AND maps WAL recycled to `engine.RecoveryFailureWALRecycled` (distinct values)
   - `TestT4d1_StorageFailureKindMapper_AllKnownKinds` — fence: enumerate all `storage.StorageRecoveryFailureKind` values; assert `classifyRecoveryFailure` maps each to a non-`Unknown` `engine.RecoveryFailureKind`. Catches future storage enum additions not mapped in transport.
   - Where: either small T4d-1 follow-up patch OR rolled into T4d-2's first commit — sw discretion
   - Hard gate: T4d-3 G-1 sign requires both tests green

**T4d-2 (replica apply gate)** — verify these specifically:
- Medium: `INV-REPL-CATCHUP-CALLBACK-RETURN-NIL-CONTINUES` (apply gate inserted between callback and substrate) — ✅ landed `bd2de99`
- Low (but high-impact regression if broken): `INV-REPL-CATCHUP-COMPLETION-FROM-BARRIER-ACHIEVED-LSN` (barrier remains authoritative; coverage feeds judgment) — ✅ forward-carry green
- **REQUIRED FOLLOW-UP — HARD GATE before T4d-3** (architect-bound 2026-04-25 round-46 review):
   - **Lane discriminator rework**: T4d-2's `bd2de99` implementation uses `lineage.TargetLSN > liveShipTargetLSN(=1)` as discriminator — payload-derived, not handler-context-derived. Architect: "I would not solve it by adding an engine precondition TargetLSN >= 2; that changes recovery semantics to protect an implementation shortcut. Choose Option 2: rework to true handler-context lane."
   - Concrete direction:
     - Change apply gate hook from single `Apply(lineage, ...)` to lane-explicit: either `ApplyLive(...)` + `ApplyRecovery(...)` separate methods OR `Apply(..., lane ApplyLane)` with explicit param
     - Caller must supply `lane` from connection/session handler context — NOT inferred from `lineage.TargetLSN`
     - Keep no wire byte (Q2 still stands)
     - Add regression test `TestApplyGate_RecoveryWithTargetLSN1_RoutesToRecoveryLane` — pins the edge case (recovery to H=1 must NOT misroute to live lane)
     - Rename/remove existing T4d-2 tests with "lane derived from TargetLSN" phrasing — that mental model must go away from the codebase
   - Where: T4d-2 follow-up patch BEFORE T4d-3 starts
   - Hard gate: T4d-3 cannot start until the rework lands (architect: "T4d-3 will thread real TargetLSN/FromLSN and could make this edge case less theoretical. T4d-3 should not build more wiring on a payload-sentinel lane rule.")
   - Rationale: `INV-REPL-LANE-DERIVED-FROM-HANDLER-CONTEXT` catalogue inscription says "accepting handler/session context" — sw's implementation drifted from the inscription text; the rework realigns implementation with the architect-locked invariant statement

**T4d-3 (R+1 + engine-driven recovery wiring)** — verify these specifically:
- Medium: `INV-REPL-LSN-ORDER-FANOUT-001` (engine drives recovery through ReplicationVolume)
- Medium: `INV-REPL-CATCHUP-CALLBACK-RETURN-NIL-CONTINUES` (R+1 scan boundary)
- Medium: `INV-REPL-CATCHUP-LASTSENT-MONOTONIC` (R+1 changes start LSN)
- **HARD CLOSE CONDITION** (architect-bound 2026-04-25 round-46+): the caller-side `TargetLSN==1` lane-discrimination shim in `core/transport/replica.go:13-18,156-165` (introduced T4d-2 follow-up `01f4ab9` as TRANSITIONAL) **cannot survive as an unbounded known bug into T4d-3 close**. T4d-3 must do EXACTLY ONE of:
   - **(option A)** Land true handler/session-context lane signal that REMOVES the TargetLSN==1 shim entirely (per-conn lane tag at handshake / separate handlers / distinct ports — sw picks)
   - **(option B)** Add an explicit failing-or-skip-marked L2 test `TestT4d3_RecoveryTargetLSN1_KnownGap` that documents the H=1 edge case as a known gap, with godoc citing `CARRY-T4D-LANE-CONTEXT-001`. This test MUST exercise the H=1 scenario (primary writes 1 entry, replica empty, catch-up emitted with TargetLSN=1) and either fail-loud or skip with explicit reference to the carry id.
   - Architect explicit prohibition: do NOT add engine precondition `TargetLSN >= 2` ("changes recovery semantics to protect an implementation shortcut")
- **PRE/WITH-T4d-3 doc fixes** (architect-bound 2026-04-25):
   - `core/replication/apply_gate.go:38-43` — type-level godoc still describes OLD payload-derived discrimination ("the gate reads the lineage's TargetLSN signal"). Rewrite to reflect lane-pure model with caller-supplied lane.
   - `core/transport/replica.go` — `NewReplicaListener*` constructor godoc that still references `hook.Apply(...)` / "lane discrimination" wording must update to reflect lane-explicit `ApplyRecovery` / `ApplyLive` hook shape.
   - Where: T4d-3 first commit OR small standalone PR before T4d-3
- **CARRY-T4D-LANE-CONTEXT-001** (named carry inscribed in catalogue §3.3 + below): replace TargetLSN==1 caller shim with true handler/session context lane signal. Owner: sw. Bind point: T4e (preferred) or post-G5 protocol-hardening. T4d-3 close evidence MUST cite this carry id if option B taken.

**T4d-4 (lifecycle + integration)** — verify these specifically:
- Medium: `INV-REPL-DURABILITY-COORDINATOR-OWNS-SYNC` (lifecycle close interacts with coordinator)
- Medium: `INV-REPL-RECOVERY-MODE-OBSERVABLE` (`RecoveryMode()` method replaces duck-typed probe)

---

## Closure evidence at T4d batch close

QA single-sign at T4d close per §8C.2 includes a §-formatted block in the closure report:

```
| Carry-forward | Status | Evidence |
|---|---|---|
| INV-REPL-PROBE-NON-MUTATING-VALIDATION | ✅ green | T4c-1 + QA #1 re-run at HEAD <sha> |
| INV-REPL-CATCHUP-RECYCLE-ESCALATES | ✅ green; pinning method changed substring→typed kind | T4d-1 chain re-run; fence TestEngine_SessionFailed_NoMoreSubstringMatch green |
| INV-REPL-LSN-ORDER-FANOUT-001 | ✅ green; engine-driven retry preserves order | T4a-4 + T4d-3 retry path |
| INV-REPL-DURABILITY-COORDINATOR-OWNS-SYNC | ✅ green; lifecycle cleanup doesn't bypass | T4b-4 + T4b-5 + T4d-4 lifecycle |
| INV-REPL-RECOVERY-MODE-OBSERVABLE | ✅ green; method replaces duck-typed probe; wraps forward correctly | T4c-2 + QA #3 + T4d-4 RecoveryMode method tests |
| ... (all 19 active T4a/T4b/T4c invariants) | ... | ... |
```

Filling this in is QA's last action before signing the T4d closure report.
