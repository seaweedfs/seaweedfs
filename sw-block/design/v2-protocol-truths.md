# V2 Protocol Truths

Date: 2026-03-30
Status: active
Purpose: record the compact, stable truths that later phases must preserve, and provide a conformance reference for implementation reviews

## Why This Document Exists

`FSM`, `simulator`, `prototype`, and `engine` are not a code-production pipeline.
They are an evidence ladder.

So the most important output to carry forward is not only code, but:

1. accepted semantics
2. must-hold boundaries
3. failure classes that must stay closed
4. explicit places where later phases may improve or drift

This document is the compact truth table for the V2 line.

Current chosen-envelope claims, accepted baselines, evidence mappings, and
evidence invalidations are tracked separately in
`v2-protocol-claim-and-evidence.md`.

## How To Use It

For each later phase or slice, ask:

1. does the new implementation remain aligned with these truths?
2. if not, is the deviation constructive or risky?
3. which truth is newly strengthened by this phase?

Deviation labels:

- `Aligned`: implementation preserves the truth
- `Constructive deviation`: implementation changes shape but strengthens the truth
- `Risky deviation`: implementation weakens or blurs the truth

## Core Truths

### T1. `CommittedLSN` is the external truth boundary

Short form:

- external promises are anchored at `CommittedLSN`, not `HeadLSN`

Meaning:

- recovery targets
- promotion safety
- flush/visibility reasoning

must all be phrased against `CommittedLSN`.

Prevents:

- using optimistic WAL head as committed truth
- acknowledging lineage that failover cannot preserve

Evidence anchor:

- strong in design
- strong in simulator
- strong in prototype
- strong in engine

### T2. `ZeroGap <=> ReplicaFlushedLSN == CommittedLSN`

Short form:

- zero-gap requires exact equality with committed truth

Meaning:

- replica ahead is not zero-gap
- replica behind is not zero-gap

Prevents:

- unsafe fast-path completion
- replica-ahead being mistaken for in-sync

Evidence anchor:

- strong in prototype
- strong in engine

### T3. `CatchUp` is bounded replay on a still-trusted base

Short form:

- `CatchUp = KeepUp with bounded debt`

Meaning:

- catch-up is a short-gap, low-cost, bounded replay path
- it only makes sense while the replica base is still trustworthy enough to continue from

Prevents:

- turning catch-up into indefinite moving-head chase
- hiding broad recovery complexity in replay logic

Evidence anchor:

- strong in design
- strong in simulator
- strong in prototype
- strong in engine

### T4. `NeedsRebuild` is explicit when replay is not the right answer

Short form:

- `NeedsRebuild <=> replay is unrecoverable, unstable, or no longer worth bounded replay`

Meaning:

- long-gap
- lost recoverability
- no trusted base
- budget violation

must escalate explicitly.

Prevents:

- pretending catch-up will eventually succeed
- carrying V1/V1.5-style unbounded degraded chase forward

Evidence anchor:

- strong in simulator
- strong in prototype
- strong in engine

### T5. `Rebuild` is the formal primary recovery path

Short form:

- `Rebuild = frozen TargetLSN + trusted base + optional tail + barrier`

Meaning:

- rebuild is not a shameful fallback
- it is the general recovery framework

Prevents:

- overloading catch-up with broad recovery semantics
- treating full/partial rebuild as unrelated protocols

Evidence anchor:

- strong in design
- strong in prototype
- strong in engine

### T6. Full and partial rebuild share one correctness contract

Short form:

- `full rebuild` and `partial rebuild` differ in transfer choice, not in truth model

Meaning:

- both require frozen `TargetLSN`
- both require trusted pinned base
- both require explicit durable completion

Prevents:

- optimization layers redefining protocol truth
- bitmap/range paths bypassing trusted-base rules

Evidence anchor:

- strong in design
- partial in engine
- stronger real-system proof still deferred

### T7. No recovery result may outlive its authority

Short form:

- `ValidMutation <=> sender exists && sessionID matches && epoch current && endpoint current`

Meaning:

- stale session
- stale epoch
- stale endpoint
- stale sender

must all fail closed.

Prevents:

- late results mutating current lineage
- changed-address stale completion bugs

Evidence anchor:

- strong in simulator
- strong in prototype
- strong in engine

### T8. `ReplicaID` is stable identity; `Endpoint` is mutable location

Short form:

- `ReplicaID != address`

Meaning:

- address changes may invalidate sessions
- address changes must not destroy sender identity

Prevents:

- reintroducing address-shaped identity
- changed-address restarting as logical removal + add

Evidence anchor:

- strong in prototype
- strong in engine
- strong in bridge P0

### T9. Truncation is a protocol boundary, not cleanup

Short form:

- replica-ahead cannot complete until divergent tail is explicitly truncated

Meaning:

- truncation is part of recovery contract
- not a side-effect or best-effort cleanup

Prevents:

- completing recovery while replica still contains newer divergent writes

Evidence anchor:

- strong in design
- strong in engine

### T10. Recoverability must be proven from real retained history

Short form:

- `CatchUp allowed <=> required replay range is recoverable from retained history`

Meaning:

- the engine should consume storage truth
- not test-reconstructed optimism

Prevents:

- replay on missing WAL
- fake recoverability based only on watermarks

Evidence anchor:

- strong in simulator
- strong in engine
- strengthened in driver/adapter phases

### T11. Trusted-base choice must be explicit and causal

Short form:

- `snapshot_tail` requires both trusted checkpoint and replayable tail

Meaning:

- snapshot existence alone is insufficient
- fallback to full-base must be explainable

Prevents:

- over-trusting old checkpoints
- silently choosing an invalid rebuild source

Evidence anchor:

- strong in simulator
- strong in engine
- strengthened by Phase 06

### T12. Current extent cannot fake old history

Short form:

- historical correctness requires reconstructable history, not current-state approximation

Meaning:

- live extent state is not sufficient proof of an old target point
- historical reconstruction must be justified by checkpoint + retained history

Prevents:

- using current extent as fake proof of older state

Evidence anchor:

- strongest in simulator
- engine currently proves prerequisites, not full reconstruction proof

### T13. Promotion requires recoverable committed prefix

Short form:

- promoted replica must be able to recover committed truth, not merely advertise a high watermark

Meaning:

- candidate selection is about recoverable lineage, not optimistic flush visibility

Prevents:

- promoting a replica that cannot reconstruct committed prefix after crash/restart

Evidence anchor:

- strong in simulator
- partially carried into engine semantics
- real-system validation still needed

### T14. `blockvol` executes I/O; engine owns recovery policy

Short form:

- adapters may translate engine decisions into concrete work
- they must not silently re-decide recovery classification or source choice

Meaning:

- master remains control authority
- engine remains recovery authority
- storage remains truth source

Prevents:

- V1/V1.5 policy leakage back into service glue

Evidence anchor:

- strong in Phase 07 service-slice planning
- initial bridge P0 aligns
- real-system proof still pending

### T15. Reuse reality, not inherited semantics

Short form:

- V2 may reuse existing Seaweed control/runtime/storage paths
- it must not inherit old semantics as protocol truth

Meaning:

- reuse existing heartbeat, assignment, `blockvol`, receiver, shipper, retention, and runtime machinery when useful
- keep `ReplicaID`, epoch authority, recovery classification, committed truth, and rebuild boundaries anchored in accepted V2 semantics

Prevents:

- V1/V1.5 structure silently redefining V2 behavior
- convenience reuse turning old runtime assumptions into new protocol truth

Evidence anchor:

- strong in Phase 07/08 direction
- should remain active in later implementation phases

### T16. Full-base rebuild completes against an explicit achieved boundary

Short form:

- `full_base` rebuild requires `AchievedLSN >= TargetLSN`
- engine and local runtime must converge to the same achieved boundary

Meaning:

- the engine plans a frozen minimum target `TargetLSN`
- the backend may produce an actual rebuilt boundary `AchievedLSN`
- exact-target extent equality is not required on a mutable-extent backend
- after install, `checkpoint`, `nextLSN`, receiver progress, flusher checkpoint, and engine-visible rebuild completion must all align to the same `AchievedLSN`

Prevents:

- local runtime truth advancing beyond engine/accounting truth
- rebuild completion at one boundary while storage/runtime state reflects another
- treating "at least target" as safe without making the newer achieved boundary explicit

Evidence anchor:

- strengthened by `Phase 09` backend execution closure work
- phase-level decision exists
- real-system proof still depends on executor/runtime alignment

### T17. Extent/WAL recovery split must be fixed before replay begins

Short form:

- `extent copy + WAL replay` is only correct if the split boundary is explicit and gap-free

Meaning:

- extent data must represent a known recovery boundary
- WAL replay must start from the matching next boundary
- if unflushed writes could otherwise fall between extent and replay, the backend must first fix the split boundary
- snapshot/CoW/bitmap export paths follow the same rule: transport must read from a stable recovery view, not a mutating mixed state

Prevents:

- missing writes between extent copy and replay
- point-in-time export that mixes two different recovery views
- rebuild correctness depending on timing accidents instead of an explicit boundary contract

Evidence anchor:

- present in rebuild correctness reasoning from V1
- strengthened by `Phase 09` full-base execution work
- future snapshot/bitmap-based paths must preserve it explicitly

## Current Strongest Evidence By Layer

| Layer | Main value |
|------|------------|
| `FSM` / design | define truth and non-goals |
| simulator | prove protocol truth and failure-class closure cheaply |
| prototype | prove implementation-shape and authority semantics cheaply |
| engine | prove the accepted contracts survive real implementation structure |
| service slice / runner | prove truth survives real control/storage/system reality |

## Phase Conformance Notes

### Phase 04

- `Aligned`: T7, T8
- strengthened sender/session ownership and stale rejection

### Phase 4.5

- `Aligned`: T3, T4, T5, T10, T12
- major tightening:
  - bounded catch-up
  - first-class rebuild
  - crash-consistency and recoverability proof style

### Phase 05

- `Aligned`: T1, T2, T3, T4, T5, T7, T8, T9, T10, T11
- engine core slices closed:
  - ownership
  - execution
  - recoverability gating
  - orchestrated entry path

### Phase 06

- `Aligned`: T10, T11, T14
- `Constructive deviation`: planner/executor split replaced convenience wrappers without changing protocol truth
- strengthened:
  - real storage/resource contracts
  - explicit release symmetry
  - failure-class validation against engine path

### Phase 07 P0

- `Aligned`: T8, T10, T14
- bridge now makes stable `ReplicaID` explicit at service boundary
- bridge states the hard rule that engine decides policy and `blockvol` executes I/O
- real `weed/storage/blockvol/` integration still pending

## Current Carry-Forward Truths For Later Phases

Later phases must not regress these:

1. `CommittedLSN` remains the external truth boundary
2. `CatchUp` stays narrow and bounded
3. `Rebuild` remains the formal primary recovery path
4. stale authority must fail closed
5. stable identity must remain separate from mutable endpoint
6. trusted-base choice must remain explicit and causal
7. service glue must not silently re-decide recovery policy
8. reuse reality, but do not inherit old semantics as V2 truth
9. full-base rebuild must converge to one explicit achieved boundary
10. extent/WAL recovery split must be fixed before replay

## Review Rule

Every later phase or slice should explicitly answer:

1. which truths are exercised?
2. which truths are strengthened?
3. does this phase introduce any constructive or risky deviation?
4. which evidence layer now carries the truth most strongly?

## Phase Alignment Rule

From `Phase 05` onward, every phase or slice should align explicitly against this document.

Minimum phase-alignment questions:

1. which truths are in scope?
2. which truths are strengthened?
3. which truths are merely carried forward?
4. does the phase introduce any constructive deviation?
5. does the phase introduce any risky deviation?
6. which evidence layer currently carries each in-scope truth most strongly?

Expected output shape for each later phase:

- `In-scope truths`
- `Strengthened truths`
- `Carry-forward truths`
- `Constructive deviations`
- `Risky deviations`
- `Evidence shift`

## Phase 05-07 Alignment

### Phase 05

Primary alignment focus:

- T1 `CommittedLSN` as external truth boundary
- T2 zero-gap exactness
- T3 bounded `CatchUp`
- T4 explicit `NeedsRebuild`
- T5/T6 rebuild correctness contract
- T7 stale authority must fail closed
- T8 stable `ReplicaID`
- T9 truncation as protocol boundary
- T10/T11 recoverability and trusted-base gating

Main strengthening:

- engine core adopted accepted protocol truths as real implementation structure

Main review risk:

- engine structure accidentally collapsing back to address identity or unfenced execution

### Phase 06

Primary alignment focus:

- T10 recoverability from real retained history
- T11 trusted-base choice remains explicit and causal
- T14 engine owns policy, adapters carry truth and execution contracts

Main strengthening:

- planner/executor/resource contracts
- fail-closed cleanup symmetry
- cross-layer proof path through engine execution

Main review risk:

- executor or adapters recomputing policy from convenience inputs
- storage/resource contracts becoming approximate instead of real

### Phase 07+

Primary alignment focus:

- T8 stable identity at the real service boundary
- T10 real storage truth into engine decisions
- T11 trusted-base proof remains explicit through service glue
- T14 `blockvol` executes I/O but does not own recovery policy
- T16 full-base rebuild converges to one explicit achieved boundary
- T17 extent/WAL split boundary remains explicit and gap-free

Main strengthening:

- real-system service-slice conformance
- real control-plane and storage-plane integration
- diagnosable failure replay through the integrated path

Main review risk:

- V1/V1.5 semantics leaking back in through service glue
- address-shaped identity reappearing at the boundary
- blockvol-side code silently re-deciding recovery policy

## Future Feature Rule

When a later feature expands the protocol surface (for example `SmartWAL` or a new rebuild optimization), the order should be:

1. `FSM / design`
- define the new semantics and non-goals

2. `Truth update`
- either attach the feature to an existing truth
- or add a new protocol truth if the feature creates a new long-lived invariant

3. `Phase alignment`
- define which later phases strengthen or validate that truth

4. `Evidence ladder`
- simulator, prototype, engine, service slice as needed

Do not start feature implementation by editing engine or service glue first and only later trying to explain what truth changed.

## Feature Review Rule

For any future feature, later reviews should ask:

1. did the feature create a new truth or just strengthen an existing one?
2. which phase first validates it?
3. which evidence layer proves it most strongly today?
4. does the feature weaken any existing truth?

This keeps feature growth aligned with protocol truth instead of letting implementation convenience define semantics.
