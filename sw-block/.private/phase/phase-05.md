# Phase 05

Date: 2026-03-29
Status: complete
Purpose: begin the real V2 engine track under `sw-block/` by moving from prototype proof to the first engine slice

## Why This Phase Exists

The project has now completed:

1. V2 design/FSM closure
2. V2 protocol/simulator validation
3. Phase 04 prototype closure
4. Phase 4.5 evidence hardening

So the next step is no longer:

- extend prototype breadth

The next step is:

- start disciplined real V2 engine work

## Phase Goal

Start the real V2 engine line under `sw-block/` with:

1. explicit engine module location
2. Slice 1 ownership-core boundaries
3. first engine ownership-core implementation
4. engine-side validation tied back to accepted prototype invariants

## Relationship To Previous Phases

`Phase 05` is built on:

- `sw-block/design/v2-engine-readiness-review.md`
- `sw-block/design/v2-engine-slicing-plan.md`
- `sw-block/.private/phase/phase-04.md`
- `sw-block/.private/phase/phase-4.5.md`

This is a new implementation phase.

It is not:

1. more prototype expansion
2. V1 integration
3. backend redesign

## Scope

### In scope

1. choose real V2 engine module location under `sw-block/`
2. define Slice 1 file/module boundaries
3. write short engine ownership-core spec
4. start Slice 1 implementation:
   - stable per-replica sender object
   - stable recovery-session object
   - session identity fencing
   - endpoint / epoch invalidation
   - ownership registry / sender-group equivalent
5. add focused engine-side ownership/fencing tests

### Out of scope

1. Smart WAL expansion
2. full storage/backend redesign
3. full rebuild-source decision logic
4. V1 production integration
5. performance work
6. full product integration

## Planned Slices

### P0: Engine Planning Setup

1. choose real V2 engine module location under `sw-block/`
2. define Slice 1 file/module boundaries
3. write ownership-core spec
4. map 3-5 acceptance scenarios to Slice 1 expectations

Status:

- accepted
- engine module location chosen: `sw-block/engine/replication/`
- Slice 1 boundaries are explicit enough to start implementation

### P1: Slice 1 Ownership Core

1. implement stable per-replica sender object
2. implement stable recovery-session object
3. implement sender/session identity fencing
4. implement endpoint / epoch invalidation
5. implement ownership registry

Status:

- accepted
- stable `ReplicaID` is now explicit and separate from mutable `Endpoint`
- engine registry is keyed by stable identity, not address-shaped strings
- real changed-`DataAddr` preservation is covered by test

### P2: Slice 1 Validation

1. engine-side tests for ownership/fencing
2. changed-address case
3. stale-session rejection case
4. epoch-bump invalidation case
5. traceability back to accepted prototype behavior

Status:

- accepted
- Slice 1 ownership/fencing tests are in place and passing
- acceptance/gate mapping is strong enough to move to Slice 2

### P3: Slice 2 Planning Setup

1. define Slice 2 boundaries explicitly
2. distinguish Slice 2 core from carried-forward prototype support
3. map Slice 2 engine expectations from accepted prototype evidence
4. prepare Slice 2 validation targets

Status:

- accepted
- Slice 2 recovery execution core is implemented and validated
- corrected tester summary accepted:
  - `12` ownership tests
  - `18` recovery tests
  - `30` total

### P4: Slice 3 Planning Setup

1. define Slice 3 boundaries explicitly
2. connect recovery decisions to real engine recoverability inputs
3. make trusted-base / rebuild-source decision use real engine data inputs
4. prepare Slice 3 validation targets

Status:

- accepted
- Slice 3 data / recoverability core is implemented and validated
- corrected tester summary accepted:
  - `12` ownership tests
  - `18` recovery tests
  - `18` recoverability tests
  - `48` total
- important boundary preserved:
  - engine proves historical-correctness prerequisites
  - full historical reconstruction proof remains simulator-side

## Slice 3 Guardrails

Slice 3 is the point where V2 must move from:

- recovery automaton is coherent

to:

- recovery basis is provable

So Slice 3 must stay tight.

### Guardrail 1: No optimistic watermark in place of recoverability proof

Do not accept:

- loose head/tail watermarks
- "looks retained enough"
- heuristic recoverability

Slice 3 should prove:

1. why a gap is recoverable
2. why a gap is unrecoverable

### Guardrail 2: No current extent state pretending to be historical correctness

Do not accept:

- current extent image as substitute for target-LSN truth
- checkpoint/base state that leaks newer state into older historical queries

Slice 3 should prove historical correctness at the actual recovery target.

### Guardrail 3: No `snapshot + tail` without trusted-base proof

Do not accept:

- "snapshot exists" as sufficient

Require:

1. trusted base exists
2. trusted base covers the required base state
3. retained tail can be replayed continuously from that base to the target

If not, recovery must use:

- `FullBase`

### Guardrail 4: Truncation is protocol boundary, not cleanup policy

Do not treat truncation as:

- optional cleanup
- post-recovery tidying

Treat truncation as:

1. divergent tail removal
2. explicit safe-boundary restoration
3. prerequisite for safe `InSync` / recovery completion where applicable

### P5: Slice 4 Planning Setup

1. define Slice 4 boundaries explicitly
2. connect engine control/recovery core to real assignment/control intent entry path
3. add engine observability / debug surface for ownership and recovery failures
4. prepare integration validation against V2-boundary failure classes

Status:

- accepted
- Slice 4 integration closure is implemented and validated
- corrected tester summary accepted:
  - `12` ownership tests
  - `18` recovery tests
  - `18` recoverability tests
  - `11` integration tests
  - `59` total

## Slice 4 Guardrails

Slice 4 should close integration, not just add an entry point and some logs.

### Guardrail 1: Entry path must actually drive recovery

Do not accept:

- tests that manually push sender/session state while only pretending to use integration entry points

Require:

1. real assignment/control intent entry path
2. session creation / invalidation / restart triggered through that path
3. recovery flow driven from that path, not only from unit-level helper calls

### Guardrail 2: Changed-address must survive the real entry path

Do not accept:

- changed-address correctness proven only at local object level

Require:

1. stable `ReplicaID` survives real assignment/update entry path
2. endpoint update invalidates old session correctly
3. new recovery session is created correctly on updated endpoint

### Guardrail 3: Observability must show protocol causality

Do not accept:

- only state snapshots
- only phase dumps

Require observability that can explain:

1. why recovery entered `NeedsRebuild`
2. why a session was superseded
3. why a completion or progress update was rejected
4. why endpoint / epoch change caused invalidation

### Guardrail 4: Failure replay must be explainable

Do not accept:

- a replay that reproduces failure but cannot explain the cause from engine observability

Require:

1. selected failure-class replays through the real entry path
2. observability sufficient to explain the control/recovery decision
3. reviewability against key V2-boundary failures

## Exit Criteria

Phase 05 Slice 1 is done when:

1. the real V2 engine module location is chosen
2. Slice 1 boundaries are explicit
3. engine ownership core exists under `sw-block/`
4. engine-side ownership/fencing tests pass
5. Slice 1 evidence is reviewable against prototype expectations

This bar is now met.

Phase 05 Slice 2 is done when:

1. engine-side recovery execution flow exists
2. zero-gap / catch-up / needs-rebuild branching is explicit
3. stale execution is rejected during active recovery
4. bounded catch-up semantics are enforced in engine path
5. rebuild execution shell is validated

This bar is now met.

Phase 05 Slice 3 is done when:

1. recoverable vs unrecoverable gap uses real engine recoverability inputs
2. trusted-base / rebuild-source decision uses real engine data inputs
3. truncation / safe-boundary handling is tied to real engine state
4. history-driven engine APIs exist for recovery decisions
5. Slice 3 validation is reviewable without overclaiming full historical reconstruction

This bar is now met.

Phase 05 Slice 4 is done when:

1. real assignment/control intent entry path exists
2. changed-address recovery works through the real entry path
3. observability explains protocol causality, not only state snapshots
4. selected V2-boundary failures are replayable and diagnosable through engine integration tests

This bar is now met.

## Assignment For `sw`

Phase 05 is now complete.

Next phase:

- `Phase 06` broader engine implementation stage

## Assignment For `tester`

Phase 05 validation is complete.

Next phase:

- `Phase 06` engine implementation validation against real-engine constraints and failure classes

## Management Rule

`Phase 05` should stay narrow.

It should start the engine line with:

1. ownership
2. fencing
3. validation

It should not try to absorb later slices early.
