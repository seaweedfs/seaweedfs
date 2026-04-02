# V2 Engine Readiness Review

Date: 2026-03-29
Status: historical readiness review
Purpose: record the decision on whether the current V2 design + prototype + simulator stack is strong enough to begin real V2 engine slicing

## Decision

Current judgment:

- proceed to real V2 engine planning
- do not open a `V2.5` redesign track at this time

This is a planning-readiness decision, not a production-readiness claim.

## Why This Review Exists

The project has now completed:

1. design/FSM closure for the V2 line
2. protocol simulation closure for:
   - V1 / V1.5 / V2 comparison
   - timeout/race behavior
   - ownership/session semantics
3. standalone prototype closure for:
   - sender/session ownership
   - execution authority
   - recovery branching
   - minimal historical-data proof
   - prototype scenario closure
4. `Phase 4.5` hardening for:
   - bounded `CatchUp`
   - first-class `Rebuild`
   - crash-consistency / restart-recoverability
   - `A5-A8` stronger evidence

So the question is no longer:

- "can the prototype be made richer?"

The question is:

- "is the evidence now strong enough to begin real engine slicing?"

## Evidence Summary

### 1. Design / Protocol

Primary docs:

- `sw-block/design/v2-acceptance-criteria.md`
- `sw-block/design/v2-open-questions.md`
- `sw-block/design/v2_scenarios.md`
- `sw-block/design/v1-v15-v2-comparison.md`
- `sw-block/docs/archive/design/v2-prototype-roadmap-and-gates.md`

Judgment:

- protocol story is coherent
- acceptance set exists
- major V1 / V1.5 failures are mapped into V2 scenarios

### 2. Simulator

Primary code/tests:

- `sw-block/prototype/distsim/`
- `sw-block/prototype/distsim/eventsim.go`
- `learn/projects/sw-block/test/results/v2-simulation-review.md`

Judgment:

- strong enough for protocol/design validation
- strong enough to challenge crash-consistency and liveness assumptions
- not a substitute for real engine / hardware proof

### 3. Prototype

Primary code/tests:

- `sw-block/prototype/enginev2/`
- `sw-block/prototype/enginev2/acceptance_test.go`

Judgment:

- ownership is explicit and fenced
- execution authority is explicit and fenced
- bounded `CatchUp` is semantic, not documentary
- `Rebuild` is a first-class sender-owned path
- historical-data and recoverability reasoning are executable

### 4. `A5-A8` Double Evidence

Prototype-side grouped evidence:

- `sw-block/prototype/enginev2/acceptance_test.go`

Simulator-side grouped evidence:

- `sw-block/docs/archive/design/a5-a8-traceability.md`
- `sw-block/prototype/distsim/`

Judgment:

- the critical acceptance items that most affect engine risk now have materially stronger proof on both sides

## What Is Good Enough Now

The following are good enough to begin engine slicing:

1. sender/session ownership model
2. stale authority fencing
3. recovery orchestration shape
4. bounded `CatchUp` contract
5. `Rebuild` as formal path
6. committed/recoverable boundary thinking
7. crash-consistency / restart-recoverability proof style

## What Is Still Not Proven

The following still require real engine work and later real-system validation:

1. actual engine lifecycle integration
2. real storage/backend implementation
3. real control-plane integration
4. real durability / fsync behavior under the actual engine
5. real hardware timing / performance
6. final production observability and failure handling

These are expected gaps. They do not block engine planning.

## Open Risks To Carry Forward

These are not blockers, but they should remain explicit:

1. prototype and simulator are still reduced models
2. rebuild-source quality in the real engine will depend on actual checkpoint/base-image mechanics
3. durability truth in the real engine must still be re-proven against actual persistence behavior
4. predicate exploration can still grow, but should not block engine slicing

## Engine-Planning Decision

Decision:

- start real V2 engine planning

Reason:

1. no current evidence points to a structural flaw requiring `V2.5`
2. the remaining gaps are implementation/system gaps, not prototype ambiguity
3. continuing to extend prototype/simulator breadth would have diminishing returns

## Required Outputs After This Review

1. `sw-block/docs/archive/design/v2-engine-slicing-plan.md`
2. first real engine slice definition
3. explicit non-goals for first engine stage
4. explicit validation plan for engine slices

## Non-Goals Of This Review

This review does not claim:

1. V2 is production-ready
2. V2 should replace V1 immediately
3. all design questions are forever closed

It only claims:

- the project now has enough evidence to begin disciplined real engine slicing
