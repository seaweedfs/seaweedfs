# Phase 4.5 Decisions

## Decision 1: Phase 4.5 remains a bounded hardening phase

It is not a new architecture line and must not expand into broad feature work.

Purpose:

1. tighten recovery boundaries
2. strengthen crash-consistency / recoverability proof
3. clear the path for engine planning

## Decision 2: `sw` Phase 4.5 P0 is accepted

Accepted basis:

1. bounded `CatchUp` now changes prototype behavior
2. `FrozenTargetLSN` is intrinsic to the session contract
3. `Rebuild` is a first-class sender-owned execution path
4. rebuild and catch-up are execution-path exclusive

## Decision 3: `tester` crash-consistency simulator strengthening is accepted

Accepted basis:

1. checkpoint semantics are explicit
2. recoverability after restart is no longer collapsed into a single loose watermark
3. crash-consistency invariants are executable and passing

## Decision 4: Remaining Phase 4.5 work is evidence hardening, not primitive-building

Completed focus:

1. `A5-A8` prototype + simulator double evidence
2. predicate exploration for dangerous states
3. adversarial search over crash-consistency / liveness states

Remaining optional work:

4. any low-priority cleanup that improves clarity without reopening design

## Decision 5: After Phase 4.5, the project should move to engine-planning readiness review

Unless new blocking flaws appear, the next major decision after `4.5` should be:

1. real V2 engine planning
2. engine slicing plan

not another broad prototype phase

## Decision 6: Phase 4.5 is complete

Reason:

1. bounded `CatchUp` is semantic in the prototype
2. `Rebuild` is first-class in the prototype
3. crash-consistency / restart-recoverability are materially stronger in the simulator
4. `A5-A8` evidence is materially stronger on both prototype and simulator sides
5. adversarial search found and helped fix a real correctness bug, validating the proof style
