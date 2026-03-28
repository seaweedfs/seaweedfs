# Phase 03

Date: 2026-03-27
Status: active
Purpose: define the next simulator tier after Phase 02, focused on timeout semantics, timer races, and a cleaner split between protocol simulation and event/interleaving simulation

## Goal

Phase 03 exists to cover behavior that current `distsim` still abstracts away:

1. timeout semantics
2. timer races
3. event ordering under competing triggers
4. clearer separation between:
   - protocol / lineage simulation
   - event / race simulation

This phase should not reopen already-closed Phase 02 protocol scope unless a clear bug is found.

## Why A New Phase

Phase 02 already delivered:

- protocol-state assertions
- V1 / V1.5 / V2 comparison scenarios
- endpoint identity modeling
- control-plane assignment-update flow
- committed-prefix-aware promotion eligibility

What remains is different in character:

- timers
- delayed events racing with each other
- timeout-triggered state changes
- more explicit event scheduling

That deserves a new phase boundary.

## Source Of Truth

Design/source-of-truth:
- `sw-block/design/v2_scenarios.md`
- `sw-block/design/v2-dist-fsm.md`
- `sw-block/design/v2-scenario-sources-from-v1.md`
- `sw-block/design/v1-v15-v2-comparison.md`

Current prototype base:
- `sw-block/prototype/distsim/`
- `sw-block/prototype/distsim/simulator.go`

## Scope

### In scope

1. timeout semantics
- barrier timeout
- catch-up timeout
- reservation expiry timeout
- rebuild timeout

2. timer races
- delayed ack vs timeout
- timeout vs promotion
- reconnect vs timeout
- catch-up completion vs expiry
- rebuild completion vs epoch bump

3. simulator split clarification
- `distsim` keeps:
  - protocol correctness
  - lineage
  - recoverability
  - reference-state checking
- `eventsim` grows into:
  - event scheduling
  - timer firing
  - same-time interleavings
  - race exploration

### Out of scope

- production integration
- real transport
- real disk timings
- SPDK
- raw allocator

## Assigned Tasks For `sw`

### P0

1. Write a concrete `eventsim` scope note in code/docs
- define what stays in `distsim`
- define what moves to `eventsim`
- avoid overlap and duplicated semantics

2. Add minimal timeout event model
- first-class timeout event type(s)
- at minimum:
  - barrier timeout
  - catch-up timeout
  - reservation expiry

3. Add timeout-backed scenarios
- stale delayed ack vs timeout
- catch-up timeout before convergence
- reservation expiry during active recovery

### P1

4. Add race-focused tests
- promotion vs delayed stale ack
- rebuild completion vs epoch bump
- reconnect success vs timeout firing

5. Keep traces debuggable
- failing runs must dump:
  - seed
  - event order
  - timer events
  - node states
  - committed prefix

### P2

6. Decide whether selected `distsim` scenarios should also exist in `eventsim`
- only when timer/event ordering is the real point
- do not duplicate every scenario blindly

## Current Progress

Delivered in this phase so far:

- `eventsim` scope note added in code
- explicit timeout model added:
  - barrier timeout
  - catch-up timeout
  - reservation timeout
- timeout-backed scenarios added and reviewed
- same-tick rule made explicit:
  - data before timers
- recovery timeout cancellation is now model-driven, not test-driven
- stale barrier ack after timeout is explicitly rejected
- stale timeouts are separated from authoritative timeouts:
  - `FiredTimeouts`
  - `IgnoredTimeouts`
- race-focused scenarios added and reviewed:
  - promotion vs stale catch-up timeout
  - promotion vs stale barrier timeout
  - rebuild completion vs epoch bump
  - epoch bump vs stale catch-up timeout
- reusable trace builder added for replay/debug support
- current `distsim` suite at latest review:
  - 86 tests passing

Remaining focus for `sw`:

- Phase 03 P0 and P1 are effectively complete
- Phase 03 P2 is also effectively complete after review
- any further simulator work should now be narrow and evidence-driven
- recommended next simulator additions only:
  - control-plane latency parameter
  - sustained-write convergence / tail-chasing load test
  - one multi-promotion lineage extension

## Invariants To Preserve

1. committed data remains durable per policy
2. uncommitted data is never revived as committed
3. stale epoch traffic never mutates current lineage
4. committed prefix remains contiguous
5. timeout-triggered transitions are explicit and explainable
6. races do not silently bypass fencing or rebuild boundaries

## Required Updates Per Task

For each completed task:

1. add or update tests
2. update `sw-block/design/v2_scenarios.md` if scenario coverage changed
3. add a short note to:
   - `sw-block/.private/phase/phase-03-log.md`
4. if the simulator boundary changed, record it in:
   - `sw-block/.private/phase/phase-03-decisions.md`

## Exit Criteria

Phase 03 is done when:

1. timeout semantics exist as explicit simulator behavior
2. at least three important timer-race scenarios are modeled and tested
3. `distsim` vs `eventsim` responsibilities are clearly separated
4. failure traces from race/timeout scenarios are replayable enough to debug
