# V2 Production Roadmap

Date: 2026-03-30
Status: historical roadmap
Purpose: define the path from the accepted V2 engine core to a production candidate

## Current Position

Completed:

1. design / FSM closure
2. simulator / protocol validation
3. prototype closure
4. evidence hardening
5. engine core slices:
   - Slice 1 ownership core
   - Slice 2 recovery execution core
   - Slice 3 data / recoverability core
   - Slice 4 integration closure

Current stage:

- entering broader engine implementation

This means the main risk is no longer:

- whether the V2 idea stands up

The main risk is:

- whether the accepted engine core can be turned into a real system without reintroducing V1/V1.5 structure and semantics

## Roadmap Summary

1. Phase 06: broader engine implementation stage
2. Phase 07: real-system integration / product-path decision
3. Phase 08: pre-production hardening
4. Phase 09: performance / scale / soak validation
5. Phase 10: production candidate and rollout gate

## Phase 06

### Goal

Connect the accepted engine core to:

1. real control truth
2. real storage truth
3. explicit engine execution steps

### Outputs

1. control-plane adapter into the engine core
2. storage/base/recoverability adapters
3. explicit execution-driver model where synchronous helpers are no longer sufficient
4. validation against selected real failure classes

### Gate

At the end of Phase 06, the project should be able to say:

- the engine core can live inside a real system shape

## Phase 07

### Goal

Move from engine-local correctness to a real runnable subsystem.

### Outputs

1. service-style runnable engine slice
2. integration with real control and storage surfaces
3. crash/failover/restart integration tests
4. decision on the first viable product path

### Gate

At the end of Phase 07, the project should be able to say:

- the engine can run as a real subsystem, not only as an isolated core

## Phase 08

### Goal

Turn correctness into operational safety.

### Outputs

1. observability hardening
2. operator/debug flows
3. recovery/runbook procedures
4. config surface cleanup
5. realistic durability/restart validation

### Gate

At the end of Phase 08, the project should be able to say:

- operators can run, debug, and recover the system safely

## Phase 09

### Goal

Prove viability under load and over time.

### Outputs

1. throughput / latency baselines
2. rebuild / catch-up cost characterization
3. steady-state overhead measurement
4. soak testing
5. scale and failure-under-load validation

### Gate

At the end of Phase 09, the project should be able to say:

- the design is not only correct, but viable at useful scale and duration

## Phase 10

### Goal

Produce a controlled production candidate.

### Outputs

1. feature-gated production candidate
2. rollback strategy
3. migration/coexistence plan with V1
4. staged rollout plan
5. production acceptance checklist

### Gate

At the end of Phase 10, the project should be able to say:

- the system is ready for a controlled production rollout

## Cross-Phase Rules

### Rule 1: Do not reopen protocol shape casually

The accepted core should remain stable unless new implementation evidence forces a change.

### Rule 2: Use V1 as validation source, not design template

Use:

1. `learn/projects/sw-block/`
2. `weed/storage/block*`

for:

1. failure gates
2. constraints
3. integration references

Do not use them as the default V2 architecture template.

### Rule 3: Keep `CatchUp` narrow

Do not let later implementation phases re-expand `CatchUp` into a broad, optimistic, long-lived recovery mode.

### Rule 4: Keep evidence quality ahead of object growth

New work should preferentially improve:

1. traceability
2. diagnosability
3. real-failure validation
4. operational confidence

not simply add new objects, states, or mechanisms.

## Production Readiness Ladder

The project should move through this ladder explicitly:

1. proof-of-design
2. proof-of-engine-shape
3. proof-of-runnable-engine-stage
4. proof-of-operable-system
5. proof-of-viable-production-candidate

Current ladder position:

- between `2` and `3`
- engine core accepted; broader runnable engine stage underway

## Next Documents To Maintain

1. `sw-block/.private/phase/phase-06.md`
2. `sw-block/docs/archive/design/v2-engine-readiness-review.md`
3. `sw-block/docs/archive/design/v2-engine-slicing-plan.md`
4. this roadmap
