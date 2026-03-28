# V2 Scenario Sources From V1 and V1.5

Date: 2026-03-27

## Purpose

This document distills V1 / V1.5 real-test material into V2 scenario inputs.

Sources:

- `learn/projects/sw-block/phases/phase13_test.md`
- `learn/projects/sw-block/phases/phase-13-v2-boundary-tests.md`

This is not the active scenario backlog.

Use:

- `v2_scenarios.md` for the active V2 scenario set
- this file for historical source and rationale

## How To Use This File

For each item below:

1. keep the real V1/V1.5 test as implementation evidence
2. create or maintain a V2 simulator scenario for the protocol core
3. define the expected V2 behavior explicitly

## Source Buckets

### 1. Core protocol behavior

These are the highest-value simulator inputs.

- barrier durability truth
- reconnect + catch-up
- non-convergent catch-up -> rebuild
- rebuild fallback
- failover / promotion safety
- WAL retention / tail-chasing
- durability mode semantics

Recommended V2 treatment:

- `sim_core`

### 2. Supporting invariants

These matter, but usually as reduced simulator checks.

- canonical address handling
- replica role/epoch gating
- committed-prefix rules
- rebuild publication cleanup
- assignment refresh behavior

Recommended V2 treatment:

- `sim_reduced`

### 3. Real-only implementation behavior

These should usually stay in real-engine tests.

- actual wire encoding / decode bugs
- real disk / `fdatasync` timing
- NVMe / iSCSI frontend behavior
- Go concurrency artifacts tied to concrete implementation

Recommended V2 treatment:

- `real_only`

### 4. V2 boundary items

These are especially important.

They should remain visible as:

- current V1/V1.5 limitation
- explicit V2 acceptance target

Recommended V2 treatment:

- `v2_boundary`

## Distilled Scenario Inputs

### A. Barrier truth uses durable replica progress

Real source:

- Phase 13 barrier / `replicaFlushedLSN` tests

Why it matters:

- commit must follow durable replica progress, not send progress

V2 target:

- barrier completion counted only from explicit durable progress state

### B. Same-address transient outage

Real source:

- Phase 13 reconnect / catch-up tests
- `CP13-8` short outage recovery

Why it matters:

- proves cheap short-gap recovery path

V2 target:

- explicit recoverability check
- catch-up if recoverable
- rebuild otherwise

### C. Changed-address restart

Real source:

- `CP13-8 T4b`
- changed-address refresh fixes

Why it matters:

- endpoint is not identity
- stale endpoint must not remain authoritative

V2 target:

- heartbeat/control-plane learns new endpoint
- reassignment updates sender target
- recovery session starts only after endpoint truth is updated

### D. Non-convergent catch-up / tail-chasing

Real source:

- Phase 13 retention + catch-up + rebuild fallback line

Why it matters:

- “catch-up exists” is not enough
- must know when to stop and rebuild

V2 target:

- explicit `CatchingUp -> NeedsRebuild`
- no fake success

### E. Slow control-plane recovery

Real source:

- `CP13-8 T4b` hardware behavior before fix

Why it matters:

- safety can be correct while availability recovery is poor

V2 target:

- explicit fast recovery path when possible
- explicit fallback when only control-plane repair can help

### F. Stale message / delayed ack fencing

Real source:

- Phase 13 epoch/fencing tests
- V2 scenario work already mirrors this

Why it matters:

- old lineage must not mutate committed prefix

V2 target:

- stale message rejection is explicit and testable

### G. Promotion candidate safety

Real source:

- failover / promotion gating tests
- V2 candidate-selection work

Why it matters:

- wrong promotion loses committed lineage

V2 target:

- candidate must satisfy:
  - running
  - epoch aligned
  - state eligible
  - committed-prefix sufficient

### H. Rebuild boundary after failed catch-up

Real source:

- Phase 13 rebuild fallback behavior

Why it matters:

- rebuild is required when retained WAL cannot safely close the gap

V2 target:

- rebuild is explicit fallback, not ad hoc recovery

## Immediate Feed Into `v2_scenarios.md`

These are the most important V1/V1.5-derived V2 scenarios:

1. same-address transient outage
2. changed-address restart
3. non-convergent catch-up / tail-chasing
4. stale delayed message / barrier ack rejection
5. committed-prefix-safe promotion
6. control-plane-latency recovery shape

## What Should Not Be Copied Blindly

Do not clone every real-engine test into the simulator.

Do not use the simulator for:

- exact OS timing
- exact socket/wire bugs
- exact block frontend behavior
- implementation-specific lock races

Instead:

- extract the protocol invariant
- model the reduced scenario if the protocol value is high

## Bottom Line

V1 / V1.5 tests should feed V2 in two ways:

1. as historical evidence of what failed or mattered in real life
2. as scenario seeds for the V2 simulator and acceptance backlog
