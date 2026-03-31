# Phase 06

Date: 2026-03-30
Status: complete
Purpose: move from validated engine slices to the first broader runnable V2 engine stage

## Why This Phase Exists

`Phase 05` established and validated:

1. ownership core
2. recovery execution core
3. recoverability/data gating core
4. integration closure

What still does not exist is a broader engine stage that can run with:

1. real control-plane inputs
2. real persistence/backing inputs
3. non-trivial execution loops instead of only synchronous convenience paths

So `Phase 06` exists to turn the accepted engine shape into the first broader runnable engine stage.

Phase 06 must connect the accepted engine core to real control and real storage truth, not just wrap current abstractions with adapters.

## Phase Goal

Build the first broader V2 engine stage without reopening protocol shape.

This phase should focus on:

1. real engine adapters around the accepted core
2. asynchronous or stepwise execution paths where Slice 4 used synchronous helpers
3. real retained-history / checkpoint input plumbing
4. validation against selected real failure classes and real implementation constraints

## Overall Roadmap

Completed:

1. Phase 01-03: design + simulator
2. Phase 04: prototype closure
3. Phase 4.5: evidence hardening
4. Phase 05: engine slice closure
5. Phase 06: broader engine implementation stage

Next:

1. Phase 07: real-system integration / product-path decision

This roadmap should stay strict:

- no return to broad prototype expansion
- no uncontrolled engine sprawl

## Scope

### In scope

1. control-plane adapter into `sw-block/engine/replication/`
2. retained-history / checkpoint adapter into engine recoverability APIs
3. replacement of synchronous convenience flows with explicit engine steps where needed
4. engine error taxonomy and observability tightening
5. validation against selected real failure classes from:
   - `learn/projects/sw-block/`
   - `weed/storage/block*`

### Out of scope

1. Smart WAL expansion
2. full backend redesign
3. performance optimization as primary goal
4. V1 replacement rollout
5. full product integration

## Phase 06 Items

### P0: Engine Stage Plan

Status:

- accepted
- module boundaries now explicit:
  - `adapter.go`
  - `driver.go`
  - `orchestrator.go` classification
- convenience flows are now classified as:
  - test-only convenience wrapper
  - stepwise engine task
  - planner/executor split

### P1: Control / History Adapters

Status:

- accepted
- `StorageAdapter` boundary exists and is exercised by tests
- full-base rebuild now has a real pin/release contract
- WAL pinning is tied to actual recovery contract, not loose watermark use
- planner fails closed on missing sender / missing session / wrong session kind

### P2: Execution Driver

Status:

- accepted
- executor now owns resource lifecycle on success / failure / cancellation
- catch-up execution is stepwise and budget-checked per progress step
- rebuild execution consumes plan-bound source/target values and does not re-derive policy at execute time
- `CompleteCatchUp` / `CompleteRebuild` remain test-only convenience wrappers
- tester validation accepted with reduced-but-sufficient rebuild failure-path coverage

### P3: Validation Against Real Failure Classes

Status:

- accepted
- changed-address restart now validated through planner/executor path with plan cancellation
- stale epoch/session during active execution now validated through the executor-managed loop
- cross-layer trusted-base / replayable-tail proof path validated end-to-end
- rebuild fallback and pin-failure cleanup now fail closed and are diagnosable

## Guardrails

### Guardrail 1: Do not reopen protocol shape

Phase 06 implemented around accepted engine slices and did not reopen:

1. sender/session authority model
2. bounded catch-up contract
3. recoverability/truncation boundary

### Guardrail 2: Do not let adapters smuggle V1 structure back in

V1 code and docs remain:

1. constraints
2. failure gates
3. integration references

not the V2 architecture template.

### Guardrail 3: Prefer explicit engine steps over synchronous convenience

Key convenience helpers remain test-only. Real engine work now has explicit planner/executor boundaries.

### Guardrail 4: Keep evidence quality high

Phase 06 improved:

1. cross-layer traceability
2. diagnosability
3. real-failure validation

without growing protocol surface.

### Guardrail 5: Do not fake storage truth with metadata-only adapters

Phase 06 now requires:

1. trusted base to come from storage-side truth
2. replayable tail to be grounded in retention state
3. observable rejection when those proofs cannot be established

## Exit Criteria

Phase 06 is done when:

1. engine has real control/history adapters into the accepted core
2. engine has real storage/base adapters into the accepted core
3. key synchronous convenience paths are explicitly classified or replaced by real engine steps where necessary
4. selected real failure classes are validated against the engine stage
5. at least one cross-layer storage/engine proof path is validated end-to-end
6. engine observability remains good enough to explain recovery causality

Status:

- met

## Closeout

`Phase 06` is complete.

It established:

1. a broader runnable engine stage around the accepted Phase 05 core
2. real planner/executor/resource contracts
3. validated failure-class behavior through the engine path
4. diagnosable proof rejection and cleanup behavior

Next step:

- `Phase 07` real-system integration / product-path decision
