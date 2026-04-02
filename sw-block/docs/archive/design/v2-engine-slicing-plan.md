# V2 Engine Slicing Plan

Date: 2026-03-29
Status: historical slicing plan
Purpose: define the first real V2 engine slices after prototype and `Phase 4.5` closure

## Goal

Move from:

- standalone design/prototype truth under `sw-block/prototype/`

to:

- a real V2 engine core under `sw-block/`

without dragging V1.5 lifecycle assumptions into the implementation.

## Planning Rules

1. reuse V1 ideas and tests selectively, not structurally
2. prefer narrow vertical slices over broad skeletons
3. each slice must preserve the accepted V2 ownership/fencing model
4. keep simulator/prototype as validation support, not as the implementation itself
5. do not mix V2 engine work into `weed/storage/blockvol/`

## First Engine Stage

The first engine stage should build the control/recovery core, not the full storage engine.

That means:

1. per-replica sender identity
2. one active recovery session per replica per epoch
3. sender-owned execution authority
4. explicit recovery outcomes:
   - zero gap
   - bounded catch-up
   - rebuild
5. rebuild execution shell only
   - do not hard-code final snapshot + tail vs full base decision logic yet
   - keep real rebuild-source choice tied to Slice 3 recoverability inputs

## Recommended Slice Order

### Slice 1: Engine Ownership Core

Purpose:

- carry the accepted `enginev2` ownership/fencing model into the real engine core

Scope:

1. stable per-replica sender object
2. stable recovery-session object
3. session identity fencing
4. endpoint / epoch invalidation
5. sender-group or equivalent ownership registry

Acceptance:

1. stale session results cannot mutate current authority
2. changed-address and epoch-bump invalidation work in engine code
3. the 4 V2-boundary ownership themes remain provable

### Slice 2: Engine Recovery Execution Core

Purpose:

- move the prototype execution APIs into real engine behavior

Scope:

1. connect / handshake / catch-up flow
2. bounded `CatchUp`
3. explicit `NeedsRebuild`
4. sender-owned rebuild execution path
5. rebuild execution shell without final trusted-base selection policy

Acceptance:

1. bounded catch-up does not chase indefinitely
2. rebuild is exclusive from catch-up
3. session completion rules are explicit and fenced

### Slice 3: Engine Data / Recoverability Core

Purpose:

- connect recovery behavior to real retained-history / checkpoint mechanics

Scope:

1. real recoverability decision inputs
2. trusted-base decision for rebuild source
3. minimal real checkpoint/base-image integration
4. real truncation / safe-boundary handling

This is the first slice that should decide, from real engine inputs, between:

1. `snapshot + tail`
2. `full base`

Acceptance:

1. engine can explain why recovery is allowed
2. rebuild-source choice is explicit and testable
3. historical correctness and truncation rules remain intact

### Slice 4: Engine Integration Closure

Purpose:

- bind engine control/recovery core to real orchestration and validation surfaces

Scope:

1. real assignment/control intent entry path
2. engine-facing observability
3. focused real-engine tests for V2-boundary cases
4. first integration review against real failure classes

Acceptance:

1. key V2-boundary failures are reproduced and closed in engine tests
2. engine observability is good enough to debug ownership/recovery failures
3. remaining gaps are system/performance gaps, not control-model ambiguity

## What To Reuse

Good reuse candidates:

1. tests and failure cases from V1 / V1.5
2. narrow utility/data helpers where not coupled to V1 lifecycle
3. selected WAL/history concepts if they fit V2 ownership boundaries

Do not structurally reuse:

1. V1/V1.5 shipper lifecycle
2. address-based identity assumptions
3. `SetReplicaAddrs`-style behavior
4. old recovery control structure

## Where The Work Should Live

Real V2 engine work should continue under:

- `sw-block/`

Recommended next area:

- `sw-block/core/`
or
- `sw-block/engine/`

Exact path can be chosen later, but it should remain separate from:

- `sw-block/prototype/`
- `weed/storage/blockvol/`

## Validation Plan For Engine Slices

Each engine slice should be validated at three levels:

1. prototype alignment
- does engine behavior preserve the accepted prototype invariant?

2. focused engine tests
- does the real engine slice enforce the same contract?

3. scenario mapping
- does at least one important V1/V1.5 failure class remain closed?

## Non-Goals For First Engine Stage

Do not try to do these immediately:

1. full Smart WAL expansion
2. performance optimization
3. V1 replacement/migration plan
4. full product integration
5. all storage/backend redesign at once

## Immediate Next Assignment

The first concrete engine-planning task should be:

1. choose the real V2 engine module location under `sw-block/`
2. define Slice 1 file/module boundaries
3. write a short engine ownership-core spec
4. map 3-5 acceptance scenarios directly onto Slice 1 expectations
