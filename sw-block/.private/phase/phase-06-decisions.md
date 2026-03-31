# Phase 06 Decisions

## Decision 1: Phase 06 is broader engine implementation, not new design

The protocol shape and engine core contracts were already accepted.

Phase 06 implemented around them.

## Decision 2: Phase 06 must connect to real constraints

This phase explicitly used:

1. `learn/projects/sw-block/` for failure gates and test lineage
2. `weed/storage/block*` for real implementation constraints

without importing V1 structure as the V2 design template.

## Decision 3: Phase 06 should replace key synchronous conveniences

The accepted Slice 4 convenience flows were sufficient for closure work, but broader engine work required real step boundaries.

This is now satisfied via planner/executor separation.

## Decision 4: Phase 06 ends with a runnable engine stage decision

Result:

- yes, the project now has a broader runnable engine stage that is ready to proceed to real-system integration / product-path work

## Decision 5: Phase 06 P0 is accepted

Accepted scope:

1. adapter/module boundaries
2. convenience-flow classification
3. initial real-engine stage framing

## Decision 6: Phase 06 P1 is accepted

Accepted scope:

1. storage/control adapter interfaces
2. `RecoveryDriver` planner/resource-acquisition layer
3. full-base and WAL retention resource contracts
4. fail-closed preconditions on planning paths

## Decision 7: Phase 06 P2 is accepted

Accepted scope:

1. explicit planner/executor split on top of `RecoveryPlan`
2. executor-owned cleanup symmetry on success/failure/cancellation
3. plan-bound rebuild execution with no policy re-derivation at execute time
4. synchronous orchestrator completion helpers remain test-only convenience

## Decision 8: Phase 06 P3 is accepted

Accepted scope:

1. selected real failure classes validated through the engine path
2. cross-layer engine/storage proof validation
3. diagnosable failure when proof or resource acquisition cannot be established

## Decision 9: Phase 06 is complete

Next step:

- `Phase 07` real-system integration / product-path decision
