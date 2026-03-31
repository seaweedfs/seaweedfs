# Phase 05 Decisions

## Decision 1: Real V2 engine work lives under `sw-block/engine/replication/`

The first real engine slice is established under:

- `sw-block/engine/replication/`

This keeps V2 separate from:

- `sw-block/prototype/`
- `weed/storage/blockvol/`

## Decision 2: Slice 1 is accepted

Accepted scope:

1. stable per-replica sender identity
2. stable recovery-session identity
3. stale authority fencing
4. endpoint / epoch invalidation
5. ownership registry

## Decision 3: Stable identity must not be address-shaped

The engine registry is now keyed by stable `ReplicaID`, not mutable endpoint address.

This is a required structural break from the V1/V1.5 identity-loss pattern.

## Decision 4: Slice 2 is accepted

Accepted scope:

1. connect / handshake / catch-up flow
2. zero-gap / catch-up / needs-rebuild branching
3. stale execution rejection during active recovery
4. bounded catch-up semantics in engine path
5. rebuild execution shell

## Decision 5: Slice 3 owns real recoverability inputs

Slice 3 should be the point where:

1. recoverable vs unrecoverable gap uses real engine inputs
2. trusted-base / rebuild-source decision uses real engine data inputs
3. truncation / safe-boundary handling is tied to real engine state
4. historical correctness at recovery target is validated from engine inputs

## Decision 6: Slice 3 is accepted

Accepted scope:

1. real engine recoverability input path
2. trusted-base / rebuild-source decision from engine data inputs
3. truncation / safe-boundary handling tied to engine state
4. recoverability gating without overclaiming full historical reconstruction in engine

## Decision 7: Slice 3 should replace carried-forward heuristics where appropriate

In particular:

1. simple rebuild-source heuristics carried from prototype should not become permanent engine policy
2. Slice 3 should tighten these decisions against real engine recoverability inputs

## Decision 8: Slice 4 is the engine integration closure slice

Next focus:

1. real assignment/control intent entry path
2. engine observability / debug surface
3. focused integration tests for V2-boundary cases
4. validation against selected real failure classes from `learn/projects/sw-block/` and `weed/storage/block*`

## Decision 9: Slice 4 is accepted

Accepted scope:

1. real orchestrator entry path
2. assignment/update-driven recovery through that path
3. engine observability / causal recovery logging
4. diagnosable V2-boundary integration tests

## Decision 10: Phase 05 is complete

Reason:

1. ownership core is accepted
2. recovery execution core is accepted
3. data / recoverability core is accepted
4. integration closure is accepted

Next:

- `Phase 06` broader engine implementation stage
