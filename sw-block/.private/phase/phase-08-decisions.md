# Phase 08 Decisions

## Decision 1: Phase 08 is pre-production hardening, not protocol rediscovery

The accepted V2 product path from `Phase 07` is the basis.

`Phase 08` should harden that path rather than reopen accepted protocol shape.

## Decision 2: The first hardening priorities are control delivery and execution closure

The most important remaining gaps are:

1. real master/control delivery into the bridge/engine path
2. integrated engine -> executor -> `v2bridge` catch-up execution closure
3. first rebuild execution path for the chosen product path

## Decision 3: Carry-forward limitations remain explicit until closed

Phase 08 must keep explicit:

1. committed truth is still not separated from checkpoint truth
2. rebuild execution is still incomplete
3. current control delivery is still simulated

## Decision 4: Phase 08 P0 is accepted

The hardening plan is sufficiently specified to begin implementation work.

In particular, `P0` now fixes:

1. the committed-truth gate decision requirement
2. the unified replay requirement after control and execution closure
3. the need for at least one real failover / reassignment validation target
## Decision 5: The committed-truth limitation must become a hardening gate

Phase 08 must explicitly decide one of:

1. `CommittedLSN != CheckpointLSN` separation is mandatory before a production-candidate phase
2. the first candidate path is intentionally bounded to the currently proven pre-checkpoint replay behavior

It must not remain only a documented carry-forward.

## Decision 6: Unified-path replay is required after control and execution closure

Once real control delivery and integrated execution closure land, `Phase 08` must replay the accepted failure-class set again on the unified live path.

This prevents independent closure of:

1. control delivery
2. execution closure

without proving that they behave correctly together.

## Decision 7: Real failover / reassignment validation is mandatory for the chosen path

Because the chosen product path depends on the existing master / volume-server heartbeat path, at least one real failover / promotion / reassignment cycle must be a named hardening target in `Phase 08`.

## Decision 8: Phase 08 should reuse the existing Seaweed control/runtime path, not invent a new one

For the first hardening path, implementation should preferentially reuse:

1. existing master / heartbeat / assignment delivery
2. existing volume-server assignment receive/apply path
3. existing `blockvol` runtime and `v2bridge` storage/runtime hooks

This reuse is about:

1. control-plane reality
2. storage/runtime reality
3. execution-path reality

It is not permission to inherit old policy semantics as V2 truth.

The hard rule remains:

1. engine owns recovery policy
2. bridge translates confirmed control/storage truth
3. `blockvol` executes I/O
