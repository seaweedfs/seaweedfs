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

## Decision 9: Phase 08 P1 is accepted with explicit scope limits

Accepted `P1` coverage is:

1. real `ProcessAssignments()` path drives V2 engine sender/session state change
2. stable remote `ReplicaID` is derived from `ServerID`, not address
3. address change preserves sender identity through the live control path
4. stale epoch/session invalidation occurs through the live control path
5. missing `ServerID` fails closed

Not accepted as part of `P1`:

1. full end-to-end gRPC heartbeat delivery proof
2. integrated catch-up execution through the live path
3. rebuild execution through the live path
4. final local stable identity beyond transport-shaped `listenAddr`

## Decision 10: Phase 08 P2 is accepted as real execution closure

Accepted `P2` coverage is:

1. `CommittedLSN` is separated from `CheckpointLSN` on the chosen `sync_all` path
2. catch-up is proven as one live chain:
   - engine plan
   - engine executor
   - `v2bridge`
   - real `blockvol` I/O
   - completion
   - cleanup
3. rebuild is proven as one live chain for the delivered path
4. cleanup/pin release is asserted after execution

Residual non-blocking scope notes:

1. `CatchUpStartLSN` is not directly asserted in tests
2. rebuild source variants are not all forced and individually asserted

## Decision 11: Phase 08 now moves to unified hardening validation

With `P1` and `P2` accepted, the next required step is:

1. replay the accepted failure-class set again on the unified live path
2. validate at least one real failover / reassignment cycle
3. validate concurrent retention/pinner behavior
4. make the committed-truth gate decision explicit for the chosen candidate path

## Decision 12: Phase 08 P3 is accepted as unified hardening validation

Accepted `P3` coverage is:

1. replay of the accepted failure-class set on the unified `P1` + `P2` live path
2. at least one real failover / reassignment cycle through the live control path
3. one true simultaneous-overlap retention/pinner safety proof
4. stronger causality assertions for invalidation, escalation, catch-up, and completion

## Decision 13: The committed-truth gate is decided for the chosen candidate path

For the chosen `RF=2 sync_all` candidate path:

1. `CommittedLSN = WALHeadLSN`
2. `CheckpointLSN` remains the durable base-image boundary
3. this separation is accepted as sufficient for the candidate-path hardening boundary

This decision is intentionally scoped:

1. it is accepted for the chosen candidate path
2. it is not yet a blanket truth for every future path or durability mode

## Decision 14: Phase 08 P4 is candidate-path judgment, not broad new engineering expansion

`P4` should close `Phase 08` by producing one explicit candidate-path judgment.

Its main output is not more isolated engineering progress, but:

1. a bounded candidate-path statement
2. an evidence-to-claim mapping from accepted `P1` / `P2` / `P3` results
3. an explicit list of accepted bounds, remaining deferrals, and production blockers

`P4` may include small closure work if needed to make the candidate statement coherent, but it should not reopen protocol design or grow into another broad hardening slice.

## Decision 15: Phase 08 P4 is accepted as candidate package closure

Accepted `P4` coverage is:

1. one explicit candidate package for the chosen `RF=2 sync_all` path
2. candidate-safe claims mapped to accepted `P1` / `P2` / `P3` evidence
3. explicit bounds, deferred items, and production blockers
4. committed-truth decision scoped to the chosen candidate path
5. module/package boundary summary for the next heavy engineering phase

Accepted judgment:

1. candidate-safe-with-bounds
2. not production-ready

## Decision 16: Phase 08 is closed and the next heavy phase is production execution closure

With `P0` through `P4` accepted, `Phase 08` is closed.

The next phase should not be a light packaging-only round.
It should begin with:

1. `Phase 09: Production Execution Closure`
2. `P0` planning for:
   - real `TransferFullBase`
   - real `TransferSnapshot`
   - real `TruncateWAL`
   - stronger live runtime execution ownership
