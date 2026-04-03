# Phase 14

Date: 2026-04-03
Status: active
Purpose: make the `V2 core` explicit inside `sw-block/engine/replication` so
accepted semantic constraints become executable ownership, rather than staying
only as design and constrained-`V1` interpretation

## Why This Phase Exists

`Phase 13` accepted a bounded replication-correctness package on the current
chosen path, including:

1. corrected `sync_all` replication semantics
2. bounded real-workload validation
3. assignment/publication closure
4. bounded mode normalization

That package matters, but it still mostly evaluates `V1` runtime behavior under
`V2` constraints.

`Phase 14` exists to change that.

The new problem is no longer:

1. keep deepening constrained-`V1` validation as the primary path

It is:

1. make `V2 core` an explicit owner inside the repo
2. turn accepted claims into core-owned state, events, commands, and projections
3. create a bounded executable basis for later adapter rebinding

## Phase Goal

Build the first real `V2 core` inside `sw-block/engine/replication` as a
deterministic, side-effect-free semantic owner for:

1. state and transitions
2. command decisions
3. outward projection meaning

This phase does not yet claim live runtime cutover.

## Execution Rule

For all `Phase 14` work, implementation order must be:

1. define core-owned state and transitions
2. define command-emission rules
3. define projection contracts
4. only then connect adapters in later phases

Do not invert this order.

If runtime wiring comes first, `V1` mixed runtime state will silently retake
semantic authority.

## Execution Model

This phase uses the new working model:

1. primary developer
   - owns `V2 core` semantic design and implementation
   - decides state/transition/command/projection shape
2. `sw`
   - supports bounded implementation work after semantic ownership is already
     defined
   - should receive only narrow, easy-to-accept tasks
3. `tester`
   - validates bounded acceptance basis and checks for overclaim
4. `manager`
   - performs phase challenge/review gates against semantic discipline

## Scope

### In scope

1. explicit core-owned state in `sw-block/engine/replication`
2. explicit bounded event vocabulary
3. explicit bounded command vocabulary
4. explicit normalized projection vocabulary
5. structural acceptance tests proving accepted constraints can be represented by
   the new core

### Out of scope

1. no live `weed/` adapter hook yet
2. no product-surface rebinding yet
3. no broad runtime migration
4. no launch or performance claims
5. no reopening accepted `Phase 13` claim boundaries

## Phase 14 Slices

### `14A`: Mode / Readiness / Publication Core Closure

Goal:

1. make mode, readiness, and publication first-class core-owned meanings

Acceptance object:

1. `VolumeState`, event vocabulary, command vocabulary, projection vocabulary,
   and deterministic engine loop exist in `sw-block/engine/replication`
2. `publish_healthy` is derived from named semantic state rather than runtime
   convenience
3. fail-closed mode distinctions stay explicit:
   - `allocated_only`
   - `bootstrap_pending`
   - `replica_ready`
   - `publish_healthy`
   - `degraded`
   - `needs_rebuild`
4. the structural acceptance tests prove:
   - stable identity ownership is preserved
   - diagnostic shipped progress does not establish durable truth
   - `publish_healthy` requires durable boundary truth
   - `degraded` and `needs_rebuild` remain distinct fail-closed meanings
   - the current integrated interpretation remains `constrained_v1`, not live
     `v2_core` cutover

Status:

1. delivered

### `14B`: Assignment / Command Semantics Closure

Goal:

1. make assignment transitions and command emission rules explicit from semantic
   state rather than runtime convenience

Acceptance object:

1. assignment intent, role application, receiver start, shipper configuration,
   and invalidation commands are emitted as bounded semantic decisions
2. one bounded event sequence produces one bounded command sequence
3. command emission does not depend on `weed/` internals

Status:

1. active

### `14C`: Boundary / Recovery Semantic Closure

Goal:

1. make durable boundary and recovery semantics explicit in the same core owner

Acceptance object:

1. boundary truth distinguishes durable progress, checkpoint truth, and
   diagnostic sender progress
2. recovery semantics preserve the accepted constraints around eligibility,
   fail-closed degradation, and rebuild escalation
3. structural tests stay bounded and do not claim live path migration yet

Status:

1. planned

## Manager Review Gate

Every `Phase 14` slice must survive one challenge review that asks:

1. which semantic constraint does this slice satisfy?
2. which overclaim does this slice prevent?
3. which accepted checkpoint proof does this slice preserve?

Reject the slice if any of those questions can only be answered by vague runtime
intuition.

## Immediate Next Step

Continue with `14B`.

Use the explicit shell from `14A` as the semantic substrate, then freeze
gap-driven command emission so repeated assignments and repeated failures do not
silently turn back into runtime-convenience command spam.
