# V2 Bounded Internal Pilot Pack

Date: 2026-04-05
Status: draft
Purpose: convert the frozen first-launch envelope into one bounded internal pilot
package without silently broadening scope

## Reading Rule

This pilot pack is a bounded validation package for the already-frozen chosen path.

It does NOT mean:

1. broad launch approval
2. generic production readiness
3. support for surfaces outside the named first-launch matrix
4. permission to redefine exclusions through pilot success

It means only:

1. the team may run a limited internal pilot inside the accepted chosen envelope
2. pilot outcomes must be read against the existing `Phase 12-17` claim boundary
3. incidents must be routed explicitly instead of becoming vague rollout lore

## Pilot Scope

The pilot is limited to the current first-launch support envelope:

1. replication / durability:
   - `RF=2`
   - `sync_all`
2. control/runtime path:
   - existing master / volume-server heartbeat path
   - bounded `Phase 16` runtime checkpoint
   - bounded `Phase 17B/17C` contract/policy interpretation
3. backend/runtime implementation:
   - `blockvol` through `v2bridge`
   - explicit `V2 core` as semantic owner
4. product surfaces:
   - bounded `iSCSI`
   - bounded `CSI`
   - bounded `NVMe`

Anything outside that scope is not a pilot finding for this pack.
It is either a known exclusion or later widening work.

## Pilot Environment And Topology

The pilot environment must stay fixed and reviewable:

1. use one explicit build/commit package for all pilot nodes
2. keep topology inside the bounded chosen path and do not introduce `RF>2`
3. keep transport/frontend choices inside the named supported matrix
4. pin operator-facing configuration and startup procedure in a written runbook
5. expose the existing diagnosis surfaces needed to read:
   - registry truth
   - publication/lookup truth
   - restart/failover state
   - inventory authority state

If the pilot needs ad hoc operator judgment to stay healthy, the pack is not ready.

## Success Criteria

The pilot is considered successful only if ALL of the following hold:

1. no observed behavior contradicts the bounded `17B` failover/publication contract
2. no observed behavior contradicts the bounded `17C` disturbance policy table
3. `Phase 12 P4` floor-gate expectations stay non-regressed on the mapped chosen
   path workloads
4. incidents can be classified using the explicit buckets in this pack without
   inventing new ambiguous categories
5. operators can execute preflight, pilot operation, and bounded diagnosis from
   written artifacts rather than tribal knowledge
6. pilot findings do not require silently widening the supported matrix

Pilot success validates the current bounded envelope only.
It does not create a broader product claim by itself.

## Incident Intake And Classification

Every pilot incident must record:

1. time, node set, workload, and surface involved
2. observed symptom
3. affected bounded claim, exclusion, or blocker
4. diagnosis evidence used
5. immediate operator action taken
6. final classification

Allowed classification buckets:

1. `config / environment issue`
   - the product behaved inside the bounded claim, but the deployment violated the
     pilot preflight or environment assumptions
2. `known exclusion`
   - the incident came from a surface or claim already excluded from the first
     launch matrix
3. `true product bug`
   - the incident contradicts an accepted bounded claim or reveals a real gap
     inside the named chosen envelope

If an incident does not fit one of those buckets, stop the pilot and refine the
artifact set before continuing.

## Decision Outputs

At the end of a bounded pilot window, the allowed outcomes are:

1. `stay in pilot`
   - more evidence is needed inside the same envelope
2. `widen within the same envelope`
   - the rollout review may expand exposure, but only without changing the named
     supported matrix
3. `block expansion`
   - a contradiction, repeated unresolved bug, or operational ambiguity prevents
     widening

These outcomes require a later controlled-rollout review artifact.
This pilot pack does not replace that review.

## Explicit Non-Claims

This pack does NOT claim:

1. generic production proof from limited pilot success
2. support for `RF>2`
3. support for a broad transport/frontend matrix
4. broad failover-under-load guarantees
5. hours/days soak proof outside the bounded chosen-path reading

## Primary Inputs

1. `sw-block/design/v2-first-launch-supported-matrix.md`
2. `sw-block/.private/phase/phase-17.md`
3. `sw-block/.private/phase/phase-17-checkpoint-review.md`
4. `sw-block/.private/phase/phase-12-p4-rollout-gates.md`
5. `sw-block/design/v2-product-completion-overview.md`
