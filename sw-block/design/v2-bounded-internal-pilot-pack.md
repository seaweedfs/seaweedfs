# V2 Bounded Internal Pilot Pack

Date: 2026-04-05
Status: draft
Purpose: define the bounded internal engineering validation pack around the
current `Phase 18` RF2 runtime-bearing envelope without silently broadening scope

## Reading Rule

This pilot pack is a bounded validation package for the current RF2
runtime-bearing envelope.

It does NOT mean:

1. broad launch approval
2. generic production readiness
3. proof that the current runtime path is already a working block product
4. permission to redefine exclusions through pilot success

It means only:

1. the team may run limited internal engineering validation inside the accepted
   runtime-bearing envelope
2. validation outcomes must be read against the delivered `Phase 18` boundary
3. incidents must be routed explicitly instead of becoming vague rollout lore

## Pilot Scope

This pack is limited to the current `Phase 18` runtime-bearing envelope:

1. kernel/runtime path:
   - `masterv2` identity authority
   - `volumev2` runtime-owned failover / Loop 2 / continuity / RF2 surface path
   - `purev2` execution adapter reuse
2. validation shape:
   - bounded in-process runtime exercises
   - artifact-driven review only
3. supported proof shape:
   - failover-time evidence seam
   - active Loop 2 observation
   - continuity handoff statement
   - compressed RF2 outward surface
4. excluded surface classes:
   - real product frontends
   - broad operator APIs
   - real transport-backed product traffic

Anything outside that scope is not a finding for this pack.
It is either a known exclusion, an explicit blocker, or later widening work.

## Pilot Environment And Topology

The validation environment must stay fixed and reviewable:

1. use one explicit build/commit package for all pilot nodes
2. keep topology inside the bounded `RF=2` runtime-bearing path and do not
   introduce `RF>2`
3. do not introduce real frontend/product traffic or broad transport claims
4. pin operator-facing configuration and startup procedure in a written runbook
5. expose the runtime diagnosis surfaces needed to read:
   - failover snapshot/result
   - Loop 2 snapshot
   - continuity snapshot
   - RF2 outward surface

If the validation needs ad hoc operator judgment to stay healthy, the pack is not
ready.

## Success Criteria

The bounded validation is considered successful only if ALL of the following
hold:

1. no observed behavior contradicts the bounded `Phase 18` runtime envelope
2. no observed behavior contradicts the bounded fail-closed reading of the new
   runtime path
3. incidents can be classified using the explicit buckets in this pack without
   inventing new ambiguous categories
4. operators can execute preflight, bounded validation, and diagnosis from
   written artifacts rather than tribal knowledge
5. findings do not require silently widening the supported envelope
6. the review outcome remains consistent with the current `block expansion /
   not pilot-ready` judgment unless new closure explicitly changes it

Validation success validates the current bounded envelope only.
It does not create a broader product claim by itself.

## Incident Intake And Classification

Every incident must record:

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

If an incident does not fit one of those buckets, stop the validation and refine
the
artifact set before continuing.

## Decision Outputs

At the end of a bounded validation window, the allowed outcomes are:

1. `stay in bounded validation`
   - more evidence is needed inside the same envelope
2. `widen bounded engineering exposure`
   - the review may expand only internal engineering validation inside the same
     envelope
3. `block expansion`
   - a contradiction, repeated unresolved bug, or operational ambiguity prevents
     widening

These outcomes require the bounded envelope review artifact.
This pack does not replace that review.

## Explicit Non-Claims

This pack does NOT claim:

1. generic production proof from limited validation success
2. support for `RF>2`
3. support for a broad transport/frontend matrix
4. broad automatic failover guarantees
5. hours/days soak proof outside the bounded runtime-bearing reading

## Primary Inputs

1. `sw-block/design/v2-rf2-runtime-bounded-envelope.md`
2. `sw-block/design/v2-rf2-runtime-bounded-envelope-review.md`
3. `sw-block/.private/phase/phase-18.md`
4. `sw-block/design/v2-protocol-claim-and-evidence.md`
5. `sw-block/design/v2-product-completion-overview.md`
