# V2 Pilot Preflight Checklist

Date: 2026-04-05
Status: draft
Purpose: define the minimum explicit checks required before running bounded
internal engineering validation on the current `Phase 18` RF2 runtime envelope

## Reading Rule

This checklist is a gate for starting or resuming bounded internal engineering
validation.

If any item below is not satisfied:

1. do not treat the environment as pilot-ready
2. either fix the issue or classify it explicitly before proceeding

## Scope Lock

Confirm the validation is still inside the frozen `Phase 18` RF2 runtime
envelope:

1. topology remains bounded `RF=2`
2. runtime path remains the delivered `masterv2 + volumev2 + purev2` `M1-M4`
   path
3. validation stays in bounded runtime/lab exercises only
4. no one is trying to use this validation to claim working block product status
5. no real frontend/product traffic is being introduced on the new runtime path
6. no one is trying to use validation success to claim broader launch approval

## Build And Artifact Pin

Confirm the software package is explicit and stable:

1. the exact build/commit for validation nodes is written down
2. all validation nodes run the same intended package
3. the operator runbook matches the package actually deployed
4. any configuration delta from the documented chosen path is reviewed and
   accepted explicitly

## Environment Readiness

Confirm the validation environment matches bounded assumptions:

1. node inventory and topology are written down
2. transport/frontend choice does not widen beyond the bounded runtime envelope
3. storage/network assumptions required by the chosen path are known to the
   operator
4. known exclusions are acknowledged before start
5. rollback/containment ownership is assigned for the validation window

## Diagnosis Surface Readiness

Confirm bounded diagnosis can be performed without ad hoc spelunking:

1. failover snapshots/results can be inspected
2. Loop 2 snapshots can be inspected
3. continuity snapshots can be inspected
4. RF2 runtime surface can be inspected
5. the operator knows which artifact defines the current contract/policy boundary:
   - `v2-rf2-runtime-bounded-envelope.md`
   - `v2-rf2-runtime-bounded-envelope-review.md`
   - this preflight checklist
   - the stop-condition artifact

## Workload And Gate Alignment

Confirm the validation workload is aligned with accepted evidence:

1. the workload maps to the bounded runtime-bearing reading rather than a new
   unsupported scenario
2. success will be judged against the validation-pack criteria rather than generic
   "looks stable" judgment
3. the workload does not assume continuous Loop 2, real transport, auto failover,
   rebuild lifecycle, or product frontends that are still excluded
4. no required proof depends on failover-under-load, hours/days soak, `RF>2`, or
   broad transport/frontend claims that are still excluded

## Incident Routing Readiness

Confirm incident handling is explicit before starting:

1. every incident will be classified as one of:
   - `config / environment issue`
   - `known exclusion`
   - `true product bug`
2. the recording location for incidents is agreed before validation starts
3. ownership for triage and decision-making is assigned
4. operators know when they must stop instead of improvising

## Preflight Result

Validation may start only if:

1. every scope-lock item is true
2. the software package and environment are pinned
3. diagnosis surfaces are available
4. incident routing is explicit
5. no remaining gap is being hand-waved as "we will figure it out during pilot"

If those conditions are not met, the correct output is:

1. `NOT READY`
2. the missing item(s)
3. the owner/action needed before retry

## Primary Inputs

1. `sw-block/design/v2-bounded-internal-pilot-pack.md`
2. `sw-block/design/v2-rf2-runtime-bounded-envelope.md`
3. `sw-block/design/v2-rf2-runtime-bounded-envelope-review.md`
4. `sw-block/.private/phase/phase-18.md`
