# V2 Pilot Preflight Checklist

Date: 2026-04-05
Status: draft
Purpose: define the minimum explicit checks required before running the bounded
internal pilot

## Reading Rule

This checklist is a gate for starting or resuming the bounded pilot.

If any item below is not satisfied:

1. do not treat the environment as pilot-ready
2. either fix the issue or classify it explicitly before proceeding

## Scope Lock

Confirm the pilot is still inside the frozen first-launch envelope:

1. topology remains `RF=2`
2. durability mode remains `sync_all`
3. control/runtime path remains the existing master / volume-server heartbeat path
4. execution backend remains `blockvol` through `v2bridge`
5. product surface remains within bounded `iSCSI`, bounded `CSI`, or bounded
   `NVMe`
6. no one is trying to use pilot success to claim broader launch approval

## Build And Artifact Pin

Confirm the software package is explicit and stable:

1. the exact build/commit for pilot nodes is written down
2. all pilot nodes run the same intended package
3. the operator runbook matches the package actually deployed
4. any configuration delta from the documented chosen path is reviewed and
   accepted explicitly

## Environment Readiness

Confirm the pilot environment matches bounded assumptions:

1. node inventory and topology are written down
2. transport/frontend choice is inside the supported matrix
3. storage/network assumptions required by the chosen path are known to the
   operator
4. known exclusions are acknowledged before start
5. rollback/containment ownership is assigned for the pilot window

## Diagnosis Surface Readiness

Confirm bounded diagnosis can be performed without ad hoc spelunking:

1. registry truth can be inspected
2. publication/lookup truth can be inspected
3. restart/failover progression can be inspected
4. inventory-authority state can be inspected
5. the operator knows which artifact defines the current contract/policy boundary:
   - `v2-first-launch-supported-matrix.md`
   - `phase-17.md`
   - this preflight checklist
   - the stop-condition artifact

## Workload And Gate Alignment

Confirm the pilot workload is aligned with accepted evidence:

1. the workload maps to the bounded chosen-path reading rather than a new
   unsupported scenario
2. success will be judged against the pilot pack criteria rather than generic
   "looks stable" judgment
3. `Phase 12 P4` floor-gate expectations are known for the workload being run
4. no required proof depends on failover-under-load, hours/days soak, `RF>2`, or
   broad transport claims that are still excluded

## Incident Routing Readiness

Confirm incident handling is explicit before starting:

1. every incident will be classified as one of:
   - `config / environment issue`
   - `known exclusion`
   - `true product bug`
2. the recording location for pilot incidents is agreed before the pilot starts
3. ownership for triage and decision-making is assigned
4. operators know when they must stop instead of improvising

## Preflight Result

The pilot may start only if:

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
2. `sw-block/design/v2-first-launch-supported-matrix.md`
3. `sw-block/.private/phase/phase-17.md`
4. `sw-block/.private/phase/phase-12-p4-rollout-gates.md`
