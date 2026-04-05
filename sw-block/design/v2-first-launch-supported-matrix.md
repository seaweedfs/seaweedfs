# V2 First-Launch Supported Matrix

Date: 2026-04-04
Status: draft
Purpose: freeze the first bounded launch envelope from accepted `Phase 12-17`
evidence, with explicit supported scope, explicit exclusions, and explicit
launch blockers

## Reading Rule

This document is a bounded support matrix.

It does NOT mean:

1. broad launch approval
2. generic production readiness
3. support for every failover/restart/disturbance branch
4. broad transport/frontend approval outside the named chosen path

It means only:

1. these are the strongest support statements currently justified by accepted
   evidence
2. anything outside them is an exclusion, a blocker, or future
   productionization work

## Supported Matrix

| Dimension | Supported in first draft | Boundary rule | Primary evidence |
|-----------|--------------------------|---------------|------------------|
| Replication factor | `RF=2` | bounded chosen path only | `CP13-1..7`, `C-RF2-SYNCALL-CONTRACT` |
| Durability mode | `sync_all` | bounded chosen path only | `CP13`, `v2-protocol-claim-and-evidence.md` |
| Control/runtime path | existing master / volume-server heartbeat path | same path as `Phase 10`, `Phase 16`, `Phase 17` checkpoints | `Phase 10`, `Phase 16` finish-line review |
| Semantic owner | explicit `V2 core` | semantics stay `V2`-owned even when implementation reuses `weed/` and `blockvol` | `Phase 14-16` |
| Execution backend | `blockvol` via `v2bridge` | reuse implementation; no V1 semantic inheritance | `Phase 09`, `Phase 14-16` |
| Product surfaces | bounded `iSCSI`, bounded `CSI`, bounded `NVMe` on the chosen path | not a generic transport matrix | `Phase 11`, publication tests, NVMe tests |
| Restart/failover reading | bounded `17B` + `17C` interpretation | use only the explicit contract and policy table from `Phase 17` | `phase-17.md`, `phase-17-checkpoint-review.md` |

## Supported Statement

The strongest currently supported first-launch statement is:

1. on the bounded chosen `RF=2 sync_all` path, using the existing
   master/volume-server heartbeat path, explicit `V2`-owned semantics and the
   accepted `Phase 16-17` contract/policy package provide one finite support
   envelope for bounded block product use
2. bounded `iSCSI`, `CSI`, and `NVMe` surfaces are supported only inside that
   same chosen-path interpretation
3. failover/publication and disturbance behavior must be read through the
   explicit `Phase 17` contract/policy package, not through broader inferred
   product assumptions

## Explicit Exclusions

The following are OUTSIDE the first-launch supported matrix:

1. `RF>2`
2. durability modes outside the accepted bounded envelope
3. broad transport/frontend matrix approval beyond bounded `iSCSI` / `CSI` /
   `NVMe` chosen-path support
4. broad whole-surface failover/publication proof
5. broad restart-window behavior outside the explicit `17C` disturbance policy
   table
6. generic soak/pilot success as production proof
7. broad rollout approval

## Launch Blockers

These are still required before this matrix can be read as a real launch
decision package:

1. a frozen `Phase 17` checkpoint review outcome
2. any additional evidence needed if the product wants claims broader than the
   current bounded `17B/17C` contract/policy package

## Current Productionization Artifacts

The first bounded productionization artifacts now exist for the chosen path:

1. `v2-bounded-internal-pilot-pack.md`
   - bounded pilot scope
   - success criteria
   - incident classification
2. `v2-pilot-preflight-checklist.md`
   - start/resume gate for the bounded pilot
3. `v2-pilot-stop-conditions.md`
   - stop/contain/rollback-exposure rules
4. `v2-controlled-rollout-review.md`
   - bounded post-pilot decision gate
   - allowed outcomes: stay in pilot / widen within same envelope / block expansion

## Not Launch-Blocking In This Draft

These are intentionally NOT blockers for the bounded first-draft envelope:

1. lack of `RF>2` support
2. lack of broad transport/frontend approval
3. lack of broad launch approval language
4. lack of generic soak proof inside the phase package itself

## Claim Mapping Rule

Any first-launch support claim must map back to accepted evidence in ALL of the
following layers:

1. hardening/floor layer
   - `Phase 12 P4`
2. contract/workload/mode layer
   - `CP13-1..9`
3. runtime truth-closure layer
   - `Phase 16` finish-line checkpoint
4. product-claim checkpoint layer
   - `Phase 17A-17D`

If a claim cannot map cleanly back through those layers:

1. it is not in the first-launch matrix
2. it belongs in exclusions, blockers, or later productionization work

## Operator Reading Guide

When using this matrix, read it with these constraints:

1. bounded chosen path first, not generic platform promise
2. explicit exclusions are real product boundaries, not temporary omissions
3. launch blockers are real blockers, not optional polish
4. pilot success later may validate this envelope, but cannot redefine it

## Primary References

1. `sw-block/.private/phase/phase-17.md`
2. `sw-block/.private/phase/phase-17-checkpoint-review.md`
3. `sw-block/design/v2-product-completion-overview.md`
4. `sw-block/design/v2-protocol-claim-and-evidence.md`
5. `sw-block/design/v2-bounded-internal-pilot-pack.md`
6. `sw-block/design/v2-pilot-preflight-checklist.md`
7. `sw-block/design/v2-pilot-stop-conditions.md`
8. `sw-block/design/v2-controlled-rollout-review.md`
