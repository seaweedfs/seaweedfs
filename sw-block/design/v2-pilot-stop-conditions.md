# V2 Pilot Stop Conditions

Date: 2026-04-05
Status: draft
Purpose: define when the bounded internal pilot must stop, contain scope, or roll
back exposure

## Reading Rule

This artifact is about pilot containment, not protocol/data rollback semantics.

`Rollback` here means:

1. stop widening pilot exposure
2. reduce or remove pilot usage if needed
3. return to a previously accepted bounded state of operation

It does NOT mean:

1. a general storage/data rollback guarantee
2. permission to claim a broader recovery contract than the current evidence
3. ad hoc operator improvisation under ambiguity

## Immediate Stop Conditions

Stop the pilot immediately if ANY of the following occurs:

1. an observed behavior contradicts the bounded `17B` failover/publication
   contract
2. an observed behavior contradicts the bounded `17C` disturbance policy table
3. publication/lookup truth and registry truth diverge outside the currently
   allowed bounded interpretation window
4. diagnosis surfaces are insufficient to classify the incident without guessing
5. a `Phase 12 P4` floor-gate expectation is materially violated on the mapped
   chosen-path workload
6. the deployment is being widened beyond the named supported matrix without an
   explicit review decision
7. the incident does not fit the allowed buckets:
   - `config / environment issue`
   - `known exclusion`
   - `true product bug`

## Stop-And-Contain Actions

When a stop condition fires:

1. freeze new pilot expansion immediately
2. preserve the evidence needed for later review
3. classify the incident explicitly
4. map the incident back to:
   - accepted bounded claim
   - known exclusion
   - unresolved blocker
5. decide whether the pilot can continue in reduced scope or must fully pause

If the team cannot perform those actions clearly, the pilot remains stopped.

## Rollback Decision Rules

Use the following bounded rules:

1. `config / environment issue`
   - fix the environment/configuration
   - rerun preflight before resuming
2. `known exclusion`
   - remove the excluded usage from the pilot
   - do not reinterpret it as product support
3. `true product bug`
   - pause affected pilot scope
   - open an explicit fix or contradiction item before resuming

If repeated incidents of the same class continue without a bounded corrective path,
block further pilot expansion.

## Expansion Blockers

Even if the pilot remains partially runnable, do NOT widen it when:

1. the same unresolved true product bug recurs
2. operators depend on tribal knowledge to recover or diagnose
3. incident records are vague or cannot be mapped back to the current evidence
   ladder
4. success depends on ignoring explicit exclusions
5. the desired next step requires broader launch claims than the current matrix

## Explicit Non-Claims

This artifact does NOT claim:

1. broad rollout approval
2. generic production readiness from pilot survival
3. support for `RF>2`
4. support for a broad transport/frontend matrix
5. failover-under-load proof or long-window soak proof beyond the current bounded
   evidence set

## Primary Inputs

1. `sw-block/design/v2-bounded-internal-pilot-pack.md`
2. `sw-block/design/v2-pilot-preflight-checklist.md`
3. `sw-block/design/v2-first-launch-supported-matrix.md`
4. `sw-block/.private/phase/phase-17.md`
5. `sw-block/.private/phase/phase-12-p4-rollout-gates.md`
