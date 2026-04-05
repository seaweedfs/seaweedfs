# V2 Pilot Stop Conditions

Date: 2026-04-05
Status: draft
Purpose: define when bounded internal engineering validation on the current
`Phase 18` RF2 runtime envelope must stop, contain scope, or block expansion

## Reading Rule

This artifact is about validation containment, not protocol/data rollback
semantics.

`Rollback` here means:

1. stop widening validation exposure
2. reduce or remove validation usage if needed
3. return to a previously accepted bounded state of operation

It does NOT mean:

1. a general storage/data rollback guarantee
2. permission to claim a broader recovery contract than the current evidence
3. ad hoc operator improvisation under ambiguity

## Immediate Stop Conditions

Stop validation immediately if ANY of the following occurs:

1. an observed behavior contradicts the bounded `Phase 18` RF2 runtime envelope
2. any run is interpreted as proving automatic failover, continuous Loop 2
   service, rebuild lifecycle, or frontend-serving behavior that is still
   explicitly excluded
3. diagnosis surfaces are insufficient to classify the incident without guessing
4. the validation is being widened beyond the named bounded envelope without an
   explicit review decision
5. the incident does not fit the allowed buckets:
   - `config / environment issue`
   - `known exclusion`
   - `true product bug`

## Stop-And-Contain Actions

When a stop condition fires:

1. freeze new validation expansion immediately
2. preserve the evidence needed for later review
3. classify the incident explicitly
4. map the incident back to:
   - accepted bounded claim
   - known exclusion
   - unresolved blocker
5. decide whether validation can continue in reduced scope or must fully pause

If the team cannot perform those actions clearly, validation remains stopped.

## Rollback Decision Rules

Use the following bounded rules:

1. `config / environment issue`
   - fix the environment/configuration
   - rerun preflight before resuming
2. `known exclusion`
   - remove the excluded usage from validation
   - do not reinterpret it as product support
3. `true product bug`
   - pause affected validation scope
   - open an explicit fix or contradiction item before resuming

If repeated incidents of the same class continue without a bounded corrective
path, block further validation expansion.

## Expansion Blockers

Even if validation remains partially runnable, do NOT widen it when:

1. the same unresolved true product bug recurs
2. operators depend on tribal knowledge to recover or diagnose
3. incident records are vague or cannot be mapped back to the current evidence
   ladder
4. success depends on ignoring explicit exclusions
5. the desired next step requires broader launch claims than the current envelope

## Explicit Non-Claims

This artifact does NOT claim:

1. broad rollout approval
2. generic production readiness from validation survival
3. support for `RF>2`
4. support for a broad transport/frontend matrix
5. failover-under-load proof or long-window soak proof beyond the current bounded
   evidence set

## Primary Inputs

1. `sw-block/design/v2-bounded-internal-pilot-pack.md`
2. `sw-block/design/v2-pilot-preflight-checklist.md`
3. `sw-block/design/v2-rf2-runtime-bounded-envelope.md`
4. `sw-block/design/v2-rf2-runtime-bounded-envelope-review.md`
5. `sw-block/.private/phase/phase-18.md`
