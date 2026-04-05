# V2 Controlled Rollout Review

Date: 2026-04-05
Status: draft
Purpose: define the bounded review used to decide whether internal engineering
validation on the current `Phase 18` RF2 runtime envelope stays limited, widens
inside the same envelope, or blocks expansion

## Reading Rule

This artifact is a bounded decision gate after runtime-envelope validation.

It does NOT mean:

1. broad launch approval
2. generic production readiness
3. permission to widen beyond the frozen runtime envelope
4. permission to reinterpret validation survival as new protocol/runtime proof

It means only:

1. validation outcomes may be reviewed against the already-accepted bounded
   envelope
2. expansion decisions must stay inside the same named support boundary
3. any broader claim still needs explicit new evidence and explicit new review

## Allowed Decisions

The rollout review may produce only one of these outputs:

1. `stay in bounded validation`
   - the chosen envelope is still the right boundary, but more bounded validation
     evidence is needed before any exposure increase
2. `widen bounded engineering exposure`
   - exposure may increase only inside the current bounded engineering envelope,
     with no change to the named support boundary
3. `block expansion`
   - the current evidence, incident record, or operational ambiguity is not strong
     enough to increase exposure safely

Any outcome outside those three is invalid for this review.

## Required Inputs

The review must not start unless these inputs exist and are explicit:

1. the frozen runtime envelope
2. the bounded pilot pack
3. the preflight checklist outcome(s)
4. the pilot stop-condition artifact
5. incident records with explicit classification
6. validation outcome summary for the bounded runtime-bearing path
7. the accepted evidence anchors that define the current boundary:
   - `Phase 18 M1-M4`
   - `v2-rf2-runtime-bounded-envelope.md`
   - `v2-rf2-runtime-bounded-envelope-review.md`

If any required input is missing, the correct review output is `block expansion`.

## Decision Questions

The rollout review must answer all of the following:

1. did validation remain fully inside the frozen runtime envelope
2. did any observed behavior contradict the bounded `Phase 18` runtime envelope
3. were any stop conditions triggered, and if so, how were they resolved
4. are all incidents classified cleanly as:
   - `config / environment issue`
   - `known exclusion`
   - `true product bug`
5. does any proposed next step depend on a broader claim than the current
   envelope
6. can operators run the validation and diagnose bounded failures from written
   artifacts rather than tribal knowledge

If the answer to question 5 is yes, the review must not approve widening inside
this artifact. That request belongs to later evidence expansion work.

## Decision Rules

Use these bounded rules:

1. approve `stay in bounded validation` when:
   - validation stayed inside scope
   - no contradiction to accepted bounded claims was found
   - more same-envelope evidence is still needed
2. approve `widen bounded engineering exposure` only when:
   - validation stayed inside scope
   - no unresolved `true product bug` remains against the bounded envelope
   - stop conditions did not reveal structural ambiguity
   - operator workflow is explicit and repeatable from the artifact set
   - the widened exposure does not change the bounded envelope
3. approve `block expansion` when:
   - any unresolved contradiction exists
   - any unresolved `true product bug` exists
   - incident records are vague
   - operators depend on tribal knowledge
   - the requested widening outruns the current matrix

## Explicit Review Record

Each review result must record:

1. decision outcome
2. date and reviewer set
3. validation window / environment covered
4. summary of incidents by classification bucket
5. any stop-condition events and their disposition
6. exact reason the decision stays inside the current envelope
7. explicit next action:
   - continue bounded validation
   - widen engineering exposure inside the same envelope
   - pause and fix

## Rejection Rules

Reject the review as invalid if:

1. it uses validation success as generic production proof
2. it broadens topology, runtime path, or supported surfaces without a new
   evidence package
3. it treats a known exclusion as if validation cleared it
4. it ignores stop-condition events or unresolved true product bugs
5. it cannot map the decision back to the accepted evidence ladder

## Explicit Non-Claims

This artifact does NOT claim:

1. broad rollout approval
2. generic production readiness
3. support for `RF>2`
4. support for a broad transport/frontend matrix
5. broad failover-under-load or long-window soak proof

## Primary Inputs

1. `sw-block/design/v2-rf2-runtime-bounded-envelope.md`
2. `sw-block/design/v2-rf2-runtime-bounded-envelope-review.md`
3. `sw-block/design/v2-bounded-internal-pilot-pack.md`
4. `sw-block/design/v2-pilot-preflight-checklist.md`
5. `sw-block/design/v2-pilot-stop-conditions.md`
6. `sw-block/.private/phase/phase-18.md`
