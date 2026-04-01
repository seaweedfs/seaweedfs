# Phase 08 Log

## 2026-03-31

### Opened

`Phase 08` opened as:

- pre-production hardening

### Starting basis

1. `Phase 07`: complete
2. first V2 product path chosen
3. remaining gaps are integration and hardening gaps, not protocol-discovery gaps

### Next

1. Phase 08 P0 accepted
2. Phase 08 P1 accepted
3. Phase 08 P2 accepted
4. Phase 08 P3 hardening validation on the unified live path
5. Phase 08 P4 candidate package closure accepted
6. Phase 08 closeout bookkeeping complete
7. next: open Phase 09 P0 for production execution closure planning

### P3 Technical Pack

Purpose:

- provide the minimum design/algo/test detail needed to execute `P3`
- reuse accepted `P1` / `P2` live-path closure
- avoid broad scenario growth or repeated proof of already accepted mechanics

#### Design / algo focus

`P3` is not another execution-closure slice.
It assumes these are already accepted on the chosen path:

- real control delivery
- real catch-up one-chain closure
- real rebuild one-chain closure

What `P3` adds is hardening evidence on top of that live path:

1. replay accepted failure classes again on the unified path
2. prove one real failover / reassignment cycle
3. prove one overlapping retention/pinner safety case
4. produce one explicit committed-truth gate decision

Key algorithm rules for `P3`:

- control truth remains primary:
  - failover / reassignment is driven by new assignment / epoch truth
  - storage/runtime must not invent role changes
- recovery choice remains engine-owned:
  - engine chooses `zero_gap` / `catchup` / `needs_rebuild`
  - bridge and `blockvol` execute what the engine already decided
- overlapping recovery must remain fail-closed:
  - retained floor = minimum active retention requirement
  - stale or cancelled plan must release its hold
  - a new authoritative plan must not inherit leaked resources from an old one
- committed-truth gate must be output, not discussed informally:
  - either the chosen candidate path is accepted with current committed/checkpoint semantics
  - or the next phase is blocked on further separation/bounding work

#### Validation matrix

Use one compact replay matrix rather than many near-duplicate tests.

1. Changed-address restart
   - trigger: address refresh / reassignment while prior identity is preserved
   - expected: old session invalidated, same logical `ReplicaID`, new recovery starts cleanly
   - assert:
     - no stale session mutation
     - no leaked pins
     - logs show why identity stayed and session changed

2. Stale epoch / stale session
   - trigger: epoch bump during or before recovery continuation
   - expected: stale execution loses authority immediately
   - assert:
     - old session cannot mutate
     - replacement assignment/session becomes the only live authority
     - logs show invalidation reason

3. Unrecoverable gap / needs-rebuild
   - trigger: replica falls behind retained WAL
   - expected: engine chooses `needs_rebuild`, rebuild path executes or is prepared according to accepted boundary
   - assert:
     - no catch-up overclaim
     - correct rebuild source/result logged
     - no leaked pins after completion/failure

4. Post-checkpoint boundary behavior
   - trigger: replica state around checkpoint / committed boundary
   - expected: classification and execution match the chosen candidate-path semantics
   - assert:
     - chosen path does not overclaim beyond the accepted boundary
     - committed/checkpoint truth used here matches the explicit gate decision

#### Required extra cases

Besides the replay matrix, `P3` should add only two new validation cases:

1. One real failover / promotion / reassignment cycle
   - primary change or reassignment through the live control path
   - verify old authority dies, new authority starts, recovery resumes/starts correctly

2. One true simultaneous-overlap retention/pinner case
   - two live recovery holds coexist before the earlier one is released
   - verify:
     - minimum retention floor is respected while both are live
     - releasing one hold leaves the other hold still contributing the correct floor
     - released/cancelled plan stops contributing to retention floor
     - final hold count returns to zero

#### Expected evidence

For each accepted `P3` case, prefer explicit evidence blocks:

- entry truth:
  - assignment / epoch / role that started the case
- engine result:
  - selected outcome or invalidation result
- execution result:
  - completion / cancel / failure
- cleanup result:
  - `ActiveHoldCount() == 0`
  - no surviving active session when case should be closed
- observability result:
  - logs explain:
    - why control truth changed
    - why session changed
    - why catch-up vs rebuild happened
    - why execution completed / failed / cancelled

#### Efficient test plan

Keep `P3` small and high-signal:

- one unified replay test package or compact matrix
- one real failover-cycle test
- one overlapping-retention test
- one explicit gate-decision record in delivery / phase status

Avoid:

- re-proving isolated `P2` one-chain mechanics
- broad combinatorial growth across many replicas / roles / timing permutations
- turning `P3` into another protocol-design slice

### P4 Technical Pack

Purpose:

- provide the minimum design/algo/test detail needed to close `Phase 08`
- convert accepted `P1` / `P2` / `P3` evidence into one candidate-path judgment
- keep `P4` as a closure slice, not another broad engineering slice

#### Delivery sequence

Use this order:

1. `sw` develops the candidate package
2. `architect` reviews code/claim shape before tester time is spent
3. `tester` validates the evidence-to-claim mapping
4. `manager` records the final phase/accounting decision

Do not collapse these roles:

- `sw` builds the candidate statement and supporting artifacts
- `architect` checks whether the resulting package has obvious semantic, scope, or evidence-shape problems before tester validation
- `tester` checks whether every claim is actually supported
- `manager` decides acceptance/bookkeeping after architect + tester feedback

Recommended handoff gate before tester:

- if architect finds obvious overclaim, missing evidence mapping, or broken candidate shape, return to `sw` first
- do not spend tester time on a package that is clearly not ready

#### Design / algo focus

`P4` should not introduce new protocol shape.
It consumes already accepted results:

- `P1`: real control delivery
- `P2`: real execution closure
- `P3`: unified hardening validation

The main design task is to classify the chosen path into three buckets:

1. candidate-safe
   - supported by accepted evidence
   - allowed to appear in the candidate statement
2. intentionally bounded
   - accepted only within narrow limits
   - must appear as explicit candidate bounds
3. deferred or blocking
   - not yet supported enough
   - must not be implied as candidate-ready

Algorithmically, `P4` is a classification/output slice:

- no new recovery FSM
- no new identity model
- no new rebuild policy
- no new durability model

It should only:

- map accepted evidence to accepted candidate claims
- map residual limitations to explicit bounds or blockers
- separate candidate readiness from production readiness

#### Required output artifacts

`sw` should produce exactly these artifacts:

1. Candidate statement
   - what the chosen `RF=2 sync_all` path is allowed to claim

2. Evidence-to-claim map
   - each candidate claim points to accepted evidence from `P1` / `P2` / `P3`

3. Bound list
   - explicit candidate-safe bounds, for example:
     - chosen path only
     - chosen durability mode only
     - accepted rebuild coverage only

4. Deferred / blocking list
   - what remains outside the candidate path
   - what still blocks production readiness

#### Candidate statement shape

Keep the candidate statement short and structured.
It should answer only:

1. What path is the candidate?
2. What is proven for that path?
3. What is intentionally bounded for that path?
4. What is still deferred or blocking?

Good pattern:

- candidate path:
  - `RF=2 sync_all` on the accepted master/heartbeat control path
- proven:
  - real control delivery
  - real catch-up closure
  - real rebuild closure for accepted coverage
  - unified replay and failover validation
- bounded:
  - only the chosen path / mode
  - only accepted rebuild/source coverage
- not yet claimed:
  - general future path/mode truth
  - production readiness

#### Candidate statement template

Use this exact structure for the `P4` delivery statement:

1. Candidate path
   - The first candidate path is:
     - `<path / topology / durability mode>`

2. Candidate-safe claims
   - The candidate path is supported for:
     - `<claim 1>` — evidence: `<P1/P2/P3 reference>`
     - `<claim 2>` — evidence: `<P1/P2/P3 reference>`
     - `<claim 3>` — evidence: `<P1/P2/P3 reference>`

3. Explicit bounds
   - This candidate statement is intentionally bounded to:
     - `<bound 1>`
     - `<bound 2>`
     - `<bound 3>`

4. Deferred or blocking items
   - Not yet claimed as candidate-safe:
     - `<deferred item 1>`
     - `<deferred item 2>`
   - Still blocking production readiness:
     - `<blocker 1>`
     - `<blocker 2>`

5. Committed-truth decision
   - For this candidate path:
     - `<committed-truth decision>`
   - Scope:
     - `<why this does not automatically generalize>`

6. Overall judgment
   - Judgment:
     - `<candidate-safe / candidate-safe-with-bounds / not-yet-candidate>`
   - Reason:
     - `<one short paragraph tying evidence to judgment>`

When `sw` fills this template:

- every positive claim must carry an evidence reference
- every important missing area must appear either under:
  - explicit bounds
  - deferred
  - blockers
- avoid prose that mixes candidate judgment with production-readiness language

#### Assignment template

Use this template when assigning `P4` work to `sw`:

1. Goal
   - Build the `P4` candidate package for the chosen path.

2. Required outputs
   - candidate statement
   - evidence-to-claim mapping
   - explicit bounds list
   - deferred / blocking list
   - committed-truth decision statement

3. Hard rules
   - no new protocol redesign
   - no broad scope growth without candidate impact
   - every positive claim must map to accepted `P1` / `P2` / `P3` evidence
   - do not mix candidate readiness with production readiness

4. Delivery order
   - first hand to architect review
   - only after architect review passes, hand to tester validation
   - manager records final acceptance/bookkeeping last

5. Reject before handoff if
   - evidence-to-claim mapping is incomplete
   - important limitations are not classified as bounded / deferred / blocking
   - claims exceed accepted evidence

Use this template when assigning `P4` validation to `tester`:

1. Goal
   - Validate that the candidate package is fully supported by accepted evidence.

2. Validate
   - each claim has accepted evidence
   - each bound/deferred/blocker is explicit
   - committed-truth decision stays scoped correctly
   - no candidate-to-production overclaim exists

3. Output
   - pass/fail on each candidate claim group
   - findings on unsupported claims, missing bounds, or hidden blockers

#### Tester validation checklist

`tester` should validate:

1. every positive candidate claim has accepted evidence
2. every important limitation appears in either:
   - bounded
   - deferred
   - blocking
3. no accepted evidence is stretched into a broader product claim
4. committed-truth decision stays scoped to the chosen candidate path
5. candidate readiness is not confused with production readiness

#### Architect review focus

`architect` should review only:

1. semantic correctness of the candidate statement
2. whether the evidence-to-claim mapping is honest
3. whether bounds are explicit enough to prevent future drift
4. whether any hidden overclaim remains

This review should not reopen already accepted `P1` / `P2` / `P3` mechanics unless the candidate statement contradicts them.

#### Efficient test / evidence plan

`P4` should mostly reuse accepted evidence rather than add new broad tests.

Preferred work:

- collect accepted evidence references
- compress them into candidate-safe claims
- write one explicit residual-gap list

Only add new code/tests if a small missing blocker prevents a coherent candidate statement.

Avoid:

- large new replay matrices
- new protocol experiments
- broad implementation growth without candidate impact

### Closeout bookkeeping

Manager follow-up after `P4` acceptance found only a minor bookkeeping concern:

- ensure `phase-08.md` is explicitly closed before treating `Phase 09` as opened

Closeout check:

1. `phase-08.md` is `Status: complete`
2. `P4` is recorded as accepted
3. `Phase-close note` points to `Phase 09: Production Execution Closure`
4. `phase-08-decisions.md` records `Decision 16`

Final bookkeeping judgment:

- `Phase 08` is closed
- `Phase 09 P0` is the active next planning/engineering package
