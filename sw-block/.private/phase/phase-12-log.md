# Phase 12 Log

Date: 2026-04-02
Status: active
Purpose: record the technical pack and hardening-plan details for moving from accepted chosen-path closure to production-safe behavior

---

### P0 Technical Pack

Date: 2026-04-02
Goal: convert `Phase 12` from a broad hardening label into a bounded execution plan with explicit slice order, hard indicators, and reject shapes

#### Step breakdown

##### Step 1: Hardening-object freeze

Goal:

- define exactly what `Phase 12` is trying to harden

Must make explicit:

1. which accepted surfaces are now considered the production-candidate chosen path
2. which kinds of failures/disturbances are first-order hardening targets
3. what remains out of scope for the first hardening slices

Accepted hardening object:

1. the accepted chosen path produced by `Phase 09` + `Phase 10` + `Phase 11`
2. including:
   - execution truth
   - control-plane truth
   - selected product-surface truth
3. excluding:
   - new product-surface expansion
   - clone/product-feature expansion
   - perf-first optimization work

Hard indicators:

1. one explicit statement exists that `Phase 12` hardens accepted chosen-path behavior rather than redefining it
2. first disturbance classes are written down explicitly
3. no active planning text still treats `Phase 12` as a vague catch-all

Reject if:

1. the hardening object is not named explicitly
2. planning mixes correctness hardening with unrelated feature expansion
3. accepted earlier semantics are casually reopened without a concrete exposed bug

##### Step 2: Slice order freeze

Goal:

- define the first hardening slices and why they are ordered that way

Recommended slice order:

1. `P1` restart / recovery disturbance hardening
2. `P2` soak / long-run stability hardening
3. `P3` diagnosability / blocker accounting
4. `P4` performance floor / rollout gates

Why this order:

1. restart/rejoin/failover disturbance threatens correctness first
2. soak matters after disturbance correctness is bounded
3. diagnosis/blocker accounting should mature alongside repeated disturbance evidence
4. performance claims should not outrun correctness hardening

Hard indicators:

1. one explicit ordered slice list exists in `phase-12.md`
2. at least one sentence explains why restart/disturbance comes before perf
3. `P1` / `P2` / `P3` / `P4` are distinguishable acceptance objects, not one blurred phase bucket

Reject if:

1. slice ordering is still ambiguous
2. perf or broad rollout work is treated as the first hardening slice
3. different hardening concerns are mixed into one unbounded mega-slice

##### Step 3: Evidence ladder freeze

Goal:

- define what counts as real hardening evidence for `Phase 12`

Required evidence classes:

1. disturbance correctness evidence
   - restart / failover / rejoin / repeated disturbance tests
2. long-run stability evidence
   - soak or repeated-cycle evidence
3. diagnosability evidence
   - observable symptoms, logs, or explicit blocker accounting
4. acceptance wording evidence
   - no-overclaim around production readiness before evidence exists

Hard indicators:

1. one explicit evidence ladder exists in `phase-12-log.md`
2. “supporting evidence” and “acceptance evidence” are distinguished
3. production-readiness wording is explicitly bounded

Reject if:

1. hardening acceptance is defined only by passing unit tests
2. soak, disturbance, and diagnosability evidence are not distinguished
3. production-readiness claims appear without an evidence ladder

#### Recommended first-cut implementation boundary

Prefer starting with correctness-hardening around the already accepted chosen path:

1. restart / failover / rejoin disturbance
2. repeated ownership/control transitions
3. heartbeat/recovery reconstruction under disturbance
4. operator-visible blocker accounting

Reference only unless hardening exposes a real mismatch:

1. accepted `Phase 09` execution semantics
2. accepted `Phase 10` control-plane closure
3. accepted `Phase 11` product-surface rebinding

#### Suggested first hardening target

`P1` should probably focus on restart / repeated disturbance correctness first, because that is the smallest hardening slice most likely to expose production-real bugs without requiring full soak infrastructure.

Candidate disturbance themes:

1. restart with same lineage
2. restart with changed address / refreshed publication
3. repeated failover / rejoin cycles
4. delayed or stale signal arrival after restart/failover

#### Assignment For `sw`

1. Goal
   - deliver `P0` as a real hardening-plan package for `Phase 12`

2. Required outputs
   - one bounded statement of the hardening object
   - one ordered hardening slice list with rationale
   - one evidence ladder distinguishing:
     - disturbance correctness
     - soak / long-run stability
     - diagnosability / blockers
     - performance / rollout gates
   - short reuse note:
     - which earlier accepted surfaces are now being hardened
     - which areas remain reference only

3. Hard rules
   - do not reopen accepted semantics casually
   - do not turn `P0` into feature expansion
   - do not let performance claims outrun correctness-hardening planning

#### Assignment For `tester`

1. Goal
   - validate that `P0` makes `Phase 12` executable rather than rhetorical

2. Validate
   - hardening object is explicit
   - slice order is explicit and justified
   - evidence ladder is explicit
   - no-overclaim around production readiness

3. Reject if
   - the hardening object is vague
   - slice ordering is missing
   - evidence classes are mixed together
   - production-readiness wording outruns the planned evidence

#### Short judgment

`P0` is acceptable when:

1. `Phase 12` has a clear hardening object
2. the first hardening slices are explicitly ordered
3. the evidence ladder is explicit
4. `sw` and `tester` can start `P1` without re-deciding what “hardening” means

---

### P0 Completion Record

Date: 2026-04-02
Result: accepted

What was accepted:

1. `Phase 12` hardens the accepted chosen path from `Phase 09` + `Phase 10` + `Phase 11`
2. the slice order is fixed as:
   - `P1` restart / recovery disturbance hardening
   - `P2` soak / long-run stability hardening
   - `P3` diagnosability / blocker accounting
   - `P4` performance floor / rollout gates
3. the evidence ladder is fixed and separated into:
   - disturbance correctness
   - soak / repeated-cycle evidence
   - diagnosability / blocker evidence
   - bounded acceptance wording

Carry-forward into `P1`:

1. `P1` must prove correctness under restart/disturbance
2. `P1` must not degrade into “recovery code exists” or “the system roughly comes back”
3. accepted earlier chosen-path semantics remain stable unless `P1` exposes a real bug

---

### P1 Technical Pack

Date: 2026-04-02
Goal: deliver bounded restart / recovery disturbance hardening on the accepted chosen path so correctness remains stable across restart, rejoin, repeated failover, and stale-signal ordering without reopening earlier accepted semantics or overclaiming production readiness

#### Layer 1: Semantic Core

##### Problem statement

`Phase 09` accepted chosen-path execution closure.
`Phase 10` accepted bounded chosen-path control-plane closure.
`Phase 11` accepted bounded chosen-path product-surface rebinding.

What is still not accepted is not whether the chosen path works in one bounded integrated pass.
It is whether that accepted path stays correct when the live system is disturbed by restart, rejoin, repeated failover, and delayed or stale signals.

The first hardening slice therefore accepts only one thing:

1. correctness under restart/disturbance on the accepted chosen path

It does not accept:

1. merely that recovery-related code exists
2. merely that the system eventually appears to recover in a rough sense
3. soak / long-run viability
4. diagnosability / blocker accounting
5. performance floor / rollout readiness

##### System reality

Current reality already includes these accepted layers:

1. chosen-path execution truth under `blockvol` / `v2bridge`
2. chosen-path control intake and master-driven ownership/control delivery
3. chosen-path product-surface rebinding for snapshot / `CSI` / `NVMe` / `iSCSI`

The `P1` question is therefore:

1. whether restart reconstructs ownership/control from accepted truth rather than accidental leftovers
2. whether rejoin/publication refresh preserve identity and visibility
3. whether repeated failover/rejoin cycles stay monotonic and convergent
4. whether stale or delayed signals after disturbance are rejected or explicitly bounded

##### High-level algorithm

Use this hardening model:

1. treat restart/rejoin/failover as disturbance applied to the already accepted chosen path
2. reconstruct ownership/control from accepted identity, epoch, session, and publication truth
3. reject or bound stale post-disturbance signals before they can reclaim authority
4. verify convergence repeatedly, not just once
5. keep all claims bounded to disturbance correctness rather than broader production readiness

##### Pseudo code

```text
on restart / rejoin / failover disturbance:
    rebuild local and master-visible state from accepted chosen-path truth
    refresh publication/identity without changing semantic ownership rules
    process only signals that remain valid under current epoch/session/authority
    reject or bound delayed stale signals
    verify convergence after repeated disturbance cycles

after proof:
    correctness under disturbance is accepted
    soak / diagnosability / performance remain deferred
```

##### State / contract

`P1` must make these truths explicit:

1. acceptance object = correctness under restart/disturbance on the chosen path
2. the contract is not “recovery feature exists”
3. the contract is not “eventual rough recovery is observed once”
4. identity, epoch, session, ownership, and publication truth must remain coherent across disturbance
5. stale post-disturbance signals must not silently regain authority

##### Reject shapes

Reject before implementation if the slice:

1. proves only one happy restart path and calls that hardening
2. skips stale or delayed signal shapes after disturbance
3. allows identity or authority drift after rejoin/publication refresh
4. absorbs soak, diagnosability, perf, or new product-surface work
5. casually reopens accepted `Phase 09` / `Phase 10` / `Phase 11` semantics without a concrete exposed bug

#### Layer 2: Execution Core

##### Current gaps `P1` must close

1. current chosen-path proofs are strong, but restart/disturbance correctness is not yet accepted as a bounded slice
2. repeated disturbance and stale-signal shapes are not yet packaged as one explicit hardening object
3. phase wording needs to prevent `P1` from collapsing into vague “recovery exists” claims
4. later hardening concerns must stay out of the first disturbance slice

##### Reuse / update instructions

1. `weed/server/block_recovery.go`
   - `update in place`
   - primary live restart/recovery ownership surface
   - keep it as the chosen-path disturbance runtime, not as a place to redefine protocol truth

2. `weed/server/master_block_failover.go`
   - `update in place`
   - primary repeated failover/rejoin disturbance surface
   - preserve accepted promotion/authority rules unless a real bug is exposed

3. `weed/server/master_block_registry.go`
   - `update in place`
   - publication/identity/freshness tracking surface under disturbance
   - keep acceptance tied to chosen-path authority truth

4. `weed/server/volume_server_block.go`
   - `update in place`
   - volume-server intake/reconstruction surface under restart/rejoin
   - do not let local leftovers silently override accepted control truth

5. `weed/server/block_recovery_test.go`
   - `update in place`
   - main positive restart/recovery correctness package

6. `weed/server/block_recovery_adversarial_test.go`
   - `update in place`
   - main stale/delayed/disturbance adversarial package

7. `weed/server/qa_block_*test.go`
   - `update in place` where a focused chosen-path disturbance proof is cleaner than overloading generic tests
   - prefer one bounded disturbance package rather than many scattered partial claims

8. `weed/storage/blockvol/*` and `weed/storage/blockvol/v2bridge/*`
   - `reference only` unless restart/disturbance hardening exposes a real bug in accepted earlier closure
   - do not casually reopen accepted execution semantics

9. copy guidance
   - prefer `update in place`
   - no parallel restart path
   - every reused V1-facing surface must be named explicitly in delivery notes

##### Validation focus

Required proofs:

1. restart correctness proof
   - restart reconstructs valid chosen-path ownership/control state
   - post-restart behavior does not rely on accidental pre-restart leftovers

2. rejoin/publication-refresh proof
   - refreshed address/publication keeps identity truth coherent
   - visibility and control routing remain aligned with accepted authority

3. repeated-disturbance proof
   - repeated failover/rejoin cycles converge
   - epoch/session monotonicity is preserved

4. stale-signal proof
   - delayed heartbeat/control signals after disturbance do not re-authorize stale ownership

5. no-overclaim proof
   - acceptance wording stays bounded to disturbance correctness
   - no soak/perf/rollout readiness is implied

Reject if:

1. evidence is only “restart succeeds once”
2. stale/delayed post-disturbance signals are not explicitly tested
3. repeated failover/rejoin is omitted
4. claims outrun disturbance correctness and imply broader production readiness

##### Suggested first cut

1. freeze one explicit disturbance matrix:
   - same-lineage restart
   - changed-address rejoin
   - repeated failover/rejoin
   - stale-signal arrival after disturbance
2. identify the smallest accepted chosen-path proof path through:
   - master registry / failover control
   - volume-server assignment intake
   - live recovery ownership
3. add or reshape tests so repeated disturbance and stale-signal rejection are first-class assertions
4. keep delivery wording explicitly bounded to correctness under disturbance

##### Assignment For `sw`

1. Goal
   - deliver `P1` restart / recovery disturbance hardening on the accepted chosen path

2. Required outputs
   - one bounded contract statement for correctness under restart/disturbance
   - one focused restart/rejoin/failover proof package
   - one stale/delayed-signal adversarial package
   - one explicit reuse note listing:
     - files updated in place
     - files reference only
     - every reused V1-facing surface and its bounded role

3. Hard rules
   - do not redefine accepted `Phase 09` / `Phase 10` / `Phase 11` semantics casually
   - do not turn `P1` into soak, diagnosability, perf, or feature expansion work
   - do not treat “recovery code exists” or “eventual rough recovery” as sufficient acceptance evidence

##### Assignment For `tester`

1. Goal
   - validate that `P1` proves correctness under restart/disturbance rather than vague recovery presence

2. Validate
   - restart correctness on the chosen path
   - rejoin/publication-refresh correctness
   - repeated failover/rejoin convergence
   - stale/delayed-signal rejection after disturbance
   - no-overclaim around soak, perf, or production readiness

3. Reject if
   - restart is only tested once in a happy path
   - stale-signal shapes are absent
   - repeated disturbance is absent
   - wording implies broader readiness than disturbance correctness

#### Short judgment

`P1` is acceptable when:

1. correctness under restart/disturbance is explicit and bounded
2. repeated disturbance proofs exist on the accepted chosen path
3. stale or delayed post-disturbance signals are proven fail-closed or explicitly bounded
4. `P1` stays clearly separate from `P2` soak, `P3` diagnosability, and `P4` performance/rollout work

---

### P1 Completion Record

Date: 2026-04-02
Result: accepted

What was accepted:

1. bounded correctness under restart/disturbance on the chosen path
2. real proto-to-`ProcessAssignments()` delivery proofs for the accepted disturbance package
3. hard VS-side post-apply checks for promoted-primary epoch truth and failover publication visibility
4. exact stale-epoch rejection via `ErrEpochRegression`

Accepted proof shape:

1. restart with same lineage:
   - initial, failover, and reconnect assignments are driven through the real proto → `ProcessAssignments()` path
   - promoted-primary volume epoch is verified non-regressive after reconnect refresh
2. failover publication switch:
   - publication truth switches to the new primary
   - VS heartbeat presence and master lookup coherence are both asserted
3. repeated failover/rejoin cycles:
   - epoch remains monotonic across repeated cycles
   - promoted-primary volume epoch is checked each round
4. stale signal:
   - stale lower epoch is rejected with the exact `ErrEpochRegression` sentinel
   - volume epoch remains at the current promoted epoch

Reuse note:

1. `weed/server/qa_block_disturbance_test.go`
   - new focused disturbance proof package
2. `weed/server/master_block_failover.go`
   - exercised as accepted disturbance surface
3. `weed/server/master_block_registry.go`
   - exercised as accepted authority/publication surface
4. `weed/server/volume_server_block.go`
   - exercised as real proto → `ProcessAssignments()` intake surface
5. `weed/server/block_recovery.go`
   - exercised as accepted recovery-management surface
6. no production code changes were required for this slice

Residual notes:

1. the single-process shared-store harness is an explicit bounded test simplification, not a production topology claim
2. `P1` acceptance does not imply soak, diagnosability, performance floor, or rollout readiness
3. `P2` is now the next active slice

---

### P2 Technical Pack

Date: 2026-04-02
Goal: deliver bounded soak / long-run stability hardening on the accepted chosen path so repeated operation does not accumulate hidden state drift or runtime residue, without turning the slice into diagnosability, perf, or rollout work

#### Layer 1: Semantic Core

##### Problem statement

`Phase 09` accepted chosen-path execution closure.
`Phase 10` accepted bounded chosen-path control-plane closure.
`Phase 11` accepted bounded chosen-path product-surface rebinding.
`P1` accepted correctness under restart/disturbance on that chosen path.

What is still not accepted is not whether the chosen path survives one bounded disturbance package.
It is whether that accepted path stays stable under repeated operation long enough to rule out hidden state drift, leftover runtime ownership artifacts, or repeated-cycle semantic erosion.

The second hardening slice therefore accepts only one thing:

1. bounded soak / long-run stability on the accepted chosen path

It does not accept:

1. merely that `P1` can be rerun several times manually
2. diagnosability / blocker accounting
3. performance floor or rollout readiness
4. broad new feature or topology expansion

##### System reality

Current reality already includes these accepted layers:

1. chosen-path execution truth under `blockvol` / `v2bridge`
2. chosen-path control intake and ownership/control delivery
3. chosen-path product-surface rebinding
4. chosen-path restart/disturbance correctness

The `P2` question is therefore:

1. whether repeated accepted-path cycles return to the same bounded truth
2. whether registry / VS-visible / product-visible state remain coherent after many cycles
3. whether repeated operation accumulates hidden runtime residue
4. whether soak evidence can be bounded cleanly without overclaiming diagnosability or production readiness

##### High-level algorithm

Use this hardening model:

1. define one bounded repeated-cycle envelope on the accepted chosen path
2. run that envelope enough times to expose hidden state drift if it exists
3. assert end-of-cycle truth after each cycle rather than only final success
4. assert bounded runtime-state hygiene as part of acceptance, not optional telemetry
5. keep all claims bounded to long-run stability only

##### Pseudo code

```text
for each bounded cycle in the soak envelope:
    run accepted chosen-path operations and disturbances
    wait for bounded convergence
    assert end-of-cycle registry / VS / product-visible truth
    assert no unbounded leftover runtime state

after repeated cycles:
    accept long-run stability only if truth remains coherent
    and runtime-state hygiene remains bounded
```

##### State / contract

`P2` must make these truths explicit:

1. acceptance object = bounded long-run stability on the accepted chosen path
2. the contract is not “we ran `P1` more than once”
3. the contract is not “telemetry looked quiet”
4. repeated cycles must return to coherent bounded truth
5. leftover runtime state must stay bounded within the tested envelope

##### Reject shapes

Reject before implementation if the slice:

1. counts iterations without asserting end-of-cycle truth
2. treats support telemetry as acceptance evidence by itself
3. ignores runtime-state hygiene and only reports pass/fail totals
4. absorbs diagnosability, perf, or launch-readiness work
5. casually reopens accepted `Phase 09` / `Phase 10` / `Phase 11` / `P1` semantics without a concrete exposed bug

#### Layer 2: Execution Core

##### Current gaps `P2` must close

1. current accepted proofs are strong per slice, but long-run repeated-cycle stability is not yet accepted as a bounded object
2. repeated-cycle end-state coherence is not yet packaged as one explicit hardening proof
3. bounded runtime-state hygiene is not yet part of slice-level acceptance wording
4. later diagnosability and perf concerns must stay outside the soak slice

##### Reuse / update instructions

1. `weed/server/qa_block_*test.go`
   - `update in place`
   - primary repeated-cycle or soak proof surface
   - prefer one focused bounded soak package over many scattered reruns

2. `weed/server/block_recovery_test.go` and related hardening tests
   - `update in place`
   - use when repeated-cycle ownership/runtime hygiene assertions are cleaner there

3. testrunner / infra / metrics helpers
   - `reuse as support instrumentation`
   - telemetry may support the proof, but acceptance must still be asserted directly in tests or bounded checks

4. `weed/server/master_block_failover.go`, `master_block_registry.go`, `volume_server_block.go`, `block_recovery.go`
   - `update in place` only if repeated-cycle hardening exposes a real bug
   - do not reopen accepted semantics casually

5. `weed/storage/blockvol/*` and `weed/storage/blockvol/v2bridge/*`
   - `reference only` unless soak evidence exposes a real accepted-path mismatch

6. copy guidance
   - prefer `update in place`
   - no separate soak-only code path
   - every reused support surface must be named explicitly in delivery notes

##### Validation focus

Required proofs:

1. repeated-cycle coherence proof
   - the accepted chosen path completes many bounded cycles
   - registry / VS / product-visible truth remain coherent at cycle boundaries

2. runtime-state hygiene proof
   - repeated cycles do not leave unbounded leftover tasks, sessions, or stale ownership artifacts inside the tested envelope

3. long-run stability proof
   - stability claims come from repeated bounded evidence, not one-shot reruns

4. no-overclaim proof
   - acceptance wording stays bounded to long-run stability
   - no diagnosability/perf/rollout readiness is implied

Reject if:

1. the package is just `P1` rerun in a loop without end-state checks
2. drift is not checked explicitly at cycle boundaries
3. runtime-state hygiene is not part of acceptance
4. the slice claims broader operational readiness than it proves

##### Suggested first cut

1. freeze one bounded repeated-cycle matrix for the chosen path
2. identify the minimum end-of-cycle truth that must remain stable:
   - registry authority/publication truth
   - VS-visible applied state
   - product-visible lookup/publication truth where relevant
3. add repeated-cycle assertions for bounded runtime-state hygiene
4. keep delivery wording explicitly bounded to soak / long-run stability

##### Assignment For `sw`

1. Goal
   - deliver `P2` bounded soak / long-run stability hardening on the accepted chosen path

2. Required outputs
   - one bounded contract statement for long-run stability
   - one focused repeated-cycle or soak proof package
   - one explicit runtime-state hygiene proof package
   - one explicit reuse note listing:
     - files updated in place
     - files reference only
     - support instrumentation reused and why

3. Hard rules
   - do not redefine accepted `Phase 09` / `Phase 10` / `Phase 11` / `P1` semantics casually
   - do not turn `P2` into diagnosability, perf, or feature-expansion work
   - do not treat iteration counts or telemetry alone as sufficient acceptance evidence

##### Assignment For `tester`

1. Goal
   - validate that `P2` proves bounded long-run stability rather than “`P1` passed repeatedly”

2. Validate
   - repeated-cycle end-state coherence
   - bounded runtime-state hygiene
   - explicit drift checks at cycle boundaries
   - no-overclaim around diagnosability, perf, or production readiness

3. Reject if
   - cycles are repeated without hard end-state assertions
   - runtime-state hygiene is omitted
   - wording implies broader readiness than bounded long-run stability

#### Short judgment

`P2` is acceptable when:

1. bounded long-run stability is explicit and separately reviewable
2. repeated-cycle evidence shows no semantic drift on the accepted chosen path
3. runtime-state hygiene remains bounded within the tested envelope
4. `P2` stays clearly separate from `P3` diagnosability and `P4` performance/rollout work

---

### P2 Completion Record

Date: 2026-04-02
Result: accepted

What was accepted:

1. bounded soak / long-run stability on the accepted chosen path
2. repeated-cycle end-of-cycle coherence checks across registry / VS-visible / product-visible truth
3. bounded runtime-state hygiene without unbounded leftover recovery tasks, stale registry entries, or assignment-queue growth inside the tested envelope
4. bounded no-overclaim discipline: `P2` does not imply diagnosability, performance floor, or rollout readiness

Accepted proof shape:

1. repeated-cycle no-drift proof:
   - repeated create / failover / recover cycles preserve coherent end-of-cycle truth
   - lookup publication is checked against registry truth rather than merely being non-empty
2. runtime-hygiene proof:
   - repeated create/delete cycles do not leave stale recovery tasks or stale registry residue
   - queue growth remains bounded inside the accepted repeated-cycle envelope
3. steady-state idempotence proof:
   - repeated same-assignment delivery does not introduce semantic drift
   - registry epoch and lookup/publication truth remain stable

Reuse note:

1. `weed/server/qa_block_soak_test.go`
   - focused repeated-cycle / soak proof package
2. accepted chosen-path production code was exercised as the target runtime
3. no additional production-code closure was required for slice acceptance

Residual notes:

1. the accepted envelope is bounded repeated-cycle evidence, not hours/days-scale soak certification
2. `P2` acceptance does not imply diagnosability, performance floor, or rollout readiness
3. `P3` is now the next active slice

---

### P3 Technical Pack

Date: 2026-04-02
Goal: deliver bounded diagnosability / blocker accounting / runbook hardening on the accepted chosen path so bounded failures can be identified from operator-visible evidence and unresolved production blockers become explicit, finite, and reviewable without turning the slice into perf or rollout work

#### Layer 1: Semantic Core

##### Problem statement

`Phase 09` accepted chosen-path execution closure.
`Phase 10` accepted bounded chosen-path control-plane closure.
`Phase 11` accepted bounded chosen-path product-surface rebinding.
`P1` accepted correctness under restart/disturbance on that chosen path.
`P2` accepted bounded long-run stability on that chosen path.

What is still not accepted is not whether the chosen path works or stays stable inside bounded repeated cycles.
It is whether bounded failures and residual gaps are diagnosable in operator-facing terms, and whether the remaining blockers to production use are explicit rather than hidden in chat memory or private intuition.

The third hardening slice therefore accepts only one thing:

1. bounded diagnosability / blocker accounting on the accepted chosen path

It does not accept:

1. merely that some logs or debug strings already exist
2. merely that an engineer can eventually understand failures by reading source code
3. performance floor or rollout readiness
4. broad topology expansion or new product-surface planning

##### System reality

Current reality already includes these accepted layers:

1. chosen-path execution truth under `blockvol` / `v2bridge`
2. chosen-path control intake and ownership/control delivery
3. chosen-path product-surface rebinding
4. chosen-path restart/disturbance correctness
5. chosen-path bounded repeated-cycle stability

The `P3` question is therefore:

1. whether bounded failures can be distinguished with operator-visible evidence
2. whether visible symptoms can be mapped to the relevant authority/runtime/publication truth
3. whether unresolved blockers are explicit, finite, and reviewable
4. whether diagnosability evidence can be bounded cleanly without overclaiming rollout readiness

##### High-level algorithm

Use this hardening model:

1. define one bounded diagnosis envelope on the accepted chosen path
2. identify the minimum operator-visible evidence required to classify the bounded failure classes
3. prove at least one diagnosis loop closes from visible symptom to accepted truth or explicit blocker
4. record unresolved blockers in one explicit finite ledger
5. keep all claims bounded to diagnosability / blocker accounting only

##### Pseudo code

```text
for each bounded chosen-path failure class:
    trigger or inspect the failure within the accepted envelope
    collect operator-visible evidence (logs / status / lookup / counters / bounded docs)
    map the symptom to the relevant ownership/control/runtime/publication truth
    decide whether the condition is:
        - diagnosed and bounded
        - an explicit unresolved blocker

after proof:
    accept diagnosability / blocker accounting only if
        bounded failures are reviewable from visible evidence
        and unresolved blockers are explicit and finite
```

##### State / contract

`P3` must make these truths explicit:

1. acceptance object = bounded diagnosability / blocker accounting on the accepted chosen path
2. the contract is not “logs exist”
3. the contract is not “an engineer can probably debug it”
4. at least one bounded diagnosis loop must close from visible symptom to accepted truth or explicit blocker
5. unresolved blockers must be explicit, finite, and reviewable

##### Reject shapes

Reject before implementation if the slice:

1. adds logs/status text without proving diagnostic usefulness
2. leaves blockers implicit or scattered across chats/docs without one bounded ledger
3. depends on source-level spelunking or debugger-only knowledge for core diagnosis
4. absorbs perf, rollout, or broad product expansion work
5. casually reopens accepted `Phase 09` / `Phase 10` / `Phase 11` / `P1` / `P2` semantics without a concrete exposed bug or visibility gap

#### Layer 2: Execution Core

##### Current gaps `P3` must close

1. accepted chosen-path behavior is now reasonably strong, but bounded operator-facing diagnosis quality is not yet accepted as its own slice
2. residual blockers are still too easy to leave implicit in discussions rather than as a finite review artifact
3. the minimum operator-visible evidence for classifying bounded failures is not yet packaged as one explicit hardening proof
4. later perf/rollout concerns must stay outside the diagnosability slice

##### Reuse / update instructions

1. `weed/server/qa_block_*test.go` and focused hardening tests
   - `update in place`
   - use where a bounded diagnosis loop can be proven with real chosen-path surfaces

2. `weed/server/block_recovery.go`, `master_block_failover.go`, `master_block_registry.go`, `volume_server_block.go`
   - `update in place` only if diagnosability work exposes a real operator-visible evidence gap
   - do not reopen accepted semantics casually

3. bounded runbook / blocker docs under `sw-block/.private/phase/` or `sw-block/docs/`
   - `update in place`
   - use to record the finite blocker ledger and the accepted diagnosis loop
   - docs may support acceptance, but docs alone are not acceptance evidence

4. telemetry / logging / status helpers
   - `reuse as support instrumentation`
   - support surfaces must remain tied to one explicit truth-closure proof

5. `weed/storage/blockvol/*` and `weed/storage/blockvol/v2bridge/*`
   - `reference only` unless diagnosability work exposes a real accepted-path mismatch

6. copy guidance
   - prefer `update in place`
   - no parallel “debug mode” code path
   - every support artifact reused must be named explicitly in delivery notes

##### Validation focus

Required proofs:

1. symptom-classification proof
   - at least one bounded failure class can be identified from operator-visible evidence
   - the evidence is specific enough to distinguish it from nearby failure classes

2. diagnosis-loop proof
   - a visible symptom can be traced to the relevant ownership/control/runtime/publication truth
   - the loop closes without requiring debugger-only knowledge

3. blocker-accounting proof
   - unresolved blockers are explicit, finite, and reviewable
   - accepted-known blockers are separated from unknown hand-waving

4. no-overclaim proof
   - acceptance wording stays bounded to diagnosability / blockers
   - no perf/rollout readiness is implied

Reject if:

1. the package adds observability text but proves no diagnosis loop
2. blocker accounting is missing or open-ended
3. visible evidence does not actually line up with the accepted truth being diagnosed
4. the slice claims broader readiness than diagnosability / blockers

##### Suggested first cut

1. freeze one bounded diagnosis matrix for the accepted chosen path
2. pick the minimum symptom classes worth proving first:
   - failover/recovery convergence stall
   - publication/lookup mismatch versus authority truth
   - leftover runtime work after an operation that should have converged
3. define one explicit blocker ledger with finite named items and out-of-scope non-claims
4. keep delivery wording explicitly bounded to diagnosability / blocker accounting

##### Concrete first delivery pack (`P3 rev1`)

Delivery object:

1. one bounded diagnosability package for the accepted chosen path covering exactly three symptom classes:
   - `S1` failover/recovery convergence stall
   - `S2` publication/lookup mismatch versus authority truth
   - `S3` leftover runtime work after delete/steady-state convergence should be complete
2. one explicit blocker ledger with finite named unresolved items
3. one bounded runbook note describing how the three symptom classes are distinguished

Closed-loop validation object:

1. for each accepted symptom class, the package must show:
   - visible symptom
   - operator-visible evidence surface
   - underlying accepted truth surface
   - diagnosis conclusion or explicit blocker classification
2. at least one symptom class must close fully from visible symptom to accepted truth without debugger/source spelunking
3. the blocker ledger must separate:
   - diagnosed and bounded
   - unresolved but explicit
   - out of scope for `P3`

Concrete symptom matrix:

1. `S1` failover/recovery convergence stall
   - visible symptom:
     - failover/reconnect does not converge within bounded time
   - operator-visible evidence target:
     - failover/recover log line family from `master_block_failover.go`
     - bounded diagnostic snapshot for pending rebuild / deferred promotion state if current logs are not sufficient
   - truth source:
     - registry authority epoch/primary
     - pending rebuild / deferred timer state
   - acceptable conclusion:
     - classified as lease-wait, catch-up/rebuild pending, or unresolved blocker

2. `S2` publication/lookup mismatch
   - visible symptom:
     - `LookupBlockVolume` publication does not match expected failover result
   - operator-visible evidence target:
     - `LookupBlockVolume` response in `master_grpc_server_block.go`
     - bounded diagnostic note tying publication fields to registry authority fields
   - truth source:
     - `blockRegistry.Lookup(name)`
   - acceptable conclusion:
     - publication is coherent, stale, or blocked by an explicit unresolved gap

3. `S3` leftover runtime work after convergence
   - visible symptom:
     - delete/steady-state is complete, but recovery/runtime work still appears live
   - operator-visible evidence target:
     - bounded recovery diagnostic snapshot replacing “for testing only” reliance on raw `ActiveTaskCount()`
   - truth source:
     - `RecoveryManager` live task set
   - acceptable conclusion:
     - runtime state is clean, bounded-in-flight, or blocked by an explicit unresolved gap

Concrete target files:

1. `weed/server/qa_block_diagnosability_test.go`
   - `new`
   - main `P3 rev1` proof package
   - should carry the three symptom-class proofs and the no-overclaim assertions

2. `weed/server/master_block_failover.go`
   - `update in place`
   - only if needed to expose a bounded read-only diagnostic snapshot for:
     - pending rebuild count/details relevant to a volume/server
     - deferred promotion timers relevant to a server
   - do not change failover semantics for `P3`

3. `weed/server/block_recovery.go`
   - `update in place`
   - add one bounded read-only diagnosis surface for runtime recovery state
   - keep current task semantics unchanged; diagnosability only

4. `weed/server/master_grpc_server_block.go`
   - `reference first`
   - reuse `LookupBlockVolume` as the primary publication-visible symptom surface
   - only update if a minimal read-only diagnosis helper is needed to close the loop cleanly

5. `weed/server/master_block_registry.go`
   - `reference first`
   - reuse `Lookup(name)` as authority truth for the diagnosis loop
   - only update if a bounded read-only projection/helper is needed

6. `sw-block/.private/phase/phase-12-p3-blockers.md`
   - `new`
   - finite blocker ledger for `P3`
   - must name:
     - blocker id
     - symptom
     - current visible evidence
     - owning truth surface
     - why unresolved
     - whether it blocks `P4` / rollout discussion

7. `sw-block/.private/phase/phase-12-p3-runbook.md`
   - `new`
   - short bounded runbook for the three accepted symptom classes
   - must stay tied to actual code/test evidence, not aspirational operations prose

Concrete tests expected in `qa_block_diagnosability_test.go`:

1. `TestP12P3_FailoverConvergence_Diagnosable`
   - trigger bounded failover/reconnect disturbance
   - prove the visible symptom can be classified using accepted evidence surfaces
   - map it to registry/failover truth

2. `TestP12P3_PublicationMismatch_Diagnosable`
   - prove lookup/publication evidence can be compared against authority truth
   - if mismatch is induced or simulated, the test must classify it explicitly rather than merely observing it

3. `TestP12P3_RuntimeResidue_Diagnosable`
   - after delete or bounded steady-state completion, prove recovery/runtime residue is visible through the bounded diagnosis surface
   - distinguish “clean” from “unexpected live work”

4. `TestP12P3_BlockerLedger_Bounded`
   - assert the blocker ledger exists
   - assert unresolved items are finite and explicitly named
   - assert no item is silently upgraded into perf/rollout readiness claims

What `P3 rev1` should not try to do:

1. do not add a broad new public admin API unless the bounded diagnosis loop cannot close any other way
2. do not redesign logging across the whole block stack
3. do not absorb hours/days soak, perf floor, rollout policy, or topology expansion
4. do not turn the blocker ledger into a generic wishlist

##### Assignment For `sw`

1. Goal
   - deliver `P3` bounded diagnosability / blocker accounting / runbook hardening on the accepted chosen path

2. Required outputs
   - one bounded contract statement for diagnosability / blockers
   - one focused diagnosis-loop proof package
   - one explicit blocker ledger with finite named unresolved items
   - one explicit reuse note listing:
     - files updated in place
     - files reference only
     - support instrumentation or docs reused and why

3. Hard rules
   - do not redefine accepted `Phase 09` / `Phase 10` / `Phase 11` / `P1` / `P2` semantics casually
   - do not turn `P3` into perf, rollout, or feature-expansion work
   - do not treat logs/docs alone as sufficient acceptance evidence

##### Assignment For `tester`

1. Goal
   - validate that `P3` proves bounded diagnosability / blocker accounting rather than “we have logs”

2. Validate
   - one closed diagnosis loop from visible symptom to accepted truth
   - one explicit blocker ledger with finite named unresolved items
   - evidence is operator-visible rather than debugger-only
   - no-overclaim around perf, rollout, or production readiness

3. Reject if
   - diagnosis only works with source spelunking
   - blockers remain implicit or open-ended
   - wording implies broader readiness than diagnosability / blockers

#### Short judgment

`P3` is acceptable when:

1. bounded diagnosability / blocker accounting is explicit and separately reviewable
2. at least one diagnosis loop closes from visible symptom to accepted truth on the chosen path
3. unresolved blockers are explicit, finite, and reviewable
4. `P3` stays clearly separate from `P4` performance / rollout-gate hardening

---

### P3 Completion Record

Date: 2026-04-02
Result: accepted

What was accepted:

1. bounded diagnosability / blocker accounting on the accepted chosen path
2. explicit read-only diagnosis surfaces for:
   - failover state
   - publication coherence
   - runtime recovery residue
3. one finite blocker ledger and one bounded runbook tied to those diagnosis surfaces
4. bounded no-overclaim discipline: `P3` does not imply performance floor or rollout readiness

Accepted proof shape:

1. failover-convergence diagnosis proof:
   - `LookupBlockVolume` plus `FailoverDiagnostic` classify failover state on the accepted path
   - deferred-promotion lifecycle is cleaned up after timer fire so stale `lease_wait` is not misreported
2. publication diagnosis proof:
   - `PublicationDiagnosticFor()` compares operator-visible lookup against registry authority via two distinct reads
   - coherence/mismatch becomes an explicit diagnosability surface rather than an implicit test assumption
3. runtime-residue diagnosis proof:
   - `RecoveryDiagnostic` exposes bounded active-task evidence sufficient for the accepted `S3` conclusion classes
4. blocker-ledger proof:
   - the actual blocker file is read and validated as finite, explicit, and bounded

Reuse note:

1. `weed/server/qa_block_diagnosability_test.go`
   - focused diagnosability proof package
2. `weed/server/master_block_failover.go`
   - updated in place to expose bounded failover and publication diagnosis surfaces
3. `weed/server/block_recovery.go`
   - bounded recovery diagnosis surface reused/retained in place
4. `sw-block/.private/phase/phase-12-p3-blockers.md`
   - finite blocker ledger
5. `sw-block/.private/phase/phase-12-p3-runbook.md`
   - bounded runbook tied to actual exposed surfaces

Residual notes:

1. the accepted diagnosability proof remains bounded and in-process; it is not a claim of full distributed operator tooling
2. `P3` acceptance does not imply performance floor or rollout readiness
3. `P4` is now the next active slice

---

### P4 Technical Pack

Date: 2026-04-02
Goal: deliver bounded performance-floor / rollout-gate hardening on the accepted chosen path so launch discussion is backed by measured floor values, explicit resource-cost characterization, and one finite supported rollout envelope rather than generic confidence language

#### Layer 1: Semantic Core

##### Problem statement

`Phase 09` accepted chosen-path execution closure.
`Phase 10` accepted bounded chosen-path control-plane closure.
`Phase 11` accepted bounded chosen-path product-surface rebinding.
`P1` accepted correctness under restart/disturbance on that chosen path.
`P2` accepted bounded long-run stability on that chosen path.
`P3` accepted bounded diagnosability / blocker accounting on that chosen path.

What is still not accepted is not whether the chosen path is correct, stable, or diagnosable inside the bounded hardening envelope.
It is whether that accepted path has an explicit measured minimum performance floor and an explicit first-launch envelope with named rollout gates.

The fourth hardening slice therefore accepts only two bounded things:

1. one measured performance floor for a named workload envelope on the accepted chosen path
2. one explicit rollout-gate / launch-envelope package derived from all accepted evidence so far

It does not accept:

1. generic “performance is good” prose
2. isolated fast runs without a named workload contract
3. broad rollout readiness outside the explicitly supported envelope
4. broad optimization campaigns or new topology expansion

##### System reality

Current reality already includes these accepted layers:

1. chosen-path execution truth
2. chosen-path control closure
3. chosen-path product-surface rebinding
4. chosen-path restart/disturbance correctness
5. chosen-path bounded long-run stability
6. chosen-path bounded diagnosability / blocker accounting

The `P4` question is therefore:

1. what is the minimum acceptable measured floor for the accepted chosen path under a named workload envelope
2. what explicit cost or replication tax accompanies that floor
3. what exact first-launch envelope is supported by all accepted evidence so far
4. which rollout gates remain open or closed after that explicit measurement package

##### High-level algorithm

Use this hardening model:

1. define one bounded workload envelope for the accepted chosen path
2. measure explicit floor values and cost characteristics for that envelope
3. translate accepted correctness/stability/diagnosability evidence plus measured floor into a finite rollout-gate document
4. keep all claims bounded to the measured envelope and named first-launch scope

##### Pseudo code

```text
define one bounded workload envelope on the accepted chosen path
run repeatable measurement package
record floor values and resource-cost envelope

combine:
    accepted correctness evidence
    accepted soak evidence
    accepted diagnosability/blocker evidence
    measured floor values

produce rollout-gate artifact:
    supported envelope
    cleared blockers
    remaining blockers
    reject conditions for rollout

accept P4 only if:
    floor is explicit
    cost is explicit
    rollout gates are explicit
    claims do not outrun the measured envelope
```

##### State / contract

`P4` must make these truths explicit:

1. acceptance object = bounded performance floor + bounded rollout-gate package on the accepted chosen path
2. the contract is not “benchmarks were run”
3. the contract is not “it seems fast enough”
4. the launch envelope must be explicit and finite
5. any remaining rollout blockers must be explicit and named

##### Reject shapes

Reject before implementation if the slice:

1. reports benchmark numbers without a named workload contract
2. mixes measurement telemetry with acceptance thresholds without clarity
3. treats rollout readiness as implied from prior slices rather than explicitly gated
4. absorbs broad performance redesign, feature expansion, or topology expansion
5. casually reopens accepted `Phase 09` / `Phase 10` / `Phase 11` / `P1` / `P2` / `P3` semantics without a concrete exposed bug

#### Layer 2: Execution Core

##### Current gaps `P4` must close

1. accepted chosen-path behavior is strong, but the minimum measured floor is not yet an accepted artifact
2. launch discussion is still too easy to leave in chat prose rather than a finite gate document
3. cost characterization is not yet tied to one named accepted workload envelope
4. post-Phase-12 productionization work must stay outside the slice

##### Reuse / update instructions

1. `weed/server/qa_block_*test.go`, testrunner scenarios, and focused perf/support harnesses
   - `update in place`
   - primary measurement and bounded perf-floor proof surface

2. `weed/server/*`, `weed/storage/blockvol/*`, `weed/storage/blockvol/v2bridge/*`
   - `update in place` only if measurement work exposes a real bug or measurement-surface gap
   - do not reopen accepted semantics casually

3. `sw-block/.private/phase/`
   - `update in place`
   - use for measured floor summary and rollout-gate / launch-envelope artifact

4. support telemetry / infra / scenario helpers
   - `reuse as support instrumentation`
   - support telemetry may help characterize cost, but acceptance must still be tied to explicit floor/gate assertions

5. copy guidance
   - prefer `update in place`
   - no parallel perf-only code path
   - every measurement/support surface reused must be named explicitly in delivery notes

##### Validation focus

Required proofs:

1. performance-floor proof
   - one named workload envelope exists
   - measured floor values are explicit for that envelope

2. cost-characterization proof
   - replication/resource tax or similar bounded cost is explicit

3. rollout-gate proof
   - first supported launch envelope is explicit and finite
   - remaining rollout blockers are explicit and named

4. no-overclaim proof
   - acceptance wording stays bounded to the measured envelope and explicit gates
   - no broad production success is implied

Reject if:

1. benchmark output exists without a named acceptance envelope
2. rollout gates are vague, open-ended, or mixed with future wishlist work
3. cost/support telemetry is presented without explicit acceptance relevance
4. the slice claims broader launch readiness than the measured envelope supports

##### Suggested first cut

1. freeze one accepted workload envelope for the RF=2 `sync_all` chosen path
2. measure one bounded floor package and one bounded cost package
3. define one `P4` launch-envelope / rollout-gate document
4. keep delivery wording explicitly bounded to measured floor and first-launch scope

##### Expected delivery shape (`P4 rev1`)

1. one bounded measurement package
   - named workload envelope
   - repeatable harness
   - explicit floor values
2. one explicit cost summary
   - resource/replication tax or similar bounded cost statement
3. one rollout-gate artifact
   - supported launch envelope
   - cleared gates
   - remaining gates/blockers
   - reject conditions for launch

##### Assignment For `sw`

1. Goal
   - deliver `P4` bounded performance-floor / rollout-gate hardening on the accepted chosen path

2. Required outputs
   - one named workload envelope for the accepted chosen path
   - one focused measured floor package
   - one explicit cost summary
   - one explicit rollout-gate / launch-envelope artifact
   - one explicit reuse note listing:
     - files updated in place
     - files reference only
     - measurement/support surfaces reused and why

3. Hard rules
   - do not redefine accepted `Phase 09` / `Phase 10` / `Phase 11` / `P1` / `P2` / `P3` semantics casually
   - do not turn `P4` into broad optimization or generic launch-marketing work
   - do not treat benchmark output alone as sufficient acceptance evidence

##### Assignment For `tester`

1. Goal
   - validate that `P4` proves a bounded floor and bounded rollout gates rather than “numbers exist”

2. Validate
   - one named workload envelope with measured floor values
   - one explicit cost characterization
   - one explicit finite rollout-gate / launch-envelope artifact
   - no-overclaim around broader rollout beyond the named envelope

3. Reject if
   - workload contract is ambiguous
   - floor values are not tied to a repeatable harness
   - rollout gates are vague or open-ended
   - wording implies broader readiness than the measured envelope supports

#### Short judgment

`P4` is acceptable when:

1. one bounded workload envelope is explicit and measured
2. one minimum acceptable floor is explicit for that envelope
3. one first-launch rollout envelope/gate package is explicit and finite
4. `P4` stays clearly separate from post-Phase-12 productionization work
