# Phase 12

Date: 2026-04-02
Status: active
Purpose: move the accepted chosen-path implementation from candidate-safe product closure toward production-safe behavior under restart, disturbance, and operational reality

## Why This Phase Exists

`Phase 09` accepted production-grade execution closure on the chosen path.
`Phase 10` accepted bounded control-plane closure on that same path.
`Phase 11` accepted bounded product-surface rebinding on that same path.

What remains is no longer:

1. whether the chosen backend path works
2. whether selected product surfaces can be rebound onto it

It is now:

1. whether the chosen path stays correct under restart, failover, rejoin, and repeated disturbance
2. whether long-run behavior is stable enough for serious production use
3. whether operators can diagnose, bound, and reason about failures in practice
4. whether remaining production blockers are explicit and finite

## Phase Goal

Move from candidate-safe chosen-path closure to explicit production-hardening closure planning and execution.

Execution note:

1. treat `P0` as real planning work, not placeholder prose
2. use `phase-12-log.md` as the technical pack for:
   - step breakdown
   - hard indicators
   - reject shapes
   - assignment text for `sw` and `tester`

## Scope

### In scope

1. restart/recovery stability under repeated disturbance
2. long-run / soak viability planning and evidence design
3. operational diagnosability and blocker accounting
4. bounded hardening slices that do not reopen accepted earlier semantics casually

### Out of scope

1. re-discovering core protocol semantics already accepted in `Phase 09` / `Phase 10`
2. re-scoping `Phase 11` product rebinding work unless a hardening proof exposes a real bug
3. broad new feature expansion unrelated to hardening
4. unbounded product-surface additions

## Phase 12 Items

### P0: Hardening Plan Freeze

Goal:

- convert `Phase 12` from a broad “hardening” label into a bounded execution plan with explicit first slices, hard indicators, and reject shapes

Accepted decision target:

1. define the first hardening slices and their order
2. define what counts as production-hardening evidence versus support evidence
3. define which accepted surfaces become the first disturbance targets

Planned first hardening areas:

1. restart / rejoin / repeated failover disturbance
2. long-run / soak stability
3. operational diagnosis quality and blocker accounting
4. performance floor and cost characterization only after correctness-hardening slices are bounded

Status:

- accepted

### Later candidate slices inside `Phase 12`

1. `P1`: restart / recovery disturbance hardening
2. `P2`: soak / long-run stability hardening
3. `P3`: diagnosability / blocker accounting / runbook hardening
4. `P4`: performance floor and rollout-gate hardening

### P1: Restart / Recovery Disturbance Hardening

Goal:

- prove the accepted chosen path remains correct under restart, rejoin, repeated failover, and disturbance ordering

Acceptance object:

1. `P1` accepts correctness under restart/disturbance on the chosen path
2. it does not accept merely that recovery-related code paths exist
3. it does not accept merely that the system eventually seems to recover in a loose or approximate sense

Execution steps:

1. Step 1: disturbance contract freeze
   - define the bounded disturbance classes for the first hardening slice:
     - restart with same lineage
     - restart with changed address / refreshed publication
     - repeated failover / rejoin cycles
     - delayed or stale signal arrival after restart/failover
2. Step 2: implementation hardening
   - harden ownership/control reconstruction on the already accepted chosen path
   - keep identity, epoch, session, and publication truth coherent across disturbance
3. Step 3: proof package
   - prove repeated disturbance correctness on the chosen path
   - prove stale or delayed signals fail closed rather than silently corrupting ownership truth
   - prove no-overclaim around soak, perf, or broader production readiness

Required scope:

1. restart/rejoin correctness for the accepted chosen path
2. publication/address refresh correctness without identity drift
3. repeated ownership/control transitions under failover and rejoin
4. bounded reject behavior for stale heartbeat/control signals after disturbance

Must prove:

1. post-restart chosen-path ownership is reconstructed from accepted truth rather than accidental local leftovers
2. stale or delayed signals after restart/failover are rejected or explicitly bounded
3. repeated failover/rejoin cycles preserve identity, epoch/session monotonicity, and convergence on the chosen path
4. acceptance wording stays bounded to disturbance correctness rather than broad production-readiness claims

Reuse discipline:

1. `weed/server/block_recovery.go` and related tests may be updated in place as the primary restart/recovery ownership surface
2. `weed/server/master_block_failover.go`, `weed/server/master_block_registry.go`, and `weed/server/volume_server_block.go` may be updated in place as the accepted control/runtime disturbance surfaces
3. `weed/server/block_recovery_test.go`, `weed/server/block_recovery_adversarial_test.go`, and focused `qa_block_*` tests should carry the main proof burden
4. `weed/storage/blockvol/*` and `weed/storage/blockvol/v2bridge/*` are reference only unless disturbance hardening exposes a real bug in accepted earlier closure
5. no reused V1 surface may silently redefine chosen-path ownership truth, recovery choice, or disturbance acceptance wording

Verification mechanism:

1. focused restart/rejoin/failover integration tests on the chosen path
2. adversarial checks for stale or delayed control/heartbeat arrival after disturbance
3. explicit no-overclaim review so `P1` does not absorb soak/perf/product-expansion work

Hard indicators:

1. one accepted restart correctness proof:
   - restart on the chosen path reconstructs valid ownership/control state
   - post-restart behavior does not depend on accidental pre-restart leftovers
2. one accepted rejoin/publication-refresh proof:
   - changed address or publication refresh does not break identity truth or visibility
3. one accepted repeated-disturbance proof:
   - repeated failover/rejoin cycles converge without epoch/session regression
4. one accepted stale-signal proof:
   - delayed heartbeat/control signals after disturbance do not re-authorize stale ownership
5. one accepted boundedness proof:
   - `P1` claims correctness under disturbance, not soak, perf, or rollout readiness

Reject if:

1. evidence only shows that recovery code paths execute, rather than that correctness is preserved under disturbance
2. tests prove only one happy restart path and skip stale/delayed signal shapes
3. identity, epoch/session, or publication truth can drift across restart/rejoin
4. `P1` quietly absorbs soak, diagnosability, perf, or new product-surface work

Status:

- accepted

Carry-forward from `P0`:

1. the hardening object is the accepted chosen path from `Phase 09` + `Phase 10` + `Phase 11`
2. `P1` is the first correctness-hardening slice because disturbance threatens correctness before soak or perf
3. later `P2` / `P3` / `P4` remain distinct acceptance objects and should not be absorbed into `P1`

### P2: Soak / Long-Run Stability Hardening

Goal:

- prove the accepted chosen path remains viable over longer duration and repeated operation without hidden state drift

Acceptance object:

1. `P2` accepts bounded long-run stability on the chosen path under repeated operation or soak-like repetition
2. it does not accept merely that one disturbance test can be repeated many times manually
3. it does not accept diagnosability, performance floor, or rollout readiness by implication

Execution steps:

1. Step 1: soak contract freeze
   - define one bounded repeated-operation envelope for the chosen path:
     - repeated create / failover / recover / steady-state cycles
     - repeated heartbeat / control / recovery interaction
     - repeated publication / ownership convergence checks
   - define what counts as state drift versus expected bounded churn
2. Step 2: harness and evidence path
   - build or adapt one repeatable soak/repeated-cycle harness on the accepted chosen path
   - collect stable end-of-cycle truth rather than only transient pass/fail output
3. Step 3: proof package
   - prove no hidden state drift across repeated cycles
   - prove no unbounded growth/leak in the bounded chosen-path runtime state
   - prove no-overclaim around diagnosability, perf, or production rollout

Required scope:

1. repeated-cycle correctness on the accepted chosen path
2. stable end-of-cycle ownership/control/publication truth after many cycles
3. bounded runtime-state hygiene across repeated operation
4. explicit distinction between acceptance evidence and support telemetry

Must prove:

1. repeated chosen-path cycles converge to the same bounded truth rather than accumulating semantic drift
2. registry / VS-visible / product-visible state remain mutually coherent after repeated cycles
3. repeated operation does not leave unbounded leftover tasks, sessions, or stale runtime ownership artifacts within the tested envelope
4. acceptance wording stays bounded to long-run stability rather than diagnosability/perf/launch claims

Reuse discipline:

1. `weed/server/qa_block_*test.go`, `block_recovery_test.go`, and related hardening tests may be updated in place as the primary repeated-cycle proof surface
2. testrunner / infra / metrics helpers may be reused as support instrumentation, but support telemetry must not replace acceptance assertions
3. `weed/server/master_block_failover.go`, `master_block_registry.go`, `volume_server_block.go`, and `block_recovery.go` may be updated in place only if repeated-cycle hardening exposes a real bug
4. `weed/storage/blockvol/*` and `weed/storage/blockvol/v2bridge/*` remain reference only unless soak evidence exposes a real accepted-path mismatch
5. no reused V1 surface may silently redefine the chosen-path steady-state truth, drift criteria, or soak acceptance wording

Verification mechanism:

1. one bounded repeated-cycle or soak harness on the chosen path
2. explicit end-of-cycle assertions for ownership/control/publication truth
3. explicit checks for bounded runtime-state hygiene after repeated cycles
4. no-overclaim review so `P2` does not absorb `P3` diagnosability or `P4` perf/rollout work

Hard indicators:

1. one accepted repeated-cycle proof:
   - the chosen path completes many bounded cycles without semantic drift
   - end-of-cycle truth remains coherent after each cycle
2. one accepted state-hygiene proof:
   - no unbounded leftover runtime artifacts accumulate within the tested envelope
3. one accepted long-run stability proof:
   - stability claims are based on repeated evidence, not one-shot reruns
4. one accepted boundedness proof:
   - `P2` claims soak/long-run stability only, not diagnosability, perf, or rollout readiness

Reject if:

1. evidence is only a renamed rerun of `P1` disturbance tests
2. the slice counts iterations but never checks end-of-cycle truth for drift
3. support telemetry is presented without a hard acceptance assertion
4. `P2` quietly absorbs diagnosability, perf, or launch-readiness claims

Status:

- accepted

Carry-forward from `P1`:

1. bounded restart/disturbance correctness is now accepted on the chosen path
2. `P2` now asks whether that accepted path stays stable across repeated operation without hidden drift
3. later `P3` / `P4` remain distinct acceptance objects and should not be absorbed into `P2`

### P3: Diagnosability / Blocker Accounting / Runbook Hardening

Goal:

- make failures, residual blockers, and operator-visible diagnosis quality explicit and reviewable on the accepted chosen path

Acceptance object:

1. `P3` accepts bounded diagnosability / blocker accounting on the chosen path
2. it does not accept merely that some logs or debug strings exist
3. it does not accept performance floor or rollout readiness by implication

Execution steps:

1. Step 1: diagnosability contract freeze
   - define one bounded diagnosis envelope for the accepted chosen path:
     - failover / recovery does not converge in time
     - publication / lookup truth does not match authority truth
     - residual runtime work or stale ownership artifacts remain after an operation
     - known production blockers remain open and must be made explicit
   - define what counts as operator-visible diagnosis versus engineer-only source spelunking
2. Step 2: evidence-surface and blocker-ledger hardening
   - identify or harden the minimum operator-visible surfaces needed to classify the bounded failure classes
   - make residual blockers explicit, finite, and reviewable rather than implicit tribal knowledge
3. Step 3: proof package
   - prove at least one bounded diagnosis loop closes from symptom to owning truth/blocker
   - prove blocker accounting is explicit and does not hide unknown gaps behind “hardening later” language
   - prove no-overclaim around perf, launch readiness, or broad topology support

Required scope:

1. operator-visible symptoms/logs/status for bounded chosen-path failure classes
2. one explicit mapping from symptom to ownership/control/runtime/publication truth
3. one explicit blocker ledger for unresolved production-hardening gaps
4. bounded runbook guidance for diagnosis of the accepted chosen path

Must prove:

1. bounded chosen-path failures can be distinguished with explicit operator-visible evidence rather than debugger-only knowledge
2. at least one diagnosis loop closes from visible symptom to the relevant authority/runtime truth without semantic ambiguity
3. residual blockers are explicit, finite, and named with a clear boundary rather than scattered across chats or memory
4. acceptance wording stays bounded to diagnosability / blocker accounting rather than perf or rollout claims

Reuse discipline:

1. `weed/server/qa_block_*test.go`, `block_recovery*_test.go`, and focused hardening tests may be updated in place where they can prove a bounded diagnosis loop on the accepted path
2. `weed/server/master_block_registry.go`, `master_block_failover.go`, `volume_server_block.go`, and `block_recovery.go` may be updated in place only if diagnosability work exposes a real visibility gap in accepted-path behavior
3. lightweight status/logging surfaces and bounded runbook docs may be updated in place as support artifacts, but support artifacts must not replace acceptance assertions
4. `weed/storage/blockvol/*` and `weed/storage/blockvol/v2bridge/*` remain reference only unless diagnosability work exposes a real accepted-path mismatch
5. no reused V1 surface may silently redefine chosen-path truth, blocker boundaries, or diagnosis acceptance wording

Verification mechanism:

1. one bounded diagnosis-loop proof on the accepted chosen path
2. one explicit blocker ledger or equivalent review artifact with finite named items
3. one explicit check that operator-visible evidence matches the underlying accepted truth being diagnosed
4. no-overclaim review so `P3` does not absorb `P4` perf/rollout work

Hard indicators:

1. one accepted symptom-classification proof:
   - bounded failure classes can be told apart by explicit operator-visible evidence
2. one accepted diagnosis-loop proof:
   - a visible symptom can be traced to the relevant ownership/control/runtime/publication truth
3. one accepted blocker-accounting proof:
   - unresolved blockers are explicit, finite, and reviewable
4. one accepted boundedness proof:
   - `P3` claims diagnosability / blockers only, not perf floor or rollout readiness

Reject if:

1. the slice merely adds logs or debug strings without proving diagnostic usefulness
2. blockers remain implicit, scattered, or dependent on private memory of prior chats
3. diagnosis requires debugger/source-level spelunking instead of bounded operator-visible evidence
4. `P3` quietly absorbs perf, rollout, or broad product/topology expansion claims

Status:

- accepted

Carry-forward from `P2`:

1. bounded restart/disturbance correctness and bounded long-run stability are now accepted on the chosen path
2. `P3` now asks whether bounded failures and residual gaps are explicit and diagnosable in operator-facing terms
3. later `P4` remains a distinct acceptance object and should not be absorbed into `P3`

### P4: Performance Floor / Rollout Gates

Goal:

- define explicit performance floor, cost characterization, and rollout-gate criteria without letting perf claims replace correctness hardening

Acceptance object:

1. `P4` accepts a bounded performance floor and a bounded rollout-gate package for the accepted chosen path
2. it does not accept generic “performance is good” prose or one-off fast runs
3. it does not accept broad production rollout readiness outside the explicitly named launch envelope

Execution steps:

1. Step 1: performance-floor contract freeze
   - define one bounded workload envelope for the accepted chosen path
   - define which metrics count as acceptance evidence:
     - throughput / latency floor
     - resource-cost envelope
     - disturbance-free steady-state behavior
   - define which metrics are support-only telemetry
2. Step 2: benchmark and cost characterization
   - run one repeatable benchmark package against the accepted chosen path
   - record measured floor values and cost trade-offs rather than “fast enough” wording
3. Step 3: rollout-gate package
   - translate accepted correctness, soak, diagnosability, and perf evidence into one bounded launch envelope
   - make explicit which blockers are cleared, which remain, and what the first supported rollout shape is

Required scope:

1. one bounded benchmark matrix on the accepted chosen path
2. one explicit performance floor statement backed by measured evidence
3. one explicit resource-cost characterization
4. one rollout-gate / launch-envelope artifact with finite named requirements and exclusions

Must prove:

1. performance claims are tied to a named workload envelope rather than generic optimism
2. the chosen path has a measurable minimum acceptable floor within that envelope
3. rollout discussion is bounded by explicit gates and supported scope, not implied from prior slice acceptance
4. acceptance wording stays bounded to performance floor / rollout gates rather than broad production success claims

Reuse discipline:

1. `weed/server/qa_block_*test.go`, testrunner scenarios, and focused perf/support harnesses may be updated in place as the primary measurement surface
2. `weed/server/*`, `weed/storage/blockvol/*`, and `weed/storage/blockvol/v2bridge/*` may be updated in place only if performance-floor work exposes a real bug or a measurement-surface gap
3. `sw-block/.private/phase/` docs may be updated in place for the rollout-gate artifact and measured envelope
4. support telemetry may help characterize cost, but support telemetry must not replace the explicit floor/gate assertions
5. no reused V1 surface may silently redefine chosen-path truth, launch envelope, or rollout-gate wording

Verification mechanism:

1. one repeatable bounded benchmark package on the accepted chosen path
2. one explicit measured floor summary with named workload and cost envelope
3. one explicit rollout-gate artifact naming:
   - supported launch envelope
   - cleared blockers
   - remaining blockers
   - reject conditions for rollout
4. no-overclaim review so `P4` does not turn into generic launch optimism

Hard indicators:

1. one accepted performance-floor proof:
   - measured floor values exist for the named workload envelope
2. one accepted cost-characterization proof:
   - resource/replication tax or similar bounded cost is explicit
3. one accepted rollout-gate proof:
   - the first supported launch envelope is explicit and finite
4. one accepted boundedness proof:
   - `P4` claims only the bounded floor/gates it actually measures

Reject if:

1. the slice presents isolated benchmark numbers without a named workload contract
2. rollout gates are replaced by vague “looks ready” wording
3. support telemetry is presented without an explicit acceptance threshold or gate
4. `P4` quietly absorbs broad new topology, product-surface, or generic ops-tooling expansion

Status:

- active

Carry-forward from `P3`:

1. bounded disturbance correctness, bounded soak stability, and bounded diagnosability / blocker accounting are now accepted on the chosen path
2. `P4` now asks whether that accepted path has an explicit measured floor and an explicit first-launch envelope
3. later work after `Phase 12` should be a productionization program, not another hidden hardening slice

## Assignment For `sw`

Current next tasks:

1. deliver `Phase 12 P4` as bounded performance-floor / rollout-gate hardening
2. keep the acceptance object fixed on one measured workload envelope plus one explicit launch-envelope / gate artifact
3. keep earlier accepted `Phase 09` / `Phase 10` / `Phase 11` / `Phase 12 P1` / `Phase 12 P2` / `Phase 12 P3` semantics stable unless `P4` exposes a real bug or measurement-surface gap
4. do not let `P4` turn into broad optimization, topology expansion, or generic launch marketing

## Assignment For `tester`

Current next tasks:

1. validate `P4` as real bounded performance-floor / rollout-gate hardening rather than “benchmark numbers exist” prose
2. require explicit validation targets for:
   - one named workload envelope with measured floor values
   - one explicit launch-envelope / rollout-gate artifact
   - no-overclaim around broad production readiness beyond the named envelope
3. keep no-overclaim active around accepted `Phase 09` / `Phase 10` / `Phase 11` / `Phase 12 P1` / `Phase 12 P2` / `Phase 12 P3` closure
4. keep `P4` bounded rather than letting it absorb post-Phase-12 productionization work
