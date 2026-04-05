# V2 Phase Development Plan

Date: 2026-04-04
Status: active
Purpose: define the execution-oriented phase plan after the current candidate-path work, with explicit module status and target phase ownership

## Why This Document Exists

The project now needs a development plan that is:

1. phase-oriented
2. execution-oriented
3. large enough to avoid overhead-heavy micro-slices
4. explicit about which module belongs to which future phase

This document is the planning bridge between:

1. `v2-product-completion-overview.md`
2. `../.private/phase/phase-08.md`
3. future implementation phases

## Planning Rules

Use these rules for all later phases:

1. one phase should close one meaningful product/engineering outcome
2. every phase must have a clear delivery object and a clear closed-loop validation mechanism
3. every slice inside a phase should also name:
   - what is delivered
   - what loop is proven closed
   - what reject shapes remain insufficient
4. phases should prefer real code/test/evidence over wording-only progress
5. later phases may reuse V1 engineering reality, but must not inherit V1 recovery semantics as truth
6. a phase is too small if it does not move the overall product-completion state clearly

## Current Baseline

Current accepted path now runs through the `Phase 16` finish-line checkpoint:

1. protocol/algo truth set is strong
2. engine recovery core is strong on the chosen path
3. control-plane closure is accepted on the chosen path
4. selected product-surface rebinding is accepted on the chosen path
5. bounded production hardening is accepted on the chosen path
6. explicit `V2 core` extraction and adapter/projection rebinding are accepted as
   bounded engineering structure
7. one bounded `V2`-native runtime checkpoint is now accepted for:
   - `RF=2`
   - `sync_all`
   - existing master / volume-server heartbeat path
   - `blockvol` as execution backend
8. `Phase 13` froze the bounded `WAL V1.5` contract package and `Phase 14-16`
   carried that package forward into a bounded runtime-owned checkpoint:
   - real-workload package accepted
   - assignment/publication closure accepted
   - bounded mode normalization accepted
   - bounded heartbeat/restart truth closure accepted

Phase-accounting note:

1. `Phase 08` is closed
2. `Phase 09` is closed
3. `Phase 10` is closed
4. `Phase 11` is closed
5. `Phase 12` is the accepted hardening baseline for the chosen path
6. `Phase 13` is closed and should be read as one bounded constrained-runtime contract package, not as launch approval
7. `Phase 14` is closed as explicit `V2 core` extraction
8. `Phase 15` is closed as bounded adapter/projection rebinding
9. `Phase 16` is closed as a bounded `V2`-native runtime checkpoint, not as broad product/launch proof
10. the immediate next planning focus is now the productionization program plus the larger post-`Phase 16` gates:
   - broader recovery/failover/publication statement
   - launch-envelope freeze
   - pilot/rollout discipline

Important interpretation rule:

1. the accepted chosen path and claim/evidence set are real
2. the current `weed/` runtime structure is not automatically the final `V2` structure
3. even after the `Phase 16` finish-line checkpoint, current integrated evidence should still be read as:
   - one bounded `V2`-owned runtime path on the chosen integration
   - not broad proof for every recovery/failover/disturbance or launch scenario
4. future phases must treat:
   - `v2-protocol-claim-and-evidence.md` as current claim authority
   - `v2_mini_core_design.md` as engineering-structure authority
   - `v2-reuse-replacement-boundary.md` as reuse vs replacement authority

This means the next work should focus mainly on:

1. keeping accepted `Phase 14-16` closure closed
2. turning the bounded runtime checkpoint into an explicit launch envelope
3. deciding broader failover/recovery/publication claims only through named evidence
4. using `Phase 13-16` evidence as acceptance input rather than reopening micro-seams by default

## Phase Roadmap

### Phase 09: Production Execution Closure

Goal:

1. turn validation-grade backend execution into production-grade backend execution

Must prove:

1. full-base rebuild performs real data transfer
2. snapshot rebuild performs real image transfer
3. replica-ahead path is physically executable, not only detected
4. runtime execution ownership is stronger than the current bounded candidate path

Typical outputs:

1. real `TransferFullBase`
2. real `TransferSnapshot`
3. real `TruncateWAL`
4. stronger executor/runtime integration in the live volume-server path

Verification mechanism:

1. one-chain execution tests on real backend paths
2. cleanup assertions after success/failure/cancel
3. focused adversarial tests for truncation and rebuild execution

Workload:

1. large
2. this is likely the single biggest remaining engineering phase

Status:

1. complete
2. accepted closeout exists in `../.private/phase/phase-09.md`

### Phase 10: Real Control-Plane Closure

Goal:

1. strengthen from accepted assignment-entry closure to fuller end-to-end control-plane closure

Why this is next:

1. `Phase 09` already closed the main backend execution gaps
2. the most important remaining product risk is no longer storage execution itself
3. it is now control-path completeness:
   - heartbeat / gRPC delivery
   - reassignment / result convergence
   - cleaner local identity than transport-shaped `listenAddr`
4. `Phase 10` can also absorb bounded low-severity cleanup discovered during `Phase 09` if it is directly relevant to live control/runtime ownership

Current accepted progress inside `Phase 10`:

1. `P1` accepted:
   - stable identity and control-truth closure on the chosen block assignment wire
2. `P2` accepted:
   - reassignment/result convergence through the accepted volume-server-side chosen-path ingress
3. `P3` accepted:
   - bounded repeated-assignment / idempotence cleanup on the chosen live path
4. `P4` accepted:
   - master-driven heartbeat / gRPC control-loop closure on the chosen path
5. `Phase 10` is now closed:
   - bounded end-to-end control-plane closure for the chosen path is accepted

Must prove:

1. heartbeat/gRPC-level delivery is real for the chosen path
2. failover / reassignment state converges through the real control path
3. local and remote identity are consistent enough for product use

Typical outputs:

1. stronger heartbeat/gRPC delivery proof
2. stronger result/reporting convergence
3. cleaner local identity than transport-shaped `listenAddr`

Verification mechanism:

1. real failover/reassignment tests at the fuller control-plane level
2. identity/fencing assertions through the end-to-end path

Suggested first targets:

1. keep accepted `Phase 10` control-plane closure closed
2. start `Phase 11` with one bounded product-surface rebinding slice
3. prefer selected surface proofs over broad surface explosion
4. keep any residual control-path cleanup narrow; do not reopen accepted `Phase 10` closure casually

Workload:

1. medium-large

### Phase 11: Product Surface Rebinding

Goal:

1. bind product-facing surfaces onto the V2-backed block path after backend closure is strong enough

Must prove:

1. the V2-backed backend can support selected product surfaces without semantic drift
2. reuse of V1 surfaces does not reintroduce V1 recovery truth

Candidate areas:

1. snapshot product path
2. `CSI`
3. `NVMe`
4. `iSCSI`

Recommended first slice:

1. start with bounded `snapshot product path` rebinding
2. defer `CSI` and `NVMe` / `iSCSI` until one simpler product-visible surface is already accepted

Suggested slice order:

1. `P1` snapshot product-path rebinding
2. `P2` `CSI` rebinding
3. `P3` `NVMe` / `iSCSI` front-end rebinding
4. `P4` broader workflow closure such as snapshot restore/clone if still needed

Verification mechanism:

1. selected surface integration tests
2. product-surface contract checks
3. no-overclaim review that the surface does not imply unsupported backend capability

Workload:

1. medium-large
2. can be split by product surface if needed, but only after backend closure is strong

### Phase 12: Production Hardening

Goal:

1. move from candidate-safe to production-safe

Must prove:

1. restart/recovery stability under repeated disturbance
2. long-run/soak viability
3. operational diagnosability
4. acceptable production blockers list or production-ready gate

Verification mechanism:

1. soak/adversarial runs
2. failover/restart under disturbance
3. runbook/debug validation

Workload:

1. large

Recommended initial planning cut:

1. treat `P0` as hardening-plan freeze
2. first hardening slice should likely target restart / recovery disturbance before soak or perf

Current slice order:

1. `P0` hardening-plan freeze
2. `P1` restart / recovery disturbance hardening
3. `P2` soak / long-run stability hardening
4. `P3` diagnosability / blocker accounting / runbook hardening
5. `P4` performance floor / rollout-gate hardening

Slice delivery / closed-loop bar:

1. every `Phase 12` slice must end with:
   - one bounded delivery object
   - one bounded closed-loop validation object
   - one explicit no-overclaim boundary
2. “tests exist” is not enough:
   - the tests must close the loop from disturbance/input to visible accepted truth
3. “code changed” is also not required:
   - a hardening slice may legitimately close by proving existing production code is already correct under the targeted disturbance class

Current status:

1. `P0` accepted:
   - hardening object frozen as the accepted chosen path from `Phase 09` + `Phase 10` + `Phase 11`
   - slice order frozen as `P1` / `P2` / `P3` / `P4`
   - evidence ladder frozen as disturbance correctness, soak, diagnosability, then perf/rollout gates
2. `P1` accepted:
   - acceptance object = correctness under restart/disturbance on the chosen path
   - not soak, not diagnosability, not performance, not rollout readiness
3. `P2` accepted:
   - acceptance object = bounded soak / long-run stability on the chosen path
   - repeated-cycle coherence and bounded runtime-state hygiene are accepted inside a bounded test envelope
4. `P3` accepted:
   - acceptance object = bounded diagnosability / blocker accounting / runbook hardening on the chosen path
   - bounded operator-visible diagnosis surfaces and finite blocker accounting are accepted
5. `P4` accepted:
   - acceptance object = bounded performance floor / rollout-gate hardening on the chosen path
   - not broad rollout readiness beyond the named launch envelope
6. `Phase 12` is now closed:
   - one bounded hardening package is accepted for the chosen path

Current `P4` first delivery shape:

1. proof-first hardening slice with explicit measured floor and launch-gate artifacts
2. one bounded performance package:
   - named workload envelope
   - repeatable measurement harness
   - explicit floor values
3. one explicit rollout-gate artifact:
   - finite supported launch envelope
   - cleared blockers/gates
   - remaining blockers/gates
4. current evidence shape:
   - measured floor values are tied to one named accepted workload envelope
   - cost/resource trade-offs are explicit
   - rollout discussion is bounded by an explicit finite gate package
5. current reuse boundary:
   - accepted chosen-path runtime/control/product surfaces remain stable unless perf-floor work exposes a real bug or measurement gap
   - focused benchmarks/tests and bounded launch-gate artifacts carry the main delivery burden

Closed-loop expectation for `P4` review:

1. one bounded workload envelope runs on the accepted chosen path
2. measured floor values and cost characteristics are explicit
3. launch claims map back to accepted prior slices plus the measured envelope
4. remaining rollout blockers are explicit and finite
5. claims remain bounded to measured floor / named launch envelope only

### Phase 13: V1.5 Contract Closure And Contradiction Ledger

Goal:

1. close the bounded `RF=2 sync_all` contract on the current chosen path
2. freeze what `WAL V1.5` is allowed to claim
3. classify remaining live contradictions as:
   - reusable-core bug
   - adapter-boundary bug
   - `V2`-authority bug

Delivery object:

1. one frozen `CP13-*` contract package
2. one centralized claim/evidence ledger for the active chosen path
3. one explicit contradiction/rerun queue for invalidated or narrowed evidence

Closed-loop validation:

1. protocol/unit/adversarial proofs for `CP13-1..7`
2. bounded real-workload validation, assignment/publication closure, and mode normalization are all accepted
3. explicit narrowing or restoration of claims in the centralized ledger

Non-claims:

1. not full `V2 core` extraction
2. not launch approval
3. not proof that current `weed/` structure is the final `V2` structure

Key files / ownership:

1. `sw-block/.private/phase/phase-13-*.md`
2. `sw-block/design/v2-protocol-claim-and-evidence.md`
3. `sw-block/design/v2-protocol-truths.md`

### Phase 14: V2 Core Extraction

Goal:

1. make the `V2 core` explicit as a long-term code structure
2. stop relying on implicit semantic ownership spread across runtime files

Execution rule:

1. define core-owned state and transitions first
2. freeze command-emission rules second
3. freeze projection contracts third
4. only then connect adapters

Delivery object:

1. one explicit `V2 core` package/file layout with named:
   - `state`
   - `event`
   - `command`
   - `projection`
2. one minimal real code path for:
   - `ApplyEvent()`
   - `Decide()`
   - `EmitCommands()`
3. one bounded parity package showing accepted prototype/FSM semantics are preserved

Closed-loop validation:

1. focused engine tests proving accepted claims can be represented through the new event/command core
2. parity checks against accepted prototype/FSM semantics
3. no-overclaim review that this is structural extraction, not live-path cutover

Recommended slice order:

1. `Phase 14A`: core-owned automata
   - explicit assignment / recovery / boundary / mode / publication automata
   - structural tests only
2. `Phase 14B`: command semantics
   - bounded command sequences derived from semantic state
   - still no live `weed/` execution
3. `Phase 14C`: projection contracts
   - lookup / heartbeat / debug / tester normalization from core-owned state
   - surface-consistency tests

Immediate focus inside `Phase 14`:

1. start from one complete semantic chain:
   - `mode`
   - `readiness`
   - `publication`
2. use accepted `CP13-8A` and `CP13-9` as the first hard input package

Non-claims:

1. not full live-path cutover
2. not replacement of all `weed/` logic
3. not a separate process yet

Key files / ownership:

1. `sw-block/design/v2_mini_core_design.md`
2. `sw-block/design/v2-phase14plus-semantic-framework.md`
3. `sw-block/engine/replication/`

Current status:

1. complete as a bounded engineering-structure phase
2. accepted output is explicit `state / event / command / projection` ownership,
   not broad live-runtime or launch proof

### Phase 15: Adapter And Projection Rebinding

Goal:

1. make `weed/` a bounded adapter/projection layer instead of mixed semantic authority
2. close assignment -> readiness -> publication through named `V2` state

Delivery object:

1. one explicit adapter-boundary package on the live path
2. one explicit projection store / projection surface package for:
   - readiness
   - publication
   - diagnostics
3. one narrowed role definition where:
   - `BlockService` is closer to command executor
   - registry is closer to projection store

Closed-loop validation:

1. live-path tests proving assignment delivered != receiver ready != publish healthy unless the named readiness/projection loop is closed
2. focused regression package for the `CP13-8A` bug class
3. operator-visible projection checks rather than internal-state-only proof

Recommended slice order:

1. `Phase 15A`: minimal adapter hook
   - one narrow event path into the core
   - one bounded command path back out
   - prove no semantic split on that narrow path
2. `Phase 15B`: projection-store rebinding
   - registry / lookup / tester-facing surfaces consume core-owned projection truth
   - prove assignment delivered != ready != publish healthy on the real path

Non-claims:

1. not backend rewrite
2. not broader productization
3. not new transport matrix claims

Key files / ownership:

1. `sw-block/design/v2-reuse-replacement-boundary.md`
2. `sw-block/design/v2-phase14plus-semantic-framework.md`
3. `weed/server/volume_server_block.go`
4. `weed/server/block_heartbeat_loop.go`
5. `weed/server/master_block_registry.go`
6. `weed/server/master_block_failover.go`

Current status:

1. complete as bounded adapter/projection rebinding on the chosen path
2. accepted output is explicit adapter/projection ownership, not broad
   runtime-closure or launch proof

### Phase 16: V2-Native Runtime Closure

Goal:

1. make the integrated runtime behave as a `V2`-owned recovery/control system
2. stop depending on a merely improved `WAL V1.5` path for correctness interpretation

Execution precondition:

1. do not enter `Phase 16` until `Phase 14` has frozen state / command / projection semantics
2. do not treat adapter rebinding alone as runtime closure

Delivery object:

1. one bounded product/runtime path where failover, recovery, publication, and selected surfaces are driven by the `V2 core` + adapter contract
2. one explicit runtime integration package that maps simulator/prototype failure classes to live-path behavior

Closed-loop validation:

1. end-to-end failover/recovery scenarios on the core-driven path
2. simulator-to-runtime consistency checks for the named failure classes that `V2` is supposed to survive
3. bounded real-workload checks on the core-driven path, not just on the legacy-integrated path

Non-claims:

1. not broad rollout approval
2. not physical split into an independent `V2 core process` unless the logic is already structurally independent

Key files / ownership:

1. `sw-block/engine/replication/`
2. `weed/server/`
3. selected `weed/storage/blockvol/v2bridge/*` files

Current status:

1. complete as a bounded runtime checkpoint through the `Phase 16` finish line
2. accepted checkpoint now covers:
   - steady-state and bounded restart reconstruction preserve accepted explicit
     truth on the chosen path
   - sparse heartbeats do not silently erase already accepted truth
   - empty full-inventory delete behavior is explicit rather than heuristic
3. non-claims remain explicit:
   - not broad recovery-loop closure
   - not broad failover/publication proof
   - not launch / rollout readiness

### Cross-Phase Review Rule For `Phase 14+`

For any new transition, command, or projection rule in `Phase 14+`, require a
short justification in the delivery note or code review:

1. semantic constraint satisfied
   - which `claim / truth / CP13-*` item it is implementing
2. overclaim avoided
   - which false healthy / ready / durable / recoverable interpretation is being prevented
3. proof preserved
   - which accepted test or checkpoint remains valid because of the rule

### Productionization Program After `Phase 16`

Goal:

1. turn the accepted `Phase 16` bounded path into a bounded first-launch product envelope without reopening protocol discovery or core-ownership questions

Program slices:

1. Program `P0`: launch-envelope freeze
   - freeze the first supported launch envelope from accepted hardening + runtime-closure evidence
   - lock:
     - supported topology / transport matrix
     - explicit exclusions
     - launch-blocking vs post-launch blockers
   - reject if any launch claim outruns the measured matrix or accepted blockers/gates
2. Program `P1`: internal pilot pack
   - convert the frozen launch envelope into a limited internal pilot package
   - define:
     - pilot environment and topology
     - preflight checklist
     - success criteria
     - stop / rollback conditions
     - incident intake template tied to accepted diagnosability surfaces
   - reject if pilot success depends on tribal knowledge or undefined operator judgment
3. Program `P2`: incident-driven hardening loop
   - route pilot findings into explicit buckets:
     - config / environment issue
     - known exclusion
     - true product bug
   - keep one bounded incident ledger and one bounded fix queue
   - reject if incidents accumulate as vague notes or exclusions are silently redefined
4. Program `P3`: controlled rollout review
   - decide whether to:
     - stay in pilot
     - widen within the same launch envelope
     - block expansion
   - require explicit mapping from any expansion decision back to:
     - accepted `Phase 16` evidence
     - pilot outcomes
     - incident dispositions
   - reject if rollout broadens beyond the named envelope or reuses pilot success as generic production proof

Cross-cutting rules:

1. do not invent a `Phase 12 P5`; productionization remains separate from hardening
2. do not collapse `Phase 13-16` into generic productionization; they are engineering-structure phases
3. keep the accepted chosen path fixed unless contradiction or incident evidence exposes a real bug
4. treat known missing evidence as explicit constraints until cleared, especially:
   - failover-under-load performance
   - hours/days soak under load
   - `RF>2`
   - broad transport matrix
   - full gRPC-stream integration evidence
5. keep the roadmap aligned with:
   - `v2-protocol-claim-and-evidence.md`
   - `v2_mini_core_design.md`
   - `v2-reuse-replacement-boundary.md`

## Module Status Map


| Module area | Current status | Current owner phase | Next target phase | Notes |
| ----------- | -------------- | ------------------- | ----------------- | ----- |
| `sw-block/engine/replication` core FSM/orchestrator/driver | Strong long-term `V2 core` asset | `Phase 14` accepted, `Phase 16` runtime checkpoint accepted | Productionization / broader failover gate | Main next work is not new core extraction; it is using the accepted core as authority when deciding broader post-`Phase 16` claims. |
| Engine executor real I/O boundary (`CatchUpIO` / `RebuildIO`) | Strong on chosen path | `Phase 09` accepted, `Phase 16` integrated on bounded path | Productionization / broader recovery-loop gate | Keep the boundary stable; later work is broader runtime/failover evidence, not first implementation. |
| `weed/storage/blockvol/v2bridge/control.go` | Strong boundary adapter on chosen path | `Phase 15` accepted, `Phase 16` checkpoint accepted | Productionization | Remains a bridge between `V2` truth and runtime execution; should not accumulate new semantic authority casually. |
| `weed/storage/blockvol/v2bridge/reader.go` | Strong backend-facing adapter | `Phase 09` accepted, `Phase 16` bounded runtime checkpoint accepted | Productionization / broader disturbance hardening | Mostly stable; later work is evidence under wider disturbance classes, not new protocol semantics. |
| `weed/storage/blockvol/v2bridge/pinner.go` | Strong backend-facing adapter | `Phase 09` accepted, `Phase 16` bounded runtime checkpoint accepted | Productionization / broader disturbance hardening | Retention safety is proven on the chosen path; later work is long-window hardening rather than ownership redesign. |
| `weed/storage/blockvol/v2bridge/executor.go` WAL scan | Strong backend-facing adapter | `Phase 09` accepted, `Phase 16` bounded runtime checkpoint accepted | Productionization / broader recovery-loop gate | Real execution path is closed on the chosen path; later work is broader runtime evidence. |
| `v2bridge` `TransferFullBase` | Strong on chosen path | `Phase 09 P1` accepted, `Phase 16` bounded runtime checkpoint accepted | Productionization / broader recovery-loop gate | Execution closure is accepted; do not reopen casually unless broader runtime evidence exposes a real contradiction. |
| `v2bridge` `TransferSnapshot` | Strong on chosen path | `Phase 09 P2` accepted, `Phase 16` bounded runtime checkpoint accepted | Productionization / launch-envelope gate | Execution closure is accepted; later work is first supported-envelope accounting. |
| `v2bridge` `TruncateWAL` | Strong on chosen path | `Phase 09 P3` accepted, `Phase 16` bounded runtime checkpoint accepted | Productionization / broader recovery-loop gate | Narrow contract is accepted; later work is preserving that contract under broader disturbance evidence. |
| `weed/server/volume_server_block.go` V2 assignment intake | Adapter-boundary reality with accepted chosen-path closure | `Phase 15` accepted, `Phase 16` finish-line checkpoint accepted | Productionization / broader failover gate | This boundary is now explicit on the chosen path; next work is wider gate evidence, not another rebinding phase. |
| `weed/server/block_recovery.go` live runtime ownership | V2-owned runtime truth on chosen path | `Phase 16` finish-line checkpoint accepted | Productionization / broader recovery-loop gate | Serialized ownership is accepted on the bounded path; later work is proving more of the surrounding loop without widening claims casually. |
| `weed/server/master_block_registry.go` / failover / handlers | Bounded projection/truth closure accepted on chosen path | `Phase 15` accepted, `Phase 16` finish-line checkpoint accepted | Productionization / broader failover/publication gate | Heartbeat/restart truth-closure seams are closed on the bounded path; remaining work is broader gate evidence and launch scoping. |
| `blockvol` WAL/flusher/checkpoint runtime | Reuse reality | Existing production code, bounded path accepted through `Phase 16` | Productionization / broader disturbance hardening | Reuse implementation; do not let `V1` replication semantics redefine `V2` truth. |
| `blockvol` rebuild transport/server reality | Reuse with redesign boundary | Existing production code, bounded path accepted through `Phase 16` | Productionization / broader recovery-loop gate | Bounded chosen-path integration is accepted; later work is wider disturbance/failover evidence under `V2` authority. |
| local server identity (`localServerID`) | Strong chosen-path rule with narrowed semantics | `Phase 10 P1` accepted, carried through `Phase 16` | Productionization / launch-envelope gate | Stable identity must remain distinct from transport address shape; next work is supported-envelope/accounting, not semantic redesign. |
| Snapshot product path | Strong on chosen path | `Phase 11` accepted, preserved through `Phase 16` | Productionization / launch-envelope gate | Product-visible snapshot workflow is accepted on the chosen path; next work is first supported envelope and exclusions. |
| `CSI` integration | Strong on chosen path | `Phase 11` accepted, preserved through `Phase 16` | Productionization / launch-envelope gate | Bounded controller/node lifecycle rebinding is accepted; next work is supported-matrix/accounting rather than first rebinding. |
| `NVMe` / `iSCSI` front-ends | Strong on chosen path | `Phase 11` accepted, preserved through `Phase 16` | Productionization / launch-envelope gate | Publication/address truth rebinding is accepted; next work is envelope freeze and broader incident-driven hardening. |
| Testrunner / infra / metrics | Strong support layer | existing, used through `Phase 13-16` checkpointing | Productionization | Reuse to validate launch-envelope gates, pilot packs, incident buckets, and broader hardening claims. |


## Completion-State Targets

Use these rough targets to judge whether a phase is moving the product meaningfully.


| Phase | Expected completion move |
| ----- | ------------------------ |
| `Phase 09` | from validation-grade backend execution to accepted execution closure on the chosen path |
| `Phase 10` | from bounded control-entry proof to stronger end-to-end control-plane closure |
| `Phase 11` | from backend-ready path to selected product-surface readiness |
| `Phase 12` | from candidate-safe to production-safe on one bounded chosen path |
| `Phase 13` | from scattered `WAL V1.5` evidence to one frozen contract/contradiction ledger |
| `Phase 14` | from implicit core semantics to explicit `V2 core` package/engine structure |
| `Phase 15` | from mixed runtime semantics to explicit adapter/projection closure |
| `Phase 16` | from improved chosen-path runtime to one bounded `V2`-native runtime path |
| Productionization | from bounded runtime closure to bounded launch-envelope / pilot / rollout review |


## Near-Term Execution Direction

If the goal is to maximize product completion efficiently, the recommended order is now:

1. keep `Phase 09-16` accepted closures closed and do not reopen them casually
2. treat the `Phase 16` finish-line checkpoint as the bounded runtime stop-line,
   not as a reason to continue indefinite micro-slicing
3. move next to the productionization program:
   - `P0` launch-envelope freeze
   - `P1` limited internal pilot pack
   - `P2` incident-driven hardening loop
   - `P3` controlled rollout review
4. only reopen runtime logic if broader failover/recovery/publication evidence
   exposes a real contradiction
5. keep larger post-`Phase 16` gates explicit:
   - broader recovery-loop closure
   - broader failover/publication statement
   - long-window restart/disturbance hardening

The most important near-term engineering weight should now go to:

1. freezing the first supported launch envelope from accepted `Phase 12-16`
   evidence
2. deciding what broader failover/recovery/publication claims are actually
   supportable before widening product scope
3. keeping `weed/` changes bounded unless a real contradiction requires broader
   runtime work

## Short Summary

The V2 line now has an accepted bounded path through:

1. execution closure
2. control-plane closure
3. product-surface rebinding
4. bounded hardening
5. constrained-runtime contract freeze
6. explicit `V2 core` extraction
7. adapter/projection rebinding
8. bounded `Phase 16` runtime closure

The roadmap should now stop treating `Phase 14-16` as future work.
Their bounded checkpoint is complete.

From here the practical plan is:

1. freeze the first supported launch envelope
2. decide the larger post-`Phase 16` failover/recovery/publication claim boundary
3. run a limited internal pilot
4. harden from incidents without silently widening scope

