# V2 Phase Development Plan

Date: 2026-04-02
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

Current accepted path through `Phase 09`:

1. protocol/algo truth set is strong
2. engine recovery core is strong
3. real control delivery exists on the chosen path
4. real one-chain catch-up and rebuild closure exist on the chosen path
5. production-grade execution closure is accepted on the chosen path:
  - real `TransferFullBase`
  - real `TransferSnapshot`
  - real `TruncateWAL`
  - stronger live runtime ownership on the volume-server path
6. unified hardening validation exists on the chosen path
7. one bounded accepted path exists for:
  - `RF=2`
  - `sync_all`
  - existing master / volume-server heartbeat path

Phase-accounting note:

1. `Phase 08` is closed
2. `Phase 09` is also closed
3. this roadmap should now be read from the post-`Phase 09` state, not the post-`Phase 08` state

This means the next phases should focus mainly on:

1. stronger control-plane closure
2. later product-surface rebinding
3. production hardening
4. bounded cleanup of low-severity residuals without reopening accepted execution semantics

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
5. `P4` active:
   - acceptance object = bounded performance floor / rollout-gate hardening on the chosen path
   - not broad rollout readiness beyond the named launch envelope

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

After `Phase 12`:

1. move to a productionization program, not more protocol-discovery phases
2. freeze production blockers and the supported launch envelope
3. run a limited internal pilot with incident-driven hardening
4. perform controlled rollout only after explicit launch-gate review

## Module Status Map


| Module area                                                   | Current status               | Current owner phase      | Next target phase | Notes                                                                                      |
| ------------------------------------------------------------- | ---------------------------- | ------------------------ | ----------------- | ------------------------------------------------------------------------------------------ |
| `sw-block/engine/replication` core FSM/orchestrator/driver    | Strong                       | `Phase 09` accepted      | `Phase 10-12`     | Main later work is control-plane/runtime integration and hardening, not new core semantics. |
| Engine executor real I/O boundary (`CatchUpIO` / `RebuildIO`) | Strong on chosen path        | `Phase 09` accepted      | `Phase 10/12`     | Keep the boundary stable; later work is orchestration/control proof and hardening.          |
| `weed/storage/blockvol/v2bridge/control.go`                   | Strong on chosen path        | `Phase 08/09/10` accepted | `Phase 12`       | Chosen-path control mapping is accepted; later work is hardening, not new mapping semantics. |
| `weed/storage/blockvol/v2bridge/reader.go`                    | Strong                       | `Phase 09` accepted      | `Phase 12`        | Mostly stable; later work is verification/hardening, not new semantics.                     |
| `weed/storage/blockvol/v2bridge/pinner.go`                    | Strong                       | `Phase 09` accepted      | `Phase 12`        | Retention safety proven; later work is hardening under disturbance.                         |
| `weed/storage/blockvol/v2bridge/executor.go` WAL scan         | Strong                       | `Phase 09` accepted      | `Phase 12`        | Real execution path closed on chosen path; later work is hardening.                         |
| `v2bridge` `TransferFullBase`                                 | Strong on chosen path        | `Phase 09 P1` accepted   | `Phase 12`        | Execution closure accepted; do not reopen casually.                                         |
| `v2bridge` `TransferSnapshot`                                 | Strong on chosen path        | `Phase 09 P2` accepted   | `Phase 12`        | Execution closure accepted; do not reopen casually.                                         |
| `v2bridge` `TruncateWAL`                                      | Strong on chosen path        | `Phase 09 P3` accepted   | `Phase 12`        | Narrow Option A contract accepted; later work is hardening/planning improvement.            |
| `weed/server/volume_server_block.go` V2 assignment intake     | Strong on chosen path        | `Phase 10 P4` accepted   | `Phase 11/12`     | VS-side ingress, convergence, idempotence, and bounded master-driven closure are accepted; later work is product-surface integration and hardening. |
| `weed/server/block_recovery.go` live runtime ownership        | Strong on chosen path        | `Phase 09/10` accepted   | `Phase 11/12`     | Serialized ownership and bounded control-plane integration are accepted; later work is product-surface integration and hardening. |
| `blockvol` WAL/flusher/checkpoint runtime                     | Reuse reality                | Existing production code | `Phase 12`        | Reuse implementation; later work is disturbance/restart hardening.                          |
| `blockvol` rebuild transport/server reality                   | Reuse with redesign boundary | Existing production code | `Phase 12`        | Bounded chosen-path integration is accepted; later work is hardening under disturbance.     |
| local server identity (`localServerID`)                       | Strong on chosen path        | `Phase 10 P1` accepted   | `Phase 12`        | Canonical `volumeServerId` now backs chosen-path local identity; later work is hardening only. |
| Snapshot product path                                         | Strong on chosen path        | `Phase 11` accepted      | `Phase 12`        | Product-visible snapshot create/list/delete closure and restore workflow closure are accepted; later work is hardening, not rebinding. |
| `CSI` integration                                             | Strong on chosen path        | `Phase 11` accepted      | `Phase 12`        | Bounded controller/node lifecycle rebinding accepted; later work is hardening.              |
| `NVMe` / `iSCSI` front-ends                                   | Strong on chosen path        | `Phase 11` accepted      | `Phase 12`        | Publication/address truth rebinding accepted; later work is runtime/perf hardening.         |
| Testrunner / infra / metrics                                  | Strong support layer         | existing                 | `Phase 11-12`     | Reuse to validate later product-surface and hardening phases.                               |


## Completion-State Targets

Use these rough targets to judge whether a phase is moving the product meaningfully.


| Phase      | Expected completion move                                                      |
| ---------- | ----------------------------------------------------------------------------- |
| `Phase 09` | from validation-grade backend execution to accepted execution closure on the chosen path |
| `Phase 10` | from bounded control-entry proof to stronger end-to-end control-plane closure |
| `Phase 11` | from backend-ready path to selected product-surface readiness                 |
| `Phase 12` | from candidate-safe to production-safe                                        |


## Near-Term Execution Direction

If the goal is to maximize product completion efficiently, the recommended order is:

1. keep `Phase 09` closed and do not reopen accepted execution semantics casually
2. keep `Phase 10` closed and do not reopen accepted bounded control-plane closure casually
3. move next to `Phase 11` product surface rebinding
4. then `Phase 12` production hardening

The most important near-term engineering weight should now go to `Phase 12`.

## Short Summary

The V2 line now has accepted execution closure on one bounded chosen path.
The next development plan should treat later work as control/product completion phases, not more protocol discovery.

The main heavy engineering work still ahead is:

1. stronger end-to-end control-plane closure
2. later product-surface rebinding
3. production hardening
4. bounded cleanup of residual operational rough edges without reopening accepted semantics

