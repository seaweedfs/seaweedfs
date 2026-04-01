# V2 Phase Development Plan

Date: 2026-03-31
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
2. every phase must have a clear verification mechanism
3. phases should prefer real code/test/evidence over wording-only progress
4. later phases may reuse V1 engineering reality, but must not inherit V1 recovery semantics as truth
5. a phase is too small if it does not move the overall product-completion state clearly

## Current Baseline

Current accepted/closing path through `Phase 08`:

1. protocol/algo truth set is strong
2. engine recovery core is strong
3. real control delivery exists on the chosen path
4. real one-chain catch-up and rebuild closure exist on the chosen path
5. unified hardening validation exists on the chosen path
6. one bounded candidate statement exists for:
  - `RF=2`
  - `sync_all`
  - existing master / volume-server heartbeat path

Phase-accounting note:

1. this document assumes the current `Phase 08` path and bookkeeping are being closed consistently
2. if `Phase 08` bookkeeping is still open, read the candidate statement items above as the current accepted/closing path, not as a fully closed phase label

This means the next phases should focus mainly on:

1. production-grade execution completeness
2. stronger runtime ownership
3. stronger control-plane closure
4. later product-surface rebinding
5. production hardening

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

### Phase 10: Real Control-Plane Closure

Goal:

1. strengthen from accepted assignment-entry closure to fuller end-to-end control-plane closure

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

## Module Status Map


| Module area                                                   | Current status               | Current owner phase      | Next target phase | Notes                                                                                      |
| ------------------------------------------------------------- | ---------------------------- | ------------------------ | ----------------- | ------------------------------------------------------------------------------------------ |
| `sw-block/engine/replication` core FSM/orchestrator/driver    | Strong                       | `Phase 08` accepted      | `Phase 09`        | Main later work is runtime/product execution closure, not new core semantics.              |
| Engine executor real I/O boundary (`CatchUpIO` / `RebuildIO`) | Strong on chosen path        | `Phase 08 P2/P3`         | `Phase 09`        | Keep the boundary; make underlying transfer/truncate production-grade.                     |
| `weed/storage/blockvol/v2bridge/control.go`                   | Strong on chosen path        | `Phase 08 P1`            | `Phase 10`        | Next step is fuller control-plane closure, not new mapping semantics.                      |
| `weed/storage/blockvol/v2bridge/reader.go`                    | Strong                       | `Phase 08 P2/P3`         | `Phase 09/10`     | Keep comments/status aligned with candidate-path committed-truth decision.                 |
| `weed/storage/blockvol/v2bridge/pinner.go`                    | Strong                       | `Phase 08 P1/P3`         | `Phase 09`        | Retention safety proven; later work is product-grade execution under that safety.          |
| `weed/storage/blockvol/v2bridge/executor.go` WAL scan         | Strong                       | `Phase 08 P2`            | `Phase 09`        | Real scan is good; later work is real transfer/truncate completeness.                      |
| `v2bridge` `TransferFullBase`                                 | Partial                      | `Phase 08 P2/P4`         | `Phase 09`        | Validation-grade now; target is real production streaming.                                 |
| `v2bridge` `TransferSnapshot`                                 | Partial                      | `Phase 08 P2/P4`         | `Phase 09`        | Validation-grade now; target is real image transfer.                                       |
| `v2bridge` `TruncateWAL`                                      | Weak/stub                    | `Phase 08 P4` bound      | `Phase 09`        | Must become a real executable path.                                                        |
| `weed/server/volume_server_block.go` V2 assignment intake     | Medium-strong                | `Phase 08 P1`            | `Phase 09/10`     | Real intake exists; later work is stronger runtime ownership + fuller control-plane proof. |
| `blockvol` WAL/flusher/checkpoint runtime                     | Reuse reality                | Existing production code | `Phase 09`        | Reuse implementation; do not let old semantics redefine V2 truth.                          |
| `blockvol` rebuild transport/server reality                   | Reuse with redesign boundary | Existing production code | `Phase 09`        | Good area for production execution closure work.                                           |
| local server identity (`localServerID`)                       | Partial                      | `Phase 08` bounded       | `Phase 10`        | Still transport-shaped; should become cleaner under control-plane closure.                 |
| Snapshot product path                                         | Partial/reuse candidate      | not core in `Phase 08`   | `Phase 11`        | Reuse implementation, but V2 semantics own placement and claims.                           |
| `CSI` integration                                             | Deferred reuse candidate     | not core in `Phase 08`   | `Phase 11`        | Product surface, not next core closure target.                                             |
| `NVMe` / `iSCSI` front-ends                                   | Deferred reuse candidate     | not core in `Phase 08`   | `Phase 11`        | Rebind after backend path is stronger.                                                     |
| Testrunner / infra / metrics                                  | Strong support layer         | existing                 | `Phase 10-12`     | Reuse to validate later control-plane and hardening phases.                                |


## Completion-State Targets

Use these rough targets to judge whether a phase is moving the product meaningfully.


| Phase      | Expected completion move                                                      |
| ---------- | ----------------------------------------------------------------------------- |
| `Phase 09` | from validation-grade backend execution to production-grade backend execution |
| `Phase 10` | from bounded control-entry proof to stronger end-to-end control-plane closure |
| `Phase 11` | from backend-ready path to selected product-surface readiness                 |
| `Phase 12` | from candidate-safe to production-safe                                        |


## Near-Term Execution Direction

If the goal is to maximize product completion efficiently, the recommended order is:

1. finish `Phase 08` bookkeeping cleanly
2. `Phase 09` production execution closure
3. `Phase 10` real control-plane closure
4. `Phase 11` product surface rebinding
5. `Phase 12` production hardening

The most important near-term engineering weight should go to `Phase 09`.

## Short Summary

The V2 line already has a real bounded candidate path.
The next development plan should treat later work as product-completion phases, not more protocol discovery.

The main heavy engineering work still ahead is:

1. production-grade execution
2. stronger runtime/control closure
3. later product-surface rebinding
4. production hardening

