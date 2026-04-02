# Phase 09

Date: 2026-03-31
Status: complete
Purpose: turn the accepted candidate-safe backend path into a production-grade execution path without reopening accepted V2 recovery semantics

## Why This Phase Exists

`Phase 08` closed:

1. real control delivery on the chosen path
2. real one-chain catch-up and rebuild closure on the chosen path
3. unified hardening replay on the accepted live path
4. one bounded candidate package for `RF=2 sync_all`

What still does not exist is production-grade execution completeness.

The main remaining gap is no longer:

1. whether the path is candidate-safe

It is now:

1. whether the backend execution path is production-grade rather than validation-grade

## Phase Goal

Close the main backend execution gaps so the chosen path is no longer blocked by validation-grade transfer/truncation behavior.

## Scope

### In scope

1. real `TransferFullBase`
2. real `TransferSnapshot`
3. real `TruncateWAL`
4. stronger live runtime execution ownership on the volume-server path

### Out of scope

1. broad control-plane redesign
2. `RF>2`
3. `best_effort` / `sync_quorum` recovery semantics
4. product-surface rebinding (`CSI` / `NVMe` / `iSCSI`)
5. broad performance optimization

## Phase 09 Items

### P0: Production Execution Closure Plan

1. convert the accepted candidate package into a production-execution closure plan
2. define the minimum execution blockers that must be closed in this phase
3. order the execution work by dependency and risk
4. keep the chosen-path bound explicit while making the backend path production-grade

Goal:

- start `Phase 09` with one substantial execution-closure plan, not another light packaging round

Must prove:

1. the phase is centered on real backend execution work
2. the required closures are explicit:
   - `TransferFullBase`
   - `TransferSnapshot`
   - `TruncateWAL`
   - stronger runtime ownership
3. the phase remains bounded to the chosen candidate path unless new evidence expands it

Verification mechanism:

1. architect review:
   - phase shape is substantial and outcome-based
   - work is ordered by real engineering dependency
2. tester review:
   - validation expectations are explicit for each execution closure target
3. manager review:
   - the phase is large enough to justify a full engineering round

Output artifacts:

1. explicit execution-closure target list
2. explicit execution blocker list
3. initial slice/package order inside `Phase 09`

Execution note:

- use `phase-09-log.md` as the technical pack for:
  - the definition of "real" for each execution target
  - recommended slice order
  - validation expectations
  - assignment templates for `sw` and `tester`

Reject if:

1. `Phase 09` is framed as another packaging/documentation phase
2. execution blockers remain implicit
3. the phase quietly expands into product surfaces or unrelated control-plane work
4. the phase has no clear verification mechanism

Status:

- accepted

### P1: Full-Base Execution Closure

Goal:

- make `TransferFullBase` a real production-grade execution path for the chosen `RF=2 sync_all` candidate path

Accepted scope:

1. real TCP full-base transfer
2. explicit local install ownership in `blockvol`
3. second catch-up after extent copy
4. achieved-boundary reporting back to engine
5. local runtime convergence to the achieved boundary
6. fail-closed behavior for transfer/runtime errors

Accepted evidence shape:

1. component proof:
   - TCP transfer
   - local install
2. one-chain proof:
   - `engine plan -> RebuildExecutor -> v2bridge -> blockvol -> InSync`
3. convergence proof:
   - `achievedLSN >= targetLSN`
   - no split truth between engine and local runtime
4. fail-closed proof:
   - connection refused
   - epoch mismatch
   - no address
   - partial transfer
5. runtime proof:
   - stale non-empty replica state cleared
   - active receiver progress converges

Status:

- accepted

Carry-forward from `P1`:

1. `TransferSnapshot` still not real
2. `TruncateWAL` still not real
3. stronger live runtime ownership still not closed

### P2: Snapshot Execution Closure

Goal:

- make `TransferSnapshot` a real production-grade execution path for the chosen `RF=2 sync_all` candidate path

Accepted scope:

1. real TCP snapshot/base transfer
2. exact snapshot-boundary verification
3. explicit manifest boundary metadata
4. local runtime convergence to the exact snapshot boundary before tail replay
5. single-executor snapshot + tail replay execution chain
6. bounded tail replay to the planned target

Accepted evidence shape:

1. component proof:
   - real snapshot image transfer
   - exact base-boundary install
2. one-chain proof:
   - `engine plan -> RebuildExecutor -> v2bridge -> blockvol -> tail replay -> InSync`
3. exact-boundary proof:
   - requested `snapshotLSN` is transferred exactly
   - newer checkpoint is rejected rather than silently accepted
4. convergence proof:
   - post-install local runtime converges to `snapshotLSN`
   - post-replay engine/runtime converge to `targetLSN`
5. cleanup proof:
   - temporary snapshot ownership released on success/failure

Status:

- accepted

Carry-forward from `P2`:

1. `TruncateWAL` still not real
2. stronger live runtime ownership still not closed

### P3: Truncation Execution Closure

Goal:

- make `TruncateWAL` a real production-grade execution path for the chosen `RF=2 sync_all` candidate path

Required scope:

1. real truncation execution closure for the truncation-safe replica-ahead case
2. explicit rebuild escalation for replica-ahead cases that are not truncation-safe
3. one-chain proof through the catch-up executor path
4. fail-closed / no-overclaim behavior when local truncation is unsafe
5. no overclaim of broader runtime-ownership closure

Status:

- accepted

Carry-forward from `P3`:

1. truncation-safe vs rebuild-required replica-ahead split still happens at execution time, not planning time
2. stronger live runtime ownership still not closed

### P4: Stronger Live Runtime Ownership

Goal:

- move the accepted execution logic from bounded test/adapter ownership into a stronger live runtime path on the chosen `RF=2 sync_all` volume-server path

Required scope:

1. stronger volume-server/runtime ownership of recovery execution
2. explicit live start / cancel / replace / cleanup semantics
3. real runtime wiring for current execution inputs and addresses
4. one-chain proof on the live runtime path, not only bounded executor tests
5. no overclaim of broader control-plane closure

Status:

- accepted

Carry-forward from `P4`:

1. repeated primary assignment still logs a low-severity rebuild-server double-start warning on the same volume
2. broader control-plane closure remains out of scope for `Phase 09`

## Assignment For `sw`

Current next tasks:

1. `Phase 09` is complete
2. no further `P4` implementation work is open in this phase
3. any next work should open under the next phase, not extend `Phase 09` implicitly

## Assignment For `tester`

Current next tasks:

1. `Phase 09` validation/bookkeeping is complete
2. keep any residual notes bounded:
   - low-severity rebuild-server double-start warning on repeated primary assignment
   - broader control-plane closure still belongs to a later phase
