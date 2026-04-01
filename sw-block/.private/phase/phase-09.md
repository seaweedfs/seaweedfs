# Phase 09

Date: 2026-03-31
Status: active
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

## Assignment For `sw`

Current next tasks:

1. define the concrete execution-closure package for `Phase 09`
2. specify what "real" means for:
   - `TransferFullBase`
   - `TransferSnapshot`
   - `TruncateWAL`
3. specify how stronger live runtime execution ownership should work on the volume-server path
4. keep the phase bounded to the chosen candidate path unless new evidence forces expansion
5. hand the package to architect review before tester work begins

## Assignment For `tester`

Current next tasks:

1. prepare the validation oracle for production execution closure
2. require explicit validation targets for:
   - real transfer behavior
   - truncation execution
   - cleanup on success/failure/cancel
   - stronger live runtime ownership
3. keep no-overclaim active around:
   - validation-grade vs production-grade execution
   - chosen path vs future paths/modes
4. review only after architect pre-review passes
