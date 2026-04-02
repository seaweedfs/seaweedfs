# Phase 10

Date: 2026-04-02
Status: complete
Purpose: close the main end-to-end control-plane gaps on the chosen `RF=2 sync_all` path without reopening accepted backend execution semantics

## Why This Phase Exists

`Phase 09` closed the main backend execution gaps on the chosen path:

1. real `TransferFullBase`
2. real `TransferSnapshot`
3. real `TruncateWAL` under the accepted narrowed contract
4. stronger live runtime ownership on the volume-server path

What still does not exist is stronger end-to-end control-plane closure.

The main remaining gap is no longer:

1. whether the backend execution path is real

It is now:

1. whether the real control path drives and reflects the chosen path coherently enough for product use

## Phase Goal

Strengthen from accepted assignment-entry closure to stronger end-to-end control-plane closure on the chosen path.

## Scope

### In scope

1. heartbeat / gRPC-level control delivery proof on the chosen path
2. reassignment / failover result convergence through the real control path
3. cleaner local identity than transport-shaped `listenAddr`
4. bounded idempotence / repeated-assignment cleanup when it directly affects live control/runtime ownership

### Out of scope

1. reopening accepted `P1` / `P2` / `P3` / `P4` backend execution semantics
2. `RF>2`
3. `best_effort` / `sync_quorum`
4. product-surface rebinding (`CSI` / `NVMe` / `iSCSI`)
5. broad performance optimization

## Phase 10 Items

### P0: Control-Plane Closure Plan

Goal:

- start `Phase 10` with one substantial control-plane closure package, not a loose collection of follow-up fixes

Must prove:

1. the phase is centered on real control-path closure rather than backend execution rework
2. the required closure targets are explicit:
   - heartbeat / gRPC delivery
   - reassignment / result convergence
   - identity cleanup
   - bounded repeated-assignment/idempotence cleanup
3. the chosen-path bound remains explicit

Verification mechanism:

1. architect review:
   - control-plane scope is explicit and bounded
   - proposed slices do not reopen accepted backend execution semantics
2. tester review:
   - required end-to-end proofs are explicit
3. manager review:
   - the package is concrete enough to assign the first implementation slice

Output artifacts:

1. explicit control-plane closure targets
2. explicit reject shapes
3. initial slice order inside `Phase 10`

Execution note:

- use `phase-10-log.md` as the technical pack for:
  - semantic scope
  - execution scope
  - proof shapes
  - assignment templates for `sw` and `tester`

Reject if:

1. `Phase 10` is framed as a vague "polish/control" phase without concrete closure targets
2. accepted `Phase 09` execution semantics are quietly reopened
3. product surfaces or unrelated hardening work are absorbed into this phase
4. no explicit end-to-end proof shape is defined

Status:

- accepted

### P1: Identity And Control-Truth Closure

Goal:

- close stable identity on the real chosen-path control wire so assignment truth, local ingest truth, and `ReplicaID` construction no longer depend on transport-shaped fallback

Accepted scope:

1. stable server identity preserved on the block assignment proto wire
2. master assignment generation preserves stable identity on the chosen path
3. volume-server local identity uses the same canonical server identity as the main volume server
4. real ingress proof:
   - proto/decode
   - `ProcessAssignments()`
   - `ControlBridge`
   - engine sender identity
5. fail-closed behavior for missing stable identity

Status:

- accepted

Carry-forward from `P1`:

1. fuller reassignment / failover result convergence is still open
2. broader control-plane reporting closure is still open

### P2: Reassignment / Result Convergence

Goal:

- prove that reassignment and failover converge through the real control path without stale local ownership or stale reported truth lingering after control truth changes

Accepted scope:

1. real failover / reassignment convergence through the chosen control path
2. no stale local runtime owner after control truth changes
3. no stale control/reporting truth after reassignment
4. one-chain proof through the real control path, not only local helper logic
5. no overclaim of broader hardening or product-surface closure

Status:

- accepted

Carry-forward from `P2`:

1. `P2` proves stale owner removal and no stale residue after control truth changes
2. bounded repeated-assignment/idempotence cleanup is still open where repeated primary assignment can still emit rebuild-server relisten warnings
3. `P2` does not claim broad master-driven failover infrastructure closure beyond the accepted volume-server-side chosen-path ingress

### P3: Bounded Repeated-Assignment / Idempotence Cleanup

Goal:

- close the remaining low-severity repeated-assignment/runtime-idempotence gap on the chosen path so duplicate or replacement primary assignments do not leave avoidable relisten/restart noise or ambiguous live-control ownership

Accepted scope:

1. repeated primary assignment on the same chosen-path volume should converge idempotently
2. rebuild-server/runtime side effects should not relaunch noisily when the authoritative control truth is unchanged or already active
3. bounded proof that repeated-assignment cleanup does not reopen accepted `P2` convergence or accepted `Phase 09` execution semantics
4. no expansion into broad runtime polish, product surfaces, or unrelated restart hardening

Status:

- accepted

Carry-forward from `P3`:

1. chosen-path repeated unchanged assignment is now absorbed idempotently across the accepted V2 + V1 live path
2. `P3` remains bounded cleanup; it does not itself close the remaining master-driven heartbeat/gRPC control-loop gap
3. fuller master-originated control delivery proof is still open

### P4: Master-Driven Control-Loop Closure

Goal:

- close the remaining chosen-path control-plane gap by proving that master-originated assignment truth delivered through the real heartbeat / gRPC control loop reaches the live volume-server path and converges without split truth

Required scope:

1. one bounded end-to-end proof from real master-produced chosen-path assignment truth into the live volume-server control path
2. proof that the real heartbeat / gRPC delivery path preserves the already accepted identity and convergence properties
3. proof that externally visible post-delivery state reflects the same new truth after the real master-driven path runs
4. no reopening of accepted `P1` / `P2` / `P3` semantics except for narrow bugs directly exposed by the fuller control-loop proof
5. no expansion into product surfaces, `RF>2`, or broad cluster-hardening work

Status:

- accepted

Carry-forward from `P4`:

1. bounded chosen-path master-driven heartbeat / gRPC control-loop closure is now accepted
2. `P4` does not claim full live transport-stream deployment proof or broad product hardening
3. the next phase should move to `Phase 11` product-surface rebinding

### Planned slice direction after `P0`

1. `P1`:
   - identity and control-truth closure on the live control path
2. `P2`:
   - reassignment / failover result convergence through the real control path
3. `P3`:
   - bounded idempotence / repeated-assignment cleanup after accepted `P1` / `P2`
4. `P4`:
   - master-driven heartbeat / gRPC control-loop closure on the chosen path

## Assignment For `sw`

Current next tasks:

1. treat `Phase 10` as closed and keep accepted `P1` / `P2` / `P3` / `P4` semantics stable
2. start `Phase 11` product-surface rebinding from `v2-phase-development-plan.md`
3. keep the first `Phase 11` slice bounded to selected product surfaces rather than broad hardening
4. do not reopen accepted backend execution or control-plane closure except for narrow bug fixes

## Assignment For `tester`

Current next tasks:

1. treat `P4` as accepted bounded control-loop closure on the chosen path
2. validate the first `Phase 11` slice as bounded product-surface rebinding rather than renewed control-plane work
3. keep no-overclaim active around:
   - accepted `Phase 09` execution closure
   - accepted `Phase 10` control-plane closure
   - selected `Phase 11` surface scope vs broader product readiness
   - chosen path vs future paths/modes
