# Phase 08

Date: 2026-03-31
Status: complete
Purpose: convert the accepted Phase 07 product path into a pre-production-hardening program without reopening accepted V2 protocol shape

## Why This Phase Exists

`Phase 07` completed:

1. a real service-slice integration around the V2 engine
2. real storage-truth bridge evidence through `v2bridge`
3. selected real-system failure replay
4. the first explicit product-path decision

What still does not exist is a pre-production-ready system path. The remaining work is no longer protocol discovery. It is closing the operational and integration gaps between the accepted product path and a hardened deployment candidate.

## Phase Goal

Harden the first accepted V2 product path until the remaining gap to a production candidate is explicit, bounded, and implementation-driven.

This phase doc is the canonical hardening contract for `sw` and `tester`.
Use `phase-08-log.md` for deeper engineering process, alternatives, and implementation detail.

Algorithm note:

- the accepted V2 algorithm / protocol shape is treated as fixed for this phase
- remaining work is engineering closure over real Seaweed/V1 runtime paths under V2 boundaries
- do not reopen protocol design unless a live contradiction is found

## Scope

### In scope

1. real master/control delivery into the engine service path
2. integrated engine -> executor -> `v2bridge` execution closure
3. rebuild execution closure for the accepted product path
4. operational/debuggability hardening
5. concurrency/load validation around retention and recovery

### Out of scope

1. new protocol redesign
2. `RF>2` coordination
3. Smart WAL optimization work
4. broad performance tuning beyond validation needed for hardening
5. full V1 replacement rollout

## Phase 08 Items

### P0: Hardening Plan

1. convert the accepted `Phase 07` product path into a hardening plan
2. define the minimum pre-production gates
3. order the remaining integration closures by risk
4. make an explicit gate decision on committed truth vs checkpoint truth:
   - either separate `CommittedLSN` from `CheckpointLSN` before a production-candidate phase
   - or explicitly bound the first candidate path to the currently proven pre-checkpoint replay behavior

Status:

- planning package accepted in this phase doc
- first hardening priorities are fixed as:
  - real master/control delivery
  - integrated engine -> executor -> `v2bridge` catch-up execution chain
  - first rebuild execution path
- the committed-truth carry-forward is now a required hardening gate, not just a note:
  - either separate `CommittedLSN` from `CheckpointLSN` before a production-candidate phase
  - or explicitly bound the first candidate path to the currently proven pre-checkpoint replay behavior
- at least one real failover / promotion / reassignment cycle is a required hardening target
- once `P1` and `P2` land, the accepted failure-class set must be replayed again on the newly unified live path
- the validation oracle for `Phase 08` is expected to reject overclaiming around:
  - catch-up semantics
  - rebuild execution
  - master/control delivery
  - candidate-path readiness vs production readiness
- accepted

Reference:

- `sw-block/docs/archive/design/phase-08-engine-skeleton-map.md` is the implementation-side skeleton map for this phase
- it is subordinate to `sw-block/design/v2-protocol-truths.md` and this `phase-08.md`; use it for module layout, execution order, interim fields, hard gates, and reuse guidance

### P1: Real Control Delivery

1. connect real master/heartbeat assignment delivery into the bridge
2. replace direct `AssignmentIntent` construction for the first live path
3. preserve stable identity and fenced authority through the real control path
4. include at least one real failover / promotion / reassignment validation target on the chosen `sync_all` path

Technical focus:

- keep the control-path split explicit:
  - master confirms assignment / epoch / role
  - bridge translates confirmed control truth into engine intent
  - engine owns sender/session/recovery policy
  - `blockvol` does not re-decide recovery policy
- preserve the identity rule through the live path:
  - `ReplicaID = <volume>/<server>`
  - endpoint change updates location but must not recreate logical identity
- preserve the fencing rule through the live path:
  - stale epoch must invalidate old authority
  - stale session must not mutate current lineage
  - address change must invalidate the old live session before the new path proceeds
- treat failover / promotion / reassignment as control-truth events first, not storage-side heuristics

Implementation route (`reuse map`):

- reuse directly as the first hardening carrier:
  - `weed/server/master_grpc_server.go`
  - `weed/server/volume_grpc_client_to_master.go`
  - `weed/server/volume_server_block.go`
  - `weed/server/master_block_registry.go`
  - `weed/server/master_block_failover.go`
- reuse as storage/runtime execution reality:
  - `weed/storage/blockvol/blockvol.go`
  - `weed/storage/blockvol/replica_apply.go`
  - `weed/storage/blockvol/replica_barrier.go`
  - `weed/storage/blockvol/v2bridge/`
- preserve the V2 boundary while reusing these files:
  - reuse transport/control/runtime reality
  - do not inherit old policy semantics as V2 truth
  - keep engine as the recovery-policy owner
  - keep `blockvol` as the I/O executor

Validation focus:

- prove live assignment delivery into the bridge/engine path
- prove stable `ReplicaID` across address refresh on the live path
- prove stale epoch / stale session invalidation through the live path
- prove at least one real failover / promotion / reassignment cycle on the chosen `sync_all` path
- prove the resulting logs explain:
  - why reassignment happened
  - why a session was invalidated
  - which epoch / identity / endpoint drove the transition

Reject if:

- address-shaped identity reappears anywhere in the control path
- bridge starts re-deriving catch-up vs rebuild policy from convenience inputs
- old epoch or old session can still mutate after the new control truth arrives
- failover / reassignment is claimed without a real replay target
- delivery claims general production readiness rather than control-path closure

Status:

- accepted
- real assignment delivery into the V2 path is now proven through `ProcessAssignments()`
- accepted evidence includes:
  - live assignment -> engine sender/session creation
  - stable remote `ReplicaID = <volume>/<ServerID>`
  - address-change identity preservation through the live path
  - stale epoch/session invalidation through the live path
  - fail-closed skip on missing `ServerID`
- accepted with explicit carry-forwards:
  - `localServerID = listenAddr` remains transport-shaped for local identity
  - heartbeat -> `ProcessAssignments()` is proven, but not full end-to-end gRPC delivery
  - integrated catch-up execution is not yet proven through the live path
  - rebuild execution remains deferred
  - `CommittedLSN = CheckpointLSN` remains unresolved

### P2: Execution Closure

1. close the live engine -> executor -> `v2bridge` execution chain
2. make catch-up execution evidence integrated rather than split across layers
3. close the first rebuild execution path required by the product path

Technical focus:

- keep execution ownership explicit:
  - engine plans and owns recovery state transitions
  - engine executor drives stepwise execution
  - `v2bridge` translates execution requests into real blockvol work
  - `blockvol` performs I/O only
- prove catch-up as one real path:
  - accepted control delivery
  - real retained-history input
  - real WAL retention pin
  - real WAL scan / progress return
  - real session completion
- choose the narrowest rebuild closure required by the current product path:
  - first real `full-base` rebuild path is preferred
  - `snapshot + tail` can remain later unless needed by the chosen path
- keep resource ownership fail-closed:
  - pin acquisition before execution
  - release on success
  - release on cancel / invalidation
  - release on partial failure
- keep observability causal:
  - execution start
  - execution progress
  - execution cancel / invalidation
  - execution failure
  - completion

Implementation route:

- reuse engine-side execution core:
  - `sw-block/engine/replication/driver.go`
  - `sw-block/engine/replication/executor.go`
  - `sw-block/engine/replication/orchestrator.go`
- reuse storage/runtime execution bridge:
  - `weed/storage/blockvol/v2bridge/executor.go`
  - `weed/storage/blockvol/v2bridge/pinner.go`
  - `weed/storage/blockvol/v2bridge/reader.go`
- reuse block runtime execution reality:
  - `weed/storage/blockvol/blockvol.go`
  - `weed/storage/blockvol/replica_apply.go`
  - `weed/storage/blockvol/replica_barrier.go`
  - rebuild-side files under `weed/storage/blockvol/`
- preserve the boundary:
  - do not move zero-gap / catch-up / rebuild classification into `blockvol`
  - do not let executor convenience paths redefine protocol semantics

Validation focus:

- prove one live integrated catch-up chain:
  - assignment/control arrives through accepted `P1` path
  - engine plans
  - executor drives `v2bridge`
  - `blockvol` executes
  - progress returns
  - session completes
- prove one real rebuild execution path for the chosen product path
- prove retention pin / release symmetry on the live path
- prove rebuild resource pin / release symmetry on the live path
- prove invalidation / cancel cleanup on the live path
- prove execution logs explain:
  - why catch-up started
  - why rebuild started
  - why execution failed
  - why execution was cancelled
  - why completion succeeded

Reject if:

- catch-up is still only proven by split evidence
- rebuild remains only a detection outcome
- `blockvol` starts deciding recovery mode or rebuild fallback
- resources leak on cancel / invalidation / partial failure
- execution logs are too weak to replay causality offline
- the slice quietly broadens protocol semantics beyond the current accepted boundary

Recommended first cut:

1. close the live catch-up chain first
2. close the first real `full-base` rebuild path second
3. leave unified replay to `P3`

Minimum closure threshold:

- do not accept `P2` on glue code + partial chain tests alone
- at least one accepted catch-up proof must drive the real engine executor path:
  - `PlanRecovery(...)`
  - `NewCatchUpExecutor(...)`
  - executor-managed progress / completion
  - real `v2bridge` / `blockvol` execution underneath
- at least one accepted rebuild proof must drive the real engine executor path:
  - rebuild assignment
  - `PlanRebuild(...)`
  - `NewRebuildExecutor(...)`
  - executor-managed completion
  - real `TransferFullBase(...)` underneath
- resource-cleanup proof must include live-path assertions, not only logs:
  - active holds released
  - retention floor no longer pinned after release
  - no surviving session/plan ownership after cancel / invalidation / failure
- observability proof should include executor-generated events, not only planner-side events
- if these thresholds are not met, record `P2` as partial execution progress, not execution closure

Carry-forward note:

- on the chosen `RF=2 sync_all` path, `CommittedLSN` separation is resolved in this slice:
  - `CommittedLSN = WALHeadLSN`
  - `CheckpointLSN` remains the durable base-image boundary
- this is not yet a blanket truth for every future path or durability mode
- post-checkpoint catch-up remains bounded unless explicitly closed
- rebuild coverage is limited to the first chosen executable path if that is all that lands

Status:

- accepted
- real one-chain execution is now proven for:
  - catch-up
  - rebuild
- accepted evidence includes:
  - `CommittedLSN` separated from `CheckpointLSN` on the chosen `sync_all` path
  - live engine plan -> executor -> `v2bridge` -> `blockvol` catch-up chain
  - live engine plan -> executor -> `v2bridge` -> `blockvol` rebuild chain
  - explicit pin cleanup assertions after execution
- accepted with explicit residual scope:
  - `CatchUpStartLSN` is not directly asserted in tests
  - rebuild source is not yet forced/verified per source variant
  - broader rebuild-source coverage can remain follow-up work

Review checklist:

- is there one accepted catch-up proof from real `P1` control path to real session completion, using `CatchUpExecutor`
- is there one accepted first rebuild proof on the chosen path, using `RebuildExecutor`
- do live-path assertions prove pin/hold release on success, cancel, invalidation, and failure
- do logs/status explain start, cancel, failure, and completion without hidden transitions
- does the delivery avoid overclaiming general post-checkpoint catch-up, broad rebuild coverage, or production readiness

### P3: Hardening Validation

1. replay the accepted failure-class set again on the unified live path after `P1` + `P2`
2. validate at least one real failover / promotion / reassignment cycle through the live control path
3. validate concurrent retention/pinner behavior under overlapping recovery activity
4. make the committed-truth gate decision explicit for the chosen candidate path

Slice adjustment note:

- if `P2` lands only partially, `P3` should first close the missing execution outcome:
  - real catch-up closure if still missing
  - real first rebuild closure if still missing
- only after both are real should `P3` spend most of its weight on unified replay, failover / reassignment validation, and concurrent retention / cleanup hardening

Efficiency note:

- `P3` is a hardening-validation slice, not another execution-closure slice
- reuse the accepted `P1` / `P2` live path as the base; do not re-prove already accepted chain mechanics in isolation
- prefer one compact replay matrix over many near-duplicate tests
- prefer one real failover cycle and one true simultaneous-overlap retention case over broad scenario expansion
- the required new outputs are:
  - unified replay evidence
  - one real failover / reassignment replay
  - one concurrent retention/pinner safety result
  - one explicit committed-truth gate decision

Validation focus:

- unified replay for:
  - changed-address restart
  - stale epoch / stale session
  - unrecoverable gap / needs-rebuild
  - post-checkpoint boundary behavior
- at least one real failover / promotion / reassignment cycle
- concurrent retention/pinner safety under at least one true simultaneous-overlap hold case
- logs explain:
  - why control truth changed
  - why a session was invalidated
  - why catch-up vs rebuild was chosen
  - why execution completed, failed, or was cancelled

Reject if:

- accepted failure classes are still only partially replayed on the unified path
- failover / reassignment is claimed without a real live-path replay
- concurrent retention/pinner behavior leaks pins or violates recovery safety
- logs are too weak to replay causality offline
- the committed-truth gate is still just a note instead of an explicit decision

Status:

- accepted
- unified hardening replay is now proven on the accepted live path
- accepted evidence includes:
  - replay of the accepted failure-class set on the unified `P1` + `P2` path
  - at least one real failover / reassignment cycle through the live control path
  - one true simultaneous-overlap retention/pinner safety proof
  - stronger causality assertions for invalidation, escalation, catch-up, and completion
- committed-truth gate decision for the chosen candidate path:
  - for the chosen `RF=2 sync_all` candidate path, `CommittedLSN = WALHeadLSN` with `CheckpointLSN` kept separate is accepted as sufficient for the candidate-path hardening boundary
  - this is not yet a blanket truth for every future path or durability mode

### P4: Candidate Package Closure

1. classify what is truly ready for a first candidate path
2. package the accepted `P1` / `P2` / `P3` evidence into one bounded candidate package
3. turn carry-forwards into explicit candidate bounds or hard gates
4. state clearly what still remains before production readiness

Goal:

- finish `Phase 08` with one explicit candidate package, not just a collection of accepted slices

Verification mechanism:

- evidence map:
  - every candidate claim must point to accepted evidence from `P1` / `P2` / `P3`
- tester validation:
  - verify each candidate claim is supported by accepted evidence
  - reject any claim that exceeds the proven boundary
- manager validation:
  - verify the candidate statement is explicit, bounded, and not confused with production readiness

Output artifacts:

1. candidate-path statement in `phase-08.md`
2. candidate/gate decision record in `phase-08-decisions.md`
3. concise candidate package summary:
   - candidate-safe capabilities
   - explicit bounds
   - deferred / blocking items
4. concise residual-gap summary:
   - candidate-safe
   - intentionally bounded
   - still deferred / still blocking
5. short module/package boundary summary for later phases:
   - what is already strong enough
   - what moves to the next heavy engineering phase

Efficiency note:

- `P4` should mostly consume already accepted evidence, not create broad new engineering work
- only add implementation work if a small remaining blocker must be closed to make the candidate statement coherent
- if a gap is real but not worth closing in `Phase 08`, classify it explicitly rather than expanding scope implicitly
- `P4` exists inside `Phase 08` so the next phase can begin with substantial engineering work, not a light packaging-only round

Validation focus:

- make the candidate-path boundary explicit:
  - what is proven
  - what is intentionally bounded
  - what is still deferred
- make the candidate package explicit:
  - candidate-safe capability list
  - evidence-to-claim mapping
  - short module/package boundary summary
- make the committed-truth decision explicit:
  - accepted for the chosen `RF=2 sync_all` candidate path
  - still unclassified for future paths / durability modes unless separately proven
- prove the accepted product path can be described as an engineering candidate, not only as a set of slice-local proofs
- provide one explicit residual-gap list that separates:
  - candidate-safe bounds
  - future hardening work
  - production blockers

Reject if:

- `P4` reopens protocol design instead of closing engineering gaps
- candidate claims are broader than the proven path
- carry-forwards remain informal notes rather than bounds or gates
- production readiness is implied from candidate readiness
- `P4` produces only prose summary without an evidence-to-claim mapping
- `P4` is too thin to leave the next phase with substantial engineering closure work

Status:

- accepted
- the first candidate package is now explicit for the chosen path
- accepted evidence includes:
  - candidate-safe claims mapped to accepted `P1` / `P2` / `P3` evidence
  - explicit bounds for `RF=2 sync_all`
  - explicit deferred / blocking items before production use
  - committed-truth decision scoped to the chosen candidate path
  - short module/package boundary summary for the next heavy engineering phase
- accepted judgment:
  - candidate-safe-with-bounds
  - not production-ready

## Guardrails

### Guardrail 1: Do not reopen accepted V2 protocol truths casually

`Phase 08` is a hardening phase. New work should preserve the accepted protocol truth set unless a real contradiction is demonstrated.

### Guardrail 2: Keep product-path claims evidence-bound

Do not claim more than the hardened path actually proves. Distinguish:

1. live integrated path
2. hardened product path
3. production candidate

### Guardrail 3: Identity and policy boundaries remain hard rules

1. `ReplicaID` must remain stable and never collapse to address shape
2. engine decides recovery policy
3. bridge translates intent/state
4. `blockvol` executes I/O only

### Guardrail 4: Carry-forward limitations must remain explicit until closed

Especially:

1. committed truth vs checkpoint truth
2. rebuild execution coverage
3. real master/control delivery coverage

### Guardrail 5: The committed-truth carry-forward must become a gate, not a note

For the chosen `RF=2 sync_all` candidate path, this gate is now decided:

1. `CommittedLSN = WALHeadLSN`
2. `CheckpointLSN` remains the durable base-image boundary
3. this separation is accepted as sufficient for the candidate-path hardening boundary

For future paths or durability modes, the gate must still be classified explicitly rather than carried forward informally.

## Exit Criteria

Phase 08 is done when:

1. the first product path runs through a real control delivery path
2. the critical execution chain is integrated and validated
3. rebuild execution for the chosen path is no longer just detected but executed
4. at least one real failover / reassignment cycle is replayed through the live control path
5. the accepted failure-class set is replayed again on the unified live path
6. operational/debug evidence is sufficient for pre-production use
7. the remaining gap to a production candidate is small and explicit

Phase-close note:

- `Phase 08` is now closed
- next phase:
  - `Phase 09: Production Execution Closure`
  - start with `P0` planning for real execution completeness:
    - real `TransferFullBase`
    - real `TransferSnapshot`
    - real `TruncateWAL`
    - stronger live runtime execution ownership

## Assignment For `sw`

Current next tasks:

1. close out `Phase 08` bookkeeping only if any wording drift remains
2. move to `Phase 09 P0` planning for production execution closure
3. focus the next heavy engineering package on:
   - real `TransferFullBase`
   - real `TransferSnapshot`
   - real `TruncateWAL`
   - stronger live runtime execution ownership

## Assignment For `tester`

Current next tasks:

1. treat `Phase 08` as closed after any final wording/bookkeeping sync
2. prepare the `Phase 09 P0` validation oracle for production execution closure
3. keep no-overclaim active around:
   - validation-grade transfer vs production-grade transfer
   - truncation execution
   - stronger runtime ownership vs current bounded path
