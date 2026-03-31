# Phase 08

Date: 2026-03-31
Status: active
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

Expectation note:

- the `P1` tester expectation is already embedded in this phase doc under:
  - `P1 / Validation focus`
  - `P1 / Reject if`
- do not grow a separate long template unless `P1` scope expands materially

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

### P2: Execution Closure

1. close the live engine -> executor -> `v2bridge` execution chain
2. make catch-up execution evidence integrated rather than split across layers
3. close the first rebuild execution path required by the product path

### P3: Hardening Validation

1. validate diagnosability under the live integrated path
2. validate retention/pinner behavior under concurrent load
3. replay the accepted failure-class set again on the newly unified live path after `P1` and `P2` land
4. confirm the remaining gap to a production candidate

Validation focus:

- prove the chosen path through a real control-delivery path
- prove the live engine -> executor -> `v2bridge` execution chain as one path, not split evidence
- prove the first rebuild execution path required by the chosen product path
- prove at least one real failover / promotion / reassignment cycle
- prove concurrent retention/pinner behavior does not break recovery guarantees

Reject if:

- catch-up semantics are overclaimed beyond the currently proven boundary
- rebuild is claimed as supported without real execution closure
- master/control delivery is claimed as real without the live path in place
- `CommittedLSN` vs `CheckpointLSN` remains an unclassified note instead of a gate decision
- `P1` and `P2` land independently but the accepted failure-class set is not replayed again on the unified live path

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

Before the next phase, `Phase 08` must decide one of:

1. committed-truth separation is mandatory before a production-candidate phase
2. the first candidate path is intentionally bounded to the currently proven pre-checkpoint replay behavior

It must not remain an unclassified carry-forward.

## Exit Criteria

Phase 08 is done when:

1. the first product path runs through a real control delivery path
2. the critical execution chain is integrated and validated
3. rebuild execution for the chosen path is no longer just detected but executed
4. at least one real failover / reassignment cycle is replayed through the live control path
5. the accepted failure-class set is replayed again on the unified live path
6. operational/debug evidence is sufficient for pre-production use
7. the remaining gap to a production candidate is small and explicit

## Assignment For `sw`

Next tasks:

1. drive `Phase 08 P1` as real master/control delivery integration
2. replace direct `AssignmentIntent` construction for the first live path
3. preserve through the real control path:
   - stable `ReplicaID`
   - epoch fencing
   - address-change invalidation
4. include at least one real failover / promotion / reassignment validation target
5. keep acceptance claims scoped:
   - real control delivery path
   - not yet general production readiness
6. keep explicit carry-forwards:
   - `CommittedLSN != CheckpointLSN` still unresolved
   - integrated catch-up execution chain still incomplete
   - rebuild execution still incomplete

## Assignment For `tester`

Next tasks:

1. use the accepted `Phase 08` plan framing as the `P1` validation oracle
2. validate real control delivery for:
   - live assignment delivery
   - stable identity through the control path
   - stale epoch/session invalidation
   - at least one real failover / reassignment cycle
3. keep the no-overclaim rule active around:
   - catch-up semantics
   - rebuild execution
   - master/control delivery
4. keep the committed-truth gate explicit:
   - still unresolved in `P1`
5. prepare `P2` follow-up expectations for:
   - integrated engine -> executor -> `v2bridge` execution closure
   - unified replay after `P1` and `P2`
