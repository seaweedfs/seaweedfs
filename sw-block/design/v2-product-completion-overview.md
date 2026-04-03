# V2 Product Completion Overview

Date: 2026-03-31
Status: active
Purpose: provide one product-level overview of current V2 engineering completion, V1 reuse strategy, and the roadmap from the accepted candidate path to a production-ready block engine

## Why This Document Exists

The project now has enough accepted V2 algorithm, engine, and hardening evidence that the next question is no longer only:

1. is the protocol correct

It is also:

1. how complete is the product path
2. which parts are already strong
3. which parts can reuse V1 engineering
4. which parts still require major implementation work
5. which future phases actually move product completion

This document is the product-completion view.

It complements:

1. `v2-protocol-truths.md` for accepted semantics
2. `../docs/archive/design/v2-production-roadmap.md` for the older roadmap ladder
3. `../.private/phase/phase-08.md` for current phase contract

## Current Position

The accepted first candidate path is:

1. `RF=2`
2. `sync_all`
3. existing master / volume-server heartbeat path
4. V2 engine owns recovery policy
5. `v2bridge` translates real storage/control truth
6. `blockvol` remains the execution backend

This means the project is no longer at "algorithm only".
It already has:

1. accepted protocol truths
2. accepted engine execution closure
3. accepted hardening replay on a real integrated path
4. one bounded candidate statement

## Engineering Completion Snapshot

These levels are rough engineering estimates, not exact percentages.

| Area | Current level | Notes |
|------|---------------|-------|
| Algorithm / protocol truths | Strong | Core V2 semantics are accepted and should remain stable unless contradicted by live evidence. |
| Simulator / prototype evidence | Strong | Main failure classes and protocol boundaries are already well-exercised. |
| Engine recovery core | Strong | Sender/session/orchestrator/driver/executor are substantially implemented. |
| Weed bridge integration | Strong | Reader / pinner / control / executor are real and tested on the chosen path. |
| Integrated candidate path | Strong on chosen path | Backend, control-plane, and selected product surfaces are now accepted on one bounded chosen path. |
| Runtime ownership inside live server loop | Strong on chosen path | Accepted chosen-path execution/control ownership exists; later work is restart/disturbance hardening, not first-closure rebinding. |
| Production-grade data transfer | Strong on chosen path | `TransferFullBase` and `TransferSnapshot` execution closure are accepted on the chosen path; later work is hardening. |
| Truncation / replica-ahead execution | Strong on chosen path | `TruncateWAL` narrow chosen-path closure is accepted; later work is hardening/planning improvement. |
| End-to-end control-plane closure | Strong on chosen path | `Phase 10` accepted bounded end-to-end control-path closure on the chosen path. |
| Product surfaces (`CSI`, `NVMe`, `iSCSI`, snapshot productization) | Strong on chosen path | `Phase 11` accepted bounded product-surface rebinding on the chosen path. |
| Production hardening / ops | Partial | `Phase 12` is now the next active stage. |

## Reuse Strategy

Use this rule:

1. if a component decides truth, V2 must own it
2. if a component consumes truth, V1 engineering can often be reused

### V2-owned semantics

These should not inherit V1 semantics casually:

1. recovery choice: `zero_gap` / `catchup` / `needs_rebuild`
2. sender/session ownership and fencing
3. stable `ReplicaID` and stale-authority rejection
4. committed/checkpoint interpretation
5. rebuild-source choice and recovery outcome meaning

### V1 engineering that is usually reusable

These are implementation/reality layers, not protocol truth:

1. `blockvol` storage runtime
2. WAL / flusher / checkpoint machinery
3. real assignment receive/apply path
4. front-end adapters such as `NVMe` / `iSCSI`
5. much of `CSI` lifecycle integration
6. monitoring / metrics / test harness infrastructure

### Reuse with explicit bounds

These can reuse implementation, but their semantic placement must remain V2-owned:

1. snapshot export / checkpoint plumbing
2. rebuild transport / extent read path
3. master/heartbeat/control delivery path

## Module Treatment Overview

| Module area | Current treatment | Near-term plan |
|-------------|-------------------|----------------|
| Recovery engine | V2-owned | Keep semantics stable and focus next on restart/disturbance hardening. |
| `v2bridge` | V2 boundary adapter | Chosen-path execution closure is accepted; later work is hardening without leaking policy downward. |
| `blockvol` WAL/flusher/runtime | Reuse reality | Reuse implementation, but do not let V1 replication semantics redefine V2 truth. |
| Snapshot capability | Reuse implementation, V2-owned semantics | Rebinding is accepted on the chosen path; later work is hardening. |
| `CSI` | Accepted product surface on chosen path | Keep the bounded contract stable and harden under disturbance. |
| `NVMe` / `iSCSI` | Accepted front-end adapters on chosen path | Keep publication/address truth stable and harden runtime behavior. |
| Rebuild server / transfer mechanisms | Reuse with redesign boundary | Chosen-path execution closure is accepted; later work is disturbance hardening. |
| Control plane | Reuse existing path | Bounded chosen-path closure is accepted; later work is restart/disturbance hardening. |

## What The Candidate Path Already Proves

For the chosen `RF=2 sync_all` path, the project can already claim:

1. stable remote identity across address change when `ServerID` is present
2. stale epoch/session fencing through the integrated path
3. real catch-up one-chain closure on the chosen path
4. rebuild control/execution chain proven on the chosen path
   - chosen-path execution closure accepted in `Phase 09`
   - later work is restart/disturbance/perf hardening rather than first-path closure
5. replay of accepted failure classes on the unified live path
6. one real failover / reassignment cycle
7. one true simultaneous-overlap retention safety proof
8. committed/checkpoint separation accepted for this candidate path:
   - `CommittedLSN = WALHeadLSN`
   - `CheckpointLSN` remains the durable base-image boundary

## What Is Still Missing For Product Completion

The biggest remaining product-completion gaps are now production-hardening gaps:

1. restart / recovery disturbance hardening
   - accepted chosen-path behavior must remain correct under restart, rejoin, and repeated failover
2. long-run / soak stability
   - accepted behavior must remain stable across repeated cycles and longer-running operation
3. operational diagnosability
   - blockers, symptoms, and operator-visible diagnosis quality must be explicit
4. performance floor and rollout gates
   - production claims need bounded floor numbers and explicit rollout criteria

## Recommended Completion Roadmap

### Stage 1: Finish Phase 08 cleanly

Target:

1. close candidate-path judgment with explicit bounds and package it cleanly inside `Phase 08 P4`

Main output:

1. one accepted candidate package for the chosen path

### Stage 2: Phase 09 Production Execution Closure

Target:

1. turn validation-grade execution into production-grade execution

Main work:

1. real `TransferFullBase`
2. real `TransferSnapshot`
3. real `TruncateWAL`
4. stronger runtime ownership of recovery execution

Why it matters:

This is the largest remaining engineering block between "candidate-safe-with-bounds" and a serious product path.

### Stage 3: Phase 10 Real Control-Plane Closure

Target:

1. strengthen from accepted assignment-entry closure to fuller end-to-end control-path closure

Status:

1. accepted and closed on the chosen path

Main work:

1. heartbeat/gRPC-level proof
2. stronger control/result convergence
3. better identity completeness for local and remote server roles

### Stage 4: Phase 11 Product Surface Rebinding

Target:

1. connect product-facing surfaces to the V2-backed block path

Status:

1. accepted and closed on the chosen path

Candidate areas:

1. snapshot product path
2. `CSI`
3. `NVMe`
4. `iSCSI`

Recommended first cut:

1. snapshot product path first
2. `CSI` and `NVMe` / `iSCSI` after one bounded product-visible surface is already accepted

Suggested order inside `Phase 11`:

1. `P1` snapshot product path
2. `P2` `CSI`
3. `P3` `NVMe` / `iSCSI`
4. `P4` broader residual workflow closure if still required

Rule:

Do this after the backend engine/runtime path is strong enough, not before.

### Stage 5: Phase 12 Production Hardening

Target:

1. move from candidate-safe to production-safe

Status:

1. accepted and closed on the bounded chosen path

Main work:

1. soak / restart / repeated failover
2. operational diagnosis quality
3. performance floor and cost characterization
4. explicit production blockers / rollout gates

### Stage 6: Post-`Phase 12` Productionization Program

Target:

1. turn the accepted `Phase 12` chosen path into a bounded first-launch product envelope without reopening protocol discovery

Status:

1. next active stage after `Phase 12`

Main work:

1. freeze the first supported launch envelope from accepted `P1`-`P4` evidence
2. define a limited internal pilot package with explicit preflight, success, and stop conditions
3. run incident-driven hardening with explicit classification:
   - config / environment issue
   - known exclusion
   - true product bug
4. perform controlled rollout review only within the named supported envelope

Rules:

1. this is not `Phase 12 P5`
2. pilot success is not generic production proof
3. missing evidence remains an explicit launch constraint until cleared, especially:
   - failover-under-load performance
   - hours/days soak under load
   - `RF>2`
   - broad transport matrix
   - full gRPC-stream integration evidence

## Completion Gates

The most important gates from here are:

1. execution gate
   - validation-grade transfer/truncation must become production-grade
2. runtime ownership gate
   - V2 recovery must be a stronger live runtime path, not only a bounded tested path
3. control-plane gate
   - stronger end-to-end control delivery proof
4. product-surface gate
   - front-end surfaces should only rebind after backend correctness is strong enough
5. production-hardening gate
   - restart, soak, diagnosis, and repeated disturbance must be acceptable
6. productionization gate
   - first launch envelope, pilot discipline, incident routing, and controlled rollout review must be explicit

## Near-Term Planning Guidance

If the goal is to maximize product completion efficiently:

1. do not reopen accepted execution, control-plane, or product-surface semantics casually
2. finish `Phase 12` hardening cleanly
3. then freeze the first supported launch envelope
4. then run a limited internal pilot
5. then widen only through explicit incident review and rollout-gate review

In short:

1. chosen-path closure first
2. production hardening second
3. bounded productionization third

## Short Summary

The V2 line is already beyond "algorithm only".
It has an accepted bounded chosen path through backend, control-plane, selected product surfaces, and `Phase 12` hardening.

But the remaining work is still substantial, and it is mostly engineering work:

1. freeze the first supported launch envelope from accepted evidence
2. run a limited internal pilot with explicit stop conditions
3. harden from incidents without silently broadening scope
4. perform controlled rollout review inside a bounded launch envelope

That is the practical path from the current production-safe chosen path to a bounded first-launch block product.
