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
2. `v2-production-roadmap.md` for the older roadmap ladder
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
| Integrated candidate path | Medium-strong | `P1` + `P2` + `P3` prove one bounded candidate path. |
| Runtime ownership inside live server loop | Medium | Real intake exists, but full product-grade recovery ownership is not yet fully closed. |
| Production-grade data transfer | Medium-weak | Validation-grade transfer exists; full production byte streaming is still incomplete. |
| Truncation / replica-ahead execution | Weak | Detection exists; full execution path is still incomplete. |
| End-to-end control-plane closure | Medium | `ProcessAssignments()` is real; full heartbeat/gRPC proof is still bounded. |
| Product surfaces (`CSI`, `NVMe`, `iSCSI`, snapshot productization) | Partial | Mostly reuse candidates, but not the current core closure target. |
| Production hardening / ops | Partial | Candidate-level evidence exists; production-grade hardening is still ahead. |

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
| Recovery engine | V2-owned | Continue closing runtime/product path under accepted semantics. |
| `v2bridge` | V2 boundary adapter | Keep expanding real I/O/runtime closure without leaking policy downward. |
| `blockvol` WAL/flusher/runtime | Reuse reality | Reuse implementation, but do not let V1 replication semantics redefine V2 truth. |
| Snapshot capability | Reuse implementation, V2-owned semantics | Do not make this a main near-term phase goal until core execution/runtime closure is stronger. |
| `CSI` | Later product surface | Rebind after the V2-backed candidate path is stable enough. |
| `NVMe` / `iSCSI` | Later product surface | Reuse as front-end adapters once the backend candidate path is stronger. |
| Rebuild server / transfer mechanisms | Reuse with redesign boundary | Good candidate for later production execution closure work. |
| Control plane | Reuse existing path | Continue from `ProcessAssignments()` toward stronger end-to-end closure. |

## What The Candidate Path Already Proves

For the chosen `RF=2 sync_all` path, the project can already claim:

1. stable remote identity across address change when `ServerID` is present
2. stale epoch/session fencing through the integrated path
3. real catch-up one-chain closure on the chosen path
4. rebuild control/execution chain proven on the chosen path
   - validation-grade execution closure
   - not yet production-grade block/image streaming
5. replay of accepted failure classes on the unified live path
6. one real failover / reassignment cycle
7. one true simultaneous-overlap retention safety proof
8. committed/checkpoint separation accepted for this candidate path:
   - `CommittedLSN = WALHeadLSN`
   - `CheckpointLSN` remains the durable base-image boundary

## What Is Still Missing For Product Completion

The biggest remaining product-completion gaps are:

1. production-grade rebuild data transfer
   - `TransferFullBase` must become real streaming, not only accessibility validation
   - `TransferSnapshot` must become real image streaming, not only checkpoint validation
2. replica-ahead physical correction
   - `TruncateWAL` must stop being a stub
3. stronger live runtime ownership
   - the V2 recovery driver/executors should become a more complete live runtime path, not only a bounded hardening path
4. stronger control-plane closure
   - current proof reaches `ProcessAssignments()`
   - full heartbeat/gRPC-level closure is still bounded
5. product-surface rebinding
   - `CSI`
   - `NVMe`
   - `iSCSI`
   - snapshot product path
6. production hardening
   - restart / soak / repeated disturbance / diagnosis quality

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

Main work:

1. heartbeat/gRPC-level proof
2. stronger control/result convergence
3. better identity completeness for local and remote server roles

### Stage 4: Phase 11 Product Surface Rebinding

Target:

1. connect product-facing surfaces to the V2-backed block path

Candidate areas:

1. snapshot product path
2. `CSI`
3. `NVMe`
4. `iSCSI`

Rule:

Do this after the backend engine/runtime path is strong enough, not before.

### Stage 5: Phase 12 Production Hardening

Target:

1. move from candidate-safe to production-safe

Main work:

1. soak / restart / repeated failover
2. operational diagnosis quality
3. performance floor and cost characterization
4. explicit production blockers / rollout gates

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

## Near-Term Planning Guidance

If the goal is to maximize product completion efficiently:

1. do not make `CSI`, `NVMe`, or broad snapshot productization the immediate next heavy phase
2. first close production execution gaps in the backend path
3. then strengthen control-plane closure
4. then rebind product surfaces

In short:

1. backend truth and execution first
2. product surfaces second
3. production hardening last

## Short Summary

The V2 line is already beyond "algorithm only".
It has a real bounded candidate path.

But the remaining work is still substantial, and it is mostly engineering work:

1. production-grade execution
2. stronger runtime/control closure
3. product-surface rebinding
4. production hardening

That is the practical path from the current candidate-safe engine to a production-ready block product.
