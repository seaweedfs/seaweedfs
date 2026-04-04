# V2 Product Completion Overview

Date: 2026-04-04
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
2. `v2-phase-development-plan.md` for the current phase ladder
3. `../.private/phase/phase-16.md` for the active bounded runtime-closure contract

## Current Position

The accepted first candidate path is:

1. `RF=2`
2. `sync_all`
3. existing master / volume-server heartbeat path
4. V2 engine owns recovery policy
5. `v2bridge` translates real storage/control truth
6. `blockvol` remains the execution backend

This means the project is no longer at "algorithm only" or even only at
"bounded prototype".
It already has:

1. accepted protocol truths
2. accepted engine execution closure on the chosen path
3. accepted control-plane and product-surface rebinding on the chosen path
4. accepted hardening evidence for the bounded chosen path
5. an active `Phase 16` runtime-closure program that is moving live ownership
   from host-local orchestration toward explicit core-driven command / event /
   projection seams

The most important current distinction is:

1. the runtime backbone is now largely present
2. product completion is no longer blocked by "missing first implementation"
3. product completion is still blocked by broader closure and launch-envelope
   proof

## Engineering Completion Snapshot

These levels are rough engineering estimates, not exact percentages.

| Area | Current level | Notes |
|------|---------------|-------|
| Algorithm / protocol truths | Strong | Core V2 semantics are accepted and should remain stable unless contradicted by live evidence. |
| Simulator / prototype evidence | Strong | Main failure classes and protocol boundaries are already well-exercised. |
| Engine recovery core | Strong | Sender/session/orchestrator/driver/executor are substantially implemented and remain the semantic center. |
| Weed bridge integration | Strong | Reader / pinner / control / executor are real and tested on the chosen path. |
| Integrated candidate path | Strong on chosen path | Backend, control-plane, and selected product surfaces are accepted on one bounded chosen path. |
| Runtime ownership inside live server loop | Strong on bounded path, still widening | `Phase 16A-16I` now give one substantial core-command-driven runtime path, but broad recovery-loop closure is not yet claimed. |
| Production-grade data transfer | Strong on chosen path | `TransferFullBase` and `TransferSnapshot` execution closure are accepted on the chosen path; later work is hardening. |
| Truncation / replica-ahead execution | Strong on chosen path | `TruncateWAL` narrow chosen-path closure is accepted; later work is hardening/planning improvement. |
| End-to-end control-plane closure | Strong on chosen path | `Phase 10` accepted bounded end-to-end control-path closure on the chosen path. |
| Product surfaces (`CSI`, `NVMe`, `iSCSI`, snapshot productization) | Strong on chosen path | `Phase 11` accepted bounded product-surface rebinding on the chosen path. |
| Production hardening / ops | Strong on bounded path, not full launch proof | `Phase 12` closed the bounded hardening bar, but not the whole first-launch envelope. |
| Multi-replica catch-up runtime ownership | Strong on bounded startup/execution path | `16F-16I` made command/event/pending/aggregation/startup ownership replica-scoped enough for the bounded primary multi-replica path. |
| Broad failover / publication / launch envelope | Partial | Main remaining product gap: prove wider runtime closure and freeze a supported launch envelope. |

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
9. bounded live runtime ownership has materially improved after `Phase 15`:
   - assignment entry is core-owned on the bounded path
   - `apply_role`, `start_receiver`, `configure_shipper`, and
     `invalidate_session` are command-driven
   - bounded catch-up / rebuild execution starts from core-emitted recovery
     commands
   - recovery command addressing and observation events are replica-scoped
   - multi-replica catch-up aggregation and startup ownership are now bounded on
     the primary path

## What Is Still Missing For Product Completion

The biggest remaining product-completion gaps are no longer "invent the first
working path". They are closure and launch-envelope gaps:

1. broader recovery-loop closure
   - current `Phase 16` evidence is strong on one bounded runtime path, but not
     yet a claim that all recovery lifecycle branches are core-owned end to end
2. broader failover / publication closure
   - product-facing truth under failover, replay, and disturbance still needs a
     stronger whole-chain statement, not only local runtime proofs
3. restart / disturbance preservation
   - accepted behavior must remain correct under restart, rejoin, repeated
     failover, and address churn without reopening semantic ambiguity
4. long-run / soak / performance floor
   - accepted behavior must remain stable under longer operation with explicit
     floor numbers and bounded rollout gates
5. launch-envelope freeze
   - the first supported launch matrix, exclusions, operator expectations, and
     stop conditions must be written down explicitly

## Recommended Completion Roadmap

### Stage 1: Finish `Phase 16` runtime closure cleanly

Target:

1. close the next bounded `Phase 16` runtime gap with explicit bounds and
   package one clear reviewable runtime checkpoint

Main output:

1. one accepted bounded `V2`-native runtime path that is explicit about what is
   core-owned, what is adapter-owned, and what is still only compatibility
2. one short list of residual recovery-loop gaps after `16I`

Why it matters:

This is now the main semantic engineering blocker between a strong bounded path
and a launchable product statement.

### Stage 2: Freeze the first supported launch envelope

Target:

1. convert accepted protocol, runtime, control-plane, and hardening evidence
   into one bounded first-launch support statement

Status:

1. not yet frozen

Main work:

1. define the supported environment / replication / durability matrix
2. define explicit exclusions that remain outside the first launch claim
3. bind product-facing surfaces to the named supported envelope
4. define preflight, success, and stop conditions

### Stage 3: Internal pilot and incident-driven hardening

Target:

1. validate the frozen launch envelope without silently broadening scope

Main work:

1. run a limited internal pilot package
2. route incidents with explicit classification:
   - config / environment issue
   - known exclusion
   - true product bug
3. harden only within the named supported envelope
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

1. runtime-closure gate
   - the strongest live recovery path must be explicit, bounded, and
     semantically owned by the core rather than spread across legacy host logic
2. failover/publication gate
   - outward truth under disturbance must be strong enough to make a product
     statement, not only a local runtime statement
3. restart/disturbance gate
   - restart, rejoin, address change, and repeated failover must preserve the
     accepted bounded semantics
4. launch-envelope gate
   - the first supported matrix and explicit exclusions must be written down
5. pilot/rollout gate
   - internal pilot discipline, incident routing, and rollout review must be
     explicit before widening scope

## Near-Term Planning Guidance

If the goal is to maximize product completion efficiently:

1. do not reopen accepted execution, control-plane, or product-surface semantics casually
2. treat the remaining `Phase 16` work as runtime-closure work, not as another
   round of broad redesign
3. finish the next bounded recovery-loop gaps before claiming more product scope
4. then freeze the first supported launch envelope
5. then run a limited internal pilot
6. then widen only through explicit incident review and rollout-gate review

In short:

1. runtime closure first
2. launch-envelope freeze second
3. bounded productionization third

## Short Summary

The V2 line is already beyond "algorithm only".
It has an accepted bounded chosen path through backend, control-plane, selected
product surfaces, bounded hardening, and substantial `Phase 16` runtime closure.

The main remaining work is not "build the first real thing".
It is:

1. finish broader recovery-loop and failover/publication closure strongly enough
   for a product statement
2. freeze the first supported launch envelope from accepted evidence
3. run a limited internal pilot with explicit stop conditions
4. harden from incidents without silently broadening scope

That is the practical path from the current bounded runtime-complete candidate
to a bounded first-launch block product.
