# Phase 16

Date: 2026-04-04
Status: active
Purpose: close one bounded `V2`-native runtime path where the explicit core
owns runtime-driving semantics and `blockvol` remains only the execution backend

## Why This Phase Exists

`Phase 14` made the explicit core real.

`Phase 15` then rebound one bounded set of integrated `weed/` surfaces so they
consume core-owned truth instead of silently inheriting adapter-local semantics.

That means the repo now has:

1. explicit core-owned state / command / projection semantics
2. bounded integrated surface rebinding across VS, registry, HTTP, gRPC, and
   cluster status

But it still does not yet have one bounded path where runtime-driving execution
ownership itself is `V2`-native.

## Phase Goal

Close one bounded integrated runtime path where:

1. the explicit core decides the runtime-driving command sequence
2. the adapter executes those commands against `blockvol`
3. runtime observations return back into the core
4. outward surfaces continue to reflect that core-owned runtime path

## Scope

### In scope

1. one bounded command-driven adapter execution path
2. one bounded observation-feedback path from execution back into the core
3. end-to-end proof that the bounded path behaves as a `V2`-owned runtime path

### Out of scope

1. no full replacement of all `blockvol` async executors
2. no broad runtime cutover across every `weed/` path
3. no protocol rediscovery
4. no launch / rollout approval

## Phase 16 Slices

### `16A`: Command-Driven Adapter Ownership

Goal:

1. replace one adapter-owned execution decision path with core-driven command
   ownership

Acceptance object:

1. one real integrated path executes because the core emitted the command
2. the adapter no longer decides that path only from its local branching
3. proof that command emission and command execution stay aligned

Current chosen path:

1. assignment-driven `apply_role` execution now runs from core command egress
2. replica-path `start_receiver` execution follows the same bounded command path
3. primary-path `configure_shipper` execution now also follows the bounded
   command path
4. failure-side `invalidate_session` execution now follows the bounded command
   path for the integrated sender path
5. catch-up / rebuild remain outside the current `16A` closure

Status:

1. delivered

### `16B`: Runtime Observation Closure

Goal:

1. make the bounded runtime path close back into the core through explicit
   observation semantics

Acceptance object:

1. one end-to-end failover/recovery/publication scenario runs on the
   core-driven path
2. proof that outward surfaces remain consistent with the same bounded runtime
   path

Current chosen path:

1. live recovery observations now return into the core on catch-up and rebuild
   entry/exit points
2. bounded catch-up execution now runs from `StartCatchUpCommand`
3. rebuild execution is the next likely runtime-driving candidate on the same
   path

Status:

1. active

## Current Checkpoint Review Target

The current review target is the first bounded integrated runtime checkpoint
after `Phase 15` closeout:

1. `Phase 15` delivered:
   - bounded surface/store/outward consume-chain rebinding
2. `16A` delivered:
   - bounded command-driven adapter ownership for:
     - `apply_role`
     - `start_receiver`
     - `configure_shipper`
     - `invalidate_session`
3. `16B` active with accepted current closure:
   - live recovery observations return into the core
   - bounded catch-up execution runs from `StartCatchUpCommand`

This checkpoint is intentionally still bounded:

1. `start_rebuild` execution ownership is not yet in scope
2. broad recovery-loop closure is not yet claimed
3. launch / rollout readiness is not claimed

## Immediate Next Step

Start `16A` with the narrowest runtime-driving execution decision still owned by
adapter branching:

1. choose one path where the core already emits a bounded command
2. let the adapter execute from that command instead of from implicit local
   control flow
3. keep `blockvol` as execution backend and treat its local state only as
   observation input

The next likely engineering target is the matching rebuild execution path,
unless the current `Phase 15 + 16A + 16B` boundary is accepted as the next
commit checkpoint first.
