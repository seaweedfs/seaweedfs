# Phase 17

Date: 2026-04-04
Status: active
Purpose: track post-`Phase 16` code separation so adapter/runtime ownership in
`weed/server` and backend-binding ownership in `weed/storage/blockvol/v2bridge`
continue to converge without losing engineering continuity

## Why This Phase Exists

`Phase 14-16` established and widened one bounded `V2`-owned runtime path:

1. `Phase 14` made the explicit core real
2. `Phase 15` rebound bounded integrated surfaces to consume core-owned truth
3. `Phase 16` closed one bounded runtime path where the core owns command and
   observation semantics

That semantic/runtime line is now real, but a separate engineering line has
been running in parallel:

1. move canonical rules toward `sw-block`
2. thin `weed/server` into a host shell
3. keep `v2bridge` limited to backend-binding responsibilities
4. avoid creating a new mixed-ownership layer while the migration proceeds

This phase exists to track that separation line explicitly so commits, reviews,
and next steps do not drift away from the phase log.

## Relationship To Phase 16

`Phase 16` remains the semantic/runtime-ownership phase.

`Phase 17` is different:

1. it does not redefine the bounded `V2` runtime claims from `Phase 16`
2. it tracks code movement and ownership cleanup after those claims became real
3. it records which adapter/binding responsibilities moved where

In short:

1. `Phase 16` answers: who owns runtime semantics?
2. `Phase 17` answers: where does the code now live, and what is left to move?

## Phase Goal

Track and complete the post-`Phase 16` separation of command/runtime/binding
responsibilities so the codebase approaches this split:

1. `sw-block`
   - owns canonical contracts, core/runtime helpers, and reusable semantics
2. `weed/storage/blockvol/v2bridge`
   - owns concrete `BlockVol` and wire/backend bindings
3. `weed/server`
   - owns host-shell orchestration, host effects, and product integration only

## Scope

### In scope

1. migration batches that thin `weed/server`
2. migration batches that keep backend binding in `v2bridge`
3. migration batches that move reusable coordination toward `sw-block`
4. explicit tracking of delivered / partial / next separation seams

### Out of scope

1. changing the semantic claims already owned by `Phase 14-16`
2. broad protocol redesign
3. full removal of legacy compatibility paths without replacement criteria
4. changing `blockvol` into a `sw-block` package

## Batch Tracking

### Batch 1: Contracts And Canonical Translation

Goal:

1. stabilize contract ownership and canonical identity/recovery-target mapping

Status:

1. delivered

Delivered result:

1. `sw-block/bridge/blockvol` owns the canonical contract/helper layer
2. `Task A` removed duplicate identity/recovery-target derivation from adapter
   code
3. `Task B/C/D` were confirmed already clean at the acceptance bar

Evidence:

1. `a38e04c03`
2. design notes:
   - `sw-block/design/v2-first-migration-batch.md`
   - `sw-block/design/v2-first-migration-task-pack.md`

### Batch 2: Backend-Binding Shim Reduction

Goal:

1. remove redundant reader/pinner shims and confirm thin backend bindings

Status:

1. delivered

Delivered result:

1. `v2bridge.Reader` returns bridge contract state directly
2. `pinnerShimForRecovery` was removed
3. `Executor` was confirmed already clean as a direct engine-IO implementer

Evidence:

1. `680b53031`
2. `519c84994`
3. design notes:
   - `sw-block/design/v2-second-migration-batch.md`
   - `sw-block/design/v2-second-migration-task-pack.md`

### Batch 3: Recovery Runtime Helper Extraction

Goal:

1. move reusable recovery coordination out of `weed/server/block_recovery.go`

Status:

1. delivered

Delivered result:

1. `sw-block/engine/replication/runtime` owns pending coordination and reusable
   execution helpers
2. `block_recovery.go` uses those helpers on the production path
3. no-core behavior is structurally isolated behind explicit legacy helpers

Evidence:

1. `6fea93e82`
2. `e200df779`
3. `e075d7761`
4. `3a5fbbfde`

### Batch 4: Typed Runtime Boundary And Host-Shell Reduction

Goal:

1. remove `interface{}` drift and simplify recovery host-shell logic

Status:

1. delivered

Delivered result:

1. `PendingExecution` is fully typed
2. rebuild completion shaping moved into runtime helper
3. `buildRecoveryBundle()` removed repeated host-side assembly

Evidence:

1. `0bcfc678d`
2. `ded84b25e`

### Batch 5: Recovery Binding Factory Extraction

Goal:

1. move recovery bundle assembly into the backend-binding layer

Status:

1. delivered

Delivered result:

1. `block_recovery.go` no longer constructs `Reader/Pinner/StorageAdapter/Executor`
   directly
2. `v2bridge.BuildRecoveryBundle()` became the concrete backend-binding seam

Evidence:

1. `263611004`

### Batch 6: Recovery Context Resolver Extraction

Goal:

1. make one resolver own the host-side recovery context

Status:

1. delivered

Delivered result:

1. `resolveRecoveryContext()` owns:
   - `volPath`
   - `rebuildAddr`
   - bundle construction
   - replica flushed progress lookup
2. `runCatchUp()` / `runRebuild()` now read as:
   - resolve
   - plan
   - branch

Evidence:

1. `a48da0f67`
2. `41082bf92`

### Batch 7: Command Dispatch Split

Goal:

1. move command-switch orchestration out of `volume_server_block.go`
2. keep it out of `v2bridge`

Status:

1. delivered

Delivered result:

1. `weed/server/blockcmd` owns command dispatch
2. `weed/server` keeps host effects
3. `v2bridge` remains free of `engine.Command` switch / event-emission
   semantics

Evidence:

1. `11c6aaf31`

### Batch 8: BlockVol Command-Binding Extraction

Goal:

1. move concrete `BlockVol`-backed command operations out of
   `volume_server_block.go`

Status:

1. delivered

Delivered result:

1. `v2bridge.CommandBindings` owns:
   - role apply binding
   - receiver startup binding
   - primary shipper configuration binding
   - shipper-connected probe
2. `weed/server` still owns readiness-state updates and event semantics

Evidence:

1. `38b504299`

### Batch 9: Non-BlockVol Service-Ops Extraction

Goal:

1. move the remaining non-`BlockVol` command operations out of
   `volume_server_block.go` into the server-adapter layer

Status:

1. delivered

Delivered result:

1. `weed/server/blockcmd.ServiceOps` owns:
   - recovery-task startup
   - pending catch-up/rebuild execution
   - sender invalidation through projection + resolver
2. `volume_server_block.go` now keeps:
   - host effects
   - backend-facing thin wrappers
   - assignment/event ingress

Evidence:

1. `38b504299`

### Batch 10: Host-Effects Adapter Extraction

Goal:

1. move the remaining command-completion host effects out of
   `volume_server_block.go` while keeping them on the `weed/server` side

Status:

1. delivered

Chosen slice:

1. `10A`: extract a dedicated server-side host-effects adapter into
   `weed/server/blockcmd`
2. keep this slice bounded to:
   - `RecordCommand`
   - `EmitCoreEvent`
   - `PublishProjection`
   - projection-cache write routing
3. do not fold backend-side readiness mutation into this first cut

Acceptance:

1. `coreCommandEffects` is no longer defined in `volume_server_block.go`
2. `PublishProjection` cache-write logic is no longer inline in
   `volume_server_block.go`
3. `weed/server` still owns host-effect semantics, but exposes them through a
   thinner adapter object
4. focused proofs remain green

Delivered result:

1. `weed/server/blockcmd/host_effects.go` now owns the concrete dispatcher-side
   host-effects adapter
2. `volume_server_block.go` now only wires recorder/event/cache dependencies
   into that adapter
3. server-owned projection cache writing moved behind `BlockService.StoreProjection()`

Evidence:

1. working tree change on top of `38b504299`

### Batch 11: Stop-Line Review For Readiness Mutation

Goal:

1. decide whether the remaining adapter-local readiness mutation should move out
   of `volume_server_block.go`

Status:

1. delivered

Decision:

1. stop at the current package boundary
2. do not extract readiness-state mutation into `weed/server/blockcmd`

Reason:

1. the remaining methods are not dispatcher-side host effects
2. they are direct writes to `BlockService`-owned `replStates`
3. they encode adapter-local cache/state semantics, not reusable command
   orchestration logic
4. moving them into `blockcmd` would mostly add callback indirection without
   clarifying ownership

Covered methods:

1. `noteRoleApplied`
2. `markPrimaryTransportConfigured`
3. `markReceiverReady`
4. `ReadinessSnapshot` remains the read-side consumer of the same local state

Accepted stop line:

1. `weed/server/blockcmd`
   - owns dispatch, service ops, and host-effects adapter
2. `weed/server`
   - owns host state fields and adapter-local readiness/cache mutation
3. `weed/storage/blockvol/v2bridge`
   - owns concrete backend bindings only

## Current Position

The separation line is now in the late-stage thinning phase.

Current split:

1. `sw-block`
   - canonical contracts / helpers
   - runtime coordination helpers
2. `weed/storage/blockvol/v2bridge`
   - `BlockVol`-backed recovery and command bindings
3. `weed/server/blockcmd`
   - command dispatch, service-side command operations, and host-effects adapter
4. `weed/server`
   - assignment ingress, product integration, and adapter-local readiness/cache state

## Non-Claims

`Phase 17` does not yet claim:

1. every remaining host-side helper must move
2. full deletion of compatibility-only paths
3. that `weed/server` becomes empty or trivial

The stop line is practical, not ideological:

1. keep real host/product integration in `weed/server`
2. keep backend binding in `v2bridge`
3. keep reusable semantics/runtime helpers out of both

## Immediate Next Step

The current separation stop line is now explicit:

1. keep dispatcher-side logic in `weed/server/blockcmd`
2. keep backend bindings in `weed/storage/blockvol/v2bridge`
3. keep `BlockService` local state mutation in `weed/server`

Any next task should now be one of two kinds:

1. cleanup inside the accepted stop line
2. a new semantic/runtime phase, not another ownership-shuffle task
