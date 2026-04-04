# V2 First Migration Batch

Date: 2026-04-04
Status: delivered

## Purpose

This note defines the first migration batch for the `sw-block` separation work.

The batch must:

1. move code toward `sw-block`
2. keep `sw-block` free of direct `weed/` imports
3. avoid moving `BlockService` or `RecoveryManager` whole

## Batch Goal

Establish one clean execution-muscle layer behind `sw-block` ports, while
keeping `weed/` as a thin adapter shell.

## Batch Scope

### In scope

1. `sw-block/bridge/blockvol` contract cleanup
2. canonical helper extraction for identity and recovery-target mapping
3. reader / pinner / executor migration target design
4. tests that prove those contracts and helpers

### Out of scope

1. moving `weed/server/volume_server_block.go`
2. moving `weed/server/block_recovery.go`
3. moving the full `blockvol` backend
4. broad master/heartbeat refactor

## Target Package Shape

### Keep as long-term owner

1. `sw-block/engine/replication`
2. `sw-block/bridge/blockvol`

### Future landing zone for execution muscles

Recommended target inside `sw-block`:

1. keep contracts in `sw-block/bridge/blockvol`
2. add a future execution-oriented package only after ports are stable, for
   example:
   - `sw-block/bridge/blockvol/runtime`
   - or `sw-block/runtime/blockvol`

For the first batch, do NOT create that new package yet unless the existing
contracts prove insufficient.

### Keep as thin adapter implementations

1. `weed/storage/blockvol/v2bridge`
2. `weed/server/*`

## Concrete Batch Steps

1. normalize `sw-block/bridge/blockvol` contracts so they match the engine's
   real IO surfaces
2. make canonical helper functions in `sw-block` for:
   - replica identity
   - recovery-target mapping
3. switch duplicate adapter-side mapping sites to consume those helpers
4. leave real `BlockVol`-backed implementations in `weed/` for now
5. only after steps 1-4 are stable, start moving implementation files

## Execution Form

This batch is executed through the validate-able tasks in:

1. `sw-block/design/v2-first-migration-task-pack.md`

That task pack turns the batch into four parallelizable work items:

1. canonical assignment translation
2. reader port separation
3. pinner port separation
4. executor muscle separation

## Why This Batch Is First

This batch is first because it creates a safe migration destination:

1. without stable ports, code movement just relocates coupling
2. without canonical helpers, control translation will drift during migration
3. moving execution muscles before shrinking `weed/server` keeps product risk low

## Exit Condition

This batch is complete when:

1. `sw-block` owns the canonical contract layer
2. `weed/` implements that layer without redefining semantics
3. future code moves become mechanical implementation relocation, not
   architecture redesign

## Delivery Note

This batch is now delivered:

1. Task A was completed by code change in `a38e04c03`
2. Tasks B/C/D were confirmed already clean by review against the task-pack
   acceptance bar
