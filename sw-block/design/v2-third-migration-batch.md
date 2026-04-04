# V2 Third Migration Batch

Date: 2026-04-04
Status: active

## Purpose

This note defines the third migration batch for the `sw-block` separation work.

Batch 1 stabilized contract ownership and canonical translation.
Batch 2 removed backend-binding shims and confirmed thin `v2bridge`
implementations.
Batch 3 now targets the remaining runtime-owner concentration in
`weed/server/block_recovery.go`.

## Batch Goal

Reduce `weed/server/block_recovery.go` to a host shell that:

1. owns goroutine lifecycle
2. owns concrete server/block-store access
3. delegates reusable recovery coordination to `sw-block`-owned helpers

## Batch Scope

### In scope

1. pending recovery execution coordination
2. catch-up/rebuild plan execution helper extraction
3. rebuild completion observation shaping
4. explicit isolation of legacy no-core startup behavior

### Out of scope

1. moving the full `RecoveryManager` out of `weed/server`
2. changing core command semantics
3. removing `legacy P4` or no-core paths prematurely
4. redesigning block-store access or sender registry ownership

## Current Boundary Problem

After Batch 2, `Reader`, `Pinner`, and `Executor` are thinner, but
`weed/server/block_recovery.go` still owns several reusable layers at once:

1. task host lifecycle
2. pending execution cache and mismatch cancellation
3. catch-up/rebuild execution helper wiring
4. rebuild completion shaping into core events
5. legacy no-core startup compatibility

That keeps too much reusable coordination trapped in the product adapter shell.

## Target Package Shape

Recommended split:

1. keep host lifecycle in `weed/server`
2. allow reusable recovery coordination helpers in
   `sw-block/engine/replication/runtime`
3. keep concrete `BlockVol` access and server integration in `weed/`

Reason:

1. pending execution and plan completion shaping are engine-oriented, not
   backend-specific
2. those helpers should not require `weed/server` ownership just to exist

## Concrete Batch Steps

1. extract pending execution coordination into reusable runtime helpers
2. extract catch-up/rebuild execution helper logic so `weed/server` only
   supplies IO bindings and host callbacks
3. extract rebuild completion observation shaping so `weed/server` only reads
   backend facts and forwards them
4. isolate no-core startup compatibility behind explicit legacy-only entry
   points

## Execution Form

This batch is executed through the validate-able tasks in:

1. `sw-block/design/v2-third-migration-task-pack.md`

## Why This Batch Is Third

This batch comes third because:

1. runtime-host thinning only becomes clear after the backend-binding layer is
   already reduced
2. otherwise `block_recovery.go` would still be compensating for low-level shim
   coupling
3. the remaining work is now primarily coordination extraction, not contract
   cleanup

## Exit Condition

This batch is complete when:

1. `weed/server/block_recovery.go` is mostly host wiring and concrete backend
   access
2. reusable pending-execution and completion-shaping logic no longer requires
   product adapter ownership
3. legacy no-core startup behavior is clearly isolated as compatibility-only
