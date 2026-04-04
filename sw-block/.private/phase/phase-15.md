# Phase 15

Date: 2026-04-03
Status: delivered
Purpose: connect the explicit `V2 core` to one narrow live adapter path so the
repo starts proving semantic ownership on the integrated path, not only inside
`sw-block/engine/replication`

## Why This Phase Exists

`Phase 14` delivered the first bounded explicit core shell:

1. `14A`: mode / readiness / publication shell closure
2. `14B`: command-sequence closure
3. `14C`: boundary / recovery closure

That shell is real, but it still mostly lives as an internal owner inside
`sw-block/engine/replication`.

`Phase 15` exists to connect one narrow live path from `weed/` into that owner
without broad rebinding or runtime cutover.

## Phase Goal

Connect one bounded adapter ingress/egress path between `weed/` and the explicit
`V2 core`, then prove the path does not silently split semantic truth.

## Scope

### In scope

1. one narrow event ingress from a live `weed/` path into the explicit core
2. one bounded command/projection egress back to the adapter layer
3. focused proof that the narrow path carries explicit core-owned truth

### Out of scope

1. no broad registry rewrite yet
2. no product-surface rebinding yet
3. no broad runtime cutover
4. no transport redesign

## Phase 15 Slices

### `15A`: Minimal Adapter Hook

Goal:

1. connect one narrow adapter ingress to the new core

Acceptance object:

1. one real event path from `weed/` into `sw-block/engine/replication`
2. one bounded command/projection path back out
3. structural proof that the narrow path updates core-owned projection truth on
   the live code path

Status:

1. delivered

### `15B`: Projection-Store Rebinding

Goal:

1. make `weed/` projection/state surfaces consume core-owned projection truth

Acceptance object:

1. bounded rebinding of one or more real `weed/` surfaces to core-owned projection truth
2. proof that assignment delivered != ready != publish healthy on the real path

Current chosen paths:

1. `weed/server/volume_server_block_debug.go`
2. `/debug/block/shipper`
3. `BlockService.CollectBlockVolumeHeartbeat()`
4. `BlockVolumeRegistry.UpdateFullHeartbeat()`
5. `entryToVolumeInfo()` in `master_server_handlers_block.go`
6. `blockVolumeLookupHandler()` and `blockVolumeListHandler()`
7. `LookupBlockVolume()` in `master_grpc_server_block.go`
8. `blockStatusHandler()` aggregate counts
9. core projection preferred when present; adapter-local readiness only as fallback

Status:

1. delivered

## Immediate Next Step

Start `Phase 16` from the first bounded runtime-driving path:

1. replace one adapter-owned execution decision path with core-driven command
   ownership
2. keep reusing `blockvol` as execution backend, but stop letting adapter-local
   execution branching remain the semantic owner

This is the next natural step after `15B`: outward surfaces now consume
core-owned truth on a bounded path; `Phase 16` must make one bounded integrated
runtime path behave as a `V2`-owned runtime rather than constrained-`V1`
semantics plus rebinding.
