# V2 Second Migration Batch

Date: 2026-04-04
Status: delivered

## Purpose

This note defines the second migration batch for the `sw-block` separation
work.

The first batch established contract ownership and canonical translation in
`sw-block`. The second batch starts the next frontier: backend-binding
extraction.

## Batch Goal

Separate reusable execution-muscle logic from concrete `BlockVol` bindings so
that more code can physically move toward `sw-block` without importing
`weed/storage/blockvol`.

## Batch Scope

### In scope

1. reader backend-binding extraction
2. pinner backend-binding extraction
3. executor backend capability extraction
4. recovery-side shim reduction where those bindings are still copied manually

### Out of scope

1. moving raw `BlockVol` backend code into `sw-block`
2. moving `weed/server/block_recovery.go` whole
3. redesigning the rebuild TCP protocol
4. changing engine semantics or recovery policy

## Current Boundary Problem

After the first batch, the ownership split is better, but the reusable logic is
still physically stuck next to `BlockVol` because:

1. `weed/storage/blockvol/v2bridge/reader.go` reads `BlockVol` directly
2. `weed/storage/blockvol/v2bridge/pinner.go` mixes hold bookkeeping with
   concrete retention-floor wiring
3. `weed/storage/blockvol/v2bridge/executor.go` mixes reusable recovery steps
   with concrete backend calls
4. `weed/server/block_recovery.go` still contains reader/pinner shims that copy
   contract shapes manually

## Target Package Shape

Recommended landing zone inside `sw-block`:

1. keep pure contracts in `sw-block/bridge/blockvol`
2. allow a new execution-oriented package for reusable muscle logic:
   `sw-block/bridge/blockvol/runtime`

Weed-side code should shrink toward:

1. thin `BlockVol` binding
2. runtime hosting
3. network/wire adaptation

## Concrete Batch Steps

1. extract reader logic so `weed/` only fetches backend snapshot data
2. extract pinner hold bookkeeping so `weed/` only performs concrete retention
   binding and state checks
3. extract executor-facing backend capabilities so reusable orchestration no
   longer depends on direct `BlockVol` imports
4. remove redundant reader/pinner contract-shape shims from
   `weed/server/block_recovery.go` where the new extracted layer makes them
   unnecessary

## Execution Form

This batch is executed through the validate-able tasks in:

1. `sw-block/design/v2-second-migration-task-pack.md`

## Why This Batch Is Second

This batch comes second because the first batch had to finish first:

1. backend-binding extraction is unsafe until contracts and canonical rules are
   stable
2. after Batch 1, the remaining coupling is mostly physical implementation
   coupling, not semantic drift
3. shrinking `weed/server` only becomes meaningful once `weed/storage/...`
   stops owning reusable muscle logic

## Exit Condition

This batch is complete when:

1. reusable reader/pinner/executor logic can live in `sw-block` without direct
   `weed/storage/blockvol` imports
2. weed-side files are reduced to thin backend bindings and runtime hosting
3. recovery-side manual shims are either removed or reduced to trivial wiring

## Delivery Note

This batch is now delivered:

1. Task E removed reader contract-shape shimming and made `v2bridge.Reader`
   return the bridge contract directly
2. Task F removed the pinner shim from `weed/server/block_recovery.go`
3. Task G was reviewed and confirmed already clean because `v2bridge.Executor`
   already satisfies the engine IO interfaces directly
