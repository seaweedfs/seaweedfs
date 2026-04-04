# V2 Separation Port Layer Audit

Date: 2026-04-04
Status: active

## Purpose

This note audits the current `sw-block` port layer for the separation effort:

1. define which contracts already belong in `sw-block`
2. identify what was still underspecified or mismatched
3. record the normalized boundary for future migration batches

## Current Port Layer

The current reusable boundary inside `sw-block` is:

1. `sw-block/bridge/blockvol/contract.go`
2. `sw-block/bridge/blockvol/storage_adapter.go`
3. `sw-block/bridge/blockvol/control_adapter.go`

These files are the intended weed-free bridge between:

1. `sw-block/engine/replication`
2. `weed/storage/blockvol/v2bridge`
3. `weed/server/*` adapter code

## Audited Contracts

### Storage state port

File:

1. `sw-block/bridge/blockvol/contract.go`

Stable contract:

1. `BlockVolReader`
2. `BlockVolState`

This is already the right ownership:

1. `sw-block` owns the shape of retained-history inputs
2. `weed/` only implements how to read those facts from real `BlockVol`

### Retention / snapshot pinning port

File:

1. `sw-block/bridge/blockvol/contract.go`

Stable contract:

1. `BlockVolPinner`

This remains correct because:

1. pin lifecycle meaning belongs to the V2 recovery driver
2. actual hold/release mechanics remain weed-side implementation detail

### Recovery execution port

Previous issue:

1. `BlockVolExecutor` in `contract.go` did not match the real engine execution
   interfaces precisely
2. in particular, rebuild full-base transfer in the engine returns achieved LSN,
   but the contract only returned `error`

Normalized decision:

1. `sw-block` now names:
   - `BlockVolCatchUpIO`
   - `BlockVolRebuildIO`
   - `BlockVolExecutor`
2. these contracts intentionally match:
   - `engine.CatchUpIO`
   - `engine.RebuildIO`

This is the right long-term boundary because:

1. `sw-block` owns the execution port shape
2. `weed/storage/blockvol/v2bridge.Executor` remains only one implementation
3. future migration can move execution code without changing engine contracts

### Assignment translation helper port

Normalized helper layer:

1. `ReplicaAssignmentForServer()`
2. `RecoveryTargetForRole()`

These are now the canonical helper rules in:

1. `sw-block/bridge/blockvol/control_adapter.go`

They exist to stop identity / recovery-target mapping from drifting between:

1. `weed/storage/blockvol/v2bridge/control.go`
2. `weed/server/volume_server_block.go`

## Code Normalization Completed

Implemented in this batch:

1. `sw-block/bridge/blockvol/doc.go`
   - clarified that the package owns weed-free contracts and thin adapters,
     not real blockvol implementations
2. `sw-block/bridge/blockvol/contract.go`
   - aligned execution contracts with engine IO interfaces
3. `sw-block/bridge/blockvol/control_adapter.go`
   - extracted canonical helper functions for identity and recovery-target
     mapping
4. `sw-block/bridge/blockvol/bridge_test.go`
   - added interface-compatibility proof for the normalized execution contracts

## Resulting Boundary Rule

After this audit, the port layer rule is:

1. `sw-block` defines contracts and canonical mapping helpers
2. `weed/` implements real storage, transport, and runtime bindings
3. no `sw-block` package in this layer should import `weed/`

## What Still Does Not Move Yet

This audit does NOT move:

1. `weed/storage/blockvol/v2bridge.Executor`
2. `weed/storage/blockvol/v2bridge.Reader`
3. `weed/storage/blockvol/v2bridge.Pinner`
4. `weed/server/BlockService`
5. `weed/server/RecoveryManager`

It only stabilizes the port layer those migrations will target.
