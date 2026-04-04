# V2 Execution Muscles Inventory

Date: 2026-04-04
Status: active

## Purpose

This note inventories the current V1/weed code that should be treated as
execution muscles for separation, rather than semantic authority.

## Movable Execution-Muscle Clusters

### 1. Recovery executor mechanics

Files:

1. `weed/storage/blockvol/v2bridge/executor.go`
2. related tests under `weed/storage/blockvol/v2bridge/*transfer*`
3. related tests under `weed/storage/blockvol/v2bridge/*snapshot*`
4. related tests under `weed/storage/blockvol/v2bridge/*truncate*`

Why this is a muscle:

1. it performs TCP rebuild/catch-up I/O
2. it applies WAL entries, snapshots, and full-base transfer
3. it does not decide recovery policy; the engine already decides the plan

### 2. Storage state read mechanics

File:

1. `weed/storage/blockvol/v2bridge/reader.go`

Why this is a muscle:

1. it only reads real `BlockVol` state
2. it populates the engine-facing retained-history view
3. it carries no semantic authority

### 3. Pin / retention hold mechanics

File:

1. `weed/storage/blockvol/v2bridge/pinner.go`

Why this is a muscle:

1. it only implements hold/release against real flusher retention
2. pin meaning belongs to the engine; this file only executes the hold

### 4. Recovery-side blockvol shims

Current location:

1. `weed/server/block_recovery.go`

Candidate functions:

1. `readerShimForRecovery`
2. `pinnerShimForRecovery`

Why they are muscles:

1. they are thin glue from runtime host code into bridge contracts
2. they should not remain mixed into recovery lifecycle ownership

## Code That Should Stay In weed/

### 1. Product adapter shell

Files:

1. `weed/server/volume_server_block.go`
2. `weed/server/block_recovery.go`

Why they stay:

1. process lifecycle and goroutine hosting
2. command dispatch into real servers/backends
3. heartbeat, iSCSI, NVMe, and server integration

### 2. Raw blockvol backend

Files:

1. `weed/storage/blockvol/*`

Why they stay:

1. they are the actual backend implementation
2. the separation goal is not to move `BlockVol` itself into `sw-block`

### 3. Real master/heartbeat wire adaptation

File:

1. `weed/storage/blockvol/v2bridge/control.go`

Why it stays for now:

1. it consumes `weed/storage/blockvol.BlockVolumeAssignment`
2. it is still closest to master / heartbeat wire shape
3. canonical mapping logic can move into `sw-block`, but source-format
   adaptation still belongs in `weed/`

## Main Migration Risks

### 1. Hidden weed dependency

Risk:

1. `executor.go`, `reader.go`, and `pinner.go` directly depend on
   `weed/storage/blockvol.BlockVol`

Implication:

1. they cannot be moved into `sw-block` unchanged
2. ports/interfaces must be stabilized first

### 2. Assignment translation drift

Risk files:

1. `weed/storage/blockvol/v2bridge/control.go`
2. `weed/server/volume_server_block.go`

Implication:

1. if migration happens before canonical helper extraction, identity and
   recovery-target rules can drift

### 3. Runtime host coupling

Risk file:

1. `weed/server/block_recovery.go`

Implication:

1. goroutine lifecycle, logging, sender invalidation, and core event emission
   are still mixed together
2. only the execution muscles should move first, not the whole runtime host

## Recommended Move Order

1. stabilize contracts in `sw-block/bridge/blockvol`
2. extract canonical mapping helpers into `sw-block`
3. migrate reader/pinner/executor logic behind those ports
4. shrink `weed/server` to adapter shell afterward
