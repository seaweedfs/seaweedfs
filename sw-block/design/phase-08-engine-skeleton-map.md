# Phase 08 Engine Skeleton Map

Date: 2026-03-31
Status: active
Purpose: provide a short structural map for the `Phase 08` hardening path so implementation can move faster without reopening accepted V2 boundaries

## Scope

This is not the final standalone `sw-block` architecture.

It is the shortest useful engine skeleton for the accepted `Phase 08` hardening path:

- `RF=2`
- `sync_all`
- existing `Seaweed` master / volume-server heartbeat path
- V2 engine owns recovery policy
- `blockvol` remains the execution backend

## Module Map

### 1. Control plane

Role:

- authoritative control truth

Primary sources:

- `weed/server/master_grpc_server.go`
- `weed/server/master_block_registry.go`
- `weed/server/master_block_failover.go`
- `weed/server/volume_grpc_client_to_master.go`

What it produces:

- confirmed assignment
- `Epoch`
- target `Role`
- failover / promotion / reassignment result
- stable server identity

### 2. Control bridge

Role:

- translate real control truth into V2 engine intent

Primary files:

- `weed/storage/blockvol/v2bridge/control.go`
- `sw-block/bridge/blockvol/control_adapter.go`
- entry path in `weed/server/volume_server_block.go`

What it produces:

- `AssignmentIntent`
- stable `ReplicaID`
- `Endpoint`
- `SessionKind`

### 3. Engine runtime

Role:

- recovery-policy core

Primary files:

- `sw-block/engine/replication/orchestrator.go`
- `sw-block/engine/replication/driver.go`
- `sw-block/engine/replication/executor.go`
- `sw-block/engine/replication/sender.go`
- `sw-block/engine/replication/history.go`

What it decides:

- zero-gap / catch-up / needs-rebuild
- sender/session ownership
- stale authority rejection
- resource acquisition / release
- rebuild source selection

### 4. Storage bridge

Role:

- translate real blockvol storage truth and execution capability into engine-facing adapters

Primary files:

- `weed/storage/blockvol/v2bridge/reader.go`
- `weed/storage/blockvol/v2bridge/pinner.go`
- `weed/storage/blockvol/v2bridge/executor.go`
- `sw-block/bridge/blockvol/storage_adapter.go`

What it provides:

- `RetainedHistory`
- WAL retention pin / release
- snapshot pin / release
- full-base pin / release
- WAL scan execution

### 5. Block runtime

Role:

- execute real I/O

Primary files:

- `weed/storage/blockvol/blockvol.go`
- `weed/storage/blockvol/replica_apply.go`
- `weed/storage/blockvol/replica_barrier.go`
- `weed/storage/blockvol/recovery.go`
- `weed/storage/blockvol/rebuild.go`
- `weed/storage/blockvol/wal_shipper.go`

What it owns:

- WAL
- extent
- flusher
- checkpoint / superblock
- receiver / shipper
- rebuild server

## Execution Order

### Control path

```text
master heartbeat / failover truth
  -> BlockVolumeAssignment
  -> volume server ProcessAssignments
  -> v2bridge control conversion
  -> engine ProcessAssignment
  -> sender/session state updated
```

### Catch-up path

```text
assignment accepted
  -> engine reads retained history
  -> engine plans catch-up
  -> storage bridge pins WAL retention
  -> engine executor drives v2bridge executor
  -> blockvol scans WAL / ships entries
  -> engine completes session
```

### Rebuild path

```text
assignment accepted
  -> engine detects NeedsRebuild
  -> engine selects rebuild source
  -> storage bridge pins snapshot/full-base/tail
  -> executor drives transfer path
  -> blockvol performs restore / replay work
  -> engine completes rebuild
```

### Local durability path

```text
WriteLBA / Trim
  -> WAL append
  -> shipping / barrier
  -> client-visible durability decision
  -> flusher writes extent
  -> checkpoint advances
  -> retention floor decides WAL reclaimability
```

## Interim Fields

These are currently acceptable only as explicit hardening carry-forwards:

### `localServerID`

Current source:

- `BlockService.listenAddr`

Meaning:

- temporary local identity source for replica/rebuild-side assignment translation

Status:

- interim only
- should become registry-assigned stable server identity later

### `CommittedLSN = CheckpointLSN`

Current source:

- `v2bridge.Reader` / `BlockVol.StatusSnapshot()`

Meaning:

- current V1-style interim mapping where committed truth collapses to local checkpoint truth

Status:

- not final V2 truth
- must become a gate decision before a production-candidate phase

### heartbeat as control carrier

Current source:

- existing master <-> volume-server heartbeat path

Meaning:

- current transport for assignment/control delivery

Status:

- acceptable as current carrier
- not yet a final proof that no separate control channel will ever be needed

## Hard Gates

These should remain explicit in `Phase 08`:

### Gate 1: committed truth

Before production-candidate:

- either separate `CommittedLSN` from `CheckpointLSN`
- or explicitly bound the first candidate path to currently proven pre-checkpoint replay behavior

### Gate 2: live control delivery

Required:

- real assignment delivery must reach the engine on the live path
- not only converter-level proof

### Gate 3: integrated catch-up closure

Required:

- engine -> executor -> `v2bridge` -> blockvol must be proven as one live chain
- not planner proof plus direct WAL-scan proof as separate evidence

### Gate 4: first rebuild execution path

Required:

- rebuild must not remain only a detection outcome
- the chosen product path needs one real executable rebuild closure

### Gate 5: unified replay

Required:

- after control and execution closure land, rerun the accepted failure-class set on the unified live path

## Reuse Map

### Reuse directly

- `weed/server/master_grpc_server.go`
- `weed/server/volume_grpc_client_to_master.go`
- `weed/server/volume_server_block.go`
- `weed/server/master_block_registry.go`
- `weed/server/master_block_failover.go`
- `weed/storage/blockvol/blockvol.go`
- `weed/storage/blockvol/replica_apply.go`
- `weed/storage/blockvol/replica_barrier.go`
- `weed/storage/blockvol/v2bridge/`

### Reuse as implementation reality, not truth

- `shipperGroup`
- `RetentionFloorFn`
- `ReplicaReceiver`
- checkpoint/superblock machinery
- existing failover heuristics

### Do not inherit as V2 semantics

- address-shaped identity
- old degraded/catch-up intuition from V1/V1.5
- `CommittedLSN = CheckpointLSN` as final truth
- blockvol-side recovery policy decisions

## Short Rule

Use this skeleton as:

- a hardening map for the current product path

Do not mistake it for:

- the final standalone `sw-block` architecture
