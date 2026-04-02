# Phase 07 Service-Slice Plan

Date: 2026-03-30
Status: historical phase-planning artifact
Scope: `Phase 07 P0`

## Purpose

Define the first real-system service slice that will host the V2 engine, choose the first concrete integration path in the existing codebase, and map engine adapters onto real modules.

This is a planning document. It does not claim the integration already works.

## Decision

The first service slice should be:

- a single `blockvol` primary on a real volume server
- with one replica target (`RF=2` path)
- driven by the existing master heartbeat / assignment loop
- using the V2 engine only for replication recovery ownership / planning / execution

This is the narrowest real-system slice that still exercises:

1. real assignment delivery
2. real epoch and failover signals
3. real volume-server lifecycle
4. real WAL/checkpoint/base-image truth
5. real changed-address / reconnect behavior

It is narrow enough to avoid reopening the whole system, but real enough to stop hiding behind engine-local mocks.

## Why This Slice

This slice is the right first integration target because:

1. `weed/server/master_grpc_server.go` already delivers block-volume assignments over heartbeat
2. `weed/server/master_block_failover.go` already owns failover / promotion / pending rebuild decisions
3. `weed/storage/blockvol/blockvol.go` already owns the current replication runtime (`shipperGroup`, receiver, WAL retention, checkpoint state)
4. the existing V1/V1.5 failure history is concentrated in exactly this master <-> volume-server <-> blockvol path

So this slice gives maximum validation value with minimum new surface.

## First Concrete Integration Path

The first integration path should be:

1. master receives volume-server heartbeat
2. master updates block registry and emits `BlockVolumeAssignment`
3. volume server receives assignment
4. block volume adapter converts assignment + local storage state into V2 engine inputs
5. V2 engine drives sender/session/recovery state
6. existing block-volume runtime executes the actual data-path work under engine decisions

In code, that path starts here:

- master side:
  - `weed/server/master_grpc_server.go`
  - `weed/server/master_block_failover.go`
  - `weed/server/master_block_registry.go`
- volume / storage side:
  - `weed/storage/blockvol/blockvol.go`
  - `weed/storage/blockvol/recovery.go`
  - `weed/storage/blockvol/wal_shipper.go`
  - assignment-handling code under `weed/storage/blockvol/`
- V2 engine side:
  - `sw-block/engine/replication/`

## Service-Slice Boundaries

### In-process placement

The V2 engine should initially live:

- in-process with the volume server / `blockvol` runtime
- not in master
- not as a separate service yet

Reason:

- the engine needs local access to storage truth and local recovery execution
- master should remain control-plane authority, not recovery executor

### Control-plane boundary

Master remains authoritative for:

1. epoch
2. role / assignment
3. promotion / failover decision
4. replica membership

The engine consumes these as control inputs. It does not replace master failover policy in `Phase 07`.

### Control-Over-Heartbeat Upgrade Path

For the first V2 product path, the recommended direction is:

- reuse the existing master <-> volume-server heartbeat path as the control carrier
- upgrade the block-specific control semantics carried on that path
- do not immediately invent a separate control service or assignment channel

Why:

1. this is the real Seaweed path already carrying block assignments and confirmations today
2. this gives the fastest route to a real integrated control path
3. it preserves compatibility with existing Seaweed master/volume-server semantics while V2 hardens its own control truth

Concretely, the current V1 path already provides:

1. block assignments delivered in heartbeat responses from `weed/server/master_grpc_server.go`
2. assignment application on the volume server in `weed/server/volume_grpc_client_to_master.go` and `weed/server/volume_server_block.go`
3. assignment confirmation and address-change refresh driven by later heartbeats in `weed/server/master_grpc_server.go` and `weed/server/master_block_registry.go`
4. immediate block heartbeat on selected shipper state changes in `weed/server/volume_grpc_client_to_master.go`

What should be upgraded for V2 is not mainly the transport, but the control contract carried on it:

1. stable `ReplicaID`
2. explicit `Epoch`
3. explicit role / assignment authority
4. explicit apply/confirm semantics
5. explicit stale assignment rejection
6. explicit address-change refresh as endpoint change, not identity change

Current cadence note:

- the block volume heartbeat is periodic (`5 * sleepInterval`) with some immediate state-change heartbeats
- this is acceptable as the first hardening carrier
- it should not be assumed to be the final control responsiveness model

Deferred design decision:

- whether block control should eventually move beyond heartbeat-only carriage into a more explicit control/assignment channel should be decided only after the `Phase 08 P1` real control-delivery path exists and can be measured

That later decision should be based on:

1. failover / reassignment responsiveness
2. assignment confirmation precision
3. operational complexity
4. whether heartbeat carriage remains too coarse for the block-control path

Until then, the preferred direction is:

- strengthen block control semantics over the existing heartbeat path
- do not prematurely create a second control plane

### Storage boundary

`blockvol` remains authoritative for:

1. WAL head / retention reality
2. checkpoint/base-image reality
3. actual catch-up streaming
4. actual rebuild transfer / restore operations

The engine consumes these as storage truth and recovery execution capabilities. It does not replace the storage backend in `Phase 07`.

## First-Slice Identity Mapping

This must be explicit in the first integration slice.

For `RF=2` on the existing master / block registry path:

- stable engine `ReplicaID` should be derived from:
  - `<volume-name>/<replica-server-id>`
- not from:
  - `DataAddr`
  - `CtrlAddr`
  - heartbeat transport endpoint

For this slice, the adapter should map:

1. `ReplicaID`
- from master/block-registry identity for the replica host entry

2. `Endpoint`
- from the current replica receiver/data/control addresses reported by the real runtime

3. `Epoch`
- from the confirmed master assignment for the volume

4. `SessionKind`
- from master-driven recovery intent / role transition outcome

This is a hard first-slice requirement because address refresh must not collapse identity back into endpoint-shaped keys.

## Adapter Mapping

### 1. ControlPlaneAdapter

Engine interface today:

- `HandleHeartbeat(serverID, volumes)`
- `HandleFailover(deadServerID)`

Real mapping should be:

- master-side source:
  - `weed/server/master_grpc_server.go`
  - `weed/server/master_block_failover.go`
  - `weed/server/master_block_registry.go`
- volume-server side sink:
  - assignment receive/apply path in `weed/storage/blockvol/`

Recommended real shape:

- do not literally push raw heartbeat messages into the engine
- instead introduce a thin adapter that converts confirmed master assignment state into:
  - stable `ReplicaID`
  - endpoint set
  - epoch
  - recovery target kind

That keeps master as control owner and the engine as execution owner.

Important note:

- the adapter should treat heartbeat as the transport carrier, not as the final protocol shape
- block-control semantics should be made explicit over that carrier
- if a later phase concludes that heartbeat-only carriage is too coarse, that should be a separate design decision after the real hardening path is measured

### 2. StorageAdapter

Engine interface today:

- `GetRetainedHistory()`
- `PinSnapshot(lsn)` / `ReleaseSnapshot(pin)`
- `PinWALRetention(startLSN)` / `ReleaseWALRetention(pin)`
- `PinFullBase(committedLSN)` / `ReleaseFullBase(pin)`

Real mapping should be:

- retained history source:
  - current WAL head/tail/checkpoint state from `weed/storage/blockvol/blockvol.go`
  - recovery helpers in `weed/storage/blockvol/recovery.go`
- WAL retention pin:
  - existing retention-floor / replica-aware WAL retention machinery around `shipperGroup`
- snapshot pin:
  - existing snapshot/checkpoint artifacts in `blockvol`
- full-base pin:
  - explicit pinned full-extent export or equivalent consistent base handle from `blockvol`

Important constraint:

- `Phase 07` must not fake this by reconstructing `RetainedHistory` from tests or metadata alone

### 3. Execution Driver / Executor hookup

Engine side already has:

- planner/executor split in `sw-block/engine/replication/driver.go`
- stepwise executors in `sw-block/engine/replication/executor.go`

Real mapping should be:

- engine planner decides:
  - zero-gap / catch-up / rebuild
  - trusted-base requirement
  - replayable-tail requirement
- blockvol runtime performs:
  - actual WAL catch-up transport
  - actual snapshot/base transfer
  - actual truncation / apply operations

Recommended split:

- engine owns contract and state transitions
- blockvol adapter owns concrete I/O work

## First-Slice Acceptance Rule

For the first integration slice, this is a hard rule:

- `blockvol` may execute recovery I/O
- `blockvol` must not own recovery policy

Concretely, `blockvol` must not decide:

1. zero-gap vs catch-up vs rebuild
2. trusted-base validity
3. replayable-tail sufficiency
4. whether rebuild fallback is required

Those decisions must remain in the V2 engine.

The bridge may translate engine decisions into concrete blockvol actions, but it must not re-decide recovery policy underneath the engine.

## First Product Path

The first product path should be:

- `RF=2` block volume replication on the existing heartbeat/assignment loop
- primary + one replica
- failover / reconnect / changed-address handling
- rebuild as the formal non-catch-up recovery path

This is the right first path because it exercises the core correctness boundary without introducing N-replica coordination complexity too early.

## What Must Be Replaced First

Current engine-stage pieces that are still mock/test-only or too abstract:

### Replace first

1. `mockStorage` in engine tests
- replace with a real `blockvol`-backed `StorageAdapter`

2. synthetic control events in engine tests
- replace with assignment-driven events from the real master/volume-server path

3. convenience recovery completion wrappers
- keep them test-only
- real integration should use planner + executor + storage work loop

### Can remain temporarily abstract in Phase 07 P0/P1

1. `ControlPlaneAdapter` exact public shape
- can remain thin while the integration path is being chosen

2. async production scheduler details
- executor can still be driven by a service loop before full background-task architecture is finalized

## Recommended Concrete Modules

### Engine stays here

- `sw-block/engine/replication/`

### First real adapter package should be added near blockvol

Recommended initial location:

- `weed/storage/blockvol/v2bridge/`

Reason:

- keeps V2 engine independent under `sw-block/`
- keeps real-system glue close to blockvol storage truth
- avoids copying engine logic into `weed/`

Suggested contents:

1. `control_adapter.go`
- convert master assignment / local apply path into engine intents

2. `storage_adapter.go`
- expose retained history, pin/release, trusted-base export handles from real blockvol state

3. `executor_bridge.go`
- translate engine executor steps into actual blockvol recovery actions

4. `observe_adapter.go`
- map engine status/logs into service-visible diagnostics

## First Failure Replay Set For Phase 07

The first real-system replay set should be:

1. changed-address restart
- current risk: old identity/address coupling reappears in service glue

2. stale epoch / stale result after failover
- current risk: master and engine disagree on authority timing

3. unreplayable-tail rebuild fallback
- current risk: service glue over-trusts checkpoint/base availability

4. plan/execution cleanup after resource failure
- current risk: blockvol-side resource failures leave engine or service state dangling

5. primary failover to replica with rebuild pending on old primary reconnect
- current risk: old V1/V1.5 semantics leak back into reconnect handling

## Non-Goals For This Slice

Do not use `Phase 07` to:

1. widen catch-up semantics
2. add smart rebuild optimizations
3. redesign all blockvol internals
4. replace the full V1 runtime in one move
5. claim production readiness

## Deliverables For Phase 07 P0

A good `P0` delivery should include:

1. chosen service slice
2. chosen integration path in the current repo
3. adapter-to-module mapping
4. list of test-only adapters to replace first
5. first failure replay set
6. explicit note of what remains outside this first slice

## Short Form

`Phase 07 P0` should start with:

- engine in `sw-block/engine/replication/`
- bridge in `weed/storage/blockvol/v2bridge/`
- first real slice = blockvol primary + one replica on the existing master heartbeat / assignment path
- `ReplicaID = <volume-name>/<replica-server-id>` for the first slice
- `blockvol` executes I/O but does not own recovery policy
- first product path = `RF=2` failover/reconnect/rebuild correctness
