# V2 Assignment Translation Unification

Date: 2026-04-04
Status: active

## Purpose

This note defines how assignment translation should be unified so that:

1. `weed/storage/blockvol/v2bridge/control.go`
2. `weed/server/volume_server_block.go`

do not drift on identity, role, or recovery-target meaning.

## Current Drift Risk

Today there are two live translation sites:

1. `ControlBridge.ConvertAssignment()` produces `engine.AssignmentIntent`
2. `BlockService.coreAssignmentEvent()` produces `engine.AssignmentDelivered`

They do not translate the same source type, but they do share semantic rules:

1. how to build stable `ReplicaID`
2. how to map role-shaped inputs to recovery target
3. how to represent one local replica endpoint in engine types

If those rules stay duplicated, later migration batches will reintroduce split
truth.

## Canonical Rule Placement

The canonical reusable rules belong in:

1. `sw-block/bridge/blockvol`

Why:

1. the rules are semantic translation, not product integration
2. both `weed/storage/blockvol/v2bridge` and `weed/server` can import this
   package
3. `sw-block` remains weed-free

## Rules That Must Be Canonical

### 1. Stable identity

Canonical helper:

1. `MakeReplicaID()`
2. `ReplicaAssignmentForServer()`

Rule:

1. `ReplicaID = <volume>/<server-id>`
2. never derive identity from transport address

### 2. Recovery-target mapping

Canonical helper:

1. `RecoveryTargetForRole()`

Rule:

1. `replica -> catchup`
2. `rebuilding -> rebuild`
3. all other roles -> no recovery target

### 3. Endpoint packaging

Canonical helper:

1. `ReplicaAssignmentForServer()`

Rule:

1. adapter code may still source endpoint fields from different wire/runtime
   inputs
2. but once packaged into `engine.ReplicaAssignment`, the shape must be uniform

## What Still Stays Local

These parts remain adapter-local and should NOT be forced into one helper yet:

1. reading `blockvol.BlockVolumeAssignment`
2. deciding whether the local VS is primary/replica/rebuilding in a given
   runtime context
3. multi-replica traversal over heartbeat/master wire structures

Those are source-format adaptation concerns, not canonical translation rules.

## Implemented First Step

The first unification step is already applied:

1. `sw-block/bridge/blockvol/control_adapter.go`
   - now exports the canonical helpers
2. `weed/server/volume_server_block.go`
   - now consumes the same helper layer for local assignment rebinding

## Next Step

The next step after this document is:

1. reduce `weed/storage/blockvol/v2bridge/control.go` to source-format
   extraction only
2. keep all shared semantic mapping rules in `sw-block/bridge/blockvol`
