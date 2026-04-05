# V2 Loop1 Surface Draft

Date: 2026-04-05
Status: active

## Purpose

This note turns the two-loop design into a code-facing draft for the current
`masterv2` and `volumev2` packages.

It does not implement the new protocol yet.
It defines the smallest surface refactor that should happen first.

## Goal

Replace the current mixed `heartbeat -> assignments` MVP surface with three
separate `Loop 1` surfaces:

1. periodic heartbeat
2. promotion query
3. assignment

## Current State

Today, `masterv2` uses one heartbeat type and one assignment type:

- `sw-block/runtime/masterv2/master.go`
- `sw-block/runtime/volumev2/control_session.go`
- `sw-block/runtime/volumev2/volume.go`

The current heartbeat still mixes:

1. applied identity
2. outward projection naming
3. fields that should later become failover evidence

## Target Surfaces

### 1. Heartbeat

Purpose:

1. liveness
2. applied identity
3. outward compressed mode

Recommended fields:

- `NodeID`
- `ReportedAt`
- per-volume:
  - `Name`
  - `Path`
  - `Epoch`
  - `Role`
  - `Mode`
  - `ModeReason`
  - passive `CommittedLSN` cache
  - `RoleApplied`
  - `ReplicaReady`

Not included:

- per-replica progress
- catch-up target
- detailed recovery phase

If `CommittedLSN` is present here, it is only a convenience cache for
`masterv2`. Fresh promotion judgment still uses the query channel.

### 2. Promotion Query

Purpose:

1. fresh promotion evidence
2. failover arbitration

Recommended request:

- `VolumeName`
- `ExpectedEpoch`

Recommended response:

- `VolumeName`
- `NodeID`
- `Epoch`
- `Role`
- `CommittedLSN`
- `WALHeadLSN`
- `ReceiverReady`
- `Eligible`
- `Reason`

Rule:

- `CommittedLSN` is the primary selection key
- `WALHeadLSN` is only a tiebreaker

### 3. Assignment

Purpose:

1. authorize role
2. fence stale owners
3. deliver member identity

Recommended fields:

- `Name`
- `Path`
- `NodeID`
- `Epoch`
- `LeaseTTL`
- `Role`
- `ReplicaSet`
- `CreateOptions`

`ReplicaSet` should carry identity plus transport addresses, not progress.

## Field Migration From Current MVP

### Current `masterv2.VolumeHeartbeat`

Today:

- `Name`
- `Path`
- `Epoch`
- `Role`
- `ProjectionMode`
- `PublicationReason`
- `RoleApplied`

Should become:

- `Name`
- `Path`
- `Epoch`
- `Role`
- `Mode`
- `ModeReason`
- `RoleApplied`
- `ReplicaReady`

Interpretation change:

- `ProjectionMode` should be renamed to `Mode`
- `PublicationReason` should stop pretending to be a generic reason field
- `ReplicaReady` should be explicit on the identity surface

### Current `masterv2.VolumeView`

Today:

- `ObservedEpoch`
- `ObservedRole`
- `ProjectionMode`
- `PublicationReason`
- `RoleApplied`

Should become:

- `ObservedEpoch`
- `ObservedRole`
- `Mode`
- `ModeReason`
- `RoleApplied`
- `ReplicaReady`

Optional later:

- bounded cached failover evidence for debugging only

### Current `volumev2.Node.Heartbeat()`

Current source:

- `snap.Status`
- `snap.Projection`

Recommended extraction:

- `Mode` from `snap.Projection.Mode.Name`
- `ModeReason` from `snap.Projection.Mode.Reason`
- passive `CommittedLSN` cache from local status snapshot
- `RoleApplied` from `snap.Projection.Readiness.RoleApplied`
- `ReplicaReady` from `snap.Projection.Readiness.ReplicaReady`

Not from heartbeat:

- `CatchUpTarget`
- `RecoveryProgress`

`CommittedLSN` may appear in heartbeat as a passive cache only.
Fresh promotion authority still belongs to the promotion-query channel.
`CatchUpTarget` and `RecoveryProgress` belong to Loop 2.

## Code Refactor Order

### Step 1

Add a small shared contract package for `Loop 1` types, for example under:

- `sw-block/runtime/protocolv2/`

Start with:

1. `Heartbeat`
2. `Assignment`
3. `PromotionQueryRequest`
4. `PromotionQueryResponse`

### Step 2

Make `masterv2` use the shared `Loop 1` contract types instead of local ad hoc
duplicates.

### Step 3

Make `volumev2` heartbeat generation write to the narrowed heartbeat shape.

### Step 4

Add a promotion-query interface without implementing full failover yet.

The first code slice only needs:

1. request and response types
2. local evidence extraction helper
3. one focused test proving query returns fresh local evidence

## Test Guidance

The first code refactor should keep existing control-loop tests and add one new
test:

1. existing heartbeat-assignment convergence should still pass
2. new promotion-query test should prove:
   - query is separate from heartbeat
   - fresh state is returned at call time
   - heartbeat `CommittedLSN` is only a passive cache
   - `WALHeadLSN` is not part of periodic heartbeat

## Non-Goals

This draft does not yet define:

1. full Loop 2 message schema
2. full new primary truth reconstruction choreography
3. quorum-specific `CommittedLSN` algorithm

Those come after the `Loop 1` surface is cleanly split.
