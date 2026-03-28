# WAL Replication V2 Orchestrator

Date: 2026-03-26
Status: design proposal
Purpose: define the volume-level orchestration model that sits above the per-replica WAL V2 FSM

## Why This Document Exists

`ReplicaFSM` alone is not enough.

It can describe one replica relative to the current primary, but it cannot by itself model:

- primary head continuing to advance
- multiple replicas in different states
- durability mode semantics
- primary lease loss and epoch change
- primary failover and replica promotion
- fencing of old recovery sessions

So WAL V2 needs a second layer:
- per-replica `ReplicaFSM`
- volume-level `Orchestrator`

## Scope

This document defines the volume-level logic only.

It does not define:
- exact network protocol
- exact master RPCs
- exact storage backend internals

It assumes the per-replica state machine from:
- `wal-replication-v2-state-machine.md`

## Core Model

The orchestrator owns:

1. current primary lineage
- `epoch`
- lease/authority state

2. volume durability mode
- `best_effort`
- `sync_all`
- `sync_quorum`

3. moving primary progress
- `headLSN`
- checkpoint/snapshot anchors

4. replica set
- one `ReplicaFSM` per replica
- per-replica role in the current volume topology

5. volume-level admission decision
- can writes proceed?
- can sync requests complete?
- must promotion/failover occur?

## Two FSM Layers

### Layer A: `ReplicaFSM`

Owns per-replica state such as:
- `Bootstrapping`
- `InSync`
- `Lagging`
- `CatchingUp`
- `PromotionHold`
- `NeedsRebuild`
- `Rebuilding`
- `CatchUpAfterRebuild`
- `Failed`

### Layer B: `VolumeOrchestrator`

Owns system-wide state such as:
- current `epoch`
- current primary identity
- durability mode
- set of required replicas
- current `headLSN`
- whether writes or promotions are allowed

The orchestrator does not replace `ReplicaFSM`.
It drives it.

## Volume State

The orchestrator should track at least:

```go
type VolumeMode string

type PrimaryState string

const (
    PrimaryServing  PrimaryState = "Serving"
    PrimaryDraining PrimaryState = "Draining"
    PrimaryLost     PrimaryState = "Lost"
)

type VolumeModel struct {
    Epoch        uint64
    PrimaryID    string
    PrimaryState PrimaryState
    Mode         VolumeMode

    HeadLSN       uint64
    CheckpointLSN uint64

    RequiredReplicaIDs []string
    Replicas           map[string]*ReplicaFSM
}
```

This is a model shape, not a required production struct.

## Orchestrator Responsibilities

### 1. Advance primary head

When primary commits a new write:
- increment `headLSN`
- enqueue/send to replica sender loops
- evaluate whether the current mode still allows ACK

### 2. Evaluate sync eligibility

The orchestrator computes volume-level durability from replica states.

Derived rule:
- only `ReplicaFSM.IsSyncEligible()` counts

### 3. Drive recovery entry

When a replica disconnects or falls behind:
- feed disconnect/lag events into that replica FSM
- decide whether to try catch-up or rebuild
- acquire recovery reservation if required

### 4. Handle primary authority changes

When lease is lost or a new primary is chosen:
- increment epoch
- abort stale recovery sessions
- reevaluate all replica relationships from the new primary's perspective

### 5. Drive promotion / failover

When current primary is lost:
- choose promotion candidate
- assign new epoch
- move old primary to stale/lost
- convert the promoted replica into the new serving primary
- reclassify remaining replicas relative to the new primary

## Required Volume-Level Events

The orchestrator should be able to simulate at least these events.

### Write/progress events
- `WriteCommitted(lsn)`
- `CheckpointAdvanced(lsn)`
- `BarrierCompleted(replicaID, flushedLSN)`

### Replica health events
- `ReplicaDisconnected(replicaID)`
- `ReplicaReconnect(replicaID, flushedLSN)`
- `ReplicaReservationLost(replicaID)`
- `ReplicaCatchupTimeout(replicaID)`
- `ReplicaRebuildTooSlow(replicaID)`

### Topology/control events
- `PrimaryLeaseLost()`
- `EpochChanged(newEpoch)`
- `PromoteReplica(replicaID)`
- `ReplicaAssigned(replicaID)`
- `ReplicaRemoved(replicaID)`

## Mode Semantics

### `best_effort`

Rules:
- ACK after primary local durability
- replicas may be `Lagging`, `CatchingUp`, `NeedsRebuild`, or `Rebuilding`
- background recovery continues

Volume implication:
- primary can keep serving while replicas recover

### `sync_all`

Rules:
- ACK only when all required replicas are `InSync` and durable through target LSN
- bounded retry only
- no silent downgrade

Volume implication:
- one lagging required replica can block sync completion
- orchestrator may fail requests, not silently reinterpret policy

### `sync_quorum`

Rules:
- ACK when quorum of required nodes are durable through target LSN
- lagging replicas may recover in background as long as quorum remains

Volume implication:
- orchestrator must count eligible replicas, not just healthy sockets

## Primary-Head Simulation Rules

The orchestrator must explicitly model that the primary keeps moving.

### Rule 1: head moves independently of replica recovery

A replica entering `CatchingUp` does not freeze `headLSN`.

### Rule 2: each recovery attempt uses explicit targets

For a replica in recovery, orchestrator chooses:
- `catchupTargetLSN = H0`
- or `snapshotCpLSN = C` and replay target `H0`

### Rule 3: promotion is explicit

A replica is not restored to `InSync` just because it reaches `H0`.

It must still pass:
- barrier confirmation
- `PromotionHold`

## Failover / Promotion Model

The orchestrator must be able to simulate:

1. old primary loses lease
2. old primary is fenced by epoch change
3. one replica is promoted
4. promoted replica becomes new primary under a higher epoch
5. all old recovery sessions from the old primary are invalidated
6. remaining replicas are reevaluated relative to the new primary's head and retained history

Important consequence:
- failover is not a `ReplicaFSM` transition only
- it is a volume-level re-rooting of all replica relationships

## Suggested Promotion Rules

Promotion candidate should prefer:
1. highest valid durable progress
2. current epoch-consistent history
3. healthiest replica among tied candidates

After promotion:
- `PrimaryID` changes
- `Epoch` increments
- all replica reservations from the previous primary are void
- all non-primary replicas must renegotiate recovery against the new primary

## Multi-Replica Examples

### Example 1: `sync_all`

- replica A = `InSync`
- replica B = `Lagging`
- replica C = `InSync`

If A and B are required replicas in RF=3 `sync_all`:
- writes needing sync durability fail or wait
- even though one replica is still healthy

### Example 2: `sync_quorum`

- replica A = `InSync`
- replica B = `CatchingUp`
- replica C = `InSync`

If quorum is 2:
- volume can continue serving sync requests
- B recovers in background

### Example 3: failover

- old primary lost
- replica A promoted
- replica B was previously `CatchingUp` under old epoch

After promotion:
- B's old session is aborted
- B re-enters evaluation against A's history

## What The Tiny Prototype Should Simulate

The V2 prototype should be able to drive at least these scenarios:

1. steady state keep-up
- primary head advances
- all required replicas remain `InSync`

2. short outage
- one replica disconnects
- primary keeps writing
- reconnect succeeds within recoverable window
- replica returns via `PromotionHold`

3. long outage
- one replica disconnects too long
- recoverability expires
- replica goes `NeedsRebuild`
- rebuild and trailing replay complete

4. tail chasing
- replica catch-up speed is below primary ingest speed
- orchestrator chooses fail, throttle, or rebuild path depending on mode

5. failover
- primary lease lost
- new epoch assigned
- replica promoted
- old recovery sessions fenced

6. mixed-state quorum
- different replicas in different states
- orchestrator computes correct `sync_all` / `sync_quorum` result

## Relationship To WAL V1

WAL V1 already contains pieces of this logic, but they are scattered across:
- shipper state
- barrier code
- retention code
- assignment/promotion code
- rebuild code
- heartbeat/master logic

V2 should separate these into:
- per-replica recovery FSM
- volume-level orchestrator

## Bottom Line

The next step after `ReplicaFSM` is not `Smart WAL`.

The next step is the volume-level orchestrator model.

Why:
- primary keeps moving
- durability mode is volume-scoped
- failover/promotion is volume-scoped
- replica recovery must be evaluated in the context of the whole volume

So V2 needs:
- `ReplicaFSM` for one replica
- `VolumeOrchestrator` for the moving multi-replica system
