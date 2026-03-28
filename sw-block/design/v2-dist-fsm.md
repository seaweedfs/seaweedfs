# WAL V2 Distributed Simulator

Date: 2026-03-26
Status: design proposal
Purpose: define the next prototype layer above `ReplicaFSM` and `VolumeModel` so WAL V2 can be validated as a distributed state machine rather than only a local state machine

## Why This Exists

The current V2 prototype already has:

- `ReplicaFSM`
- `VolumeModel`
- `RecoveryPlanner`
- scenario tracing

That is enough to reason about local recovery logic and volume-level admission.

It is not enough to prove the distributed safety claim.

The real system question is:

- when time moves forward, nodes start/stop/disconnect/reconnect, and the coordinator changes epoch,
- do all acknowledged writes remain recoverable according to the configured durability policy?

That requires a distributed simulator.

## Core Idea

Model the system as:

1. node-local state machines
2. a coordinator state machine
3. a time-driven message simulator
4. a reference data model used as the correctness oracle

## Layers

### 1. `NodeModel`

Each node has:

- role
- epoch seen
- local WAL state
  - head
  - tail
  - `receivedLSN`
  - `flushedLSN`
- checkpoint/snapshot state
  - `cpLSN`
- local extent state
- local connectivity state
- local `ReplicaFSM` for each remote relationship as needed

### 2. `CoordinatorModel`

The coordinator owns:

- current epoch
- primary assignment
- membership
- durability policy
- rebuild assignments
- promotion decisions

### 3. `Network/Time Simulator`

The simulator owns:

- logical time ticks
- message delivery queues
- delay, drop, and disconnect events
- node start/stop/restart

### 4. `Reference Model`

The reference model is the correctness oracle.

It applies the committed write history to an idealized block map.
At any target `LSN = X`, it can answer:

- what value should each block contain at `X`?

## Data Correctness Model

### Synthetic 4K writes

For simulation, each 4K write should be represented as:

- block ID
- value

A simple deterministic choice is:
- `value = LSN`

Example:
- `LSN 10`: write block 7 = 10
- `LSN 11`: write block 2 = 11
- `LSN 12`: write block 7 = 12

This makes correctness checks trivial.

### Why this matters

This catches the exact extent-recovery trap:

1. `LSN 10`: block 7 = 10
2. `LSN 12`: block 7 = 12

If recovery claims to rebuild state at `LSN 10` using current extent and returns block 7 = 12, the simulator detects the bug immediately.

## Golden Invariant

For any node declared recovered to target `LSN = T`:

- node extent state must equal the reference model's state at `T`

Not:
- equal to current latest state
- equal to any valid-looking value

Exactly:
- the reference state at target `LSN`

## Recovery Correctness Rules

### WAL replay correctness

For `(startLSN, endLSN]` replay to be valid:

- every record in the interval must exist
- every payload must be the correct historical version for its LSN
- no replay gaps are allowed
- no stale-epoch records are allowed

### Extent/snapshot correctness

Extent-based recovery is valid only if the data source is version-correct.

Allowed examples:
- immutable snapshot at `cpLSN`
- pinned copy-on-write generation
- pinned payload object referenced by a recovery record

Not allowed:
- current live extent used as if it were historical state at old `cpLSN`

## Suggested Prototype Package

Prototype location:
- `sw-block/prototype/distsim/`

Suggested files:
- `types.go`
- `node.go`
- `coordinator.go`
- `network.go`
- `reference.go`
- `scenario.go`
- `sim_test.go`

## Minimal First Milestone

Do not try to simulate the whole product first.

First milestone:

1. one primary
2. one replica
3. time ticks
4. synthetic 4K writes with deterministic values
5. canonical reference model
6. simple recovery check:
   - WAL replay recovers correct value
   - current extent alone does not recover old `LSN`
   - snapshot/base image at `cpLSN` does recover correct value

If that milestone is solid, then add:
- failover
- quorum
- multi-replica
- coordinator promotion rules

## Test Cases To Add Early

### 1. WAL replay preserves historical values
- write block 7 = 10
- write block 7 = 12
- replay only to `LSN 10`
- expect block 7 = 10

### 2. Current extent cannot reconstruct old `LSN`
- same write sequence
- try rebuilding `LSN 10` from latest extent
- expect mismatch/error

### 3. Snapshot at `cpLSN` works
- snapshot at `LSN 10`
- later overwrite block 7 at `LSN 12`
- rebuild from snapshot `LSN 10`
- expect block 7 = 10

### 4. Reservation expiration invalidates recovery
- recovery window initially valid
- time advances
- reservation expires
- recovery must abort rather than return partial or wrong state

## Relationship To Existing Prototype

This simulator should reuse existing prototype concepts where possible:

- `fsmv2` for node-local recovery lifecycle
- `volumefsm` ideas for mode semantics and admission
- `RecoveryPlanner` for recoverability decisions

The simulator is the next proof layer:
- not just whether transitions are legal
- but whether data remains correct under those transitions

## Bottom Line

WAL V2 correctness is not only a state problem.
It is also a data-version problem.

The distributed simulator should therefore prove two things together:

1. state-machine safety
2. data correctness at target `LSN`

That is the right next prototype layer if the goal is to prove:
- quorum commit safety
- no committed data loss
- no incorrect recovery from later extent state
