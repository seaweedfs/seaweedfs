# V2 Two-Loop Protocol

Date: 2026-04-05
Status: active

## Purpose

This note fixes the protocol boundary for the next V2 step.

The goal is not to finalize every wire field before implementation.
The goal is to make the ownership boundary stable enough that automata,
constraints, and runtime packages can be reorganized without mixing identity
control and replication consensus again.

## Core Rule

The protocol is split into two loops:

1. `Loop 1`: identity control
2. `Loop 2`: data control

These loops must not be collapsed into one heartbeat or one state owner.

## Authority Principle

Each volume should be treated as a small distributed cluster:

1. `masterv2` is the identity authority outside the cluster
2. the selected primary is the data-control authority inside the cluster
3. replicas report bounded facts to the primary, not full truth to `masterv2`

This means:

1. `masterv2` decides who is allowed to own the role
2. the new primary decides how takeover, catch-up, and rebuild proceed
3. `masterv2` may query bounded facts for arbitration, but it does not choreograph
   data recovery step by step

## Loop 1: Identity Control

Owner:

- `masterv2 <-> volumev2`

Frequency:

- low
- heartbeat scale
- assignment scale
- promotion-query scale

## Three Control Channels

Within `Loop 1`, the control plane should be split into three different
channels. They must not be collapsed into one message type.

### 1. Heartbeat

Direction:

- `volumev2 -> masterv2`

Frequency:

- periodic
- lightweight

Purpose:

1. liveness detection
2. compressed outward mode
3. confirmation that assignment was applied

Heartbeat should carry only:

1. `NodeID`
2. per-volume `Mode`
3. applied `Epoch`
4. applied `Role`
5. `RoleApplied`
6. `ReplicaReady`
7. optional passive `CommittedLSN` cache

Heartbeat should not carry:

1. per-replica progress
2. catch-up targets
3. rebuild detail
4. full failover evidence

If `CommittedLSN` is carried in heartbeat, it is only a passive cache. The
authoritative failover-time value still comes from promotion query.

### 2. Promotion Query

Direction:

- `masterv2 -> candidate volumev2`
- candidate `volumev2 -> masterv2`

Frequency:

- on demand
- only during failover or promotion arbitration

Purpose:

1. obtain fresh failover evidence
2. avoid treating stale heartbeat cache as authority

Candidate response should include:

1. `CommittedLSN`
2. `WALHeadLSN` as a weaker tiebreaker
3. `Epoch`
4. `Role`
5. `ReceiverReady`
6. bounded eligibility reason if not promotable

The promotion query is where fresh identity-loop evidence is collected.
It is not a replacement for the data-control loop, and it is not a continuous
replication-progress feed.

If heartbeat also carries `CommittedLSN`, promotion query still wins whenever
fresh arbitration is required.

### 3. Assignment

Direction:

- `masterv2 -> volumev2`

Frequency:

- on demand
- role change or membership change

Purpose:

1. authorize role ownership
2. fence stale owners
3. deliver replica-set identity

### Master To Volume

`masterv2 -> volumev2` carries only:

1. `Epoch`
2. `Role`
3. `LeaseTTL`
4. `ReplicaSet` identities and addresses

It does not carry:

1. per-replica progress
2. catch-up target history
3. detailed rebuild plan

### Volume To Master

`volumev2 -> masterv2` carries only bounded identity evidence:

1. applied `Epoch`
2. applied `Role`
3. outward `Mode`
4. `RoleApplied`
5. `ReplicaReady`

This channel must remain lightweight. Fresh failover evidence belongs to the
promotion-query channel, not the periodic heartbeat.

## Loop 2: Data Control

Owner:

- `primary engine <-> replica engine`

Frequency:

- high
- write scale
- barrier scale
- reconnect scale

This is where replication consensus lives.

### Primary To Replica

Primary-side data-control messages should cover:

1. WAL entry stream
2. barrier request with `Epoch` and target LSN
3. reconnect or resume handshake
4. rebuild/catch-up execution requests when needed

### Replica To Primary

Replica-side data-control messages should cover:

1. `FlushedLSN`
2. bounded status such as `ok`, `epoch_mismatch`, `timeout`, `fsync_failed`
3. reconnect gap evidence
4. coarse local recovery state

### Primary-Owned Per-Replica State

The primary brain should own:

1. `ReplicaFlushedLSN`
2. `ShippedLSN` as diagnostic only
3. replica `State`
4. `CatchUpTarget`
5. `RetentionFloor`
6. `LastContactTime`

## Role Of Masterv2

`masterv2` authorizes:

1. who is primary
2. who is replica
3. which epoch is active
4. when stale owners must be fenced

`masterv2` must not decide:

1. replay from LSN `X` to `Y`
2. whether the next action is `keepup` or `catchup`
3. how rebuild is executed
4. continuous commit progress

`masterv2` may query candidates for fresh promotion evidence, but it still does
not become the owner of replication history.

## Reconstruction And Takeover

Promotion and reconstruction are related, but they do not have the same owner.

### What Masterv2 Leads

`masterv2` leads:

1. failover detection
2. epoch fencing
3. candidate query for fresh promotion evidence
4. primary selection
5. assignment of the new primary role

### What The New Primary Leads

The selected replacement primary leads:

1. local assignment realization
2. collection of self and peer replica summaries
3. bounded truth reconstruction
4. fail-closed takeover gating
5. follow-on `keepup`, `catchup`, or `rebuild` orchestration

### Rule

`masterv2` may say:

1. "you are now the authorized primary candidate for epoch `E`"
2. "these are the members of the replica set"

But `masterv2` must not say:

1. "replay from LSN `X` to `Y`"
2. "use replica `R` as the rebuild source"
3. "enter `catchup` before `rebuild`"
4. "the cluster is safe because my last cached view looked healthy"

## Role Of Primary Brain

The primary brain discovers:

1. current replica state
2. gap or retention situation
3. barrier success or failure
4. whether the volume is `keepup`, `catchup`, `degraded`, or `needs_rebuild`

The primary brain decides:

1. keep shipping
2. start catch-up
3. escalate to `needs_rebuild`
4. start rebuild after role/assignment allows it

## Distributed State-Machine Rules

### 1. Different Nodes Have Different Views

Each node must distinguish:

1. local execution truth
2. last observed peer truth
3. cluster identity truth from `masterv2`

Do not collapse these into one blob.

### 2. All Peer Observations Are Epoch-Scoped

Any peer observation that affects recovery must be tied to:

1. `Epoch`
2. session or generation token

Old-epoch observations must be ignored or fail closed.

### 3. New Primary Reconstructs Truth

After failover, the new primary must rebuild its own data-control truth from:

1. local state
2. peer summaries
3. reconnect handshakes

It must not trust `masterv2` as a cache of full recovery history.
It may use `masterv2` only as the source of authorization and replica identity.

### 4. Outward Mode Is Compressed Evidence

`allocated_only`, `bootstrap_pending`, `publish_healthy`, `degraded`, and
`needs_rebuild` are public meanings, not the full internal recovery automaton.

### 5. Ambiguity Fails Closed

When barrier lineage, progress lineage, or epoch lineage is unclear, the system
must prefer:

1. `degraded`
2. `needs_rebuild`
3. no promotion without enough eligibility evidence

## Constraint Migration

Most of the last week's V2 work remains valid. The important change is where
each constraint belongs.

### Keep As-Is

These constraints still stand:

1. epoch fencing
2. one active session per replica per epoch
3. `catchup` and `rebuild` are different paths
4. fail closed on ambiguous recovery truth
5. semantics first, adapters later

### Move To Loop 1

These belong to identity control:

1. assignment application
2. role ownership
3. lease ownership
4. stable replica identity and addressing
5. compressed heartbeat evidence
6. on-demand promotion query for fresh evidence

### Move To Loop 2

These belong to data control:

1. committed/durable/checkpoint boundaries
2. barrier result meaning
3. replica progress
4. keepup/catchup/rebuild progression
5. retention-floor and catch-up targeting

## Existing V2 Seeds To Reuse

The current V2 code already has the right seeds for the primary-led loop:

1. `sw-block/engine/replication/state.go`
2. `sw-block/engine/replication/event.go`
3. `sw-block/engine/replication/command.go`
4. `sw-block/engine/replication/sender.go`
5. `sw-block/engine/replication/session.go`
6. `sw-block/engine/replication/registry.go`

The current MVP already has the right seeds for the identity loop:

1. `sw-block/runtime/masterv2/master.go`
2. `sw-block/runtime/volumev2/control_session.go`
3. `sw-block/runtime/volumev2/orchestrator.go`

## Immediate Next Step

Before deeper implementation, the codebase should next define:

1. the minimal `Loop 1` contract types in code, split into heartbeat, promotion
   query, and assignment
2. the minimal `Loop 2` progress and reconnect contract draft
3. the automata ownership map showing which engine events and commands belong
   to identity control versus data control

## Promotion Logic

Promotion should use fresh on-demand evidence, not stale heartbeat cache.

Recommended judgment order:

1. fence the old primary by epoch
2. query all surviving candidates
3. reject any candidate with wrong epoch, wrong role lineage, or not-ready
   receiver state
4. choose the candidate with highest `CommittedLSN`
5. use `WALHeadLSN` only as a tiebreaker for equally committed candidates
6. assign new primary role at a new epoch

This keeps the durability boundary centered on `CommittedLSN`, which is the
last LSN that satisfied the configured durability mode such as `sync_all` or
`sync_quorum`.
