# WAL V1 To V2 Mapping

Date: 2026-03-26
Status: working note
Purpose: map the current WAL V1 scattered state across `sw-block` into the proposed WAL V2 FSM vocabulary

## Why This Note Exists

Current WAL V1 correctness logic is spread across:

- `wal_shipper.go`
- `replica_apply.go`
- `dist_group_commit.go`
- `blockvol.go`
- `promotion.go`
- `rebuild.go`
- heartbeat/master reporting

This note does not propose immediate code changes.

It exists to answer two questions:

1. what state already exists in WAL V1 today?
2. how does that state map into the cleaner WAL V2 FSM model?

## Current V1 State Owners

### 1. Shipper state

Primary-side per-replica transport and recovery state lives mainly in:
- `weed/storage/blockvol/wal_shipper.go`

Current V1 shipper states:
- `ReplicaDisconnected`
- `ReplicaConnecting`
- `ReplicaCatchingUp`
- `ReplicaInSync`
- `ReplicaDegraded`
- `ReplicaNeedsRebuild`

Other shipper-owned flags/anchors:
- `replicaFlushedLSN`
- `hasFlushedProgress`
- `catchupFailures`
- `lastContactTime`

### 2. Replica receiver progress

Replica-side receive/apply progress lives mainly in:
- `weed/storage/blockvol/replica_apply.go`

Current V1 replica progress:
- `receivedLSN`
- `flushedLSN`
- duplicate/gap handling in `applyEntry()`

### 3. Volume-level durability policy

Volume-level sync semantics live mainly in:
- `weed/storage/blockvol/dist_group_commit.go`

Current V1 policy uses:
- local WAL sync result
- per-shipper barrier results
- `DurabilityBestEffort`
- `DurabilitySyncAll`
- `DurabilitySyncQuorum`

### 4. Volume-level retention/checkpoint state

Primary-side local checkpoint and WAL retention state lives mainly in:
- `weed/storage/blockvol/blockvol.go`
- `weed/storage/blockvol/flusher.go`

Current V1 anchors:
- `nextLSN`
- `CheckpointLSN()`
- WAL retained range
- retention-floor callbacks from `ShipperGroup`

### 5. Role/assignment state

Master-driven volume role state lives mainly in:
- `weed/storage/blockvol/promotion.go`
- `weed/storage/blockvol/blockvol.go`
- `weed/server/volume_server_block.go`

Current V1 roles:
- `RolePrimary`
- `RoleReplica`
- `RoleStale`
- `RoleRebuilding`
- `RoleDraining`

### 6. Rebuild state

Existing V1 rebuild transport/process lives mainly in:
- `weed/storage/blockvol/rebuild.go`

Current V1 rebuild phases:
- WAL catch-up attempt
- full extent copy
- trailing WAL catch-up
- rejoin via assignment + fresh shipper bootstrap

### 7. Heartbeat/master-visible replication state

Master-visible state lives mainly in:
- `weed/storage/blockvol/block_heartbeat.go`
- `weed/storage/blockvol/blockvol.go`
- server-side registry/master handling

Current V1 visible fields include:
- `ReplicaDegraded`
- `ReplicaShipperStates []ReplicaShipperStatus`
- role/epoch/checkpoint/head state

## V1 To V2 Mapping

### Shipper state mapping

| WAL V1 shipper state | Proposed WAL V2 FSM state | Notes |
| --- | --- | --- |
| `ReplicaDisconnected` | `Bootstrapping` or `Lagging` | Fresh shipper with no durable progress maps to `Bootstrapping`; previously-synced disconnected replica maps to `Lagging`. |
| `ReplicaConnecting` | transitional part of `Lagging -> CatchingUp` | V2 should model this as an event/session phase, not a durable steady state. |
| `ReplicaCatchingUp` | `CatchingUp` | Direct mapping for short-gap replay. |
| `ReplicaInSync` | `InSync` | Direct mapping. |
| `ReplicaDegraded` | `Lagging` | V1 transport failure state becomes the cleaner V2 recovery-needed state. |
| `ReplicaNeedsRebuild` | `NeedsRebuild` | Direct mapping. |

Main V1 cleanup opportunity:
- V1 mixes transport/session detail (`Connecting`) with recovery lifecycle state.
- V2 should keep the long-lived FSM smaller and push connection mechanics into sender-loop/session logic.

### Replica receiver progress mapping

| WAL V1 field | WAL V2 concept | Notes |
| --- | --- | --- |
| `receivedLSN` | `receivedLSN` | Keep as transport/apply progress only. |
| `flushedLSN` | `replicaFlushedLSN` | Keep as authoritative durability anchor. |
| duplicate/gap rules | replay validity rules | These become part of the V2 replay contract, not ad hoc receiver behavior. |

Main V1 cleanup opportunity:
- V1 receiver progress is already conceptually sound.
- V2 should keep it but drive it from explicit FSM transitions and replay reservations.

### Volume durability policy mapping

| WAL V1 behavior | WAL V2 concept | Notes |
| --- | --- | --- |
| `BarrierAll` against current shippers | promotion and sync gate | V2 should keep barrier-based durability truth. |
| `sync_all` requires all barriers | `InSync` eligibility gate | Same rule, but V2 eligibility should come from FSM state rather than scattered checks. |
| `best_effort` ignores barrier failures | background recovery mode | Same high-level policy. |
| `sync_quorum` counts successful barriers | quorum over `InSync` replicas | Same direction, but should be derived from explicit FSM state. |

Main V1 cleanup opportunity:
- durability mode logic should depend on `IsSyncEligible()`-style state, not raw shipper state enums spread across code.

### Retention/checkpoint mapping

| WAL V1 concept | WAL V2 concept | Notes |
| --- | --- | --- |
| `CheckpointLSN()` | checkpoint/base anchor | Keep, but V2 also adds explicit `cpLSN` snapshot semantics. |
| retention floor from recoverable replicas | recoverability budget | Keep the idea, but V2 turns this into explicit reservation management. |
| timeout-based `NeedsRebuild` | janitor-driven `Lagging -> NeedsRebuild` | Keep as background control logic, not hot-path mutation. |

Main V1 cleanup opportunity:
- V1 retains data because replicas might need it.
- V2 should reserve specific recovery windows, not rely only on ambient retention conditions.

### Role/assignment mapping

| WAL V1 role state | WAL V2 meaning | Notes |
| --- | --- | --- |
| `RolePrimary` | primary ownership / epoch authority | Not a replica FSM state; remains volume/control-plane state. |
| `RoleReplica` | replica service role | Orthogonal to replication FSM state. A replica volume may be `RoleReplica` while its sender-facing state is `Bootstrapping`, `Lagging`, or `InSync`. |
| `RoleStale` | pre-rebuild/non-serving | Closest to `NeedsRebuild` preparation on the volume role side. |
| `RoleRebuilding` | rebuild session role | Maps to volume-wide orchestration around V2 `Rebuilding`. |
| `RoleDraining` | assignment/failover coordination | Outside replica FSM; remains a volume transition role. |

Main V1 cleanup opportunity:
- role state and replication FSM state are different dimensions.
- V1 sometimes implicitly blends them.
- V2 should keep them separate:
  - control-plane role FSM
  - per-replica replication FSM

### Rebuild flow mapping

| WAL V1 rebuild phase | WAL V2 FSM phase | Notes |
| --- | --- | --- |
| WAL catch-up pre-pass | `Lagging -> CatchingUp` if feasible | Same idea, but V2 requires recoverability proof and reservation. |
| full extent copy | `NeedsRebuild -> Rebuilding` | Same high-level phase. |
| trailing WAL catch-up | `CatchUpAfterRebuild` | Direct conceptual mapping. |
| fresh shipper bootstrap after reassignment | `Bootstrapping` then promotion | V1 does this through assignment refresh; V2 may eventually do it with cleaner local transitions. |

Main V1 cleanup opportunity:
- V1 rebuild success is currently rejoined indirectly through control-plane reassignment.
- V2 should eventually make rebuild completion and promotion explicit FSM transitions.

### Heartbeat/master state mapping

| WAL V1 visible state | WAL V2 meaning | Notes |
| --- | --- | --- |
| `ReplicaShipperStatus{DataAddr, State, FlushedLSN}` | control-plane view of per-replica FSM | Good starting shape. |
| `ReplicaDegraded` | derived summary only | Too coarse for V2 decision-making; keep only as convenience/compat field. |
| role/epoch/head/checkpoint | role FSM + replication anchors | Continue reporting; V2 may need richer recovery reservation visibility later. |

Main V1 cleanup opportunity:
- master-facing replication state should be per replica, not summarized as one degraded bit.

## Current V1 Event Sources vs V2 Events

### V1 event source: `Barrier()` outcome

Current effects:
- mark `InSync`
- update `replicaFlushedLSN`
- mark degraded on error

V2 event mapping:
- `BarrierSuccess`
- `BarrierFailure`
- `PromotionHealthy`

### V1 event source: reconnect handshake

Current effects:
- `Connecting`
- choose `InSync`, `CatchingUp`, or `NeedsRebuild`

V2 event mapping:
- `ReconnectObserved`
- `RecoveryFeasible`
- `RecoveryReservationGranted`
- `ReconnectNeedsRebuild`

### V1 event source: retention budget evaluation

Current effects:
- stale replica becomes `NeedsRebuild`

V2 event mapping:
- `RecoverabilityExpired`
- `BackgroundJanitorNeedsRebuild`

### V1 event source: rebuild assignment and `StartRebuild`

Current effects:
- role becomes `RoleRebuilding`
- run baseline + trailing catch-up
- rejoin later via reassignment

V2 event mapping:
- `StartRebuild`
- `RebuildBaseApplied`
- `RebuildReservationLost`
- `RebuildCompleteReadyForPromotion`

## Main Gaps Between V1 And V2

### 1. V1 has shipper state, but not a pure FSM

Current V1 state is embedded in:
- transport logic
- barrier logic
- retention logic
- rebuild orchestration

V2 goal:
- one pure FSM that owns state and anchors
- transport/session code only executes actions

### 2. V1 does not model reservation explicitly

Current V1 asks, roughly:
- is WAL still retained?

V2 must ask:
- is `(startLSN, endLSN]` fully recoverable?
- can the primary reserve that window until recovery completes?

### 3. V1 has no explicit promotion debounce state

Current V1 goes effectively:
- caught up -> `InSync`

V2 adds:
- `PromotionHold`

### 4. V1 rebuild completion is control-plane indirect

Current V1:
- old `NeedsRebuild` shipper stays stuck
- master reassigns
- fresh shipper bootstraps

V2 likely wants:
- cleaner local FSM transitions, even if control plane still participates

### 5. V1 does not yet encode recovery classes

Current V1 is mostly WAL-centric.

V2 should support:
- `WALInline`
- `ExtentReferenced`

without leaking storage details into replica state.

## What Should Stay From V1

These V1 ideas are solid and should be preserved:

1. `replicaFlushedLSN` as sync truth
2. barrier-driven durability confirmation
3. explicit `NeedsRebuild`
4. per-replica status reporting to master
5. retention budgets eventually forcing rebuild
6. rebuild as a separate path from normal catch-up

## What Should Move In V2

These are the main redesign items:

1. move scattered shipper/recovery state into one pure FSM
2. separate transport/session phases from durable FSM state
3. add `Bootstrapping` and `PromotionHold`
4. add recoverability proof and reservation as first-class concepts
5. make replay/rebuild admission depend on reservation, not just present-time checks
6. cleanly separate:
   - control-plane role FSM
   - per-replica replication FSM

## Bottom Line

WAL V1 already contains most of the important primitives:

- durable progress
- barrier truth
- catch-up
- rebuild detection
- master-visible per-replica state

What V2 changes is not the existence of these ideas.

It changes their organization:
- from scattered transport/rebuild logic
- to one explicit, testable FSM with recovery reservations and cleaner state boundaries
