# V2 Reuse vs Replacement Boundary

Date: 2026-04-03
Status: active

## Purpose

This note makes one architectural split explicit for the current chosen path:

1. what we reuse from the existing `blockvol`/`weed` stack as mechanics
2. what must be owned by `V2` as semantic authority
3. what sits in the adapter boundary between them

The goal is to stop `V1` mixed control/data state from silently redefining `V2`
behavior through convenience wiring.

Scope is still bounded to:

1. `RF=2`
2. `sync_all`
3. current master / volume-server heartbeat path
4. `blockvol` as the execution backend

## Boundary Rule

`V1` reuse is allowed for execution mechanics.

`V2` replacement is required for semantic authority.

If a change decides protocol meaning, failover meaning, durability meaning, or
external publication meaning, it belongs to a `V2`-owned layer even if the
underlying I/O still runs through reused `blockvol` code.

This is the practical interpretation of:

- `v2-protocol-truths.md` `T14`: engine remains recovery authority
- `v2-protocol-truths.md` `T15`: reuse reality, not inherited semantics

## Three Buckets

### 1. Reusable V1 Core

These components remain useful as mechanics:

| Area | Files | What stays reusable |
|------|-------|---------------------|
| Local storage truth | `weed/storage/blockvol/blockvol.go`, `flusher.go`, `rebuild.go`, WAL/extent helpers | WAL append, flush, checkpoint, dirty-map, extent install |
| Replica transport | `weed/storage/blockvol/replica_apply.go`, `wal_shipper.go`, `shipper_group.go`, `dist_group_commit.go`, `repl_proto.go` | TCP receiver/shipper mechanics, barrier transport, replay/apply |
| Frontend serving | `weed/storage/blockvol/iscsi/`, `weed/storage/blockvol/nvme/` | block-device serving once a local volume is authoritative |
| Local role guardrails | `weed/storage/blockvol/promotion.go`, `role.go` | drain, lease revoke, local role gate enforcement |

Rule:

- these layers execute I/O and transport
- they do not decide whether a replica is eligible, authoritative, published, or healthy in the `V2` sense

### 2. Adapter Boundary

These components translate `V2` truth into concrete runtime wiring:

| Area | Files | Responsibility |
|------|-------|----------------|
| Assignment ingest | `weed/server/volume_server_block.go` | authoritative assignment lifecycle for role apply, receiver/shipper wiring, readiness closure |
| Heartbeat/runtime loop | `weed/server/block_heartbeat_loop.go` | collect/report status and process assignments through the same lifecycle |
| Local store helper | `weed/storage/store_blockvol.go` | local volume open/close/iteration; no longer the authoritative assignment lifecycle |
| Bridge | `weed/storage/blockvol/v2bridge/control.go` | convert service/control truth into engine intents |

Rule:

- the adapter boundary may reuse `blockvol` primitives
- it must name and own lifecycle closure states explicitly
- it must not let store-only role application masquerade as ready publication

### 3. V2-Owned Replacement

These areas define truth and therefore must remain `V2`-owned:

| Area | Files | Responsibility |
|------|-------|----------------|
| Control and identity truth | `sw-block/engine/replication/`, `weed/storage/blockvol/v2bridge/control.go` | assignment truth, stable identity, session truth |
| Recovery ownership | `weed/server/block_recovery.go` | live runtime owner for catch-up/rebuild tasks |
| Publication and health closure | `weed/server/master_block_registry.go`, `weed/server/master_block_failover.go` | what the system reports as ready, degraded, publishable |
| External product surfaces | `weed/server/master_grpc_server_block.go`, `weed/server/master_server_handlers_block.go`, debug/diagnostic surfaces | operator-visible truth, not convenience guesses |

Rule:

- if the system exposes a condition to master, tester, CSI, or operator tooling, that condition must come from `V2`-named state

## Assignment-To-Readiness Lifecycle

The authoritative lifecycle for the current chosen path is:

```text
assignment delivered
-> local role applied
-> replica receiver or primary shipper configured
-> readiness closed
-> heartbeat publication
-> master registry health/publication
```

More concretely:

1. master intent is delivered
2. `BlockService.ApplyAssignments()` applies local role truth
3. the same path wires receiver/shipper runtime
4. the same path records named readiness state
5. heartbeat publishes only what is actually publish-healthy
6. master registry derives lookup/health from explicit readiness, not from allocation alone

## Named Readiness States

For the current implementation slice, the service boundary now names:

1. `roleApplied`
2. `receiverReady`
3. `shipperConfigured`
4. `shipperConnected`
5. `replicaEligible`
6. `publishHealthy`

Ownership:

- owned by `BlockService` / adapter layer
- observed by debug surfaces and heartbeat/publication logic
- not delegated to `blockvol` as implicit mixed state

## Current File Map

### Reuse

- `weed/storage/blockvol/blockvol.go`
- `weed/storage/blockvol/flusher.go`
- `weed/storage/blockvol/replica_apply.go`
- `weed/storage/blockvol/wal_shipper.go`
- `weed/storage/blockvol/shipper_group.go`
- `weed/storage/blockvol/dist_group_commit.go`
- `weed/storage/blockvol/iscsi/`
- `weed/storage/blockvol/nvme/`

### Adapter boundary

- `weed/server/volume_server_block.go`
- `weed/server/block_heartbeat_loop.go`
- `weed/storage/store_blockvol.go`
- `weed/server/volume_server_block_debug.go`

### V2-owned replacement / truth

- `weed/storage/blockvol/v2bridge/control.go`
- `sw-block/engine/replication/`
- `weed/server/block_recovery.go`
- `weed/server/master_block_registry.go`
- `weed/server/master_block_failover.go`
- `weed/server/master_grpc_server_block.go`
- `weed/server/master_server_handlers_block.go`

## Immediate Engineering Rule

When a new bug appears, classify it first:

1. `v1 reusable core`: local storage or transport mechanics
2. `adapter boundary`: assignment/readiness/publication closure bug
3. `v2 replacement`: semantic authority, identity, ownership, eligibility, rebuild, or operator-visible truth

Do not patch semantic authority directly into `blockvol` unless the same change is
also reflected as an explicit `V2` state/rule at the service or registry layer.

## Why This Matters For CP13-8

`CP13-8` found the exact class of bug this split is meant to expose:

- allocation/control truth said the replica existed
- but runtime publication/read visibility was not yet closed

That is not a reason to throw away `blockvol`.
It is a reason to stop treating mixed `V1` runtime state as if it were already
closed `V2` publication truth.
