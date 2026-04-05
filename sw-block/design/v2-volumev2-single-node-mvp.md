# V2 VolumeV2 Single-Node MVP

Date: 2026-04-05
Status: active

## Purpose

This note defines the target shape for a single-node `volumev2` MVP that can
ship as a normal block service before HA/failover exists.

The core idea is:

1. `masterv2` is fully new control ownership
2. `volumev2` is a new shell and brain host
3. `blockvol` and related backend mechanics remain reusable muscles

## Target Layering

`volumev2` should be strengthened around four layers.

### 1. Engine

Owner:

- `sw-block/engine/replication/`

Responsibility:

1. state
2. event ingestion
3. command emission
4. outward projection

Rule:

- semantic truth lives here
- no backend I/O or network ownership

### 2. Engine Interface

Owner:

- command/event vocabulary between control/runtime and backend execution

Responsibility:

1. assignment -> event translation
2. observation -> event translation
3. command -> execution dispatch contract

Rule:

- runtime shell may not mutate engine truth directly

### 3. Control Plane

Owner:

- `masterv2 <-> volumev2` coordination

Responsibility:

1. node identity
2. registration and heartbeat
3. assignment receipt
4. state reporting
5. future recovery-control vocabulary (`keepup`, `catchup`, `rebuild`)

Rule:

- control plane carries protocol messages
- it does not own local data execution

### 4. Data Plane

Owner:

- local storage and serving mechanics

Responsibility:

1. WAL/extent management
2. read/write/flush
3. background workers
4. receiver/shipper mechanics
5. NVMe/iSCSI/frontend serving

Rule:

- data plane knows how to execute
- it does not define publication or role semantics

## Single-Node MVP Contract

The first ship-capable `volumev2` slice should include:

1. `masterv2` declaration of one RF1 primary volume
2. `volumev2` control session to fetch assignments
3. local create/open through reused `blockvol`
4. local primary assignment application through the V2 engine
5. local read/write plus restart durability
6. debug/status snapshot
7. one small executable entrypoint for smoke usage

The first slice explicitly excludes:

1. failover
2. RF2 replication
3. catch-up/rebuild ownership
4. CSI

## Why This Is Enough

This is enough to prove:

1. the `masterv2 + volumev2` head is viable
2. `volumev2` can host V2 semantics while reusing V1 muscles
3. a useful non-HA block service can exist before HA complexity is added

## Module Shape

Recommended package split:

1. `sw-block/runtime/masterv2/`
2. `sw-block/runtime/volumev2/`
3. `sw-block/runtime/purev2/`
4. `sw-block/engine/replication/`
5. `sw-block/bridge/blockvol/`

Within `volumev2`, strengthen toward:

1. `control_session.go`
2. `orchestrator.go`
3. `node.go`
4. later: `heartbeat.go`, `frontend.go`, `workers.go`

## Stage Gate

`volumev2` may be treated as a single-node MVP only when:

1. assignment sync is repeatable and idempotent
2. local IO is data-verified
3. restart/open path is proven
4. status/debug state is explicit
5. no `weed/server` lifecycle owner is required

## Related References

- `v2-pure-runtime-rf1-bootstrap.md`
- `v2-proof-and-retest-pyramid.md`
- `v2-capability-map.md`
