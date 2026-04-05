# Pure V2 RF1 Bootstrap

Date: 2026-04-05
Status: active

## Purpose

This note turns the bootstrap plan into a concrete runtime boundary that can be
implemented and tested without going through `weed/server`.

The goal of the first executable slice is not feature completeness.

The goal is to establish one small, closed, V2-owned runtime that can accumulate
semantic truth and local execution proof without mixed-runtime distortion.

## Implemented Boundary

### Pure V2 shell

The first pure runtime shell lives in:

- `sw-block/runtime/purev2/`

It owns:

- local process/runtime lifecycle
- local block volume registration
- V2 core engine ownership
- local projection/debug cache
- static RF1 assignment injection

It does not own:

- master heartbeat loops
- assignment queues
- failover control
- recovery task orchestration
- CSI or product-facing control APIs

### Reused execution muscles

The pure shell reuses mechanics instead of re-implementing them:

- `weed/storage/store_blockvol.go`
- `weed/storage/blockvol/blockvol.go`
- `weed/storage/blockvol/v2bridge/command_bindings.go`
- `weed/server/blockcmd/`

The intended rule is:

- `sw-block/runtime/purev2` owns local runtime closure
- `sw-block/engine/replication` owns semantic authority
- reused `weed/` pieces execute concrete backend actions only

### Small executable entrypoint

The first operator-facing entrypoint lives in:

- `sw-block/cmd/purev2rf1/`

Current commands:

1. `bootstrap`
2. `status`

This is intentionally small.

It exists to make the first slice executable and inspectable, not to define the
final product surface.

## RF1 First Slice Contract

The first slice is closed only if all of the following are true.

### Included

1. create one local block volume
2. open an existing local block volume
3. inject one static RF1 primary assignment
4. apply local role through V2 command dispatch
5. perform real local read/write through `blockvol`
6. survive restart and preserve data
7. expose explicit projection/debug state

### Explicitly excluded

1. replica membership truth
2. receiver/shipper wiring as a required path
3. catch-up or rebuild orchestration
4. manual or auto failover
5. CSI

If a new requirement needs any excluded item, it belongs to a later tier and
must not be forced into the RF1 shell.

## Runtime Shape

The implemented ownership split is:

```text
purev2 runtime
-> V2 core engine
-> dispatcher
-> command bindings
-> blockvol store/backend
-> projection/debug snapshot
```

The critical closure path is:

```text
static assignment
-> core ApplyEvent(AssignmentDelivered)
-> dispatcher executes apply_role
-> runtime feeds back RoleApplied
-> projection is cached explicitly
-> local debug snapshot becomes inspectable
```

## Current Behavioral Meaning

For the current engine semantics, an RF1 primary with zero replicas remains:

1. locally writable as a block volume
2. explicitly visible in debug/projection state
3. projected as `allocated_only`, not `publish_healthy`

That is acceptable for this first bootstrap slice because:

1. the purpose is shell closure, not final RF1 publication semantics
2. publication meaning remains explicit instead of being guessed from local role
3. RF1 publication policy can evolve later without changing the shell boundary

## Immediate Engineering Rules

While the pure runtime remains in the RF1 stage:

1. new shell work goes into `sw-block/runtime/purev2/`
2. pure runtime must not import `weed/server/volume_server_block.go`
3. new proof should prefer unit/component tests in the pure shell
4. mixed `weed` scenario results remain oracle coverage, not semantic authority

## Related References

- `v2-capability-map.md`
- `v2-reuse-replacement-boundary.md`
- `v2-legacy-runtime-exit-criteria.md`
- `v2-protocol-truths.md`
