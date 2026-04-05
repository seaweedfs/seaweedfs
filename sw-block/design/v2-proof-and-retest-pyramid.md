# V2 Proof And Retest Pyramid

Date: 2026-04-05
Status: active

## Purpose

This note defines how the pure V2 runtime should accumulate proof so that most
closure stays in fast, reusable tests instead of expensive mixed scenarios.

It also defines where reuse is allowed to keep narrow regression coverage and
where truth-owner changes require full V2 retesting.

## Proof Pyramid

### Layer 1: core semantic tests

Owner:

- `sw-block/engine/replication/`

This layer proves:

1. assignment semantics
2. role-application semantics
3. readiness/publication semantics
4. stale/replay/idempotence behavior
5. recovery and fencing meaning

Rule:

- if a behavior changes V2 protocol truth, this layer is mandatory

### Layer 2: component and seam tests

Owner:

- `sw-block/runtime/purev2/`
- narrow bridge/dispatcher seams reused from `weed/`

This layer proves:

1. static assignment ingestion
2. dispatcher to backend binding
3. local role application feedback into the core
4. local restart/open/replay behavior
5. debug/projection cache consistency

Rule:

- this is the default daily development surface for the new runtime

### Layer 3: minimal integrated runtime tests

Owner:

- pure runtime binary/package smoke

This layer proves only:

1. create -> assign -> write -> read
2. restart -> reopen -> read
3. status/debug snapshot visibility

Rule:

- keep this pack tiny and fast
- do not pull failover, multi-node, or CSI into this layer

### Layer 4: compatibility and oracle checks

Owner:

- current `weed` integrated path

This layer remains useful for:

1. regression oracle coverage
2. parity comparison
3. late acceptance confidence

Rule:

- it is not the daily semantic development surface for pure V2

## Reuse Versus Full Retest

### Reuse with narrow regression

The following areas remain execution muscles and should keep focused coverage:

1. WAL append and flush mechanics
2. checkpoint and dirty-map mechanics
3. extent install mechanics
4. local read/write I/O
5. transport/frontend plumbing when semantic meaning is unchanged

Typical proof:

1. component tests
2. mechanical regression tests
3. one narrow smoke if the seam changed

### Full V2 retest required

The following areas change truth ownership and therefore require explicit V2 proof:

1. assignment meaning
2. publication meaning
3. readiness closure
4. fencing and lineage meaning
5. recovery classification
6. operator-visible health and degraded semantics

Typical proof:

1. core semantic tests first
2. pure-runtime component tests second
3. only then one small integrated confirmation

## Stage Gates

### Stage A: RF1 pure shell

Must be green before any RF2 work starts:

1. create/open works
2. static primary assignment works
3. local write/read works
4. restart durability works
5. debug/projection snapshot works

### Stage B: RF2 replication base

May start only after Stage A is closed.

This stage adds:

1. replica membership truth
2. receiver/shipper wiring
3. barrier semantics
4. degraded versus publish closure
5. catch-up and rebuild base semantics

### Stage C: failover and rejoin

May start only after RF2 replication base is closed.

This stage adds:

1. manual promote
2. auto failover
3. rejoin
4. recovery ownership handoff

### Stage D: product surfaces

May start only after failover/rejoin closure exists.

This stage adds:

1. CSI
2. operator APIs
3. external readiness and health surfaces
4. acceptance and soak packs

## Daily Working Rule

When a new bug appears, classify it first:

1. core semantic bug
2. pure-runtime seam bug
3. execution-muscle bug
4. compatibility-only oracle bug

Then choose the cheapest proof tier that still matches the truth owner.

If the answer is "mixed scenario first", the classification is probably still
too vague.

## Related References

- `v2-pure-runtime-rf1-bootstrap.md`
- `v2-capability-map.md`
- `v2-reuse-replacement-boundary.md`
- `v2-legacy-runtime-exit-criteria.md`
