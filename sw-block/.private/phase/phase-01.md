# Phase 01

Date: 2026-03-26
Status: completed
Purpose: drive V2 simulator development by closing the scenario backlog in `sw-block/design/v2_scenarios.md`

## Goal

Make the V2 simulator cover the important protocol scenarios as explicitly as possible.

This phase is about:
- simulator fidelity
- scenario coverage
- invariant quality

This phase is not about:
- product integration
- SPDK
- raw allocator
- production transport

## Source Of Truth

Design/source-of-truth:
- `sw-block/design/v2_scenarios.md`

Prototype code:
- `sw-block/prototype/fsmv2/`
- `sw-block/prototype/volumefsm/`
- `sw-block/prototype/distsim/`

## Assigned Tasks For `sw`

### P0

1. `S19` chain of custody across multiple promotions
- add fixed test(s)
- verify committed data from `A -> B -> C`
- update coverage matrix

2. `S20` live partition with competing writes
- add fixed test(s)
- stale side must not advance committed lineage
- update coverage matrix

### P1

3. `S5` flapping replica stays recoverable
- repeated disconnect/reconnect
- no unnecessary rebuild while recovery remains possible

4. `S6` tail-chasing under load
- primary keeps writing while replica catches up
- explicit outcome:
  - converge and promote
  - or abort to rebuild

5. `S18` primary restart without failover
- same-lineage restart behavior
- no stale session assumptions

6. stronger `S12`
- more than one promotion candidate
- choose valid lineage, not merely highest apparent LSN

### P2

7. protocol-version comparison support
- model:
  - `V1`
  - `V1.5`
  - `V2`
- use the same scenario set to show:
  - V1 breaks
  - V1.5 improves but still strains
  - V2 handles recovery more explicitly

8. richer Smart WAL scenarios
- time-varying `ExtentReferenced` availability
- recoverable then unrecoverable transitions

9. delayed/drop network scenarios beyond simple disconnect

10. multi-node reservation expiry / rebuild timeout cases

## Invariants To Preserve

After every scenario or random run, preserve:

1. committed data is durable per policy
2. uncommitted data is not revived as committed
3. stale epoch traffic does not mutate current lineage
4. recovered/promoted node matches reference state at target `LSN`
5. committed prefix remains contiguous

## Required Updates Per Task

For each completed scenario:

1. add or update test(s)
2. update `sw-block/design/v2_scenarios.md`
   - package
   - test name
   - status
3. note any missing simulator capability

## Current Progress

Already in place before this phase:
- `fsmv2` local FSM prototype
- `volumefsm` orchestrator prototype
- `distsim` distributed simulator
- randomized `distsim` runs
- first event/interleaving simulator work in `distsim/simulator.go`

Open focus:
- `S19` covered in `distsim`
- `S20` partially covered in `distsim`
- `S5` partially covered in `distsim`
- `S6` partially covered in `distsim`
- `S18` partially covered in `distsim`
- stronger `S12` covered in `distsim`
- protocol-version comparison design added in:
  - `sw-block/design/protocol-version-simulation.md`
- remaining focus is now P2 plus stronger versions of partial scenarios

## Phase Status

### P0

- `S19` chain of custody across multiple promotions: done
- `S20` live partition with competing writes: partial

### P1

- `S5` flapping replica stays recoverable: partial
- `S6` tail-chasing under load: partial
- `S18` primary restart without failover: partial
- stronger `S12`: done

### P2

- active next step:
  - protocol-version comparison support
  - stronger versions of current partial scenarios

## Exit Criteria

Phase 01 is done when:

1. `S19` and `S20` are covered
2. `S5`, `S6`, `S18`, and stronger `S12` are at least partially covered
3. coverage matrix in `v2_scenarios.md` is current
4. random simulation still passes after added scenarios

## Completion Note

Phase 01 completed with:
- `S19` covered
- stronger `S12` covered
- `S20`, `S5`, `S6`, `S18` strengthened but correctly left as `partial`

Next execution phase:
- `sw-block/.private/phase/phase-02.md`
