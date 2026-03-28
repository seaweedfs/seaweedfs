# Phase 01 Log

Date: 2026-03-26
Status: active

## Log Protocol

Use dated entries like:

## 2026-03-26
- work completed
- tests run
- failures found
- seeds/traces worth keeping
- follow-up items

## Initial State

- Phase 01 created from the earlier `phase-01-v2-scenarios.md` working note
- scenario source of truth remains:
  - `sw-block/design/v2_scenarios.md`
- current active asks for `sw`:
  - `S19`
  - `S20`

## 2026-03-26

- created Phase 01 file set:
  - `phase-01.md`
  - `phase-01-log.md`
  - `phase-01-decisions.md`
- promoted scenario execution checklist into `phase-01.md`
- kept `sw-block/design/v2_scenarios.md` as the shared backlog and coverage matrix
- current simulator milestone:
  - `fsmv2` passing
  - `volumefsm` passing
  - `distsim` passing
  - randomized `distsim` seeds passing
  - event/interleaving simulator work present in `sw-block/prototype/distsim/simulator.go`
- current immediate development priority for `sw`:
  - implement `S19`
  - implement `S20`
- `sw` added Phase 01 P0/P1 scenario tests in `distsim`:
  - `S19`
  - `S20`
  - `S5`
  - `S6`
  - `S18`
  - stronger `S12`
- review result:
  - `S19` looks solid
  - stronger `S12` now looks solid
  - `S20`, `S5`, `S6`, `S18` are better classified as `partial` than fully closed
- updated `v2_scenarios.md` coverage matrix to reflect actual status
- next development focus:
  - P2 scenarios
  - stronger versions of current partial scenarios
- added protocol-version comparison design:
  - `sw-block/design/protocol-version-simulation.md`
- added minimal protocol policy prototype in `distsim`:
  - `ProtocolV1`
  - `ProtocolV15`
  - `ProtocolV2`
  - focused on:
    - catch-up policy
    - tail-chasing outcome policy
    - restart/rejoin policy
