# Phase 02

Date: 2026-03-27
Status: active
Purpose: extend the V2 simulator from final-state safety checking into protocol-state simulation that can reproduce `V1`, `V1.5`, and `V2` behavior on the same scenarios

## Goal

Make the simulator model enough node-local replication state and message-level behavior to:

1. reproduce `V1` / `V1.5` failure modes
2. show why those failures are structural
3. close the current `partial` V2 scenarios with stronger protocol assertions

This phase is about:
- protocol-version comparison
- per-node replication state
- message-level fencing / accept / reject behavior
- explicit catch-up abort / rebuild transitions

This phase is not about:
- product integration
- production transport
- SPDK
- raw allocator

## Source Of Truth

Design/source-of-truth:
- `sw-block/design/v2_scenarios.md`
- `sw-block/design/protocol-version-simulation.md`
- `sw-block/design/v1-v15-v2-simulator-goals.md`

Prototype code:
- `sw-block/prototype/distsim/`

## Assigned Tasks For `sw`

### P0

1. Add per-node replication state to `distsim`
- minimum states:
  - `InSync`
  - `Lagging`
  - `CatchingUp`
  - `NeedsRebuild`
  - `Rebuilding`
- keep state lightweight; do not clone full `fsmv2` into `distsim`

2. Add message-level protocol decisions
- stale-epoch write / ship / barrier traffic must be explicitly rejected
- record whether a message was:
  - accepted
  - rejected by epoch
  - rejected by state

3. Add explicit catch-up abort / rebuild entry
- non-convergent catch-up must move to explicit modeled failure:
  - `NeedsRebuild`
  - or equivalent abort outcome

### P1

4. Re-close `S20` at protocol level
- stale-side writes must go through protocol delivery path
- prove stale-side traffic cannot advance committed lineage

5. Re-close `S6` at protocol level
- assert explicit abort/escalation on non-convergence
- not only final-state safety

6. Re-close `S18` at protocol level
- assert committed-prefix behavior around delayed old ack / restart races
- not only final-state oracle checks

### P2

7. Expand protocol-version comparison
- run selected scenarios under:
  - `V1`
  - `V1.5`
  - `V2`
- at minimum:
  - brief disconnect
  - restart with changed address
  - tail-chasing

8. Add V1.5-derived failure scenarios
- replica restart with changed receiver address
- same-address transient outage
- slow control-plane recovery vs fast local reconnect

9. Prepare richer recovery modeling
- time-varying recoverability
- reservation loss during active catch-up
- rebuild timeout / retry in mixed-state cluster

## Invariants To Preserve

After every scenario or random run, preserve:

1. committed data is durable per policy
2. uncommitted data is not revived as committed
3. stale epoch traffic does not mutate current lineage
4. recovered/promoted node matches reference state at target `LSN`
5. committed prefix remains contiguous
6. protocol-state transitions are explicit, not inferred from final data only

## Required Updates Per Task

For each completed task:

1. add or update test(s)
2. update `sw-block/design/v2_scenarios.md`
   - package
   - test name
   - status
   - source if new scenario was derived from V1/V1.5 behavior
3. add a short note to:
   - `sw-block/.private/phase/phase-02-log.md`
4. if a design choice changed, record it in:
   - `sw-block/.private/phase/phase-02-decisions.md`

## Current Progress

Already in place before this phase:
- `distsim` final-state safety invariants
- randomized simulation
- event/interleaving simulator work
- initial `ProtocolVersion` / policy scaffold
- `S19` covered
- stronger `S12` covered

Known partials to close in this phase:
- none in the current named backlog slice

Delivered in this phase so far:
- delivery accept/reject tracking added
- protocol-level rejection assertions added
- explicit `CatchingUp -> NeedsRebuild` state transition tested
- selected protocol-version comparison tests added
- `S20`, `S6`, and `S18` moved from `partial` to `covered`
- Smart WAL transition scenarios added
- `S5` moved from `partial` to `covered`
- endpoint identity / endpoint-version modeling added
- explicit heartbeat -> detect -> assignment-update control-plane flow added for changed-address restart
- explicit recovery-session triggers added for `V1.5` and `V2`
- promotion selection now uses explicit eligibility, including committed-prefix gating
- safe and desperate promotion paths are separated
- full `distsim` suite at latest review: 73 tests passing

Remaining focus for `sw`:
- Phase 02 core scope is now largely delivered
- remaining work should be treated as future-strengthening, not baseline closure
- if more simulator depth is needed next, it should likely start as Phase 03:
  - timeout semantics
  - timer races
  - richer event/interleaving behavior
  - stronger endpoint/control-plane realism beyond the current abstract model

## Immediate Next Tasks For `sw`

1. Add a documented compare artifact for new scenarios
- for each new `V1` / `V1.5` / `V2` comparison:
  - record scenario name
  - what fails in `V1`
  - what improves in `V1.5`
  - what is explicit in `V2`
- keep `sw-block/design/v1-v15-v2-comparison.md` updated

2. Keep the coverage matrix honest
- do not mark a scenario `covered` unless the test asserts protocol behavior directly
- final-state oracle checks alone are not enough

3. Prepare Phase 03 proposal instead of broadening ad hoc
- if more depth is needed, define it cleanly first:
  - timers / timeout events
  - event ordering races
  - richer endpoint lifecycle
  - recovery-session uniqueness across competing triggers

## Exit Criteria

Phase 02 is done when:

1. `S5`, `S6`, `S18`, and `S20` are covered at protocol level
2. `distsim` can reproduce at least one `V1` failure, one `V1.5` failure, and the corresponding `V2` behavior on the same named scenario
3. protocol-level rejection/accept behavior is asserted in tests, not only inferred from final-state oracle checks
4. coverage matrix in `v2_scenarios.md` is current
5. changed-address and reconnect scenarios are modeled through explicit endpoint / control-plane behavior rather than helper-only abstraction
6. promotion selection uses explicit eligibility, including committed-prefix safety
