# Phase 19

Date: 2026-04-05
Status: complete
Purpose: turn the delivered `Phase 18` RF2 runtime-bearing kernel slice into one
real working RF2 block path without collapsing the ownership split

## Why This Phase Exists

`Phase 18` closed the kernel/runtime proof stack:

1. failover-time evidence crosses an explicit seam
2. active Loop 2 observation exists
3. bounded continuity through handoff exists
4. one RF2-facing outward surface exists
5. one bounded productionization envelope now exists with explicit non-readiness

That is enough to stop `Phase 18`.

It is not enough to claim a working RF2 block product.

The next work is now narrower and more mechanical:

1. make the transport path real
2. make Loop 2 continuously active
3. trigger failover from runtime-owned signals
4. attach real frontend and rebuild/catch-up lifecycle wiring
5. prove one end-to-end serving path
6. bind CSI and operator surfaces on top of runtime-owned truth

## Entry Checkpoint

`Phase 19` starts from the completed `Phase 18` boundary:

1. `masterv2` is explicit identity/promotion authority
2. `volumev2` owns failover, takeover, Loop 2 observation, continuity, and RF2
   surface projection
3. failover evidence already crosses an explicit adapter seam
4. the productionization envelope already says:
   - `block expansion`
   - `not pilot-ready`

Entry interpretation:

1. the authority split is already stable enough
2. the next main risk is live integration, not protocol rediscovery
3. later work should widen from this checkpoint rather than redefine it

## Phase Goal

Produce one bounded post-entry sequence where:

1. runtime participants communicate through a real transport path
2. Loop 2 is continuously meaningful
3. failover can trigger automatically from bounded runtime-owned signals
4. one real frontend path works on the new runtime
5. one degraded replica can return to healthy through V2-owned orchestration
6. one end-to-end RF2 handoff path serves real client I/O
7. CSI and operator surfaces attach without becoming truth owners

## Scope

### In scope

1. real transport-backed failover-time evidence path
2. continuous Loop 2 service on the runtime path
3. bounded auto-failover trigger
4. runtime-managed frontend binding
5. bounded rebuild/catch-up orchestration on the runtime path
6. one end-to-end RF2 handoff proof
7. CSI rebinding and operator surface attachment

### Out of scope

1. reopening the kernel ownership split
2. broad `RF>2` product closure
3. broad transport/frontend matrix approval
4. broad launch approval
5. silent fallback to legacy mixed ownership as the truth source

## Working Rules

`Phase 19` should continue the `Phase 18` discipline:

1. work by major milestones, not ad hoc rewiring
2. each major milestone should normally close in `2-3` implementation steps
3. each milestone must keep a healthy proof and a fail-closed counterproof
4. frontend, CSI, and operator surfaces must remain projections/integrations over
   runtime truth, not new truth owners

After each major milestone:

1. update this phase file status
2. update `phase-19-log.md`
3. update `phase-19-decisions.md` if ordering or boundaries change
4. update review-base docs if the claim boundary changes

## Phase 19 Major Milestones

### `M6`: Live Transport-Backed RF2 Runtime Queries

Goal:

1. replace the current in-memory failover-time evidence path with one real
   transport-backed runtime path

Planned steps:

1. seam step:
   add one real transport implementation behind the existing evidence adapter
   seam
2. runtime step:
   make runtime registration and resolution use that live transport path
3. closure step:
   prove healthy and gated 2-node failover through the live transport path

Exit criteria:

1. promotion evidence and replica summaries cross a live transport path
2. failover session/manager observability still survives
3. takeover authority does not move out of the selected primary

Current status:

1. delivered
2. one live loopback HTTP transport now exists behind the evidence seam
3. healthy and gated 2-node failover tests now pass through that live transport

### `M7`: Continuous Loop 2 Service And Auto Failover Trigger

Goal:

1. turn Loop 2 into a continuously active runtime service and allow bounded
   automatic failover on top of it

Planned steps:

1. seam step:
   add a background Loop 2 service over the current observation slice
2. runtime step:
   attach a bounded auto-failover trigger to explicit liveness/runtime signals
3. closure step:
   prove healthy trigger behavior and fail-closed suppression

Exit criteria:

1. Loop 2 no longer depends on ad hoc `ObserveOnce()` calls
2. auto failover is downstream of honest observation and liveness
3. RF2 surfaces refresh from continuous runtime ownership

Current status:

1. delivered
2. one bounded background Loop 2 service now exists
3. one bounded auto-failover service now triggers on explicit primary evidence
   loss and suppresses ambiguous runtime states

### `M8`: Frontend And Rebuild/Catch-Up Wiring

Goal:

1. bind a real serving path and a bounded recovery lifecycle to the new runtime

Planned steps:

1. seam step:
   attach one real frontend to the runtime-managed primary path
2. runtime step:
   add bounded rebuild/catch-up orchestration around existing execution pieces
3. closure step:
   prove return-to-healthy from one degraded state

Exit criteria:

1. one real frontend serves from the V2 runtime path
2. one degraded replica can return to healthy
3. rebuild/catch-up remain V2-orchestrated

Current status:

1. delivered
2. one runtime-managed iSCSI export path now exists
3. one bounded replica repair wrapper now returns a lagging replica to healthy

### `M9`: End-To-End Working RF2 Block Path Proof

Goal:

1. prove the first real user story:
   create volume -> serve I/O -> lose primary -> continue service on the new
   primary

Planned steps:

1. seam step:
   build a 2-node end-to-end harness on the new runtime path
2. runtime step:
   execute the real handoff path with live serving and bounded client I/O
3. closure step:
   add a gated counterproof that stops safely

Exit criteria:

1. one real end-to-end RF2 handoff path exists
2. the path uses real transport and real serving, not only in-process
   composition

Current status:

1. delivered
2. one real client path now proves:
   - write through runtime-managed frontend
   - lose primary
   - auto fail over
   - reconnect to new primary
   - continue I/O
3. one gated handoff counterproof now stops fail-closed

### `M10`: CSI Rebinding And Operator Surface

Goal:

1. attach CSI and operator-facing surfaces on top of the proven runtime path

Planned steps:

1. seam step:
   rebind CSI lifecycle integration to the new runtime-bearing path
2. runtime step:
   expose bounded operator-facing surfaces from runtime-owned truth
3. closure step:
   add integration checks for CSI and operator visibility

Exit criteria:

1. CSI and operator surfaces sit on top of V2 runtime truth
2. no new product/API surface becomes a hidden truth owner

Current status:

1. delivered
2. one bounded HTTP operator surface now exposes runtime-owned views
3. one bounded CSI runtime backend adapter now creates/looks up/publishes
   volumes from runtime-owned export truth

## Initial Order

The required execution order is:

1. `M6`
2. `M7`
3. `M8`
4. `M9`
5. `M10`

This order should not be broadly reordered without a written decision in
`phase-19-decisions.md`.

## Current Focus

`Phase 19` close-out:

1. `M6-M10` are now delivered
2. one bounded working RF2 block path now exists
3. later work should widen from this point only through explicit new closure and
   multi-process/pilot-ready evidence, not by rereading `Phase 19` as broad
   launch proof

## Review Base

Use these files together when reviewing `Phase 19` work:

1. `sw-block/design/v2-two-loop-protocol.md`
2. `sw-block/design/v2-automata-ownership-map.md`
3. `sw-block/design/v2-kernel-closure-review.md`
4. `sw-block/design/v2-protocol-claim-and-evidence.md`
5. `sw-block/design/v2-rf2-runtime-bounded-envelope.md`
6. `sw-block/design/v2-rf2-runtime-bounded-envelope-review.md`
7. `sw-block/.private/phase/phase-19.md`

## Non-Goals For This Phase Document

This file should not become:

1. an unbounded product roadmap
2. a day-by-day log
3. a substitute for the claim/evidence ledger
4. a substitute for detailed runtime design docs
