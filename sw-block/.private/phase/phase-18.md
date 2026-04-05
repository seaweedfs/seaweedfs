# Phase 18

Date: 2026-04-05
Status: active
Purpose: drive the new `masterv2 + volumev2 + purev2` kernel from the current
in-process RF2 failover runtime slice toward a bounded productizable RF2 runtime
in disciplined major milestones

## Why This Phase Exists

The current kernel has crossed the first important threshold:

1. `masterv2` now behaves like explicit identity authority
2. `volumev2` now has explicit takeover preparation and activation gating
3. failover now exists as a runtime-owned slice rather than only implicit mixed
   runtime behavior

That is enough to stop the current milestone.

It is not enough to claim a transport-backed RF2 runtime, continuous
replication-runtime ownership, or product-ready RF2 surfaces.

The next work is larger than micro-slicing:

1. the failover seam must cross a real transport/session boundary
2. the primary-led Loop 2 runtime must become continuously active
3. data continuity must be closed through real handoff paths
4. product/runtime surfaces must be attached without breaking authority split
5. productionization evidence must be bounded and explicit

This phase exists to package those larger objects as one ordered program rather
than continuing disconnected local improvements.

## Entry Checkpoint

`Phase 18` starts from the current completed kernel/runtime slice:

1. explicit `masterv2` promotion authorization
2. explicit `volumev2` takeover prepare/gate seams
3. stepwise `FailoverSession` with observable stages and failure snapshots
4. in-process failover driver seam
5. runtime-owned in-process RF2 failover manager entry point

Entry interpretation:

1. this is a real kernel/runtime checkpoint
2. this is not yet transport-backed RF2 closure
3. this is not yet active Loop 2 runtime closure
4. this is not yet RF2 product or production closure

## Phase Goal

Produce one bounded post-entry sequence where:

1. the failover runtime crosses a real transport/session seam without changing
   authority ownership
2. the primary-led Loop 2 runtime becomes continuously meaningful rather than
   appearing only at failover boundaries
3. one replicated continuity statement is supported through real handoff
4. one bounded RF2 product/runtime surface exists on top of the new runtime
5. one bounded productionization envelope is explicit and reviewable

## Scope

### In scope

1. transport/session seam for failover-time evidence and bounded replica
   summaries
2. runtime-owned RF2 failover flow beyond in-process direct calls
3. primary-led active Loop 2 runtime growth
4. replicated continuity closure
5. RF2 runtime/product surface attachment
6. bounded pilot/productionization review package

### Out of scope

1. broad protocol rediscovery
2. silent return to `weed/server` ownership
3. premature `RF>2` product closure
4. broad transport/frontend matrix approval before bounded RF2 runtime closure
5. broad launch claim before explicit productionization evidence

## Working Rules

`Phase 18` should be executed in major milestones, not micro-patches.

Each major milestone should normally complete in `2-3` implementation steps:

1. seam step
2. runtime step
3. closure/review step

After each major milestone:

1. update this phase file status
2. update `phase-18-log.md` with what changed and what was tested
3. update `phase-18-decisions.md` if any boundary or tradeoff changed
4. update review-base docs if the claim boundary changed

## Phase 18 Major Milestones

### `M1`: Transport-Backed RF2 Failover Runtime

Goal:

1. replace the current in-process participant shortcut with an explicit
   transport/session adapter seam for promotion evidence and bounded replica
   summary exchange

Planned steps:

1. seam step:
   define transport/session adapter contracts while keeping current authority
   split intact
2. runtime step:
   make runtime-owned failover use adapter-backed participants instead of direct
   in-process coupling
3. closure step:
   prove healthy and gated failover through the runtime entry point across the
   adapter seam

Exit criteria:

1. runtime failover no longer depends on direct `*Node` method calls as the only
   implementation path
2. stage/error/result observability survives across the transport/session seam
3. no recovery-planner responsibility leaks into `masterv2`

Current status:

1. delivered
2. failover-time evidence now crosses an explicit transport/session adapter seam
3. runtime-owned failover still preserves stage/error/result observability
4. current implementation uses an in-memory request/response transport, not a
   network transport matrix claim

Review/test update:

1. adapter seam introduced:
   - `FailoverEvidenceAdapter`
   - `FailoverTakeoverAdapter`
   - `FailoverTarget`
2. first in-process adapter implementation delivered:
   - `NewInProcessFailoverTarget(...)`
3. first transport-backed evidence implementation delivered:
   - `InMemoryFailoverEvidenceTransport`
   - `NewTransportEvidenceAdapter(...)`
   - `NewHybridInProcessFailoverTarget(...)`
4. failover session/driver/runtime manager now use explicit targets instead of
   direct `*Node` coupling as the primary path
5. healthy and gated failover tests now pass with promotion evidence and replica
   summary traffic crossing the transport/session seam

### `M2`: Active Loop 2 Replication Runtime

Goal:

1. turn primary-led Loop 2 from bounded takeover semantics into a continuously
   active replication/runtime owner

Planned steps:

1. seam step:
   define the minimum active Loop 2 runtime contracts for keepup/catchup/rebuild
   progression
2. runtime step:
   connect the active Loop 2 runtime to primary-side runtime ownership and
   boundary observation
3. closure step:
   prove at least one bounded active progression path beyond failover-only logic

Exit criteria:

1. Loop 2 has runtime-owned meaning outside failover
2. keepup/catchup/rebuild are not merely comments or future placeholders
3. outward mode remains a compressed projection, not the full runtime automaton

Current status:

1. delivered
2. one runtime-owned active Loop 2 session/controller now derives bounded
   `keepup` / `catching_up` / `needs_rebuild` runtime modes from replica
   summaries
3. the runtime manager now owns explicit active Loop 2 observation entry points
   and snapshots
4. current boundary:
   - the active runtime is bounded summary-driven, not full shipper/rebuild task
     choreography

Review/test update:

1. delivered code:
   - `Loop2RuntimeSession`
   - `Loop2RuntimeSnapshot`
   - `Loop2RuntimeMode`
   - runtime-manager `ObserveLoop2(...)` / `LastLoop2Snapshot(...)` /
     `Loop2Snapshot(...)`
2. delivered tests:
   - healthy `keepup`
   - lagging `catching_up`
   - explicit `needs_rebuild`
3. result:
   - Loop 2 now has a runtime-owned active slice outside failover-only logic

### `M3`: Replicated Data Continuity Closure

Goal:

1. prove one bounded replicated continuity statement through real primary handoff

Planned steps:

1. seam step:
   define the exact continuity contract to be claimed
2. runtime step:
   run the handoff path through the new runtime instead of ad hoc proof-only
   slices
3. closure step:
   verify write -> progress -> failover -> continued service/data continuity

Exit criteria:

1. one healthy continuity path is explicit and repeatable
2. one degraded/gated path fails closed through the same runtime
3. the claim is bounded and does not silently widen into generic RF2 product
   proof

Current status:

1. not started

Review/test update:

1. pending

### `M4`: RF2 Product Runtime Surfaces

Goal:

1. attach bounded product/runtime surfaces to the new RF2 runtime without
   breaking the ownership split

Planned steps:

1. seam step:
   choose the first bounded RF2-facing product/runtime surfaces
2. runtime step:
   attach them to the new runtime rather than to legacy mixed ownership
3. closure step:
   prove one bounded product/runtime surface package on the new runtime

Exit criteria:

1. at least one RF2-facing runtime/product surface works on the new runtime
2. surface truth is still derived from the kernel/runtime authority model
3. no frontend/backend code becomes the hidden truth owner

Current status:

1. not started

Review/test update:

1. pending

### `M5`: Productionization / Launch Envelope

Goal:

1. freeze one bounded productionization envelope for the new RF2 runtime path

Planned steps:

1. seam step:
   define the explicit supported envelope and exclusions
2. runtime step:
   collect the bounded pilot/preflight/stop-condition artifacts around the new
   runtime path
3. closure step:
   produce the review package for bounded productionization judgment

Exit criteria:

1. supported envelope, exclusions, and blockers are explicit
2. pilot/preflight/stop-condition artifacts exist for the bounded path
3. the result is reviewable as bounded productionization, not broad launch
   approval

Current status:

1. not started

Review/test update:

1. pending

## Initial Order

The required execution order is:

1. `M1`
2. `M2`
3. `M3`
4. `M4`
5. `M5`

This order may be refined locally, but should not be broadly reordered without a
written decision in `phase-18-decisions.md`.

## Current Focus

The active next work is:

1. `M3` seam step
2. turn the current failover + active Loop 2 runtime slices into one bounded
   replicated continuity statement

## Review Base

Use these files together when reviewing `Phase 18` work:

1. `sw-block/design/v2-two-loop-protocol.md`
2. `sw-block/design/v2-automata-ownership-map.md`
3. `sw-block/design/v2-kernel-closure-review.md`
4. `sw-block/design/v2-protocol-claim-and-evidence.md`
5. `sw-block/.private/phase/phase-18.md`

## Non-Goals For This Phase Document

This file should not become:

1. an unbounded idea dump
2. a day-by-day development log
3. a substitute for the claim/evidence ledger
4. a substitute for detailed kernel boundary documents
