# Phase 07 Decisions

## Decision 1: Phase 07 is real-system integration, not protocol redesign

The V2 protocol shape, engine core, and broader runnable engine stage are already accepted.

Phase 07 should integrate them into a real-system service slice.

## Decision 2: Phase 07 should make the first product-path decision

This phase should not only integrate a service slice.

It should also decide:

1. what the first product path is
2. what remains before pre-production hardening

## Decision 3: Phase 07 must preserve accepted V2 boundaries

Phase 07 should preserve:

1. narrow catch-up semantics
2. rebuild as the formal recovery path
3. trusted-base / replayable-tail proof boundaries
4. stable identity / fenced execution / diagnosable failure handling

## Decision 4: Phase 07 P0 service-slice direction is set

Current direction:

1. first service slice = `RF=2` block volume primary + one replica
2. engine remains in `sw-block/engine/replication/`
3. current bridge work starts in `sw-block/bridge/blockvol/`
4. deferred real blockvol-side bridge target = `weed/storage/blockvol/v2bridge/`
5. stable identity mapping is explicit:
   - `ReplicaID = <volume-name>/<server-id>`
6. `blockvol` executes I/O but does not own recovery policy

## Decision 5: Phase 07 P1 is accepted with explicit scope limits

Accepted `P1` coverage is:

1. real reader mapping from `BlockVol` state
2. real retention hold / release wiring into the flusher retention floor
3. one real WAL catch-up scan path through `v2bridge`
4. direct real-adapter tests under `weed/storage/blockvol/v2bridge/`

This acceptance means:

1. the real bridge path is now integrated and evidenced
2. `P1` is not yet acceptance proof of general post-checkpoint catch-up viability

Not accepted as part of `P1`:

1. snapshot transfer execution
2. full-base transfer execution
3. WAL truncation execution
4. master-side confirmed failover / control-intent integration

## Decision 6: Interim committed-truth limitation remains active

`Phase 07 P1` is accepted with an explicit carry-forward limitation:

1. interim `CommittedLSN = CheckpointLSN` is a service-slice mapping, not final V2 protocol truth
2. post-checkpoint catch-up semantics are therefore narrower than final V2 intent
3. later `Phase 07` work must not overclaim this limitation as solved until commit truth is separated from checkpoint truth

## Decision 7: Phase 07 P2 is accepted with scoped replay claims

Accepted `P2` coverage is:

1. real service-path replay for changed-address restart
2. stale epoch / stale session invalidation through the integrated path
3. unrecoverable-gap / needs-rebuild replay with diagnosable proof
4. explicit replay of the post-checkpoint boundary under the interim model

Not accepted as part of `P2`:

1. general integrated engine-driven post-checkpoint catch-up semantics
2. real control-plane delivery from master heartbeat into the bridge
3. rebuild execution beyond the already-deferred executor stubs

## Decision 8: Phase 07 now moves to product-path choice, not more bridge-shape proof

With `P0`, `P1`, and `P2` accepted, the next step is:

1. choose the first product path from accepted service-slice evidence
2. define what remains before pre-production hardening
3. keep unresolved limits explicit rather than hiding them behind broader claims

## Decision 7: Phase 07 P2 must replay the interim limitation explicitly

`Phase 07 P2` should not only replay happy-path or ordinary failure-path integration.

It should also include one explicit replay where:

1. the live bridge path is exercised after checkpoint truth has advanced
2. the observed catch-up limitation is diagnosed as a consequence of the interim mapping
3. the result is not overclaimed as proof of final V2 post-checkpoint catch-up semantics

## Decision 10: Phase 07 P3 is accepted and Phase 07 is complete

The first V2 product path is now explicitly chosen as:

1. `RF=2`
2. `sync_all`
3. existing master / volume-server heartbeat path
4. V2 engine owns recovery policy
5. `v2bridge` provides real storage truth

This decision is accepted with explicit non-claims:

1. not production-ready
2. no real master-side control delivery proof yet
3. no full rebuild execution proof yet
4. no general post-checkpoint catch-up proof yet
5. no full integrated engine -> executor -> `v2bridge` catch-up proof yet

Phase 07 is therefore complete, and the next phase is pre-production hardening.
