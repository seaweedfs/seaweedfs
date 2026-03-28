# V2 Acceptance Criteria

Date: 2026-03-27

## Purpose

This document defines the minimum protocol-validation bar for V2.

It is not the full scenario backlog.

It is the smaller acceptance set that should be true before we claim:

- the V2 protocol shape is validated enough to guide implementation

## Scope

This acceptance set is about:

- protocol correctness
- recovery correctness
- lineage / fencing correctness
- data correctness at target `LSN`

This acceptance set is not yet about:

- production performance
- frontend integration
- wire protocol
- disk implementation details

## Acceptance Rule

A V2 acceptance item should satisfy all of:

1. named scenario
2. explicit expected behavior
3. simulator coverage
4. clear invariant or pass condition
5. mapped reason why it matters

## Acceptance Set

### A1. Committed Data Survives Failover

Must prove:

- acknowledged data is not lost after primary failure and promotion

Evidence:

- `S1`
- distributed simulator pass

Pass condition:

- promoted node matches reference state at committed `LSN`

### A2. Uncommitted Data Is Not Revived

Must prove:

- non-acknowledged writes do not become committed after failover

Evidence:

- `S2`

Pass condition:

- committed prefix remains at the previous valid boundary

### A3. Stale Epoch Traffic Is Fenced

Must prove:

- old primary / stale sender traffic cannot mutate current lineage

Evidence:

- `S3`
- stale write / stale barrier / stale delayed ack scenarios

Pass condition:

- stale traffic is rejected
- committed prefix does not change

### A4. Short-Gap Catch-Up Works

Must prove:

- brief outage with recoverable gap returns via catch-up, not rebuild

Evidence:

- `S4`
- same-address transient outage comparison

Pass condition:

- recovered replica returns to `InSync`
- final state matches reference

### A5. Non-Convergent Catch-Up Escalates Explicitly

Must prove:

- tail-chasing or failed catch-up does not pretend success

Evidence:

- `S6`

Pass condition:

- explicit `CatchingUp -> NeedsRebuild`

### A6. Recoverability Boundary Is Explicit

Must prove:

- recoverable vs unrecoverable gap is decided explicitly

Evidence:

- `S7`
- Smart WAL availability transition scenarios

Pass condition:

- recovery aborts when reservation/payload availability is lost
- rebuild becomes the explicit fallback

### A7. Historical Data Correctness Holds

Must prove:

- recovered data for target `LSN` is historically correct
- current extent cannot fake old history

Evidence:

- `S8`
- `S9`

Pass condition:

- snapshot + tail rebuild matches reference state
- current-extent reconstruction of old `LSN` fails correctness

### A8. Durability Mode Semantics Are Correct

Must prove:

- `best_effort`, `sync_all`, and `sync_quorum` behave as intended under mixed replica states

Evidence:

- `S10`
- `S11`
- timeout-backed quorum/all race tests

Pass condition:

- `sync_all` remains strict
- `sync_quorum` commits only with true durable quorum
- invalid `sync_quorum` topology assumptions are rejected

### A9. Promotion Uses Safe Candidate Eligibility

Must prove:

- promotion requires:
  - running
  - epoch alignment
  - state eligibility
  - committed-prefix sufficiency

Evidence:

- stronger `S12`
- candidate eligibility tests

Pass condition:

- unsafe candidates are rejected by default
- desperate promotion, if any, is explicit and separate

### A10. Changed-Address Restart Is Explicitly Recoverable

Must prove:

- endpoint is not identity
- changed-address restart does not rely on stale endpoint reuse

Evidence:

- V1 / V1.5 / V2 changed-address comparison
- endpoint-version / assignment-update simulator flow

Pass condition:

- stale endpoint is rejected
- control-plane update refreshes primary view
- recovery proceeds only after explicit update

### A11. Timeout Semantics Are Explicit

Must prove:

- barrier, catch-up, and reservation timeouts are first-class protocol behavior

Evidence:

- Phase 03 P0 timeout tests

Pass condition:

- timeout effects are explicit
- stale timeouts do not regress recovered state
- late barrier ack after timeout is rejected

### A12. Timer Races Are Stable

Must prove:

- timer/event ordering does not silently break protocol guarantees

Evidence:

- Phase 03 P1/P2 race tests

Pass condition:

- same-tick ordering is explicit
- promotion / epoch bump / timeout interactions preserve invariants
- traces are debuggable

## Compare Requirement

Where meaningful, V2 acceptance should include comparison against:

- `V1`
- `V1.5`

Especially for:

- changed-address restart
- same-address transient outage
- tail-chasing
- slow control-plane recovery

## Required Evidence

Before calling V2 protocol validation “good enough”, we want:

1. scenario coverage in `v2_scenarios.md`
2. selected simulator tests in `distsim`
3. timing/race tests in `eventsim`
4. V1 / V1.5 / V2 comparison where relevant
5. review sign-off that the tests prove the right thing

## What This Does Not Prove

Even if all acceptance items pass, this still does not prove:

- production implementation quality
- wire protocol correctness
- real performance
- disk-level behavior

Those require later implementation and real-system validation.

## Bottom Line

If A1 through A12 are satisfied, V2 is validated enough at the protocol/design level to justify:

1. implementation slicing
2. Smart WAL design refinement
3. later real-engine integration
