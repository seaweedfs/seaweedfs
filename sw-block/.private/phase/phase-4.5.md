# Phase 4.5

Date: 2026-03-29
Status: complete
Purpose: harden Gate 4 / Gate 5 credibility after Phase 04 by tightening bounded `CatchUp`, elevating `Rebuild` as a first-class path, and strengthening crash-consistency / recoverability proof

## Related Plan

Strategic phase:

- `sw-block/.private/phase/phase-4.5.md`

Simulator implementation plan:

- `learn/projects/sw-block/design/phase-05-crash-consistency-simulation.md`

Use them together:

- `Phase 4.5` defines the gate-hardening purpose and priorities
- `phase-05-crash-consistency-simulation.md` is the detailed simulator implementation plan

## Why This Phase Exists

Phase 04 has already established:

1. per-replica sender identity
2. one active recovery session per replica per epoch
3. stale authority fencing
4. sender-owned execution APIs
5. assignment-intent orchestration
6. minimal historical-data prototype
7. prototype scenario closure

The next risk is no longer ownership structure.

The next risk is:

1. `CatchUp` becoming too broad, too long-lived, or too optimistic
2. `Rebuild` remaining underspecified even though it will likely become a common path
3. simulator proof still being weaker than desired on crash-consistency and restart-recoverability

So `Phase 4.5` exists to harden the decision gate before real engine planning.

## Relationship To Phase 04

`Phase 4.5` is not a new architecture line.

It is a narrow hardening step after normal Phase 04 closure.

It should:

- keep the V2 core
- not reopen sender/session ownership architecture
- strengthen recovery boundaries and proof quality

## Main Questions

1. how narrow should `CatchUp` be?
2. when must recovery escalate to `Rebuild`?
3. what exactly is the `Rebuild` source of truth?
4. what does restart-recoverable / crash-consistent state mean in the simulator?

## Core Decisions To Drive

### 1. Bounded CatchUp

`CatchUp` should be explicitly bounded by:

1. target range
2. retention proof
3. time budget
4. progress budget
5. resource budget

It should stop and escalate when:

1. target drifts too long
2. progress stalls
3. recoverability proof is lost
4. retention cost becomes unreasonable
5. session budget expires

### 2. Rebuild Is First-Class

`Rebuild` is not an embarrassment path.

It is the formal path for:

1. long gap
2. unstable recoverability
3. non-convergent catch-up
4. excessive replay cost
5. restart-recoverability uncertainty

### 3. Rebuild Source Model

To address the concern that tightening `CatchUp` makes `Rebuild` too dominant:

`Rebuild` should be split conceptually into two modes:

1. **Snapshot + Tail**
   - preferred path
   - use a dated but internally consistent base snapshot/checkpoint
   - then apply retained WAL tail up to the committed recovery boundary

2. **Full Base Rebuild**
   - fallback path
   - used when no acceptable snapshot/base image exists
   - more expensive and slower

Decision boundary:

- use `Snapshot + Tail` when a trusted snapshot/checkpoint/base exists that covers the required base state
- use `Full Base Rebuild` when no such trusted base exists

So "rebuild" should not mean only:

- copy everything from scratch

It should usually mean:

- re-establish a trustworthy base image
- then catch up from that base to the committed boundary

This keeps `Rebuild` practical even if `CatchUp` becomes narrower.

### 4. Safe Recovery Truth

The simulator should explicitly separate:

1. `ReceivedLSN`
2. `WALDurableLSN`
3. `ExtentAppliedLSN`
4. `CheckpointLSN`
5. `RecoverableLSNAfterRestart`

This is needed so that:

- `ACK` truth
- visible-state truth
- crash-restart truth

do not collapse into one number.

## Priority

### P0

1. document bounded `CatchUp` rule
2. document `Rebuild` modes:
   - snapshot + tail
   - full base rebuild
3. define escalation conditions from `CatchUp` to `Rebuild`

Status:

- accepted on both prototype and simulator sides
- prototype: bounded `CatchUp` is semantic, target-frozen, budget-enforced, and rebuild is a sender-owned exclusive path
- simulator: crash-consistency state split, checkpoint-safe restart boundary, and core invariants are in place

### P1

4. strengthen simulator state model with crash-consistency split:
   - `ReceivedLSN`
   - `WALDurableLSN`
   - `ExtentAppliedLSN`
   - `CheckpointLSN`
   - `RecoverableLSNAfterRestart`

5. add explicit invariants:
   - `AckedFlushLSN <= RecoverableLSNAfterRestart`
   - visible state must have recoverable backing
   - promotion candidate must possess recoverable committed prefix

Status:

- accepted on the simulator side
- remaining work is no longer basic state split; it is stronger traceability and adversarial exploration

### P2

6. add targeted scenarios:
   - `ExtentAheadOfCheckpoint_CrashRestart_ReadBoundary`
   - `AckedFlush_MustBeRecoverableAfterCrash`
   - `UnackedVisibleExtent_MustNotSurviveAsCommittedTruth`
   - `CatchUpChasingMovingHead_EscalatesOrConverges`
   - `CheckpointGCBreaksRecoveryProof`

Status:

- baseline targeted scenarios accepted
- predicate-guided/adversarial exploration remains open

### P3

7. make prototype traceability stronger for:
   - `A5`
   - `A6`
   - `A7`
   - `A8`

8. decide whether Gate 4 / Gate 5 are now credible enough for engine planning

Status:

- partially complete
- Gate 4 / Gate 5 are materially stronger
- remaining work is to make `A5-A8` double evidence more explicit and reviewable

## Scope

### In scope

1. bounded `CatchUp`
2. first-class `Rebuild`
3. snapshot + tail rebuild model
4. crash-consistency simulator state split
5. targeted liveness / recoverability scenarios

### Out of scope

1. Smart WAL expansion
2. V1 production integration
3. backend/storage engine redesign
4. performance optimization as primary goal
5. frontend/wire protocol work

## Exit Criteria

`Phase 4.5` is done when:

1. `CatchUp` budget / escalation rule is explicit in docs and simulator
2. `Rebuild` is explicitly modeled as:
   - snapshot + tail preferred
   - full base rebuild fallback
3. simulator has explicit crash-consistency state split
4. simulator has targeted crash / liveness scenarios for the listed risks
5. acceptance items `A5-A8` have stronger executable proof, ideally with explicit prototype + simulator evidence pairs
6. we can make a more credible decision on:
   - real V2 engine planning
   - or `V2.5` correction

## Review Gates

These are explicit review gates for `Phase 4.5`.

### Gate 1: Bounded CatchUp Must Be Semantic

It is not enough to add budget fields in docs or structs.

To count as complete:

1. timeout / budget exceed must force exit
2. moving-head chase must not continue indefinitely
3. escalation to `NeedsRebuild` must be explicit
4. tests must prove those behaviors

### Gate 2: State Split Must Change Decisions

It is not enough to add more state names.

To count as complete, the new crash-consistency state split must materially change:

1. `ACK` legality
2. restart recoverability judgment
3. visible-state legality
4. promotion-candidate legality

### Gate 3: A5-A8 Need Double Evidence

It is not enough for only prototype or only simulator to cover them.

To count as complete, each of:

- `A5`
- `A6`
- `A7`
- `A8`

should have:

1. one prototype-side evidence path
2. one simulator-side evidence path

## Scope Discipline

`Phase 4.5` must remain a bounded gate-hardening phase.

It should stay focused on:

1. tightening boundaries
2. strengthening proof
3. clearing the path for engine planning

It should not turn into a broad new feature-expansion phase.

## Current Status Summary

Accepted now:

1. `sw` `Phase 4.5 P0`
   - bounded `CatchUp` is semantic, not documentary
   - `FrozenTargetLSN` is a real session invariant
   - `Rebuild` is an exclusive sender-owned execution path
2. `tester` crash-consistency simulator strengthening
   - checkpoint/restart boundary is explicit
   - recoverability is no longer a single collapsed watermark
   - core crash-consistency invariants are executable

Open now:

1. low-priority cleanup such as redundant frozen-target bookkeeping fields

Completed since initial approval:

1. `A5-A8` explicit double-evidence traceability materially strengthened
2. predicate exploration / adversarial search added on simulator side
3. crash-consistency random/adversarial search found and helped fix a real `StateAt(lsn)` historical-state bug

## Assignment For `sw`

Focus: prototype/control-path formalization

Completed work:

1. updated prototype traceability for:
   - `A5`
   - `A6`
   - `A7`
   - `A8`
2. made rebuild-source decision evidence explicit in prototype tests:
   - snapshot + tail chosen only when trusted base exists
   - full base chosen when it does not
3. added focused prototype evidence grouping for engine-planning review

Remaining optional cleanup:

4. optionally clean low-priority redundancy:
   - `TargetLSNAtStart` if superseded by `FrozenTargetLSN`

## Assignment For `tester`

Focus: simulator/crash-consistency proof

Completed work:

1. wired simulator-side evidence explicitly into acceptance traceability for:
   - `A5`
   - `A6`
   - `A7`
   - `A8`
2. added predicate exploration / adversarial search around the new crash-consistency model
3. added danger predicates for major failure classes:
   - acked flush lost
   - visible unrecoverable state
   - catch-up livelock / rebuild-required-but-not-escalated
