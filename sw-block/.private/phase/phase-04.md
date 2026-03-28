# Phase 04

Date: 2026-03-27
Status: active
Purpose: start the first standalone V2 implementation slice under `sw-block/`, centered on per-replica sender ownership and explicit recovery-session ownership

## Goal

Build the first real V2 implementation slice without destabilizing V1.

This slice should prove:

1. per-replica sender identity
2. explicit one-session-per-replica recovery ownership
3. endpoint/assignment-driven recovery updates
4. clean handoff between normal sender and recovery session

## Why This Phase Exists

The simulator and design work are now strong enough to support a narrow implementation slice.

We should not start with:

- Smart WAL
- new storage engine
- frontend integration

We should start with the ownership problem that most clearly separates V2 from V1.5.

## Source Of Truth

Design:
- `sw-block/design/v2-first-slice-session-ownership.md`
- `sw-block/design/v2-acceptance-criteria.md`
- `sw-block/design/v2-open-questions.md`

Simulator reference:
- `sw-block/prototype/distsim/`

## Scope

### In scope

1. per-replica sender owner object
2. explicit recovery session object
3. session lifecycle rules
4. endpoint update handling
5. basic tests for sender/session ownership

### Out of scope

- Smart WAL in production code
- real block backend redesign
- V1 integration
- frontend publication

## Assigned Tasks For `sw`

### P0

1. create standalone V2 implementation area under `sw-block/`
- recommended:
  - `sw-block/prototype/enginev2/`

2. define sender/session types
- sender owner per replica
- recovery session per replica per epoch

3. implement basic lifecycle
- create sender
- attach session
- supersede stale session
- close session on success / invalidation

## Current Progress

Delivered in this phase so far:

- standalone V2 area created under:
  - `sw-block/prototype/enginev2/`
- core types added:
  - `Sender`
  - `RecoverySession`
  - `SenderGroup`
- sender/session lifecycle shell implemented
- per-replica ownership implemented
- endpoint-change invalidation implemented
- sender epoch coherence implemented
- session epoch attach validation implemented
- session phase transitions now enforce a real transition map
- session identity fencing implemented
- stale completion rejected by session ID
- execution APIs implemented:
  - `BeginConnect`
  - `RecordHandshake`
  - `BeginCatchUp`
  - `RecordCatchUpProgress`
  - `CompleteSessionByID`
- completion authority tightened:
  - catch-up must converge
  - zero-gap handshake fast path allowed
- attach/supersede now establish ownership only
- sender-group orchestration tests added
- current `enginev2` test state at latest review:
  - 46 tests passing

Next focus for `sw`:

- continue Phase 04 beyond execution gating:
  - recovery outcome branching
  - sender-group orchestration from assignment intent
  - prototype-level end-to-end recovery flow
- do not integrate into V1 production tree yet

### P1

4. implement endpoint update handling
- changed-address update must refresh the right sender owner

5. implement epoch invalidation
- stale session must stop after epoch bump

6. add tests matching the slice acceptance

### P2

7. add recovery outcome branching
- distinguish:
  - zero-gap fast completion
  - positive-gap catch-up completion
  - unrecoverable gap / `NeedsRebuild`

8. add assignment-intent driven orchestration
- move beyond raw reconcile-only tests
- make sender-group react to explicit recovery intent

9. add prototype-level end-to-end flow tests
- assignment/update
- session creation
- execution
- completion / invalidation
- rebuild escalation

## Exit Criteria

Phase 04 is done when:

1. standalone V2 sender/session slice exists under `sw-block/`
2. sender ownership is per replica, not set-global
3. one active recovery session per replica per epoch is enforced
4. endpoint update and epoch invalidation are tested
5. sender-owned execution flow is validated
6. recovery outcome branching exists at prototype level
