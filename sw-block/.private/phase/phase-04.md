# Phase 04

Date: 2026-03-27
Status: complete
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
- `sw-block/docs/archive/design/v2-first-slice-session-ownership.md`
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
  - `RecordHandshakeWithOutcome`
  - `BeginCatchUp`
  - `RecordCatchUpProgress`
  - `CompleteSessionByID`
- completion authority tightened:
  - catch-up must converge
  - zero-gap handshake fast path allowed
- attach/supersede now establish ownership only
- sender-group orchestration tests added
- recovery outcome branching implemented:
  - `OutcomeZeroGap`
  - `OutcomeCatchUp`
  - `OutcomeNeedsRebuild`
- assignment-intent orchestration implemented:
  - reconcile + recovery target session creation
  - stale assignment epoch rejected
  - created/superseded/failed outcomes distinguished
- P2 data-boundary correction accepted:
  - zero-gap now requires exact equality to committed boundary
  - replica-ahead is not zero-gap
- minimal historical-data prototype implemented:
  - `WALHistory`
  - retained-prefix / recycled-range semantics
  - executable recoverability proof
  - base snapshot for historical state after tail advance
- explicit safe-boundary handling implemented:
  - divergent tail requires truncation before `InSync`
  - truncation recorded via sender-owned execution API
- WAL-backed prototype tests added:
  - catch-up recovery with data verification
  - rebuild fallback with proof of unrecoverability
  - truncate-then-`InSync` with committed-boundary verification
- current `enginev2` test state at latest review:
-  - 95 tests passing
- prototype scenario closure completed:
  - acceptance criteria mapped to prototype evidence
  - V2-boundary scenarios expressed against `enginev2`
  - small end-to-end prototype harness added

Next phase:

- `Phase 4.5`
  - bounded `CatchUp`
  - first-class `Rebuild`
  - crash-consistency / recoverability / liveness proof hardening
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

### P3

10. add minimal historical-data prototype
- retained prefix/window
- minimal recoverability state
- explicit "why catch-up is allowed" proof

11. make safe-boundary data handling explicit
- divergent tail cleanup / truncate rule
- or equivalent explicit boundary handling before `InSync`

12. strengthen recoverability/rebuild tests
- executable proof of:
- recoverable gap
- unrecoverable gap
- rebuild fallback boundary

### P4

13. close prototype scenario coverage
- map key acceptance criteria onto `enginev2` scenarios/tests
- make prototype evidence reviewable scenario-by-scenario

14. express the 4 V2-boundary cases against the prototype
- changed-address identity-preserving recovery
- `NeedsRebuild` persistence
- catch-up without overwriting safe data
- repeated disconnect/reconnect cycles

15. add one small prototype harness if needed
- enough to show assignment -> recovery -> outcome flow end-to-end
- no product/backend integration yet

## Exit Criteria

Phase 04 is done when:

1. standalone V2 sender/session slice exists under `sw-block/`
2. sender ownership is per replica, not set-global
3. one active recovery session per replica per epoch is enforced
4. endpoint update and epoch invalidation are tested
5. sender-owned execution flow is validated
6. recovery outcome branching exists at prototype level
7. minimal historical-data / recoverability model exists at prototype level
8. prototype scenario closure is achieved for key V2 acceptance cases
