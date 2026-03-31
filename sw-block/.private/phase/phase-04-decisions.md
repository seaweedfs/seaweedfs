# Phase 04 Decisions

Date: 2026-03-27
Status: complete

## First Slice Decision

The first standalone V2 implementation slice is:

- per-replica sender ownership
- one active recovery session per replica per epoch

## Why Not Start In V1

V1/V1.5 remains:

- production line
- maintenance/fix line

It should not be the place where V2 architecture is first implemented.

## Why This Slice

This slice:

- directly addresses the clearest V1.5 structural pain
- maps cleanly to the V2-boundary tests
- is narrow enough to implement without dragging in the entire future architecture

## Accepted P0 Refinements

### Sender epoch coherence

Sender-owned epoch is real state, not decoration.

So:

- reconcile/update paths must refresh sender epoch
- stale active session must be invalidated on epoch advance

### Session lifecycle

The first slice should not use a totally loose lifecycle shell.

So:

- session phase changes now follow an explicit transition map
- invalid jumps are rejected

### Session attach rule

Attaching a session at the wrong epoch is invalid.

So:

- `AttachSession(epoch, kind)` must reject epoch mismatch with the owning sender

## Accepted P1 Refinements

### Session identity fencing

The standalone V2 slice must reject stale completion by explicit session identity.

So:

- `RecoverySession` has stable unique identity
- sender completion must be by session ID, not by "current pointer"
- stale session results are rejected at the sender authority boundary

### Ownership vs execution

Ownership creation is not the same as execution start.

So:

- `AttachSession()` and `SupersedeSession()` establish ownership only
- `BeginConnect()` is the first execution-state mutation

### Completion authority

An ID match alone is not enough to complete recovery.

So:

- completion must require a valid completion-ready phase
- normal completion requires converged catch-up
- zero-gap fast completion is allowed explicitly from handshake

## P2 Direction

The next prototype step is not broader simulation.

It is:

- recovery outcome branching
- assignment-intent orchestration
- prototype-level end-to-end recovery flow

## Accepted P2 Refinements

### Recovery boundary

Recovery classification must use a lineage-safe boundary, not a raw primary WAL head.

So:

- handshake outcome classification uses committed/safe recovery boundary
- stale or divergent extra tail must not be treated as zero-gap by default

### Stale assignment fencing

Assignment intent must not create current live sessions from stale epoch input.

So:

- stale assignment epoch is rejected
- assignment result distinguishes:
  - created
  - superseded
  - failed

### Phase discipline on outcome classification

The outcome API must respect execution entry rules.

So:

- handshake-with-outcome requires valid connecting phase before acting

## P3 Direction

The next prototype step is:

- minimal historical-data model
- recoverability proof
- explicit safe-boundary / divergent-tail handling

## Accepted P3 Refinements

### Recoverability proof

The historical-data prototype must prove why catch-up is allowed.

So:

- recoverability now checks retained start, end within head, and contiguous coverage
- rebuild fallback is backed by executable unrecoverability

### Historical state after recycling

Retained-prefix modeling needs a base state, not only remaining WAL entries.

So:

- tail advance captures a base snapshot
- historical state reconstruction uses snapshot + retained WAL

### Divergent tail handling

Replica-ahead state must not collapse directly to `InSync`.

So:

- divergent tail requires explicit truncation
- completion is gated on recorded truncation when required

## P4 Direction

The next prototype step is:

- prototype scenario closure
- acceptance-criteria to prototype traceability
- explicit expression of the 4 V2-boundary cases against `enginev2`

## Accepted P4 Refinements

### Prototype scenario closure

The prototype must stop being only a set of local mechanisms.

So:

- acceptance criteria are mapped to prototype evidence
- key V2-boundary scenarios are expressed directly against `enginev2`
- prototype behavior is reviewable scenario-by-scenario

### Phase 04 completion decision

Phase 04 has now met its intended prototype scope:

- ownership
- execution gating
- outcome branching
- minimal historical-data model
- prototype scenario closure

So:

- no broad new Phase 04 work should be added
- next work should move to `Phase 4.5` gate-hardening
