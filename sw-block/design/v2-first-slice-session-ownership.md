# V2 First Slice: Per-Replica Sender and Recovery Session Ownership

Date: 2026-03-27

## Purpose

This document defines the first real V2 implementation slice.

The slice is intentionally narrow:

- per-replica sender ownership
- explicit recovery session ownership
- clear coordinator vs primary responsibility

This is the first step toward a standalone V2 block engine under `sw-block/`.

## Why This Slice First

It directly addresses the clearest V1.5 structural limits:

- sender identity loss when replica sets are refreshed
- changed-address restart recovery complexity
- repeated reconnect cycles without stable per-replica ownership
- adversarial Phase 13 boundary tests that V1.5 cannot cleanly satisfy

It also avoids jumping too early into:

- Smart WAL
- new backend storage layout
- full production transport redesign

## Core Decision

Use:

- **one sender owner per replica**
- **at most one active recovery session per replica per epoch**

Healthy replicas may only need their steady sender object.

Degraded / reconnecting replicas gain an explicit recovery session owned by the primary.

## Ownership Split

### Coordinator

Owns:

- replica identity / endpoint truth
- assignment updates
- epoch authority
- session creation / destruction intent

Does not own:

- byte-by-byte catch-up execution
- local sender loop scheduling

### Primary

Owns:

- per-replica sender objects
- per-replica recovery session execution
- reconnect / catch-up progress
- timeout enforcement for active session
- transition from:
  - normal sender
  - to recovery session
  - back to normal sender

### Replica

Owns:

- receive/apply path
- barrier ack
- heartbeat/reporting

Replica remains passive from the recovery-orchestration point of view.

## Data Model

## Sender Owner

Per replica, maintain a stable sender owner with:

- replica logical ID
- current endpoint
- current epoch view
- steady-state health/status
- optional active recovery session reference

## Recovery Session

Per replica, per epoch:

- `ReplicaID`
- `Epoch`
- `EndpointVersion` or equivalent endpoint truth
- `State`
  - `connecting`
  - `catching_up`
  - `in_sync`
  - `needs_rebuild`
- `StartLSN`
- `TargetLSN`
- timeout / deadline metadata

## Session Rules

1. only one active session per replica per epoch
2. new assignment for same replica:
- supersedes old session only if epoch/session generation is newer
3. stale session must not continue after:
- epoch bump
- endpoint truth change
- explicit coordinator replacement

## Minimal State Transitions

### Healthy path

1. replica sender exists
2. sender ships normally
3. replica remains `InSync`

### Recovery path

1. sender detects or is told replica is not healthy
2. coordinator provides valid assignment/endpoint truth
3. primary creates recovery session
4. session connects
5. session catches up if recoverable
6. on success:
- session closes
- steady sender resumes normal state

### Rebuild path

1. session determines catch-up is not sufficient
2. session transitions to `needs_rebuild`
3. higher layer rebuild flow takes over

## What This Slice Does Not Include

Not in the first slice:

- Smart WAL payload classes in production
- snapshot pinning / GC logic
- new on-disk engine
- frontend publication changes
- full production event scheduler

## Proposed V2 Workspace Target

Do this under `sw-block/`, not `weed/storage/blockvol/`.

Suggested area:

- `sw-block/prototype/enginev2/`

Suggested first files:

- `sw-block/prototype/enginev2/session.go`
- `sw-block/prototype/enginev2/sender.go`
- `sw-block/prototype/enginev2/group.go`
- `sw-block/prototype/enginev2/session_test.go`

The first code does not need full storage I/O.
It should prove ownership and transition shape first.

## Acceptance For This Slice

The slice is good enough when:

1. sender identity is stable per replica
2. changed-address reassignment updates the right sender owner
3. multiple reconnect cycles do not lose recovery ownership
4. stale session does not survive epoch bump
5. the 4 Phase 13 V2-boundary tests have a clear path to become satisfiable

## Relationship To Existing Simulator

This slice should align with:

- `v2-acceptance-criteria.md`
- `v2-open-questions.md`
- `v1-v15-v2-comparison.md`
- `distsim` / `eventsim` behavior

The simulator remains the design oracle.
The first implementation slice should not contradict it.
