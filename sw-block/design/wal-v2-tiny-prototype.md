# WAL V2 Tiny Prototype

Date: 2026-03-26
Status: design/prototyping plan
Purpose: validate the core V2 replication logic before committing to a broader redesign

## Goal

Build a small, non-production prototype that proves the core V2 ideas:

1. `ExtentBackend` abstraction
2. 3-tier replication FSM
3. async ordered sender loop
4. barrier-driven durability tracking
5. short-gap catch-up vs long-gap rebuild boundary
6. recovery feasibility and reservation semantics

This prototype is for discovering:
- state complexity
- recovery correctness
- sender-loop behavior
- performance shape

It is not for shipping.

## Prototype Scope

### 1. Extent backend isolation layer

Define a clean backend interface for extent reads/writes.

Initial implementation:
- `FileBackend`
- normal Linux file
- `pread`
- `pwrite`
- optional `fallocate`

Do not start with raw-device allocation.

The point is to stabilize:
- extent semantics
- base-image import/export assumptions
- checkpoint/snapshot integration points

### 2. V2 asynchronous replication FSM

Build a pure in-memory FSM for one replica.

FSM owns:
- state
- anchor LSNs
- transition legality
- sync eligibility
- action suggestions
- recovery reservation metadata

Target state set:
- `Bootstrapping`
- `InSync`
- `Lagging`
- `CatchingUp`
- `PromotionHold`
- `NeedsRebuild`
- `Rebuilding`
- `CatchUpAfterRebuild`
- `Failed`

The FSM must not do:
- network I/O
- disk I/O
- goroutine management

### 3. Sender loop + barrier primitive

For each replica:
- one ordered sender goroutine
- one non-blocking enqueue path from primary write path
- one barrier/progress path

Primary write path:
1. allocate `LSN`
2. append local WAL/journal metadata
3. enqueue to sender loop
4. return according to durability mode

The sender loop is responsible for:
- live ordered send
- reconnect handling
- catch-up replay
- rebuild-tail replay

## Explicit Non-Goals

These are intentionally excluded from the tiny prototype:

- raw allocator
- garbage collection
- `NVMe-oF`
- `ublk`
- chain replication
- CSI / control plane
- multi-replica quorum
- encryption
- real snapshot storage optimization

These are extension layers, not the core logic being validated here.

## Design Principle

Those excluded items are not being rejected.

They are treated as:
- extensions of the core logic

The prototype should be designed so they can later plug in without rewriting the state machine.

## Suggested Layout

One reasonable layout:

- `weed/storage/blockvol/fsmv2/`
  - `fsm.go`
  - `events.go`
  - `actions.go`
  - `fsm_test.go`
- `weed/storage/blockvol/prototypev2/`
  - `backend.go`
  - `file_backend.go`
  - `sender_loop.go`
  - `barrier.go`
  - `prototype_test.go`

Preferred direction:
- keep it close enough to production packages that later reuse is easy
- but clearly marked experimental

## Core Interfaces

### Extent backend

Example direction:

```go
type ExtentBackend interface {
    ReadAt(p []byte, off int64) (int, error)
    WriteAt(p []byte, off int64) (int, error)
    Sync() error
    Size() uint64
}
```

### FSM

Example direction:

```go
type ReplicaFSM struct {
    // state
    // epoch
    // anchor LSNs
    // reservation metadata
}

func (f *ReplicaFSM) Apply(evt ReplicaEvent) ([]ReplicaAction, error)
```

### Sender loop

Example direction:

```go
type SenderLoop struct {
    // input queue
    // FSM
    // transport mock/adapter
}
```

## What The Prototype Must Prove

### A. FSM correctness

The FSM must show that the state set is sufficient and coherent.

Key scenarios:

1. `Bootstrapping -> InSync`
2. `InSync -> Lagging -> CatchingUp -> PromotionHold -> InSync`
3. `Lagging -> NeedsRebuild -> Rebuilding -> CatchUpAfterRebuild -> PromotionHold -> InSync`
4. epoch change aborts catch-up
5. epoch change aborts rebuild
6. reservation-lost aborts catch-up
7. rebuild-too-slow aborts reconstruction
8. flapping replica does not instantly re-enter `InSync`

### B. Sender ordering

The sender loop must prove:
- strict LSN order per replica
- no inline ship races from concurrent writes
- decoupled foreground write path

### C. Barrier semantics

Barrier must prove:
- it waits on replica progress
- it uses `flushedLSN`, not transport guesses
- it can drive promotion eligibility cleanly

### D. Recovery boundary

Prototype must make the handoff explicit:
- recent lag -> reserved replay window
- long lag -> rebuild from base image + trailing replay

### E. Recovery reservation

Prototype must make this explicit:
- a window is not enough
- it must be provable and then reserved
- losing the reservation must abort recovery cleanly

## Performance Questions The Prototype Should Answer

Not benchmark headlines.

Instead:

1. how much contention disappears from the hot write path after removing inline ship
2. how queue depth grows under slow replicas
3. when catch-up stops converging
4. how expensive promotion hold is
5. how much complexity is added by rebuild-tail replay
6. how much complexity is added by reservation management

## Success Criteria

The tiny prototype is successful if it gives clear answers to:

1. can the V2 FSM be made explicit and testable?
2. does sender-loop ordering materially simplify the replication path?
3. is the catch-up vs rebuild boundary coherent under a moving primary head?
4. does reservation-based recoverability make the design safer and clearer?
5. does the architecture look simpler than extending WAL V1 forever?

## Failure Criteria

The prototype should be considered unsuccessful if:

1. state count explodes and remains hard to reason about
2. sender loop does not materially simplify ordering/recovery
3. promotion and recovery rules remain too coupled to ad hoc timers and network callbacks
4. rebuild-from-base + trailing replay is still ambiguous even in a controlled prototype
5. reservation handling turns into unbounded complexity

## Relationship To WAL V1

WAL V1 remains the current delivery line.

This prototype is not a replacement for:
- `CP13-6`
- `CP13-7`
- `CP13-8`
- `CP13-9`

It exists to inform what should move into WAL V2 after WAL V1 closes.

## Bottom Line

The tiny prototype should validate the core logic only:

- clean backend boundary
- explicit FSM
- ordered async sender
- recoverability as a proof-plus-reservation problem
- rebuild as a separate recovery mode, not a WAL accident
