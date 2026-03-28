# WAL Replication V2

Date: 2026-03-26
Status: design proposal
Purpose: redesign WAL-based block replication around explicit short-gap catch-up and long-gap reconstruction

## Goal

Provide a replication architecture that:

- keeps the primary write path fast
- supports correct synchronous durability semantics
- supports short-gap reconnect catch-up using WAL
- avoids paying unbounded WAL retention tax for long-lag replicas
- uses reconstruction from a real checkpoint/snapshot base for larger lag

This design replaces a "WAL does everything" mindset with a 3-tier recovery model.

## Core Principle

WAL is excellent for:
- recent ordered delta
- local crash recovery
- short-gap replica catch-up

WAL is not the right long-range recovery mechanism for lagging block replicas.

Long-gap recovery should use:
- a real checkpoint/snapshot base image
- plus WAL tail replay after that base point

## Correctness Boundary

Never reconstruct old state from current extent alone.

Example:

1. `LSN 100`: block `A = foo`
2. `LSN 120`: block `A = bar`

If a replica needs state at `LSN 100`, current extent contains `bar`, not `foo`.

Therefore:
- current extent is latest state
- not historical state

So long-gap recovery must use a base image that is known to represent a real checkpoint/snapshot `cpLSN`.

## 3-Tier Replication Model

### Tier A: Keep-up

Replica is close enough to the primary that normal ordered streaming keeps it current.

Properties:
- normal steady-state mode
- no special recovery path
- replica stays `InSync`

### Tier B: Lagging Catch-up

Replica fell behind, but the primary still has enough recoverable history covering the missing range.

Properties:
- reconnect handshake determines the replica durable point
- primary proves and reserves a bounded recovery window
- primary replays missing history
- replica returns to `InSync` only after replay, barrier confirmation, and promotion hold

### Tier C: Reconstruction

Replica is too far behind for direct replay.

Properties:
- replica must rebuild from a real checkpoint/snapshot base
- after base image install, primary replays trailing history after `cpLSN`
- replica only re-enters `InSync` after durable catch-up completes

## Architecture

### Primary Artifacts

The primary owns three forms of state:

1. `Active WAL`
- recent ordered metadata/delta stream
- bounded by retention policy

2. `Checkpoint Snapshot`
- immutable point-in-time base image at `cpLSN`
- used for long-gap reconstruction

3. `Current Extent`
- latest live block state
- not a substitute for historical checkpoint state

### Replica Artifacts

Replica maintains:

1. local WAL or equivalent recovery log
2. replica `receivedLSN`
3. replica `flushedLSN`
4. local extent state

## Sender Model

Do not ship recovery data inline from foreground write goroutines.

Per replica, use:
- one ordered send queue
- one sender loop

The sender loop owns:
- live stream shipping
- reconnect handling
- short-gap catch-up
- reconstruction tail replay

This guarantees:
- strict LSN order per replica
- clean transport state ownership
- no inline shipping races in the primary write path

## Write Path

Primary write path:

1. allocate monotonic `LSN`
2. append recovery metadata to local WAL or journal
3. enqueue the record to each replica sender queue
4. return according to durability mode semantics

Flusher later:
- flushes dirty data to extent
- manages checkpoints
- manages bounded retention of WAL and other recovery dependencies

## Recovery Classes

V2 supports more than one local record type.

### `WALInline`

Properties:
- payload lives directly in WAL
- recoverable while WAL is retained

### `ExtentReferenced`

Properties:
- journal entry contains metadata only
- payload is resolved from extent/snapshot generation state
- direct-extent writes and future smart-WAL paths fall into this class

Replica state does not encode these classes.

Instead, the primary must answer a stricter question for reconnect:
- is `(startLSN, endLSN]` fully recoverable under the current epoch, and can it be reserved for the duration of recovery?

## Replica Progress Model

Each replica reports progress explicitly.

### `receivedLSN`
- highest LSN received and appended locally
- not yet a durability guarantee

### `flushedLSN`
- highest LSN durably persisted on the replica
- authoritative sync durability signal

Only `flushedLSN` counts for:
- `sync_all`
- `sync_quorum`

## Replica States

Replica state is defined by `wal-replication-v2-state-machine.md`.

Important highlights:
- `Bootstrapping`
- `InSync`
- `Lagging`
- `CatchingUp`
- `PromotionHold`
- `NeedsRebuild`
- `Rebuilding`
- `CatchUpAfterRebuild`
- `Failed`

Only `InSync` replicas count toward sync durability.

## Protocol

### 1. Normal Streaming

Primary sender loop:
- sends ordered replicated write records

Replica:
1. validates ordering
2. appends locally
3. advances `receivedLSN`

### 2. Barrier / Sync

Primary sends:
- `BarrierReq{LSN, Epoch}`

Replica:
1. wait until `receivedLSN >= LSN`
2. flush durable local state
3. set `flushedLSN = LSN`
4. reply `BarrierResp{Status, FlushedLSN}`

Primary uses this to evaluate mode policy.

### 3. Reconnect Handshake

On reconnect, primary obtains:
- current epoch
- primary head
- replica durable `flushedLSN`

Then primary evaluates recovery feasibility.

Possible outcomes:

1. replica already caught up
- state -> `PromotionHold` or `InSync` depending on policy

2. bounded catch-up possible
- reserve recovery window
- state -> `CatchingUp`

3. direct replay not possible
- state -> `NeedsRebuild`

## Recovery Feasibility and Reservation

The key V2 rule is:
- `fully recoverable` is not enough
- the primary must also reserve the recovery window

Recommended engine-side flow:

1. `CheckRecoveryFeasibility(startLSN, endLSN)`
2. if feasible, `ReserveRecoveryWindow(startLSN, endLSN)`
3. only then start `CatchingUp` or `CatchUpAfterRebuild`

A recovery reservation pins:
- recovery metadata
- referenced payload generations
- required snapshots/base images
- current epoch lineage for the session

If the reservation is lost during recovery:
- abort the current attempt
- fall back to `NeedsRebuild`

## Tier B: Lagging Catch-up Algorithm

When a replica is behind but within a recoverable retained window:

1. choose a bounded target `H0`
2. reserve `(ReplicaFlushedLSN, H0]`
3. replay the missing range
4. barrier confirms durable `flushedLSN >= H0`
5. enter `PromotionHold`
6. only then restore `InSync`

### Tail-chasing problem

If the primary is writing faster than the replica can catch up, the replica may never converge.

To handle this:

1. define a bounded catch-up window
2. if catch-up rate is slower than ingest rate for too long:
   - either temporarily throttle primary admission for strict `sync_all`
   - or fail `sync_all` requests and let control-plane policy react
   - or abort to rebuild
3. do not let a replica remain in unbounded perpetual `CatchingUp`

### Important rule

For `sync_all`, the data path must not silently downgrade to `best_effort`.

Correct behavior:
- bounded retry
- then fail

Any mode change must be explicit policy, not silent transport behavior.

## Tier C: Reconstruction Algorithm

When a replica is too far behind for direct replay:

1. mark replica `NeedsRebuild`
2. choose a real checkpoint/snapshot base at `cpLSN`
3. create a rebuild reservation
4. replica enters `Rebuilding`
5. replica pulls immutable checkpoint/snapshot image
6. replica installs that base image and sets base progress to `cpLSN`
7. primary replays trailing history `(cpLSN, H0]`
8. barrier confirms durable replay
9. replica enters `PromotionHold`
10. replica returns to `InSync`

### Why snapshot/base image must be real

If the replica needs state at `cpLSN`, the base image must represent exactly that checkpoint.

Invalid:
- current extent copied at some later time and treated as historical `cpLSN`

Valid:
- immutable snapshot
- copy-on-write checkpoint image
- frozen base image

## Retention and Budget

V2 retention is bounded.

### WAL / recovery metadata retention

Primary keeps only a bounded recent recovery window:
- `max_retained_wal_bytes`
- optionally `max_retained_wal_time`

### Recovery reservation budget

Reservations are also bounded:
- timeout
- bytes pinned
- snapshot dependency lifetime

If a catch-up or rebuild session exceeds its reservation budget:
- primary aborts the session
- replica falls back to `NeedsRebuild`
- a newer rebuild plan may be chosen later

## Sync Modes

### `best_effort`
- ACK after primary local durability
- replicas may lag
- background catch-up or rebuild allowed

### `sync_all`
- ACK only when all required replicas are `InSync` and durably at target LSN
- bounded retry only
- no silent downgrade

### `sync_quorum`
- ACK when enough replicas are `InSync` and durably at target LSN

## Why This Direction

V2 separates three different concerns cleanly:

1. fast steady-state replication
2. short-gap replay
3. long-gap reconstruction

This avoids forcing WAL alone to solve all recovery cases.

## Implementation Order

Recommended order:

1. pure FSM
2. ordered sender loop
3. bounded direct replay
4. checkpoint/snapshot reconstruction
5. smarter local write path and recovery classes
6. policy and control-plane integration

## Phase 13 current direction

Current Phase 13 / WAL V1 is still:
- fixing correctness of WAL-centered sync replication
- still focused mainly on bounded WAL replay and rebuild fallback

That is the right bridge.

V2 should follow after WAL V1 closes.

## Bottom Line

V2 is not "more WAL features."

It is:
- explicit recovery feasibility
- explicit recovery reservations
- ordered sender loops
- short-gap replay for recent lag
- checkpoint/snapshot reconstruction for long lag
- promotion back to `InSync` only after durable proof
