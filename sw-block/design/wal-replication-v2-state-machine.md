# WAL Replication V2 State Machine

Date: 2026-03-26
Status: design proposal
Purpose: define the V2 replication state machine for a moving-head primary where replicas may transition between keep-up, catch-up, and reconstruction while the primary continues accepting writes

## Why This Document Exists

The hard part of V2 is not the existence of three modes:

- keep-up
- catch-up
- reconstruction

The hard part is that the primary head continues advancing while replicas move between those modes.

So V2 must be specified as a real state machine:

- state definitions
- state-owned LSN anchors
- allowed transitions
- retention obligations
- abort rules

This document treats edge cases as state-transition cases.

## Scope

This is a protocol/state-machine design.

It does not yet define:
- exact RPC payloads
- exact snapshot storage format
- exact implementation package boundaries

Those can follow after the state model is stable.

## Core Terms

### `headLSN`

The primary's current highest WAL LSN.

### `replicaFlushedLSN`

The highest LSN durably persisted on the replica.

### `cpLSN`

A checkpoint/snapshot base point. A snapshot at `cpLSN` represents the block state exactly at that LSN.

### `promotionBarrierLSN`

The LSN a replica must durably reach before it can re-enter `InSync`.

### `Recovery Feasibility`

Whether `(startLSN, endLSN]` can be reconstructed completely, in order, under the current epoch.

This is not a static fact. It changes over time as WAL is reclaimed, payload generations are garbage-collected, or snapshots are released.

### `Recovery Reservation`

A bounded primary-side reservation proving a recovery window is recoverable and pinning all dependencies needed to finish the current catch-up or rebuild-tail replay.

A transition into recovery is valid only after the reservation is granted.

## State Set

Replica may be in one of these states:

1. `Bootstrapping`
2. `InSync`
3. `Lagging`
4. `CatchingUp`
5. `PromotionHold`
6. `NeedsRebuild`
7. `Rebuilding`
8. `CatchUpAfterRebuild`
9. `Failed`

Only `InSync` replicas count for sync durability.

## State Semantics

### 1. `Bootstrapping`

Replica has not yet earned sync eligibility and does not yet have trusted reconnect progress.

Properties:
- fresh replica identity or newly assigned replica
- may receive initial baseline/live stream
- not yet eligible for `sync_all`

Counts for:
- `sync_all`: no
- `sync_quorum`: no
- `best_effort`: background/bootstrap only

Owned anchors:
- current assignment epoch

### 2. `InSync`

Replica is eligible for sync durability.

Properties:
- receiving live ordered stream
- `replicaFlushedLSN` is near the primary head
- normal barrier protocol is valid

Counts for:
- `sync_all`: yes
- `sync_quorum`: yes
- `best_effort`: yes, but not required for ACK

Owned anchors:
- `replicaFlushedLSN`

### 3. `Lagging`

Replica has fallen out of the normal live-stream envelope but recovery path is not yet chosen.

Properties:
- primary no longer treats it as sync-eligible
- replica may still be recoverable from WAL or extent-backed recovery records
- or may require rebuild

Counts for:
- `sync_all`: no
- `sync_quorum`: no
- `best_effort`: background recovery only

Owned anchors:
- last known `replicaFlushedLSN`

### 4. `CatchingUp`

Replica is replaying from its own durable point toward a chosen target.

Properties:
- short-gap recovery mode
- primary must reserve and pin the required recovery window
- primary head continues to move

Counts for:
- `sync_all`: no
- `sync_quorum`: no
- `best_effort`: background recovery only

Owned anchors:
- `catchupStartLSN = replicaFlushedLSN`
- `catchupTargetLSN`
- `promotionBarrierLSN`
- `recoveryReservationID`
- `reservationExpiry`

### 5. `PromotionHold`

Replica has reached the chosen promotion point but must demonstrate short stability before re-entering `InSync`.

Properties:
- prevents immediate flapping back into sync eligibility
- replica has already reached `promotionBarrierLSN`
- promotion requires stable barriers or elapsed hold time

Counts for:
- `sync_all`: no
- `sync_quorum`: no
- `best_effort`: stabilization only

Owned anchors:
- `promotionBarrierLSN`
- `promotionHoldUntil` or equivalent hold criterion

### 6. `NeedsRebuild`

Replica cannot recover from retained recovery records alone.

Properties:
- catch-up window is insufficient or no longer provable
- replica must not count toward sync durability
- replica no longer pins old catch-up history

Counts for:
- `sync_all`: no
- `sync_quorum`: no
- `best_effort`: background repair candidate only

Owned anchors:
- last known `replicaFlushedLSN`

### 7. `Rebuilding`

Replica is fetching and installing a checkpoint/snapshot base image.

Properties:
- primary must preserve the chosen snapshot/base
- primary must preserve the required WAL or recovery tail after `cpLSN`

Counts for:
- `sync_all`: no
- `sync_quorum`: no
- `best_effort`: background rebuild only

Owned anchors:
- `snapshotID`
- `snapshotCpLSN`
- `tailReplayStartLSN = snapshotCpLSN + 1`
- `recoveryReservationID`
- `reservationExpiry`

### 8. `CatchUpAfterRebuild`

Replica has installed the base image and is replaying trailing history after it.

Properties:
- semantically similar to `CatchingUp`
- base point is checkpoint/snapshot, not the replica's original own state

Counts for:
- `sync_all`: no
- `sync_quorum`: no
- `best_effort`: background recovery only

Owned anchors:
- `snapshotCpLSN`
- `catchupTargetLSN`
- `promotionBarrierLSN`
- `recoveryReservationID`
- `reservationExpiry`

### 9. `Failed`

Replica recovery failed in a way that needs operator/control-plane action beyond normal retry.

Properties:
- terminal or semi-terminal fault state
- may require delete/recreate/manual intervention

Counts for:
- `sync_all`: no
- `sync_quorum`: no
- `best_effort`: no direct role

## Transition Rules

### `Bootstrapping -> InSync`

Trigger:
- initial bootstrap completes
- barrier confirms durable progress under the current epoch

Action:
- establish trusted `replicaFlushedLSN`
- grant sync eligibility for the first time

### `InSync -> Lagging`

Trigger:
- disconnect
- barrier timeout
- barrier fsync failure
- stream error

Action:
- remove sync eligibility immediately

### `Lagging -> CatchingUp`

Trigger:
- reconnect succeeds
- primary grants a recovery reservation proving `(replicaFlushedLSN, catchupTargetLSN]` is recoverable for a bounded window

Action:
- choose `catchupTargetLSN`
- pin required recovery dependencies for the reservation lifetime

### `Lagging -> NeedsRebuild`

Trigger:
- required recovery window is not recoverable
- impossible progress reported
- epoch mismatch invalidates direct catch-up
- background janitor determines the replica is outside recoverable budget

Action:
- stop treating replica as a catch-up candidate

### `CatchingUp -> PromotionHold`

Trigger:
- replica replays to `catchupTargetLSN`
- barrier confirms `promotionBarrierLSN`

Action:
- start promotion debounce window

### `PromotionHold -> InSync`

Trigger:
- promotion hold criteria satisfied
  - stable barrier successes
  - or elapsed hold time

Action:
- restore sync eligibility
- clear promotion anchors

### `PromotionHold -> Lagging`

Trigger:
- disconnect
- failed barrier
- failed live stream health check

Action:
- cancel promotion attempt
- remove sync eligibility

### `CatchingUp -> NeedsRebuild`

Trigger:
- catch-up cannot converge
- recovery reservation is lost
- catch-up timeout policy exceeded
- epoch changes

Action:
- abandon WAL-only catch-up
- move to reconstruction path

### `NeedsRebuild -> Rebuilding`

Trigger:
- control plane or primary chooses reconstruction base
- snapshot/base image transfer starts
- primary grants a rebuild reservation

Action:
- bind replica to `snapshotID` and `snapshotCpLSN`

### `Rebuilding -> CatchUpAfterRebuild`

Trigger:
- snapshot/base image installed successfully
- trailing recovery reservation is still valid

Action:
- replay trailing history after `snapshotCpLSN`

### `Rebuilding -> NeedsRebuild`

Trigger:
- rebuild copy fails
- rebuild reservation is lost
- rebuild WAL-tail budget is exceeded
- epoch changes

Action:
- abort current rebuild session
- remain excluded from sync durability

### `CatchUpAfterRebuild -> PromotionHold`

Trigger:
- trailing replay reaches target
- barrier confirms durable replay through `promotionBarrierLSN`

Action:
- start promotion debounce

### `CatchUpAfterRebuild -> NeedsRebuild`

Trigger:
- reservation is lost
- replay cannot converge
- epoch changes

Action:
- abandon current attempt
- require a fresh rebuild plan

### Any state -> `Failed`

Trigger examples:
- unrecoverable protocol inconsistency
- repeated rebuild failure beyond retry policy
- snapshot corruption
- local replica storage failure

## Retention Obligations By State

The key V2 rule is:

- recoverability is not a static fact
- it is a bounded promise the primary must honor once it admits a replica into recovery

### `InSync`

Primary must retain:
- recent WAL under normal retention policy

Primary does not need:
- snapshot pin purely for this replica

### `Lagging`

Primary must retain:
- enough recent information to evaluate recoverability or intentionally declare `NeedsRebuild`

This state should be short-lived.

### `CatchingUp`

Primary must retain for the reservation lifetime:
- recovery metadata for `(catchupStartLSN, promotionBarrierLSN]`
- every payload referenced by that recovery window
- current epoch lineage for the session

### `PromotionHold`

Primary must retain:
- whatever live-stream and barrier state is required to validate promotion

This state should be brief and must not pin long-lived history.

### `NeedsRebuild`

Primary retains:
- no special old recovery window for this replica

This state explicitly releases the old catch-up hold.

### `Rebuilding`

Primary must retain for the reservation lifetime:
- chosen `snapshotID`
- any base-image dependencies
- trailing history after `snapshotCpLSN`

### `CatchUpAfterRebuild`

Primary must retain for the reservation lifetime:
- recovery metadata for `(snapshotCpLSN, promotionBarrierLSN]`
- every payload referenced by that trailing window

## Moving-Head Rules

The primary head continues advancing during:
- `CatchingUp`
- `Rebuilding`
- `CatchUpAfterRebuild`

Therefore transitions must never use current head at finish time as an implicit target.

Instead, each transition must select explicit targets.

### Catch-up target

When catch-up starts, choose:
- `catchupTargetLSN = H0`

Replica first chases to `H0`, not to an infinite moving head.

Then:
- either enter `PromotionHold` and promote
- or begin another bounded cycle
- or abort to rebuild

### Rebuild target

When rebuild starts, choose:
- `snapshotCpLSN = C`
- trailing replay target `H0`

Replica installs the snapshot at `C`, then replays `(C, H0]`, then enters `PromotionHold`.

## Tail-Chasing Rule

Replica may fail to converge if:
- catch-up speed < primary ingest speed

V2 must define bounded behavior:

1. bounded catch-up window
2. bounded catch-up time
3. policy after failure to converge:
   - for `sync_all`: bounded retry, then fail requests
   - for `best_effort`: keep serving and continue background recovery or escalate to rebuild

No silent downgrade of `sync_all` is allowed.

## Recovery Feasibility

The primary must not admit a replica into catch-up based on a best-effort guess.

It must prove the requested recovery window is recoverable and then reserve it.

Recommended abstraction:

- `CheckRecoveryFeasibility(startLSN, endLSN) -> fully recoverable | needs rebuild`
- `ReserveRecoveryWindow(startLSN, endLSN) -> reservation`

Only a successful reservation may drive:
- `Lagging -> CatchingUp`
- `NeedsRebuild -> Rebuilding`
- `Rebuilding -> CatchUpAfterRebuild`

## Recovery Classes

V2 must support more than one local record type without leaking that detail into replica state.

### `WALInline`

Properties:
- payload lives directly in WAL
- recoverable while WAL is retained

### `ExtentReferenced`

Properties:
- recovery metadata points at payload outside WAL
- payload must be resolved from extent/snapshot generation state

The FSM does not care how payload is stored.

It only cares whether the requested window is fully recoverable for the lifetime of the reservation.

The engine-level rule is:

- every record in `(startLSN, endLSN]` must be payload-resolvable
- the resolved version must correspond to that record's historical state
- the payload must stay pinned until the reservation ends

If any required payload is not resolvable:
- the window is not recoverable
- the replica must go to `NeedsRebuild`

## Snapshot Rule

Rebuild must use a real checkpoint/snapshot base image.

Valid:
- immutable snapshot at `cpLSN`
- copy-on-write checkpoint image
- frozen base image with exact `cpLSN`

Invalid:
- current extent treated as historical `cpLSN`

## Epoch / Fencing Rule

Every transition is epoch-bound.

If epoch changes during:
- `Bootstrapping`
- `Lagging`
- `CatchingUp`
- `PromotionHold`
- `Rebuilding`
- `CatchUpAfterRebuild`

Then:
- abort current transition
- discard old sender assumptions
- restart negotiation under the new epoch

This prevents stale-primary recovery traffic from being accepted.

## Multi-Replica Volume Rules

Different replicas may be in different states simultaneously.

Example:
- replica A = `InSync`
- replica B = `CatchingUp`
- replica C = `Rebuilding`

Volume-level durability policy is computed per mode.

### `sync_all`
- all required replicas must be `InSync`

### `sync_quorum`
- enough replicas must be `InSync`

### `best_effort`
- primary local durability only
- replicas recover in background

## Illegal or Suspicious Conditions

These should force rejection or abort:

1. replica reports `replicaFlushedLSN > headLSN`
2. replica progress belongs to wrong epoch
3. requested recovery window is not recoverable
4. recovery reservation cannot be granted
5. snapshot base does not match claimed `cpLSN`
6. replay stream shows impossible gap/ordering after reconstruction

## Design Guidance

V2 should be implemented so that:

1. state owns recovery semantics
2. anchors make transitions explicit
3. retention obligations are derived from state
4. catch-up admission requires reservation, not guesswork
5. mode semantics are derived from `InSync` eligibility

This is better than burying recovery behavior across many ad hoc code paths.

## Bottom Line

V2 is fundamentally a state machine problem.

The correct abstraction is not:
- some edge cases around WAL replay

It is:
- replicas move through explicit states while the primary head continues advancing and recovery windows must be provable and reserved

So V2 must be designed around:
- state definitions
- anchor LSNs
- transition rules
- retention obligations
- recoverability checks
- recovery reservations
- abort conditions
