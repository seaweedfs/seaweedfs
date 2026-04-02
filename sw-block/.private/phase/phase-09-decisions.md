# Phase 09 Decisions

## Decision 1: Phase 09 is production execution closure, not packaging

The candidate-path packaging/judgment work remains inside `Phase 08 P4`.

`Phase 09` starts directly with substantial backend engineering closure.

## Decision 2: The first Phase 09 targets are real transfer, truncation, and stronger runtime ownership

The initial heavy execution blockers are:

1. real `TransferFullBase`
2. real `TransferSnapshot`
3. real `TruncateWAL`
4. stronger live runtime execution ownership

## Decision 3: Phase 09 remains bounded to the chosen candidate path unless evidence forces expansion

Default scope remains:

1. `RF=2`
2. `sync_all`
3. existing master / volume-server heartbeat path

Future paths or durability modes should not be absorbed casually into this phase.

## Decision 4: Full-base rebuild completion is defined by an achieved boundary, not exact target equality

For the chosen `RF=2 sync_all` backend path, `full_base` rebuild does not require:

1. extent image exactly equal to the engine's frozen `targetLSN`

It does require:

1. the engine plans a frozen minimum target `targetLSN`
2. the backend produces an actual rebuilt boundary `achievedLSN`
3. correctness requires `achievedLSN >= targetLSN`
4. after install, local runtime state and engine-visible completion must align to the same `achievedLSN`
5. the system must not keep engine truth at `targetLSN` while local runtime truth has advanced to `achievedLSN`

Reason:

1. the current full-base path copies a mutable extent image from the live backend
2. this backend does not provide an immutable extent export at an exact requested LSN
3. forcing exact-target extent equality would require a different protocol, not just a tighter implementation
4. rollback to an older target after a newer stable base is installed is much harder than accepting the newer stable boundary

Algorithm guarantees required by this decision:

1. minimum-target guarantee:
   - rebuild completion must never leave the replica behind the engine's frozen minimum target
2. single-truth guarantee:
   - `checkpoint`
   - `nextLSN`
   - receiver progress
   - flusher checkpoint
   - engine-visible rebuild progress/completion
   must all converge to the same `achievedLSN`
3. no split-truth guarantee:
   - do not allow local runtime state to reflect a newer boundary while engine/accounting still records the older one
4. backend-realism guarantee:
   - it is acceptable for the achieved boundary to be newer than the frozen minimum target
   - it is not acceptable for the achieved boundary to remain implicit

## Decision 5: P1 full-base execution closure accepted

P1 delivers real full-base execution closure under the Decision 4 contract.

Accepted properties:

1. `TransferFullBase(committedLSN) → (achievedLSN, error)` — achieved boundary surfaced explicitly
2. rebuild server pre-flushes before extent copy — no unflushed-entry hole
3. full state handoff on install — dirty map, WAL, superblock, flusher, receiver progress all aligned
4. second catch-up bounded to target — no unbounded replay
5. engine uses `achievedLSN` for progress recording — no split truth
6. rebuild server fail-closes on pre-copy flush failure
7. stale-higher local/runtime state is reset to the rebuilt achieved boundary, not preserved by monotonic advance

Evidence closure:

1. live-receiver convergence is now covered directly in `P1`
2. `P1` accepted state is final for full-base closure on the chosen path

## Decision 6: P2 snapshot execution closure accepted

`P2` delivers real `snapshot_tail` execution closure on the chosen path.

Accepted properties:

1. `TransferSnapshot(snapshotLSN)` now performs real TCP snapshot transfer
2. snapshot base boundary is exact, not conservative:
   - requested `snapshotLSN` must match the transferred base
   - newer checkpoints are rejected instead of silently accepted
3. snapshot transfer carries explicit boundary metadata through `SnapshotArtifactManifest.BaseLSN`
4. snapshot install converges local runtime to the exact snapshot boundary before tail replay begins
5. the `snapshot_tail` path now closes through one executor:
   - `TransferSnapshot(snapshotLSN)`
   - `StreamWALEntries(snapshotLSN, targetLSN)`
6. tail replay remains bounded to `targetLSN`
7. temporary snapshot ownership is cleaned up on both success and failure paths

Evidence closure:

1. component proof now covers real snapshot transfer and exact-boundary install
2. one-chain proof now covers `engine -> RebuildExecutor -> v2bridge -> blockvol -> tail replay -> InSync`
3. boundary-drift rejection is covered directly in `P2`

## Decision 7: P3 truncation execution closure accepted under the narrowed Option A contract

`P3` does not mean "all replica-ahead cases can be corrected by local truncate."

Accepted contract:

1. local truncation is allowed only when the local base boundary exactly matches the kept boundary:
   - `checkpointLSN == truncateLSN`
2. if `checkpointLSN > truncateLSN`:
   - ahead entries already contaminated extent
   - truncation is unsafe
   - the path must escalate to rebuild
3. if `checkpointLSN < truncateLSN`:
   - part of the kept range may still exist only in WAL
   - truncation would discard committed kept data
   - the path must escalate to rebuild
4. no path may record truncation completion while extent/base truth is known to be unsafe for local truncate
5. execution-time escalation to `NeedsRebuild` is acceptable for `P3`

Accepted properties:

1. `TruncateWAL(truncateLSN)` now performs real local correction for the truncation-safe case
2. `TruncateToLSN()` pauses the flusher and drains I/O before mutating local runtime truth
3. `blockvol.ErrTruncationUnsafe` is bridged to `engine.ErrTruncationUnsafe`
4. `CatchUpExecutor` escalates unsafe truncation cases to `StateNeedsRebuild`
5. the mixed case `checkpointLSN < truncateLSN < headLSN` is now covered directly in tests

Evidence closure:

1. component proof covers exact local truncation only for the safe case
2. one-chain proof covers both:
   - safe truncation to `InSync`
   - unsafe truncation escalation to `NeedsRebuild`
3. `P3` accepted state is final for truncation execution closure on the chosen path

## Decision 8: P4 stronger live runtime ownership accepted

`P4` closes the bounded runtime-ownership gap for the chosen `RF=2 sync_all` live volume-server path.

Accepted properties:

1. `ProcessAssignments()` now drives live recovery ownership through:
   - assignment conversion
   - orchestrator session creation/supersede
   - `RecoveryManager` start/cancel/replace/cleanup
2. runtime inputs are sourced from the live path rather than test-only injection:
   - live volume path
   - live storage adapter / pinner / reader
   - rebuild address scoped by volume path
3. replacement is serialized:
   - stale owner is cancelled and drained before replacement starts
   - no concurrent live owners remain for the same `replicaID`
4. shutdown drains live recovery owners before the block service closes volumes
5. engine policy remains in engine; `P4` does not move policy into the volume-server runtime

Evidence closure:

1. live-path proof now covers:
   - `ProcessAssignments -> plan_catchup -> exec_catchup_started -> exec_completed -> in_sync`
2. serialized replacement proof now directly demonstrates:
   - old owner alive
   - old owner `done` still open before supersede
   - `ProcessAssignments(epoch+1)` returns only after old owner `done` closes
3. shutdown proof now covers a live blocked task, not only an already-finished task

Residual note:

1. repeated primary assignment on the same volume still logs a low-severity rebuild-server double-start warning
2. broader control-plane closure remains outside `Phase 09`
