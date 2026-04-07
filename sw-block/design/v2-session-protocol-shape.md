# V2 Session Protocol Shape

Date: 2026-04-07
Status: active draft

Implementation-oriented companion:

- `v2-rebuild-mvp-session-protocol.md` — concrete rebuild MVP protocol target

## Purpose

This note fixes the current protocol direction for VS-to-VS recovery control so
the engine can eventually shrink its semantic surface instead of re-explaining
transport/runtime details through many events.

The goal is to keep one clear split:

1. `sync` asks for facts
2. the primary decides the session
3. the replica executes and reports progress
4. data transport stays separate from semantic ack

## Message Families

The preferred bounded surface is:

1. `walData`
   - primary -> replica
   - steady-state live WAL lane
2. `sync`
   - primary -> replica
   - bounded fact query
3. `syncAck`
   - replica -> primary
   - bounded facts only
4. `sessionControl`
   - primary -> replica
   - start/cancel/supersede one session contract
5. `sessionAck`
   - replica -> primary
   - accepted/progress/completed/failed
6. `sessionData`
   - primary -> replica
   - historical repair lane
7. `sessionDataAck` (optional)
   - replica -> primary
   - transport/window control only

## Ack Separation Rule

These meanings must stay separate:

1. transport ack
2. session ack
3. sync ack

`sessionDataAck` must never imply:

1. quorum eligibility
2. recovery completion
3. return to `keepup`

## Session Decision Rule

The primary should decide from fresh sync facts:

1. `keepup` if normal sync closure is still true
2. `catchup` if the replica is still within recoverable WAL history
3. `rebuild` if the replica is below recoverable retained history

The replica does not choose the next session kind.

## Recovery Paths

### 1. Catch-up

`catchup` is the narrow WAL-only recovery path.

Expected role:

1. network delay
2. short temporary gap
3. recoverable WAL-only replay

It should not be treated as the main recovery framework.

Catch-up uses two WAL lanes:

1. replay lane from `pin_lsn` to frozen `current_lsn1`
2. live lane beyond `current_lsn1`

No bitmap is needed because WAL is ordered by LSN.

### 2. Rebuild

`rebuild` is the formal primary recovery path.

It should behave as one integrated contract with two concurrent lanes:

1. base lane
   - primary exposes a trusted snapshot/CoW view at `base_lsn`
   - replica receives extent/base data from that frozen view
2. WAL lane
   - replica accepts WAL from `base_lsn`
   - replica applies WAL into its local recovery state while base transfer
     continues

This avoids a large delayed post-snapshot catch-up that would pin old WAL too
long.

### Rebuild Variants

All rebuild variants share the same semantic contract:

1. trusted base
2. explicit target
3. live WAL lane
4. single completion boundary accepted by the primary

The data source may vary:

1. `full_copy`
   - copy the full base image
2. `snapshot_or_cow`
   - copy a trusted frozen snapshot/CoW view
3. `delta_blocks_since_base`
   - copy only blocks known to have changed since a trusted base boundary

This is an optimization choice, not a different session truth model.

## Bitmap Rule For Rebuild

The replica maintains a bitmap of LBAs already covered by applied WAL.

The rule is:

1. WAL-applied LBA => later base-copy data for that LBA must be skipped
2. WAL-received-but-not-applied LBA => not protected by bitmap

So the bit is set on `applied`, not on `received`.

### Meaning of Applied

For this protocol, `applied` means:

1. accepted into the replica's local WAL/recovery truth
2. replayable after replica restart

It does not require the update to be flushed into the final extent image before
the bitmap may protect the LBA.

## Range Bitmap Optimization

### Purpose

A persistent range bitmap can turn some rebuilds from "copy the full base" into
"copy only blocks changed since a trusted base boundary."

This is a rebuild optimization, not a new engine-level recovery kind.

### Trusted-Base Rule

Range-bitmap optimization is only valid relative to a trusted base boundary.

Valid anchors include:

1. checkpoint/snapshot at `base_lsn`
2. previously accepted rebuild/session completion at `base_lsn`

Invalid anchor:

1. arbitrary replica-reported old `applied_lsn` with no trusted-base proof

So the optimization rule is:

1. choose trusted `base_lsn`
2. compute changed blocks for `(base_lsn, target_lsn]`
3. copy only that changed-block set as the base lane
4. keep live WAL lane running in parallel

### Data Shape

Conceptually:

1. `rangeBitmap[lsn_range] -> changed_blocks`
2. planner computes `union(changed_blocks over requested range)`
3. rebuild sends only those blocks from the trusted base image

This is similar in spirit to changed-block tracking or activity-log-assisted
resync, but it must remain anchored to one explicit trusted base point.

### Layering Rule

`rangeBitmap` belongs to:

1. rebuild planner
2. storage/checkpoint metadata
3. execution optimization

It does not belong to:

1. engine projection truth
2. session semantic ownership
3. sync-decision semantics

The engine still only needs to know:

1. session kind
2. base boundary
3. target boundary
4. progress/completion/failure

## Failure Rule

Session failure must not silently decide the next semantic state.

`SessionFailed` means only:

1. this primary-issued contract did not complete

After failure:

1. the replica reports fresh facts again
2. the primary re-decides `keepup` / `catchup` / `rebuild`

No local component may self-escalate to semantic `needs_rebuild`.

## Rebuild-Time Ack Rule

During rebuild:

1. the replica may continue applying new WAL
2. the replica must continue reporting session progress
3. the replica must not be treated as normal quorum-eligible sync success until
   the rebuild contract closes

So `syncAck` during rebuild should carry:

1. current facts
2. active session state
3. not-ready-for-quorum meaning

Only after the primary accepts `SessionCompleted` may later `syncAck` regain
normal quorum semantics.

## Minimal Session Shapes

### `sessionControl`

The minimum contract should carry:

1. `session_id`
2. `epoch`
3. `replica_id`
4. `kind`
5. `base_lsn`
6. `target_lsn`
7. `deadline_ms`

For rebuild it may also carry:

1. `base_kind`
2. `snapshot_id` or `cow_view_id`
3. `reservation`

### `sessionAck`

The minimum replica response should carry:

1. `session_id`
2. `epoch`
3. `kind`
4. `phase`
5. `accepted | progress | completed | failed`

For progress reporting, the important facts are:

1. `wal_applied_lsn`
2. `base_progress`
3. `base_complete`
4. `achieved_lsn` on completion

`bitmap_coverage` may be added later if needed, but it is not required as the
first semantic surface.

## Engine Consequence

If this shape is preserved, the engine can eventually reduce its semantic
surface to a smaller set of facts:

1. assignment truth
2. sync facts and session decision
3. session progress
4. session completion
5. session failure

That reduction is only safe because transport ack, session ack, and sync ack are
kept separate at the protocol boundary.
