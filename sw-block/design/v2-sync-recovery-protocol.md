# V2 Sync / Recovery Protocol

Date: 2026-04-07
Status: active design

## Purpose

Define the complete replication protocol for sw-block V2. This document
covers sync, keepup, catch-up, and rebuild as one unified protocol so
the sw agent has full context for implementation.

`v2-rebuild-mvp-session-protocol.md` is the narrower first-slice spec.
This document is the surrounding context and long-term design.

## Design Principles

1. **Primary decides everything.** Replica only reports facts and executes
   contracts. Replica never self-escalates to `needs_rebuild`.

2. **One threshold.** `applied_lsn >= primary_wal_tail` → WAL-recoverable.
   Otherwise → rebuild. Matches Ceph's `last_update >= log_tail`.

3. **Deterministic engine.** Event in → state + commands + projection out.
   No side effects inside the engine. Host executes commands.

4. **Three authority layers:**
   - Assignment: master → identity (who is primary/replica, epoch, replica set)
   - Session: primary → per-replica recovery contract (keepup/catchup/rebuild)
   - Projection: primary → derived volume mode/health

5. **Failure never auto-escalates.** A failed session stays `failed`. The
   primary re-decides from fresh `syncAck` facts. Only the primary can
   issue a rebuild.

## Reference Systems

| System | Catch-up | Rebuild | Decision owner | Decision input |
|---|---|---|---|---|
| Ceph | PG log replay | Full backfill | Primary OSD (peering) | `last_update >= log_tail` |
| Mayastor | None | Segment copy | Control plane | Child sync state |
| Longhorn | None | Snapshot file sync | Controller | Revision counters |
| **sw-block V2** | WAL replay | Flushed extent + live WAL (two-line) | **Primary** | `applied_lsn >= wal_tail` |

## Protocol Overview

### Normal Operation (keepup)

```
Primary                          Replica
  │                                │
  ├─ WriteLBA ────────────────────►│ (live WAL shipping via ShipAll)
  │                                │ apply to local WAL
  │                                │
  ├─ sync(target_lsn=N) ─────────►│
  │◄─ syncAck(durable=N, applied=N)│
  │                                │
  │  decision: quorum → keepup     │
  │  derive: publish_healthy       │
```

### Catch-up (replica behind but within retained WAL)

```
Primary                          Replica
  │                                │
  ├─ sync(target_lsn=1000) ──────►│
  │◄─ syncAck(applied=500)         │
  │                                │
  │  decision: 500 >= wal_tail(100)│
  │  → WAL catch-up               │
  │                                │
  ├─ sessionControl(start_catchup  │
  │    start=500, target=1000,     │
  │    pin=500) ─────────────────►│
  │                                │
  │  LINE 1: WAL replay [500..1000]│
  ├─ walReplay(lsn=501...) ──────►│ apply, pin advances
  │                                │
  │  LINE 2: live WAL from 1001+   │
  ├─ walData(lsn=1001...) ───────►│ apply to local WAL
  │                                │
  │◄─ sessionAck(completed,        │
  │    achieved=1050) ─────────────│
  │                                │
  │  replica back in keepup        │
```

### Rebuild (replica beyond retained WAL, or fresh join)

```
Primary                          Replica
  │                                │
  ├─ sync(target_lsn=5000) ──────►│
  │◄─ syncAck(applied=0)           │
  │                                │
  │  decision: 0 < wal_tail(2000)  │
  │  → rebuild                    │
  │                                │
  ├─ sessionControl(start_rebuild  │
  │    base_lsn=flushedLSN,       │
  │    base_kind=flushed_extent) ─►│
  │                                │
  │  LINE 1: flushed extent blocks │
  ├─ sessionData(chunk...) ──────►│ apply if bitmap clear
  │                                │
  │  LINE 2: live WAL from base_lsn+1
  ├─ walData(lsn=5001...) ───────►│ apply, set bitmap bit
  │                                │
  │  Bitmap: WAL-applied LBA wins  │
  │  over later base block         │
  │                                │
  │◄─ sessionAck(base_complete)    │
  │◄─ sessionAck(completed,        │
  │    achieved=5200) ─────────────│
  │                                │
  │  replica back in keepup        │
```

## Primary Decision Logic

```
func decide(ack SyncAck, walTail, walHead uint64) SessionKind {
    replicaPos := max(ack.AppliedLSN, ack.DurableLSN)

    if replicaPos >= walHead && replicaPos > 0:
        return keepup       // fully caught up

    if replicaPos >= walTail && replicaPos > 0:
        return catchup      // behind but within retained WAL

    if replicaPos == 0 && walTail <= 1:
        return catchup      // fresh replica, WAL retained from beginning

    return rebuild          // gap exceeds retained WAL
}
```

This is one function, one threshold. Matches Ceph's `last_update >= log_tail`.

## Normalized Primary-side Sync Facts

The wire protocol still uses `sync` and `syncAck` as the bounded control
exchange. But inside the primary-owned host/runtime layer, raw wire results and
local control-path observations may be normalized into one small fact
vocabulary before the primary re-decides the next step.

This normalization is not a new replica-visible message family. It is the
primary-owned semantic shape for "what kind of sync fact just arrived?"

Current normalized fact kinds:

| Kind | Meaning | Typical sources |
|---|---|---|
| `sync_quorum_acked` | normal sync closure reached | wire `syncAck(ack_kind=quorum)`, accepted barrier |
| `sync_quorum_timed_out` | control-plane sync closure timed out | wire `syncAck(ack_kind=timed_out)`, rejected barrier |
| `sync_replay_required` | replica is behind but replay-recoverable | fresh planner classification after sync facts |
| `sync_rebuild_required` | replica is outside retained replay coverage | fresh planner classification after sync facts |
| `sync_replay_failed` | a replay/catch-up attempt failed | catch-up failure callback, replay execution failure |

Rules:

1. normalized sync facts are still facts only, never session recommendations
2. different producers may map to the same normalized fact kind
3. only the primary may turn these facts into `keepup`, `catchup`, or `rebuild`
4. this is the bridge between raw protocol input and primary-owned session
   authority

## Two-Line Recovery Model

Both catch-up and rebuild use two concurrent data lines:

### Catch-up: WAL replay + live WAL

- **Line 1**: replay retained WAL entries from `pin_lsn` to `target_lsn`
- **Line 2**: forward live WAL entries from `target_lsn+1` onward
- **Pin movement**: as replay cursor advances, pin can advance (releases old WAL)
- **No bitmap needed**: WAL entries are strictly ordered by LSN, no LBA conflict
- **Completion**: replay cursor reaches target → lines merge → keepup

### Rebuild: flushed extent base + live WAL

- **Line 1**: copy the primary's flushed extent image at `base_lsn`
- **Line 2**: forward live WAL entries newer than `base_lsn`
- **Bitmap required**: base blocks and WAL entries may target the same LBA
- **Bitmap rule**: bit set on WAL `applied` (not received). Base block skipped if bit set.
- **Completion**: all base blocks transferred AND `wal_applied_lsn >= target_lsn`
- **Base boundary rule**: `base_lsn` is a flushed/checkpoint boundary, not a merely committed boundary

### Why two lines instead of sequential (base → then catch-up)

Sequential model:
1. Copy entire flushed extent image
2. Then replay WAL from base LSN to current
3. Problem: must pin WAL for duration of snapshot copy (hours for large volumes)
4. Risk: WAL recycled before replay starts → must restart entire rebuild

Two-line model:
1. Copy flushed extent AND receive live WAL simultaneously
2. WAL pin pressure = only gap between current replay and live head (small)
3. If snapshot copy is slow, WAL line keeps replica current
4. Crash recovery is safe at any point (bitmap + local WAL)

## Bitmap Rules (Rebuild Only)

### When to set bit

Set bitmap bit when WAL entry is **applied to replica's local WAL**:
- Entry has been written to local WAL file
- Entry is replayable after crash
- Does NOT require flush to final extent

### When NOT to set bit

Do not set on:
- Network receive (TCP buffer)
- Queue but not yet local WAL write

### Conflict resolution

When base lane sends a chunk for LBA range:
- Bitmap clear → write base data
- Bitmap set → skip (WAL-applied data is newer)

Short form: **WAL always wins over base.**

### Crash safety

At any crash point:
- Bitmap can be volatile (session-local, in memory)
- Local WAL is durable → replay recovers all applied entries
- After crash: fresh sync → primary re-decides → new session if needed
- The bitmap itself need not persist, but its protected coverage must be
  re-hydrated from local durable WAL before a new rebuild session opens the base lane
- No need to persist bitmap across crashes in MVP

Current bounded claim:
- crash during rebuild is handled by `restart rebuild`
- this is not yet a claim of resumable rebuild with durable `base_progress`
- hydration of bitmap coverage protects durable WAL facts during the fresh
  rebuild; it does not by itself resume prior base-copy progress

### Issue #3 restart hydration rule

To close the volatile-bitmap restart hole:

1. a fresh rebuild session must rebuild bitmap coverage from local durable WAL
   newer than `base_lsn` before `accepted` becomes externally visible
2. the base lane must remain closed until this hydration completes
3. if the replica's local durable base is already newer than the claimed
   `base_lsn`, startup must fail closed instead of accepting a stale rebuild
4. this is why rebuild starts from a flushed/checkpoint boundary rather than a
   merely committed boundary: committed data may still live only in WAL, so it
   is not a safe direct-base image

## Replica State Machine

```
idle → accepted → running → base_complete → completed
                    │              │
                    └──► failed ◄──┘
```

- `idle`: no active session
- `accepted`: session contract valid, epoch/session_id accepted
- `running`: both base lane and WAL lane active
- `base_complete`: base transfer done, WAL lane still running
- `completed`: replica reports `achieved_lsn >= target_lsn`
- `failed`: session stopped without completion, reason reported

Failure does NOT auto-escalate. Primary re-decides from next syncAck.

## Mode Derivation (Projection)

Primary derives volume mode from all replica states + boundaries:

```
any replica in rebuild session     → needs_rebuild
any replica session failed         → degraded
any replica in catch-up session    → bootstrap_pending
no replicas assigned               → allocated_only
replica role + receiver ready      → replica_ready
primary + all readiness + durable  → publish_healthy
assigned but not ready             → bootstrap_pending
```

## Failure Handling

### Principle

No failure auto-escalates. All failures go through:
1. Session marked `failed` with reason
2. Primary waits for next `syncAck` from replica
3. Primary re-decides based on fresh facts

Primary-side normalized reading:

1. failure may first surface as `sync_replay_failed`
2. fresh sync/planner facts may then normalize to either:
   - `sync_replay_required`
   - `sync_rebuild_required`
3. only then does the primary issue the next session contract

### Failure scenarios

| Scenario | Replica does | Primary does |
|---|---|---|
| Transport lost during session | Reports `failed(transport_lost)` or goes silent | Marks session failed, waits for reconnect |
| Replica crash | Restarts, recovers local WAL, hydrates bitmap coverage before any new rebuild base lane opens, then reports facts via syncAck | Re-decides: if `applied_lsn >= wal_tail` → catch-up, else → new rebuild; current claim is fresh restart, not resume of prior base-copy offset |
| Primary crash | Nothing (waits for new primary) | New primary elected, fresh epoch, all replicas report via syncAck |
| Slow progress / timeout | Continues trying | Can cancel session via `cancel_session`, then re-decide |
| WAL recycled during outage | Reports `applied_lsn` which is now < `wal_tail` | Decides rebuild (gap exceeds retained WAL) |

### Failure reason vocabulary (stable)

- `epoch_mismatch`
- `transport_lost`
- `progress_stalled`
- `deadline_exceeded`
- `pin_lost`
- `snapshot_unavailable`
- `local_wal_corrupt`
- `local_extent_corrupt`

## Catch-up Details (Post-MVP)

Catch-up uses the same session contract shape as rebuild, but without
base copy:

```
sessionControl {
    op: start_catchup
    start_lsn: <replica_applied_lsn>
    target_lsn: <primary_wal_head>
    pin_lsn: <replica_applied_lsn>
}
```

Replica receives:
- WAL replay entries [start_lsn .. target_lsn] (line 1)
- Live WAL entries [target_lsn+1 ..] (line 2)

No bitmap needed because WAL entries are ordered by LSN — no LBA conflict
between replay and live.

Pin advances as replay cursor moves forward, releasing old WAL entries.

## Future: Range Bitmap / Delta Rebuild

If primary maintains persistent per-checkpoint dirty block tracking:

```
modified_blocks[checkpoint_lsn_range] → set of dirty LBAs
```

Then rebuild can skip copying blocks that haven't changed since the
replica's last known position. Only modified blocks need to be sent.

This turns rebuild from O(volume_size) to O(changed_blocks), similar to
VMware CBTT (Changed Block Tracking) or DRBD activity log.

Implementation: persist DirtyMap snapshot at each flusher checkpoint along
with the checkpoint LSN. On rebuild, union of DirtyMap snapshots from
`replica_applied_lsn` to `current_checkpoint` = minimal copy set.

## Future: Resumable Rebuild

Current V2/MVP does **not** claim resumable rebuild. After crash during rebuild,
the protocol restarts from fresh `sync` and a fresh rebuild session.

A future resumable rebuild path may be added only with explicit durable state:

1. durable `base_progress`
2. durable proof of replica-side applied WAL coverage
3. primary-side proof that historical change coverage remains reconstructible
   through retained WAL or primary-owned CBT / changed-block history
4. a live delta channel for writes that arrive after resume begins

Without those conditions, resume is not a safe current claim.

## Component Test Requirements

All tests use real BlockVol with real WAL and extent data.
No mocks for storage. Network can be in-process (localhost TCP or direct
function call).

### Protocol tests (unit, engine only)

1. `syncAck` returns facts only, never action
2. `sessionControl(start_rebuild)` rejected on epoch mismatch
3. New `session_id` supersedes old session
4. Primary decision: `applied >= wal_tail` → catch-up
5. Primary decision: `applied < wal_tail` → rebuild
6. Primary decision: `applied >= wal_head` → keepup
7. Session failure does not auto-escalate to needs_rebuild
8. Session failure then fresh syncAck → primary re-decides

### Rebuild correctness (component, real BlockVol)

These tests create real primary + replica volumes with actual WAL and
extent data:

1. **Two-line convergence**: primary writes N blocks, records a flushed
   `base_lsn`, starts rebuild session. Base lane copies extent data from that
   flushed boundary. WAL lane
   ships live entries. Verify replica has all N blocks correct at end.

2. **WAL wins over base**: primary writes block A=1, flushes base, then
   writes A=2. Rebuild sends base (A=1) and WAL (A=2). Verify replica
   has A=2 (WAL-applied wins).

3. **Bitmap on applied not received**: ship WAL entry but delay local
   apply. Send base block for same LBA. Base block should land (bitmap
   not set yet). Then apply WAL entry. Bitmap now set. Final state
   should be WAL data.

4. **Large rebuild**: 1000+ blocks, concurrent base + WAL, verify all
   blocks correct at completion.

5. **Writes during rebuild**: primary continues writing new blocks during
   rebuild. Verify new blocks reach replica via WAL lane and are
   preserved at rebuild completion.

### Crash / failure (component, real BlockVol)

1. **Crash after WAL receive before apply**: restart replica, verify
   base can still cover the LBA (bitmap was not set).

2. **Crash after WAL apply**: restart replica, verify local WAL replay
   recovers the applied data correctly.

3. **Transport lost during rebuild**: session reports `failed(transport_lost)`.
   Primary re-decides from fresh syncAck. New rebuild session starts.

4. **Rebuild completion does not restore quorum until primary accepts**:
   verify replica reports `completed` but mode stays `needs_rebuild` until
   primary processes the completion event.

### Session lifecycle (unit, engine only)

1. `idle → accepted → running → completed → keepup`
2. `idle → accepted → running → failed → (syncAck) → re-decide`
3. `running → cancel → idle`
4. Session supersede: new session_id replaces old

### End-to-end (integration, hardware — after component tests pass)

1. Fresh replica join on m01/m02: create RF=2, kill replica VS, restart,
   verify rebuild completes and volume returns to `publish_healthy`.

2. Sustained I/O during rebuild: fio running on primary while rebuild
   progresses. Verify data continuity after rebuild completion.

## Implementation Order

1. Protocol engine (`sw-block/protocol/`) — already started, 7 events, 398 lines
2. Rebuild session on blockvol layer — two-line model, bitmap, completion
3. Session control wiring on volume server — sessionControl/sessionAck
4. Flushed extent base transfer — extent read + chunk send
5. Component tests against real BlockVol
6. Integration test on hardware
