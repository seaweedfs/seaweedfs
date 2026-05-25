# Distributed FUSE POSIX locks: ownership migration and owner restart

## Problem

Under `-dlm`, POSIX advisory locks (flock/fcntl) are held in an in-memory
`posixlock.Manager` on the inode's *owner filer*. Ownership is
`LockRing.GetPrimary(key)` (key = `HardLinkId` for a hardlinked inode, else the
path). The mount routes lock RPCs to its filer, which forwards one hop
(`is_moved`) to the owner. Lock state is deliberately kept off the meta-log — it
is transient coordination.

This reuses the ring for **routing only**. It does not use the DLM table's
handoff machinery (`GetPrimaryAndBackup`, backup replication,
`PromoteLock`/`DemoteLock`, `movedTo`, the cooling-off snapshot history). So when
a key's owner changes, or an owner restarts, the in-memory lock state is lost or
stranded and cross-mount mutual exclusion breaks.

## Failure modes

### Ring change (filer join/leave)

Key `K` moves from filer A to filer B:

- `K`'s lock `Set` stays in A's memory; B (new primary) has nothing for it.
- New `TryLock`/`GetLk` for `K` route to B → B grants blind to A's holders →
  mutual exclusion lost for `K`.
- Reaping does not fix it. `Renew(sid)` updates a per-filer, per-session
  `lastSeen`. While the mount still renews any key A owns, A keeps `sid` alive
  and never reaps the orphaned `K`. Only if the mount held `K` alone does A's TTL
  eventually expire it.

### Owner restart

The owner's in-memory `Manager` is wiped, so it comes back blind to every lock it
held — same double-grant. This is likely the more common trigger than a
membership change.

## Non-goal: reuse the DLM table's replication

The DLM solves this for its own locks (primary+backup replication, seq-ordered
ops, promote-on-takeover, cooling-off snapshots). Reusing it would re-couple
`posixlock` to the DLM internals and require Set-aware replication — the posix
`Set` is richer than the DLM's single-token lock. The direction of travel is to
reduce DLM coupling, so this is rejected.

## Design

Treat the Manager as reconstructable soft state owned by clients, plus a
transition guard.

### A. Client re-assertion (rebuild)

The mount is the source of truth: `posixKeep` already tracks every key it holds.
Extend each tracked hold to carry its range, and change the keepalive
(`loopRenewPosixLeases`) from `KEEP_ALIVE {Sid}` to a re-register that carries the
held ranges per key. The current owner reconstructs/refreshes its `Set` from the
assertion. A ring change or owner restart self-heals on the next keepalive tick:
the new or restarted owner rebuilds from its clients.

- Re-register is idempotent: it sets (not appends) the `(sid, owner)` ranges for
  the key.
- Reaping is unchanged: a dead mount stops re-asserting → the owner reaps after
  TTL.
- Re-assertion only ever (re)installs the asserting mount's own `(sid, owner)`
  locks; it never overrides another mount's. Conflicts between two mounts during
  the window are handled by C.

### C. Cooling-off dual-read (transition guard)

Re-assertion alone leaves a window (≤ one keepalive interval) where the new owner
is missing locks and could grant a conflict. Reuse the ring's existing snapshot
history: within the cooling interval after a ring change, a grant on the new
owner first checks the *previous*-snapshot owner for a conflict (a `GetLk` to the
old owner) and refuses if it conflicts. After the cooling interval — by which
point re-assertion has rebuilt state — the new owner is authoritative alone.

- Reuses `LockRing.GetSnapshot` / `GetPrimaryAndBackup` and the snapshot cooling
  already on `LockRing`, which the posix path does not currently consult.
- Owner restart: cooling-off does not help (the old state is gone, not on another
  node). Re-assertion is the only recovery there; the window is the keepalive
  interval (see phase 3 to tighten it).

## Phasing

1. **Re-assertion.** `posixKeep` tracks ranges; keepalive carries them; the filer
   reconstructs its `Set` from the assertion. Handles owner restart and
   (eventually-consistent) ring change. Window = keepalive interval.
2. **Cooling-off dual-read** on grant during ring transitions, reusing the
   snapshot history. Closes the ring-change grant race.
3. *(Optional)* Tighten the restart window: have a freshly started/promoted owner
   refuse grants until a re-assertion sweep settles, or carry an owner generation
   so clients re-assert immediately on a generation change rather than waiting for
   the next tick.

## Tradeoffs / open questions

- Keepalive payload grows with the number of held locks (already dispatched
  through the bounded worker pool).
- Re-assertion makes the filer's `Set` eventually consistent with clients —
  acceptable for advisory (cooperative) locks.
- Window vs TTL: keepalive interval (5s) and `posixLockSessionTTL` (15s) must be
  chosen so a full re-assertion sweep completes well within the TTL.
