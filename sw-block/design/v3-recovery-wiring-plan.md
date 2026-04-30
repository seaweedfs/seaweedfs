# Recovery wiring plan — `core/recovery` into `core/transport` + cmd

**Status**: design draft, not yet implemented. Targets architect priority **wiring + 2.5** (the natural next milestone after `g7-redo/pin-floor`).
**Branch target**: `g7-redo/wiring`, stacked on `g7-redo/pin-floor`.
**Depends on**: `g7-redo/dual-lane-recovery-poc` + `g7-redo/pin-floor` merged or reviewed first; this PR is meaningless without them.

This doc fixes the strategy for moving the dual-lane recovery package from a self-contained POC into the production daemon path, alongside (NOT replacing) the existing single-lane `core/transport` path. Architect ruling: "parallel + flag, don't replace existing path until one happy path runs green alongside".

---

## 1. Why parallel-flag, not in-place replacement

The existing single-lane path (`core/transport/rebuild_sender.go::doRebuild` + `replica.go::handleConn` MsgRebuildBlock/MsgRebuildDone handlers) is what hardware integration tests (G6 §close, prior G5-5* milestones) currently exercise. A direct in-place replacement of `doRebuild` carries the risk of breaking those without giving QA a deterministic A/B comparison.

Plan: introduce a flag (config + cmd-line + env) that selects between:

- `--recovery-mode=legacy` (default) — existing single-lane path, no new code on hot path.
- `--recovery-mode=dual-lane` — the new `core/recovery` package drives rebuild sessions.

Both paths share the same `BlockExecutor` outer surface (`StartRebuild` / `StartRecoverySession` callers do not care which is used). Wire-format compatibility is NOT required between paths — the dual-lane mode opens its own conn / port (TBD; see §4 below), the legacy mode keeps its existing one.

Until a green hardware run on `--recovery-mode=dual-lane` is observed, the default stays `legacy`. Removal of the legacy path is a separate PR after at least one milestone of dual-lane GREEN observability.

---

## 2. Surface inventory — files that need changes

### Sender side (primary)

| File | Change | Notes |
|---|---|---|
| `core/transport/rebuild_sender.go::StartRebuild` | Branch on mode flag. Legacy path unchanged. Dual-lane path delegates to `recovery.PrimaryBridge.StartRebuildSession`. | Outer signature unchanged so all upstream callers (`core/host/volume/peer_command_executor.go`, `core/transport/recovery_session.go::StartRecoverySession`'s `full_extent` branch, etc.) continue working. |
| `core/transport/recovery_session.go::StartRecoverySession` | The `full_extent` branch (line 117 today) calls `StartRebuild`. No change needed if `StartRebuild` itself branches; the dispatch already lives one level up. | Lowest-touch option. |
| `core/transport/executor.go::BlockExecutor` struct | Add optional fields: `dualLaneCoordinator *recovery.PeerShipCoordinator`, `dualLaneBridge *recovery.PrimaryBridge`. Nil ⇒ legacy mode. | Constructor variant `NewBlockExecutorWithDualLane(...)` initializes these. Existing `NewBlockExecutor` stays legacy. |

### Receiver side (replica)

| File | Change | Notes |
|---|---|---|
| `core/transport/replica.go::ReplicaListener` | Two listener configurations: legacy listens on existing port for old MsgRebuildBlock dispatch; dual-lane listens on a separate port (or same port + frame-type discriminator). Decision in §4. | Most invasive surface. |
| `core/transport/replica.go::handleConn` | Branch on first frame: if it's `recovery.frameSessionStart` (0x01), hand off to a `recovery.Receiver` for the rest of the conn lifetime. Otherwise use existing dispatch. | Type byte 0x01 in the new wire vs 0x01 (`MsgShipEntry`) in the old wire — collision; see §4. |
| `core/transport/replica.go::liveShipTargetLSNSentinel` + the lane-shim block | Document as deprecated when dual-lane is active (rebuild lineage no longer rejects live ship — that's the whole point of dual-lane). For legacy mode, keep working. | Removal in a later PR after dual-lane is default. |

### cmd / daemon

| File | Change |
|---|---|
| `cmd/blockvolume/main.go` (or wherever the executor is constructed) | Add `--recovery-mode` flag. Legacy: build `NewBlockExecutor(...)`. Dual-lane: build coordinator + bridge + `NewBlockExecutorWithDualLane(...)`. |
| `cmd/blockvolume/main.go` | Add `--recovery-pin-floor-recycle` flag (default off) gating priority 2.5 hookup (see §6). |

### Replication wiring

| File | Change |
|---|---|
| `core/replication/component/cluster.go` | If integration tests construct executors directly, propagate the mode flag through the test fixture. Otherwise unchanged. |
| `core/host/volume/peer_command_executor.go` | No change — wraps `BlockExecutor`'s `StartRebuild` which itself branches. |

---

## 3. Lifecycle alignment (architect's "不能做第二套相位机")

**Rule**: there must be exactly one source of truth for "is replica P in a rebuild session right now" — the `recovery.PeerShipCoordinator`. The legacy `BlockExecutor.activeSession` map is internal to the legacy path; the dual-lane path consults `coordinator.Phase()`. Mode is exclusive — at any moment only one tracker is authoritative for a given replica.

**StartSession ↔ EndSession**:

- Legacy mode: `BlockExecutor.registerSession` / `finishSession` pair (existing).
- Dual-lane mode: `coord.StartSession` / `coord.EndSession` pair (already implemented in `core/recovery`). The `PrimaryBridge.StartRebuildSession` calls `StartSession` synchronously before returning to its caller; the goroutine spawned underneath handles `EndSession` via `Sender.Run`'s defer.

**OnSessionStart / OnSessionClose callbacks** (`adapter.CommandExecutor` surface):

- Legacy: `BlockExecutor.SetOnSessionStart / SetOnSessionClose` already wired.
- Dual-lane: `PrimaryBridge` accepts the same callback shape; `BlockExecutor` constructor in dual-lane mode forwards its `onStart / onClose` down to `PrimaryBridge`. Caller (`HealthyPathExecutor` etc.) cannot tell which path fired the callback — by design.

---

## 4. Wire-format collision

**Problem**: legacy `MsgShipEntry = 0x01`, dual-lane `frameSessionStart = 0x01`. If both paths share a port, the receiver cannot dispatch on first byte alone.

**Three resolution options**:

### Option A — separate port per mode

`--rebuild-listen=:9221` (legacy) and `--recovery-listen=:9222` (dual-lane). Daemon binds whichever the mode flag selects. **Pro**: zero protocol coupling; **con**: cmd flag surface grows; integration tests need to know which port.

### Option B — wire-version handshake on connect

First byte after dial is a wire-version byte (0x00 = legacy, 0x01 = dual-lane). Subsequent bytes follow the corresponding protocol. **Pro**: single port, cleanly versioned; **con**: existing legacy hardware tests don't send a version byte — backward-compat shim required.

### Option C — re-number dual-lane frames to avoid 0x01

Move `frameSessionStart` to e.g. `0x10`, leaving `0x01..0x07` for legacy. **Pro**: same port, no handshake, cheap; **con**: special-cases the frame-type table; collision-free zone shrinks for future frames.

**Recommend Option A** for this milestone. Cleanest separation, least risk to the legacy hardware path. Reverts to single-port multiplexing later if/when legacy is removed.

---

## 5. Coordinator instance scope

One `PeerShipCoordinator` per **volume**, NOT per **executor or per replica**. The coordinator's `MinPinAcrossActiveSessions` is meaningful only when it sees ALL replicas of a single volume. Per-replica coordinators would each report their own pin floor with no minimum.

**Wiring**: cmd constructs a coordinator at volume open; passes it to all per-replica `BlockExecutor` constructors for that volume. The daemon owns the coordinator lifecycle.

This is also why the WAL recycle hookup (§6 below) lives at the volume level, not the executor level.

---

## 6. Priority 2.5 — WAL recycle path consumes `MinPinAcrossActiveSessions()`

**Problem**: today `core/storage/walstore.go`'s flusher / recycle path advances `walTail` based on its own retention policy + checkpoint. It does NOT consult any per-peer pin floor. Until the recycle path knows about active sessions' commitments, `pin_floor` advancement is a paper exercise.

**Hookup point**: `walstore.go::flusher.advanceTail` (or wherever the flusher decides what to recycle). New gate:

```go
recycleFloor := computeRetentionFloor()  // existing logic
if pinSrc != nil {
    if pinned, anyActive := pinSrc.MinPinAcrossActiveSessions(); anyActive {
        if pinned < recycleFloor {
            recycleFloor = pinned  // pin holds back recycle
        }
    }
}
// trim WAL to recycleFloor
```

**Interface seam**: `walstore.WALStore` does not know about `core/recovery` (and shouldn't — substrate stays orthogonal to engine). Solution: a small interface in `core/storage`:

```go
type RecycleFloorSource interface {
    MinPinAcrossActiveSessions() (floor uint64, anyActive bool)
}

func (s *WALStore) SetRecycleFloorSource(src RecycleFloorSource)
```

`recovery.PeerShipCoordinator` already implements this signature exactly. cmd wires the coordinator into the volume's WAL store after both are constructed. Default: nil source (legacy behavior).

**Test**: a new `walstore_recycle_pin_test.go` confirms recycle floor is gated by min pin when source is set; falls back to existing logic when nil. No e2e change required — the unit test is sufficient because the coordinator's contract is already covered.

**Architect priority 2.5 closes when this hookup lands** AND a brief test confirms the integration works end-to-end (set pin floor on coord, observe recycle path respect it).

---

## 7. Test plan

Existing tests must continue to pass on `--recovery-mode=legacy` (default). The wiring PR adds:

| Test | Scope |
|---|---|
| `core/transport/rebuild_sender_dual_lane_test.go` | Dual-lane mode end-to-end with the existing `BlockExecutor` shape: same OnSessionStart/Close callbacks fire, achievedLSN equals primary H. |
| `core/transport/replica_dual_lane_test.go` | Receiver-side dispatch for `frameSessionStart` correctly hands off to a `recovery.Receiver`. |
| `core/storage/walstore_recycle_pin_test.go` | §6 — recycle path consults `MinPinAcrossActiveSessions`. |
| Existing `TestE2E_*` in `core/recovery/` | UNCHANGED — they use `core/recovery` directly, not `core/transport`. Wiring PR doesn't touch them. |

Hardware acceptance (G7 §2 #2 / #5 / #6): NOT a gate for this PR. Architect ruling: "validate happy-path one round green alongside legacy first, then enable as default in a separate PR".

---

## 8. Rollback / removal plan

If something is critically wrong with the dual-lane path post-merge:

- **Default flip back**: change `--recovery-mode` default in cmd from `legacy` to `dual-lane` (or vice versa) is a single-line revert.
- **Code removal**: requires reverting the wiring PR. Because `core/recovery` is independent of `core/transport`, the package itself can stay even if the integration is rolled back.
- **legacy removal**: a future PR removes `core/transport/rebuild_sender.go::doRebuild` + the old wire frames. Should NOT be in this PR. Track as `g7-redo/legacy-removal`.

---

## 9. Open questions for architect — Resolved (architect ACK 2026-04-29)

1. **Wire collision** (§4): **RESOLVED — Option A (separate ports)**. Daemon binds a second listen address; `--recovery-mode` flag pairs with it. Firewall / Helm registration must follow. Fallback to Option B (version handshake) is permitted only if a platform constraint requires single-port. Option C (re-number frames) is rejected as a long-term solution.
2. **2.5 split**: **RESOLVED — split into `g7-redo/recycle-pin-hookup`**. Wiring PR stays focused on transport + cmd + lifecycle. Recycle hookup lands immediately after wiring merges, NOT held for a long gap (otherwise pin advances in the air while flusher recycles by old policy).
3. **Default mode flip timing**: **RESOLVED — stays `legacy`** until CI + hardware-targeted dual-lane scenarios run GREEN, then a separate single-line PR flips the default. Default flip is a release / ops event, not a wiring implementation detail.
4. **Legacy removal timing**: **RESOLVED — after default flip + at least one release cycle (canary / N weeks)**, NOT just calendar time. Pre-removal checklist: no-flag run green, monitoring / rollback documented. Tracked as `g7-redo/legacy-removal`.

---

## 10. Out of scope (explicit)

- `§3.2 #3 single-queue real-time interleave` (architect priority #2). Wiring PR ships sequential phases, same as POC.
- `INV-REPL-OVERLAP-HISTORY-NO-REGRESS` bitmap-as-WAL-claim refinement. Separate PR after apply-gate redesign.
- Sparse base + substrate basement-clearing.
- Layer 3 retry budget at engine level (architect priority #3 [retry]). Architect: "可与 wiring 并行 ... 不同目录"; tracked as a separate concurrent PR.
- Removing `core/transport/replica.go::liveShipTargetLSNSentinel` and the lane-shim. Lives until legacy is removed.
