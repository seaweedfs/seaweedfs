# Recovery — `BaseBatchAck` wire + `pin_floor` advancement (design draft)

**Status**: design draft, not yet implemented. Targets architect priority **#3** in the G7-redo backlog.
**Branch target**: `g7-redo/pin-floor` (or extend `g7-redo/dual-lane-recovery-poc` if QA / architect prefer one PR).

This doc fixes the protocol surface for receiver→primary acknowledgement so `coord.SetPinFloor` has a real driver. Closes the two forward-direction INVs in `v3-recovery-inv-test-map.md` Layer-2 section: `INV-PIN-ADVANCES-ONLY-ON-REPLICA-ACK` and `INV-PIN-COMPATIBLE-WITH-RETENTION`.

Implementation is intentionally NOT in this doc — only the contract, the cadence, the failure surface, and the test plan.

---

## 1. Why this exists

Today (`g7-redo/dual-lane-recovery-poc`):

- Coordinator already exposes `SetPinFloor(replicaID, lsn) error` and `MinPinAcrossActiveSessions() (uint64, bool)` — see `core/recovery/peer_ship_coordinator.go`. They are testable but never called by anything: the wire has no path from receiver back to primary that delivers ack progress.
- `pin_floor` is therefore frozen at `fromLSN` for the entire session lifetime; recycle is gated indefinitely.

Without `pin_floor` advancement, two things break under sustained writes / multi-replica:

1. Primary's WAL retention sits at the lowest active session's `fromLSN` until session ends — recycle stalls.
2. Multi-replica `min(pin_floor)` cannot reflect heterogeneous progress; one slow replica blocks WAL recycle for everyone (`INV-RECYCLE-GATED-BY-MIN-ACTIVE-PIN`).

`#3` adds the incremental ack so primary's recycle floor catches up with the slowest replica's actually-durable frontier.

---

## 2. Wire frame

**Decision: new frame type, NOT piggyback.**

| Option | Choice | Why |
|---|---|---|
| New `frameBaseBatchAck` (receiver → primary) | ✅ chosen | Distinct cadence (incremental during base lane, NOT tied to barrier-end). Wire trace audit is unambiguous. |
| Piggyback on `frameBarrierResp` | rejected | Barrier fires once at session end. Pin only advances at session close — useless for retention recycle during a long base lane. |
| Piggyback on next inbound frame from primary | rejected | Receiver may not have an inbound to piggyback on (primary is also blocked-on-write during base lane). |

### Frame layout

```
frameBaseBatchAck = 0x07         // next available type after BarrierResp(0x06)

payload (20 bytes):
  [8] SessionID         uint64 BE
  [8] AcknowledgedLSN   uint64 BE   // receiver's durable frontier as of this ack
  [4] BaseLBAUpper      uint32 BE   // LBA prefix [0, BaseLBAUpper) durably installed; 0 if no base progress yet
```

`AcknowledgedLSN` is the load-bearing field; `BaseLBAUpper` is advisory (sender uses it for retransmit logic, NOT for pin floor).

### Lineage / session binding

`SessionID` in the payload identifies which session this ack belongs to. Receiver sends only on the same conn that it accepted SessionStart on (no fan-out, no cross-conn acks). Primary's sender validates `SessionID` matches its current session before calling `coord.SetPinFloor`; mismatched session → `FailureProtocol`.

Lineage's `Epoch` and `EndpointVersion` are NOT in the ack payload — the conn already implies them (same conn established the session). Saves 16 bytes per ack and removes a redundant validation step.

---

## 3. Cadence — when does receiver send `BaseBatchAck`?

Receiver-side rule (per session):

```
After every K base-lane blocks applied  → enqueue ack
After every T milliseconds elapsed     → enqueue ack (whichever first)
On MarkBaseComplete                    → MUST send a final ack
On TryComplete returns done            → MUST send a final ack just before BarrierResp
```

POC defaults:
- `K = 256` blocks (~1 MiB at 4 KiB blocks)
- `T = 100ms`

Both are tunable via `Receiver` constructor params (test override) or session config (production). The cadence is the receiver's call — sender MUST tolerate any cadence including "zero acks until end" (legacy/fallback path).

`AcknowledgedLSN` for each ack:
- During base lane: `min(walApplied, syncedLSN-at-ack-time)`.
- After `MarkBaseComplete`: substrate `Sync()` → use returned frontier.
- Final ack at `TryComplete`: same as barrier — receiver's syncedLSN after final Sync.

---

## 4. `SetPinFloor` semantics

Coordinator-side rule (already in code, just unused):

```
SetPinFloor(replicaID, lsn):
  if no active session for replicaID → error (Idle peer)
  if lsn ≤ current pinFloor          → silently ignored (monotonic)
  else                                → pinFloor = lsn
```

What `pinFloor = X` means in the spec:

> Primary commits: replica has durably installed everything with LSN ≤ X. Primary MAY safely recycle WAL up to X for THIS replica's account.

What it does NOT mean:
- Does NOT mean primary's WAL has actually been recycled (the recycle path consults `MinPinAcrossActiveSessions()` plus retention policy independently).
- Does NOT mean replica claims InSync — that requires barrier-ack, not incremental BaseBatchAck.

---

## 5. Retention inequality (closes `INV-PIN-COMPATIBLE-WITH-RETENTION`)

At any moment during an active session for replica P:

```
pin_floor(P)  ≥  S_primary                                         // (1)
pin_floor(P)  ≤  walApplied(P)  ≤  H_primary                       // (2)
```

Where:
- `S_primary` = primary's WAL retain start (`storage.LogicalStorage.Boundaries()` returns S).
- `H_primary` = primary's WAL head.
- `walApplied(P)` = receiver's highest applied WAL LSN.

**Violation handling**:

- **(1) violated** (`pin_floor < S_primary`): primary has already recycled past where this session committed. This is catastrophic: the session contract is unrecoverable on this primary. Action: invalidate session → `Failure(SingleFlight, ?)` or new typed kind `FailurePinUnderRetention` (see §7 below) → engine starts new lineage with fresher fromLSN.
- **(2) violated** (`pin_floor > walApplied`): receiver lied about its frontier OR coord computed wrongly. Treat as `Failure(Contract, …)`.

The check happens at `coord.SetPinFloor`:

```go
func (c *PeerShipCoordinator) SetPinFloor(id ReplicaID, floor uint64) error {
    // existing monotonic guard
    if floor <= existing { return nil }
    // NEW: retention compatibility — needs primary's S boundary as input
    if floor < primaryRetainStart {
        return &FailureSingleFlight{...}  // or a new kind, see §7
    }
    // existing update
}
```

Open question for review: should `SetPinFloor` take `primaryRetainStart` as a parameter, or should the coordinator hold a callback that fetches it from the substrate? Latter is cleaner but introduces a dependency direction. **Recommend**: parameter form, caller (sender) fetches `primary.Boundaries()` before calling `SetPinFloor`. Substrate call is cheap; coordinator stays substrate-free.

---

## 6. Failure surface — extends `core/recovery/failure.go`

### Reuse of existing kinds

| Wire / coord event | `FailureKind` | Phase | Retryable |
|---|---|---|---|
| Decode error on `frameBaseBatchAck` payload | `FailureProtocol` | `recv-dispatch` | no |
| `SessionID` in ack ≠ active session | `FailureProtocol` | `recv-dispatch` | no |
| `SetPinFloor` error: `floor > walApplied` (impossible without bug) | `FailureContract` | `pin-update` (new Phase) | yes (re-probe) |
| `SetPinFloor` error: `floor < S_primary` (recycle past commitment) | new kind `FailurePinUnderRetention` (§7) | `pin-update` | no — escalate to new lineage |
| Receiver wire write error sending ack | `FailureWire` | `recv-ack-write` (new Phase) | yes |

### New `FailureKind`?

`FailurePinUnderRetention` is semantically distinct from `FailureWALRecycled`:
- `WALRecycled` = sender's `ScanLBAs(fromLSN)` returned recycled error → couldn't even start streaming.
- `PinUnderRetention` = mid-session, primary's S advanced past replica's pin floor → session must invalidate even though it was streaming fine.

**Recommend**: add the new kind. Mapping at engine boundary still feeds `RebuildPinned` for both (rebuild from a fresher anchor).

---

## 7. Code surface (sender / receiver / coordinator changes)

Only sketched; no implementation in this doc.

```
core/recovery/wire.go
  + frameBaseBatchAck = 0x07
  + encodeBaseBatchAck / decodeBaseBatchAck

core/recovery/receiver.go
  + cadence config (K blocks, T duration; defaults 256/100ms)
  + ack goroutine OR inline ack-send after each Apply* if cadence reached
  + final ack on MarkBaseComplete + barrier path

core/recovery/sender.go
  + new goroutine: read frames from conn, multiplex BarrierResp + BaseBatchAck
    (today sender only reads BarrierResp at end — needs a frame demux loop)
  + on BaseBatchAck: validate sessionID, fetch primary.Boundaries().S, call coord.SetPinFloor
  + new Phase tag: PhasePinUpdate

core/recovery/peer_ship_coordinator.go
  + SetPinFloor(id, floor uint64, primarySBoundary uint64) error
  + new return: FailurePinUnderRetention (or whatever name lands)

core/recovery/failure.go
  + FailurePinUnderRetention (if new kind chosen)
  + PhasePinUpdate, PhaseRecvAckWrite
```

The frame demux loop on the sender side is the largest delta — today the sender only reads ONE inbound frame (BarrierResp at end). Switching to a demux pattern with a goroutine + channel is straightforward but worth flagging.

---

## 8. Test plan

In `core/recovery/`:

| Test | Pins |
|---|---|
| `TestWire_BaseBatchAck_RoundTrip` | encode/decode wire format |
| `TestCoordinator_SetPinFloor_RejectsBelowRetention` | INV-PIN-COMPATIBLE-WITH-RETENTION (new) |
| `TestCoordinator_SetPinFloor_RejectsAboveWalApplied` | inequality (2) — Contract failure |
| `TestE2E_PinFloorAdvancesIncrementally` | INV-PIN-ADVANCES-ONLY-ON-REPLICA-ACK — drive 1000 LBAs, observe pinFloor advance in a series of steps via Status snapshots |
| `TestE2E_AckCadence_K_Triggered` | every K blocks → one ack |
| `TestE2E_AckCadence_T_Triggered` | bursty workload + idle → time-based ack |
| `TestE2E_NoAckUntilEnd_StillCorrect` | receiver sends only the mandatory final ack; pinFloor advances exactly once at session end (legacy/fallback path) |
| `TestIntegrationStub_FailurePinUnderRetention` | typed `*Failure` surface for the new kind |

Test count expected: ~8 new, all in `core/recovery/`. No new substrate work; `MemoryWAL` already exposes `Boundaries().S` in V2-faithful form.

---

## 9. Cross-references

After this lands, update:

- `core/recovery/doc.go` — pin INV section: replace `_Pending #3_` markers with `see v3-recovery-pin-floor-wire.md §N`.
- `v3-recovery-inv-test-map.md` — Layer-2 forward rows for the two pin INVs: move from "forward" to "pinned" with the new test cites.
- `v3-recovery-inv-test-map.md` — Layer-3 taxonomy section: add row for `FailurePinUnderRetention` if that kind lands.

---

## 10. Out of scope for this milestone

- **§3.2 #3 single-queue real-time interleave** (architect priority #2). Pin advancement does NOT depend on it.
- **Wiring into `core/transport`**. Pin advancement lives entirely in `core/recovery/`'s self-contained POC; the wiring PR consumes it later.
- **Multi-replica fanout in primary**. `MinPinAcrossActiveSessions` is already implemented and tested; no new code there for this milestone.
- **Retention policy at WAL recycle**. The recycle path in `core/storage/walstore.go` is unchanged. A separate doc / PR will wire `MinPinAcrossActiveSessions()` into the recycle decision.

---

## 11. Open questions for architect — Resolved (architect ACK 2026-04-29)

1. **`FailurePinUnderRetention` as a new kind** vs reusing `FailureWALRecycled`. **RESOLVED: new kind.** Rationale: `WALRecycled` is a cold-start scan failure (typical fix: bump pin / new lineage); `PinUnderRetention` is a mid-session contract violation (typical fix: invalidate session, log Invariant breach). Different `Retryable()` defaults, different operator response. Implementation MUST update `failure.go` matrix + `doc.go` INV ledger + `recovery-inv-test-map.md` row in lockstep — no verbal-only distinction.
2. **`SetPinFloor` signature**: parameter vs callback. **RESOLVED: explicit parameter.** Caller computes floor from `(S, H, AckLSN)` and calls `SetPinFloor(floor)`. Coordinator stays substrate-free. If a unified entry is needed later, it should be a value type (`PinFloorInput struct`), NOT a `func() uint64` callback.
3. **Wire compat**: independent frame `0x07` vs piggyback on `MsgShipEntry`. **RESOLVED: keep independent frame for this milestone.** Ack / data split → packet-capture, log, review all simpler; control-plane (receiver→primary ack) decoupled from data-plane (rate, retry). Piggyback / merge is a later bandwidth optimization — does not gate #3 correctness.
