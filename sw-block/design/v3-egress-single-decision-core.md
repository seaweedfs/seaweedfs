# V3 Egress Components — Single Decision Core (Single-Queue) Principle

**Status**: architecture principle, ratified-pending; consensus candidate
**Anchors**: `v3-recovery-algorithm-consensus.md` §I P1 / §I P7 / §6.8 (mechanical SINGLE-SERIALIZER); `v3-recovery-wal-shipper-spec.md` §3 INV-SINGLE
**Hardware grounding**: `seaweed_block@bc4286e..c14e652` g7-dual-lane on m01/M02 (multiple side-door incidents diagnosed)

---

## §1 — The principle (one-liner for the consensus)

> **V3 egress components (per-peer shipper / session pump / flusher / barrier driver) MUST be modeled as a single decision core (equivalent: single serializable worker over a single ordered queue).** Monotonic pointers (cursor, applied, emit profile, conn binding) advance ONLY via the core's own deterministic transitions. External callers deliver commands/events; the core translates them into dequeue, mode switches, conn/frame-format selection. Direct external mutation of the core's internal state is a design-debt side door.

In project shorthand: **one brain + monotonic internals; outside only sends commands, never writes the entity's internal organs.**

## §2 — The shape, in three bullets

| Aspect | Required form |
|---|---|
| **Queue / scheduler** | One ordered intake. Whether physically a queue or a single goroutine driving a state machine, there is exactly one thing that decides "what's next" for this egress lane. No parallel decision-makers. |
| **Monotonic pointers** | `cursor`, `applied LSN`, `emit profile`, bound `conn`, `head observation` — all advance by deterministic transitions inside the core. External code never `swap conn`, `swap profile`, `set cursor`, etc. on its own. |
| **API to the outside** | Commands / events: `StartSession(...)`, `EndSession()`, `LiveWrite(lba, lsn, data)`, `Probe()`, `Abort()`. Callbacks for terminal observations. The core internally dispatches each command to the appropriate transition; callers do not pre-stage half the transition externally. |

If shipper and flusher share backlog semantics for the same peer, this collapses further: **one monotonic world-line** for that peer's egress. Otherwise "who flushes / ships / advances head first" is undefined under any number of mutexes — locks paper over the order, the language doesn't define it.

## §3 — Why it matters: hardware-grounded violations

Three V3 dual-lane incidents diagnosed on m01/M02 during 2026-05-02 each turn out to be the same shape: a side-door wrote internal state of the shipper / engine while a session-state-machine was the only thing entitled to change it.

### §3.1 — `executor.Ship` overwrites the WalShipper's emit context mid-session

`executor.Ship`, called for every steady-state replication write, unconditionally:
1. Calls `updateWalShipperEmitContext(replicaID, legacy_conn, steady_lineage, EmitProfileSteadyMsgShip)` — overwriting the dual-lane context the recovery session had pinned at `RecoverySink.StartSession`.
2. Calls `WalShipper.NotifyAppend(...)` — which then emits on the **legacy port**.

When a dual-lane rebuild is active, this means concurrent live writes during the session ship to the legacy port, never reach the rebuild session's receiver, and the base lane subsequently clobbers those LBAs in the extent. Hardware: `g7-20260502T013032Z` scenario #5 — 498 mismatches at the concurrent-write LBA range (zero-hash on replica → those writes never landed).

**Side-door diagnosis**: `Ship()` is an external entry that directly writes the shipper's `emitConn` / `emitProfile` (its `bound conn / profile` pointers). The shipper's session-state-machine was not consulted. The session's "I am bracketed in dual-lane mode" invariant got silently invalidated by an unrelated caller.

**Per the principle**: `Ship()` should be a command (`LiveWrite(replicaID, lba, lsn, data)`). The shipper's core inspects its own state — *am I in a session right now?* — and routes accordingly: dual-lane sink during session, steady emit otherwise. The caller does not get to choose.

The current temporary patch (gate `Ship()` on `bridge.HasActiveSession(replicaID)` and re-route through `PushLiveWrite`) is a workaround for the same root cause: callers are still pre-deciding routing. The principled fix folds both paths into one command into one core.

### §3.2 — The bridge `onStart` / `onClose` lambdas dropping `ReplicaID`

`PrimaryBridge`'s session callbacks were constructed with `SessionStartResult{SessionID: sid}` — *missing the ReplicaID field*. The engine's `checkReplicaID` then dropped both events as `wrong_replica`, leaving `Session.Phase` stuck at `Starting` forever, `hasActiveSession` returning true permanently, and every subsequent `StartRebuild` dispatch silently suppressed.

**Side-door diagnosis**: the runtime peer state went `catching_up → healthy` (via `peer.SetState`, an external mutation), while the engine's session-state-machine was frozen at `Starting`. Two state machines for "is this replica done?" — the runtime one (peer) and the truth one (engine) — diverged because one had been written from the side and the other from a command that got dropped. Hardware: `g7-20260502T012553Z`.

**Per the principle**: peer-state should be a *projection* of engine truth, not an independent writable. Today `peer.SetState` is called from multiple places (transport callbacks, engine commands), and there is no single core deciding it.

### §3.3 — A-class coord-side `RecordBarrierWalLegOk` (now reverted)

The A-class wave I shipped at `a7eb135` made the sender call `coord.RecordBarrierWalLegOk(walLegOk)` between probe and BarrierReq emit, so `CanEmitSessionComplete` could read the witness as a conjunct. This was itself a side-door: the **sender** was writing into the **coord's** internal session state to satisfy a conjunct the coord would later read.

The reverted state (working tree as of 2026-05-02 evening) removes this — correctly, by this principle. The PrimaryWalLegOk witness belongs on the shipper's own observable surface (`ProbeBarrierEligibility`); the close-eligibility decision should consult that surface as a command result, not depend on prior side-channel writes to coord state.

This is the same anti-pattern as §3.1: state owned by entity A, written from entity B without A getting to refuse / sequence / validate. The principle calls for: A exposes a query (or a callback fires on the right transition), B consumes; B never writes A's internals.

## §4 — Honest delta with current implementation

| Component | Has a "core" | Decision-only API | Monotonic pointers internal-only |
|---|---|---|---|
| `core/transport/WalShipper` | yes (shipMu serialized) | NO — `Ship()` writes emit context from outside | NO — `cursor` advanced internally, but `emitConn`/`emitProfile` writeable from outside |
| `core/recovery/RebuildSession` | yes | partial — `ApplyBaseBlock` / `ApplyWALEntry` / `MarkBaseComplete` are commands | yes (bitmap, baseDone, walApplied internal) |
| `core/recovery/PeerShipCoordinator` | yes | partial — was being written from sender via `RecordBarrierWalLegOk` (now reverted) | yes post-revert |
| `core/replication/ReplicaPeer` (runtime peer state) | NO — `SetState` callable from multiple paths | NO — multiple writers | NO — projection diverged from engine truth |
| `core/engine` (per-replica engine state) | yes (event-sourced) | yes — events in, commands out | yes |

The pattern: the engine itself is the strongest decision core (event-sourced, commands-out). The further from the engine, the more side doors creep in.

## §5 — Implications for in-flight work

1. **`Ship()` → `LiveWrite()` collapse**: `executor.Ship` and `bridge.PushLiveWrite` should fold into a single command surface on a per-peer egress core. The core dispatches based on its own session state. The current `HasActiveSession` gate is a transitional patch until this collapse happens.
2. **Peer state machine ownership**: `ReplicaPeer.SetState` and `Invalidate` callable paths should reduce to one. Either the engine commands all transitions and the peer is a projection, or the peer owns its state and the engine reads it. Mixed writes are the §3.2 incident.
3. **A-class binding (when re-attempted)**: the witness should be a method on the shipper (e.g., `ShipperBarrierWitness()`), called by the close-decision site. No sender→coord side-write.
4. **Single-queue test discipline**: any new V3 production wiring that writes a monotonic pointer from outside an entity's core MUST cite this memo and justify why it is not a side door (typically: it cannot, and the wiring needs reshape).

## §6 — Anti-discipline notes

- This memo does NOT introduce a new wire field, predicate, or invariant. It pins an architecture principle that earlier docs (§I P1 single-decision-authority, §6.8 mechanical SINGLE-SERIALIZER, INV-SINGLE per-replica shipper) imply but don't articulate as one rule.
- The principle applies to V3 egress / per-peer pump components specifically. It does NOT mandate single-queue for the cluster-wide control plane (engine event sourcing already handles that).
- Side doors that exist today are design debt, not bugs in isolation. The bugs surface when a session-state invariant ("I am pinned to the dual-lane conn") meets a side-door write ("steady Ship just swapped your conn"). Identifying side doors is the lever for prioritizing collapse work.

## §7 — Receipts

- Side door §3.1 incident & temporary patch: `seaweed_block@(working tree)` — `core/transport/ship_sender.go` `HasActiveSession` gate; hardware `g7-20260502T013032Z` 498 LBA mismatches.
- Side door §3.2 incident & fix: `seaweed_block@c14e652` — bridge `onStart` / `onClose` ReplicaID propagation; hardware `g7-20260502T012553Z` `wrong_replica` trace.
- Side door §3.3 incident & revert: A-class wave `seaweed_block@a7eb135`; reverted in working tree 2026-05-02.
- Anchors in consensus: `v3-recovery-algorithm-consensus.md` §I P1 (one decision authority per session-axis), §I P7 (`fromLSN`/`pinLSN` parity — engine owns the number, transport mustn't silently overwrite), §6.8 mechanical SINGLE-SERIALIZER (one mutex per (volume, replicaID) entry serializes all conn writes).
- Companion clarification: `v3-rebuild-from-lsn-pin-clarification.md` (§I P7 sentinel translation rule for the rebuild path).
