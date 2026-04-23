# V3 Phase 15 — T4 (G5 Replicated Write Path) L1 Entity Survey

**Date**: 2026-04-22
**Status**: DRAFT — open for sw + architect + PM review; NOT yet signed
**Owner**: QA (initial draft) → sw review → architect + PM sign as part of T4 T-start three-sign
**Purpose**: L1 stateful-entity enumeration per `v3-phase-15-qa-system.md` §8C.8 top-down port discipline, effective T4+. This doc is the raw entity landscape BEFORE any L2 bridge verdicts are proposed. Review target: is the entity set complete and correctly scoped? L2 builds on this.

**Scope**: V2 replication code under `weed/storage/blockvol/` — the files listed in `v3-phase-15-mvp-scope-gates.md` §G5 "Port from V2" (wal_shipper / shipper_group / repl_proto / replica_apply / replica_barrier) plus the Phase 4A additions (dist_group_commit / promotion / rebuild / rebuild_session / rebuild_transport / rebuild_bitmap). No V3 comparison made here — that is L2's job.

---

## §1 Discipline note

Per QA system §8C.8 (rule added post-T3 retrospective):

> Prior tracks cut port scope bottom-up (file-by-file). This produced silent drift: BUG-001 (split mis-assigned concurrency), BUG-005 (merge lost lifetime contract), Addendum A (1:1 mis-classified V2 bug as feature). Root cause: L1 / L2 logical boundaries were never enumerated before L3 files were touched.

T4 is the first track where this discipline is enforced pre-sketch. L1 survey is this document; L2 bridge verdicts land in `v2-v3-contract-bridge-catalogue.md` §3 after L1 review closes; L3 file classification enters the T4 port plan sketch only after L1+L2 sign.

---

## §2 Entity survey

Ten V2 stateful entities identified (updated per architect F1 — RebuildBitmap split out from RebuildSession). For each: scope / lifecycle owner / concurrency model / cross-session behavior / authority coupling / network protocol surface / known tricky invariants.

### §2.1 WALShipper

| Attribute | Value |
|---|---|
| V2 file(s) | `wal_shipper.go` (lines 57–912) |
| Scope | per-replica (one shipper per replica in the fan-out set) |
| Lifecycle owner | Constructed by `ShipperGroup.New()` or host caller; destroyed via `Stop()` (line 522). Owned by `BlockVol.shipperGroup` (`blockvol.go` line 67); lifecycle tied to assignment via `HandleAssignment → promote/demote` (`promotion.go` lines 19–137). |
| Concurrency | **Up to 3 goroutines:** (1) caller of `Ship()` (write path); (2) caller of `Barrier()` (flusher); (3) internal reconnect/catch-up via `CatchUpTo()`. `mu` protects `dataConn`; `ctrlMu` protects `ctrlConn`; `atomic.Uint32` for state machine; `atomic.Uint64` for LSNs; `atomic.Bool` for `stopped` / `activeRebuildSession`. Callbacks (`onStateChange`, `onBarrierFailure`) fire synchronously. |
| Cross-session behavior | Does NOT survive disconnect. `ShippedLSN()` diagnostic only; `ReplicaFlushedLSN()` authoritative, updated only via barrier success. On reconnect, full `reconnectWithHandshake()` required. `replicaFlushedLSN` survives across demote/promote IF the shipper object is retained (typically recreated). |
| Authority coupling | `epochFn()` injected at construction; validated on every `Ship()` and barrier. Role-agnostic. No direct lease coupling. `liveShippingPolicy` callback gates write eligibility. Epoch mismatch triggers `ReplicaNeedsRebuild`; barrier epoch mismatch fails barrier. |
| Network protocol surface | **Two TCP channels.** Data: `MsgWALEntry` (0x01); on reconnect `MsgResumeShipReq` (0x03) / `MsgResumeShipResp` (0x04) / `MsgCatchupDone` (0x05). Control: `MsgBarrierReq` (0x01) / `MsgBarrierResp` (0x02). Degraded mode on write error; bootstrap from `ReplicaDisconnected` triggers `CatchUpTo()`. |
| Known tricky invariants | (1) Contiguous LSN via `StreamEntries()`; gaps detected by `ErrWALRecycled`. (2) Epoch== fencing drops stale entries silently. (3) `activeRebuildSession` flag prevents stale state transitions from opening live lane. (4) Barrier `BarrierOK` without `FlushedLSN` must NOT count toward `sync_all` (legacy replica safety). (5) `maxCatchupRetries=3`; then escalate to `ReplicaNeedsRebuild`. (6) `replicaFlushedLSN` uses `CompareAndSwap` for monotonic forward-only advance. |

### §2.2 ShipperGroup

| Attribute | Value |
|---|---|
| V2 file(s) | `shipper_group.go` (lines 19–408) |
| Scope | per-volume (one group per primary; spans N configured replicas) |
| External deps | **RF is externally supplied, not internally decided.** `N = RF` (Replication Factor) comes from master assignment via `BlockVol.SetReplicaAddrs([]string)` (see `blockvol.go`). Group resizing is an assignment-op (teardown + rebuild), never a shipper-internal decision. RF change semantics are thus a cross-entity contract: `master assignment ↔ ShipperGroup size ↔ ReplicaReceiver expected-connection-count ↔ DistGroupCommit quorum arithmetic`. Losing this alignment during L2 split silently breaks sync_quorum. |
| Lifecycle owner | Constructed via `NewShipperGroup()`; owned by `BlockVol.shipperGroup`. Torn down via `StopAll()` during demote. No reconstruction mid-assignment. |
| Concurrency | No internal goroutines. `BarrierAll()` spawns N goroutines (one per shipper) in parallel via `WaitGroup`. Accessors hold `RWMutex.RLock()`. |
| Cross-session behavior | Pure aggregation; no durable state. Individual shippers retain `replicaFlushedLSN` and state machine across group lifecycle. `MinReplicaFlushedLSN()` reflects live shipper states at query time. |
| Authority coupling | No direct epoch coupling at group level (each shipper holds its own `epochFn`). `EvaluateRetentionBudgets()` transitions shippers to `ReplicaNeedsRebuild` based on timeout/max-bytes. |
| Network protocol surface | Aggregator only; no direct network calls. Routes via shipper methods. Retention budget callbacks inspect shipper contact time and state. |
| Known tricky invariants | (1) `MinReplicaFlushedLSNAll()` returns (0, false) unless EVERY shipper reports progress; used to gate WAL reclaim (`blockvol.go` line 928). (2) Retention budget escalation can flip shipper state to `ReplicaNeedsRebuild` under RWMutex.RLock. (3) MinShipped (Ceph retention watermark) vs MinReplicaFlushed (authoritative durability) — two different watermarks for two different consumers. (4) `AnyHasFlushedProgress()` seeded on reassignment to detect cold-start shippers (per `design/sync-all-reconnect-protocol.md`). |

### §2.3 ReplicaReceiver

| Attribute | Value |
|---|---|
| V2 file(s) | `replica_apply.go` (lines 20–425) + `replica_barrier.go` (lines 10–204) |
| Scope | per-volume replica-side; singleton receiver for one primary's connection set |
| Lifecycle owner | `NewReplicaReceiver()` constructor; `Serve()` starts; `Stop()` tears down. Owned by replica-side `BlockVol`. Created/destroyed per assignment cycle. |
| Concurrency | Two accept loops (`acceptDataLoop`, `acceptCtrlLoop`) in dedicated goroutines; per-connection handlers (`handleDataConn`, `handleControlConn`) spawned per Accept. `mu` protects `receivedLSN` + `flushedLSN`; `cond = sync.NewCond(&mu)` signals barrier waiters. `connMu` protects `activeConns`. `stopCh` broadcasts to all goroutines. |
| Cross-session behavior | Persists across primary demote/promote cycles (same receiver instance). `receivedLSN` = high-water mark of applied WAL; `flushedLSN` = authoritative durability (advanced only in `handleBarrier`). On receiver restart, init from `vol.nextLSN` + `vol.flusher.CheckpointLSN()`. Rebuild session boundaries preserved via `ActiveRebuildSession()`. |
| Authority coupling | Epoch validation on data apply (`entry.Epoch` vs `vol.epoch.Load()`); mismatch → `ErrStaleEpoch`. Barrier epoch mismatch → `BarrierEpochMismatch`. `ApplyRebuildSessionWALEntry()` routes active-session entries; fallback to normal WAL apply. Contiguous-LSN fence: `entry.LSN == receivedLSN+1` or `ErrDuplicateLSN`. |
| Network protocol surface | Two listeners (data + control). Data: `MsgWALEntry` applied; `MsgResumeShipReq` reconnect handshake; `MsgCatchupDone` catch-up boundary. Control: `MsgBarrierReq` → `handleBarrier`; `MsgSessionControl` → rebuild-session lifecycle. Multi-protocol via message type dispatch on separate streams. |
| Known tricky invariants | (1) Contiguous LSN contract enables barrier `cond.Wait()` to be safe. (2) Three-phase barrier: wait-LSN → `fd.Sync()` → atomically advance `flushedLSN`. (3) WAL-full retry releases `mu` to unblock barriers, re-acquires after flusher notify. (4) Rebuild session routing: entries during `RebuildPhaseRunning` / `RebuildPhaseBaseComplete` → session WAL lane; else normal apply. (5) `ioMu.RLock` held for entire apply to prevent concurrent restore/import; only released inside `replicaAppendWithRetry` during WAL-full wait. |

### §2.4 ReplicaBarrier FSM (barrier handling)

| Attribute | Value |
|---|---|
| V2 file(s) | `replica_barrier.go` (lines 147–204) |
| Scope | **per-request call-closure, BUT queue-state shared per-volume.** Each `handleBarrier` invocation is ephemeral, but the underlying `cond = sync.NewCond(&ReplicaReceiver.mu)` + the `receivedLSN` / `flushedLSN` counters it waits on are **per-volume durable** (for the life of the receiver process). Concurrent barriers share the cond; one broadcast may release several waiters. **L2 hazard**: mis-reading this as "stateless per-request" would 1:1-port it into a V3 stateless function, losing the queue-share semantics and the multi-watcher correctness. |
| Lifecycle owner | Invoked synchronously from `handleControlConn`. The per-call closure has no external owner; the **shared queue state lives in ReplicaReceiver** (§2.3). |
| Concurrency | Serial per control connection; races with concurrent data-channel entries. `cond.Wait()` blocks until `applyEntry` broadcast or timeout fires. Timer goroutine spawned per barrier; broadcasts cond on timeout. `vol.fd.Sync()` single-threaded. |
| Cross-session behavior | Does not persist. Durable boundary `flushedLSN` lives in `ReplicaReceiver`; barrier only advances it. |
| Authority coupling | `req.Epoch` vs `vol.epoch.Load()`; mismatch → `BarrierEpochMismatch`. Waits for strictly contiguous LSN receipt. |
| Network protocol surface | In: `MsgBarrierReq` (epoch + LSN). Out: `MsgBarrierResp` (status byte + `FlushedLSN` 8 bytes). Legacy 1-byte response decoded for old replicas (`repl_proto.go` line 64). |
| Known tricky invariants | (1) Hard-coded `barrierTimeout=5s` (`wal_shipper.go` line 18) — could be replica-side configurable but isn't. (2) Three-phase fsync sequence: wait-cond → fd.Sync → advance. (3) Cond wakeup from both `applyEntry` broadcasts AND timeout; barrier unblocks on any. (4) Multiple-watcher case: two barriers in quick succession share the same cond; first fsync satisfies both LSN requirements. |

### §2.5 DistGroupCommit (durability closure)

| Attribute | Value |
|---|---|
| V2 file(s) | `dist_group_commit.go` (lines 15–83) |
| Scope | per-write-operation (ephemeral closure); bound once per volume at init |
| Lifecycle owner | `MakeDistributedSync()` creates the closure; bound to `vol.writeSync`. No teardown; closure is stateless. |
| Concurrency | Parallel: local `walSync()` and `group.BarrierAll()` run concurrently via WaitGroup. Read-only access to `vol.DurabilityMode()`, `vol.nextLSN.Load()`, `group.BarrierAll()` results. |
| Cross-session behavior | Stateless. Each write invokes closure; durability mode checked at call time. |
| Authority coupling | `DurabilityMode` check at entry: `DurabilitySyncAll` / `DurabilitySyncQuorum` / `DurabilitySyncBestEffort`. LSN target = `vol.nextLSN.Load()-1`. Role implicit (only invoked on primary by upstream gating). |
| Network protocol surface | Aggregates `ShipperGroup.BarrierAll()` results. No direct network calls. |
| Known tricky invariants | (1) Quorum arithmetic: `rf = group.Len() + 1`; `quorum = rf/2 + 1`; primary counts as one durable node. (2) sync_all: any barrier failure → error; write must retry or fail. (3) sync_quorum: `durableNodes < quorum` → `ErrDurabilityQuorumLost`. (4) best_effort: barrier failures logged + `degradeReplica()` but do NOT fail the write. (5) Metrics only on failure: `DurabilityBarrierFailedTotal`, `DurabilityQuorumLostTotal`. |

### §2.6 RebuildSession

| Attribute | Value |
|---|---|
| V2 file(s) | `rebuild_session.go` (lines 41–327). **Embeds RebuildBitmap (see §2.10)** — bitmap is a separate stateful entity with its own on-disk schema, NOT a session-internal implementation detail. |
| Scope | per-replica-session; volatile (tied to session lifecycle, NOT volume lifecycle) |
| Lifecycle owner | `NewRebuildSession()` called from primary host (via `StartRebuildSession()`). Owned by `BlockVol.rebuildSess`. Destroyed via `CancelRebuildSession()`. Does NOT survive crash; rebuild must restart from scratch. |
| Concurrency | All mutation under `mu`. Two concurrent data lanes: WAL lane via `ApplyWALEntry()`; base lane via `ApplyBaseBlock()`. Both acquire+release `mu` individually (short-hold, no deadlock). Bitmap update atomic within `mu`. Ack emit after `mu` release. |
| Cross-session behavior | NOT durable; session state lost on crash. Recovery hydrates bitmap from recovered WAL via `hydrateBitmapFromRecoveredWAL()` IF `TargetLSN != BaseLSN` (two-lane mode). Full-base sessions (`TargetLSN == BaseLSN`) skip hydration. |
| Authority coupling | Epoch on WAL entry (`entry.Epoch` vs `config.Epoch`). Phase FSM: `Idle → Accepted → Running → BaseComplete → Completed` (or `Failed`). Only `Running` / `BaseComplete` accept entries. LSN boundaries: `BaseLSN` (flushed extent) + `TargetLSN` (WAL target). |
| Network protocol surface | No direct network I/O; driven by `RebuildTransportServer` (primary) + `RebuildTransportClient` (replica). Progress via `emitRebuildSessionAck()` → callback. |
| Known tricky invariants | (1) Bitmap conflict resolution: WAL-applied LBA wins over base (`ShouldApplyBase()`). (2) Hydration guard fails closed if local checkpoint > base LSN. (3) WAL apply precedes bitmap set (bitmap set only on successful append). (4) Only `Start()` transitions Accepted→Running; host must explicitly start. (5) ACK cadence: emitted after each WAL entry apply; primary observes progress in real time. |

### §2.7 RebuildServer

| Attribute | Value |
|---|---|
| V2 file(s) | `rebuild.go` (lines 21–307) |
| Scope | per-primary; singleton listener; handles N concurrent rebuild sessions |
| Lifecycle owner | `NewRebuildServer()`; owned by `BlockVol.rebuildServer`. `Serve()` starts; `Stop()` stops. Torn down during demote (`promotion.go` line 123). |
| Concurrency | Accept loop in goroutine; per-connection handlers spawned on each accept. No shared state; `vol` accessed read-only for extent/WAL reads. `Stop()` closes listener + waits all goroutines. |
| Cross-session behavior | Stateless per-connection. Each rebuild session independent. Snapshot LSN captured at call time; no cross-session memory. ForceFlush required before streaming extent + snapshot. |
| Authority coupling | Epoch validation per request; mismatch → `MsgRebuildError("EPOCH_MISMATCH")`. Request-type dispatch: `RebuildWALCatchUp` / `RebuildFullExtent` / `RebuildSessionBase` / `RebuildSnapshot`. |
| Network protocol surface | Single TCP listener. In: `MsgRebuildReq` (0x10). Out: `MsgRebuildEntry` (0x11), `MsgRebuildExtent` (0x12), `MsgRebuildDone` (0x13), `MsgRebuildError` (0x14). Two-phase rebuild: WAL catch-up → extent → second catch-up (client-driven sequencing). Snapshot: temp snapshot → stream manifest + image → delete temp. |
| Known tricky invariants | (1) `StreamEntries()` shared with shipper; `ErrWALRecycled` escapes to client forcing full-extent fallback. (2) `MsgRebuildDone` carries LSN so client knows where second catch-up should start. (3) Extent chunk size fixed 64KB; no flow control; client reads until EOF. (4) `handleSnapshotExport()` verifies checkpoint matches requested BaseLSN; mismatch fails hard. (5) `readBlockFromExtent()` bypasses dirty map (avoids unflushed WAL data). |

### §2.8 RebuildTransportServer

| Attribute | Value |
|---|---|
| V2 file(s) | `rebuild_transport.go` (lines 152–246) |
| Scope | per-session (one per base-lane connection); ephemeral |
| Lifecycle owner | `NewRebuildTransportServer()` in rebuild handler; tied to single socket. Connection closure ends session; no explicit destructor. |
| Concurrency | Single-threaded (one handler goroutine). Vol access read-only. No contention. |
| Cross-session behavior | Session-scoped; holds config (epoch, baseLSN, targetLSN, sessionID); no cross-session state. |
| Authority coupling | Verifies `vol.epoch` at `FlushBeforeStreaming`; no direct epoch check in `ServeBaseBlocks`. |
| Network protocol surface | Inbound implicit (already-connected). Outbound: `MsgRebuildExtent` frames [8B LBA][block data]; done marker `MsgRebuildDone` [8B sentBlocks][8B achievedLSN]. |
| Known tricky invariants | (1) Base stream NOT point-in-time; concurrent flusher writes may advance some LBAs past baseLSN during stream — two-line model + bitmap ensures convergence (`rebuild_transport.go` lines 176–181). (2) Achieved LSN = `vol.nextLSN.Load()-1` at completion, surfaced to session executor. (3) `readBlockFromExtent()` avoids dirty map, preventing unflushed WAL data being served. |

### §2.9 RebuildTransportClient

| Attribute | Value |
|---|---|
| V2 file(s) | `rebuild_transport.go` (lines 252–321) |
| Scope | per-session replica-side; ephemeral |
| Lifecycle owner | `NewRebuildTransportClient()` in `runBaseLaneClient()` (`replica_barrier.go` line 136). Tied to socket connection. |
| Concurrency | Single-threaded. Vol access write-only via `ApplyBaseBlock()`. |
| Cross-session behavior | Session-scoped; holds sessionID only. |
| Authority coupling | Epoch passed to server in RebuildRequest; server validates. |
| Network protocol surface | In: `MsgRebuildExtent` (0x12), `MsgRebuildDone` (0x13), `MsgRebuildError` (0x14). Out: implicit (receiver). |
| Known tricky invariants | (1) Frame parsing extracts LBA from first 8 bytes of extent frame. (2) Block apply routed to `RebuildSession.ApplyBaseBlock()`; bitmap conflict → skip (not an error). (3) Graceful disconnect on error; replica's session handles timeout/retry. |

### §2.10 RebuildBitmap

Added per architect F1: RebuildBitmap has independent on-disk schema and independent conflict-resolution invariant; collapsing it into §2.6 RebuildSession at L1 would lose granularity for L2 verdicts (bitmap and session may have different PRESERVE/REBUILD outcomes).

| Attribute | Value |
|---|---|
| V2 file(s) | `rebuild_bitmap.go` (~84 LOC) + hydration in `rebuild_session.go` `hydrateBitmapFromRecoveredWAL()` |
| Scope | per-rebuild-session (one bitmap per active session); **has on-disk representation** independent of session state |
| Lifecycle owner | Constructed by `RebuildSession` during session init; persisted to sidecar file during session lifetime; unlinked on session `Completed` / `Failed`. Embedded in `RebuildSession.bitmap` field (§2.6). |
| Concurrency | Mutations under `RebuildSession.mu`. Bit-set operations are atomic within that lock; reads (for `ShouldApplyBase`) also under `mu`. No own internal synchronization. |
| Cross-session behavior | **NOT durable across crash** (matches session volatility per §2.6). On primary or replica restart, sidecar file is discarded; rebuild restarts. **WAL hydration** (§2.6 invariant): if recovery shows `TargetLSN != BaseLSN`, bitmap is re-populated from recovered WAL entries to avoid re-applying stale base blocks; hydration guard fails closed if local checkpoint > base LSN. |
| Authority coupling | Indirect — bitmap bit-set happens only AFTER successful WAL `append` (per §2.6 invariant). Epoch validation is upstream (in `ApplyWALEntry`); bitmap itself is epoch-agnostic. |
| Network protocol surface | None — bitmap is local-only state. Conflict resolution happens inside `ApplyBaseBlock` on the replica. |
| Known tricky invariants | (1) **WAL-wins-over-base conflict resolution**: `ShouldApplyBase(lba)` returns false if the bit is set; `ApplyBaseBlock` becomes a no-op (not an error). (2) **Hydration guard** fails closed: prevents unsound recovery if local checkpoint has advanced past the requested base LSN. (3) **Sidecar schema** is an independent on-disk format (84 LOC worth of serialization); any L2 REBUILD verdict would need to decide whether V3 keeps the same on-disk layout for wire-compat / recovery replay. (4) **Bit-set precedes ACK**: bitmap bit is set before the session emits `emitRebuildSessionAck()` (§2.6), so primary sees a consistent "LBA is WAL-owned now" view when it gets the ack. |

---

## §3 L1-level observations (for L2 to chew on)

These are patterns visible at L1 that will drive L2 bridge verdicts. **Not** recommendations — L2 will decide shape per-entity.

1. **Epoch fencing is pervasive.** Five of nine entities take an `epochFn()` callback; every boundary (Ship, Barrier, Rebuild, ApplyEntry) validates or enforces epoch. V3's `frontend.Identity{Epoch, EndpointVersion}` is per-session; replication entities would need their own epoch source or explicit delegation.
2. **LSN contiguity is a hard cross-cutting invariant.** ReplicaReceiver enforces strict `receivedLSN+1` on data; rebuild hydration guards against stale recovery; shipper catch-up detects gaps via `ErrWALRecycled`. This invariant traverses three entities and must not fragment during L2 splits/merges.
3. **Two-lane rebuild (base + WAL) with bitmap conflict resolution** — WAL-applied LBA always wins. The bitmap is the only integration point between the two lanes; moving lanes into different V3 entities without preserving bitmap ownership is a structural hazard.
4. **Durability semantics are mode-dependent.** sync_all / sync_quorum / best_effort determine shipper degradation vs barrier failure routing. V3's `LogicalStorage.Sync(ctx)` has no durability-mode argument today; L2 needs to decide whether mode lives on adapter, Provider, or a new dedicated entity.
5. **No cross-crash session recovery.** RebuildSession is volatile by design. Lost on crash; rebuild restarts from scratch. WAL recovery (separate) handles post-crash WAL replay. V3 DurableProvider's per-volumeID cache (BUG-005 lesson) must NOT cache rebuild session state — Provider owns Backend lifecycle, not replication session lifecycle.
6. **Reconnect protocol is explicit, not TCP-transparent.** Shipper `Disconnected` / `Degraded` states trigger full handshake + catch-up, not passive retry. `activeRebuildSession` flag prevents stale state transitions from opening the live lane during rebuild.
7. **Barrier is a three-phase operation.** Wait-LSN → fsync → advance `flushedLSN`. Both timeout and entry-apply broadcast the same cond; multi-watcher case handled naturally.
8. **ReplicaReceiver's `ioMu.RLock` nesting around apply** — the lock is held across the entire apply path for restore/import exclusion. Only released inside `replicaAppendWithRetry` during WAL-full wait. Any V3 bridge must either preserve this nesting or explicitly rebuild the exclusion (with the rationale documented).
9. **ShipperGroup double watermark** — `MinShippedLSN` (Ceph retention watermark, advisory) vs `MinReplicaFlushedLSNAll` (authoritative sync_all durability). Two consumers, two semantics; losing the distinction in V3 would silently break one or the other.
10. **Cross-node epoch consistency observation window** (H5, architect-added). V2 `dist_group_commit` sync_quorum needs primary to know each replica's ack `{epoch, lsn}`; epoch mismatch → ack ignored for quorum. V2 makes this implicit — wire frame carries epoch, decoder in `repl_proto` extracts it. V3's `frontend.Identity.Epoch` is this-primary's view; the replica's epoch comes from replica-side `ProjectionView` (or from assignment directly). L2 decision required: does the V3 ack frame still carry epoch (wire-compat / simplest), OR does primary maintain a `per-replica epoch cache` (cleaner layering but needs refresh semantics on failover)? These choices produce different failover semantics and different rebuild-trigger conditions — cannot be deferred to implementation.
11. **Write-path vs replication-path concurrency residence** (H6, architect-added). V2 `BlockVol.Write` is one function that does durable-local-write AND triggers `ShipperGroup` broadcast. V3's `StorageBackend.Write → LogicalStorage.Write` is strictly local durable. **Where does replication get triggered?** Three L2 options:
    - **Option A** — `StorageBackend.Write` calls local storage, then synchronously calls shipper. Matches V2 semantics, but adapter-layer gains a replication dependency — violates T3a layering.
    - **Option B** — introduce a `ReplicatedBackend` wrapping `StorageBackend + shipper`; `frontend.Provider` returns Replicated vs Plain based on RF. Clean layering; adds one entity.
    - **Option C** — replication lives inside `DurableProvider`; `StorageBackend` is unaware; Provider intercepts Write. Requires Provider to gain replication responsibility — rhymes with BUG-005's "Provider owns Backend lifecycle" lesson but extends it.

    L1 makes no recommendation; L2 must pick and LOCK this before L3 touches files.

---

## §4 Open questions for review

For sw / architect / PM before L1 sign:

1. **Scope completeness**: is the 10-entity set complete (post architect F1 adding RebuildBitmap as §2.10)? Anything still missed — `sync_all_reconnect_protocol` logic (not a distinct type but a cross-entity invariant)? `split_brain_*` tests reference a primary-takeover arbiter — is that an entity or just a protocol handshake?
2. **Scope accuracy**: any entity I've categorized at the wrong scope? Notably, **ReplicaReceiver** I've marked "per-volume replica-side" (survives demote/promote). Verify against V2 teardown code — if receiver is actually per-assignment, L2 verdicts change.
3. **RebuildSession volatility stance**: V2 documented "does NOT survive crash". Is there any post-4A work that made rebuild sessions durable? If yes, that changes the Provider-cache lesson in §3 observation 5.
4. **DistGroupCommit residence**: at L1 this is a closure bound to `vol.writeSync`. L2 will ask: does V3 put this inside DurableProvider, next to it as a new entity, or somewhere else entirely? Opinions now save a revision cycle later.
5. **Protocol-frame stability** for `repl_proto.go` message types (MsgWALEntry 0x01, MsgBarrierReq/Resp, MsgRebuildReq 0x10, etc.) — V3 can keep wire-compatible (replicate V2 replicas) OR break cleanly (V3 cluster only). This is an L2 REBUILD-vs-PRESERVE call that needs an architect-line decision.

---

## §5 Next steps (ordered; each a gate for the next)

1. **sw V3 pre-scan** (blocks L1 sign; ~5 min) — sw greps `core/frontend/durable/` + `core/frontend/*.go` for pre-baked replication-adjacent assumptions. Minimum checklist:
   - Any `SetReplicaAddrs`, `ReplicaAddrs`, or similar field on `DurableProvider` / `LogicalStorage` / `StorageBackend`?
   - Any `Sync` / `Write` return type or callback that implies waiting on remote acks?
   - Does `LogicalStorage.Write` return a pure-local LSN or something that looks coordinated across nodes?
   - Any `Ship`, `Replicate`, `Quorum`, `Barrier`, `Durability` identifiers in V3 code today?
   - Any stub / comment referencing a future replication surface?

   Output: one line "V3 has no pre-baked replication assumptions" OR a list of "V3 has X at Y, L2 must align/override". Rationale per architect feedback: BUG-005's latent drift came from "V3 already has implicit convention more opaque than V2 contract" — L1 must surface any such convention BEFORE L2 verdicts lock.

2. **L1 three-sign** — architect + PM + QA sign this doc, with sw pre-scan result attached. Triggers L2 work.

3. **L2** — Fill `v2-v3-contract-bridge-catalogue.md` §3 Replication with per-entity bridge rows (shape tag + V3 embedding note + per-contract verdicts). Must LOCK answers to H5 (cross-node epoch ack window) and H6 (Options A/B/C for write-path vs replication-path residence) from §3. Reference V3's five model shifts (event / storage / authority / lifecycle / concurrency) explicitly in embedding notes.

4. **L3** — Derive `v3-phase-15-t4-port-plan-sketch.md` structure: scope / V2 file classification / V3 target files / non-claims / test strategy / provisional ledger rows.

5. **T4 T-start three-sign** — architect + PM + QA on the combined L1 + L2 + L3 package.

---

## §6 Change log

| Date | Change | Author |
|---|---|---|
| 2026-04-22 | Initial L1 survey drafted post-T3 close; 9 entities identified; 9 L1-level observations recorded; 5 open questions raised for sw/architect/PM review | QA Owner |
| 2026-04-22 | Architect feedback round 1 incorporated: F1 split RebuildBitmap into standalone §2.10 (10 entities total); F2 ShipperGroup gains "External deps" row citing master-assignment-provided RF as cross-entity contract; F3 ReplicaBarrier scope rewritten to "per-request call-closure BUT queue-state shared per-volume via cond.Wait"; H5 (cross-node epoch ack window) and H6 (Options A/B/C for write-path vs replication-path residence) added to §3 observations; §5 updated to require sw V3 pre-scan as blocking step before L1 three-sign | QA Owner |
