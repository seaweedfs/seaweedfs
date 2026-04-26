# V2/V3 Contract Bridge Catalogue

**Date**: 2026-04-22
**Status**: LIVING — T3 retrofilled 2026-04-22; T4/T5/T6/T7 sections mandatory-LOCKED before respective T-start three-sign
**Owner**: QA drafts + co-signs per track; sw reviews; architect + PM sign as part of T-start sketch

---

## §0 Purpose + discipline

Port is NOT a file-by-file copy exercise. It is a **cross-version stateful-entity bridging** exercise. Each V2 entity carries contracts (explicit + implicit) governing scope, lifecycle, concurrency, and cross-session behavior. V3 rebuilds the topology around new models (event / storage / authority / concurrency / lifecycle); every bridge must be audited.

**Top-down cutting discipline**:

| Level | Output | Order |
|---|---|---|
| L1 — Entity enumeration | For each V2 stateful entity: scope / lifecycle owner / concurrency / cross-session behavior | FIRST |
| L2 — Bridge + contracts | Map V2 entity → V3 entity(ies); V3 embedding notes; per-contract PRESERVE/REBUILD/BREAK verdict | SECOND |
| L3 — Files / functions | Which V2 files map to which V3 files; function-level classification | THIRD (derived from L1+L2) |

Prior T-end sketches went straight to L3 (file-by-file audit). Every known drift (BUG-001, BUG-005, Addendum A) traces back to an L1/L2 decision that was never made explicit. Enforcing top-down order means the drift surface gets caught at T-start sign, not at integration bug time.

**Bridge shape** (shorthand tag, attached to each entity, NOT primary verdict):

| Shape | V2 : V3 | Meaning |
|---|---|---|
| `1:1` | 1 → 1 | One V2 entity → one V3 entity. **Does NOT imply verbatim port** — the V3 event/storage/authority model may embed the entity differently. Re-audit mandatory. |
| `split` | 1 → N | One V2 entity → multiple V3 entities. Contracts distribute across the split; each contract must name which V3 entity owns it. |
| `merge` | N → 1 | Multiple V2 entities → one V3 entity. Contracts from multiple origins coexist; explicit reconciliation rules required. |
| `retired` | 1 → 0 | V2 entity has no V3 equivalent. BREAK all contracts with rationale. |
| `new` | 0 → 1 | V3 entity has no V2 predecessor. Contracts must be written fresh (not ported); subject to §8C.5 semantic-contract audit. |

**1:1 does NOT mean "port verbatim"**. Example: V2 `GroupCommitter` and V3 `core/storage.GroupCommitter` are 1:1 by name, but V3 drops the `OnDegraded` callback (V3 event model uses return-value error propagation, not V2's callback fan-out). Even 1:1 requires V3-embedding review per §1.1.

---

## §1 Frameworks

### §1.1 V3 model shifts (baseline — why even 1:1 entities may need re-audit)

V3 is not a line-by-line copy of V2. It reshapes five cross-cutting models. Every entity bridge must be evaluated against these shifts:

| Model | V2 | V3 | Impact on port |
|---|---|---|---|
| **Event model** | Callbacks (`NotifyFn`, `ClosedFn`, `OnDegraded`, `PostSyncCheck`) fan out from inside storage/engine | Typed errors + interface return values + `ctx.Done()`; observer responsibility moves to adapter/host layer | 1:1 entity ports may need callback → return-value rewiring |
| **Storage model** | `BlockDevice` flat Read/Write at byte offset; BlockVol engine owns WAL + dirty + fsync | `LogicalStorage` interface with LSN + Sync + Recover + Boundaries; pluggable impls (walstore / smartwal); adapter does byte↔LBA | Even if V2 entity name survives, the V3 interface wraps with LSN + Sync + Recover semantics |
| **Authority model** | Fence / epoch / role checked inside storage layer (`write_gate`, `superblock.Epoch`, `PostSyncCheck`) | Authority strictly master-side; per-I/O fence at adapter boundary via `ProjectionView`; storage never mints/advances/publishes | Any V2 entity holding authority state → REBUILD (move state out) or BREAK (retire) |
| **Lifecycle model** | Admin adds `AddVolume(nqn, dev, nguid)`; lifetime tied to RemoveVolume | `DurableProvider` lazy-opens + caches per-volumeID; explicit Provider.Close tears down; `SetOperational` for readiness | V2 admin-driven lifecycle → V3 lazy/cached Provider lifecycle; Session MUST NOT close Backend (BUG-005 lesson) |
| **Concurrency model** | `Controller.rxLoop` single-threaded serial; `GroupCommitter` has a goroutine; most paths serial | rxLoop still serial post-BUG-001 revert; smartwal has internal goroutine; per-impl variability | Controller split: rxLoop stays serial (Session); I/O submission may fan out to goroutine owned by impl |

**When doing §2+ bridge audit, each contract must be checked against every relevant model shift** — "PRESERVE" does not mean "copy code"; it means "equivalent semantic under the new model".

### §1.2 Entity attribute columns (L1 output)

For each V2 entity being ported, fill:

| Attribute | Example |
|---|---|
| **Name** | `NVMe Subsystem` |
| **V2 file(s)** | `weed/storage/blockvol/nvme/server.go` |
| **Scope** | request / session / volume / process / persistent |
| **Lifecycle owner** | which V2 function creates/destroys it |
| **Concurrency discipline** | locks, goroutines, serial vs parallel |
| **Cross-session behavior** | what happens on session end / process restart |
| **Bridge shape** | `1:1 / split / merge / retired / new` |
| **V3 entity(ies)** | where in V3 this maps |
| **V3 embedding note** | how V3's model (event / storage / authority / lifecycle / concurrency) wraps this entity — what shifts apply |

### §1.3 Contract catalogue columns (L2 output)

For each contract carried by an entity:

| Column | Purpose |
|---|---|
| **Contract ID** | `C1-NVME-SUBSYS-DEV-LIFETIME` |
| **Statement** | 1-sentence rule |
| **V2 evidence** | file:line or function reference |
| **V3 verdict** | PRESERVE / REBUILD / BREAK |
| **V3 rationale** | why this verdict (not "because we said so") |
| **V3 impl location** | which V3 file + symbol enforces this |
| **Test anchor** | regression test that pins the contract |

If the entity is `new` (§1.0), contracts are fresh and subject to §8C.5 semantic-contract audit instead of V2 evidence.

---

## §2 Storage layer (T3 / G4) — retrofilled 2026-04-22

### §2.1 Entity bridge map (V2 → V3)

| V2 entity | Bridge | V3 entity(ies) | Primary drift risk |
|---|---|---|---|
| `Subsystem` (NVMe) | merge | `DurableProvider.volumes` + `Target.ctrls` | ⚠️ Lifecycle transfer (BUG-005 landed here) |
| `Subsystem.Dev` (BlockDevice) | merge | Per-volume `LogicalStorage` cached in `DurableProvider` | ⚠️ Same as above |
| `Controller` (NVMe session) | split | `nvme.Session` + `StorageBackend` (borrowed) + adapter-layer fence | ⚠️ Concurrency model (BUG-001 landed here) |
| `BlockVol` (engine) | split | `LogicalStorage` + `ProjectionView` (T1) + `StorageBackend` adapter (T3a) | Authority model shift — fence lifted out |
| `walWriter` | 1:1 | `core/storage.walWriter` | Low — port stable |
| `walAdmission` | 1:1 | `core/storage.walAdmission` | Low; V3 drops Metrics callback |
| `DirtyMap` | 1:1 | `core/storage.dirtyMap` | Low — Phase 08 lesson honored |
| `GroupCommitter` | 1:1 | `core/storage.GroupCommitter` | **V3 drops `OnDegraded`, `PostSyncCheck`** (event model shift) |
| `Superblock` | 1:1 | `core/storage.superblock` + `smartwal.superblock` | **V3 drops `.Epoch` field** (authority model shift) |
| `flusher` | 1:1 | `core/storage.flusher` | Low |
| `pendingCapsules` | 1:1 | `nvme.Session` internal (rxLoop private) | Low; preserved post-BUG-001 revert |
| `KATO timer` | 1:1 | `nvme.Session` KATO state | V3 T3-scope: stored only, no timer enforcement (Set-only) |
| `AsyncEventRequest` slot | 1:1 | `nvme.Session.pendingAER` | Low |
| `CNTLID registry` | 1:1 | `nvme.Target.ctrls` | Low |
| `Target` (NVMe server) | 1:1 | `nvme.Target` | Low |
| `write_gate` (function) | **retired** | (no V3 entity; fence at adapter) | Authority model shift — correctly retired |
| — | **new** | `DurableProvider` | No V2 analog |
| — | **new** | `StorageBackend` (adapter) | No V2 analog |
| — | **new** | `ProjectionView` (T1) | Fence-aware contract new to V3 |
| — | **new** | `RecoveryReport` | No V2 analog; read-side readiness signal |

### §2.2 Detailed entity audits (drift-prone + new)

#### §2.2.1 `NVMe Subsystem` + `Subsystem.Dev` (merge → DurableProvider + LogicalStorage)

- **V2 file(s)**: `weed/storage/blockvol/nvme/server.go` + inline `Subsystem` struct
- **Scope**: volume (NQN → one Subsystem → one Dev)
- **Lifecycle owner**: `Server.AddVolume` creates; `Server.RemoveVolume` destroys
- **Concurrency**: `Server.subsystems` map under `sync.RWMutex`; read from Controllers, write from admin
- **Cross-session behavior**: **persists** (sessions lookup by NQN; never modify Dev lifecycle)
- **Bridge shape**: merge (Subsystem + Subsystem.Dev + Subsystem.NQN metadata → split across DurableProvider + Identity)
- **V3 entity(ies)**:
  - `DurableProvider.volumes map[string]*cachedVolume` — provides the "NQN → opened backend" lookup
  - `LogicalStorage` instance — the durable engine, analogous to Subsystem.Dev
  - `Identity` struct — VolumeID / ReplicaID / Epoch / EndpointVersion (authority-derived)
- **V3 embedding note**:
  - *Lifecycle model shift*: V2 admin `AddVolume` (eager) → V3 Provider.Open (lazy, on first Connect); V3 Provider.Close (explicit at cmd/blockvolume shutdown) replaces RemoveVolume
  - *Storage model shift*: V2 `BlockDevice` flat Read/Write → V3 `LogicalStorage` with LSN + Sync + Recover + Boundaries semantics
  - *Authority model shift*: V2 Subsystem carried no authority state (NQN is just a label). V3 adds Identity struct alongside; adapter consumes for fence. Authority never flows into storage

**Contracts**:

| ID | Statement | V2 evidence | V3 verdict | V3 impl + rationale | Test anchor |
|---|---|---|---|---|---|
| C1-SUBSYS-DEV-LIFETIME | Dev lifetime = AddVolume → RemoveVolume; NOT session-managed | `server.go:AddVolume/RemoveVolume` | PRESERVE (explicit) | `DurableProvider.volumes` cache; `provider.go:Close` owns teardown. `Session.Close` MUST NOT touch storage (post-BUG-005 fix removed `defer backend.Close()` from iSCSI+NVMe handleConn). Godoc on `frontend.Backend.Close`: "owned by Provider; session layer MUST NOT call" | `t3b_bug005_backend_reuse_across_sessions_test.go` (BUG-005 regression) |
| C2-SUBSYS-REUSE-ACROSS-SESSIONS | Next Connect via NQN lookup returns same Dev | `server.go:handleConn` dials Subsystem by NQN | PRESERVE | `Provider.Open(volumeID)` returns cached Backend; same underlying LogicalStorage across sessions | `TestT3b_DurableProvider_Open_Caches` |
| C3-SUBSYS-REGISTRY-IS-SERVER-SCOPE | Registry lives one level above individual session | `server.go:subsystems` map | PRESERVE | `DurableProvider.volumes` sync.RWMutex; per-process | unit tests around Provider.Open |
| C4-SUBSYS-NO-AUTHORITY-WRITES | Subsystem + Dev never mint / advance / publish epoch | V2 had no such code — implicit | PRESERVE (explicit now) | Storage has no Epoch field (§G.2 audit §10.5 Option 3); fence via adapter's per-I/O ProjectionView | `INV-FRONTEND-002.*` facet tests under durable |

#### §2.2.2 `NVMe Controller` (split → Session + StorageBackend + adapter fence)

- **V2 file(s)**: `weed/storage/blockvol/nvme/controller.go`
- **Scope**: session (one per TCP connection)
- **Lifecycle owner**: `Server.acceptLoop` creates on Accept; `Controller.shutdown` destroys on conn close
- **Concurrency**: **rxLoop single goroutine**; **inline `collectR2TData` + `recvH2CData`** during Write; **`pendingCapsules` buffer** for CapsuleCmds arriving mid-recv
- **Cross-session behavior**: **session-scope** — Controller dies with the TCP connection; Subsystem.Dev and Server.subsystems remain
- **Bridge shape**: split
- **V3 entity(ies)**:
  - `nvme.Session` — session logic (admin dispatch, Connect, Fabric handling, rxLoop); pure session state
  - `StorageBackend` (from T3a) — storage-access façade; **borrowed** by Session, NOT owned
  - Adapter-layer fence check — uses ProjectionView + Identity for per-I/O drift detection
- **V3 embedding note**:
  - *Concurrency model*: **preserved post-BUG-001 revert**. `session.go`'s rxLoop stays single-threaded; `collectR2TData` inline with `bufferInterleaved`. Do NOT re-pragmatize. §8C.3 trigger #4
  - *Authority model shift*: Controller-layer fencing (`writeGate`) removed; fence is now adapter-layer per-I/O
  - *Lifecycle*: Session closes → storage stays (BUG-005 fix)

**Contracts**:

| ID | Statement | V2 evidence | V3 verdict | V3 impl + rationale | Test anchor |
|---|---|---|---|---|---|
| C3-CONTROLLER-RXLOOP-SERIAL | rxLoop reads one CapsuleCmd at a time; no concurrent cmd dispatch | `controller.go:rxLoop` + `collectR2TData` inline | PRESERVE (revert-recovered) | `nvme/session.go` rxLoop serial; kernel's piplined cmds buffered via `pendingCapsules` | `t2_v2port_nvme_pipelined_writes_test.go` + m01 Matrix A |
| C6-CONTROLLER-R2T-BEFORE-H2CDATA | Write cmd triggers R2T BEFORE any H2CData consumed; bufferInterleaved absorbs CapsuleCmd arriving mid-H2C | `controller.go:recvH2CData` + `bufferInterleaved` | PRESERVE | session.go matches V2 pattern byte-for-byte post-revert | same |
| C1-CONTROLLER-NOT-CLOSE-STORAGE | Controller.shutdown does NOT call Subsystem.Dev.Close | `controller.go:shutdown` (no Dev.Close) | PRESERVE | Session.Close doesn't call backend.Close (post-BUG-005 fix). Backend.Close godoc enforces | BUG-005 regression test |
| C5-CONTROLLER-NO-RETRY | IO errors returned verbatim to host; target doesn't retry | V2's `write_retry.go` was the E-category pattern; Controller itself didn't retry | PRESERVE (write_retry never ported) | `INV-FRONTEND-NO-RETRY-001` — `TestT2V2Port_NVMe_IO_NoTargetRetry_*` | QA A11.4 test |

#### §2.2.3 `BlockVol` (engine, split → LogicalStorage + ProjectionView + StorageBackend)

- **V2 file(s)**: `weed/storage/blockvol/blockvol.go`
- **Scope**: volume
- **Lifecycle owner**: `OpenBlockVol` (crash recovery inline) / `Close`
- **Concurrency**: many internal locks + goroutines (flusher, groupCommit, dirtyMap shards, WAL)
- **Cross-session behavior**: **persists** — survives session lifecycle
- **Bridge shape**: split (engine functions distribute to three V3 layers)
- **V3 entity(ies)**:
  - `core/storage.WALStore` / `smartwal.Store` — pure storage mechanism (Read / Write / Sync / Recover / Boundaries)
  - `ProjectionView` (T1) — authority lineage source consumed by adapter
  - `StorageBackend` (T3a) — adapts storage to `frontend.Backend` + per-I/O fence + operational gate
- **V3 embedding note**:
  - *Storage model shift*: V2 flat BlockDevice → V3 LogicalStorage with LSN + Sync + Recover
  - *Authority model shift*: V2 BlockVol could fence locally (`writeGate`) → V3 storage never fences; adapter fences via ProjectionView
  - *Event model shift*: V2 group_commit error propagation via `OnDegraded` callback → V3 storage Sync returns error; adapter propagates upward via SCSI `MEDIUM_ERROR` / NVMe `InternalError`

**Contracts** (selected; most already audited in T3.0 §3):

| ID | Statement | V2 evidence | V3 verdict | V3 impl + rationale | Test anchor |
|---|---|---|---|---|---|
| C4-BLOCKVOL-WRITE-GATE-INTERNAL | Engine's write_gate check inside BlockVol.Write path | `blockvol.go:writeGate` | BREAK (retired) | V3 storage has no fence; moved to adapter. Storage serves blindly. T3.0 §3.7 + §10.3.7 LOCK | `INV-FRONTEND-002.*` fence tests |
| C2-BLOCKVOL-EPOCH-PERSIST | Epoch field in Superblock advance on fence changes | `blockvol.go` + `superblock.go:.Epoch` | BREAK (no V3 field) | V3 superblock has no Epoch (§10.5 Option 3); authority drift guard is per-I/O adapter check | same |
| C5-BLOCKVOL-NO-RETRY | `write_retry.go` retry-as-authority NOT ported | `write_retry.go` | BREAK | E-category per T3.0; QA A11.4 pins no-retry | `TestT2V2Port_NVMe_IO_ErrorsReturnedVerbatim` |

#### §2.2.4 `GroupCommitter` (1:1 by name — V3 embedding differs)

- **V2 file**: `weed/storage/blockvol/group_commit.go`
- **Scope**: volume (embedded in BlockVol)
- **Lifecycle owner**: BlockVol init/shutdown
- **Concurrency**: dedicated goroutine batching fsync waiters
- **Cross-session behavior**: persists
- **Bridge shape**: 1:1 (name preserved)
- **V3 entity**: `core/storage.GroupCommitter`
- **V3 embedding note (why 1:1 ≠ verbatim)**:
  - *Event model shift* — V2 had `OnDegraded` callback fired on fsync error → V3 dropped the field; error flows via Sync return value instead. Upstream observers (adapter / volume host) derive "degraded" from error rate, NOT from a callback
  - *Authority model shift* — V2 had `PostSyncCheck` callback that could re-check fencing after a batch. V3 dropped the field entirely (MATCHES-BETTER per T3.0 §10.3.6); mechanically cannot wire authority check

**Contracts**:

| ID | Statement | V2 evidence | V3 verdict | V3 impl + rationale | Test anchor |
|---|---|---|---|---|---|
| C3-GROUPCOMMIT-BATCH-SERIAL | Single writer fsyncs in batches; waiters queue + wake | `group_commit.go:Run + Submit` | PRESERVE | `core/storage/group_commit.go` — same shape | `flusher_test.go` |
| C5-GROUPCOMMIT-ERROR-PROPAGATION | Fsync error → observer notified | V2 `OnDegraded` callback | REBUILD (V3 event model) | V3 returns error via Sync; observer derivation lives at adapter / host layer (not reinstated as callback per T3.0 sw note A) | contract-check in T4 mini-plan if derived signal needed |
| C4-GROUPCOMMIT-NO-POSTSYNC-AUTHORITY | Post-sync hook can re-check fencing | V2 `PostSyncCheck` field (sometimes wired) | BREAK (field absent) | V3 GroupCommitterConfig lacks field; mechanically unreachable | T3.0 §10.3.6 audit |

#### §2.2.5 `Superblock` (1:1 by name — field shape diverges)

- **V2 file**: `weed/storage/blockvol/superblock.go`
- **Scope**: persistent (on-disk header)
- **Lifecycle**: Written at volume create; read at open / recovery
- **Bridge shape**: 1:1
- **V3 entity**: `core/storage.superblock` + `core/storage/smartwal.superblock`
- **V3 embedding note**:
  - *Authority model shift* — V2 had `.Epoch` + `.ExpandEpoch`; V3 has neither (§10.3.1 GAP + §10.5 Option 3)
  - *Lifecycle model shift* — V2 had `DurabilityMode` / `StorageProfile` / `PreparedSize` fields; V3 simplified to block geometry + WAL geometry + (T3a-new) ImplKind + ImplVersion

**Contracts**:

| ID | Statement | V2 evidence | V3 verdict | V3 impl + rationale | Test anchor |
|---|---|---|---|---|---|
| C4-SUPERBLOCK-EPOCH-PERSIST | Epoch field on disk, read on open | V2 `superblock.go:.Epoch` | BREAK (no V3 field) | Per §10.5 Option 3; stronger §3.2 boundary | audit doc §10.5 |
| C3-SUPERBLOCK-WAL-GEOMETRY | WALOffset / WALSize / CheckpointLSN persist | V2 + V3 both have | PRESERVE | `core/storage/superblock.go` + `walstore.go:250 WALCheckpointLSN write-back` | `flusher_test.go` |
| C-SUPERBLOCK-IMPL-IDENTITY (new) | Impl kind + version recorded on create; mismatch fail-fast | — (V3-new) | NEW per Addendum A #2 | `superblock.go` ImplKind + ImplVersion fields; `Provider.Open` ErrImplKindMismatch | `INV-DURABLE-IMPL-IDENTITY-001` |

#### §2.2.6 Retired: `write_gate` (BREAK — no V3 entity)

- **V2 file**: `weed/storage/blockvol/write_gate.go` (28 LOC)
- **V2 role**: engine-internal pre-Write fencing check
- **Bridge shape**: retired
- **Rationale for BREAK**: Authority model shift. Storage must not fence. Fence is adapter-layer, per-I/O via ProjectionView.
- **V3 enforcement**: grep for `writeGate` / `ErrNotPrimary` / `ErrEpochStale` / `ErrLeaseExpired` in `core/storage/` returns zero matches. Reintroduction = §8C.3 trigger #4.
- **Test anchor**: existing T1 `INV-FRONTEND-002.*` facet tests (fence at frontend boundary, not storage).

#### §2.2.7 New V3-era entities (no V2 analog)

**`DurableProvider`** (no V2 analog)
- **Scope**: process
- **Lifecycle owner**: `cmd/blockvolume` main (Open lazy; Close at shutdown)
- **Concurrency**: `volumes map` under `sync.Mutex`
- **Cross-session behavior**: cached; multiple sessions share same Backend pointer
- **Contracts**:

| ID | Statement | V3 verdict | Rationale | Test |
|---|---|---|---|---|
| C-PROVIDER-CACHE | Open returns same Backend for same volumeID | PRESERVE (new) | Mirrors V2 Server.subsystems lookup semantic | `TestT3b_DurableProvider_Open_Caches` |
| C-PROVIDER-OWNS-LIFECYCLE | Only Provider.Close tears down Backend + LogicalStorage | PRESERVE (new) | Rebuilds V2's AddVolume/RemoveVolume ownership; prevents BUG-005-like session-Close violations | `t3b_bug005_backend_reuse_across_sessions_test.go` |
| C-PROVIDER-IMPLKIND-GUARD | Open fail-fast if selector ≠ on-disk ImplKind | NEW | Addendum A #2 | `TestT3b_DurableProvider_Open_ImplKindMismatch_FailsFast` |

**`StorageBackend`** (adapter, no V2 analog)
- **Scope**: volume
- **Contracts** (partial — full list in T3a mini plan):

| ID | Statement | Rationale | Test |
|---|---|---|---|
| C-BACKEND-OPGATE | Before `SetOperational(true, _)`, all I/O returns `ErrNotReady` | Authority-safety gate; mirrors "not yet ready" without publishing | `INV-DURABLE-OPGATE-001` |
| C-BACKEND-LINEAGE-CHECK | Per-I/O ProjectionView lineage comparison; drift → ErrStalePrimary | New fence location (was V2 write_gate territory) | `INV-FRONTEND-002.*` under durable |
| C-BACKEND-CLOSE-PROVIDER-OWNED | Close may be called only by Provider.Close | Prevents BUG-005 | Backend.Close godoc + BUG-005 test |

#### §2.2.8 T1 `frontend.Backend` interface (new V3-era — BUG-005 root entity)

- **V2 analog**: none. V2 iSCSI / NVMe session code held direct pointers to `BlockDevice` / `Subsystem.Dev`. No per-session handle abstraction.
- **Scope**: volume (one Backend per VolumeID, shared across sessions via Provider cache)
- **Lifecycle owner**: `frontend.Provider` (T1) / `durable.DurableProvider` (T3b). Session layer borrows — NEVER owns.
- **Concurrency**: implementation-defined; atomic operational flag + lineage RWMutex in `StorageBackend` (T3a impl)
- **Cross-session behavior**: **persists** across session disconnect/reconnect; same pointer returned by repeat `Provider.Open(volumeID)`
- **Bridge shape**: new (no V2 predecessor)
- **V3 embedding note (semantic contract audit per §8C.5)**:
  - *Lifecycle model*: V2's admin-lifetime pattern (`Subsystem.Dev` lives AddVolume → RemoveVolume) was transferred conceptually to Provider. `Backend` is the borrowed handle analog of V2's `Controller.subsystem` pointer
  - *Event model*: Session reports errors via return values (no callbacks). `SetOperational` is push-based from Provider → Backend, not event-subscribed
  - *Authority model*: `Identity()` accessor returns captured authority facts (VolumeID/ReplicaID/Epoch/EndpointVersion); Backend consumes lineage via ProjectionView

**Contracts** (mostly new V3-era; fresh semantic-contract definitions):

| ID | Statement | V3 verdict | V3 rationale + impl | Test anchor |
|---|---|---|---|---|
| C1-BACKEND-CLOSE-PROVIDER-OWNED | `Backend.Close()` may be called ONLY by Provider.Close (or test code); session layer MUST NOT call | NEW (post-BUG-005 explicit) | Backend godoc enforces; iSCSI `session.go` + NVMe `session.go` handleConn do NOT `defer backend.Close()` (sw fix); BUG-005 regression test pins | `t3b_bug005_backend_reuse_across_sessions_test.go` + iSCSI equivalent |
| C2-BACKEND-OPERATIONAL-GATE | Before `SetOperational(true, _)` all I/O returns `ErrNotReady`; after, I/O proceeds under lineage check | NEW | `core/frontend/durable/storage_adapter.go` gate; testback mirrors | `INV-DURABLE-OPGATE-001` |
| C3-BACKEND-LINEAGE-CHECK-PER-IO | Every Read/Write/Sync re-validates Identity vs current ProjectionView lineage; drift → `ErrStalePrimary` | NEW (replaces V2 `write_gate` location) | `storage_adapter.go:lineageCheck` | `INV-FRONTEND-002.EPOCH/.EV/.REPLICA/.HEALTHY` facets under durable |
| C4-BACKEND-IDENTITY-CAPTURED-AT-OPEN | `Identity()` returns the lineage snapshot taken at `Provider.Open`; never mutates after | PRESERVE-equivalent (new pattern aligned with V2's implicit "Dev config immutable during session") | `StorageBackend.id` set once in `NewStorageBackend`; not exposed for mutation | `TestT3a_StorageBackend_ImplementsBackend` |
| C5-BACKEND-SYNC-SPEC-LEGAL | `Sync(ctx)` error propagates to host as spec-legal status (SCSI `MEDIUM_ERROR` / NVMe `InternalError`); NEVER silent success | NEW | iSCSI `scsi.go` SYNC_CACHE wire; NVMe `io.go` Flush wire | `INV-DURABLE-SYNC-WIRED-001` |

#### §2.2.9 T1 `frontend.ProjectionView` interface (new V3-era)

- **V2 analog**: none. V2 authority state was checked inside storage via `write_gate`.
- **Scope**: volume
- **Lifecycle owner**: T1 volume host (`core/host/volume/projection_bridge.go` implements `AdapterProjectionView`)
- **Concurrency**: internal implementation uses atomic.Value for projection publication; `Projection()` accessor is lock-free read
- **Cross-session behavior**: persists; multiple sessions read concurrent; updated by host when authority changes
- **Bridge shape**: new
- **V3 embedding note**:
  - *Authority model*: this is where authority becomes observable at frontend layer. Read-only interface — Backend consumers can check drift, NOT mutate

**Contracts**:

| ID | Statement | V3 verdict | V3 rationale + impl | Test anchor |
|---|---|---|---|---|
| C1-PROJECTIONVIEW-READONLY | No mutation method; only `Projection()` accessor | NEW | Interface has one method; boundary guard rejects any mutation-shaped call | `TestNoOtherAssignmentInfoConstruction` boundary test |
| C2-PROJECTIONVIEW-NEVER-ADVANCE-EPOCH | Consumers read Epoch, never write; epoch advance is authority-only (PCDD-STUFFING-001) | PRESERVE (explicit boundary) | Enforced by boundary_guard_test recursive walk + Projection struct fields unexported | `PCDD-STUFFING-001` rows |
| C3-PROJECTIONVIEW-SNAPSHOT-CONSISTENT | A single `Projection()` call returns an internally-consistent tuple; no tearing between fields | NEW | `atomic.Value` stored full Projection struct; readers see whole-struct atomicity | T1 tests around `AdapterProjectionView` |

#### §2.2.10 T1 `frontend.Provider` interface (new V3-era, parent abstraction)

- **V2 analog**: partially V2 `Server.AddVolume / subsystems map` but without admin semantics
- **Scope**: process
- **Lifecycle owner**: T1 defined interface; T3b `durable.DurableProvider` is impl; `testback.StaticProvider` is test impl; `memback.Provider` is memback impl
- **Concurrency**: impl-defined; DurableProvider uses per-Provider Mutex on volumes map
- **Cross-session behavior**: N/A (Provider isn't session-scoped — it IS the thing sessions consult)
- **Bridge shape**: new (with V2 conceptual echo of admin lookup registry)
- **V3 embedding note**:
  - *Lifecycle model*: V3 introduces lazy-open + cached pattern; V2 had eager `AddVolume`. Provider.Open is idempotent; repeated Open for same volumeID returns same Backend
  - *Event model*: Open is request/response, no subscriptions

**Contracts**:

| ID | Statement | V3 verdict | V3 rationale + impl | Test anchor |
|---|---|---|---|---|
| C1-PROVIDER-SINGLE-ENTRY | `Open(ctx, volumeID)` is the sole entry for session layer; no direct storage access | PRESERVE (boundary) | Session layer holds only `Provider` reference; frontend/iscsi + frontend/nvme `handleConn` call `provider.Open` | iSCSI + NVMe boundary guard tests |
| C2-PROVIDER-OPEN-IDEMPOTENT-BY-VOLUMEID | Repeated Open(same volumeID) returns same Backend instance | NEW (T3b) | `DurableProvider.volumes` cache; `TestT3b_DurableProvider_Open_Caches` | cache test |

#### §2.2.11 T2 iSCSI `Target` (1:1 — V2 registry analog)

- **V2 analog**: V2 `iscsi.TargetRegistry` / `LookupDevice(iqn)` (implicit — pointer-held by session code)
- **Scope**: process
- **Lifecycle owner**: `cmd/blockvolume` main
- **Concurrency**: `Target.sessions sync.WaitGroup` for draining; no shared-device registry (single-target shape in T2 scope)
- **Cross-session behavior**: Target outlives any session
- **Bridge shape**: 1:1 (name preserved)
- **V3 embedding note**:
  - *Lifecycle model shift* (minor): T2 Target single-target shape (one IQN per Target); V2 had multi-target registry (lookup by IQN). Multi-target deferred to post-T2 if needed

**Contracts**:

| ID | Statement | V3 verdict | Rationale + impl | Test anchor |
|---|---|---|---|---|
| C1-ISCSI-TARGET-SESSION-NOT-CLOSE-DEVICE | Target's `handleConn` session close does NOT close Backend | PRESERVE (post-BUG-005 fix for iSCSI same as NVMe) | `core/frontend/iscsi/session.go:80 "backend holds... so serve() can Close it"` **comment is misleading**; actual serve() does NOT call backend.Close (removed) — referenced `bugs/005_backend_close_cross_session.md` | same BUG-005 test family |
| C2-ISCSI-TARGET-ACCEPT-LOOP | One goroutine per connection; Target.sessions WaitGroup drains on Close | PRESERVE | `target.go:handleConn` spawned goroutine; WaitGroup pattern same as V2 | target close tests |

#### §2.2.12 T2 iSCSI `Session` (session-scope; BUG-005 sibling site)

- **V2 analog**: V2 iSCSI Session equivalent (session state machine + PDU loop)
- **Scope**: session (one per TCP connection)
- **Lifecycle owner**: `Target.handleConn` creates; `Session.Close` on session end
- **Concurrency**: single `serve()` goroutine (PDU rx/tx loop)
- **Cross-session behavior**: dies with TCP conn
- **Bridge shape**: 1:1
- **V3 embedding note**:
  - *Lifecycle model shift*: V3 Session holds `backend frontend.Backend` borrowed from Provider; same pattern as NVMe Session post-BUG-005 revert
  - *Concurrency model*: PDU loop stays serial; no pragmatic multi-goroutine (BUG-001 lesson transferred to iSCSI preemptively)

**Contracts**:

| ID | Statement | V3 verdict | Rationale + impl | Test anchor |
|---|---|---|---|---|
| C1-ISCSI-SESSION-NOT-CLOSE-BACKEND | Session.Close does NOT call backend.Close | PRESERVE (BUG-005 fix applied symmetrically) | `session.go` serve loop; backend is borrowed | Integration tests + BUG-005 test |
| C3-ISCSI-SESSION-PDU-SERIAL | PDU rx/tx loop serial per session (matches NVMe rxLoop pattern) | PRESERVE | `session.go:serve` serial read/write; no per-PDU goroutine spawn | existing iSCSI tests + T2 m01 sanity |
| C4-ISCSI-SESSION-DISCOVERY-SKIPS-BACKEND | Discovery sessions don't open Backend | PRESERVE | `session.go:45` comment; backend opened only in Normal session path | `t2_iscsi_discovery_test.go` |

#### §2.2.13 T2 iSCSI VPD / Identify metadata (Addendum A root entity)

- **V2 file**: `weed/storage/blockvol/iscsi/scsi.go` VPD builders; Serial / Model / Vendor / NAA fields
- **Scope**: per-volume, derived from Identity
- **Lifecycle**: computed on-demand per INQUIRY response (stateless)
- **Bridge shape**: 1:1 (same INQUIRY VPD pages) with **V3 embedding shift**: Serial/NAA derivation from VolumeID instead of V2's hardcoded stub
- **V3 embedding note**:
  - *Authority model*: VPD 0x80 (Serial) + VPD 0x83 (NAA) now derived from `sha256(SubsysNQN+VolumeID)` for cross-volume uniqueness (was V2 hardcoded string)

**Contracts**:

| ID | Statement | V2 | V3 verdict | Rationale + impl | Test anchor |
|---|---|---|---|---|---|
| C1-ISCSI-VPD80-SERIAL-DERIVATION | VPD 0x80 Serial = sha256-derived per VolumeID (16-char hex) | V2 **hardcoded** `"SeaweedFS"` stub | **REBUILD** (was mis-classified PORT-AS-IS at Batch 10.5 → caught at addendum A) | `scsi.go:serialFromVolumeID` | `PCDD-ISCSI-VPD80-SERIAL-DETERMINISM-001` |
| C2-ISCSI-VPD83-NAA-DERIVATION | VPD 0x83 NAA-6 derived sha256 per (SubsysNQN, VolumeID) | — (V3 introduced) | NEW | `scsi.go:naaFromVolumeID` | `PCDD-ISCSI-VPD83-NAA-DETERMINISM-001` |
| C3-ISCSI-VPD00-ADVERTISED-MATCHES-IMPL | Advertised page list ≡ actually-served page set | — | NEW | `TestT2V2Port_SCSI_InquiryVPD00_AdvertisesOnlyImplemented` | same test |
| C4-ISCSI-VPD-STATIC-FIELDS-HARDCODED | Model / Vendor / Firmware-Rev / ProductRev fields come from HandlerConfig (defaults are V2-era constants `"SeaweedF"` / `"BlockVol"`); no per-volume derivation | V2 hardcoded same way | **PORT-AS-IS (LOCKED explicit)** — per-volume uniqueness NOT a SCSI requirement for these fields; V2 pattern correct. **REBUILD trigger**: if ANA-reporting or multi-tenant isolation starts requiring per-subsystem distinguishability, reclassify to REBUILD (sha256-derive like Serial/NAA). See `bugs/inventory/nvme-test-coverage-deferred.md` T3-DEF-5 | none (no regression needed; discipline row) |

**Latent-gap audit note (Tier A retrofill finding)**: Parallel entities — Model string (`"BlockVol"`), Vendor ID (`"SeaweedFS"`), Firmware Rev — are currently still **hardcoded constants** (same pattern V2 had). Not a bug today (per-volume uniqueness not required for Model/Vendor), but SHOULD be explicitly marked PORT-AS-IS (not forgotten-without-review) to avoid future "second Addendum A" if e.g. ANA or multi-tenant requirements make them matter later. **Inventory row T3-DEF-5** added to `bugs/inventory/nvme-test-coverage-deferred.md`: "iSCSI VPD Model/Vendor/FW-Rev still V2-hardcoded; audit if per-subsystem uniqueness becomes required".

#### §2.2.14 T2 NVMe Session state (KATO / AER / CNTLID — beyond §2.2.2 Controller-split row)

- **V2 analog**: V2 `Controller.kato` / `Controller.pendingAER` / `Controller.cntlID`
- **Scope**: session
- **Lifecycle owner**: Session init on admin Connect; destroyed on Session.Close
- **Concurrency**: accessed from session serve loop; AER slot is atomic.Value for single-slot check
- **Cross-session behavior**: dies with session
- **Bridge shape**: 1:1 (fields live inside Controller (V2) → inside Session (V3))
- **V3 embedding note**:
  - *Timing model shift*: V2 KATO fired a timer goroutine; V3 T3-scope stores KATO value only (no timer). Revisit if m01 exposes need (per BUG-003 Discovery Bridge scope limitation)
  - *Event model shift (AER)*: V2 AER had full event source (namespace attr change, etc.); V3 AER is **park-only** stub (no events produced); advertised capability bits in Identify Controller deliberately 0 (INV-NVME-IDENTIFY-CTRL-ADVERTISED-LIST-001)

**Contracts**:

| ID | Statement | V3 verdict | Rationale + impl | Test anchor |
|---|---|---|---|---|
| C1-NVME-SESSION-KATO-STORED-NOT-ENFORCED | KATO from Fabric Connect CDW12 stored; no timer enforcement in T3 scope | PRESERVE-partial (REBUILD needed vs V2 full-timer path) — **VIOLATED today; tracked by BUG-006** | `session.go` stores `katoMS` but never arms watchdog. Contract gap surfaced by m01 Matrix D kernel warning `long keepalive RTT (2522123190 ms)`. Target G21 / post-T3 | `PCDD-NVME-FABRIC-CONNECT-KATO-001` + `bugs/006_nvme_kato_timer_not_enforced.md` |
| C2-NVME-SESSION-AER-PARK-ONLY | AER request parked in single slot; never emits events; slot cleared on session close | REBUILD (V2 full-event-source → V3 park-only) | `session.go` tryParkAER + clearPendingAER | `TestT3b_NVMe_AER_Parks` + OAES advertised=0 pin |
| C3-NVME-SESSION-CNTLID-ECHO | IO Connect must echo admin CNTLID; mismatch rejected | NEW (V3 strict; V2 was looser) | `session.go` handleIOConnect + Target.ctrls lookup | `PCDD-NVME-FABRIC-CNTLID-ECHO-001` |
| C4-NVME-SESSION-SERIAL-PDU-LOOP | rxLoop single-threaded; same C3-CONTROLLER-RXLOOP-SERIAL but session-scope view | PRESERVE | same as §2.2.2 C3 | BUG-001 test family |
| C5-NVME-SESSION-STATE-CLEANUP-ON-CLOSE | Contract: all session-scope state (KATO, AER slot, CNTLID reservation, pendingCapsules) MUST be released by Session.Close; leaks across sessions forbidden | NEW-explicit (T3-DEF-6 retrofit) — **pin strength today: smoke + goroutine-leak guard only**; full state-release introspection NOT exercised | `session.go` Close path releases each; behavior today confirmed via indirect observation (no goroutine leak across 20 open/close cycles; CNTLID allocator strictly monotonic across 8 cycles with no zero-sentinel / no wedge). Test-only introspection of Target.ctrls map size, AER slot occupancy, and stored KATO ms is **not** exercised — queued as follow-up if/when BUG-006 timer work touches this area or a test-only introspection surface is added | `TestT3_NVMe_Session_CleanupOnClose_NoLeaksAcrossCycles` + `TestT3_NVMe_Session_CleanupOnClose_CNTLIDAllocator` (`t3_qa_session_cleanup_addendum_test.go` at `seaweed_block@313dd52`) |

**Latent-gap audit note (closed 2026-04-22)**: C5 (cleanup-on-close) was formerly an implicit contract tested only via goroutine leak count. Now explicit as a catalogue row with smoke + leak-guard pinning via T3-DEF-6 addendum; the full contract (state-release introspection) is documented but not strongly pinned — queued for post-BUG-006 follow-up. C1 (KATO enforcement) was previously tagged PRESERVE-partial assuming revisit later; Matrix D m01 evidence proves the contract is already **violated** — reclassified with BUG-006 anchor (T3-DEF-7).

### §2.3 Known drift events mapped to this catalogue (audit trail)

| Event | Root entity | Missed discipline | Catalogue row |
|---|---|---|---|
| BUG-001 (serial-vs-pipelined R2T) | Controller (split) | rxLoop concurrency verdict not written; split mis-assigned pipelining to goroutines | C3-CONTROLLER-RXLOOP-SERIAL + C6-CONTROLLER-R2T-BEFORE-H2CDATA |
| BUG-005 (Backend.Close in session) | Subsystem.Dev (merge) | Lifetime contract not transferred explicitly during merge to DurableProvider | C1-SUBSYS-DEV-LIFETIME |
| Addendum A (VPD 0x80 Serial stub) | iSCSI VPD/Identify metadata (§2.2.13) | V2 hardcoded stub treated as PORT-AS-IS; actually needed REBUILD (derive per-VolumeID) | C1-ISCSI-VPD80-SERIAL-DERIVATION (REBUILD verdict) |
| BUG-001 (revert) | §2.2.2 Controller split | rxLoop concurrency verdict wrongly BREAK in initial fix | C3-CONTROLLER-RXLOOP-SERIAL + C6-CONTROLLER-R2T-BEFORE-H2CDATA (now PRESERVE verdicts locked) |
| BUG-005 | §2.2.1 Subsystem.Dev merge + §2.2.8 Backend + §2.2.12 iSCSI Session + §2.2.2 NVMe Controller | Lifetime contract dropped in Subsystem→Provider merge; session Close path wrongly touched Backend | C1-SUBSYS-DEV-LIFETIME + C1-BACKEND-CLOSE-PROVIDER-OWNED + C1-ISCSI-SESSION-NOT-CLOSE-BACKEND (all now PRESERVE / NEW-explicit) |
| T3-rev-1 scope drift (Perf → G4) | Track identity | Not an entity drift but a roadmap drift; covered by §8C.3 trigger + architect review, not by this catalogue |
| BUG-006 (KATO timer not enforced) — T3-DEF-7 | §2.2.14 NVMe Session state | C1-NVME-SESSION-KATO row originally tagged PRESERVE-partial deferring timer work to "follow-up"; m01 Matrix D kernel warning proves contract is violated today, not deferred-safe | C1-NVME-SESSION-KATO-STORED-NOT-ENFORCED (reclassified to VIOLATED with BUG-006 anchor) |
| BUG-007 (walstore umount+remount data loss) | §2.2.5 Superblock + §2.2.4 GroupCommitter — **walstore impl side** | Cross-session durability under fs-triggered umount sync was never explicitly contracted for the walstore impl; smartwal happens to satisfy, walstore happens to fail | (new PROVISIONAL row to add post-T3): C?-DURABLE-READ-SERVES-UNCOMMITTED-WAL (walstore gap) |
| T3-DEF-5 (iSCSI VPD static fields — model/vendor/firmware) | §2.2.13 iSCSI VPD metadata | Reviewed during Addendum A but not catalogued; risk of future drift if multi-tenant isolation need arises | C4-ISCSI-VPD-STATIC-FIELDS-HARDCODED (PORT-AS-IS LOCKED explicit) |
| T3-DEF-6 (NVMe session cleanup contract implicit) | §2.2.14 NVMe Session state | Cleanup-on-close was tested implicitly via goroutine leak, never explicit contract row | C5-NVME-SESSION-STATE-CLEANUP-ON-CLOSE (NEW-explicit; QA L1 addendum pins it) |

---

## §3 Replication (T4 / G5)

**Status**: LIVING — T4 rounds 1–11 consolidated 2026-04-23; architect re-read pending before T4 T-start three-sign; architect-line items A/B/C at sketch §6.1 still open for sign.

**Scope**: T4 delivers primary-writes-to-N-replicas with explicit durability ack and short-disconnect tolerance. Rebuild beyond WAL retention → T5. Failover → T6.

### §3.0 Framing

This section consolidates T4 port judgments from the six T4-specific docs into the canonical catalogue format:

- `v3-phase-15-t4-l1-survey.md` — V2 entity inventory (§2.1–§2.10)
- `v3-phase-15-t4-v3-entity-skeleton.md` — V3 entity reality (layers A–E) + skeleton signatures
- `v3-phase-15-t4-function-level-audit.md` — 24-method action classification + batch implications
- `v3-phase-15-t4-sketch.md` — T4 scope/gate/batches
- `v3-phase-15-t4a-mini-plan.md` — T4a task list + Gates
- `v3-phase-15-t4a-2-g1-v2-read.md` — V2 Ship source read + Option B shape

#### §3.0.1 Architect-LOCKed framings (round 4)

- **H5 LOCK**: Replication wire frames carry authority facts explicitly; no primary-side cache is correctness source. Two invariants inscribed across §3 rows: `INV-REPL-ACK-FRAME-IS-AUTHORITY` + `INV-REPL-CACHE-ADVISORY-ONLY`.
- **Q5 LOCK**: V3 replication wire is clean-break from V2; versioned envelope `{magic, protocol_version, message_type, flags_or_reserved}` from day one. No V2 replica interop in T4. Envelope **landed at T4a-1** (commit `seaweed_block@56ad349`): 12B preamble = 4B magic `SWRP` + 1B protocol_version=1 + 1B message_type + 1B flags + 1B reserved + 4B length. Architect acceptance: the landed shape matches the Q5 LOCK recommendation as drafted in sketch §6.1 Item C.1; any future revision requires a §8C.3 Discovery Bridge (not a plain sketch revision). Sketch §6.1 Item C remaining open surface is narrowed to C.2 (BarrierResponse epoch echo) + C.3 (ProbeResponse epoch echo) only — the envelope layout question is no longer an architect-line decision surface.
- **Item B pending**: T4c catch-up semantics — Path A (retained-WAL delta, preserves T5 self-definition) vs Path B (full-transfer, defers retention-window + T5 rebuild boundary to P10). Architect decides at T4 T-start three-sign.

#### §3.0.2 Substrate discipline (inherited from T3)

- **smartwal** = sole product-signing substrate for G5; walstore is non-gating secondary-matrix coverage until BUG-007 closes. T4 does NOT silently re-open BUG-007 as a hidden blocker.

#### §3.0.3 V3 model shifts relevant to replication

- **Authority model**: V2 primary advanced epoch locally on promotion; V3 master authority publishes, replica storage is passive recipient. Per-frame epoch on wire is correctness source (H5 LOCK).
- **Event model**: V2 replication callbacks (barrier fn, degradation fn) → V3 typed error return + ctx cancellation + `transport.OnSessionClose/OnFenceComplete` hooks.
- **Storage model**: V2's local `BlockVol` was monolith (engine + replication + host-session); V3 decomposes — `DurableProvider` owns engine lifecycle, `ReplicationVolume` owns replication lifecycle, `StorageBackend` owns host-session I/O. BUG-005 lesson drives "borrowed vs owned" contract on every new public method (Gate G-2).
- **Lifecycle model**: V2 shippers/receivers had internal dial/reconnect logic; V3 `core/transport/` is sole `net.Dial` + frame-I/O owner (all peer execution goes through `*BlockExecutor` public methods).
- **Concurrency model**: V2 shipper had bounded in-flight state with per-peer mutex; V3 preserves bounded in-flight but uses typed ctx cancellation and `core/transport/` session/conn attach/detach helpers.

#### §3.0.4 Status legend (for the per-entity rows below)

- **✓ PORTED** — V3 code exists and merged on `phase-15` branch
- **⊙ DECIDED-NOT-CODED** — T4 plan names concrete shape + action; body to be written per T4a/b/c mini-plans
- **⏭ DEFERRED-T5** — explicit non-claim in T4; rebuild-lane or post-G5
- **~ PARTIAL** — some parts PORTED, others DECIDED-NOT-CODED

---

### §3.1 Entity bridge map (V2 → V3)

Ten V2 stateful entities (per L1 survey §2) + four V3-native entities (per skeleton §2.E). Consolidated verdicts — see §3.2 per-entity rows for detail.

| L1 ref | V2 entity | V2 scope | Bridge shape | V3 home | T4 status |
|---|---|---|---|---|---|
| §2.1 | WALShipper | per-replica | **split + partial-port** | `transport.BlockExecutor.Ship` (new method, T4a-2) + `ReplicaPeer` state (T4a-3) | ~ PARTIAL |
| §2.2 | ShipperGroup | per-volume | **merge into new V3 entity** | `ReplicationVolume` (new, T4a-4) | ⊙ DECIDED |
| §2.3 | ReplicaReceiver | per-volume (replica-side) | **split — wire/frame + storage-apply already V3-covered; state-machine to T4c** | `transport.ReplicaListener` (V3-covered) + `storage.LogicalStorage.ApplyEntry` (V3-covered) + `ReplicaPeer` state (T4c adds full state machine) | ~ PARTIAL |
| §2.4 | ReplicaBarrier FSM | per-request closure + per-volume queue | **merge into new V3 entity** | `DurabilityCoordinator.SyncLocalAndReplicas` + per-peer barrier calls (T4b) | ⊙ DECIDED |
| §2.5 | DistGroupCommit | per-write closure | **1:1-shift into new V3 entity** (closure → entity; same math) | `DurabilityCoordinator.EvaluateBarrierAcks` (T4b) | ⊙ DECIDED |
| §2.6 | RebuildSession | per-session volatile | **merge into new T5 entity** | `RebuildCoordinator` (T5) | ⏭ DEFERRED-T5 |
| §2.7 | RebuildServer | per-primary listener | **split — listener already V3-covered via `BlockExecutor.StartRebuild`; policy to T5** | `transport.BlockExecutor.StartRebuild` (V3-covered, semantic-gap per §3.2.7) + `RebuildCoordinator` policy (T5) | ~ PARTIAL |
| §2.8 | RebuildTransportServer | per-session | **already V3-covered** | `transport.rebuild_sender.go` internals (private) | ✓ PORTED |
| §2.9 | RebuildTransportClient | per-session replica-side | **already V3-covered** | `transport.ReplicaListener` receive path (V3-covered) | ✓ PORTED |
| §2.10 | RebuildBitmap | per-session volatile | **move to T5 entity** | `RebuildCoordinator` internal state (T5) | ⏭ DEFERRED-T5 |
| — | (new) ReplicationVolume | per-volume | **V3-native, no V2 analog** (closes gap left when ShipperGroup merged) | `core/replication/volume.go` (new package, T4a-4) | ⊙ DECIDED |
| — | (new) ReplicaPeer | per-replica | **V3-native, no V2 analog** (collects per-peer state the V2 WALShipper carried implicitly) | `core/replication/peer.go` (T4a-3, T4b extends, T4c state machine) | ⊙ DECIDED |
| — | (new) DurabilityCoordinator | per-volume | **V3-native, no V2 analog** (replaces V2's `vol.writeSync` closure binding) | `core/replication/durability.go` (T4b) | ⊙ DECIDED |
| — | (new) RebuildCoordinator | per-volume | **V3-native, no V2 analog** | `core/replication/rebuild.go` (T5) | ⏭ DEFERRED-T5 |

---

### §3.2 Per-entity detail

Each row carries V2 attributes (from L1 survey) + V3 home + per-contract verdict + test anchor. Contracts marked with LOC and status.

#### §3.2.1 WALShipper (L1 §2.1) — split + partial-port

- **V2 file**: `weed/storage/blockvol/wal_shipper.go` (lines 57–912)
- **V2 scope**: per-replica; one goroutine set per replica in fan-out
- **Bridge shape**: split — V2's `Ship()` kernel (98 LOC per G-1 V2 read) decomposes into multiple V3 homes; state machine lifts to `ReplicaPeer`
- **T4 status**: ~ PARTIAL — T4a-2 adopts **Option B** (architect round 11) per §3.4 drift event; sw coding in progress on `phase-15` branch; round-11 Gate G-1 re-signed

**Contracts**:

| ID | Statement | V3 verdict | Rationale + impl | Test anchor | Status |
|---|---|---|---|---|---|
| C1-SHIP-EPOCH-EQ-SILENT-DROP | Stale-epoch entry dropped silently (`return nil`); caller treats as success; V2 line 217 | **PRESERVE** verbatim | `BlockExecutor.Ship` at `ship_sender.go:89-95` checks `lineage.Epoch != session.lineage.Epoch`; logs + returns nil. Source of epoch: frame-borne lineage (H5 LOCK compliant), replacing V2's `epochFn()` callback | `TestExecutor_Ship_StaleEpoch_SilentDrop` (landed `seaweed_block@043b9f7`) | ✓ PORTED T4a-2 |
| C2-SHIP-3S-WRITE-DEADLINE | `conn.SetWriteDeadline(time.Now().Add(3*time.Second))` around `WriteMsg`; V2 line 240 | **PRESERVE** byte-identical | `ship_sender.go:143` via `shipWriteDeadline` const; set before write, cleared after | `TestExecutor_Ship_WriteDeadline_Fires` landed `seaweed_block@8fed2a8`; uses `net.Pipe()` to bypass kernel-buffer auto-tuning; three-way assertion (err≠nil + elapsed≥2.5s + elapsed≤10s) proves deadline IS the unblocking mechanism. Runs 3.00s exactly on Windows loopback | ✓ PORTED T4a-2 |
| C3-SHIP-LAZY-DIAL-ON-REGISTERED-SESSION | `ensureDataConn` — if the **already-registered** session's `conn` is nil, `net.DialTimeout` + attach; V2 line 246. **Scope boundary**: Ship does NOT bootstrap session creation — if the session is not registered, Ship returns error and does not auto-register. Session registration is done by `Probe` / `StartCatchUp` / `StartRebuild` / future steady-state-attach path | **PRESERVE** per Option B (round 11) | `BlockExecutor.Ship` body at `ship_sender.go:87-90` returns error on unknown session; at `ship_sender.go:107-135` lazy-dials only when `session.conn == nil` for an already-registered session. Was NEARLY dropped at round 10 (Option C moved dial to ReplicaPeer layer); architect corrected at round 11 — "transport = sole net.Dial owner" layering principle applies, composite stability requires lazy-dial to stay in muscle | `TestExecutor_Ship_LazyDial_OnRegisteredSessionWithoutConn` + `TestExecutor_Ship_LazyDial_DialFailure_ReturnsError` + `TestExecutor_Ship_NoSession` (T4a-2, landed `seaweed_block@043b9f7`) | ✓ PORTED T4a-2 |
| C4-SHIP-NO-HARD-STOP | Ship failure returns error without panicking upstream caller; caller degrades peer and continues | **PRESERVE** — composite with C5 (handoff to peer-layer degradation) | Architect round-11 clarified: V2 `Ship()` does NOT retry in-call; it returns + caller (V2 shipper state machine) calls `markDegraded`. V3 Ship returns error; `ReplicaPeer.ShipEntry` (T4a-3) catches + calls `Invalidate`. **Behavior delta noted**: V2 returned nil after markDegraded (no error bubbled); V3 returns error because BlockExecutor does not own ReplicaState. Upstream composite-behavior preserved because ReplicaPeer layer absorbs the error (forward-carry CARRY-1) | `TestExecutor_Ship_ConnFailure` (T4a-2 `043b9f7`); `TestReplicaPeer_ShipEntry_ConnFailure_MarksDegraded` (T4a-3 `99c4e1d`) proves Healthy→Degraded via Invalidate | ✓ PORTED T4a-3 (CARRY-1 closed round 13) |
| C5-SHIP-REPLICA-FLUSHED-LSN-MONOTONIC | `replicaFlushedLSN` advances via `CompareAndSwap` forward-only; V2 line 414 | **DEFER to T4b** (barrier mode work; not T4a) | `ReplicaPeer.durableLSN` field; CAS forward-only on barrier ack | T4b barrier test (TBD) | ⏭ T4b |
| C6-SHIP-BARRIER-OK-WITHOUT-FLUSHED-LSN-NOT-SYNCALL | `BarrierOK` without `FlushedLSN` MUST NOT count toward `sync_all`; V2 legacy-replica safety | **DEFER to T4b** | Decision encoded in `DurabilityCoordinator.EvaluateBarrierAcks` quorum math | T4b barrier test | ⏭ T4b |
| C7-SHIP-MAX-CATCHUP-RETRIES-3 | After 3rd catch-up fail, escalate to `ReplicaNeedsRebuild`; V2 line 286 | **DEFER to T4c** | T4c recovery policy owns this (via `RebuildCoordinator.Decide` call path) | T4c recovery test | ⏭ T4c |
| C8-SHIP-ACTIVE-REBUILD-SESSION-FLAG | `activeRebuildSession` atomic prevents stale state transitions from opening live lane; V2 line 143 | **DEFER to T5** (coupled to RebuildSession) | Rebuild-lane concern | T5 rebuild tests | ⏭ T5 |

**Partial-port audit (round 11)**: V2 Ship's 5 concerns — (1) stale-epoch drop → PRESERVE in C1; (2) 3s deadline → PRESERVE in C2; (3) lazy-dial → PRESERVE in C3 (after architect round 11); (4) ReplicaState machine → `ReplicaPeer.state` one layer up (same gate); (5) liveShippingPolicy gate → T4b. LOC estimate ~50 (was ~30 at round-10 Option C; revised to ~50 after Option B adoption).

#### §3.2.2 ShipperGroup (L1 §2.2) — merge into ReplicationVolume

- **V2 file**: `weed/storage/blockvol/shipper_group.go` (lines 19–408)
- **V2 scope**: per-volume (one group per primary); spans N configured replicas
- **V2 external dep**: RF = replica count — comes from master assignment via `BlockVol.SetReplicaAddrs([]string)`; group resizing is an assignment-op
- **Bridge shape**: merge — ShipperGroup's responsibilities decompose into `ReplicationVolume` (T4a-4; fan-out owner) + `DurabilityCoordinator` (T4b; quorum math)
- **T4 status**: ⊙ DECIDED — T4a-4 ships `ReplicationVolume` MVP

**Contracts**:

| ID | Statement | V3 verdict | Rationale + impl | Test anchor | Status |
|---|---|---|---|---|---|
| C1-SHIPPERGROUP-RF-EXTERNAL | N = RF supplied from master assignment via `UpdateReplicaSet`; group resizing = teardown+rebuild, never internal | **PRESERVE** — cross-entity contract: master assignment ↔ `ReplicationVolume.peers` map size ↔ `ReplicaListener` expected-connection-count ↔ `DurabilityCoordinator` quorum arithmetic | `ReplicationVolume.UpdateReplicaSet(targets []ReplicaTarget)` called from Host authority-callback path | `TestReplicationVolume_UpdateReplicaSet_AddPeer` + `_RemovePeer_ExecutorTornDown` (T4a-4) | ⊙ T4a-4 |
| C2-SHIPPERGROUP-BARRIER-ALL-PARALLEL | N goroutines in parallel via `WaitGroup`; accessors use `RWMutex.RLock` | **PRESERVE** — V3 `DurabilityCoordinator.SyncLocalAndReplicas` spawns N goroutines | T4b `DurabilityCoordinator` impl | T4b barrier parallel test | ⏭ T4b |
| C3-SHIPPERGROUP-MIN-REPLICA-FLUSHED-LSN-ALL | `MinReplicaFlushedLSNAll` returns (0, false) unless EVERY shipper reports progress | **PRESERVE** — quorum-arithmetic input | T4b `DurabilityCoordinator` | T4b quorum test | ⏭ T4b |
| C4-SHIPPERGROUP-RETENTION-BUDGET-TRANSITIONS-UNDER-RLOCK | `EvaluateRetentionBudgets` can flip shipper state to `ReplicaNeedsRebuild` under `RWMutex.RLock` | **DEFER to T4c** — retention-window behavior tied to Item B decision | `ReplicationVolume.EvaluatePeerProgress` (T4c) | T4c peer-state test | ⏭ T4c |
| C5-SHIPPERGROUP-DOUBLE-WATERMARK | `MinShippedLSN` (diagnostic, Ceph retention) vs `MinReplicaFlushedLSNAll` (authoritative durability) — two consumers, two semantics | **PRESERVE** — L1 §2.2 invariant #3 | Shipped LSN not tracked in T4a; both watermarks exposed in T4b via `DurabilityCoordinator` query methods | T4b watermark test | ⏭ T4b |
| C6-SHIPPERGROUP-ANY-HAS-FLUSHED-PROGRESS-SEED | Seeded on reassignment to detect cold-start shippers | **PRESERVE** | `ReplicationVolume.UpdateReplicaSet` initializes new `ReplicaPeer` with `lastProbe.ReplicaFlushedLSN = 0, ProgressSeen = false` | T4c reassignment test | ⏭ T4c |

#### §3.2.3 ReplicaReceiver (L1 §2.3) — split; wire + apply V3-covered; full state machine T4c

- **V2 files**: `weed/storage/blockvol/replica_apply.go` (lines 20–425) + `replica_barrier.go` (lines 10–204)
- **V2 scope**: per-volume replica-side singleton
- **Bridge shape**: split — network/frame-handling is covered by `transport.ReplicaListener`; storage-apply is covered by `LogicalStorage.ApplyEntry`; state machine (`receivedLSN`/`flushedLSN` tracking + barrier cond + `ioMu.RLock` nesting) distributes across T4b (barrier) + T4c (state machine)
- **T4 status**: ~ PARTIAL — wire/apply PORTED; full state-machine DECIDED (T4c) with bits in T4a

**Contracts**:

| ID | Statement | V3 verdict | Rationale + impl | Test anchor | Status |
|---|---|---|---|---|---|
| C1-REPLICARECV-STATE-ON-ENGINE-NOT-SINGLETON | V2 kept `receivedLSN`/`flushedLSN` on a per-volume `ReplicaReceiver` singleton that outlived promote/demote (`blockvol.go:1515`). V3 has no equivalent singleton: replica-side LSN counters are carried by `LogicalStorage` (one per `DurableProvider` cached volume handle) and surfaced via `LogicalStorage.Boundaries()` | **REBUILD** — container shape changed; contract surface changed | V3 impl: `LogicalStorage.Boundaries()` returns `(R, S, H)` tuple; `transport.ReplicaListener` reads/advances via `ApplyEntry`/`AdvanceFrontier`. **Residual property**: "LSN state survives across an assignment churn that keeps the same volume cached in Provider" follows from `DurableProvider.volumes` map — BUG-005 regression indirectly fences a related lifetime property, but does NOT prove the specific demote→promote→read-LSN-unchanged sequence for replica-side state | **Proof coverage is narrow today**: `TestT3_Durable_RecoverThenServe` proves *process-restart* durability only, not *in-process demote/promote* lifetime. A dedicated proof — e.g. `TestT4c_ReplicaLSN_SurvivesDemotePromote` — is scoped for **T4c** when the full state machine lands | ⏭ T4c (proof); ✓ engine-side surface is PORTED via T3 |
| C2-REPLICARECV-CONTIGUOUS-LSN | `entry.LSN == receivedLSN+1` fence; gaps → `ErrDuplicateLSN`; V2 line 306 | **PRESERVE** | `LogicalStorage.ApplyEntry` enforces contiguous LSN | `walstore` / `smartwal` apply tests (V3-covered) | ✓ PORTED |
| C3-REPLICARECV-EPOCH-FENCE | `entry.Epoch` vs `vol.epoch.Load()`; mismatch → `ErrStaleEpoch` silent drop; V2 line 294 | **PRESERVE** — same silent drop as C1-SHIP | Replica-side epoch source: receiver's most-recent observed lineage from `RecoveryLineage` frame (H5 LOCK compliant) | T4c apply test covering epoch mismatch on replica | ⊙ T4c |
| C4-REPLICARECV-BARRIER-3-PHASE | Wait-LSN → `fd.Sync()` → atomically advance `flushedLSN`; V2 `replica_barrier.go:147-204` | **PRESERVE** across `DurabilityCoordinator` + `LogicalStorage.Sync` | Barrier arrives over wire; `ReplicaListener` routes to Sync; `BarrierResponse` includes epoch echo (H5 LOCK; §3.3 C.2) | T4b barrier test | ⊙ T4b |
| C5-REPLICARECV-IOMU-RLOCK-NESTING | `ioMu.RLock` held for entire apply path; released inside `replicaAppendWithRetry` during WAL-full wait; V2 line 287 | **DEFER to T4c** — if needed; V3's `LogicalStorage.ApplyEntry` may already satisfy via internal flusher synchronization (verify at T4c) | L1 §2.3 invariant #5; verify V3 equivalent during T4c | T4c concurrent apply test | ⏭ T4c |
| C6-REPLICARECV-WAL-FULL-RETRY-RELEASES-MU | `replicaAppendWithRetry` releases `mu` on full → re-acquires after flusher notify | **DEFER to T4c** (if needed; V3 flusher may have different pressure-release seam) | L1 §2.3 invariant #4 | T4c WAL-full test | ⏭ T4c |
| C7-REPLICARECV-REBUILD-SESSION-ROUTING | Entries during `RebuildPhaseRunning` route to session WAL lane; else normal apply | **DEFER to T5** | Rebuild-lane | T5 | ⏭ T5 |

#### §3.2.4 ReplicaBarrier FSM (L1 §2.4) — merge into DurabilityCoordinator

- **V2 file**: `weed/storage/blockvol/replica_barrier.go` (lines 147–204)
- **V2 scope**: per-request call-closure, BUT queue-state shared per-volume via `cond.Wait()` (L1 §2.4 clarified scope)
- **Bridge shape**: merge — barrier FSM responsibilities absorb into `DurabilityCoordinator.SyncLocalAndReplicas` closure + per-peer wire round-trip
- **T4 status**: ⊙ DECIDED — T4b

**Contracts**:

| ID | Statement | V3 verdict | Rationale + impl | Test anchor | Status |
|---|---|---|---|---|---|
| C1-BARRIER-3-PHASE | Wait-LSN-receipt → fsync → advance `flushedLSN` | **PRESERVE** (from §3.2.3 C4) | `DurabilityCoordinator.SyncLocalAndReplicas` drives; `LogicalStorage.Sync` performs fsync | T4b barrier 3-phase test | ⊙ T4b |
| C2-BARRIER-TIMEOUT-5S-HARDCODED | `barrierTimeout=5s` — V2 line 18 hardcoded | **REBUILD** — V3 elevates to config (context-cancellable; default 5s) | `ctx` arg on `SyncLocalAndReplicas` | T4b timeout test | ⊙ T4b |
| C3-BARRIER-COND-MULTI-WATCHER | Two barriers in quick succession share cond; first fsync satisfies both LSN requirements | **REBUILD** — V3 uses ctx + goroutine per barrier (simpler, no cond needed) | Per-call goroutine in `DurabilityCoordinator` | T4b concurrent-barrier test | ⊙ T4b |
| C4-BARRIER-LEGACY-1B-RESPONSE | V2 supports 1-byte response from old replica decoder at `repl_proto.go:64` | **BREAK** per Q5 LOCK (clean-break wire) | V3 `BarrierResponse` structured + versioned | — (no V2 interop claimed) | ✓ decided |
| C5-BARRIER-RESP-FULL-LINEAGE-ECHO | **Architect round-21 decision text (verbatim; drops into T4b mini-plan §2 T4b-1)**: "`BarrierResponse` adopts **full `RecoveryLineage` echo** and extends its payload to **`[32B lineage][8B achievedLSN]`**, carried inside the existing `MsgBarrierResp` body; the 12B envelope preamble remains unchanged. Encode/decode is strict and fail-closed: payloads shorter than 40 bytes, zero-valued or malformed lineage fields, or decode layouts that do not exactly match the field order `SessionID, Epoch, EndpointVersion, TargetLSN, AchievedLSN` MUST be rejected as invalid barrier acknowledgements and MUST NOT contribute to correctness decisions. Receiver-side filtering MUST treat the echoed lineage as the authority identity of the ack: a barrier ack only counts if its full lineage matches the request/session the caller is awaiting; mismatched, stale, or partially-zeroed lineage is advisory for logging only and MUST NOT be accepted. `T4b-2` may initially key its coordinator logic primarily on epoch when evaluating durability progress, but it MUST preserve the stronger rule that only full-lineage-valid acks are eligible for counting; no future optimization may weaken this to epoch-only acceptance. Diagnostics should log peer ID plus the full expected/actual lineage tuple on mismatch so asymmetric authority bugs are observable without relying on cached primary-side assumptions." | **NEW-explicit** per H5 LOCK + architect round-21 sign + uniform rule | See verbatim decision text above. Rule applies uniformly across all barrier-ack-like surfaces per `INV-REPL-LINEAGE-BORNE-ON-BARRIER-ACK` uniform clause | `TestDurabilityCoordinator_BarrierAck_ShortPayload_Rejected` + `_ZeroedLineage_Rejected` + `_MismatchedLineage_NotCounted` + `_DiagnosticLogFormat` (T4b-1) | ⊙ T4b-1 |

#### §3.2.5 DistGroupCommit (L1 §2.5) — 1:1-shift to DurabilityCoordinator

- **V2 file**: `weed/storage/blockvol/dist_group_commit.go` (lines 15–83)
- **V2 scope**: per-write-operation ephemeral closure bound once to `vol.writeSync` at volume init
- **Bridge shape**: 1:1-shift — closure becomes entity (`DurabilityCoordinator`); math preserved verbatim; mode selectable at call time instead of bound at init
- **T4 status**: ⊙ DECIDED — T4b

**Contracts**:

| ID | Statement | V3 verdict | Rationale + impl | Test anchor | Status |
|---|---|---|---|---|---|
| C1-DGC-QUORUM-ARITHMETIC | `rf = group.Len() + 1`; `quorum = rf/2 + 1`; primary counts as 1 durable node | **PRESERVE** pure math | `DurabilityCoordinator.EvaluateBarrierAcks(mode, rf, acks)` | `TestDurabilityCoordinator_Quorum_RF3` + `_RF5` (T4b) | ⊙ T4b |
| C2-DGC-SYNC-ALL-FAIL-ON-ANY-BARRIER-FAILURE | Any peer barrier failure → write fails; no silent swallow | **PRESERVE** | `EvaluateBarrierAcks` returns error on first ack.Success=false under `DurabilitySyncAll` | T4b sync_all failure test | ⊙ T4b |
| C3-DGC-SYNC-QUORUM-FAIL-ON-INSUFFICIENT | `durableNodes < quorum` → `ErrDurabilityQuorumLost` | **PRESERVE** | `EvaluateBarrierAcks` under `DurabilitySyncQuorum` | T4b quorum-loss test | ⊙ T4b |
| C4-DGC-BEST-EFFORT-SILENCE-ON-FAIL | Peer barrier failures logged + `degradeReplica` called; write NOT failed | **PRESERVE** | `EvaluateBarrierAcks` under `DurabilityBestEffort`; calls `ReplicaPeer.Invalidate` | T4b best_effort test | ⊙ T4b |
| C5-DGC-EPOCH-ACK-VALIDITY | Stale-epoch ack MUST NOT count toward quorum (H5 LOCK) | **PRESERVE** + **NEW INV** per H5 LOCK | `EvaluateBarrierAcks` filters `ack.Epoch != primary.Epoch.Load()` from quorum count; frame-borne epoch is source (`INV-REPL-ACK-FRAME-IS-AUTHORITY`) | `TestDurabilityCoordinator_StaleEpochAck_NotCounted` (T4b) | ⊙ T4b |
| C6-DGC-PARALLEL-LOCAL-AND-REMOTE | Parallel local `walSync` + `group.BarrierAll` via `WaitGroup` | **PRESERVE** | `SyncLocalAndReplicas` spawns local + per-peer goroutines | T4b parallel-sync test | ⊙ T4b |

#### §3.2.6 RebuildSession (L1 §2.6) — deferred T5

- **V2 file**: `weed/storage/blockvol/rebuild_session.go` (lines 41–327)
- **V2 scope**: per-replica-session; volatile (non-durable across crash)
- **Bridge shape**: merge into T5 `RebuildCoordinator`
- **T4 status**: ⏭ DEFERRED-T5 — non-durable across crash per L1 §2.6 invariant (sw round-3 verified `rebuild_bitmap.go` has zero file I/O)
- **Contracts**: all deferred to T5. Notable: phase FSM (Idle→Accepted→Running→BaseComplete→Completed|Failed); bitmap WAL-wins rule; hydration guard fails closed if local checkpoint > baseLSN; bitmap-set-precedes-ack.

#### §3.2.7 RebuildServer (L1 §2.7) — split; listener V3-covered, policy T5

- **V2 file**: `weed/storage/blockvol/rebuild.go` (lines 21–307)
- **V2 scope**: per-primary singleton TCP listener
- **Bridge shape**: split — TCP listener + per-request dispatch is absorbed by `transport.BlockExecutor.StartRebuild` (V3-covered, wire-identical frame types) + `transport.ReplicaListener`; rebuild policy + 2-phase coordination + snapshot-export → T5 `RebuildCoordinator`
- **T4 status**: ~ PARTIAL — wire PORTED; semantic gap noted; policy DEFERRED-T5

**Semantic gap** (from audit §6.3): V3 `BlockExecutor.StartCatchUp` AND `StartRebuild` both ship `primaryStore.AllBlocks()` (every LBA), not V2's retained-WAL-delta (`StartCatchUp` per V2 `StreamEntries`) or extent-bitmap (`StartRebuild` per V2 `handleFullExtent`). File comment at `catchup_sender.go:13-18` acknowledges: *"today catch-up ships every block in the primary store, not just the WAL window. Bounded incremental WAL streaming belongs to later execution-lifecycle work (P10)."* This is sketch §6.1 Item B — **architect decides at T4 T-start three-sign**:

- **Path A**: T4c upgrades `StartCatchUp` to retained-WAL delta + `ErrWALRecycled` escalation; preserves V2 parity; T5 rebuild boundary cleanly definable (`catch-up exhausted → rebuild`).
- **Path B**: T4c uses current full-store `StartCatchUp`; retention-window property deferred to P10; **T5 rebuild boundary also gated on P10** because "WAL exhausted" has no retention-window definition yet.

**Contracts**:

| ID | Statement | V3 verdict | Rationale + impl | Test anchor | Status |
|---|---|---|---|---|---|
| C1-REBUILDSVR-WIRE-FRAMES | `MsgRebuildBlock` / `MsgRebuildDone` / `MsgRebuildError` wire types | **PRESERVE** (semantic equivalent via V3 shape) | `transport.protocol.go` MsgRebuildBlock/MsgRebuildDone — Q5-envelope-compliant versions exist | `core/transport/transport_test.go` StartRebuild tests (V3-covered) | ✓ PORTED |
| C2-REBUILDSVR-EPOCH-FENCE | Request-time epoch validation; mismatch → `MsgRebuildError("EPOCH_MISMATCH")` | **PRESERVE** | `BlockExecutor.StartRebuild` carries lineage; replica-side validates | T5 rebuild-epoch test | ⏭ T5 |
| C3-REBUILDSVR-TWO-PHASE | WAL catch-up → extent → second catch-up (client-driven) | **REBUILD per Item B** — Path A preserves; Path B collapses to single-shot full-transfer | `RebuildCoordinator` (T5) decides phase sequence based on Item B outcome | T5 rebuild phase test | ⏭ T5 |
| C4-REBUILDSVR-FLUSH-BEFORE-STREAMING | ForceFlush before extent stream | **PRESERVE** | V3 `StartRebuild` calls `LogicalStorage.Sync` pre-stream (verify during T5 wire-up) | T5 rebuild-flush test | ⏭ T5 |
| C5-REBUILDSVR-EXTENT-BYPASS-DIRTY-MAP | V2 `readBlockFromExtent` bypasses dirty map (serves only flushed) | **REBUILD** — V3 `AllBlocks()` reads through dirty map per §2.3 V3 observation 14 | Semantic divergence called out in audit §3.14; decision at T5 via Path A/B outcome | T5 rebuild vs concurrent-write test | ⏭ T5 |
| C6-REBUILDSVR-SNAPSHOT-EXPORT | Snapshot manifest + image stream (CSI feature) | **DEFER** — not T4/T5; CSI snapshot is G10-era | — | — | ⏭ G10 |

#### §3.2.8 RebuildTransportServer (L1 §2.8) — V3-covered

- **V2 file**: `weed/storage/blockvol/rebuild_transport.go` (lines 152–246)
- **V2 scope**: per-session (one per base-lane connection)
- **Bridge shape**: already V3-covered — `transport.rebuild_sender.go` internals (private; not exposed)
- **T4 status**: ✓ PORTED (wire); policy ⏭ T5
- **Contracts**: all wire-level PRESERVE via `RecoveryLineage` + `MsgRebuildBlock` frames (Q5-compliant). Achieved-LSN semantic preserved (replica-side tracks). Base-stream-NOT-point-in-time property + two-line bitmap convergence → T5.

#### §3.2.9 RebuildTransportClient (L1 §2.9) — V3-covered

- **V2 file**: `weed/storage/blockvol/rebuild_transport.go` (lines 252–321)
- **V2 scope**: per-session replica-side
- **Bridge shape**: already V3-covered — `transport.ReplicaListener` receive path (routes `MsgRebuildBlock` to `LogicalStorage.ApplyEntry`)
- **T4 status**: ✓ PORTED (wire); session/policy → T5
- **Contracts**: frame-parsing PRESERVE; bitmap-conflict-skip → T5; graceful disconnect → T5.

#### §3.2.10 RebuildBitmap (L1 §2.10) — deferred T5

- **V2 file**: `weed/storage/blockvol/rebuild_bitmap.go` (~84 LOC; purely in-memory per L1 §2.10 corrected invariant)
- **V2 scope**: per-rebuild-session; volatile
- **Bridge shape**: move into T5 `RebuildCoordinator` internal state
- **T4 status**: ⏭ DEFERRED-T5
- **Contracts**: WAL-wins-over-base PRESERVE (T5 core invariant); hydration guard PRESERVE (T5); bit-set-precedes-ack PRESERVE (T5).

#### §3.2.11 (new V3) ReplicationVolume — no V2 analog

- **V3 file**: `core/replication/volume.go` (new package; T4a-4)
- **V3 scope**: per-volume coordination owner
- **Why new**: V2 `BlockVol` was monolith; V3 decomposes. `ReplicationVolume` closes the layer-E gap between layer A (authority consumption) + layer C (local engine) + layer D (per-peer transport). Nothing in V2 maps 1:1 — closest are fragments of `BlockVol.HandleAssignment` + `ShipperGroup` + `DistGroupCommit` binding.
- **T4 status**: ⊙ DECIDED — MVP lands T4a-4 (UpdateReplicaSet + OnLocalWrite best_effort); Sync/ProbeAll/EvaluatePeerProgress/ExecuteRecoveryPlan land T4b/T4c
- **Key contracts**:
  - **INV-REPL-LIFECYCLE-HANDLE-BORROWED-001** — BUG-005 non-repeat: `ReplicationVolume` borrows `LogicalStorage`, NEVER calls `store.Close()`. Enforced via Gate G-2 three-line godoc contract.
  - **INV-REPL-FANOUT-001** — Every acked primary write either ships to every eligible peer or reports ship failure; no silent drop.
  - **INV-REPL-PEER-LIFECYCLE-BY-UPDATE** — `UpdateReplicaSet` is sole entry point for peer add/remove; tears down executor on remove (Opt-3 explicit test `TestReplicationVolume_UpdateReplicaSet_RemovePeer_ExecutorTornDown`).

#### §3.2.12 (new V3) ReplicaPeer — no V2 analog

- **V3 file**: `core/replication/peer.go` (T4a-3 leaf; T4b adds Fence; T4c extends state machine)
- **V3 scope**: per-remote-replica runtime
- **Why new**: V2 `WALShipper` tangled per-peer state with transport + state machine + retention-budget + reconnect; V3 decomposes — transport in layer D, retention in `ReplicationVolume.EvaluatePeerProgress`, state-machine here. V2 approximate analog is `WALShipper` shorn of its non-peer-state concerns.
- **T4 status**: ⊙ DECIDED — T4a-3 leaf (Healthy/Degraded); T4b Fence; T4c full state machine (+CatchingUp, Rebuilding, NeedsRebuild)
- **Key contracts**:
  - ShipEntry carries full `RecoveryLineage` (`INV-REPL-LINEAGE-BORNE-ON-SHIPENTRY`)
  - Fence carries full `{sessionID, epoch, endpointVersion}` (`INV-REPL-LINEAGE-BORNE-ON-FENCE`)
  - ShipEntry error → `Invalidate` → Degraded (round-11 forward-carry CARRY-1)
  - Close is idempotent; tears down `*BlockExecutor` cleanly

#### §3.2.13 (new V3) DurabilityCoordinator — no V2 analog

- **V3 file**: `core/replication/durability.go` (T4b)
- **V3 scope**: per-volume quorum/durability closure
- **Why new**: replaces V2's `vol.writeSync` closure-bound-at-init pattern with call-time dispatch entity; same math (C1-DGC-QUORUM-ARITHMETIC PRESERVED verbatim).
- **T4 status**: ⊙ DECIDED — T4b
- **Key contracts**: all §3.2.5 C1–C6 install into this entity. BarrierResponse wire extension (§3.3 C.2) lands alongside to enable `INV-REPL-ACK-FRAME-IS-AUTHORITY`.

#### §3.2.14 (new V3) RebuildCoordinator — deferred T5

- **V3 file**: `core/replication/rebuild.go` (T5)
- **V3 scope**: per-volume rebuild-path decision owner
- **T4 status**: ⏭ DEFERRED-T5 — landing depends on Path A vs Path B resolution (sketch §6.1 Item B)
- **Key contracts**: deferred. Notable: Decide (policy); Begin/Complete (session bookkeeping); InvalidateOlderSessions (`activeRebuildSession` semantic lifted up).

---

### §3.3 Cross-entity invariants (architect-LOCKED + T4-defined)

| ID | Statement | Source | T4 status |
|---|---|---|---|
| INV-REPL-ACK-FRAME-IS-AUTHORITY | Replica ack validity is determined by fields carried in the frame, not by primary-side inferred replica state | H5 LOCK (round 4) | ⊙ enforced T4b via §3.3 C.2 BarrierResponse epoch |
| INV-REPL-CACHE-ADVISORY-ONLY | Any primary-side per-replica cache is advisory only and must never upgrade an otherwise-invalid ack into a quorum-contributing ack | H5 LOCK (round 4) | ⊙ enforced T4b |
| INV-REPL-WIRE-CLEAN-BREAK | V3 replication wire is clean-break from V2; versioned envelope `{magic, protocol_version, message_type, flags_or_reserved}` from day one; no V2 replica interop | Q5 LOCK (round 4) | ✓ PORTED T4a-1 (commit `seaweed_block@56ad349`, 12B preamble SWRP + version 1) |
| INV-REPL-LINEAGE-BORNE-ON-SHIPENTRY | ShipEntry frames carry explicit `RecoveryLineage` sourced from the peer's registered live-ship session; session is fresh per authority era (peer is torn down + recreated by `ReplicationVolume.UpdateReplicaSet` on target `{Epoch,EndpointVersion}` change, **not** in-place updated) | Sketch §8 + T4a-3 design | ✓ PORTED T4a-2 (wire) + T4a-3 (peer-session ownership); peer-rebuild-on-authority-change binding on T4a-4 |
| INV-REPL-PEER-REBUILD-ON-AUTHORITY-CHANGE | **Statement**: Any change to the authority-bearing peer target (`Epoch`, `EndpointVersion`, or any field carried on replication frames as correctness-bearing lineage) MUST tear down the existing `*ReplicaPeer` and create a new one; in-place lineage mutation is forbidden. **Lifecycle ordering**: old peer sessions/attachments MUST be invalidated (conn closed, transport session marked invalid, old peer rejects all ship/fence/etc. after the signal) BEFORE the new peer becomes active / visible to callers. Replaces V2's per-Ship `epochFn()` callback with V3's "each peer instance = one authority-snapshot-era" ownership model | T4a-3 round-13 design call + round-14 user co-sign strengthening | ✓ PORTED T4a-4 (`seaweed_block@84ec9bf`) — pinned by `TestReplicationVolume_UpdateReplicaSet_LineageBump_RecreatesPeer` |
| INV-REPL-LSN-ORDER-FANOUT-001 | **Statement**: `ReplicationVolume.OnLocalWrite` serializes fan-out in LSN order for a given volume. Caller order is not trusted as the correctness mechanism. **Lock-scope binding**: the per-volume mutex MUST guard the full per-peer Ship dispatch loop — not `LSN-read + peer-map-snapshot + unlock + ship`. Unlocking before ship loses the serialization property. **Accepted trade-off**: T4a is correctness-first, not throughput-first — `OnLocalWrite` may block behind lazy dial + 3s write deadline + slow/dead peer timeouts. Async-queue decoupling (Option Z from T4a-4 G-1) is deferred optimization, NOT forgotten design debt. **Why it matters**: V2 kept this atomic via `shipMu` spanning `LSN allocate + WAL append + ShipAll`; V3 splits these across LogicalStorage (allocate + append) and ReplicationVolume (fan-out); without this invariant, two goroutines with LSN=1 and LSN=2 can hit Ship(2)-before-Ship(1), and `ReplicaListener.acceptMutationLineage` does NOT re-order on receipt. Direct structural port of V2 safety property; skipping it = BUG-001 class | T4a-4 G-1 V2 read (round 15) + architect co-sign | ✓ PORTED T4a-4 (`seaweed_block@84ec9bf`) — pinned by `TestReplicationVolume_OnLocalWrite_ConcurrentLSNs_OrderedAtReplica` (40 concurrent goroutines, LBA/LSN deliberately interleaved so any reorder is byte-detectable at replica's `LogicalStorage.AllBlocks()`); runs deterministically (10 consecutive runs clean) |
| INV-REPL-LINEAGE-BORNE-ON-FENCE | Fence calls carry explicit `{sessionID, epoch, endpointVersion}` | Sketch §8 | ✓ layer D already has this shape at `executor.go:75` |
| INV-REPL-LOCAL-FSYNC-GATES-QUORUM | **Statement**: Local-fsync result gates mode-arithmetic evaluation. If `localSync` returns error, `DurabilityCoordinator.SyncLocalAndReplicas` returns that error immediately — mode-dependent quorum/sync_all arithmetic is NOT evaluated. Rationale: primary is the "1" in `durableNodes = 1 + successful_barriers`; if primary itself isn't durable, the arithmetic is meaningless (would count primary as durable when it isn't). V2 precedent: `dist_group_commit.go` lines 46-48 return `localErr` before running mode arithmetic. **Why it matters**: silently running quorum math over a failed local fsync would let a fake "success" leak when primary lost its own local durability | T4b-3 G-1 V2 read concern #7 | ⊙ T4b-3 test `_LocalSyncFails_AllModesError` pins |
| INV-REPL-BARRIER-FAILURE-DEGRADES-PEER | **Statement**: Every per-peer barrier failure inside `DurabilityCoordinator.SyncLocalAndReplicas` MUST trigger `peer.Invalidate(reason)` on that peer BEFORE the coordinator returns. This holds in ALL three durability modes (sync_all / sync_quorum / best_effort) — even when the mode's arithmetic itself doesn't fail the Sync call (best_effort especially). **Why it matters**: V2 implemented this inside `WALShipper.Barrier` via `failBarrier → markDegraded`; V3's `BlockExecutor.Barrier` (T4b-2) intentionally does NOT own ReplicaState, so `DurabilityCoordinator` MUST translate barrier failure → peer degrade explicitly. Silent barrier failure without peer degrade = BUG-001 class: future writes to an effectively-dead peer would continue through best_effort without surfacing the problem | T4b-3 G-1 V2 read concern #8 | ⊙ T4b-3 test `_BestEffort_Silences_Failures` asserts peer becomes Degraded |
| INV-REPL-ZERO-PEER-NO-SPAWN | **Statement**: `DurabilityCoordinator.SyncLocalAndReplicas` with zero peers (standalone / RF=1) MUST short-circuit: run local sync only, no parallel goroutine spawn. All three modes return success if local sync succeeds (primary = 1 durable ≥ 1 quorum for RF=1). V2 precedent: `dist_group_commit.go` lines 19-22. **Why it matters**: consistent with T4a-5 Q3 empty peer set semantics (standalone is a real production case); goroutine spawn for zero peers is wasteful and obscures the arithmetic | T4b-3 G-1 V2 read concern #9 | ⊙ T4b-3 test (to add: `_ZeroPeers_StandaloneFastPath`) |
| INV-REPL-NO-ZERO-LINEAGE-FIELDS | **Statement**: No RecoveryLineage construction anywhere in `core/transport/` or `core/replication/` may intentionally use zero values for `SessionID`, `Epoch`, `EndpointVersion`, or `TargetLSN` as semantic sentinels. The strict-decode rule (reject-on-zero) makes such conventions observable as bugs. Use non-zero sentinel constants (e.g. `fenceSentinelTargetLSN = 1` in `core/transport/executor.go:98`) when a field is not meaningful for a given op. **Why it matters**: landed during T4b-1 self-test (`seaweed_block@0a43eff`) when `executor.Fence`'s `TargetLSN=0` convention collided with new strict decode. Same pattern will fire at T4c catch-up-done-ack and T5 rebuild-done-ack extensions; scan every existing `RecoveryLineage{...}` construction when those tasks start | T4b-1 self-test round 23 | ✓ enforced at transport decode layer (per T4b-1 tests); sw to scan for new instances at T4c/T5 wire extensions |
| INV-REPL-LINEAGE-BORNE-ON-BARRIER-ACK | **Statement**: `BarrierResponse` carries full `RecoveryLineage` echo (SessionID + Epoch + EndpointVersion + TargetLSN), NOT epoch-only, alongside the durability result payload. **Symmetry rule**: request carries authority tuple; response proves which authority tuple it is acknowledging. **UNIFORM APPLICATION (architect round 21 meta-call)**: this rule applies uniformly across ALL barrier-ack-like surfaces in the replication system — **durability barriers** (T4b), **fence acks** (layer-D already has this shape), **catch-up done acks** (T4c), **rebuild done acks** (T5). Epoch-only echo is NOT accepted on ANY of these surfaces. **Why full echo (architect round 21)**: (1) Barrier is an authority-bearing ack surface under H5 LOCK, not just a durability bit — epoch-only recreates the asymmetric-invariant smell T4 worked to remove; (2) avoids a second wire bump in T4c when `sessionID`/`endpointVersion` turn out to be needed; (3) lets late/mismatched acks be rejected against exact lineage for better callback filtering + diagnostics, not accepted on a too-weak epoch match; (4) uniform mental model across frames — matches `ShipEntry` full-lineage shape. **T4b logic latitude**: `DurabilityCoordinator.EvaluateBarrierAcks` MAY initially key correctness primarily on `epoch` if that is the immediate need, BUT MUST preserve the stronger rule that only full-lineage-valid acks are eligible for counting. No future optimization may weaken this to epoch-only acceptance. **Eligibility rule**: a barrier ack only counts if its full lineage matches the request/session the caller is awaiting; mismatched, stale, or partially-zeroed lineage is advisory for logging only and MUST NOT be accepted | Sketch §6.1 C.2 — architect sign round 21 (full echo + uniform rule) | ✓ LOCKED (architect round 21); ⊙ T4b-1 coding; binding on T4c (catch-up done ack) + T5 (rebuild done ack) |
| INV-REPL-LINEAGE-BORNE-ON-PROBE-PAIR | **Statement**: probe is a symmetric authority-bearing pair, not a one-direction echo. **`ProbeReq` carries full `RecoveryLineage`** (32B preamble before any payload); **`ProbeResponse` carries the full echoed `RecoveryLineage` + R/S/H tuple** (32B + 24B = 56B body). Replica validates incoming `ProbeReq` lineage AND echoes it; primary validates echoed lineage before accepting R/S/H facts; mismatched/stale/partial-zeroed lineage on either direction is diagnostic-log only and MUST NOT enter recovery decisions. Strict decode inherits from `BarrierResponse` rules: <56B / zero-valued lineage / field-order malformed → fail closed. **No zero-TargetLSN exception**: probe uses the standard `RecoveryLineage` from the registered session even though probe is non-mutating; carving out a separate non-mutating-probe lineage type is explicitly rejected by architect for this batch | Sketch §6.1 C.3 — architect sign round 26 (full echo + symmetric rule) | ✓ PORTED T4c-1 (`seaweed_block@4dfe582`); pinned by `TestProbeResp_FullLineageEcho_RoundTrip` + `_ShortPayload_Rejected` + `_ZeroedLineage_Rejected` + `_MalformedFieldOrder_Rejected` + `_ByteLayoutFence` + `TestReplica_ProbeReq_AcceptedLineage_EchoesAndReturnsRSH` + `_StaleLineage_Rejected` + `_ZeroedLineage_Rejected` + `TestExecutor_Probe_LineageMismatch_Rejected` + `ErrProbeLineageMismatch` sentinel test |
| INV-REPL-PROBE-NON-MUTATING-VALIDATION | **Statement**: probe lineage validation MUST NOT advance `activeLineage` on the replica side; probe is a non-mutating observation operation. The replica uses `validateProbeLineage` (gate-without-advance) NOT `acceptMutationLineage` (gate-with-advance). **Why it matters**: T4c-1 architectural pin — using `acceptMutationLineage` for probes caused C5 calibration scenario to fail because probe's monotonic sessionID raced ahead of an in-flight rebuild's lower sessionID, then rebuild frames at the lower sessionID got rejected as stale. **Discovered at PR review time during T4c-1 implementation** (no G-1 pre-code on T4c-1 because wire+validation is V3-native design); fixed in `seaweed_block@4dfe582` with new `validateProbeLineage` function + comment block at the call site `replica.go:122`. Generalizes: any future non-mutating frame consumer (status query, health check, etc.) should use gate-without-advance discipline | T4c-1 round-37 architectural pin (sw-discovered + fixed in same commit) | ✓ PORTED T4c-1 (`seaweed_block@4dfe582`); covered by C5 calibration scenario continuing to pass + dedicated probe-non-mutation test surface |
| INV-REPL-FANOUT-001 | Every acked local primary write is either shipped to every eligible peer or reported as a ship failure; no silent drop | Sketch §8 | ✓ PORTED T4a-4 (`seaweed_block@84ec9bf`) — `ReplicationVolume.OnLocalWrite` iterates every peer; per-peer ship errors degrade the peer (best_effort) but do not silently skip |
| INV-REPL-SHIP-TRANSPORT-MUSCLE-001 | `Ship` preserves V2 composite stability (lazy dial + silent drop + 3s deadline + no-hard-stop-on-failure + degrade-handoff-to-caller) | Architect round 11 (Option B) | ⊙ T4a-2 coding |
| INV-REPL-LIFECYCLE-HANDLE-BORROWED-001 | `ReplicationVolume` borrows `LogicalStorage`; never closes it (BUG-005 non-repeat) | Gate G-2 | ✓ PORTED T4a-4 (`seaweed_block@84ec9bf`) — pinned by `TestReplicationVolume_Constructor_DoesNotOwnStore` regression fence |
| INV-REPL-DURABILITY-SYNC-ALL-001 | Under `sync_all`, any peer barrier failure fails the write | §3.2.5 C2 | ⊙ T4b |
| INV-REPL-DURABILITY-QUORUM-001 | Under `sync_quorum`, `durableNodes >= rf/2+1` required; primary = 1 durable; stale-epoch acks excluded | §3.2.5 C1+C3+C5 | ⊙ T4b |
| INV-REPL-DURABILITY-BEST-EFFORT-001 | Under `best_effort`, peer barrier failures are logged + peer degraded, do not fail the write | §3.2.5 C4 | ⊙ T4b |
| INV-REPL-FENCE-EPOCH-EQ | Replica drops WAL entries with `entry.Epoch != currentEpoch` silently | §3.2.1 C1 + §3.2.3 C3 | ⊙ coding T4a-2 + T4c |
| INV-REPL-CATCHUP-WITHIN-RETENTION-001 *(Path A only)* | Reconnect within retained WAL window catches up via delta; `ErrWALRecycled` escalates to rebuild | Sketch §6.1 B Path A | **⏭ T4d (downgraded from T4c per closure §B delta #4)** — un-pinned at T4c because catch-up sender hardcodes `ScanLBAs(1)` not R+1; spurious-rebuild risk under checkpoint-past-1 + replica-within-retention. T4d-2 (apply gate) + T4d-3 (R+1 threading) co-pin |
| INV-REPL-CATCHUP-FULL-TRANSFER-001 *(Path B only)* | Reconnect after any disconnect catches up via full-state transfer of `AllBlocks()`; retention-window deferred to P10 | Sketch §6.1 B Path B | NOT ACTIVE — Path A signed by architect |
| **INV-REPL-NO-PER-LBA-DATA-REGRESSION** *(round-43 lock, GOAL-LEVEL)* | Per-LBA data on a replica MUST NOT regress under any apply path (live, recovery, retry, replica restart mid-recovery). Frontier monotonicity (walHead/nextLSN/R/H) is necessary but not sufficient — `BlockStore.ApplyEntry` regressed walHead AND blindly overwrote LBA bytes on older-LSN apply pre-`f6084ee`. The mechanism invariants below (STALE-ENTRY-SKIP-PER-LBA + COVERAGE-ADVANCES-ON-SKIP + LIVE-LANE-STALE-FAILS-LOUD + RESTART-SAFE) collectively make this hold | Round-43 architect lock | ⏭ T4d-2 |
| **INV-REPL-RECOVERY-STALE-ENTRY-SKIP-PER-LBA** *(round-43 lock, MECHANISM)* | Replica must skip recovery-stream entry data writes when `entry.LSN <= appliedLSN[LBA]`. Apply only if strictly newer; on apply, update `appliedLSN[LBA] = entry.LSN`. Substrate fixes are defense-in-depth; the replica recovery apply gate (T4d-2) is authoritative | Round-43 architect lock | ⏭ T4d-2 |
| **INV-REPL-RECOVERY-COVERAGE-ADVANCES-ON-SKIP** *(round-44 refinement)* | Skipped recovery entries (data-skip per stale-LSN check) MUST still update `recoveryCovered[LBA]=true`. Skip data ≠ ignore frame: completion accounting depends on every LBA in the recovery window being marked processed. Architect text: "Recovery-stream stale entries are valid duplicates and must be skipped as data writes while still counted as recovery-stream coverage." | Round-44 architect refinement | ⏭ T4d-2 |
| **INV-REPL-LIVE-LANE-STALE-FAILS-LOUD** *(round-44 refinement)* | Live-lane stale entries (received `MsgShipEntry` on the live handler with `LSN <= appliedLSN[LBA]`) are abnormal — repeat packet, stale session, lineage error, or retry on wrong lane. They MUST NOT mutate data, MUST be skipped/rejected with diagnostic surfacing, and MUST NOT advance `recoveryCovered[LBA]`. Distinct from the recovery-lane stale-skip, which is normal | Round-44 architect refinement | ⏭ T4d-2 |
| **INV-REPL-RECOVERY-COVERAGE-RESTART-SAFE** *(Option C hybrid lock)* | Replica restart mid-recovery MUST NOT regress per-LBA data. Apply gate seeds `appliedLSN[LBA]` from substrate query at session start (Option C hybrid: substrate-native truth where available, in-memory session map updated thereafter). Re-shipped recovery window after restart correctly skips entries already applied pre-restart | Architect §2.5 #1 ratification (Option C) | ⏭ T4d-2 |
| **INV-REPL-LANE-DERIVED-FROM-HANDLER-CONTEXT** *(§9 architect Q2 lock; round-46 architect re-emphasis)* | `MsgShipEntry` lane is derived from the **accepting handler/session context** in T4d. **NOT from payload-derived signals like `lineage.TargetLSN` sentinels.** Live handlers MUST NOT execute recovery stale-skip/coverage logic; recovery handlers MUST NOT bypass it. Apply gate hook MUST expose lane-explicit methods (e.g. `ApplyLive(...)` + `ApplyRecovery(...)`, or `Apply(..., lane ApplyLane)`) — caller supplies lane from connection/session context. A test must fail if a recovery entry reaches the live path or a live entry reaches the recovery accounting path. Regression `TestApplyGate_RecoveryWithTargetLSN1_RoutesToRecoveryLane` pins the edge case where TargetLSN=1 (sentinel-collision-with-live) MUST still route via handler-context to recovery. Explicit lane wire-tag deferred to post-G5 protocol-hardening if handler-context discipline becomes fragile. **Round-46 history**: T4d-2 `bd2de99` initial implementation drifted to payload-derived discrimination via `lineage.TargetLSN > 1` sentinel; QA T4d-2 review surfaced the edge case (recovery with H=1 misroutes); architect reaffirmed handler-context as the correct rule + bound T4d-2 follow-up rework as HARD GATE before T4d-3 | §9 architect Q2 + round-46 architect re-emphasis | ⏭ T4d-2 follow-up (REWORK GATE) → T4d-3 |
| **INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY** *(§9 architect Q1+Q3 lock; T4d-1 strengthened)* | `core/transport` may depend on `core/storage` recovery-contract symbols only (`LogicalStorage`, `RecoveryEntry`, `ErrWALRecycled` / structured replacement, `RecoveryMode`), never substrate internals (`core/storage/walstore`, `core/storage/smartwal`). Engine retry loop lives in `core/engine` (Q1 Option α); package move to `core/recovery` deferred per Q3. Lane-aware apply decisions go through the T4d-2 apply gate interface, not via direct substrate queries from transport. **T4d-1 strengthening (architect-endorsed 2026-04-25)**: 2-enum split inscribed — `storage.StorageRecoveryFailureKind` is substrate classification; `engine.RecoveryFailureKind` is engine decision vocabulary; `transport.classifyRecoveryFailure` is the explicit boundary mapper. `core/engine` MUST NOT import `core/storage` at all (stricter than Q1+Q3 baseline; verified at `1edeb36` build). | §9 architect Q1+Q3 + T4d-1 architect endorsement | ⏭ T4d-3 (transport fence); ⊙ T4d-1 (engine-purity verified) |
| **INV-REPL-CATCHUP-FROMLSN-IS-REPLICA-FLUSHED-PLUS-1** *(T4d-3 G-1 §5 hidden invariant)* | Engine emits `StartCatchUp.FromLSN = Recovery.R + 1`. Sender does NOT add `+1` (avoids double-add). The "+1 to skip already-applied LSN" semantic lives at the engine command-emit site, not transport. Surfaced at G-1 §6.1 placement decision; QA-ratified Option A (engine adds +1) | T4d-3 G-1 §5 (architect Q1 derivation) | ⏭ T4d-3 |
| **INV-REPL-CATCHUP-FROMLSN-FROM-ENGINE-STATE-NOT-PROBE** *(T4d-3 G-1 §5 hidden invariant)* | Engine populates `StartCatchUp.FromLSN` from its **own** `Recovery.R` state (the single source of truth). Probe results are ingested as facts that update engine state; the command emit path reads the updated state, never the raw probe payload directly. Pins the race window: probe → recovery decision → command emit always goes through engine state, no shortcut. Positive-form per QA nit; predecessor name `INV-REPL-CATCHUP-FROMLSN-NOT-FROM-PROBE-DIRECTLY` retained in G-1 history only | T4d-3 G-1 §5 (architect Q1 derivation) | ⏭ T4d-3 |
| **CARRY-T4D-LANE-CONTEXT-001** *(named carry — round-46+ architect)* | **NOT AN INVARIANT — A NAMED CARRY.** T4d-2 follow-up `01f4ab9` made the apply gate lane-pure (correct architectural fence: gate ≠ payload-sniffer) but moved the lane-discrimination payload sniffing one layer up to `core/transport/replica.go:13-18,156-165` as a TRANSITIONAL CALLER-SIDE SHIM — replica handler reads `lineage.TargetLSN==liveShipTargetLSN(=1)` to dispatch to gate's `ApplyLive` vs `ApplyRecovery`. Edge case: recovery session with H=1 (primary has exactly 1 entry, replica empty) ships with `TargetLSN=1` and misroutes to live-lane via the shim → fail-loud fires incorrectly. **Bound resolution**: replace the TargetLSN==1 caller shim with true handler/session context lane signal (per-conn lane tag at handshake / separate handlers / distinct ports — implementation choice). **Owner**: sw. **Bind point**: T4e (preferred) or post-G5 protocol-hardening (latest). **T4d-3 close gate**: T4d-3 MUST do exactly one of (A) land the fix and remove the shim, or (B) add explicit failing/skip-marked L2 test `TestT4d3_RecoveryTargetLSN1_KnownGap` documenting H=1 as known gap with godoc citing this carry id. Architect explicit prohibition: NO engine precondition `TargetLSN >= 2`. | T4d-2 follow-up review round-46+ architect direction | ⏳ CARRY (active) — T4d-3 close evidence cites; resolution at T4e or post-G5 |

Exactly one of `INV-REPL-CATCHUP-{WITHIN-RETENTION,FULL-TRANSFER}` queues ACTIVE per architect Item B choice (Path A signed; WITHIN-RETENTION active, FULL-TRANSFER not).

---

### §3.4 Drift events (T4 port judgment trail)

| Round | Event | Entity | Root cause | Corrected to |
|---|---|---|---|---|
| Round 6 (sw) | `core/transport/` package entirely missed in initial V3 pre-scan; §3.11 H6 + §3.12 H7 + §3.13 hazards reasoned against incomplete V3 picture | Layer D reality | QA V3 pre-scan limited to `core/frontend/durable/` + `core/storage/` | Entity skeleton §2.D added; §3.11–§3.14 retired/reframed |
| Round 7 (sw) | `BlockExecutor.Ship` + `.Barrier` don't exist as public API (claim in audit §6 was wrong); `BlockExecutor.StartCatchUp` ships AllBlocks, not WAL-window delta | Layer D reality | QA conflated test-call helpers (`exec.StartCatchUp` in test files) with real public API + assumed semantic | Audit §7 counts recalibrated; sketch §5 layer-D addendum gains wire-shape work; Item B elevated to architect-line |
| Round 8 (architect) | walstore substrate scope ambiguity; lineage row over-broad; Path B weakening hidden in §6.1 | T4 scope discipline | QA missing T3 walstore inheritance; failed to split lineage per frame family | Sketch §1/§2/§7 walstore discipline; §8 lineage rows split by frame family; §3.2 Path B weakening surfaced in main gate text |
| Round 11 (architect) | V2 Ship characterized as "transparent in-call retry"; V3 Option C (retry in `ReplicaPeer`) accepted prematurely | §3.2.1 WALShipper | QA mis-read V2 composite stability as single-call behavior; would have additionally forced exposing `registerSession`/`attachConn` public seams violating transport-ownership principle | §3.2.1 C3-C4 Option B adopted; INV-REPL-SHIP-TRANSPORT-MUSCLE-001 inscribed; partial-port discipline surfaced (QA is 2-for-2 on V3-existing-shape under-scanning within T4) |

**Pattern**: QA-solo sign has recurring failure mode on partial-port + V3-existing-shape. sw and architect caught both mis-signs before code landed. Round-11 proposal (sketch of `§8C.2.a partial-port architect co-sign rule`) deferred — architect to decide discipline shape after full re-read of this §3 + backing T4 docs.

---

### §3.5 Change log

| Date | Change | Author |
|---|---|---|
| 2026-04-23 | §3 filled from T4 rounds 1–11 consolidated. 10 V2 entities + 4 new V3 entities; 14 per-entity sections in §3.2; 16 cross-entity invariants in §3.3 (3 architect-LOCKed, 4 sketch-§6.1-conditional); 4-round drift trail in §3.4. T4a-2 rows reflect Option B adoption (architect round 11); sw coding in progress. Item B catch-up semantics still pending architect sign at T4 T-start | QA Owner |
| 2026-04-23 | **T4b BATCH CLOSE — QA single-sign per §8C.2.** `seaweed_block@21e9e83` landed T4b-6 L2 integration matrix: 8 scenario subtests (`{SyncAll_TwoPeers_AllHealthy, SyncAll_OnePeer_FailsOnBarrierError, SyncQuorum_RF3_TolerantOfOneFailure, BestEffort_AllPeersFail_StillSucceeds} × {smartwal, walstore}`) + 1 wire-shape fence (`BarrierWire_FullLineageEcho_StillEnforced`) all green. Both impls passed at L2 per round-22 honesty correction (smartwal sign-bearing; walstore non-gating only for BUG-007-class which L2 does not exercise). T4b cumulative: 6 commits, 34+3 sanity pins, ~590 LOC production + ~1200 LOC tests landed against `phase-15` branch. **8 invariants pinned** at T4b close: INV-REPL-LINEAGE-BORNE-ON-BARRIER-ACK (wire+executor layers), INV-REPL-BARRIER-FAILURE-DEGRADES-PEER (peer+coordinator layers — round-24 §0-B two-layer coverage), INV-REPL-DURABILITY-{SYNC-ALL,QUORUM,BEST-EFFORT}-001 (coord+L2 matrix), INV-REPL-LOCAL-FSYNC-GATES-QUORUM (G-1 concern #7), INV-REPL-ZERO-PEER-NO-SPAWN (G-1 concern #9), INV-REPL-NO-ZERO-LINEAGE-FIELDS (T4b-1 self-test surfacing). **Forward-carry from T4a all verified green**: T4a-2 Ship invariants + T4a-3 peer state + T4a-4 INV-REPL-LSN-ORDER-FANOUT-001 (under T4b-5 concurrent Sync+Write) + T4a-6 BasicEndToEnd + DisconnectThenReassign. 21-package repo suite green at every commit. QA ✓ for T4b batch close. **Architect + PM sign at T4 T-end** per §8C.1 (after T4c + T4d optional) | QA Owner |
| 2026-04-23 | T4b-5 closure: `seaweed_block@907e368` landed `ReplicationVolume.Sync` + `WriteObserver.Sync` seam (~30 LOC core + interface extension). Lock-scope binding honored: v.mu held across full Sync call (architect round-15 Condition A discipline forward-carried). Per-volume durability mode (set via `SetDurabilityMode`); per-Sync override deferred per mini-plan §5. `StorageBackend.Sync` branches: observer==nil → pre-T4b local-only behavior preserved; observer!=nil → delegates full sync (local + barriers) to `ReplicationVolume.Sync` → `DurabilityCoordinator`. 2 pins green: `_Sync_BestEffort_E2E` (full wire write→ship→sync→barrier→primary R reaches H) + `_Sync_PreservesLSNOrderUnderConcurrency` (40 goroutines, caller-serialized LSN, Sync interleaved every 4th iteration, inverse-LBA-order plan; all markers land correctly). **Forward-carry fence verified**: T4a-4 adversarial pin `TestReplicationVolume_OnLocalWrite_ConcurrentLSNs_OrderedAtReplica` continues to pass — INV-REPL-LSN-ORDER-FANOUT-001 not regressed by Sync path. 21-package suite green | QA Owner |
| 2026-04-23 | T4b-4 closure: `seaweed_block@921521d` landed `DurabilityCoordinator` (~260 LOC prod + ~580 LOC tests). V2 `MakeDistributedSync` ported to V3 `core/replication/durability.go` per G-1 signed outline. §0-B discipline verified — zero `peer.executor.Barrier` refs; all 7 call sites use `peer.Barrier` wrapper. 9 pins green including `_ZeroPeers_StandaloneFastPath` (3 mode subtests; goroutine-ID check for INV-REPL-ZERO-PEER-NO-SPAWN), `_LocalSyncFails_AllModesError` (3 mode subtests for INV-REPL-LOCAL-FSYNC-GATES-QUORUM), `_BestEffort_Silences_Failures` (INV-REPL-BARRIER-FAILURE-DEGRADES-PEER coordinator-layer pin via peer.Barrier), `_ParallelExecution` (0.10s actual vs 150ms budget), `_StaleLineageAck_NotCounted` (H5 LOCK). T4b-3 pin + T4b-4 pin together cover INV-REPL-BARRIER-FAILURE-DEGRADES-PEER at both peer and coordinator layers (architect round-24 two-layer coverage). T4b-3 `seaweed_block@90e6c64` also landed in sequence (peer wrappers — leaf shipped first per Opt-4 pattern). T4b catalogue §3 entity rows can upgrade ⊙ → ✓ at T4b batch close after T4b-5 + T4b-6 | QA Owner |
| 2026-04-23 | T4b round-24 §0-B-correct reorder absorbed. QA realized prior-turn Decision #1 sign conflated two questions (peer.Invalidate locality vs peer.Barrier-vs-peer.executor.Barrier routing); sw had correctly flagged the §0-B question separately. Signed §0-B-correct reading: T4b-3 scope expanded from "Fence-only thin wrap" to "ReplicaPeer.Barrier + ReplicaPeer.Fence wraps" leaf (mirrors T4a-3 leaf-first pattern). T4b-4 becomes DurabilityCoordinator (was T4b-3); T4b-5 becomes ReplicationVolume.Sync (was T4b-4); test count unchanged at 9 for DurabilityCoordinator. G-1 artifact renamed `v3-phase-15-t4b-3-g1-v2-read.md` → `v3-phase-15-t4b-4-g1-v2-read.md` with 7 revision points absorbed (call-site `peer.Barrier` not `peer.executor.Barrier`; INV-REPL-BARRIER-FAILURE-DEGRADES-PEER pinned at TWO layers — peer.Barrier wrapper + DurabilityCoordinator best-effort test; "always attempt BarrierAll MUST NOT gate on Degraded" binding relocated to T4b-3 wrapper). Short-cut alternative explicitly rejected per §0-B discipline. G-1 re-signed after revision. sw codes T4b-3 first, then T4b-4 against peer.Barrier surface | QA Owner |
| 2026-04-23 | T4b-3 Gate G-1 SIGNED. sw posted G-1 V2 read at `v3-phase-15-t4b-3-g1-v2-read.md`; LOC 130-160 V3 vs 45 V2 muscle (3×, not shrinkage — justified by `EvaluateBarrierAcks` split + explicit per-peer lineage construction). **5 hidden invariants surfaced beyond mini-plan's enumerated 6**: #7 local-fsync-short-circuits-quorum-arithmetic; #8 barrier-failure-degrades-peer (the big one — V2's peer-state mutation lives inside `WALShipper.Barrier` not in `dist_group_commit`; V3 `BlockExecutor.Barrier` intentionally doesn't own ReplicaState per T4b-2 design, so `DurabilityCoordinator` MUST explicitly call `peer.Invalidate` to preserve V2 semantic); #9 zero-peer-fast-path; #10 targetLSN derivation responsibility shift (doc-level); #11 WaitGroup flat vs nested layout (pure refactor). Three QA decisions signed: (1) peer-state mutation lives in `DurabilityCoordinator` directly, not T4b-5 wrapper (same-commit validation); (2) metrics DEFER to T4-end pass (V2 was log-only anyway); (3) flat `wg.Add(1 + len(peers))`. Three new invariants inscribed: `INV-REPL-LOCAL-FSYNC-GATES-QUORUM` (#7), `INV-REPL-BARRIER-FAILURE-DEGRADES-PEER` (#8), `INV-REPL-ZERO-PEER-NO-SPAWN` (#9). Test count increased from 8 → 9 (new `_ZeroPeers_StandaloneFastPath` for #9). Clarified meta-question for user: T4b-3 is NOT "adding V2-absent logic" — it's relocating V2's degrade-on-barrier-fail mechanism from `WALShipper` layer (where V2 had it) to V3 `DurabilityCoordinator` layer (the equivalent same-level owner), consistent with T4b-2's explicit decision that `BlockExecutor` does not own ReplicaState. §0 PRESERVE baseline intact: mechanism ported, container relocated | QA Owner |
| 2026-04-25 | T4c-2 closure: `seaweed_block@ff731dd` landed catch-up muscle port + state machine + ScanLBAs interface seam per G-1 V2 read signed at round 36-37. **Storage interface seam** (G-1 §3 row #4): unified `RecoveryEntry` + `ErrWALRecycled` + `RecoveryMode` types in new `core/storage/recovery_contract.go`; `LogicalStorage.ScanLBAs` interface method; walstore + smartwal POC code promoted to production; `BlockStore` adapter implements ScanLBAs. **Catch-up sender muscle port** (G-1 §3 narrow scope): `catchup_sender.go` rewritten — AllBlocks walk → ScanLBAs callback. **All 6 invariants honored** (5 from G-1 §5 + 1 from §4.2 binding): callback-return-nil-continues, lastSent-monotonic, deadline-per-call-scope, target-not-reached-vs-recycled-distinguished, completion-from-barrier-achieved-lsn, done-marker-always-emitted. `recovery_mode` label emitted in success log per memo §5.1 / §13.0a observability requirement. **Engine wiring** (G-1 §4.1 + §4.3): `RecoveryRuntimePolicy.MaxRetries` per-kind defaults (3 / 0 / 1); `applySessionFailed` detects `ErrWALRecycled` sentinel and escalates `recovery.Decision = Rebuild` (engine→storage decoupling preserved via string match, not import). **Peer state machine** (memo §2.2): `ReplicaCatchingUp` + `ReplicaNeedsRebuild` states + `replicaStateTransitionAllowed` table-driven gate; NeedsRebuild terminal in T4c. **15 new tests** (8 catch-up sender + 4 peer state machine + 3 engine pins). 21-package suite green. **LOC actual ~120 vs G-1 estimated 60-70**: structure-driven (ScanLBAs callback adds boilerplate vs V2 inline `StreamEntries(fn)+WriteFrame(MsgCatchupDone)` pattern + mode-label probe + per-error-class wrapping); LOC heuristic check is V3<40% V2 = red flag for SIMPLIFICATION drift; 120>67 is the opposite direction so PRESERVE concern N/A. **§4.1 retry-loop wiring deferred to T4c-3**: MaxRetries field present + per-kind defaults pinned at T4c-2 (3 engine tests pin); engine SessionFailed→re-emit-with-counter loop is observable behavior testable at L2/L3 in T4c-3 integration matrix, NOT at unit scope. **Forward-carry binding to T4c-3**: integration matrix MUST exercise retry path end-to-end with budget exhaustion + escalation; without this, T4c retry mechanism has no live exercise. INV-REPL-CATCHUP-* invariants queue ACTIVE at T4c batch close after T4c-3 | QA Owner |
| 2026-04-25 | T4c-1 closure: `seaweed_block@4dfe582` landed ProbeReq + ProbeResponse symmetric full-lineage wire + validation per architect round-26 Item C.3. **Wire**: ProbeReq 32B body (full RecoveryLineage); ProbeResponse 56B body (32B echoed lineage + 24B R/S/H tuple); strict decode mirrored from BarrierResp (short / zeroed / field-order rejected). **Validation**: replica `validateProbeLineage` gates incoming ProbeReq; primary validates echoed lineage in response via new `ErrProbeLineageMismatch` sentinel. **Adapter**: `prepareQueuedCommands` mints transient probe sessionID for ProbeReplica (parallel to FenceAtEpoch); 7 test executor stubs updated with new sessionID param. **Architectural pin discovered at PR time + fixed in same commit**: `acceptMutationLineage` advances `activeLineage`, which broke C5 calibration when probe's monotonic sessionID raced ahead of in-flight rebuild's lower sessionID. Fix: new `validateProbeLineage` gates without advancing (probe is non-mutating). Inscribed as new invariant `INV-REPL-PROBE-NON-MUTATING-VALIDATION` in §3.3 — generalizes to any future non-mutating frame consumer. **21 new tests + 1 rewritten** (12 wire + 7 validation + 2 transient sessionID); 21-package suite green. `INV-REPL-LINEAGE-BORNE-ON-PROBE-PAIR` upgraded ⊙ → ✓ PORTED with full pin-test list | QA Owner |
| 2026-04-23 | T4b-2 closure: `seaweed_block@a55eacd` landed `BlockExecutor.Barrier` public method (83 LOC) + `executor.doFence` round-22 validation upgrade. 5 pins green: `Barrier_Happy` / `_TimeoutFires` (5.00s exact — deadline IS the mechanism, mirrors T4a-2 pattern) / `_LineageMismatch_Rejected` (new `ErrBarrierLineageMismatch` sentinel) / `Fence_LineageMismatch_MarksFailure` (round-22 pin — no more silent-accept at first existing consumer) / `Fence_ShortOrZeroedLineage_MarksFailure`. `_ = resp` silent-accept at `executor.go:111-121` replaced with `resp.Lineage != lineage` validation + log with expected+actual tuple per round-21 diagnostic shape. Full-lineage uniform rule now enforced at two ack-consuming surfaces: new `Barrier` (T4b-2) + upgraded `doFence` (T4b-2 round-22). Remaining surfaces covered at T4c catch-up-done + T5 rebuild-done per mini-plan preserve clause. 21-package suite green | QA Owner |
| 2026-04-23 | T4b-1 closure: `seaweed_block@0a43eff` landed BarrierResponse full-lineage wire extension. Four pins green: `TestBarrierResp_FullLineageEcho_RoundTrip` (3 sub-cases) / `_ShortPayload_Rejected` (exhaustive 0..39-byte loop) / `_ZeroedLineage_Rejected` (5 sub-cases: all-zero + 4 per-field-zero) / `_MalformedFieldOrder_Rejected`. §3.2.4 C5-BARRIER-RESP-FULL-LINEAGE-ECHO row can upgrade ⊙ → ✓ at T4b batch close (holding until then per ledger discipline). **Non-trivial regression caught in self-test**: `executor.Fence` previously used `TargetLSN=0` as a local "fence has no recovery target" convention; new strict decode (rule: zero lineage field → reject) rejected fence acks. Fix landed same commit: `fenceSentinelTargetLSN = 1` constant + comment block explaining why. Calibration + sparrow suites flagged it immediately. This is exactly the class of latent drift the strict-decode fail-closed rule was designed to surface — validates the architect round-21 decision text's "zero-valued fields MUST be rejected" clause. Replica echo sites (`replica.go` MsgBarrierReq handler + MsgRebuildDone handler) now echo request's full lineage via `EncodeBarrierResp`; catch-up + rebuild decode paths still work, their lineage validation deferred to T4c/T5 per T4b-2 Preserve clause. Full 21-package suite green | QA Owner |
| 2026-04-23 | **Round-21 architect sign: Item C.2 LOCKED on full `RecoveryLineage` echo** (not epoch-only) in `BarrierResponse`. Four rationale points: (1) Barrier is authority-bearing ack under H5 LOCK, not just durability bit — epoch-only recreates asymmetric-invariant smell; (2) avoids second wire bump at T4c; (3) late/mismatched acks rejected against exact lineage (diagnostics + callback filtering); (4) uniform mental model — matches `ShipEntry` full-lineage shape. T4b logic latitude: may initially key correctness primarily on `epoch`, but wire contract is full-lineage from day one. Catalogue §3.3 `INV-REPL-LINEAGE-BORNE-ON-BARRIER-ACK` updated with signed shape + rationale; new contract row §3.2.4 `C5-BARRIER-RESP-FULL-LINEAGE-ECHO` added. Sketch §6.1 Item C.2 row marked ✓ LOCKED; remaining architect-line items narrowed to Item A (Ship approve — Ship already landed), Item B (Path A/B catch-up — T4c), Item C.3 (ProbeResponse echo shape — T4c). **T4b is now unblocked on architect side**; QA to draft T4b mini-plan (architect offered to draft exact T4b-1 wire-shape decision text — QA accepting) | QA Owner |
| 2026-04-23 | **T4a BATCH CLOSE** (QA single-sign per §8C.2): `seaweed_block@e373753` landed L2 subprocess integration test sealing T4a. 4 matrix rows green: `BasicEndToEnd × {smartwal_gating, walstore_nongating}` + `BestEffort_DisconnectThenReassign × {smartwal_gating, walstore_nongating}`. Scope-leak fence inscribed: disconnect-test asserts second-batch LBAs do NOT appear on fresh replica — catches any future refactor that silently pulls catch-up (T4c) into T4a. In-scope addition: `StorageBackend.Write` → `ReplicationVolume.OnLocalWrite` hook via narrow `WriteObserver` interface (prevents `core/frontend/durable` importing `core/replication`; optional-pointer preserves backward compat with all T3 callers). Cumulative T4a: 6 commits across `core/transport/` + `core/replication/` + host wire + integration test. All 5 invariants pinned with adversarial tests: INV-REPL-LSN-ORDER-FANOUT-001, INV-REPL-SHIP-TRANSPORT-MUSCLE-001, INV-REPL-PEER-REBUILD-ON-AUTHORITY-CHANGE, INV-REPL-LIFECYCLE-HANDLE-BORROWED-001, Opt-3 executor-teardown. QA ✓ for T4a batch close; architect + PM sign at T4 T-end per §8C.1 (after T4b + T4c + T4d optional) | QA Owner |
| 2026-04-23 | T4a-5 closure: `seaweed_block@5711b19` landed Host authority-callback → `ReplicationVolume.UpdateReplicaSet` wire per P-refined design. AssignmentFact wire extended with master-minted peer set + monotonic generation (Epoch<<32 \| EndpointVersion). Separate `decodeReplicaTargets` path preserves AST fence `TestNoOtherAssignmentInfoConstruction`. Four P-guardrails verified: AST fence green; adapter + engine zero changes; peer set master-minted only (`collectPeers` reads `Publisher.LastPublished`); generation monotonic. Three Q-bindings honored: Q1 nil-return + `replayedGens atomic.Uint64` counter (test #9); Q2 peer-ID-set delta log format; Q3 empty peer set flows through shared teardown (test #11 three assertions). 380 LOC net delta (ex-protobuf). Closes §8C.3 Discovery Bridge opened at T4a-5.0. Spot-checked: `TestReplicationVolume_UpdateReplicaSet_Generation_MonotonicGuard` + `_GenerationZero_DoesNotAdvanceGuard` + `_EmptyPeerSet_AppliesWithTeardown` + `TestNoOtherAssignmentInfoConstruction` all pass | QA Owner |
| 2026-04-23 | T4a-3 closure: `seaweed_block@99c4e1d` landed `ReplicaPeer` leaf in new `core/replication/` package. CARRY-1 closed via `TestReplicaPeer_ShipEntry_ConnFailure_MarksDegraded`. §3.2.1 C4 upgraded ~ PARTIAL → ✓ PORTED. §3.3 INV-REPL-LINEAGE-BORNE-ON-SHIPENTRY expanded to document peer-owned-session mechanism; new invariant `INV-REPL-PEER-REBUILD-ON-AUTHORITY-CHANGE` added as T4a-4 binding — peer tear-down+recreate on target epoch/endpointVersion change (NOT in-place mutation) is the V3 mechanism preserving the V2 "lineage reflects current authority" property. One sw-flagged design call (ShipEntry accepts lineage param but silently ignores) deferred to round-13 follow-up commit: QA recommends drop the param for signature honesty + BUG-001-class anti-silent-contract discipline. Sw decides final shape; either is a small diff | QA Owner |
| 2026-04-23 | T4a-2 closure: `seaweed_block@8fed2a8` landed `TestExecutor_Ship_WriteDeadline_Fires` pin test (net.Pipe-based; asserts elapsed ≥ 2.5s AND ≤ 10s + Timeout() chain check). Passes 3.00s exactly. §3.2.1 C2 row updated from `~ PARTIAL` to `✓ PORTED T4a-2`. All four `INV-REPL-SHIP-TRANSPORT-MUSCLE-001` composite components (lazy dial / silent drop / 3s deadline / no-hard-stop) now have explicit pin tests | QA Owner |
| 2026-04-23 | Round-12 architect review absorbed: three truthfulness fixes. (H) Q5 envelope state alignment — §3.0.1 Q5 LOCK row expanded with concrete landed shape (12B `SWRP` + v1 + type + flags + reserved + length); explicit note that C.1 is no longer an architect-line decision surface (any future rev = §8C.3). Sketch §6.1 Item C narrowed to C.2+C.3; §5 layer-D addendum C.1 row marked LANDED; §9 sign-table hint updated. (M) C1-REPLICARECV row rewritten — renamed to `C1-REPLICARECV-STATE-ON-ENGINE-NOT-SINGLETON`; acknowledges V3 has no per-volume receiver singleton; test anchor split between PORTED engine-side surface (T3 `TestT3_Durable_RecoverThenServe`) and PENDING demote/promote lifetime proof (T4c `TestT4c_ReplicaLSN_SurvivesDemotePromote`); status downgraded to `⏭ T4c (proof); ✓ engine-side surface`. (L) C3-SHIP-LAZY-DIAL renamed to `C3-SHIP-LAZY-DIAL-ON-REGISTERED-SESSION`; scope boundary "Ship does NOT bootstrap session creation" made explicit; cites actual `ship_sender.go:87-90` unknown-session-returns-error code path to prevent Option-C-shape re-reading. Also updated T4a-2 Ship C1/C2/C4 rows from `⊙ coding` to PORTED / ~ PARTIAL reflecting `seaweed_block@043b9f7` landed state + QA round-12 validation noting `TestExecutor_Ship_WriteDeadline_Fires` pin test pending | QA Owner |

---

## §4 Rebuild (T5 / G6) — STUB, mandatory pre-T5

## §5 Failover (T6 / G8) — STUB, mandatory pre-T6

## §6 ALUA / auth / CHAP (T7+) — STUB

---

## §7 Discipline integration into §8C (QA system doc)

### §7.1 Addition to §8C.1 (T-start three-sign)

Port plan / track sketch MUST include, in top-down order:

1. **L1 entity enumeration** (§1.2 column set) — all V2 entities in scope
2. **L2 bridge + contracts** (§1.3 columns) — §N.1 entity bridge map + §N.2 detailed entity audits
3. **L3 file/function classification** — derived from L1/L2; not signed without L1/L2

T-start three-sign REJECTED if §N.1 / §N.2 skipped.

### §7.2 Addition to §8C.3 triggers

- **#4 port-model violation** already covers "pragmatic evolution / simplify during port"
- **NEW #8**: L1 entity enumeration missing in current track's port sketch; any code PR against the track should surface this before the PR merges

### §7.3 Maintenance

- This catalogue is LIVING per track
- Every bug doc (BUG-NNN) that traces to a missed contract MUST retrofill the row with the drift-event footnote (§2.3 style)
- Future agents consulting "why did V3 do X instead of V2's Y" find answer in §2.2.X entity row (not in scattered audit docs)

---

## §8 Change log

| Date | Change | Author |
|---|---|---|
| 2026-04-22 | Initial catalogue. §2 Storage T3/G4 retrofill: entity map (21 rows) + detailed audits for drift-prone + new entities; §2.3 drift history mapped (BUG-001 / BUG-005 / Addendum A); §3-§6 stubs; §7 discipline integration into §8C. §1.1 V3 model shifts documented as baseline framework. | QA Owner |
