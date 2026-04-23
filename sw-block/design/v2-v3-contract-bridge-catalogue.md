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

## §3 Replication (T4 / G5) — MANDATORY BEFORE T4 T-START THREE-SIGN

### §3.1 Entity bridge map (V2 → V3) — TO BE FILLED pre-T4

Expected V2 entities (per `v2-test-db.md` + memory references):

- `wal_shipper` (WAL ship protocol state machine) — session-scope
- `shipper_group` (multi-replica fan-out) — volume-scope
- `replica_apply` (receiver side) — volume-scope
- `replica_barrier` (write ack barrier) — volume-scope
- Replication session lifecycle (per-connection between primary + replica)
- Durable replicated LSN (state on primary + replica)

Expected V3 topology: TBD — sw + QA T4 sketch must enumerate. 1:1 / split / merge / new for each.

### §3.2 Contract catalogue (per entity) — TO BE FILLED

### §3.3 V3 model shifts relevant to replication

Pre-fill hints (subject to T4 sketch authoring):

- *Event model*: V2 replication callbacks (barrier fn, degradation fn) → V3 likely typed error return + ctx cancellation
- *Authority model*: V2 primary advanced epoch on promotion locally → V3 master authority publishes; replica storage is passive recipient
- *Concurrency model*: V2 shipper had bounded in-flight state → V3 likely similar but with clean context cancellation

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
