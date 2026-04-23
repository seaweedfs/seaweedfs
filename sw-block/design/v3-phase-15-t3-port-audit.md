# V3 Phase 15 T3 — Function-Level Port Audit (T3.0)

**Status**: **LOCKED + QA single-signed 2026-04-22 per §8C.2** (see §8 sign-off table + §10.6)
**Date**: 2026-04-22
**Owner**: sw
**Purpose**: Pre-code hard blocker for T3. Classify every public function of 7 V2 durable-layer muscle files into PORT-AS-IS / PORT-REBIND / DEFER / NEVER, each with explicit rebind target or stop-rule reference.
**Context**: T3-start rev-2.1 §3.2 authority-boundary fix + §11.5 #4 require that storage **observes, recovers, and reports — does not publish**. Any V2 field, function, or comment that implies storage mints/advances/publishes authority facts (Epoch / EV / assignment state) must be reclassified.
**Companion to**:
  - [v3-phase-15-t2-nvme-port-audit.md](v3-phase-15-t2-nvme-port-audit.md) — T2 production audit that precipitated this function-level discipline
  - [feedback_porting_discipline.md](../../.claude/projects/C--work-seaweedfs/memory/feedback_porting_discipline.md) — BUG-001 lesson: incidental V2 code is usually load-bearing

---

## 1. Classification

| Code | Meaning | Action on port | Audit rationale required |
|---|---|---|---|
| **PORT-AS-IS** | Pure mechanism, ports verbatim | Copy; adapt module paths + V3 log/imports only | 1-line why |
| **PORT-REBIND** | Mechanism + non-authority binding (backend, metrics, frontend.Identity) | Copy; replace binding call sites | 1-line why + rebind target |
| **DEFER** | In scope for T3 but not this batch | Out of T3a–T3c; document the target batch | 1-line why + target batch |
| **NEVER** | Authority-coupled, boundary violation, or obsolete | Do NOT port; stop-rule citation mandatory | §3.2 fix citation + what V3 does instead |

Decision discipline: if a function **reads** or **writes** state that is an authority fact (Epoch / EV / role / lease / assigned subject), it is NEVER unless it can be reclassified as "observes last-published authority fact for local state comparison" with explicit §3.2 reconciliation path. The latter is PORT-REBIND with a comment explaining the semantic.

---

## 2. File inventory (7 durable-layer muscle files)

| # | V2 file | LOC | Why in T3.0 | Risk flag |
|---|---|---|---|---|
| 1 | `superblock.go` | 298 | On-disk header; contains Epoch / ExpandEpoch fields | **#3 epoch** |
| 2 | `wal_entry.go` | 153 | WAL record format (wire-stable) | — |
| 3 | `wal_writer.go` | 302 | WAL append/advance/scan path | — |
| 4 | `wal_admission.go` | 178 | WAL-pressure watermark gate | **#2 pressure** |
| 5 | `dirty_map.go` | 156 | In-memory pending-flush set | — |
| 6 | `group_commit.go` | 222 | fsync batching | — |
| 7 | `write_gate.go` | 28 | Pre-write fencing check | **#1 fence** |

Out of T3.0 scope (second-wave audit for T3b / T3c batches, NOT this doc):
`flusher.go`, `recovery.go`, `smartwal*.go`, `snapshot.go`, `scrub.go`, `replica_*.go`, `shipper_group.go`, `rebuild*.go`, `wal_shipper.go`. Each covered by its own mini-port-plan audit.

Out of T3 entirely (authority-coupled surface, NEVER — not revisited):
`epoch.go`, `lease.go`, `role.go`, `promotion.go`, `dist_group_commit.go`, `repl_proto.go` authority portions.

---

## 3. File-by-file classification

### 3.1 `superblock.go` — on-disk header

| Function / Type | Classification | Rebind / rationale |
|---|---|---|
| `type Superblock struct` (layout) | **PORT-AS-IS** | Byte-for-byte on-disk format; version-gated. |
| `type superblockOnDisk struct` | **PORT-AS-IS** | Same. |
| `SuperblockSize`, `MagicSWBK`, `CurrentVersion` constants | **PORT-AS-IS** | On-disk contract. |
| `NewSuperblock(volumeSize, opts)` | **PORT-REBIND** | Constructor takes `CreateOptions`; rebind to V3 create-path. Callers DO NOT pass Epoch — must be zero at create (see field note below). |
| `(sb *Superblock) WriteTo(w io.Writer)` | **PORT-AS-IS** | Serialization; no semantic change. |
| `ReadSuperblock(r io.Reader)` | **PORT-AS-IS** | Deserialization. |
| `(sb *Superblock) Validate()` | **PORT-AS-IS** | Invariant check; purely local (magic, version, sizes, LSN monotonicity). |
| `Superblock.Epoch` **field** | **PORT-REBIND (semantic)** | Persists as `uint64` for on-disk compat, BUT semantic rebinds from "fencing epoch authority writes here" to "last-observed assignment epoch, snapshot at time of Assignment.Accept". Storage NEVER advances this value. On startup, if superblock.Epoch > currently-assigned epoch → report NotReady with diagnostic evidence (§3.2 #4). Code comment + godoc rewritten. |
| `Superblock.ExpandEpoch` **field** | **PORT-AS-IS** | Local operation-ID for expand flow; not an authority epoch. Keep semantic; rename to `ExpandOpID` during port if time allows to avoid future confusion. |
| `Superblock.DurabilityMode` / `StorageProfile` / `PreparedSize` | **PORT-AS-IS** | Local configuration/state. |
| `Superblock.WAL*` fields (Offset, Size, Head, Tail, CheckpointLSN) | **PORT-AS-IS** | Pure WAL geometry/state. |
| `ErrNotBlockVol`, `ErrUnsupportedVersion`, `ErrInvalidVolumeSize`, `ErrInvalidSuperblock` | **PORT-AS-IS** | Local errors. |

**Risk #3 resolution**: `Epoch` field ports because the on-disk format is stable and the recovery comparison needs some local mirror. It is NOT a fencing knob — storage has no writeGate in V3 (see §3.7). The godoc **must** be rewritten to match §3.2; failure to update the godoc is a stop-rule #4 failure (advertised != implemented).

### 3.2 `wal_entry.go` — WAL record format

| Function / Type | Classification | Rebind / rationale |
|---|---|---|
| `type WALEntry struct` | **PORT-AS-IS** | On-disk record format. |
| `(e *WALEntry) Encode()` | **PORT-AS-IS** | Serialization. |
| `DecodeWALEntry(buf)` | **PORT-AS-IS** | Deserialization. |
| WAL entry type constants (`WALEntryWrite`, `WALEntryTrim`, etc.) | **PORT-AS-IS** | Wire-stable enum. |

No authority coupling. Fully PORT-AS-IS.

### 3.3 `wal_writer.go` — WAL append path

| Function / Type | Classification | Rebind / rationale |
|---|---|---|
| `type WALWriter struct` | **PORT-AS-IS** | Local WAL state. |
| `NewWALWriter(fd, walOffset, walSize, head, tail)` | **PORT-REBIND** | Constructor; rebind fd source from V2 `*os.File` to V3 backend fd accessor if V3 wraps the file in an abstraction. If V3 keeps raw `*os.File`, pure PORT-AS-IS. |
| `physicalPos`, `used`, `UsedFraction` | **PORT-AS-IS** | Pure math. |
| `Append(entry *WALEntry)` | **PORT-AS-IS** | Append path; no authority coupling. |
| `writePadding` | **PORT-AS-IS** | Internal. |
| `AdvanceTail(newTail)` | **PORT-AS-IS** | Local state transition. |
| `Reset()` | **PORT-AS-IS** | Local state. |
| `Head`, `Tail`, `LogicalHead`, `LogicalTail` | **PORT-AS-IS** | Accessors. |
| `ScanFrom(fd, walOffset, ...)` | **PORT-AS-IS** | Recovery scan; local-only state. |
| `Sync()` | **PORT-AS-IS** | fsync. |

No authority coupling. Fully PORT-AS-IS / PORT-REBIND.

### 3.4 `wal_admission.go` — WAL pressure gate

| Function / Type | Classification | Rebind / rationale |
|---|---|---|
| `type WALAdmission struct` | **PORT-AS-IS** | Pure mechanism (semaphore + watermarks). |
| `type WALAdmissionConfig struct` | **PORT-REBIND** | Callbacks rebind: `NotifyFn` → V3 flusher wake; `ClosedFn` → V3 volume-host close signal; `Metrics` → V3 metrics facility. None are authority callbacks. |
| `NewWALAdmission(cfg)` | **PORT-REBIND** | Uses Config above. |
| `PressureState()` | **PORT-AS-IS** | Pure observation. |
| `SoftPressureWaitNs`, `HardPressureWaitNs`, `SoftMark`, `HardMark` | **PORT-AS-IS** | Accessors. |
| `Acquire(timeout)` | **PORT-AS-IS** | Blocking admission; no authority surface. |
| `recordAdmit`, `Release` | **PORT-AS-IS** | Local. |
| `ErrWALFull`, `ErrVolumeClosed` | **PORT-AS-IS** | Local errors. |

**Risk #2 resolution**: WAL pressure is pure local mechanism. No function reads/writes Epoch / EV / role. `NotifyFn` wakes flusher (local); `ClosedFn` queries volume closure (local). **No reclassification needed** — the risk was "conflated with authority backpressure", but V2 already kept them separate. Ports cleanly.

Stop-rule guard: if during T3 port any caller wires `NotifyFn` or `ClosedFn` to an authority-ringing callback, that's a §3.2 violation. Port-time QA should grep wiring sites.

### 3.5 `dirty_map.go` — in-memory pending flush set

| Function / Type | Classification | Rebind / rationale |
|---|---|---|
| `type dirtyEntry`, `dirtyShard`, `DirtyMap`, `rangeEntry`, `SnapshotEntry` | **PORT-AS-IS** | In-memory sharded map. |
| `NewDirtyMap(numShards)` | **PORT-AS-IS** | Constructor. |
| `shard(lba)` | **PORT-AS-IS** | Internal shard lookup. |
| `Put(lba, walOffset, lsn, length)` | **PORT-AS-IS** | Covered by feedback_porting_discipline.md Phase 08 lesson: V2's compare-and-delete pattern (LBA + expected LSN match) MUST port verbatim. |
| `Get(lba)` | **PORT-AS-IS** | |
| `Delete(lba)` | **PORT-AS-IS** | **CRITICAL** — V2 uses compare-and-delete (LBA + expected LSN). Simplifying to unconditional delete-by-LBA is the Phase 08 bug (see memory feedback entry). sw must copy the V2 algorithm verbatim. |
| `Range(start, count, fn)` | **PORT-AS-IS** | Iteration. |
| `Clear()` | **PORT-AS-IS** | Used during recovery / rebuild. |
| `Len()` | **PORT-AS-IS** | Accessor. |
| `Snapshot()` | **PORT-AS-IS** | For flusher batching; LSN-sorted per Phase 08 lesson. |

Stop-rule guard: Phase 08 lesson — V2's sort-by-LSN-before-batching MUST port. Any V3 "simplified" version that drops the sort will re-introduce the data-loss bug on recovery.

### 3.6 `group_commit.go` — fsync batching

| Function / Type | Classification | Rebind / rationale |
|---|---|---|
| `type GroupCommitter struct` | **PORT-AS-IS** | Local batching mechanism. |
| `type GroupCommitterConfig struct` | **PORT-REBIND** | Callbacks rebind: `SyncFunc` → V3 backend.Sync; `OnDegraded` → V3 local health event (consumed by T1 volume-host, ringed to master via heartbeat — observation path, NOT authority publish); `PostSyncCheck` → **DROP** (see row below); `Metrics` → V3 metrics. |
| `NewGroupCommitter(cfg)` | **PORT-REBIND** | Uses Config above. |
| `Run()` | **PORT-AS-IS** | Main loop; local mechanism. |
| `Submit()` | **PORT-AS-IS** | Waiter registration. |
| `Stop()`, `markStoppedAndDrain` | **PORT-AS-IS** | Shutdown. |
| `SyncCount()` | **PORT-AS-IS** | Test accessor. |
| `callSyncFunc()` | **PORT-AS-IS** | Wraps syncFunc with panic-guard. |
| **PostSyncCheck semantic** | **NEVER (semantic)** | In V2 this hook fired `writeGate()` post-sync to detect fencing expiry during a batch. V3 has no writeGate; callers must NOT populate `PostSyncCheck` with any authority check. Keep the struct field for V2 port-fidelity (it's an optional callback; nil is valid), but the V3 constructor's default MUST leave it nil, and port-time QA greps callers to confirm none wire an authority check into it. |

Rebind targets summary:
- `SyncFunc` → V3 backend / fd-provider
- `OnDegraded` → V3 local health event stream (observed by volume host → heartbeat → master, per §3.2)
- `PostSyncCheck` → nil; NEVER rebind to authority check
- `Metrics` → V3 metrics

### 3.7 `write_gate.go` — fencing check

| Function / Type | Classification | Rebind / rationale |
|---|---|---|
| `(v *BlockVol) writeGate()` | **NEVER** | §3.2 fix + §11.5 #4: storage does not fence. V3 frontend already checked identity via `frontend.Backend`; by the time storage sees a Write, authority validity is the frontend's problem. No storage-side fencing. |
| `ErrNotPrimary`, `ErrEpochStale`, `ErrLeaseExpired` | **NEVER** | Authority-surface errors. Equivalent errors in V3 live in the authority package and surface via `frontend.Backend.Write` returning `frontend.ErrStalePrimary` / `frontend.ErrBackendClosed`. Storage never emits them. |

**Risk #1 resolution**: `write_gate.go` does NOT port. The whole file is NEVER. This is the cleanest outcome — single-function file, unambiguous. §3.2 fix #4 makes this call explicitly: "If local state disagrees with master-published, report NotReady + evidence; do not act as authority." writeGate is the classic act-as-authority pattern.

If sw accidentally ports writeGate (copy-paste from V2), that's a §8C.3 trigger #4 escalation — sw stops, user signs a Discovery Bridge acknowledging the audit miss, then removes it.

---

## 4. Rebind target reference

All PORT-REBIND decisions in this audit reference one of:

| Target | Description | Example |
|---|---|---|
| `frontend.Backend` | V3 per-volume backend interface (already used by T2 iSCSI/NVMe) | wal_writer fd source |
| `frontend.Identity` | Opaque authority-identity handle, passed through to storage but never inspected for fencing | — |
| V3 metrics facility | (name TBD — likely `core/metrics` or similar) | GroupCommitter.Metrics |
| V3 local health event stream | T1-built volume host (path not yet named in T3-start) | GroupCommitter.OnDegraded |
| V3 volume-host close signal | T1 coordination of volume lifecycle | WALAdmission.ClosedFn |

Any rebind target not in this list surfaces during T3a–T3e port work; new targets require mini-port-plan §5.2 note.

---

## 5. Stop-rule application

Three stop-rules inform this audit (from T3-start §8 + BUG-001 lessons):

| # | Rule | Trigger in this audit |
|---|---|---|
| 1 | Port means port — do not simplify | dirty_map.go `Delete` + `Snapshot` — V2's sort + compare-delete MUST port verbatim (Phase 08 lesson) |
| 2 | Mechanism vs policy — policy goes elsewhere | write_gate.go (policy = fencing) → NEVER |
| 3 | Observed fact vs published fact | Superblock.Epoch → PORT-REBIND (observed), not PORT-AS-IS with V2 "fencing" semantic |
| 4 | Advertised ≡ implemented | Superblock.Epoch godoc MUST be rewritten to match observation-only semantic |

---

## 6. Out-of-audit observations

Things I noticed during review that are NOT function-level classifications but deserve flagging for T3a mini port plan:

1. **`CreateOptions` struct** (referenced in superblock.go `NewSuperblock`) — not inspected in this audit. T3a plan should classify it separately if its fields cross into authority surface.
2. **`EngineMetrics`** — referenced by `WALAdmission` and `GroupCommitter`. V3 needs a concrete metrics interface with matching shape; TBD in T3a design doc.
3. **`ErrVolumeClosed`** — used by WALAdmission; needs a V3 home. Probably co-located with the new volume-host close signal.
4. **Phase 4A residue**: the V2 `Superblock.Epoch` comment says "fencing epoch (0 = no fencing, Phase 3 compat)". That comment is Phase 4A-era. V3 port MUST rewrite the comment; any leftover Phase-4A language is a §3.2 leak.

---

## 7. Summary matrix

| File | PORT-AS-IS | PORT-REBIND | DEFER | NEVER |
|---|:-:|:-:|:-:|:-:|
| superblock.go | 8 | 2 | 0 | 0 |
| wal_entry.go | 4 | 0 | 0 | 0 |
| wal_writer.go | 11 | 1 | 0 | 0 |
| wal_admission.go | 8 | 2 | 0 | 0 |
| dirty_map.go | 12 | 0 | 0 | 0 |
| group_commit.go | 7 | 2 | 0 | 1 (semantic) |
| write_gate.go | 0 | 0 | 0 | 2 |
| **totals** | **50** | **7** | **0** | **3** |

50 PORT-AS-IS → the bulk is faithful port, consistent with V2 being mature mechanism.
7 PORT-REBIND → rebind targets all local (metrics / backend / volume-host); no authority publish.
0 DEFER → these 7 files are T3a foundation; no deferrals.
3 NEVER → all risk-flagged (write_gate 2 + GroupCommitter.PostSyncCheck semantic).

---

## 8. Sign-off

| Role | Name | Date | Status |
|---|---|---|---|
| sw | Claude (sw agent) | 2026-04-22 | ✅ initial draft (§1-§9) + §10.7 sanity confirmation (5/5 findings verified) + §13 Q2/Q3/Q4 LOCKED |
| QA Owner | Claude (QA agent) | 2026-04-22 | ✅ **SIGNED** per §8C.2 — §10.3 walk-through LOCKED (48 MATCHES + 2 MATCHES-BETTER + 0 VIOLATIONS), Addendum A (impl-identity + matrix) landed, Option 3 for Epoch gap deferred to T3b per §10.5 |

QA review checklist (per §8C.5 + T3-start):
- [ ] Every public function appears exactly once
- [ ] Every PORT-REBIND has an explicit rebind target
- [ ] Every NEVER cites §3.2 or a stop-rule
- [ ] Risk #1 (write_gate) classified NEVER
- [ ] Risk #2 (WAL pressure) classified PORT-AS-IS / PORT-REBIND with no authority coupling
- [ ] Risk #3 (Superblock.Epoch) classified PORT-REBIND with semantic note, not PORT-AS-IS
- [ ] Phase 08 dirty_map lesson cited explicitly
- [ ] Second-wave files (flusher / recovery / smartwal / etc.) called out as OUT of this audit
- [ ] No function escalates §8C.3 trigger #4 after classification

---

## 9. Change log

| Date | Change | Author |
|---|---|---|
| 2026-04-22 | Initial draft covering 7 T3.0 foundation files; 50 / 7 / 0 / 3 split | sw |
| 2026-04-22 | §10–§14 appended: scope reframed per user direction "keep V3 `core/storage/`, find gaps"; sw's §1–§9 V2 function classification reinterpreted as **validation spec** (V3 existing port must match); integration-layer gap analysis + operational-gate API lock + 4-batch split added | QA + sw joint |

---

## 10. Scope reframe — sw's §1–§9 as validation spec, not re-port plan

After sw's audit was drafted, user direction LOCKED (see chat 2026-04-22 late afternoon):

> *"V3 设计了 interface 本来是想支持 regular/smartWAL，比 V2 好，所以保留 interface 和核心已经 test 过的 storage code，其他看看差什么。"*

V3 `core/storage/` commit `97a9695` + `9004c74` (2026-04-16) landed a pluggable `LogicalStorage` interface + 3 implementations (`BlockStore` + `WALStore` + `smartwal.Store`); tests green. This is **kept**. T3 does NOT re-port V2 files into V3 from scratch.

**Sw's §1–§9 reinterpretation** (same content, different use):

| Sw said | Reframed as |
|---|---|
| "PORT-AS-IS: copy verbatim from V2" | **V3 must exhibit the same behavior**; QA verifies by cross-reading V3 impl vs sw's V2 classification |
| "PORT-REBIND: copy, rebind call sites" | **V3's rebind target must match sw's expectation**; if V3 already rebinds correctly, no action; if V3 rebinds to wrong target (e.g., authority surface), §8C.3 trigger #4 escalate |
| "NEVER: do not port" | **V3 must NOT contain this behavior**; QA greps V3 `core/storage/` to confirm absence (e.g., `writeGate` must not exist) |
| "DEFER" (none in sw's audit) | N/A for this audit |

This reframe turns sw's 60-row function table into a **validation checklist** for QA to walk over `core/storage/`. Each row becomes a "V3 is/isn't doing X" question.

### 10.1 V3 existing-port validation (QA to complete before T3a opens)

QA walks each row of §3.1–§3.7 against V3 `core/storage/` and marks:
- ✅ **MATCHES** — V3 impl agrees with sw's classification
- ⚠️ **DRIFT** — V3 deviates; specify how (may still be acceptable if deviation is defensible — e.g., V3's better interface)
- ❌ **GAP** — V3 is missing a behavior sw's classification says must exist; T3a must add
- 🚫 **VIOLATION** — V3 contains a NEVER behavior; §8C.3 escalate

Completion blocks T3a sign. QA estimates 1-2 hours for 60 rows.

### 10.2 Initial spot-checks (QA pre-audit, not comprehensive)

| Item | V3 `core/storage/` status | Verdict |
|---|---|---|
| `writeGate` function (sw: NEVER) | grep shows zero occurrences in `core/storage/` | ✅ no violation |
| `Superblock.Epoch` field (sw: PORT-REBIND with observation-only semantic) | `superblock.go` has epoch field; advancement path not yet audited | ⚠️ needs QA §10.1 walk-through |
| `DirtyMap.Delete` compare-and-delete (sw: CRITICAL — Phase 08 lesson) | `dirty_map.go` behavior not yet audited | ⚠️ needs QA §10.1 walk-through + test assertion |
| `GroupCommitter.PostSyncCheck` field (sw: NEVER semantic; optional field kept but default nil) | V3 existing `group_commit.go` presence unknown; QA to check | ⚠️ needs QA §10.1 walk-through |
| `WALAdmission.Acquire / PressureState` (sw: pure mechanism) | `wal_admission.go:90 Acquire(timeout)`, line 71 `PressureState()` — matches V2 shape | ✅ signature match (behavior match TBV) |

Full §10.1 walk-through below in §10.3.

### 10.3 §10.1 V3 existing-port walk-through (QA completed 2026-04-22)

Each row: sw's V2 classification → V3 `core/storage/` verdict.

Legend: ✅ MATCHES / ⚠️ DRIFT (acceptable) / ❌ GAP / 🚫 VIOLATION

#### 10.3.1 `superblock.go` (sw §3.1)

| Sw row | V3 equivalent | Verdict |
|---|---|---|
| `type Superblock struct` layout PORT-AS-IS | `type superblock` pkg-private, line 32 | ✅ MATCHES |
| `type superblockOnDisk` PORT-AS-IS | embedded serialization | ✅ MATCHES |
| `SuperblockSize/MagicSWBK/CurrentVersion` | lines 12-30 | ✅ MATCHES |
| `NewSuperblock(volumeSize, opts)` PORT-REBIND | `newSuperblock(volumeSize, opts createOptions)` line 50; `createOptions` has BlockSize/ExtentSize/WALSize only — **no Epoch param** | ✅ MATCHES |
| `WriteTo / ReadSuperblock / Validate` | `writeTo` (82) / `readSuperblock` (115) / `validate` (162) | ✅ MATCHES |
| **`Superblock.Epoch` field PORT-REBIND** | **NOT PRESENT in V3** — zero grep matches | ❌ **GAP (non-blocking)** — see §10.5 |
| `Superblock.ExpandEpoch` PORT-AS-IS | NOT PRESENT in V3 | ❌ GAP (T3 OUT; expand flow not in G4 canonical) |
| DurabilityMode / StorageProfile / PreparedSize PORT-AS-IS | NOT PRESENT in V3 (V3 createOptions simpler) | ⚠️ DRIFT (acceptable — V3 subset sufficient for G4) |
| WAL fields (Offset/Size/Head/Tail/CheckpointLSN) | `WALCheckpointLSN` present (line 250 write-back path) | ✅ MATCHES — G-int.6 row 2 checkpoint-LSN write-back CONFIRMED |
| Error vars | lines 18-30 | ✅ MATCHES |

#### 10.3.2 `wal_entry.go` (sw §3.2)

All 4 rows MATCH. `walEntry` (line 49), `encode` (63), `decodeWALEntry` (114), entry type constants (line 11).

#### 10.3.3 `wal_writer.go` (sw §3.3)

11/12 MATCH. `walWriter` (26), `newWALWriter` fd-as-raw-`*os.File` (38), `physicalPos`/`used`/`usedFraction` (48/50/186), `append` (58), `writePadding` (101), `advanceTail` (134), `reset` (149), accessors (156/162/168/174), `fsync` (181; semantic = V2 `Sync`). `ScanFrom` → 1 row DEFER for sw spot-check T3a.

#### 10.3.4 `wal_admission.go` (sw §3.4)

9/10 MATCH. `walAdmission` (30), `walAdmissionConfig` with NotifyFn+ClosedFn (44), `newWALAdmission` (58), `PressureState` (71), accessors (82-85), `Acquire` (90), `Release` (156), `errVolumeClosed` (11). Doc comment at line 52-56 explicitly "Behavior mirrors V2's weed/storage/blockvol/wal_admission.go" — **G-int.6 row 1 WAL-pressure 1:1 CONFIRMED**. Metrics callback absent (⚠️ DRIFT acceptable — V3 has no metrics facility yet; G17/T8 territory).

#### 10.3.5 `dirty_map.go` (sw §3.5)

10/12 MATCH + 1 MATCHES-BETTER + 1 VERIFY. `dirtyMap` (23), `newDirtyMap` (30), `shard` (44), `put` (46), `get` (53), `clear` (134), `len` (123), `snapshot` (108). **MATCHES-BETTER**: V3 has **both** `delete(lba)` (64, unconditional) AND `compareAndDelete(lba, expectedLSN)` (85). Flusher uses `compareAndDelete` (flusher.go:171, 236) — **Phase 08 lesson correctly applied**. Unconditional `delete()` has **zero callers in storage package** — footgun unexercised. VERIFY: snapshot LSN-sort invariant needs sw spot-check in T3a test; `Range` not found line-by-line (DEFER spot-check).

#### 10.3.6 `group_commit.go` (sw §3.6)

7/9 MATCH + 1 MATCHES-BETTER + 1 DRIFT. `GroupCommitter` (20), `NewGroupCommitter` (43), `Run` (61), `Stop` (155), `markStoppedAndDrain` (122), `SyncCount` (164), `fsyncSafe` (113; = V2 `callSyncFunc`). **MATCHES-BETTER**: `GroupCommitterConfig` has NO `PostSyncCheck` field — **mechanically cannot wire authority check** (sw's NEVER semantic achieved by absence, stronger than sw's spec). ⚠️ DRIFT: V3 `GroupCommitterConfig{SyncFunc, MaxDelay, MaxBatch}` lacks `OnDegraded` / `Metrics` callbacks — T3b mini-plan decides if `OnDegraded` is needed for Recovery health event emission. `Submit` V3 equivalent may be `SyncCache` (135) — sw VERIFY T3a.

#### 10.3.7 `write_gate.go` (sw §3.7)

Both NEVER rows ✅ MATCHES (absence):
- `grep writeGate` in `core/storage/` — **zero matches**
- `grep ErrNotPrimary|ErrEpochStale|ErrLeaseExpired` in `core/storage/` — **zero matches**

Authority-surface errors correctly absent from storage. No violation.

### 10.4 Walk-through summary

| Metric | Count |
|---|---|
| Total rows audited | 59 |
| ✅ MATCHES | 48 |
| ✅ MATCHES-BETTER (V3 cleaner than sw spec) | 2 (compareAndDelete split; no PostSyncCheck field) |
| ⚠️ DRIFT acceptable / VERIFY (sw spot-check T3a) | 5 (ScanFrom, Range, snapshot LSN-sort, Submit, DurabilityMode subset) |
| ⚠️ DRIFT needing T3b decision | 1 (GroupCommitterConfig OnDegraded) |
| ❌ GAP blocking | **1 — `superblock.Epoch` absent (see §10.5)** |
| ❌ GAP deferrable | 2 (Metrics callbacks; ExpandEpoch) |
| 🚫 VIOLATION | 0 |

### 10.5 Blocking gap: `superblock.Epoch` absent

Sw §3.1 classified `Superblock.Epoch` as PORT-REBIND (observation-only). V3 dropped the field entirely. Architecturally clean (storage can't accidentally advance what it doesn't store) but creates gap: T3b Recovery "compare local superblock.epoch vs assigned epoch" has no local source.

**Resolution options** (T3b mini-plan decides):

- **Option 1**: T3b adds `Epoch uint64` field to V3 `superblock` as observation-only (~10 LOC + godoc)
- **Option 2**: Use `WALCheckpointLSN` as comparison proxy (requires authority to publish LSN watermarks)
- **Option 3**: No local epoch mirror; per-I/O adapter fence check (G-int.2) is authoritative; Recovery always reports ready if reload clean

**QA recommendation: Option 3**. Per-I/O adapter fence check already preserves `INV-FRONTEND-002.*` facet rows. Local epoch in superblock is redundant. Zero LOC added to storage layer. Decision deferred to T3b mini-plan; does NOT block T3.0.

### 10.6 Walk-through complete — T3.0 Q1 ready

Q1 (§13) **LOCKED**. V3 existing-port validation:
- 48 MATCHES / 2 MATCHES-BETTER / 6 DRIFT-or-VERIFY / 3 GAP / 0 VIOLATION
- Phase 08 lesson correctly applied (flusher uses compareAndDelete)
- writeGate correctly absent
- G-int.6 row 1 (WAL-pressure 1:1) + row 2 (checkpoint-LSN write-back) CONFIRMED
- Blocking gap (superblock.Epoch) has QA-recommended Option 3 resolution deferred to T3b

**QA side ✅ ready to single-sign T3.0**. Awaiting sw confirmation that §14.1 sw-side sanity pass over §10.3 is clean.

### 10.7 sw sanity-pass comments (2026-04-22, recorded for closure report)

**A. GroupCommitterConfig `OnDegraded` direction (per §10.3.6 DRIFT)**

sw preferred direction, locked here for T3b mini-plan:

> V3's existing "fsync error → returned to waiters via pending channels → propagates up to `WALStore.Write` caller" is a cleaner coupling than V2's `OnDegraded` fan-out. The missing piece is the higher-layer observer (adapter or host) watching error rates and firing local health events. Recommendation: **do NOT reinstate `OnDegraded` on GroupCommitterConfig**; keep storage pure-mechanism; put degraded-signal derivation at the adapter/host layer where it has Identity + ProjectionView context.

T3b mini-plan adopts this direction unless QA surfaces a counter-argument during T3b sign.

**B. `superblock.Epoch` framing for closure report (per §10.5 Option 3)**

sw note on §3.1 row vs §10.3 walk-through:

> My sw §3.1 row said `Superblock.Epoch` PORT-REBIND (persist but rebind semantic). QA §10.3 found V3 doesn't have the field at all. That's a **stronger §3.2 outcome** than my original proposal — storage can't accidentally misuse epoch because it's not there. The "local epoch vs assigned epoch startup comparison" in §G-int.5 becomes unnecessary; adapter's per-I/O lineage check is the only authority-drift guard. Both rows can stand (mine = "what sw proposed", QA's = "what V3 implements and why it's stronger"); note in T3 closure report so future agents don't re-introduce Epoch thinking it was dropped by accident.

T3 closure report §B must include this framing: "V3 `superblock` intentionally has no `Epoch` field; per-I/O adapter fence check is sole authority-drift guard; re-introducing an Epoch field to storage is a §8C.3 trigger #4 violation absent a new Discovery Bridge".

### 10.8 Addendum A — Pre-T3a-open additions (PM direction 2026-04-22)

PM added two concrete T3 requirements after T3.0 sign, pre-T3a-open. Recorded here (sign holds; these are scope additions to T3a and T3c, not modifications to the §10.3 audit verdicts):

**Addition 1: Both `walstore` and `smartwal.Store` must be tested**

Rationale: the two `LogicalStorage` implementations are interchangeable by interface design; production prefers `smartwal.Store`, but `walstore` must keep working as a fallback + comparison baseline. Tests that exercise only one variant leave the other unguarded against silent regression.

Scope:
- **T3a adapter unit tests**: parameterize over both impls (test table with `LogicalStorage` constructor factory), assert adapter behaves identically against both
- **T3c scenarios**: each of the 4 scenario YAMLs (crash-recovery / fsync-boundary / disk-fill / wal-replay) runs against both impls — either duplicated YAMLs (`*-walstore.yaml` / `*-smartwal.yaml`) or single YAML with a variant axis in the Go replay
- **T3c `DurableProvider` smoke**: Provider opens either impl based on a selector (production default = `smartwal`); smoke tests both selector paths

Locked constraint: if any single scenario passes on one impl but fails on the other, T3c closure does NOT pass. Variant skew is a regression.

**Addition 2: Superblock records impl kind + version (storage-layer self-identification)**

Rationale: "prod 如果能用 smart 最好" + ops/tooling need ability to inspect storage and know what they're looking at without guessing. Impl-kind + version in the superblock enables:
- Fail-fast if `DurableProvider` opens with wrong impl selector vs stored format (catches config drift)
- Future format migrations know the starting point
- Diagnostic tools (`blockvol inspect`) report impl identity

Scope (T3a):
- Add two fields to `core/storage/superblock.go` `superblock` struct:
  - `ImplKind uint8` (enum: 0=unassigned/error, 1=walstore, 2=smartwal)
  - `ImplVersion uint32` (per-impl internal schema version)
- Both fields set at create (`newSuperblock` callers pass them)
- Both fields validated on `readSuperblock`; unknown `ImplKind` → `ErrInvalidSuperblock`
- Superblock on-disk layout version constant `CurrentVersion` bumps by 1 (on-disk format change — existing tests of old superblock payload need a one-shot migration or are re-created)

Classification under §1 framework (addendum to §3.1):
- `ImplKind` + `ImplVersion` fields: **NEW (V3-native)** — not in V2; no conflict with PCDD-STUFFING-001 (these identify local mechanism, not authority)
- No re-classification of existing §3.1 rows

Locked: T3a delivers Addition 2 with superblock schema bump; Addition 1 test coverage matrix lands through T3a + T3c.

**Ledger row addition** (queued for T3a close):
- `INV-DURABLE-IMPL-IDENTITY-001` — Superblock records `ImplKind` + `ImplVersion`; `DurableProvider.Open` mismatch-detection rejects with a clear error, not silent coercion

**Impact on T3a scope + LOC estimate**:
- T3a prod LOC: +30 (superblock field extension + validation) + ~50 (matrix test parameterization plumbing) = 400-550 → 480-630
- T3a test LOC: +100 (per-impl test table) = 250-300 → 350-400
- T3c test LOC: +200 (scenarios replayed over two impls) = 500-700 → 700-900
- Wall-clock: +0.5 day in T3a + 0.5 day in T3c = net +1 day

**Impact on T3.0 sign**: none. Additions are T3a/T3c scope; T3.0 single-sign per §8C.2 still ✅ valid. Documented here for T3a mini-plan referencee.

---

## 11. Integration gap analysis (what's missing BEYOND the 7 muscle files)

Even after §10 validation passes, V3 reaching G4 needs integration layer NOT in `core/storage/`:

### G-int.1 — Interface bridge: `LogicalStorage` → `frontend.Backend`

V3 `core/frontend/types.go`:
```go
type Backend interface {
    Read(ctx, offset int64, p []byte) (int, error)
    Write(ctx, offset int64, p []byte) (int, error)
    Identity() Identity
    Close() error
}
```

V3 `core/storage/LogicalStorage`:
```go
Write(lba uint32, data []byte) (lsn uint64, err error)
Read(lba uint32) ([]byte, error)
Sync() (stableLSN uint64, err error)
Recover() (recoveredLSN uint64, err error)
Boundaries() (R, S, H uint64)
NextLSN() uint64
```

Gap: byte↔LBA translation, ctx threading, Sync policy (see G-int.2).

### G-int.2 — Sync policy DECISION: explicit `Backend.Sync()` (sw addition A)

- iSCSI emits `SYNCHRONIZE CACHE` (SPC-5 §7.26); NVMe emits `Flush` opcode
- Auto-sync-per-Write kills IOPS perf. **Rejected.**
- **Locked**: extend `frontend.Backend` interface with `Sync(ctx) error`. T3a adapter calls `LogicalStorage.Sync()`. T3b wires iSCSI `SYNC_CACHE` handler + NVMe `Flush` handler to `Backend.Sync(ctx)`.

### G-int.3 — Operational gate API LOCKED signature (sw granularity alignment)

Prevents BUG-001-class fragmentation where T3a adapter ships without the gate API and T3b retrofits. API:

```go
// core/frontend/types.go — additional Backend method
SetOperational(ok bool, evidence string)

// core/frontend/durable/storage_adapter.go
type StorageBackend struct {
    storage      storage.LogicalStorage
    identity     frontend.Identity
    lineageSrc   frontend.ProjectionView  // current authority lineage
    blockSize    uint32

    operational   atomic.Bool
    nonopEvidence atomic.Value // string
}

// SetOperational flips the backend between "operational" (accepts I/O)
// and "not operational" (all I/O returns ErrNotReady).
//
// Invariant: before first SetOperational(true, _) call, every I/O
// returns frontend.ErrNotReady.
//
// Invariant: SetOperational does NOT touch identity/lineage/epoch;
// it is pure local-state readiness. Authority publication stays on
// the master path (PCDD-STUFFING-001).
func (b *StorageBackend) SetOperational(ok bool, evidence string)

func (b *StorageBackend) operationalGate() error {
    if !b.operational.Load() {
        return fmt.Errorf("%w: %s", frontend.ErrNotReady, b.nonopEvidence.Load())
    }
    return nil
}
// Read/Write/Sync each call operationalGate() first, then lineage
// check (captures Identity at Open; compares to ProjectionView on
// each call; mismatch → ErrStalePrimary), then the storage call.
```

**This signature is LOCKED at T3.0 sign.** T3a ships full adapter surface. T3b wires the gate without retrofit.

### G-int.4 — `DurableProvider` (sw addition E: testback stays for tests)

New `core/frontend/durable/provider.go` implements `frontend.Provider` for production (`cmd/blockvolume`). `core/frontend/testback.StaticProvider` unchanged — unit tests keep injection path.

### G-int.5 — Recovery integration (PM fix #1 read-side only)

Verified wire point: `core/host/volume/healthy_executor.go` exists with `SetOnSessionStart / SetOnSessionClose / SetOnFenceComplete / Probe / StartCatchUp` hooks. Recovery plugs in via these — no new publication path.

Sequence:
```
cmd/blockvolume startup:
  1. Open LogicalStorage (walstore or smartwal per config)
  2. LogicalStorage.Recover() → (recoveredLSN, err)
  3. Read superblock.epoch + compare to Identity from assignment
     3a. If local epoch > assigned epoch:
         → adapter.SetOperational(false, "local epoch ahead of assignment")
         → report NotReady via status_server.go /status (read-side; no publish)
     3b. If local epoch ≤ assigned epoch:
         → adapter.SetOperational(true, "recovered LSN=X epoch=Y")
         → HealthyPathExecutor.Probe proceeds (T1 path)
  4. Backend accepts I/O
```

### G-int.6 — sw addition C: 5-item V2 flusher drift-confirm

Beyond the 7 muscle files in sw's §3, sw flagged 5 V2 flusher behaviors needing drift-confirm against V3:

| V2 item | V3 substitute | Verdict (sw LOCKED 2026-04-22) |
|---|---|---|
| V2 WAL-pressure exits (`ErrWALFull` / `ErrVolumeClosed`) via `wal_admission.Acquire` | V3 `core/storage/wal_admission.go:110 errWALFull`, `:104 errVolumeClosed`, `:146 errWALFull`, `:149 errVolumeClosed` | ✅ **COVERED — LOCKED**. Exit-code 1:1 map confirmed; V3 uses unexported `err*` names (package-private by design; callers go through `WALStore.Write` which wraps). Control flow in V3 `wal_admission.go:90-156 Acquire` is structurally identical to V2: hard-watermark spinwait → soft-watermark scaled delay → semaphore acquisition. `PressureState()` returns the same `"hard"` / `"soft"` / `"normal"` trichotomy (V3 line 71, V2 line 67). |
| V2 `updateSuperblockCheckpoint / CheckpointLSN / SetCheckpointLSN` (public setter) | V3 `walstore.go:243 persistCheckpoint(highestLSN)` called by `flusher.go:226` after extent writes; V3 `walstore.go:272 CheckpointLSN()` public getter; V3 `walstore.go:260-266` re-encodes superblock + pwrite@0 | ✅ **COVERED — LOCKED**. Public getter + on-disk persistence wired. Note: V3 intentionally removes V2's public `SetCheckpointLSN` setter — checkpoint advance in V3 is *only* through the flusher path (one write authority). Sw classifies this drop as **safety improvement, not drift** — prevents external munging of checkpoint position. Any future caller that needs to force checkpoint must go through flusher hooks, not a direct setter. |
| `RetentionFloorFn / SetRetentionFloorFn` | None in V3 | ⏸ **DEFER-TO-T4 — LOCKED**. Rebuild retention is rebuild-layer concern; rebuild OUT of T3 per §G-int.7. When rebuild path lands (T4 or later), retention floor becomes a REINSTATE item against that batch's audit — NOT against T3. |
| `AddSnapshot / PauseAndFlush / Pause / Resume` | None in V3 | ⏸ **DEFER-TO-Tn — LOCKED**. Snapshot export + rebuild-gate coordination both OUT of T3. Same treatment as retention floor: reinstate with the owning batch's audit. |
| V2 `Flusher.SetFD` | V3 `walstore.go:131 OpenWALStore(path)` + `walstore.go:156 openInitialized(path, fd, sb)` | ✅ **COVERED — LOCKED**. V2 `flusher.go:524 SetFD` is explicitly marked test-only ("Test-only"). V3 achieves the same test-injection surface via the path-based `OpenWALStore` (production) + `openInitialized` (test; callers pass `*os.File` + pre-validated superblock). Production move-reopen pattern in V3 is `Close()` → `OpenWALStore(path)` — no FD-swap hook needed in production. |

**All 5 rows LOCKED 2026-04-22 by sw.** Zero REINSTATE items → T3a LOC budget unchanged. Two DEFER rows tracked against future batches per §G-int.7 rebuild-OUT discipline.

### G-int.7 — sw addition D: Rebuild OUT of T3 (explicit)

Rebuild-time durability (`ReplicaRebuilding` state, WAL retention-floor coordination surfaced during BUG-001) is **OUT of T3**. Any T3c scenario touching rebuild → §8C.3 trigger #3 (cross-T pollution) escalate.

---

## 12. Batch split (LOCKED)

Per sw granularity alignment to prevent BUG-001-class fragmentation:

| Batch | Content | Gate | Sign |
|---|---|---|---|
| **T3.0** | This doc. Sw §1–§9 V2 spec + QA §10.1 validation + §11 integration gaps + G-int.3 operational-gate API lock + G-int.6 drift-confirm LOCKED | No code; sign-gate | QA single-sign after sw flips §11 G-int.6 DRAFT→LOCKED + QA completes §10.1 walk-through |
| **T3a** | G-int.1 + G-int.2 + G-int.3 — complete `core/frontend/durable/storage_adapter.go` + Backend interface extension (Sync + SetOperational) + testback shim updates | Unit A (adapter) | QA single-sign §8C.2 |
| **T3b** | G-int.4 Provider + G-int.5 Recovery integration + iSCSI SYNC_CACHE wire + NVMe Flush wire | Units B+C (provider + recovery) | QA single-sign §8C.2 |
| **T3c** | 4 scenarios (crash-recovery / fsync-boundary / disk-fill / wal-replay) + iSCSI/NVMe continuity smoke + perf baseline artifact + T3 closure report | Acceptance | architect + PM + QA three-sign §8C.1 |

Wall-clock: T3.0 half-day + T3a 1-1.5 day + T3b 1-1.5 day + T3c 1-2 day = **4-5.5 days**.

LOC (revised):

| Batch | Prod LOC | Test LOC |
|---|---|---|
| T3.0 | 0 | 0 |
| T3a | 400-550 | 250-300 |
| T3b | 350-450 | 250-300 |
| T3c | 0-50 | 500-700 |
| **Total** | **~800-1050** | **~1000-1300** |

---

## 13. Open questions (pre-T3a blockers)

| # | Question | Owner | Blocks | Status |
|---|---|---|---|---|
| Q1 | §10.1 V3 existing-port validation walk-through complete | QA | T3.0 sign | ⏸ in progress |
| Q2 | G-int.6 row 1 (WAL-pressure exit-code 1:1 map) LOCKED | sw | T3.0 sign | ✅ **LOCKED 2026-04-22** (V3 `wal_admission.go:104/110/146/149`) |
| Q3 | G-int.6 row 2 (checkpoint-LSN write-back path wired) LOCKED | sw | T3.0 sign | ✅ **LOCKED 2026-04-22** (V3 `walstore.go:243 persistCheckpoint` called by `flusher.go:226`) |
| Q4 | G-int.6 row 5 (`SetFD` covered by `OpenWALStore/Recover`) LOCKED | sw | T3.0 sign | ✅ **LOCKED 2026-04-22** (V3 `walstore.go:131 OpenWALStore` + `:156 openInitialized`; V2 SetFD was test-only) |
| Q5 | Default WAL impl (smartwal vs walstore) for production `DurableProvider` | sw + QA joint | T3b mini-plan | ⏸ open |
| Q6 | `superblock.epoch` write policy — only on fence advance via adapter, or every checkpoint? | sw + QA joint | T3b mini-plan | ⏸ open |

Q2-Q4 ✅ sw side cleared. Q1 awaits QA §10.1 walk-through. Q5-Q6 block T3b open (not T3.0).

---

## 14. Updated sign-off

### 14.1 T3.0 sign table (single-sign per §8C.2)

| Role | Signer | Date | Prerequisite | Status |
|---|---|---|---|---|
| sw | Claude (sw agent) | 2026-04-22 | Flips G-int.6 Q2/Q3/Q4 DRAFT → LOCKED; sanity-passes §10.3 | ✅ **Q2/Q3/Q4 LOCKED**; §10.3 sanity ✅ confirmed (5/5 findings verified) + sw-side comments A (OnDegraded direction) + B (Epoch gap framing) recorded in §10.7 |
| QA Owner | Claude (QA agent) | 2026-04-22 | §10.3 walk-through LOCKED (§10.6); Option 3 recommended for Epoch gap (deferred to T3b) | ✅ **Q1 LOCKED**; T3.0 **SIGNED per §8C.2 single-sign** |

**Effect upon sign**: T3a opens. sw implements against §11 G-int.3 operational-gate API signature + Backend interface extension in §11 G-int.1/G-int.2. QA files T3a mini port plan (expected ~50 lines; single-signed) before code commit.

### 14.2 Ledger row plan

Rows queued ACTIVE at respective batch close:

| ID | Statement | Added at |
|---|---|---|
| `INV-DURABLE-001` | Acknowledged Write survives kill + restart; Read returns byte-exact | T3c crash-recovery scenario |
| `INV-DURABLE-WAL-REPLAY-001` | N acked writes + crash → all N recoverable | T3c wal-replay scenario |
| `INV-DURABLE-FSYNC-BOUNDARY-001` | Flushed preserved; unflushed may be lost; neither corrupted | T3c fsync-boundary scenario |
| `PCDD-DURABLE-DISK-FULL-001` | Disk-full → hard error; no silent success | T3c disk-fill scenario |
| `INV-DURABLE-EPOCH-PERSISTED-001` | Epoch persists through superblock; restart rejects stale-lineage | T3b recovery test + T3c |
| `INV-DURABLE-OPGATE-001` | Before `SetOperational(true)`, all I/O → `ErrNotReady`; preserves `INV-FRONTEND-002.*` under durable | T3a adapter unit test |
