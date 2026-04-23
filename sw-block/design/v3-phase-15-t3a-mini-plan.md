# V3 Phase 15 T3a — Mini Port Plan

**Date**: 2026-04-22
**Status**: QA single-signed per §8C.2; sw cleared to start
**Owner**: QA drafts + signs; sw implements
**Predecessor**: T3.0 port audit SIGNED (`v3-phase-15-t3-port-audit.md` incl. §10.8 Addendum A)
**Gate**: T3a — adapter + Backend interface extension + superblock impl-identity fields
**Successor**: T3b mini-plan (post T3a close)
**Governing methodology**: §8B PCDD + §8C accelerated-cadence governance (mid-T QA single-sign)

---

## 0. Scope (locked)

T3a delivers 4 things in one commit:

1. **G-int.1 + G-int.2 + G-int.3** — `core/frontend/durable/storage_adapter.go` with full surface: byte↔LBA translation, per-I/O fence check (preserves `INV-FRONTEND-002.*`), operational gate (`SetOperational`), and `Sync` implementation via `LogicalStorage.Sync`.
2. **Backend interface extension** — `core/frontend/types.go` gains `Sync(ctx) error` + `SetOperational(ok bool, evidence string)`. All existing `Backend` implementations (memback testbacks, iSCSI/NVMe harness backends) gain trivial impls.
3. **Addendum A Addition 2** — `core/storage/superblock.go` gains `ImplKind` + `ImplVersion` fields; `newSuperblock` accepts them; `readSuperblock` validates; `CurrentVersion` bumps +1 with migration strategy for existing test data.
4. **Addendum A Addition 1 (test infra only)** — unit tests for the adapter are parameterized over both `walstore` and `smartwal.Store` impls (full scenario matrix in T3c).

Out of T3a (per T3.0 §12 + Addendum A):
- `DurableProvider` → T3b
- Recovery integration → T3b
- Frontend-side `SYNC_CACHE` / Flush wires → T3b
- 4 G6 scenarios → T3c
- Perf smoke → T3c

---

## 1. File layout

### 1.1 New files

| Path | Purpose | Est LOC |
|---|---|---|
| `core/frontend/durable/storage_adapter.go` | `StorageBackend` struct + Read/Write/Sync/Close/Identity/SetOperational + byte↔LBA math + fence check | 350-450 |
| `core/frontend/durable/storage_adapter_test.go` | Unit tests (matrix-parameterized over walstore + smartwal) | 350-400 |

### 1.2 Modified files

| Path | Change | Est LOC delta |
|---|---|---|
| `core/frontend/types.go` | Add `Sync(ctx) error` + `SetOperational(ok bool, evidence string)` to `Backend` interface | +4 |
| `core/frontend/testback/testback.go` | `RecordingBackend` + `StaleRejectingBackend` gain trivial `Sync` (returns nil) + `SetOperational` (store in atomic.Value for diagnostics) | +20 |
| `core/storage/superblock.go` | Add `ImplKind uint8` + `ImplVersion uint32` fields; extend serialization; bump `CurrentVersion` to next value; validate in `readSuperblock` | +30 |
| `core/storage/walstore.go` | Pass `ImplKind=1` at `CreateWALStore` | +2 |
| `core/storage/smartwal/store.go` | Pass `ImplKind=2` at `CreateStore` | +2 |
| `core/storage/superblock_test.go` (if exists) + new `superblock_impl_identity_test.go` | Schema-bump migration test; unknown-ImplKind validation | +80 |
| Existing QA test backends (`writeCountingBackend`, `countingFaultBackend` in QA test files) | Gain trivial `Sync` + `SetOperational` | +15 |

### 1.3 Touched but not modified

- `core/storage/logical_storage.go` — interface unchanged (stop rule §5.5 in audit)
- `core/frontend/nvme/*.go` — SYNC wiring deferred to T3b; NVMe Flush still no-op in T3a; behavior unchanged
- `core/frontend/iscsi/*.go` — same, SYNC_CACHE wiring in T3b

### 1.4 Total T3a LOC budget

| Line class | Budget |
|---|---|
| Production | 480-630 (per audit §10.8 Addendum A revised estimate) |
| Test | 350-400 |
| Test-backend shim | ~35 |

---

## 2. Acceptance criteria (gate 1 full — QA single-sign per §8C.2)

| # | Criterion | Evidence |
|---|---|---|
| 1 | `StorageBackend` implements `frontend.Backend` complete — all 6 methods (Read, Write, Sync, SetOperational, Identity, Close) | Compile + unit test `TestT3a_StorageBackend_ImplementsBackend` |
| 2 | Byte↔LBA translation correct across sizes — writes spanning LBA boundaries aggregate, partial-block reads zero-pad tail | `TestT3a_StorageBackend_ByteLBATranslation_Matrix` |
| 3 | Per-I/O fence check rejects stale lineage — preserves `INV-FRONTEND-002.EPOCH / .EV / .REPLICA / .HEALTHY` under durable backend | `TestT3a_StorageBackend_FenceCheck_*` (4 sub-tests, one per facet) |
| 4 | Operational gate honored — before `SetOperational(true, _)`, every I/O returns `ErrNotReady`; after flip, I/O proceeds | `TestT3a_StorageBackend_OperationalGate_*` |
| 5 | `Sync` dispatches to `LogicalStorage.Sync` and returns error verbatim | `TestT3a_StorageBackend_Sync_DispatchesToStorage` |
| 6 | Addition 1 matrix: ALL adapter unit tests run against both `walstore` and `smartwal.Store` impls | Table-driven tests with factory slice; every test case executes N×2 |
| 7 | Addition 2: superblock on create records `ImplKind`=1 or 2; readSuperblock validates; unknown kind → `ErrInvalidSuperblock` | `TestT3a_Superblock_ImplKind_*` |
| 8 | Addition 2: `CurrentVersion` bump is forward-only; existing test data either migrated or regenerated; no silent-corrupt read of old format | `TestT3a_Superblock_VersionBump_*` + regen any existing test fixtures |
| 9 | Backend interface extension doesn't break existing tests — all prior T0/T1/T2 unit + component + L2 tests stay green | `go test ./core/... -count=1` |
| 10 | Boundary guard green — new `core/frontend/durable/` package does not import `core/authority` or `core/adapter` | `boundary_guard_test.go` extended to cover `core/frontend/durable/` |

All 10 green → QA single-signs T3a close → T3b opens.

---

## 3. Stop rules (T3a-specific + inherited)

Inherited from T3 sketch §6 + audit §5:
- Port-model default; no pragmatic evolution of the 7 V2 muscle files' behavior
- Storage NEVER publishes authority; adapter NEVER advances epoch
- `writeGate` NEVER reappears in `core/storage/`
- `LogicalStorage` interface signature NEVER changes in T3

T3a-specific:
1. **Adapter file is atomic** — G-int.1 + G-int.2 + G-int.3 ship in one commit. No partial adapter that T3b retrofits. If unable to land atomically (e.g., interface extension breaks too many tests to fix in one commit), sw escalates §8C.3 trigger (architectural issue) instead of splitting.
2. **Interface extension atomic with consumers** — `Backend` interface additions ship in same commit as all existing `Backend` implementations' trivial impls. No "interface added in commit A, implementations in commit B" — that breaks the build.
3. **Superblock schema bump is one-way** — `CurrentVersion++` lands with the `ImplKind` + `ImplVersion` additions; tests with hard-coded old-format bytes regen in same commit. If any existing production data exists on disk (none expected since T3 hasn't shipped), migration is a separate §8C.3 trigger.
4. **Matrix test factory is a shared helper** — adapter + superblock tests use one `logicalStorageFactories()` helper returning both walstore + smartwal constructor factories. Duplicating the factory list in multiple test files is a review-reject (makes adding new impl later require N edits).
5. **No frontend wiring in T3a** — NVMe Flush + iSCSI SYNC_CACHE handlers stay no-op. Wiring them = §8C.3 trigger #3 (cross-batch pollution).

---

## 4. Ledger rows queued (ACTIVE at T3a close)

| ID | Statement |
|---|---|
| `INV-DURABLE-OPGATE-001` | Before `SetOperational(true, _)`, all I/O returns `ErrNotReady`; preserves `INV-FRONTEND-002.*` under durable backend |
| `INV-DURABLE-IMPL-IDENTITY-001` | Superblock records `ImplKind` + `ImplVersion`; mismatch between stored ImplKind and opener's selector is rejected, not silently coerced |

Existing `INV-FRONTEND-002.EPOCH / .EV / .REPLICA / .HEALTHY` rows MUST remain ACTIVE post-T3a (adapter preserves them under durable).

---

## 5. Sign-off

### 5.1 QA single-sign (T3a open)

| Role | Signer | Date | Decision |
|---|---|---|---|
| QA Owner | Claude (QA agent) | 2026-04-22 | ✅ **SIGNED — T3a open for sw implementation** per §8C.2 |

Sw may now begin T3a code commit.

### 5.2 Sw delivery log (fill on commit)

| Date | Action | Commit |
|---|---|---|
| 2026-04-22 | T3a code delivered | `0e1595c` |
| 2026-04-22 | QA §2 acceptance walk-through complete: 10/10 green (incl. bonus Close-semantics test) | — |
| 2026-04-22 | **QA T3a-close single-sign per §8C.2** — T3a CLOSED | — |

### 5.3 Review + close

QA runs §2 acceptance 10-row walk-through after sw commit. All 10 green → QA single-signs T3a close → T3b mini-plan opens.

If any § 2 criterion fails:
- Minor (test wiring / layout): sw + QA iterate mid-batch (QA-autonomous per §8C.2)
- Major (architectural mismatch in adapter or superblock schema): §8C.3 trigger — escalate architect + PM

---

## 6. Change log

| Date | Change | Author |
|---|---|---|
| 2026-04-22 | Initial T3a mini plan; QA single-signed; sw cleared to code | QA Owner |
